//go:generate mockgen -destination mock_inbox/mock_inbox.go github.com/anyproto/any-sync-coordinator/inbox InboxService
package inbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/subscribe"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const CName = "common.inbox"

const (
	defaultCollName   = "inboxMessages"
	defaultFetchLimit = 1000
)

var log = logger.NewNamed(CName)

var (
	ErrSomeError = errors.New("some error")
)

func New() InboxService {
	return new(inbox)
}

type InboxService interface {
	InboxAddMessage(ctx context.Context, msg *InboxMessage) (err error)
	InboxFetch(ctx context.Context, offset string) (result *InboxFetchResult, err error)
	app.ComponentRunnable
}
type inboxConfigProvider interface {
	GetInbox() Config
}
type inbox struct {
	coll             *mongo.Collection
	ctx              context.Context
	ctxCancel        context.CancelFunc
	subscribeService subscribe.SubscribeService
	conf             Config
}

func (s *inbox) Init(a *app.App) (err error) {
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
	s.conf = a.MustComponent("config").(inboxConfigProvider).GetInbox()

	if s.conf.CollName == "" {
		s.conf.CollName = defaultCollName
		log.Warn("using default inbox collection name", zap.String("collName", defaultCollName))
	}
	if s.conf.FetchLimit == 0 {
		s.conf.FetchLimit = defaultFetchLimit
		log.Warn("using default fetch limit for inbox fetch", zap.Int("fetchLimit", defaultFetchLimit))
	}

	s.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(s.conf.CollName)
	s.subscribeService = a.MustComponent(subscribe.CName).(subscribe.SubscribeService)
	return
}

func (s *inbox) Name() (name string) {
	return CName
}

func (s *inbox) Run(ctx context.Context) error {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{bson.E{Key: "packet.receiverIdentity", Value: 1}, bson.E{Key: "_id", Value: 1}},
	})
	if err != nil {
		return err
	}

	s.runStreamListener(s.ctx)
	return nil
}

func (s *inbox) Close(_ context.Context) (err error) {
	s.ctxCancel()
	return nil
}

func verifyInboxMessageSignature(ctx context.Context, msg *InboxMessage) (err error) {
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		err = fmt.Errorf("%w, failed to get pub key from ctx", coordinatorproto.ErrUnexpected)
		return
	}

	verified, err := accountPubKey.Verify(msg.Packet.Payload.Body, msg.Packet.SenderSignature)
	if err != nil {
		err = fmt.Errorf("%w, Verify() failed unexpectedly", coordinatorproto.ErrInboxMessageVerifyFailed)
		return
	}

	if !verified {
		err = fmt.Errorf("%w, signature doesn't match", coordinatorproto.ErrInboxMessageVerifyFailed)
		return
	}

	return nil
}

func (s *inbox) InboxAddMessage(ctx context.Context, msg *InboxMessage) (err error) {
	err = verifyInboxMessageSignature(ctx, msg)
	if err != nil {
		return
	}

	msg.Packet.Payload.Timestamp = time.Now().Unix()
	_, err = s.coll.InsertOne(ctx, msg)
	return err
}

type matchPipeline struct {
	Match struct {
		OT string `bson:"operationType"`
	} `bson:"$match"`
}

func (s *inbox) runStreamListener(ctx context.Context) (err error) {
	var mp matchPipeline
	mp.Match.OT = "insert"
	stream, err := s.coll.Watch(ctx, []matchPipeline{mp})
	if err != nil {
		return
	}
	go s.streamListener(stream)
	return
}

func (s *inbox) streamListener(stream *mongo.ChangeStream) {
	fmt.Printf("mongo streams\n")
	for stream.Next(context.Background()) {
		// TODO: not triggered in tests?
		fmt.Printf("mongo stream next\n")
		var res streamResult
		if err := stream.Decode(&res); err != nil {
			// mongo driver maintains connections and handles reconnects so that the stream will work as usual in these cases
			// here we have an unexpected error and should stop any operations to avoid an inconsistent state between db and cache
			log.Fatal("stream decode error:", zap.Error(err))
		}
		receiver := string(res.InboxMessage.Packet.ReceiverIdentity)
		log.Debug("stream receiver", zap.String("r", receiver))

		s.subscribeService.NotifyAllPeers(coordinatorproto.NotifyEventType_InboxNewMessageEvent, receiver, []byte(res.DocumentKey.Id))
	}
}

type streamResult struct {
	DocumentKey struct {
		Id string `bson:"_id"`
	} `bson:"documentKey"`
	InboxMessage InboxMessage `bson:"fullDocument"`
}

type InboxFetchResult struct {
	Messages []*InboxMessage
	HasMore  bool
}

func strToObjId(strId string) (objectID primitive.ObjectID, err error) {
	objectID, err = primitive.ObjectIDFromHex(strId)
	if err != nil {
		err = fmt.Errorf("failed to convert %s to objectId: %w", strId, err)
		return
	}

	return
}

// Fetches <= FetchLimit+1 amount of messages from inbox.
// If len(messages) > FetchLimit, sets `HasMore` to true.
func (s *inbox) InboxFetch(ctx context.Context, offset string) (result *InboxFetchResult, err error) {
	result = new(InboxFetchResult)
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		log.Error("failed to get account pub key")
		return nil, err
	}
	receiverIdentity := accountPubKey.Account()
	filter := bson.D{bson.E{Key: "packet.receiverIdentity", Value: receiverIdentity}}

	if offset != "" {
		objectId, errOffset := strToObjId(offset)
		if errOffset != nil {
			return nil, errOffset
		}

		filter = append(filter, bson.E{Key: "_id", Value: bson.D{bson.E{Key: "$gt", Value: objectId}}})
	}

	var messages []*InboxMessage
	sort := bson.D{bson.E{Key: "_id", Value: 1}}

	cursor, err := s.coll.Find(ctx, filter, options.Find().SetSort(sort).SetLimit(int64(s.conf.FetchLimit+1)))
	if err != nil {
		log.Warn("no new messages", zap.String("offset", offset))
		return
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &messages); err != nil {
		err = fmt.Errorf("error decoding inbox messages: %w", err)
		return
	}

	hasMore := (len(messages) > s.conf.FetchLimit)
	result.HasMore = hasMore
	result.Messages = messages
	if hasMore {
		result.Messages = result.Messages[:len(result.Messages)-1]
	}

	return
}
