package inbox

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/anyproto/any-sync-coordinator/db"
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
	collName   = "inboxMessages"
	fetchLimit = 1000
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
	InboxFetch(ctx context.Context, receiverIdentity string, offset string) (result InboxFetchResult, err error)
	SubscribeClient(stream coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream) error
	app.ComponentRunnable
}

type inbox struct {
	coll          *mongo.Collection
	mu            sync.Mutex
	notifyStreams map[string]map[string]coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream
}

func (s *inbox) Init(a *app.App) (err error) {
	s.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	s.notifyStreams = make(map[string]map[string]coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream)
	log.Info("inbox service init")
	return
}

func (s *inbox) Name() (name string) {
	return CName
}

func (s *inbox) Run(ctx context.Context) error {
	log.Info("inbox service run")
	s.runStreamListener(ctx)
	return nil
}

func (s *inbox) Close(_ context.Context) (err error) {
	for _, streams := range s.notifyStreams {
		for _, stream := range streams {
			stream.Close()
		}
	}
	return nil
}

func (s *inbox) InboxAddMessage(ctx context.Context, msg *InboxMessage) (err error) {
	randomID := primitive.NewObjectID()
	msg.Id = randomID.Hex()
	_, err = s.coll.InsertOne(ctx, msg)
	return err
}

func (s *inbox) addStream(accountId, peerId string, stream coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.notifyStreams[accountId]; !ok {
		s.notifyStreams[accountId] = make(map[string]coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream)
	}
	s.notifyStreams[accountId][peerId] = stream
}

func (s *inbox) removeStream(accountId, peerId string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.notifyStreams[accountId], peerId)
	if len(s.notifyStreams[accountId]) == 0 {
		delete(s.notifyStreams, accountId)
	}
}

func (s *inbox) waitCloseStream(accountId, peerId string, stream coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream) {
	<-stream.Context().Done()
	s.removeStream(accountId, peerId)
}

func (s *inbox) SubscribeClient(rpcStream coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream) error {
	accountPubKey, err := peer.CtxPubKey(rpcStream.Context())
	if err != nil {
		log.Error("failed to get account pub key")
		return err
	}
	accountId := accountPubKey.Account()

	peerId, err := peer.CtxPeerId(rpcStream.Context())
	if err != nil {
		return err
	}

	s.addStream(accountId, peerId, rpcStream)
	go s.waitCloseStream(accountId, peerId, rpcStream)

	return nil
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
	for stream.Next(context.TODO()) {
		var res streamResult
		if err := stream.Decode(&res); err != nil {
			// mongo driver maintains connections and handles reconnects so that the stream will work as usual in these cases
			// here we have an unexpected error and should stop any operations to avoid an inconsistent state between db and cache
			log.Fatal("stream decode error:", zap.Error(err))
		}
		receiver := string(res.InboxMessage.Packet.ReceiverIdentity)
		log.Debug("stream receiver", zap.String("r", receiver))
		if streams, ok := s.notifyStreams[receiver]; ok {
			for peerId, stream := range streams {
				event := coordinatorproto.InboxNotifySubscribeEvent{
					NotifyId: res.DocumentKey.Id,
				}
				log.Debug("sending to notify stream", zap.String("receiver", receiver), zap.String("peerId", peerId))
				err := stream.Send(&event)
				if err != nil {
					log.Warn("error sending to notify stream", zap.String("receiver", receiver), zap.String("peerId", peerId), zap.Error(err))
					s.removeStream(receiver, peerId)
				}
			}
		} else {
			log.Warn("no such recepient", zap.String("id", receiver))
		}

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

// Fetches <= FetchLimit+1 amount of messages from inbox.
// If len(messages) > FetchLimit, sets `HasMore` to true.
func (s *inbox) InboxFetch(ctx context.Context, receiverIdentity string, offset string) (result InboxFetchResult, err error) {
	log.Info("fetching inbox after offset", zap.String("offset", offset))
	filter := bson.M{"packet.receiverIdentity": receiverIdentity}

	if offset != "" {
		var offsetMessage InboxMessage
		err = s.coll.FindOne(ctx, bson.M{"_id": offset}).Decode(&offsetMessage)
		if err != nil {
			log.Warn("offset not found: return all notifications", zap.String("offset", offset))
		} else {
			filter["packet.payload.timestamp"] = bson.M{"$gt": offsetMessage.Packet.Payload.Timestamp}
		}
	}

	var messages []*InboxMessage
	sort := bson.M{"packet.payload.timestamp": 1}

	cursor, err := s.coll.Find(ctx, filter, options.Find().SetSort(sort).SetLimit(fetchLimit+1))
	if err != nil {
		log.Warn("no new messages", zap.String("offset", offset))
		return
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &messages); err != nil {
		err = fmt.Errorf("error decoding inbox messages: %w", err)
		return
	}

	result.Messages = messages
	result.HasMore = (len(messages) > fetchLimit)

	return

}
