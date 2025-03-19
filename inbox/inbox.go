package inbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const CName = "common.inbox.here"

var log = logger.NewNamed(CName)

var (
	ErrSomeError = errors.New("some error")
)

func New() InboxService {
	return new(inbox)
}

type InboxService interface {
	InboxAddMessage(ctx context.Context, msg *InboxMessage) (err error)
	InboxFetch(ctx context.Context, receiverIdentity string, offset string) (messages []*InboxMessage, err error)
	SubscribeClient(identity string, stream coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream)
	app.ComponentRunnable
}

type inbox struct {
	db            db.Database
	notifyStreams map[string]coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream
}

func (s *inbox) Init(a *app.App) (err error) {
	s.db = a.MustComponent(db.CName).(db.Database)
	s.notifyStreams = make(map[string]coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream)
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
	return nil
}

func (s *inbox) InboxAddMessage(ctx context.Context, msg *InboxMessage) (err error) {
	randomID := primitive.NewObjectID()
	msg.Id = randomID.Hex()
	_, err = s.db.GetInboxCollection().InsertOne(ctx, msg)
	return err
}

func (s *inbox) SubscribeClient(identity string, stream coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream) {
	s.notifyStreams[identity] = stream
}

type matchPipeline struct {
	Match struct {
		OT string `bson:"operationType"`
	} `bson:"$match"`
}

func (s *inbox) runStreamListener(ctx context.Context) (err error) {
	var mp matchPipeline
	mp.Match.OT = "insert"
	stream, err := s.db.GetInboxCollection().Watch(ctx, []matchPipeline{mp})
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
		fmt.Printf("FullDocument: %#v\n", res.InboxMessage)
		receiver := string(res.InboxMessage.Packet.ReceiverIdentity)
		log.Debug("stream receiver", zap.String("r", receiver))
		if stream, ok := s.notifyStreams[receiver]; ok {
			event := coordinatorproto.InboxNotifySubscribeEvent{
				NotifyId: res.DocumentKey.Id,
			}
			err := stream.Send(&event)
			if err != nil {
				log.Warn("error sending to notify stream", zap.String("receiver", receiver))
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

func (s *inbox) InboxFetch(ctx context.Context, receiverIdentity string, offset string) (msgs []*InboxMessage, err error) {
	log.Info("fetching inbox after offset", zap.String("offset", offset))
	filter := bson.M{"packet.receiverIdentity": receiverIdentity}

	collection := s.db.GetInboxCollection()

	if offset != "" {
		var offsetMessage InboxMessage
		err = collection.FindOne(ctx, bson.M{"_id": offset}).Decode(&offsetMessage)
		if err != nil {
			log.Warn("offset not found: return all notifications", zap.String("offset", offset))
		} else {
			filter["packet.payload.timestamp"] = bson.M{"$gt": offsetMessage.Packet.Payload.Timestamp}
		}
	}

	var messages []*InboxMessage
	sort := bson.M{"packet.payload.timestamp": 1}

	cursor, err := collection.Find(ctx, filter, options.Find().SetSort(sort))
	if err != nil {
		log.Warn("no new messages", zap.String("offset", offset))
		return messages, nil
	}
	defer cursor.Close(ctx)

	if err := cursor.All(ctx, &messages); err != nil {
		return nil, fmt.Errorf("error decoding messages: %w", err)
	}

	return messages, nil

}
