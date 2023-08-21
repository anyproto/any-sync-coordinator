package spacestatus

import (
	"context"
	"github.com/anyproto/any-sync/util/periodicsync"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"time"
)

const deletionTimeout = time.Second * 100

type Deleter func(ctx context.Context, spaceId string) (err error)

type SpaceDeleter interface {
	Run(spaces *mongo.Collection, delSender Deleter)
	Close()
}

type pendingSpacesQuery struct {
	deletionPeriod time.Duration
}

func (d pendingSpacesQuery) toMap() bson.M {
	return bson.M{"$and": bson.A{
		bson.D{{"status",
			bson.M{
				"$eq": SpaceStatusDeletionPending}}},
		bson.D{{"deletionTimestamp",
			bson.M{
				"$gt":  0,
				"$lte": time.Now().Add(-d.deletionPeriod).Unix()}}}}}
}

type StatusEntry struct {
	SpaceId             string `bson:"_id"`
	Identity            string `bson:"identity"`
	OldIdentity         string `bson:"oldIdentity"`
	DeletionPayloadType int    `bson:"deletionPayloadType"`
	DeletionPayload     []byte `bson:"deletionPayload"`
	DeletionTimestamp   int64  `bson:"deletionTimestamp"`
	Status              int    `bson:"status"`
}

type spaceDeleter struct {
	spaces         *mongo.Collection
	runSeconds     int
	deletionPeriod time.Duration
	loop           periodicsync.PeriodicSync
	deleter        Deleter
}

var getSpaceDeleter = newSpaceDeleter

func newSpaceDeleter(runSeconds int, deletionPeriod time.Duration) SpaceDeleter {
	return &spaceDeleter{
		deletionPeriod: deletionPeriod,
		runSeconds:     runSeconds,
	}
}

func (s *spaceDeleter) Run(spaces *mongo.Collection, deleter Deleter) {
	s.deleter = deleter
	s.spaces = spaces
	s.loop = periodicsync.NewPeriodicSync(s.runSeconds, deletionTimeout, s.delete, log)
	s.loop.Run()
}

func (s *spaceDeleter) delete(ctx context.Context) (err error) {
	query := pendingSpacesQuery{s.deletionPeriod}.toMap()
	cur, err := s.spaces.Find(ctx, query)
	if err != nil {
		return
	}
	for cur.Next(ctx) {
		err = s.processEntry(ctx, cur)
		if err != nil {
			log.Debug("failed to process entry", zap.Error(err))
			continue
		}
	}
	return
}

func (s *spaceDeleter) processEntry(ctx context.Context, cur *mongo.Cursor) (err error) {
	entry := &StatusEntry{}
	err = cur.Decode(entry)
	if err != nil {
		return
	}
	return s.deleter(ctx, entry.SpaceId)
}

func (s *spaceDeleter) Close() {
	s.loop.Close()
}
