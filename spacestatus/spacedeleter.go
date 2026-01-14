package spacestatus

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/util/periodicsync"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

const deletionTimeout = time.Second * 100

type Deleter func(ctx context.Context, spaceId string) (err error)

type SpaceDeleter interface {
	Run(spaces *mongo.Collection, delSender Deleter)
	Close()
}

// oldPendingSpacesQuery returns spaces that are pending deletion and have no deletion timestamp
type oldPendingSpacesQuery struct {
	deletionPeriod time.Duration
}

func (d oldPendingSpacesQuery) toMap() bson.M {
	return bson.M{"$and": bson.A{
		bson.D{{"status",
			bson.M{
				"$eq": SpaceStatusDeletionPending}}},
		bson.D{{"toBeDeletedTimestamp",
			bson.M{
				"$exists": false}}},
		bson.D{{"deletionTimestamp",
			bson.M{
				"$gt":  0,
				"$lte": time.Now().Add(-d.deletionPeriod).Unix()}}},
	}}
}

// newPendingSpacesQuery returns spaces that are pending deletion and have a deletion timestamp
// this allows us to have different deletion periods for different spaces
type newPendingSpacesQuery struct {
}

func (n newPendingSpacesQuery) toMap() bson.M {
	return bson.M{"$and": bson.A{
		bson.D{{"status",
			bson.M{
				"$eq": SpaceStatusDeletionPending}}},
		bson.D{{"toBeDeletedTimestamp",
			bson.M{
				"$gt":  0,
				"$lte": time.Now().Unix()}}},
	}}
}

type StatusEntry struct {
	SpaceId              string    `bson:"_id"`
	Identity             string    `bson:"identity"`
	DeletionPayloadType  int       `bson:"deletionPayloadType"`
	DeletionPayload      []byte    `bson:"deletionPayload"`
	DeletionTimestamp    int64     `bson:"deletionTimestamp"`
	ToBeDeletedTimestamp int64     `bson:"toBeDeletedTimestamp"`
	Status               int       `bson:"status"`
	Type                 SpaceType `bson:"type"`
	IsShareable          bool      `bson:"isShareable"`
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
	processQuery := func(query interface{}) error {
		cur, err := s.spaces.Find(ctx, query)
		if err != nil {
			return err
		}
		for cur.Next(ctx) {
			err = s.processEntry(ctx, cur)
			if err != nil {
				log.Debug("failed to process entry", zap.Error(err))
				continue
			}
		}
		return nil
	}
	for _, query := range []interface{}{
		oldPendingSpacesQuery{s.deletionPeriod}.toMap(),
		newPendingSpacesQuery{}.toMap(),
	} {
		err = processQuery(query)
		if err != nil {
			return
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
	if s.loop != nil {
		s.loop.Close()
	}
}
