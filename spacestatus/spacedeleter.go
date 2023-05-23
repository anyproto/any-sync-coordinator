package spacestatus

import (
	"context"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/util/periodicsync"
	"github.com/golang/protobuf/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"time"
)

const deletionTimeout = time.Second * 100

type DelSender interface {
	Delete(ctx context.Context, spaceId string, raw *treechangeproto.RawTreeChangeWithId) (err error)
}

type SpaceDeleter interface {
	Run(spaces *mongo.Collection, delSender DelSender)
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
	SpaceId           string `bson:"_id"`
	Identity          string `bson:"identity"`
	OldIdentity       string `bson:"oldIdentity"`
	DeletionPayload   []byte `bson:"deletionPayload"`
	DeletionTimestamp int64  `bson:"deletionTimestamp"`
	Status            int    `bson:"status"`
}

type spaceDeleter struct {
	spaces         *mongo.Collection
	runSeconds     int
	deletionPeriod time.Duration
	loop           periodicsync.PeriodicSync
	delSender      DelSender
}

var getSpaceDeleter = newSpaceDeleter

func newSpaceDeleter(runSeconds int, deletionPeriod time.Duration) SpaceDeleter {
	return &spaceDeleter{
		deletionPeriod: deletionPeriod,
		runSeconds:     runSeconds,
	}
}

func (s *spaceDeleter) Run(spaces *mongo.Collection, delSender DelSender) {
	s.delSender = delSender
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
	raw := &treechangeproto.RawTreeChangeWithId{}
	err = proto.Unmarshal(entry.DeletionPayload, raw)
	if err != nil {
		return
	}
	op := modifyStatusOp{}
	op.Set.DeletionPayload = entry.DeletionPayload
	op.Set.Status = SpaceStatusDeletionStarted
	op.Set.DeletionTimestamp = entry.DeletionTimestamp
	status := SpaceStatusDeletionPending
	res := s.spaces.FindOneAndUpdate(ctx, findStatusQuery{
		SpaceId:  entry.SpaceId,
		Status:   &status,
		Identity: entry.Identity,
	}, op)
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil
		}
		return res.Err()
	}
	err = s.delSender.Delete(ctx, entry.SpaceId, raw)
	// TODO: if this is an error related to contents of change that would never be accepted, we should remove the deletion status altogether
	if err != nil {
		op.Set.Status = SpaceStatusDeletionPending
	} else {
		op.Set.Status = SpaceStatusDeleted
	}
	status = SpaceStatusDeletionStarted
	res = s.spaces.FindOneAndUpdate(ctx, findStatusQuery{
		SpaceId:  entry.SpaceId,
		Status:   &status,
		Identity: entry.Identity,
	}, op)
	return res.Err()
}

func (s *spaceDeleter) Close() {
	s.loop.Close()
}
