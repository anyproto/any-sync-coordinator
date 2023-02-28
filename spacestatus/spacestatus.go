package spacestatus

import (
	"context"
	"errors"
	"github.com/anytypeio/any-sync-coordinator/db"
	"github.com/anytypeio/any-sync/app"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

const CName = "coordinator.spacestatus"

type StatusChange struct {
	DeletePayload []byte
	Identity      []byte
	Status        string
}

var (
	ErrDeletePayloadIsEmpty  = errors.New("delete payload is empty")
	ErrIncorrectStatusChange = errors.New("incorrect status change")
)

const (
	SpaceStatusCreated         = "created"
	SpaceStatusDeletionPending = "pending"
	SpaceStatusDeletionStarted = "deletionStarted"
	SpaceStatusDeleted         = "deleted"
)

type SpaceStatus interface {
	NewStatus(ctx context.Context, spaceId string, identity []byte) (err error)
	ChangeStatus(ctx context.Context, spaceId string, change StatusChange) (err error)
	app.ComponentRunnable
}

func New() SpaceStatus {
	return &spaceStatus{}
}

type spaceStatus struct {
	db     db.Database
	spaces *mongo.Collection
}

type findStatusQuery struct {
	SpaceId  string `bson:"_id"`
	Status   string `bson:"status"`
	Identity []byte `bson:"identity"`
}

type modifyStatusOp struct {
	Set struct {
		Status          string    `bson:"status"`
		DeletionPayload []byte    `bson:"deletionPayload"`
		DeletionDate    time.Time `bson:"deletionDate"`
	} `bson:"$set"`
}

type insertNewSpaceQuery struct {
	SpaceId string `bson:"_id"`
}

type insertNewSpaceOp struct {
	Identity []byte `bson:"identity"`
	Status   string `bson:"status"`
	SpaceId  string `bson:"_id"`
}

func (s *spaceStatus) ChangeStatus(ctx context.Context, spaceId string, change StatusChange) (err error) {
	modify := func(oldStatus, newStatus string, deletionChange []byte, t time.Time) error {
		op := modifyStatusOp{}
		op.Set.DeletionPayload = deletionChange
		op.Set.Status = newStatus
		op.Set.DeletionDate = t
		res := s.spaces.FindOneAndUpdate(ctx, findStatusQuery{
			SpaceId:  spaceId,
			Status:   oldStatus,
			Identity: change.Identity,
		}, op)
		return res.Err()
	}
	switch change.Status {
	case SpaceStatusCreated:
		return modify(SpaceStatusDeletionPending, SpaceStatusCreated, nil, time.Time{})
	case SpaceStatusDeletionPending:
		if change.DeletePayload == nil {
			return ErrDeletePayloadIsEmpty
		}
		return modify(SpaceStatusCreated, SpaceStatusDeletionPending, change.DeletePayload, time.Now())
	default:
		return ErrIncorrectStatusChange
	}
}

func (s *spaceStatus) NewStatus(ctx context.Context, spaceId string, identity []byte) (err error) {
	_, err = s.spaces.InsertOne(ctx, insertNewSpaceOp{
		Identity: identity,
		Status:   SpaceStatusCreated,
		SpaceId:  spaceId,
	})
	return
}

func (s *spaceStatus) Init(a *app.App) (err error) {
	s.db = a.MustComponent(db.CName).(db.Database)
	return
}

func (s *spaceStatus) Name() (name string) {
	return CName
}

func (s *spaceStatus) Run(ctx context.Context) (err error) {
	s.spaces = s.db.SpacesCollection()
	return
}

func (s *spaceStatus) Close(ctx context.Context) (err error) {
	return
}
