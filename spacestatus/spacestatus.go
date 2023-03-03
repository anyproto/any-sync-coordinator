package spacestatus

import (
	"context"
	"errors"
	"github.com/anytypeio/any-sync-coordinator/db"
	"github.com/anytypeio/any-sync-coordinator/nodeservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace/object/tree/treechangeproto"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

const CName = "coordinator.spacestatus"

var log = logger.NewNamed(CName)

type StatusChange struct {
	DeletionPayload *treechangeproto.RawTreeChangeWithId
	Identity        []byte
	Status          int
	PeerId          string
}

var (
	ErrIncorrectDeletePayload = errors.New("delete payload is incorrect")
	ErrIncorrectStatusChange  = errors.New("incorrect status change")
)

const (
	SpaceStatusCreated = iota
	SpaceStatusDeletionPending
	SpaceStatusDeletionStarted
	SpaceStatusDeleted
)

type configProvider interface {
	GetSpaceStatus() Config
}

type SpaceStatus interface {
	NewStatus(ctx context.Context, spaceId string, identity []byte) (err error)
	ChangeStatus(ctx context.Context, spaceId string, change StatusChange) (entry StatusEntry, err error)
	Status(ctx context.Context, spaceId string, identity []byte) (entry StatusEntry, err error)
	app.ComponentRunnable
}

func New() SpaceStatus {
	return &spaceStatus{}
}

type spaceStatus struct {
	db       db.Database
	conf     Config
	spaces   *mongo.Collection
	verifier ChangeVerifier
	deleter  SpaceDeleter
	sender   DelSender
}

type findStatusQuery struct {
	SpaceId  string `bson:"_id"`
	Status   *int   `bson:"status,omitempty"`
	Identity []byte `bson:"identity"`
}

type modifyStatusOp struct {
	Set struct {
		Status          int       `bson:"status"`
		DeletionPayload []byte    `bson:"deletionPayload"`
		DeletionDate    time.Time `bson:"deletionDate"`
	} `bson:"$set"`
}

type insertNewSpaceOp struct {
	Identity []byte `bson:"identity"`
	Status   int    `bson:"status"`
	SpaceId  string `bson:"_id"`
}

func (s *spaceStatus) ChangeStatus(ctx context.Context, spaceId string, change StatusChange) (entry StatusEntry, err error) {
	modify := func(oldStatus, newStatus int, deletionChange []byte, t time.Time) (entry StatusEntry, err error) {
		op := modifyStatusOp{}
		op.Set.DeletionPayload = deletionChange
		op.Set.Status = newStatus
		op.Set.DeletionDate = t
		res := s.spaces.FindOneAndUpdate(ctx, findStatusQuery{
			SpaceId:  spaceId,
			Status:   &oldStatus,
			Identity: change.Identity,
		}, op, options.FindOneAndUpdate().SetReturnDocument(options.After))
		if res.Err() != nil {
			err = res.Err()
			return
		}
		err = res.Decode(&entry)
		return
	}
	switch change.Status {
	case SpaceStatusCreated:
		return modify(SpaceStatusDeletionPending, SpaceStatusCreated, nil, time.Time{})
	case SpaceStatusDeletionPending:
		err = s.verifier.Verify(change.DeletionPayload, change.Identity, change.PeerId)
		if err != nil {
			log.Debug("failed to verify payload", zap.Error(err))
			return StatusEntry{}, ErrIncorrectDeletePayload
		}
		res, err := change.DeletionPayload.Marshal()
		if err != nil {
			return StatusEntry{}, err
		}
		return modify(SpaceStatusCreated, SpaceStatusDeletionPending, res, time.Now())
	default:
		return StatusEntry{}, ErrIncorrectStatusChange
	}
}

func (s *spaceStatus) Status(ctx context.Context, spaceId string, identity []byte) (entry StatusEntry, err error) {
	res := s.spaces.FindOne(ctx, findStatusQuery{
		SpaceId:  spaceId,
		Identity: identity,
	})
	if res.Err() != nil {
		err = res.Err()
		return
	}
	err = res.Decode(&entry)
	return
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
	s.sender = a.MustComponent(nodeservice.CName).(DelSender)
	s.verifier = getChangeVerifier()
	s.conf = a.MustComponent("config").(configProvider).GetSpaceStatus()
	s.deleter = getSpaceDeleter(s.conf.RunSeconds, time.Duration(s.conf.DeletionPeriodDays*24)*time.Hour)
	return
}

func (s *spaceStatus) Name() (name string) {
	return CName
}

func (s *spaceStatus) Run(ctx context.Context) (err error) {
	s.spaces = s.db.SpacesCollection()
	s.deleter.Run(s.spaces, s.sender)
	return
}

func (s *spaceStatus) Close(ctx context.Context) (err error) {
	s.deleter.Close()
	return
}
