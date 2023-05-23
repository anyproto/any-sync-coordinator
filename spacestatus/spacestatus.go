package spacestatus

import (
	"context"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/nodeservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

const CName = "coordinator.spacestatus"

var log = logger.NewNamed(CName)

type StatusChange struct {
	DeletionPayload *treechangeproto.RawTreeChangeWithId
	Identity        crypto.PubKey
	Status          int
	PeerId          string
}

const (
	SpaceStatusCreated = iota
	SpaceStatusDeletionPending
	SpaceStatusDeletionStarted
	SpaceStatusDeleted
)

const collName = "spaces"

type configProvider interface {
	GetSpaceStatus() Config
}

type SpaceStatus interface {
	NewStatus(ctx context.Context, spaceId string, identity, oldIdentity crypto.PubKey) (err error)
	ChangeStatus(ctx context.Context, spaceId string, change StatusChange) (entry StatusEntry, err error)
	Status(ctx context.Context, spaceId string, pubKey crypto.PubKey) (entry StatusEntry, err error)
	app.ComponentRunnable
}

func New() SpaceStatus {
	return &spaceStatus{}
}

type spaceStatus struct {
	conf     Config
	spaces   *mongo.Collection
	verifier ChangeVerifier
	deleter  SpaceDeleter
	sender   DelSender
}

type findStatusQuery struct {
	SpaceId  string `bson:"_id"`
	Status   *int   `bson:"status,omitempty"`
	Identity string `bson:"identity"`
}

type modifyStatusOp struct {
	Set struct {
		Status            int    `bson:"status"`
		DeletionPayload   []byte `bson:"deletionPayload"`
		DeletionTimestamp int64  `bson:"deletionTimestamp"`
	} `bson:"$set"`
}

type insertNewSpaceOp struct {
	Identity    string `bson:"identity"`
	OldIdentity string `bson:"oldIdentity"`
	Status      int    `bson:"status"`
	SpaceId     string `bson:"_id"`
}

func (s *spaceStatus) ChangeStatus(ctx context.Context, spaceId string, change StatusChange) (entry StatusEntry, err error) {
	switch change.Status {
	case SpaceStatusCreated:
		return s.modifyStatus(ctx, spaceId, SpaceStatusDeletionPending, SpaceStatusCreated, nil, change.Identity, 0)
	case SpaceStatusDeletionPending:
		err = s.verifier.Verify(change.DeletionPayload, change.Identity, change.PeerId)
		if err != nil {
			log.Debug("failed to verify payload", zap.Error(err))
			return StatusEntry{}, coordinatorproto.ErrUnexpected
		}
		res, err := change.DeletionPayload.Marshal()
		if err != nil {
			log.Debug("failed to marshal payload", zap.Error(err))
			return StatusEntry{}, coordinatorproto.ErrUnexpected
		}
		return s.modifyStatus(ctx, spaceId, SpaceStatusCreated, SpaceStatusDeletionPending, res, change.Identity, time.Now().Unix())
	default:
		return StatusEntry{}, coordinatorproto.ErrUnexpected
	}
}

func (s *spaceStatus) modifyStatus(
	ctx context.Context,
	spaceId string,
	oldStatus,
	newStatus int,
	deletionChange []byte,
	identity crypto.PubKey,
	timestamp int64) (entry StatusEntry, err error) {
	encodedIdentity := identity.Account()
	op := modifyStatusOp{}
	op.Set.DeletionPayload = deletionChange
	op.Set.Status = newStatus
	op.Set.DeletionTimestamp = timestamp
	res := s.spaces.FindOneAndUpdate(ctx, findStatusQuery{
		SpaceId:  spaceId,
		Status:   &oldStatus,
		Identity: encodedIdentity,
	}, op, options.FindOneAndUpdate().SetReturnDocument(options.After))
	if res.Err() != nil {
		curStatus, err := s.Status(ctx, spaceId, identity)
		if err != nil {
			return StatusEntry{}, notFoundOrUnexpected(err)
		}
		return StatusEntry{}, incorrectStatusError(curStatus.Status)
	}
	err = res.Decode(&entry)
	if err != nil {
		log.Debug("failed to decode entry", zap.Error(err))
		err = coordinatorproto.ErrUnexpected
	}
	return
}

func (s *spaceStatus) Status(ctx context.Context, spaceId string, identity crypto.PubKey) (entry StatusEntry, err error) {
	res := s.spaces.FindOne(ctx, findStatusQuery{
		SpaceId:  spaceId,
		Identity: identity.Account(),
	})
	if res.Err() != nil {
		return StatusEntry{}, notFoundOrUnexpected(res.Err())
	}
	err = res.Decode(&entry)
	return
}

func (s *spaceStatus) NewStatus(ctx context.Context, spaceId string, identity, oldIdentity crypto.PubKey) (err error) {
	_, err = s.spaces.InsertOne(ctx, insertNewSpaceOp{
		Identity:    identity.Account(),
		OldIdentity: oldIdentity.Account(),
		Status:      SpaceStatusCreated,
		SpaceId:     spaceId,
	})
	if mongo.IsDuplicateKeyError(err) {
		err = nil
	}
	return
}

func (s *spaceStatus) Init(a *app.App) (err error) {
	s.spaces = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
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
	s.deleter.Run(s.spaces, s.sender)
	return
}

func (s *spaceStatus) Close(ctx context.Context) (err error) {
	s.deleter.Close()
	return
}

func notFoundOrUnexpected(err error) error {
	if err == mongo.ErrNoDocuments {
		return coordinatorproto.ErrSpaceNotExists
	} else {
		return coordinatorproto.ErrUnexpected
	}
}

func incorrectStatusError(curStatus int) (err error) {
	switch curStatus {
	case SpaceStatusCreated:
		return coordinatorproto.ErrSpaceIsCreated
	case SpaceStatusDeletionPending:
		return coordinatorproto.ErrSpaceDeletionPending
	default:
		return coordinatorproto.ErrSpaceIsDeleted
	}
}
