//go:generate mockgen -destination mock_spacestatus/mock_spacestatus.go github.com/anyproto/any-sync-coordinator/spacestatus SpaceStatus
package spacestatus

import (
	"context"
	"errors"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/deletionlog"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
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
	DeletionPayloadType coordinatorproto.DeletionPayloadType
	DeletionPayload     []byte
	DeletionPayloadId   string
	Identity            crypto.PubKey
	Status              int
	PeerId              string
	SpaceId             string
	NetworkId           string
}

const (
	SpaceStatusCreated = iota
	SpaceStatusDeletionPending
	SpaceStatusDeletionStarted
	SpaceStatusDeleted
)

var (
	ErrStatusExists = errors.New("space status exists")
)

const collName = "spaces"

type configProvider interface {
	GetSpaceStatus() Config
}

type SpaceStatus interface {
	NewStatus(ctx context.Context, spaceId string, identity, oldIdentity crypto.PubKey, force bool) (err error)
	ChangeStatus(ctx context.Context, change StatusChange) (entry StatusEntry, err error)
	Status(ctx context.Context, spaceId string, pubKey crypto.PubKey) (entry StatusEntry, err error)
	app.ComponentRunnable
}

func New() SpaceStatus {
	return &spaceStatus{}
}

type spaceStatus struct {
	conf        Config
	spaces      *mongo.Collection
	verifier    ChangeVerifier
	deleter     SpaceDeleter
	db          db.Database
	deletionLog deletionlog.DeletionLog
}

type findStatusQuery struct {
	SpaceId  string  `bson:"_id"`
	Status   *int    `bson:"status,omitempty"`
	Identity *string `bson:"identity,omitempty"`
}

type modifyStatusOp struct {
	Set struct {
		Status              int    `bson:"status"`
		DeletionPayloadType int    `bson:"deletionPayloadType"`
		DeletionPayload     []byte `bson:"deletionPayload"`
		DeletionTimestamp   int64  `bson:"deletionTimestamp"`
	} `bson:"$set"`
}

type insertNewSpaceOp struct {
	Identity    string `bson:"identity"`
	OldIdentity string `bson:"oldIdentity"`
	Status      int    `bson:"status"`
	SpaceId     string `bson:"_id"`
}

func (s *spaceStatus) ChangeStatus(ctx context.Context, change StatusChange) (entry StatusEntry, err error) {
	switch change.Status {
	case SpaceStatusCreated:
		err = s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
			if entry, err = s.setStatusTx(txCtx, change, SpaceStatusDeletionPending); err != nil {
				return err
			}
			return s.checkLimitTx(txCtx, change.Identity)
		})
		return
	case SpaceStatusDeletionPending:
		err = s.verifier.Verify(change)
		if err != nil {
			log.Debug("failed to verify payload", zap.Error(err))
			return StatusEntry{}, coordinatorproto.ErrUnexpected
		}
		return s.setStatus(ctx, change, SpaceStatusCreated)
	default:
		return StatusEntry{}, coordinatorproto.ErrUnexpected
	}
}

func (s *spaceStatus) setStatus(ctx context.Context, change StatusChange, oldStatus int) (entry StatusEntry, err error) {
	err = s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		entry, err = s.setStatusTx(txCtx, change, oldStatus)
		return err
	})
	return
}

func (s *spaceStatus) setStatusTx(txCtx mongo.SessionContext, change StatusChange, oldStatus int) (entry StatusEntry, err error) {
	entry, err = s.modifyStatus(txCtx, change, oldStatus)
	if err != nil {
		return
	}
	var status deletionlog.Status
	switch change.Status {
	case SpaceStatusDeletionPending:
		status = deletionlog.StatusRemovePrepare
	case SpaceStatusCreated:
		status = deletionlog.StatusOk
	case SpaceStatusDeleted:
		status = deletionlog.StatusRemove
	default:
		log.Error("unexpected space status", zap.Int("status", change.Status))
		err = coordinatorproto.ErrUnexpected
		return
	}
	_, err = s.deletionLog.Add(txCtx, change.SpaceId, status)
	return
}

func (s *spaceStatus) modifyStatus(ctx context.Context, change StatusChange, oldStatus int) (entry StatusEntry, err error) {
	var encodedIdentity *string
	if change.Identity != nil {
		idn := change.Identity.Account()
		encodedIdentity = &idn
	}
	op := modifyStatusOp{}
	op.Set.DeletionPayload = change.DeletionPayload
	op.Set.DeletionPayloadType = int(change.DeletionPayloadType)
	op.Set.Status = change.Status
	if change.Status != SpaceStatusCreated {
		op.Set.DeletionTimestamp = time.Now().Unix()
	}
	res := s.spaces.FindOneAndUpdate(ctx, findStatusQuery{
		SpaceId:  change.SpaceId,
		Status:   &oldStatus,
		Identity: encodedIdentity,
	}, op, options.FindOneAndUpdate().SetReturnDocument(options.After))
	if res.Err() != nil {
		curStatus, err := s.Status(ctx, change.SpaceId, change.Identity)
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
	var ident *string
	if identity != nil {
		idn := identity.Account()
		ident = &idn
	}
	res := s.spaces.FindOne(ctx, findStatusQuery{
		SpaceId:  spaceId,
		Identity: ident,
	})
	if res.Err() != nil {
		return StatusEntry{}, notFoundOrUnexpected(res.Err())
	}
	err = res.Decode(&entry)
	return
}

func (s *spaceStatus) NewStatus(ctx context.Context, spaceId string, identity, oldIdentity crypto.PubKey, force bool) error {
	return s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		entry, err := s.Status(txCtx, spaceId, identity)
		notFound := err == coordinatorproto.ErrSpaceNotExists
		if err != nil && !notFound {
			return err
		}
		if entry.Status == SpaceStatusCreated && !notFound {
			// save back compatibility
			return nil
		}
		var inserted bool
		if notFound {
			if _, err = s.spaces.InsertOne(txCtx, insertNewSpaceOp{
				Identity:    identity.Account(),
				OldIdentity: oldIdentity.Account(),
				Status:      SpaceStatusCreated,
				SpaceId:     spaceId,
			}); err != nil {
				return err
			} else {
				inserted = true
			}
		}
		if !inserted {
			if force {
				_, err = s.setStatusTx(txCtx, StatusChange{
					Identity: identity,
					Status:   SpaceStatusCreated,
					SpaceId:  spaceId,
				}, entry.Status)
			} else {
				return coordinatorproto.ErrSpaceIsDeleted
			}
		}
		if err = s.checkLimitTx(txCtx, identity); err != nil {
			return err
		}
		return nil
	})
}

type byIdentityAndStatus struct {
	Identity string `bson:"identity"`
	Status   int    `bson:"status"`
}

func (s *spaceStatus) checkLimitTx(txCtx mongo.SessionContext, identity crypto.PubKey) (err error) {
	if s.conf.SpaceLimit <= 0 {
		return
	}
	count, err := s.spaces.CountDocuments(txCtx, byIdentityAndStatus{
		Identity: identity.Account(),
		Status:   SpaceStatusCreated,
	})
	if err != nil {
		return
	}
	if count > int64(s.conf.SpaceLimit) {
		return coordinatorproto.ErrSpaceLimitReached
	}
	return
}

func (s *spaceStatus) Init(a *app.App) (err error) {
	s.db = a.MustComponent(db.CName).(db.Database)
	s.spaces = s.db.Db().Collection(collName)
	s.verifier = getChangeVerifier()
	s.conf = a.MustComponent("config").(configProvider).GetSpaceStatus()
	s.deleter = getSpaceDeleter(s.conf.RunSeconds, time.Duration(s.conf.DeletionPeriodDays*24)*time.Hour)
	s.deletionLog = app.MustComponent[deletionlog.DeletionLog](a)
	return
}

func (s *spaceStatus) Name() (name string) {
	return CName
}

func (s *spaceStatus) Run(ctx context.Context) (err error) {
	_ = s.spaces.Database().CreateCollection(ctx, collName)
	s.deleter.Run(s.spaces, func(ctx context.Context, spaceId string) error {
		_, err = s.setStatus(
			ctx,
			StatusChange{
				Status:  SpaceStatusDeleted,
				SpaceId: spaceId,
			},
			SpaceStatusDeletionPending,
		)
		return err
	})
	return
}

func (s *spaceStatus) Close(ctx context.Context) (err error) {
	if s.deleter != nil {
		s.deleter.Close()
	}
	return
}

func notFoundOrUnexpected(err error) error {
	if err == mongo.ErrNoDocuments {
		return coordinatorproto.ErrSpaceNotExists
	} else {
		log.Info("", zap.Error(err))
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
