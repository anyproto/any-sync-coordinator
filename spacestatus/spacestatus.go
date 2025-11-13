//go:generate mockgen -destination mock_spacestatus/mock_spacestatus.go github.com/anyproto/any-sync-coordinator/spacestatus SpaceStatus
package spacestatus

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/deletionlog"
)

const CName = "coordinator.spacestatus"

var log = logger.NewNamed(CName)

type StatusChange struct {
	DeletionPayloadType coordinatorproto.DeletionPayloadType
	DeletionPayload     []byte
	DeletionPayloadId   string
	Identity            crypto.PubKey
	// DeletionTimestamp is ignored when setting in ChangeStatus
	DeletionTimestamp int64
	// ToBeDeletedTimestamp is ignored when setting in ChangeStatus
	ToBeDeletedTimestamp int64
	Status               int
	PeerId               string
	SpaceId              string
	NetworkId            string
}

type AccountInfo struct {
	Identity  crypto.PubKey
	PeerId    string
	NetworkId string
}

type AccountDeletion struct {
	DeletionPayload   []byte
	DeletionPayloadId string
	AccountInfo
}

type SpaceDeletion struct {
	DeletionPayload   []byte
	DeletionPayloadId string
	SpaceId           string
	DeletionPeriod    time.Duration
	AccountInfo
}

const (
	SpaceStatusCreated = iota
	SpaceStatusDeletionPending
	SpaceStatusDeletionStarted
	SpaceStatusDeleted
)

type SpaceType int

const (
	SpaceTypePersonal SpaceType = iota
	SpaceTypeTech
	SpaceTypeRegular
	SpaceTypeChat
	SpaceTypeOneToOne
)

var (
	ErrStatusExists = errors.New("space status exists")
)

const collName = "spaces"

type configProvider interface {
	GetSpaceStatus() Config
}

type SpaceStatus interface {
	NewStatus(ctx context.Context, spaceId string, identity, oldIdentity crypto.PubKey, spaceType SpaceType, force bool) (err error)
	// ChangeStatus is deprecated, use only for backwards compatibility
	ChangeStatus(ctx context.Context, change StatusChange) (entry StatusEntry, err error)
	ChangeOwner(ctx context.Context, spaceId, newOwnerId string) (err error)
	SpaceDelete(ctx context.Context, payload SpaceDeletion) (toBeDeleted int64, err error)
	AccountDelete(ctx context.Context, payload AccountDeletion) (toBeDeleted int64, err error)
	AccountRevertDeletion(ctx context.Context, payload AccountInfo) (err error)
	Status(ctx context.Context, spaceId string) (entry StatusEntry, err error)

	MakeShareable(ctx context.Context, spaceId string, spaceType SpaceType, limit uint32) (err error)
	MakeUnshareable(ctx context.Context, spaceId string) (err error)

	app.ComponentRunnable
}

func New() SpaceStatus {
	return &spaceStatus{}
}

type spaceStatus struct {
	conf           Config
	spaces         *mongo.Collection
	verifier       ChangeVerifier
	deleter        SpaceDeleter
	db             db.Database
	deletionLog    deletionlog.DeletionLog
	deletionPeriod time.Duration
}

type findStatusQuery struct {
	SpaceId  string `bson:"_id,omitempty"`
	Status   *int   `bson:"status,omitempty"`
	Type     *int   `bson:"type,omitempty"`
	Identity string `bson:"identity,omitempty"`
}

func newTechAccountQuery(identity string, status int) bson.M {
	return bson.M{
		"identity": identity,
		"$and": []bson.M{
			{
				"$or": []bson.M{
					{"type": SpaceTypeTech},
					{"type": bson.M{"$exists": false}},
				},
			},
			{"status": status},
		},
	}
}

type modifyStatusOp struct {
	Set struct {
		Status               int                                   `bson:"status"`
		DeletionPayloadType  *coordinatorproto.DeletionPayloadType `bson:"deletionPayloadType,omitempty"`
		DeletionPayload      *[]byte                               `bson:"deletionPayload,omitempty"`
		DeletionTimestamp    *int64                                `bson:"deletionTimestamp,omitempty"`
		ToBeDeletedTimestamp *int64                                `bson:"toBeDeletedTimestamp,omitempty"`
	} `bson:"$set"`
}

type insertNewSpaceOp struct {
	Identity    string    `bson:"identity"`
	OldIdentity string    `bson:"oldIdentity"`
	Status      int       `bson:"status"`
	Type        SpaceType `bson:"type"`
	IsShareable bool      `bson:"isShareable"`
	SpaceId     string    `bson:"_id"`
}

func (s *spaceStatus) AccountDelete(ctx context.Context, payload AccountDeletion) (toBeDeleted int64, err error) {
	var identity string
	if payload.Identity != nil {
		identity = payload.Identity.Account()
	}
	err = s.verifier.Verify(StatusChange{
		DeletionPayloadType: coordinatorproto.DeletionPayloadType_Account,
		DeletionPayload:     payload.DeletionPayload,
		DeletionPayloadId:   payload.DeletionPayloadId,
		Identity:            payload.Identity,
		PeerId:              payload.PeerId,
		NetworkId:           payload.NetworkId,
	})
	if err != nil {
		return
	}
	var (
		tm                   = time.Now()
		deletionTimestamp    = tm.Unix()
		toBeDeletedTimestamp = tm.Add(s.deletionPeriod).Unix()
	)
	err = s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		// Find tech space with SpaceStatusCreated status for the given identity
		if !s.accountStatusFindTx(txCtx, identity, SpaceStatusCreated) {
			return coordinatorproto.ErrAccountIsDeleted
		}
		status := SpaceStatusCreated
		// Find all spaces with SpaceStatusCreated status for the given identity
		cursor, err := s.spaces.Find(txCtx, findStatusQuery{
			Status:   &status,
			Identity: identity,
		})
		if err != nil {
			return coordinatorproto.ErrUnexpected
		}
		var spaces []StatusEntry
		if err = cursor.All(txCtx, &spaces); err != nil {
			return coordinatorproto.ErrUnexpected
		}

		// Iterate through the spaces and call setStatusTx
		for _, space := range spaces {
			change := StatusChange{
				DeletionPayloadType:  coordinatorproto.DeletionPayloadType_Account,
				DeletionPayload:      payload.DeletionPayload,
				DeletionPayloadId:    payload.DeletionPayloadId,
				Identity:             payload.Identity,
				PeerId:               payload.PeerId,
				NetworkId:            payload.NetworkId,
				Status:               SpaceStatusDeletionPending,
				ToBeDeletedTimestamp: toBeDeletedTimestamp,
				DeletionTimestamp:    deletionTimestamp,
				SpaceId:              space.SpaceId,
			}
			if _, err := s.setStatusTx(txCtx, change, SpaceStatusCreated); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return
	}
	return toBeDeletedTimestamp, nil
}

func (s *spaceStatus) AccountRevertDeletion(ctx context.Context, payload AccountInfo) (err error) {
	var identity string
	if payload.Identity != nil {
		identity = payload.Identity.Account()
	}
	return s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		if !s.accountStatusFindTx(txCtx, identity, SpaceStatusDeletionPending) {
			return coordinatorproto.ErrUnexpected
		}
		status := SpaceStatusDeletionPending
		// Find all spaces with SpaceStatusDeletionPending status for the given identity
		cursor, err := s.spaces.Find(txCtx, findStatusQuery{
			Status:   &status,
			Identity: identity,
		})
		if err != nil {
			return coordinatorproto.ErrUnexpected
		}
		var spaces []StatusEntry
		if err = cursor.All(txCtx, &spaces); err != nil {
			return coordinatorproto.ErrUnexpected
		}

		// Iterate through the spaces and call setStatusTx
		for _, space := range spaces {
			change := StatusChange{
				Identity:  payload.Identity,
				PeerId:    payload.PeerId,
				NetworkId: payload.NetworkId,
				Status:    SpaceStatusCreated,
				SpaceId:   space.SpaceId,
			}
			if _, err := s.setStatusTx(txCtx, change, SpaceStatusDeletionPending); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *spaceStatus) SpaceDelete(ctx context.Context, payload SpaceDeletion) (toBeDeleted int64, err error) {
	var (
		tm                   = time.Now()
		deletionTimestamp    = tm.Unix()
		toBeDeletedTimestamp = tm.Add(payload.DeletionPeriod).Unix()
	)
	err = s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		spType, err := s.getSpaceTypeTx(txCtx, payload.SpaceId)
		if err != nil {
			return coordinatorproto.ErrSpaceNotExists
		}
		switch spType {
		case SpaceTypeTech:
			log.Debug("cannot delete tech space", zap.Error(err), zap.String("spaceId", payload.SpaceId))
			return coordinatorproto.ErrUnexpected
		}
		change := StatusChange{
			DeletionPayloadType:  coordinatorproto.DeletionPayloadType_Confirm,
			DeletionPayload:      payload.DeletionPayload,
			DeletionPayloadId:    payload.DeletionPayloadId,
			Identity:             payload.Identity,
			PeerId:               payload.PeerId,
			NetworkId:            payload.NetworkId,
			Status:               SpaceStatusDeletionPending,
			ToBeDeletedTimestamp: toBeDeletedTimestamp,
			DeletionTimestamp:    deletionTimestamp,
			SpaceId:              payload.SpaceId,
		}
		_, err = s.setStatusTx(txCtx, change, SpaceStatusCreated)
		return err
	})
	if err != nil {
		return
	}
	return toBeDeletedTimestamp, nil
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
		tm := time.Now()
		change.DeletionTimestamp = tm.Unix()
		change.ToBeDeletedTimestamp = tm.Add(s.deletionPeriod).Unix()
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
	_, err = s.deletionLog.Add(txCtx, change.SpaceId, entry.Identity, status)
	return
}

func (s *spaceStatus) modifyStatus(ctx context.Context, change StatusChange, oldStatus int) (entry StatusEntry, err error) {
	var encodedIdentity string
	if change.Identity != nil {
		encodedIdentity = change.Identity.Account()
	}
	op := modifyStatusOp{}
	op.Set.Status = change.Status
	switch change.Status {
	case SpaceStatusDeletionPending:
		// setting deletion data to the values from the change
		op.Set.DeletionPayload = &change.DeletionPayload
		op.Set.DeletionPayloadType = &change.DeletionPayloadType
		op.Set.DeletionTimestamp = &change.DeletionTimestamp
		op.Set.ToBeDeletedTimestamp = &change.ToBeDeletedTimestamp
	case SpaceStatusCreated:
		// setting deletion data to empty values
		emptyPayload := []byte(nil)
		var (
			emptyTimestamp   int64
			emptyPayloadType coordinatorproto.DeletionPayloadType
		)
		op.Set.DeletionPayload = &emptyPayload
		op.Set.DeletionPayloadType = &emptyPayloadType
		op.Set.DeletionTimestamp = &emptyTimestamp
		op.Set.ToBeDeletedTimestamp = &emptyTimestamp
	default:
		// don't change anything (e.g. deletion data stays from SpaceStatusDeletionPending)
	}
	res := s.spaces.FindOneAndUpdate(ctx, findStatusQuery{
		SpaceId:  change.SpaceId,
		Status:   &oldStatus,
		Identity: encodedIdentity,
	}, op, options.FindOneAndUpdate().SetReturnDocument(options.After))
	if res.Err() != nil {
		curStatus, err := s.Status(ctx, change.SpaceId)
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

func (s *spaceStatus) Status(ctx context.Context, spaceId string) (entry StatusEntry, err error) {
	res := s.spaces.FindOne(ctx, findStatusQuery{
		SpaceId: spaceId,
	})
	if res.Err() != nil {
		return StatusEntry{}, notFoundOrUnexpected(res.Err())
	}
	err = res.Decode(&entry)
	return
}

func (s *spaceStatus) accountStatusFindTx(txCtx mongo.SessionContext, identity string, status int) (found bool) {
	err := s.spaces.FindOne(txCtx, newTechAccountQuery(identity, status)).Err()
	if err == nil {
		return true
	}
	return false
}

func (s *spaceStatus) getSpaceTypeTx(txCtx mongo.SessionContext, spaceId string) (spaceType SpaceType, err error) {
	var entry StatusEntry
	err = s.spaces.FindOne(txCtx, findStatusQuery{
		SpaceId: spaceId,
	}).Decode(&entry)
	if err != nil {
		return
	}
	return entry.Type, nil
}

func (s *spaceStatus) NewStatus(ctx context.Context, spaceId string, identity, oldIdentity crypto.PubKey, spaceType SpaceType, force bool) error {
	return s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		if s.accountStatusFindTx(txCtx, identity.Account(), SpaceStatusDeletionPending) {
			return coordinatorproto.ErrAccountIsDeleted
		}
		entry, err := s.Status(txCtx, spaceId)
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
				Type:        spaceType,
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

func (s *spaceStatus) MakeShareable(ctx context.Context, spaceId string, spaceType SpaceType, limit uint32) (err error) {
	return s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		entry, err := s.Status(txCtx, spaceId)
		if err != nil {
			return err
		}
		// already shareable
		if entry.IsShareable {
			return nil
		}
		count, err := s.spaces.CountDocuments(txCtx, bson.D{
			{"identity", entry.Identity},
			{"type", spaceType},
			{"status", SpaceStatusCreated},
			{"isShareable", true},
		})
		if err != nil {
			return err
		}
		if uint32(count) >= limit {
			return coordinatorproto.ErrSpaceLimitReached
		}
		_, err = s.spaces.UpdateOne(txCtx, bson.D{{"_id", spaceId}}, bson.D{{"$set", bson.D{{"isShareable", true}}}})
		return err
	})
}

func (s *spaceStatus) MakeUnshareable(ctx context.Context, spaceId string) (err error) {
	return s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		entry, err := s.Status(txCtx, spaceId)
		if err != nil {
			return err
		}
		// already shareable
		if !entry.IsShareable {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = s.spaces.UpdateOne(txCtx, bson.D{{"_id", spaceId}}, bson.D{{"$set", bson.D{{"isShareable", false}}}})
		return err
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

func (s *spaceStatus) ChangeOwner(ctx context.Context, spaceId, ownerId string) (err error) {
	_, err = s.spaces.UpdateOne(ctx, bson.D{{"_id", spaceId}}, bson.D{{"$set", bson.D{
		{"identity", ownerId},
	}}})
	return
}

func (s *spaceStatus) Init(a *app.App) (err error) {
	s.db = a.MustComponent(db.CName).(db.Database)
	s.spaces = s.db.Db().Collection(collName)
	s.verifier = getChangeVerifier()
	s.conf = a.MustComponent("config").(configProvider).GetSpaceStatus()
	s.deletionPeriod = time.Duration(s.conf.DeletionPeriodDays*24) * time.Hour
	s.deleter = getSpaceDeleter(s.conf.RunSeconds, s.deletionPeriod)
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
	if errors.Is(err, mongo.ErrNoDocuments) {
		return coordinatorproto.ErrSpaceNotExists
	} else {
		log.Info("unexpected error received", zap.Error(err))
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
