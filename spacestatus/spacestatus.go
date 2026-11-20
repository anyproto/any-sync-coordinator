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
	// ByParentOwner tells that a deletion is made by the owner of the parent of the space
	ByParentOwner bool
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
	// OwnerOfParent is the space the coordinator found the account to be the owner of. The owner of
	// a parent deletes its children as their owners do; it is ignored for any other space.
	OwnerOfParent string
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
	// ErrChildTakesPlace is answered to a sign of a child that takes a place when its caller found it to
	// take none: the deletion of the child became final after the caller read its status. The caller
	// reads the status again and decides anew.
	ErrChildTakesPlace = errors.New("the sign of the child space takes a place")
)

const collName = "spaces"

// ChildSpacesLimit returns the childSpacesLimit of an account
type ChildSpacesLimit func(ctx context.Context, identity string) (limit uint32, err error)

type configProvider interface {
	GetSpaceStatus() Config
}

type SpaceStatus interface {
	NewStatus(ctx context.Context, spaceId string, identity crypto.PubKey, spaceType SpaceType, headerType string, force bool) (err error)
	// NewChildStatus registers a space as NewStatus does, as a child of parentSpaceId. A child is
	// shareable. A sign that takes a place (see TakesChildPlace) needs a parent that passes CheckParent
	// and room in the childSpacesLimit of the account that owns the parent, which limit tells. Any
	// other sign of a child is answered as the sign of a top-level space is. creatorChecked tells that
	// the caller found the sign to take a place and checked its creator against the parent; a sign that
	// takes a place without that check answers ErrChildTakesPlace.
	NewChildStatus(ctx context.Context, spaceId string, identity crypto.PubKey, spaceType SpaceType, headerType string, force bool, parentSpaceId string, limit ChildSpacesLimit, creatorChecked bool) (err error)
	// ChangeStatus is deprecated, use only for backwards compatibility
	ChangeStatus(ctx context.Context, change StatusChange) (entry StatusEntry, err error)
	ChangeOwner(ctx context.Context, spaceId, newOwnerId string) (err error)
	SpaceDelete(ctx context.Context, payload SpaceDeletion) (toBeDeleted int64, err error)
	AccountDelete(ctx context.Context, payload AccountDeletion) (toBeDeleted int64, err error)
	AccountRevertDeletion(ctx context.Context, payload AccountInfo) (err error)
	Status(ctx context.Context, spaceId string) (entry StatusEntry, err error)

	MakeShareable(ctx context.Context, spaceId string, spaceType SpaceType, limit uint32) (err error)
	MakeUnshareable(ctx context.Context, spaceId string) (err error)

	// RegisterSpaceRemoveHook registers fn to run inside the same transaction
	// that records a space's final deletion (StatusRemove) in the deletion log.
	// Call during component Init only.
	RegisterSpaceRemoveHook(fn func(txCtx mongo.SessionContext, spaceId string) error)

	app.ComponentRunnable
}

func New() SpaceStatus {
	return &spaceStatus{}
}

type spaceStatus struct {
	conf             Config
	spaces           *mongo.Collection
	verifier         ChangeVerifier
	deleter          SpaceDeleter
	db               db.Database
	deletionLog      deletionlog.DeletionLog
	deletionPeriod   time.Duration
	spaceRemoveHooks []func(txCtx mongo.SessionContext, spaceId string) error
}

func (s *spaceStatus) RegisterSpaceRemoveHook(fn func(txCtx mongo.SessionContext, spaceId string) error) {
	s.spaceRemoveHooks = append(s.spaceRemoveHooks, fn)
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
		DeletedByParentOwner *bool                                 `bson:"deletedByParentOwner,omitempty"`
	} `bson:"$set"`
}

type insertNewSpaceOp struct {
	Identity    string    `bson:"identity"`
	Status      int       `bson:"status"`
	Type        SpaceType `bson:"type"`
	HeaderType  string    `bson:"headerType,omitempty"`
	IsShareable bool      `bson:"isShareable"`
	SpaceId     string    `bson:"_id"`
	// ParentSpaceId is set for a child space
	ParentSpaceId string `bson:"parentSpaceId,omitempty"`
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
			return unexpectedOrTransient(err)
		}
		var spaces []StatusEntry
		if err = cursor.All(txCtx, &spaces); err != nil {
			return unexpectedOrTransient(err)
		}
		if err = s.checkNoChildrenOfOthersTx(txCtx, identity, spaces); err != nil {
			return err
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
		// Find all spaces with SpaceStatusDeletionPending status for the given identity. A child that
		// the owner of its parent deleted is not among them: its owner does not take that deletion back.
		cursor, err := s.spaces.Find(txCtx, bson.M{
			"status":               SpaceStatusDeletionPending,
			"identity":             identity,
			"deletedByParentOwner": bson.M{"$ne": true},
		})
		if err != nil {
			return unexpectedOrTransient(err)
		}
		var spaces []StatusEntry
		if err = cursor.All(txCtx, &spaces); err != nil {
			return unexpectedOrTransient(err)
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
		entry, err := s.getEntryTx(txCtx, payload.SpaceId)
		if err != nil {
			if db.IsTransientTransactionError(err) {
				return err
			}
			return coordinatorproto.ErrSpaceNotExists
		}
		switch entry.Type {
		case SpaceTypeOneToOne:
			log.Debug("cannot delete onetoone space", zap.Error(err), zap.String("spaceId", payload.SpaceId))
			return coordinatorproto.ErrUnexpected
		case SpaceTypeTech:
			log.Debug("cannot delete tech space", zap.Error(err), zap.String("spaceId", payload.SpaceId))
			return coordinatorproto.ErrUnexpected
		}
		identity := payload.Identity
		byParentOwner := entry.ParentSpaceId != "" && entry.ParentSpaceId == payload.OwnerOfParent
		if byParentOwner {
			// without an identity the document is matched whoever owns the child
			identity = nil
		}
		change := StatusChange{
			DeletionPayloadType:  coordinatorproto.DeletionPayloadType_Confirm,
			DeletionPayload:      payload.DeletionPayload,
			DeletionPayloadId:    payload.DeletionPayloadId,
			Identity:             identity,
			PeerId:               payload.PeerId,
			NetworkId:            payload.NetworkId,
			Status:               SpaceStatusDeletionPending,
			ToBeDeletedTimestamp: toBeDeletedTimestamp,
			DeletionTimestamp:    deletionTimestamp,
			SpaceId:              payload.SpaceId,
			ByParentOwner:        byParentOwner,
		}
		if _, err = s.setStatusTx(txCtx, change, SpaceStatusCreated); err != nil {
			return err
		}
		return s.checkNoChildrenTx(txCtx, payload.SpaceId)
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
			// the owner of a child does not take back a deletion that the owner of the parent made
			if current, err := s.getEntryTx(txCtx, change.SpaceId); err == nil && current.DeletedByParentOwner &&
				change.Identity != nil && current.Identity == change.Identity.Account() {
				return coordinatorproto.ErrParentPermissionDenied
			}
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
		err = s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
			if entry, err = s.setStatusTx(txCtx, change, SpaceStatusCreated); err != nil {
				return err
			}
			return s.checkNoChildrenTx(txCtx, change.SpaceId)
		})
		return
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
	if err != nil {
		return
	}
	if status == deletionlog.StatusRemove {
		for _, hook := range s.spaceRemoveHooks {
			if err = hook(txCtx, change.SpaceId); err != nil {
				return
			}
		}
	}
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
		if change.ByParentOwner {
			op.Set.DeletedByParentOwner = &change.ByParentOwner
		}
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
		// A WriteConflict (or other transient transaction error) must be
		// propagated with its label intact so the enclosing Tx can retry it;
		// it does not mean the status precondition failed.
		if db.IsTransientTransactionError(res.Err()) {
			return StatusEntry{}, res.Err()
		}
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

func (s *spaceStatus) getEntryTx(txCtx mongo.SessionContext, spaceId string) (entry StatusEntry, err error) {
	err = s.spaces.FindOne(txCtx, findStatusQuery{
		SpaceId: spaceId,
	}).Decode(&entry)
	return
}

// childrenQuery matches the children of a space. A child is one until its deletion is final.
func childrenQuery(parentSpaceId string) bson.M {
	return bson.M{
		"parentSpaceId": parentSpaceId,
		"status":        bson.M{"$ne": SpaceStatusDeleted},
	}
}

// checkNoChildrenTx refuses the deletion of a space that still has children: they would be left
// without the owner that answers for them. It runs after the deletion passed its own checks, so only
// an account that may delete the space learns that it has children.
func (s *spaceStatus) checkNoChildrenTx(txCtx mongo.SessionContext, spaceId string) error {
	count, err := s.spaces.CountDocuments(txCtx, childrenQuery(spaceId), options.Count().SetLimit(1))
	if err != nil {
		return unexpectedOrTransient(err)
	}
	if count > 0 {
		return coordinatorproto.ErrSpaceHasChildren
	}
	return nil
}

// checkNoChildrenOfOthersTx refuses the deletion of an account while a space it owns has a child of
// another account: the child would be left without a parent. The children the account owns itself are
// deleted with it.
func (s *spaceStatus) checkNoChildrenOfOthersTx(txCtx mongo.SessionContext, identity string, owned []StatusEntry) error {
	parentIds := make([]string, 0, len(owned))
	for _, space := range owned {
		parentIds = append(parentIds, space.SpaceId)
	}
	count, err := s.spaces.CountDocuments(txCtx, bson.M{
		"parentSpaceId": bson.M{"$in": parentIds},
		"status":        bson.M{"$ne": SpaceStatusDeleted},
		"identity":      bson.M{"$ne": identity},
	}, options.Count().SetLimit(1))
	if err != nil {
		return unexpectedOrTransient(err)
	}
	if count > 0 {
		return coordinatorproto.ErrSpaceHasChildren
	}
	return nil
}

func (s *spaceStatus) NewStatus(ctx context.Context, spaceId string, identity crypto.PubKey, spaceType SpaceType, headerType string, force bool) error {
	return s.newStatus(ctx, spaceId, identity, spaceType, headerType, force, "", nil, false)
}

func (s *spaceStatus) NewChildStatus(ctx context.Context, spaceId string, identity crypto.PubKey, spaceType SpaceType, headerType string, force bool, parentSpaceId string, limit ChildSpacesLimit, creatorChecked bool) error {
	if parentSpaceId == "" {
		return coordinatorproto.ErrParentSpaceInvalid
	}
	return s.newStatus(ctx, spaceId, identity, spaceType, headerType, force, parentSpaceId, limit, creatorChecked)
}

// TakesChildPlace tells whether a sign registers a child anew and so takes a place, under its parent
// and in the limit of the parent's owner: the child is not registered, or its deletion is final and
// the sign is forced, which takes it back. A registered child holds its place until its deletion is
// final, and a sign of it takes none. No sign takes back a child that the owner of its parent deleted.
func TakesChildPlace(registered bool, child StatusEntry, force bool) bool {
	return !registered || (force && child.Status == SpaceStatusDeleted && !child.DeletedByParentOwner)
}

// parentTx returns the parent of a space that is being registered, if it can take the child. Its
// write makes the transaction conflict with a deletion of the parent and with another registration
// under it, so each of them sees the other.
func (s *spaceStatus) parentTx(txCtx mongo.SessionContext, parentSpaceId string) (parent StatusEntry, err error) {
	err = s.spaces.FindOneAndUpdate(txCtx,
		bson.M{"_id": parentSpaceId},
		bson.M{"$inc": bson.M{"childRev": 1}},
	).Decode(&parent)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return StatusEntry{}, coordinatorproto.ErrParentSpaceNotAvailable
		}
		return StatusEntry{}, unexpectedOrTransient(err)
	}
	return parent, CheckParent(parent)
}

// checkChildSpacesLimitTx refuses a child that takes the owner of its parent over the owner's
// childSpacesLimit. It runs after the child is stored, so the count includes it. Registrations under
// one parent run one after another (see parentTx); two under different parents of one owner at the same
// instant do not see each other's child and can both pass, as with the shared spaces limit.
func (s *spaceStatus) checkChildSpacesLimitTx(txCtx mongo.SessionContext, owner string, limit ChildSpacesLimit) error {
	allowed, err := limit(txCtx, owner)
	if err != nil {
		return err
	}
	count, err := s.countChildrenOfOwnerTx(txCtx, owner)
	if err != nil {
		return unexpectedOrTransient(err)
	}
	if count > int64(allowed) {
		return coordinatorproto.ErrChildSpacesLimitReached
	}
	return nil
}

// countChildrenOfOwnerTx counts the children of every space the account owns now, so the children
// of a parent follow a change of its owner. A child counts until its deletion is final.
func (s *spaceStatus) countChildrenOfOwnerTx(txCtx mongo.SessionContext, owner string) (count int64, err error) {
	cur, err := s.spaces.Find(txCtx, bson.M{"identity": owner}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return 0, err
	}
	var owned []struct {
		SpaceId string `bson:"_id"`
	}
	if err = cur.All(txCtx, &owned); err != nil {
		return 0, err
	}
	parentIds := make([]string, 0, len(owned))
	for _, space := range owned {
		parentIds = append(parentIds, space.SpaceId)
	}
	return s.spaces.CountDocuments(txCtx, bson.M{
		"parentSpaceId": bson.M{"$in": parentIds},
		"status":        bson.M{"$ne": SpaceStatusDeleted},
	})
}

func (s *spaceStatus) newStatus(ctx context.Context, spaceId string, identity crypto.PubKey, spaceType SpaceType, headerType string, force bool, parentSpaceId string, limit ChildSpacesLimit, creatorChecked bool) error {
	return s.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		if s.accountStatusFindTx(txCtx, identity.Account(), SpaceStatusDeletionPending) {
			return coordinatorproto.ErrAccountIsDeleted
		}
		isChild := parentSpaceId != ""
		entry, err := s.Status(txCtx, spaceId)
		notFound := err == coordinatorproto.ErrSpaceNotExists
		if err != nil && !notFound {
			return err
		}
		if entry.Status == SpaceStatusCreated && !notFound {
			// save back compatibility
			return nil
		}
		// only a child that takes a place is checked against the parent and the limit
		takesPlace := isChild && TakesChildPlace(!notFound, entry, force)
		if takesPlace && !creatorChecked {
			return ErrChildTakesPlace
		}
		var parent StatusEntry
		if takesPlace {
			if parent, err = s.parentTx(txCtx, parentSpaceId); err != nil {
				return err
			}
		}
		var inserted bool
		if notFound {
			if _, err = s.spaces.InsertOne(txCtx, insertNewSpaceOp{
				Identity:      identity.Account(),
				Status:        SpaceStatusCreated,
				SpaceId:       spaceId,
				Type:          spaceType,
				HeaderType:    headerType,
				ParentSpaceId: parentSpaceId,
				IsShareable:   isChild,
			}); err != nil {
				return err
			} else {
				inserted = true
			}
		}
		if !inserted {
			// a forced sign takes a deleted space back, but not a child that the owner of its parent deleted
			if force && !entry.DeletedByParentOwner {
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
		if takesPlace {
			return s.checkChildSpacesLimitTx(txCtx, parent.Identity, limit)
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
		// a child space is outside the shared spaces limit: it neither counts against it nor is held by it
		if entry.ParentSpaceId == "" {
			count, err := s.spaces.CountDocuments(txCtx, bson.M{
				"identity":      entry.Identity,
				"type":          spaceType,
				"status":        SpaceStatusCreated,
				"isShareable":   true,
				"parentSpaceId": bson.M{"$exists": false},
			})
			if err != nil {
				return err
			}
			if uint32(count) >= limit {
				return coordinatorproto.ErrSpaceLimitReached
			}
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
	// serves the children of a parent; top-level spaces have no such field and stay out of it
	if _, err = s.spaces.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "parentSpaceId", Value: 1}},
		Options: options.Index().SetSparse(true),
	}); err != nil {
		return err
	}
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
	} else if db.IsTransientTransactionError(err) {
		// keep the retry label so the enclosing Tx can retry
		return err
	} else {
		log.Info("unexpected error received", zap.Error(err))
		return coordinatorproto.ErrUnexpected
	}
}

// unexpectedOrTransient preserves a transient transaction error (e.g. a mongo
// WriteConflict) so the enclosing Tx can retry it, while mapping any other
// error to the generic ErrUnexpected sentinel.
func unexpectedOrTransient(err error) error {
	if db.IsTransientTransactionError(err) {
		return err
	}
	return coordinatorproto.ErrUnexpected
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
