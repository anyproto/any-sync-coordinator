package spacestatus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func newIdentity(t *testing.T) crypto.PubKey {
	_, identity, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	return identity
}

func newChildFixture(t *testing.T) *fixture {
	fx := newFixture(t, 1, 0)
	fx.Run()
	t.Cleanup(func() { fx.Finish(t) })
	return fx
}

// newParent registers a space that can be a parent
func (fx *fixture) newParent(t *testing.T, spaceId string, owner crypto.PubKey) {
	require.NoError(t, fx.NewStatus(ctx, spaceId, owner, SpaceTypeRegular, anySpaceType, false))
}

func (fx *fixture) newChild(spaceId string, creator crypto.PubKey, parentSpaceId string) error {
	return fx.NewChildStatus(ctx, spaceId, creator, SpaceTypeRegular, anySpaceType, false, parentSpaceId, noChildLimit, true)
}

// forceChild signs a child with the force request, which takes a deleted space back
func (fx *fixture) forceChild(spaceId string, creator crypto.PubKey, parentSpaceId string) error {
	return fx.NewChildStatus(ctx, spaceId, creator, SpaceTypeRegular, anySpaceType, true, parentSpaceId, noChildLimit, true)
}

// noChildLimit lets every account have any number of children
func noChildLimit(context.Context, string) (uint32, error) {
	return 1 << 20, nil
}

// childLimits gives each account its limit, zero when it has none, and records the accounts asked
type childLimits struct {
	limits map[string]uint32
	asked  []string
}

func newChildLimits() *childLimits {
	return &childLimits{limits: map[string]uint32{}}
}

func (l *childLimits) set(owner crypto.PubKey, limit uint32) *childLimits {
	l.limits[owner.Account()] = limit
	return l
}

func (l *childLimits) limit(_ context.Context, identity string) (uint32, error) {
	l.asked = append(l.asked, identity)
	return l.limits[identity], nil
}

func (fx *fixture) signChild(spaceId string, creator crypto.PubKey, parentSpaceId string, force bool, limits *childLimits) error {
	return fx.NewChildStatus(ctx, spaceId, creator, SpaceTypeRegular, anySpaceType, force, parentSpaceId, limits.limit, true)
}

// finalDelete ends the deletion of a space as the deleter does
func (fx *fixture) finalDelete(t *testing.T, spaceId string, from int) {
	_, err := fx.SpaceStatus.(*spaceStatus).setStatus(ctx, StatusChange{
		Status:  SpaceStatusDeleted,
		SpaceId: spaceId,
	}, from)
	require.NoError(t, err)
}

func (fx *fixture) setPending(t *testing.T, spaceId string, owner crypto.PubKey) {
	_, err := fx.SpaceStatus.(*spaceStatus).setStatus(ctx, StatusChange{
		DeletionPayloadType: coordinatorproto.DeletionPayloadType_Confirm,
		Identity:            owner,
		Status:              SpaceStatusDeletionPending,
		SpaceId:             spaceId,
	}, SpaceStatusCreated)
	require.NoError(t, err)
}

func TestSpaceStatus_NewChildStatus(t *testing.T) {
	const (
		parentId = "parent.id"
		childId  = "child.id"
	)
	owner, creator := newIdentity(t), newIdentity(t)

	t.Run("the document of a child names its parent", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, creator, parentId))

		child, err := fx.Status(ctx, childId)
		require.NoError(t, err)
		assert.Equal(t, StatusEntry{
			SpaceId:       childId,
			Identity:      creator.Account(),
			Type:          SpaceTypeRegular,
			HeaderType:    anySpaceType,
			Status:        SpaceStatusCreated,
			IsShareable:   true,
			ParentSpaceId: parentId,
		}, child)

		parent, err := fx.Status(ctx, parentId)
		require.NoError(t, err)
		assert.Empty(t, parent.ParentSpaceId)
	})
	t.Run("a repeated sign keeps the child", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, creator, parentId))
		require.NoError(t, fx.newChild(childId, creator, parentId))
	})
	t.Run("no parent id", func(t *testing.T) {
		fx := newChildFixture(t)
		require.ErrorIs(t, fx.newChild(childId, creator, ""), coordinatorproto.ErrParentSpaceInvalid)
	})
	t.Run("the parent does not exist", func(t *testing.T) {
		fx := newChildFixture(t)
		require.ErrorIs(t, fx.newChild(childId, creator, parentId), coordinatorproto.ErrParentSpaceNotAvailable)
		_, err := fx.Status(ctx, childId)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceNotExists)
	})
	t.Run("the deletion of the parent is pending", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		fx.setPending(t, parentId, owner)
		require.ErrorIs(t, fx.newChild(childId, creator, parentId), coordinatorproto.ErrParentSpaceNotAvailable)
	})
	t.Run("the parent is deleted", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		fx.finalDelete(t, parentId, SpaceStatusCreated)
		require.ErrorIs(t, fx.newChild(childId, creator, parentId), coordinatorproto.ErrParentSpaceNotAvailable)
	})
	t.Run("a registered child is signed again whatever became of its parent", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, creator, parentId))
		fx.finalDelete(t, parentId, SpaceStatusCreated)
		require.NoError(t, fx.newChild(childId, creator, parentId))

		// also one whose deletion is pending, which a forced sign takes back
		fx.setPending(t, childId, creator)
		require.NoError(t, fx.forceChild(childId, creator, parentId))
		child, err := fx.Status(ctx, childId)
		require.NoError(t, err)
		assert.Equal(t, SpaceStatusCreated, child.Status)
	})
	t.Run("a deleted child answers that it is deleted before any check of its parent", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, creator, parentId))
		fx.setPending(t, childId, creator)
		fx.finalDelete(t, parentId, SpaceStatusCreated)
		require.ErrorIs(t, fx.newChild(childId, creator, parentId), coordinatorproto.ErrSpaceIsDeleted)
		fx.finalDelete(t, childId, SpaceStatusDeletionPending)
		require.ErrorIs(t, fx.newChild(childId, creator, parentId), coordinatorproto.ErrSpaceIsDeleted)
	})
	t.Run("a forced sign of a finally deleted child needs its parent as a new child does", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, creator, parentId))
		fx.finalDelete(t, childId, SpaceStatusCreated)
		fx.finalDelete(t, parentId, SpaceStatusCreated)
		require.ErrorIs(t, fx.forceChild(childId, creator, parentId), coordinatorproto.ErrParentSpaceNotAvailable)
		child, err := fx.Status(ctx, childId)
		require.NoError(t, err)
		assert.Equal(t, SpaceStatusDeleted, child.Status)
	})
	t.Run("a sign that takes a place is refused when its creator was not checked", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		unchecked := func(force bool) error {
			return fx.NewChildStatus(ctx, childId, creator, SpaceTypeRegular, anySpaceType, force, parentId, noChildLimit, false)
		}
		// a new child
		require.ErrorIs(t, unchecked(false), ErrChildTakesPlace)
		_, err := fx.Status(ctx, childId)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceNotExists)

		// a registered child takes no place, also when a forced sign takes it back
		require.NoError(t, fx.newChild(childId, creator, parentId))
		require.NoError(t, unchecked(false))
		fx.setPending(t, childId, creator)
		require.NoError(t, unchecked(true))

		// a finally deleted child that a forced sign takes back
		fx.finalDelete(t, childId, SpaceStatusCreated)
		require.ErrorIs(t, unchecked(true), ErrChildTakesPlace)
		child, err := fx.Status(ctx, childId)
		require.NoError(t, err)
		assert.Equal(t, SpaceStatusDeleted, child.Status)
	})
	t.Run("the parent is not a regular any.space", func(t *testing.T) {
		for name, tc := range map[string]struct {
			spaceType  SpaceType
			headerType string
		}{
			"tech":    {SpaceTypeTech, anyTechSpaceType},
			"1-1":     {SpaceTypeOneToOne, anyOneToOneSpaceType},
			"derived": {SpaceTypePersonal, anySpaceType},
			"anytype": {SpaceTypeRegular, regularSpaceType},
			"chat":    {SpaceTypeRegular, chatSpaceType},
			"untyped": {SpaceTypeRegular, ""},
		} {
			t.Run(name, func(t *testing.T) {
				fx := newChildFixture(t)
				require.NoError(t, fx.NewStatus(ctx, parentId, owner, tc.spaceType, tc.headerType, false))
				require.ErrorIs(t, fx.newChild(childId, creator, parentId), coordinatorproto.ErrParentSpaceInvalid)
				_, err := fx.Status(ctx, childId)
				require.ErrorIs(t, err, coordinatorproto.ErrSpaceNotExists)
			})
		}
	})
	t.Run("the parent is a child", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, creator, parentId))
		require.ErrorIs(t, fx.newChild("grandchild.id", creator, childId), coordinatorproto.ErrParentSpaceInvalid)
	})
}

func TestSpaceStatus_ParentIndex(t *testing.T) {
	fx := newChildFixture(t)
	cur, err := fx.SpaceStatus.(*spaceStatus).spaces.Indexes().List(ctx)
	require.NoError(t, err)
	var indexes []bson.M
	require.NoError(t, cur.All(ctx, &indexes))
	var found bool
	for _, index := range indexes {
		if keys, ok := index["key"].(bson.M); ok && len(keys) == 1 && keys["parentSpaceId"] != nil {
			found = true
			assert.Equal(t, true, index["sparse"])
		}
	}
	assert.True(t, found, "no index on parentSpaceId: %v", indexes)
}

func TestSpaceStatus_ChildSpacesLimit(t *testing.T) {
	const (
		parentId      = "parent.id"
		otherParentId = "otherParent.id"
	)
	owner, otherOwner, creator, otherCreator := newIdentity(t), newIdentity(t), newIdentity(t), newIdentity(t)
	status := func(t *testing.T, fx *fixture, spaceId string) int {
		entry, err := fx.Status(ctx, spaceId)
		require.NoError(t, err)
		return entry.Status
	}

	t.Run("the limit is the one of the parent's owner, whoever creates the child", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		limits := newChildLimits().set(owner, 2).set(creator, 100)

		require.NoError(t, fx.signChild("child.1", creator, parentId, false, limits))
		require.NoError(t, fx.signChild("child.2", otherCreator, parentId, false, limits))
		require.ErrorIs(t, fx.signChild("child.3", creator, parentId, false, limits), coordinatorproto.ErrChildSpacesLimitReached)
		_, err := fx.Status(ctx, "child.3")
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceNotExists)
		assert.Equal(t, []string{owner.Account(), owner.Account(), owner.Account()}, limits.asked)
	})
	t.Run("an owner without a limit has no children", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.ErrorIs(t, fx.signChild("child.1", creator, parentId, false, newChildLimits()), coordinatorproto.ErrChildSpacesLimitReached)
	})
	t.Run("the limit is counted across the parents of one owner", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		fx.newParent(t, otherParentId, owner)
		limits := newChildLimits().set(owner, 2)

		require.NoError(t, fx.signChild("child.1", creator, parentId, false, limits))
		require.NoError(t, fx.signChild("child.2", creator, otherParentId, false, limits))
		require.ErrorIs(t, fx.signChild("child.3", creator, parentId, false, limits), coordinatorproto.ErrChildSpacesLimitReached)
		require.ErrorIs(t, fx.signChild("child.3", creator, otherParentId, false, limits), coordinatorproto.ErrChildSpacesLimitReached)
	})
	t.Run("the children of another owner do not count", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		fx.newParent(t, otherParentId, otherOwner)
		limits := newChildLimits().set(owner, 1).set(otherOwner, 1)

		require.NoError(t, fx.signChild("child.1", creator, otherParentId, false, limits))
		require.NoError(t, fx.signChild("child.2", creator, parentId, false, limits))
	})
	t.Run("a repeated sign is not counted again and is not held by the limit", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		limits := newChildLimits().set(owner, 2)

		require.NoError(t, fx.signChild("child.1", creator, parentId, false, limits))
		require.NoError(t, fx.signChild("child.1", creator, parentId, false, limits))
		require.NoError(t, fx.signChild("child.1", creator, parentId, true, limits))
		// one place is left
		require.NoError(t, fx.signChild("child.2", creator, parentId, false, limits))

		// the limit drops below what the owner has: the registered children stay signable
		limits.set(owner, 0)
		require.NoError(t, fx.signChild("child.1", creator, parentId, false, limits))
		require.NoError(t, fx.signChild("child.2", creator, parentId, true, limits))
		require.ErrorIs(t, fx.signChild("child.3", creator, parentId, false, limits), coordinatorproto.ErrChildSpacesLimitReached)
	})
	t.Run("a child holds its place while its deletion is pending", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		limits := newChildLimits().set(owner, 1)

		require.NoError(t, fx.signChild("child.1", creator, parentId, false, limits))
		fx.setPending(t, "child.1", creator)
		require.ErrorIs(t, fx.signChild("child.2", creator, parentId, false, limits), coordinatorproto.ErrChildSpacesLimitReached)

		// a forced sign takes it back into the place it still holds
		require.NoError(t, fx.signChild("child.1", creator, parentId, true, limits))
		assert.Equal(t, SpaceStatusCreated, status(t, fx, "child.1"))
	})
	t.Run("a finally deleted child frees its place", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		limits := newChildLimits().set(owner, 1)

		require.NoError(t, fx.signChild("child.1", creator, parentId, false, limits))
		fx.finalDelete(t, "child.1", SpaceStatusCreated)
		require.NoError(t, fx.signChild("child.2", creator, parentId, false, limits))

		// a forced sign of the deleted child needs a place again
		require.ErrorIs(t, fx.signChild("child.1", creator, parentId, true, limits), coordinatorproto.ErrChildSpacesLimitReached)
		assert.Equal(t, SpaceStatusDeleted, status(t, fx, "child.1"))
		limits.set(owner, 2)
		require.NoError(t, fx.signChild("child.1", creator, parentId, true, limits))
		assert.Equal(t, SpaceStatusCreated, status(t, fx, "child.1"))
	})
	t.Run("the children of a parent follow a change of its owner", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		fx.newParent(t, otherParentId, owner)
		limits := newChildLimits().set(owner, 1).set(otherOwner, 1)

		require.NoError(t, fx.signChild("child.1", creator, parentId, false, limits))
		require.ErrorIs(t, fx.signChild("child.2", creator, otherParentId, false, limits), coordinatorproto.ErrChildSpacesLimitReached)

		require.NoError(t, fx.ChangeOwner(ctx, parentId, otherOwner.Account()))

		// the previous owner has its place back, the new owner's is taken
		limits.asked = nil
		require.NoError(t, fx.signChild("child.2", creator, otherParentId, false, limits))
		require.ErrorIs(t, fx.signChild("child.3", creator, parentId, false, limits), coordinatorproto.ErrChildSpacesLimitReached)
		assert.Equal(t, []string{owner.Account(), otherOwner.Account()}, limits.asked)
	})
	t.Run("a change of the child's owner moves nothing", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		limits := newChildLimits().set(owner, 1).set(otherCreator, 100)

		require.NoError(t, fx.signChild("child.1", creator, parentId, false, limits))
		require.NoError(t, fx.ChangeOwner(ctx, "child.1", otherCreator.Account()))

		child, err := fx.Status(ctx, "child.1")
		require.NoError(t, err)
		assert.Equal(t, otherCreator.Account(), child.Identity)
		assert.Equal(t, parentId, child.ParentSpaceId)
		require.ErrorIs(t, fx.signChild("child.2", creator, parentId, false, limits), coordinatorproto.ErrChildSpacesLimitReached)
	})
	t.Run("signs under one parent at the same time do not pass the limit", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		const (
			limit = 3
			signs = 8
		)
		errs := make(chan error, signs)
		for i := 0; i < signs; i++ {
			go func() {
				errs <- fx.NewChildStatus(ctx, fmt.Sprint("child.", i), creator, SpaceTypeRegular, anySpaceType, false, parentId,
					func(context.Context, string) (uint32, error) { return limit, nil }, true)
			}()
		}
		var signed int
		for i := 0; i < signs; i++ {
			if err := <-errs; err == nil {
				signed++
			} else {
				require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
			}
		}
		assert.Equal(t, limit, signed)
	})
	t.Run("an error of the limit refuses the child", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		limitErr := errors.New("no limits")
		err := fx.NewChildStatus(ctx, "child.1", creator, SpaceTypeRegular, anySpaceType, false, parentId, func(context.Context, string) (uint32, error) {
			return 0, limitErr
		}, true)
		require.ErrorIs(t, err, limitErr)
		_, err = fx.Status(ctx, "child.1")
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceNotExists)
	})
}

func TestSpaceStatus_ChildShareable(t *testing.T) {
	const (
		parentId = "parent.id"
		childId  = "child.id"
		spaceId  = "space.id"
	)
	owner, creator := newIdentity(t), newIdentity(t)

	t.Run("a child does not count against the shared spaces limit of its owner", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, creator, parentId))
		require.NoError(t, fx.NewStatus(ctx, spaceId, creator, SpaceTypeRegular, anySpaceType, false))

		require.NoError(t, fx.MakeShareable(ctx, spaceId, SpaceTypeRegular, 1))
		require.NoError(t, fx.NewStatus(ctx, "space.2", creator, SpaceTypeRegular, anySpaceType, false))
		require.ErrorIs(t, fx.MakeShareable(ctx, "space.2", SpaceTypeRegular, 1), coordinatorproto.ErrSpaceLimitReached)
	})
	t.Run("a child does not count against the shared spaces limit of the parent's owner", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, owner, parentId))
		require.NoError(t, fx.MakeShareable(ctx, parentId, SpaceTypeRegular, 1))
	})
	t.Run("making a child shareable neither counts nor fails", func(t *testing.T) {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, creator, parentId))

		require.NoError(t, fx.MakeShareable(ctx, childId, SpaceTypeRegular, 0))
		require.NoError(t, fx.MakeUnshareable(ctx, childId))
		require.NoError(t, fx.MakeShareable(ctx, childId, SpaceTypeRegular, 0))
		child, err := fx.Status(ctx, childId)
		require.NoError(t, err)
		assert.True(t, child.IsShareable)
	})
}

func TestSpaceStatus_DeleteParentAndChild(t *testing.T) {
	const (
		parentId = "parent.id"
		childId  = "child.id"
	)
	owner, creator, other := newIdentity(t), newIdentity(t), newIdentity(t)
	deletion := func(spaceId string, account crypto.PubKey, ownerOfParent string) SpaceDeletion {
		return SpaceDeletion{
			SpaceId:        spaceId,
			DeletionPeriod: time.Hour,
			OwnerOfParent:  ownerOfParent,
			AccountInfo:    AccountInfo{Identity: account},
		}
	}
	status := func(t *testing.T, fx *fixture, spaceId string) int {
		entry, err := fx.Status(ctx, spaceId)
		require.NoError(t, err)
		return entry.Status
	}
	newFamily := func(t *testing.T) *fixture {
		fx := newChildFixture(t)
		fx.newParent(t, parentId, owner)
		require.NoError(t, fx.newChild(childId, creator, parentId))
		return fx
	}

	t.Run("a parent that has a child is not deleted", func(t *testing.T) {
		fx := newFamily(t)
		_, err := fx.SpaceDelete(ctx, deletion(parentId, owner, ""))
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceHasChildren)
		assert.Equal(t, SpaceStatusCreated, status(t, fx, parentId))

		_, err = fx.ChangeStatus(ctx, StatusChange{
			DeletionPayloadType: coordinatorproto.DeletionPayloadType_Confirm,
			Identity:            owner,
			Status:              SpaceStatusDeletionPending,
			SpaceId:             parentId,
		})
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceHasChildren)
		assert.Equal(t, SpaceStatusCreated, status(t, fx, parentId))
	})
	t.Run("a child whose deletion is pending still holds its parent", func(t *testing.T) {
		fx := newFamily(t)
		_, err := fx.SpaceDelete(ctx, deletion(childId, creator, ""))
		require.NoError(t, err)
		_, err = fx.SpaceDelete(ctx, deletion(parentId, owner, ""))
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceHasChildren)
	})
	t.Run("a parent is deleted once the deletion of its children is final", func(t *testing.T) {
		fx := newFamily(t)
		_, err := fx.SpaceDelete(ctx, deletion(childId, creator, ""))
		require.NoError(t, err)
		fx.finalDelete(t, childId, SpaceStatusDeletionPending)
		_, err = fx.SpaceDelete(ctx, deletion(parentId, owner, ""))
		require.NoError(t, err)
		assert.Equal(t, SpaceStatusDeletionPending, status(t, fx, parentId))
	})
	t.Run("an account that cannot delete the parent does not learn of its children", func(t *testing.T) {
		fx := newFamily(t)
		_, err := fx.SpaceDelete(ctx, deletion(parentId, other, ""))
		require.Error(t, err)
		require.NotErrorIs(t, err, coordinatorproto.ErrSpaceHasChildren)
	})
	t.Run("the owner of the parent deletes a child", func(t *testing.T) {
		fx := newFamily(t)
		_, err := fx.SpaceDelete(ctx, deletion(childId, owner, parentId))
		require.NoError(t, err)
		child, err := fx.Status(ctx, childId)
		require.NoError(t, err)
		assert.Equal(t, SpaceStatusDeletionPending, child.Status)
		assert.Equal(t, creator.Account(), child.Identity)
		assert.True(t, child.DeletedByParentOwner)
	})
	t.Run("the owner of a child does not take back a deletion that the owner of the parent made", func(t *testing.T) {
		fx := newFamily(t)
		require.NoError(t, fx.NewStatus(ctx, "tech.id", creator, SpaceTypeTech, anyTechSpaceType, false))
		require.NoError(t, fx.NewStatus(ctx, "own.id", creator, SpaceTypeRegular, anySpaceType, false))
		_, err := fx.SpaceDelete(ctx, deletion(childId, owner, parentId))
		require.NoError(t, err)

		// not by a forced sign
		require.ErrorIs(t, fx.forceChild(childId, creator, parentId), coordinatorproto.ErrSpaceIsDeleted)
		// not by the status change
		_, err = fx.ChangeStatus(ctx, StatusChange{Identity: creator, Status: SpaceStatusCreated, SpaceId: childId})
		require.ErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied)
		// not by the revert of the deletion of its account, which takes back the rest
		_, err = fx.AccountDelete(ctx, AccountDeletion{AccountInfo: AccountInfo{Identity: creator}})
		require.NoError(t, err)
		require.NoError(t, fx.AccountRevertDeletion(ctx, AccountInfo{Identity: creator}))
		assert.Equal(t, SpaceStatusCreated, status(t, fx, "own.id"))
		assert.Equal(t, SpaceStatusDeletionPending, status(t, fx, childId))

		// nor once the deletion is final, when a forced sign takes back any other space
		fx.finalDelete(t, childId, SpaceStatusDeletionPending)
		require.ErrorIs(t, fx.forceChild(childId, creator, parentId), coordinatorproto.ErrSpaceIsDeleted)
		assert.Equal(t, SpaceStatusDeleted, status(t, fx, childId))
	})
	t.Run("the owner of a child takes back a deletion it made", func(t *testing.T) {
		fx := newFamily(t)
		_, err := fx.SpaceDelete(ctx, deletion(childId, creator, ""))
		require.NoError(t, err)
		child, err := fx.Status(ctx, childId)
		require.NoError(t, err)
		assert.False(t, child.DeletedByParentOwner)
		require.NoError(t, fx.forceChild(childId, creator, parentId))
		assert.Equal(t, SpaceStatusCreated, status(t, fx, childId))

		_, err = fx.SpaceDelete(ctx, deletion(childId, creator, ""))
		require.NoError(t, err)
		_, err = fx.ChangeStatus(ctx, StatusChange{Identity: creator, Status: SpaceStatusCreated, SpaceId: childId})
		require.NoError(t, err)
		assert.Equal(t, SpaceStatusCreated, status(t, fx, childId))
	})
	t.Run("the owner of a space that is not the parent deletes no child", func(t *testing.T) {
		fx := newFamily(t)
		fx.newParent(t, "otherParent.id", other)
		_, err := fx.SpaceDelete(ctx, deletion(childId, other, "otherParent.id"))
		require.Error(t, err)
		assert.Equal(t, SpaceStatusCreated, status(t, fx, childId))
	})
	t.Run("the owner of the parent has no such right over a top-level space", func(t *testing.T) {
		fx := newFamily(t)
		fx.newParent(t, "space.id", other)
		_, err := fx.SpaceDelete(ctx, deletion("space.id", owner, parentId))
		require.Error(t, err)
		_, err = fx.SpaceDelete(ctx, deletion("space.id", owner, "space.id"))
		require.Error(t, err)
		assert.Equal(t, SpaceStatusCreated, status(t, fx, "space.id"))
	})
	t.Run("a child and the deletion of its parent do not pass each other", func(t *testing.T) {
		fx := newChildFixture(t)
		for i := 0; i < 20; i++ {
			parent, child := fmt.Sprint("parent.", i), fmt.Sprint("child.", i)
			fx.newParent(t, parent, owner)
			signed, deleted := make(chan error, 1), make(chan error, 1)
			go func() { signed <- fx.newChild(child, creator, parent) }()
			go func() {
				_, err := fx.SpaceDelete(ctx, deletion(parent, owner, ""))
				deleted <- err
			}()
			signErr, deleteErr := <-signed, <-deleted
			if signErr == nil {
				require.ErrorIs(t, deleteErr, coordinatorproto.ErrSpaceHasChildren, "the child is signed")
			} else {
				require.ErrorIs(t, signErr, coordinatorproto.ErrParentSpaceNotAvailable)
				require.NoError(t, deleteErr, "the child is refused")
			}
		}
	})
}

func TestSpaceStatus_AccountDeleteWithChildren(t *testing.T) {
	const (
		techId   = "tech.id"
		parentId = "parent.id"
		childId  = "child.id"
	)
	owner, other := newIdentity(t), newIdentity(t)
	status := func(t *testing.T, fx *fixture, spaceId string) int {
		entry, err := fx.Status(ctx, spaceId)
		require.NoError(t, err)
		return entry.Status
	}
	// newAccount is an account with its tech space, which stands for the account, and a parent
	newAccount := func(t *testing.T) *fixture {
		fx := newChildFixture(t)
		require.NoError(t, fx.NewStatus(ctx, techId, owner, SpaceTypeTech, anyTechSpaceType, false))
		fx.newParent(t, parentId, owner)
		return fx
	}
	deleteAccount := func(fx *fixture) error {
		_, err := fx.AccountDelete(ctx, AccountDeletion{AccountInfo: AccountInfo{Identity: owner}})
		return err
	}

	t.Run("an account whose space has a child of another account is not deleted", func(t *testing.T) {
		fx := newAccount(t)
		require.NoError(t, fx.newChild(childId, other, parentId))
		require.ErrorIs(t, deleteAccount(fx), coordinatorproto.ErrSpaceHasChildren)
		for _, spaceId := range []string{techId, parentId, childId} {
			assert.Equal(t, SpaceStatusCreated, status(t, fx, spaceId), spaceId)
		}

		// the child holds the account until its deletion is final
		fx.setPending(t, childId, other)
		require.ErrorIs(t, deleteAccount(fx), coordinatorproto.ErrSpaceHasChildren)
		fx.finalDelete(t, childId, SpaceStatusDeletionPending)
		require.NoError(t, deleteAccount(fx))
		assert.Equal(t, SpaceStatusDeletionPending, status(t, fx, parentId))
	})
	t.Run("the children the account owns are deleted with it", func(t *testing.T) {
		fx := newAccount(t)
		require.NoError(t, fx.newChild(childId, owner, parentId))
		require.NoError(t, deleteAccount(fx))
		for _, spaceId := range []string{techId, parentId, childId} {
			assert.Equal(t, SpaceStatusDeletionPending, status(t, fx, spaceId), spaceId)
		}
	})
	t.Run("a child the account owns under the space of another account does not hold it", func(t *testing.T) {
		fx := newAccount(t)
		fx.newParent(t, "otherParent.id", other)
		require.NoError(t, fx.newChild(childId, owner, "otherParent.id"))
		require.NoError(t, deleteAccount(fx))
		assert.Equal(t, SpaceStatusDeletionPending, status(t, fx, childId))
		assert.Equal(t, SpaceStatusCreated, status(t, fx, "otherParent.id"))
	})
}
