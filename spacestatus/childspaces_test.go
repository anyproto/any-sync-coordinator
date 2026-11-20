package spacestatus

import (
	"testing"

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
	return fx.NewChildStatus(ctx, spaceId, creator, SpaceTypeRegular, anySpaceType, false, parentSpaceId, true)
}

// forceChild signs a child with the force request, which takes a deleted space back
func (fx *fixture) forceChild(spaceId string, creator crypto.PubKey, parentSpaceId string) error {
	return fx.NewChildStatus(ctx, spaceId, creator, SpaceTypeRegular, anySpaceType, true, parentSpaceId, true)
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
			return fx.NewChildStatus(ctx, childId, creator, SpaceTypeRegular, anySpaceType, force, parentId, false)
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
