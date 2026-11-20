package coordinator

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/util/cidutil"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

const (
	testParentId     = "parent.id"
	testAnySpaceType = "any.space"
)

// accountCtx is the context of a request made by the account
func accountCtx(t *testing.T, identity crypto.PubKey) context.Context {
	raw, err := identity.Marshall()
	require.NoError(t, err)
	return peer.CtxWithPeerId(peer.CtxWithIdentity(context.Background(), raw), "peer.id")
}

// newChildHeader builds the header any-sync gives a regular any.space created under a parent
func newChildHeader(t *testing.T, creator *accountdata.AccountKeys, parentSpaceId string) (spaceId string, header []byte) {
	masterKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	readKey, err := crypto.NewRandomAES()
	require.NoError(t, err)
	payload, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
		SigningKey:       creator.SignKey,
		SpaceType:        testAnySpaceType,
		ReplicationKey:   42,
		MasterKey:        masterKey,
		ReadKey:          readKey,
		MetadataKey:      metadataKey,
		Metadata:         []byte("metadata"),
		FileProtoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
		ParentSpaceId:    parentSpaceId,
	})
	require.NoError(t, err)
	return payload.SpaceHeaderWithId.Id, payload.SpaceHeaderWithId.RawHeader
}

// newRawHeader signs a header that names a parent, after change adjusted it
func newRawHeader(t *testing.T, creator *accountdata.AccountKeys, change func(h *spacesyncproto.SpaceHeader)) (spaceId string, header []byte) {
	identity, err := creator.SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	spaceHeader := &spacesyncproto.SpaceHeader{
		Identity:         identity,
		Timestamp:        time.Now().Unix(),
		SpaceType:        testAnySpaceType,
		ReplicationKey:   42,
		FileprotoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
		ParentSpaceId:    testParentId,
		Version:          spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion1,
	}
	change(spaceHeader)
	headerBytes, err := spaceHeader.MarshalVT()
	require.NoError(t, err)
	signature, err := creator.SignKey.Sign(headerBytes)
	require.NoError(t, err)
	header, err = (&spacesyncproto.RawSpaceHeader{SpaceHeader: headerBytes, Signature: signature}).MarshalVT()
	require.NoError(t, err)
	id, err := cidutil.NewCidFromBytes(header)
	require.NoError(t, err)
	return id + "." + strconv.FormatUint(spaceHeader.ReplicationKey, 36), header
}

// isKey matches the public key of the account, whichever instance carries it
func isKey(key crypto.PubKey) gomock.Matcher {
	return gomock.Cond(func(x crypto.PubKey) bool { return x.Equals(key) })
}

// newSignFixture is a fixture whose coordinator has the network key that signs receipts
func newSignFixture(t *testing.T) *fixture {
	fx := newFixture(t)
	networkKeys, err := accountdata.NewRandom()
	require.NoError(t, err)
	fx.account = networkKeys
	return fx
}

func parentEntry(change func(e *spacestatus.StatusEntry)) spacestatus.StatusEntry {
	entry := spacestatus.StatusEntry{
		SpaceId:    testParentId,
		Identity:   "parent.owner",
		Type:       spacestatus.SpaceTypeRegular,
		HeaderType: testAnySpaceType,
		Status:     spacestatus.SpaceStatusCreated,
	}
	if change != nil {
		change(&entry)
	}
	return entry
}

func TestCoordinator_SpaceSignChild(t *testing.T) {
	creator, err := accountdata.NewRandom()
	require.NoError(t, err)
	creatorKey := creator.SignKey.GetPublic()
	signCtx := accountCtx(t, creatorKey)
	childId, childHeader := newChildHeader(t, creator, testParentId)

	expectReceipt := func(fx *fixture) {
		fx.coordLog.EXPECT().SpaceReceipt(signCtx, gomock.Any()).Return(nil)
		fx.aclEventLog.EXPECT().AddLog(signCtx, gomock.Any()).Return(nil)
	}
	// expectFirstSign makes the child one that is not registered yet
	expectFirstSign := func(fx *fixture) {
		fx.spaceStatus.EXPECT().Status(signCtx, childId).Return(spacestatus.StatusEntry{}, coordinatorproto.ErrSpaceNotExists)
	}
	registered := func(status int) spacestatus.StatusEntry {
		return spacestatus.StatusEntry{
			SpaceId:       childId,
			Identity:      creatorKey.Account(),
			Type:          spacestatus.SpaceTypeRegular,
			HeaderType:    testAnySpaceType,
			Status:        status,
			IsShareable:   true,
			ParentSpaceId: testParentId,
		}
	}

	t.Run("a Writer or above in the parent signs a child", func(t *testing.T) {
		for _, permissions := range []list.AclPermissions{list.AclPermissionsWriter, list.AclPermissionsAdmin, list.AclPermissionsOwner} {
			fx := newSignFixture(t)
			expectFirstSign(fx)
			fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(parentEntry(nil), nil)
			fx.acl.EXPECT().Permissions(signCtx, isKey(creatorKey), testParentId).Return(permissions, nil)
			fx.spaceStatus.EXPECT().NewChildStatus(signCtx, childId, isKey(creatorKey), spacestatus.SpaceTypeRegular, testAnySpaceType, false, testParentId, gomock.Any(), true).Return(nil)
			expectReceipt(fx)

			receipt, headerType, err := fx.SpaceSign(signCtx, childId, childHeader, false)
			require.NoError(t, err)
			require.NotNil(t, receipt)
			assert.Equal(t, testAnySpaceType, headerType)
			fx.finish(t)
		}
	})
	t.Run("a registered child is signed again whatever changed in the parent", func(t *testing.T) {
		// neither the parent nor the creator's role in it is looked at
		for _, tc := range []struct {
			status int
			force  bool
		}{
			{spacestatus.SpaceStatusCreated, false},
			{spacestatus.SpaceStatusCreated, true},
			{spacestatus.SpaceStatusDeletionPending, true},
		} {
			fx := newSignFixture(t)
			fx.spaceStatus.EXPECT().Status(signCtx, childId).Return(registered(tc.status), nil)
			fx.spaceStatus.EXPECT().NewChildStatus(signCtx, childId, isKey(creatorKey), spacestatus.SpaceTypeRegular, testAnySpaceType, tc.force, testParentId, gomock.Any(), false).Return(nil)
			expectReceipt(fx)

			receipt, _, err := fx.SpaceSign(signCtx, childId, childHeader, tc.force)
			require.NoError(t, err)
			require.NotNil(t, receipt)
			fx.finish(t)
		}
	})
	t.Run("a deleted child answers that it is deleted, with no look at the parent", func(t *testing.T) {
		for _, status := range []int{spacestatus.SpaceStatusDeleted, spacestatus.SpaceStatusDeletionPending} {
			fx := newSignFixture(t)
			fx.spaceStatus.EXPECT().Status(signCtx, childId).Return(registered(status), nil)
			fx.spaceStatus.EXPECT().NewChildStatus(signCtx, childId, isKey(creatorKey), spacestatus.SpaceTypeRegular, testAnySpaceType, false, testParentId, gomock.Any(), false).
				Return(coordinatorproto.ErrSpaceIsDeleted)

			receipt, _, err := fx.SpaceSign(signCtx, childId, childHeader, false)
			require.ErrorIs(t, err, coordinatorproto.ErrSpaceIsDeleted)
			assert.Nil(t, receipt)
			fx.finish(t)
		}
	})
	t.Run("a forced sign of a finally deleted child takes it back and is checked as a new one", func(t *testing.T) {
		fx := newSignFixture(t)
		defer fx.finish(t)
		fx.spaceStatus.EXPECT().Status(signCtx, childId).Return(registered(spacestatus.SpaceStatusDeleted), nil)
		fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(parentEntry(nil), nil)
		fx.acl.EXPECT().Permissions(signCtx, isKey(creatorKey), testParentId).Return(list.AclPermissionsReader, nil)

		_, _, err := fx.SpaceSign(signCtx, childId, childHeader, true)
		require.ErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied)
	})
	t.Run("a child whose deletion becomes final during a forced sign is checked as a new one", func(t *testing.T) {
		fx := newSignFixture(t)
		defer fx.finish(t)
		gomock.InOrder(
			// the deletion is pending when the sign starts and final when the child is registered
			fx.spaceStatus.EXPECT().Status(signCtx, childId).Return(registered(spacestatus.SpaceStatusDeletionPending), nil),
			fx.spaceStatus.EXPECT().NewChildStatus(signCtx, childId, isKey(creatorKey), spacestatus.SpaceTypeRegular, testAnySpaceType, true, testParentId, gomock.Any(), false).
				Return(spacestatus.ErrChildTakesPlace),
			fx.spaceStatus.EXPECT().Status(signCtx, childId).Return(registered(spacestatus.SpaceStatusDeleted), nil),
			fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(parentEntry(nil), nil),
			fx.acl.EXPECT().Permissions(signCtx, isKey(creatorKey), testParentId).Return(list.AclPermissionsReader, nil),
		)

		receipt, _, err := fx.SpaceSign(signCtx, childId, childHeader, true)
		require.ErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied)
		assert.Nil(t, receipt)
	})
	t.Run("a forced first sign passes the same checks", func(t *testing.T) {
		fx := newSignFixture(t)
		defer fx.finish(t)
		expectFirstSign(fx)
		fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(parentEntry(nil), nil)
		fx.acl.EXPECT().Permissions(signCtx, isKey(creatorKey), testParentId).Return(list.AclPermissionsWriter, nil)
		fx.spaceStatus.EXPECT().NewChildStatus(signCtx, childId, isKey(creatorKey), spacestatus.SpaceTypeRegular, testAnySpaceType, true, testParentId, gomock.Any(), true).Return(nil)
		expectReceipt(fx)

		_, _, err := fx.SpaceSign(signCtx, childId, childHeader, true)
		require.NoError(t, err)
	})
	t.Run("the limit is the childSpacesLimit of the account asked for", func(t *testing.T) {
		fx := newSignFixture(t)
		defer fx.finish(t)
		expectFirstSign(fx)
		fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(parentEntry(nil), nil)
		fx.acl.EXPECT().Permissions(signCtx, isKey(creatorKey), testParentId).Return(list.AclPermissionsWriter, nil)
		fx.accountLimit.EXPECT().GetLimits(signCtx, "parent.owner").Return(accountlimit.Limits{SharedSpacesLimit: 3, ChildSpacesLimit: 7}, nil)
		fx.spaceStatus.EXPECT().NewChildStatus(signCtx, childId, isKey(creatorKey), spacestatus.SpaceTypeRegular, testAnySpaceType, false, testParentId, gomock.Any(), true).
			DoAndReturn(func(ctx context.Context, _ string, _ crypto.PubKey, _ spacestatus.SpaceType, _ string, _ bool, _ string, limit spacestatus.ChildSpacesLimit, _ bool) error {
				max, err := limit(ctx, "parent.owner")
				require.NoError(t, err)
				assert.Equal(t, uint32(7), max)
				return coordinatorproto.ErrChildSpacesLimitReached
			})

		receipt, _, err := fx.SpaceSign(signCtx, childId, childHeader, false)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
		assert.Nil(t, receipt)
	})
	t.Run("the creator is below Writer in the parent", func(t *testing.T) {
		for _, permissions := range []list.AclPermissions{list.AclPermissionsNone, list.AclPermissionsGuest, list.AclPermissionsReader} {
			fx := newSignFixture(t)
			expectFirstSign(fx)
			fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(parentEntry(nil), nil)
			fx.acl.EXPECT().Permissions(signCtx, isKey(creatorKey), testParentId).Return(permissions, nil)

			receipt, _, err := fx.SpaceSign(signCtx, childId, childHeader, false)
			require.ErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied)
			assert.Nil(t, receipt)
			fx.finish(t)
		}
	})
	t.Run("the parent does not exist", func(t *testing.T) {
		fx := newSignFixture(t)
		defer fx.finish(t)
		expectFirstSign(fx)
		fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(spacestatus.StatusEntry{}, coordinatorproto.ErrSpaceNotExists)

		_, _, err := fx.SpaceSign(signCtx, childId, childHeader, false)
		require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceNotAvailable)
	})
	t.Run("the parent is deleted or its deletion is pending", func(t *testing.T) {
		for _, status := range []int{spacestatus.SpaceStatusDeletionPending, spacestatus.SpaceStatusDeleted} {
			fx := newSignFixture(t)
			expectFirstSign(fx)
			fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(parentEntry(func(e *spacestatus.StatusEntry) { e.Status = status }), nil)

			_, _, err := fx.SpaceSign(signCtx, childId, childHeader, false)
			require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceNotAvailable)
			fx.finish(t)
		}
	})
	t.Run("the acl of the parent is not on the network", func(t *testing.T) {
		fx := newSignFixture(t)
		defer fx.finish(t)
		expectFirstSign(fx)
		fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(parentEntry(nil), nil)
		fx.acl.EXPECT().Permissions(signCtx, isKey(creatorKey), testParentId).Return(list.AclPermissionsNone, consensuserr.ErrLogNotFound)

		_, _, err := fx.SpaceSign(signCtx, childId, childHeader, false)
		require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceNotAvailable)
	})
	t.Run("the parent is not a top-level regular any.space", func(t *testing.T) {
		for name, change := range map[string]func(e *spacestatus.StatusEntry){
			"a child":          func(e *spacestatus.StatusEntry) { e.ParentSpaceId = "grandparent.id" },
			"a tech space":     func(e *spacestatus.StatusEntry) { e.Type, e.HeaderType = spacestatus.SpaceTypeTech, "any.techspace" },
			"a 1-1 space":      func(e *spacestatus.StatusEntry) { e.Type, e.HeaderType = spacestatus.SpaceTypeOneToOne, "any.onetoone" },
			"a derived space":  func(e *spacestatus.StatusEntry) { e.Type = spacestatus.SpaceTypePersonal },
			"an anytype space": func(e *spacestatus.StatusEntry) { e.HeaderType = "anytype.space" },
		} {
			fx := newSignFixture(t)
			expectFirstSign(fx)
			fx.spaceStatus.EXPECT().Status(signCtx, testParentId).Return(parentEntry(change), nil)

			_, _, err := fx.SpaceSign(signCtx, childId, childHeader, false)
			require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid, name)
			fx.finish(t)
		}
	})
	t.Run("the header of the child is not a v1 header of a regular any.space", func(t *testing.T) {
		for name, change := range map[string]func(h *spacesyncproto.SpaceHeader){
			"not v1":           func(h *spacesyncproto.SpaceHeader) { h.Version = spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion0 },
			"a derived space":  func(h *spacesyncproto.SpaceHeader) { h.Timestamp = 0 },
			"a tech space":     func(h *spacesyncproto.SpaceHeader) { h.SpaceType = "any.techspace" },
			"an anytype space": func(h *spacesyncproto.SpaceHeader) { h.SpaceType = "anytype.space" },
			"a chat space":     func(h *spacesyncproto.SpaceHeader) { h.SpaceType = "anytype.chatspace" },
			"an untyped space": func(h *spacesyncproto.SpaceHeader) { h.SpaceType = "" },
		} {
			fx := newSignFixture(t)
			spaceId, header := newRawHeader(t, creator, change)

			_, _, err := fx.SpaceSign(signCtx, spaceId, header, false)
			require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid, name)
			fx.finish(t)
		}
	})
	t.Run("a header that names no parent is signed as before", func(t *testing.T) {
		fx := newSignFixture(t)
		defer fx.finish(t)
		spaceId, header := newRawHeader(t, creator, func(h *spacesyncproto.SpaceHeader) { h.ParentSpaceId = "" })
		fx.spaceStatus.EXPECT().NewStatus(signCtx, spaceId, isKey(creatorKey), spacestatus.SpaceTypeRegular, testAnySpaceType, false).Return(nil)
		expectReceipt(fx)

		receipt, _, err := fx.SpaceSign(signCtx, spaceId, header, false)
		require.NoError(t, err)
		require.NotNil(t, receipt)
	})
}
