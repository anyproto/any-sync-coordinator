package coordinator

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/acl"
	"github.com/anyproto/any-sync/acl/mock_acl"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/pool/mock_pool"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/accountlimit/mock_accountlimit"
	"github.com/anyproto/any-sync-coordinator/acleventlog"
	"github.com/anyproto/any-sync-coordinator/acleventlog/mock_acleventlog"
	"github.com/anyproto/any-sync-coordinator/config"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog/mock_coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/deletionlog"
	"github.com/anyproto/any-sync-coordinator/deletionlog/mock_deletionlog"
	"github.com/anyproto/any-sync-coordinator/fileusage"
	"github.com/anyproto/any-sync-coordinator/fileusage/mock_fileusage"
	"github.com/anyproto/any-sync-coordinator/inbox"
	"github.com/anyproto/any-sync-coordinator/inbox/mock_inbox"
	"github.com/anyproto/any-sync-coordinator/invitestore"
	"github.com/anyproto/any-sync-coordinator/invitestore/mock_invitestore"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync-coordinator/spacestatus/mock_spacestatus"
	"github.com/anyproto/any-sync-coordinator/subscribe"
	"github.com/anyproto/any-sync-coordinator/subscribe/mock_subscribe"
)

var ctx = context.Background()

func TestCoordinator_MakeSpaceShareable(t *testing.T) {
	var spaceId = "space.id"

	_, pubKey, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	pubKeyData, err := pubKey.Marshall()
	require.NoError(t, err)
	ctx = peer.CtxWithIdentity(ctx, pubKeyData)
	ctx = peer.CtxWithPeerId(ctx, "peer.addr")

	t.Run("no pub key", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		require.ErrorIs(t, fx.MakeSpaceShareable(context.Background(), spaceId), coordinatorproto.ErrForbidden)
	})
	t.Run("not owner", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:  spaceId,
			Identity: "owner",
		}, nil)
		require.ErrorIs(t, fx.MakeSpaceShareable(ctx, spaceId), coordinatorproto.ErrForbidden)
	})
	t.Run("already shareable", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:     spaceId,
			Identity:    pubKey.Account(),
			IsShareable: true,
		}, nil)
		require.NoError(t, fx.MakeSpaceShareable(ctx, spaceId))
	})
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:  spaceId,
			Identity: pubKey.Account(),
			Type:     spacestatus.SpaceTypeRegular,
		}, nil)
		fx.accountLimit.EXPECT().GetLimitsBySpace(ctx, spaceId).Return(accountlimit.SpaceLimits{
			SharedSpacesLimit: 3,
		}, nil)
		fx.spaceStatus.EXPECT().MakeShareable(ctx, spaceId, spacestatus.SpaceTypeRegular, uint32(3))
		fx.aclEventLog.EXPECT().AddLog(ctx, gomock.Any()).Return(nil)

		require.NoError(t, fx.MakeSpaceShareable(ctx, spaceId))
	})
}

func TestCoordinator_MakeSpaceUnshareable(t *testing.T) {
	var (
		spaceId = "space.id"
		headId  = "headId"
	)

	_, pubKey, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	pubKeyData, err := pubKey.Marshall()
	require.NoError(t, err)
	ctx = peer.CtxWithIdentity(ctx, pubKeyData)
	ctx = peer.CtxWithPeerId(ctx, "peer.addr")

	t.Run("no pub key", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		require.ErrorIs(t, fx.MakeSpaceUnshareable(context.Background(), spaceId, headId), coordinatorproto.ErrForbidden)
	})
	t.Run("not owner", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:  spaceId,
			Identity: "owner",
		}, nil)
		require.ErrorIs(t, fx.MakeSpaceUnshareable(ctx, spaceId, headId), coordinatorproto.ErrForbidden)
	})
	t.Run("already unshareable", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:  spaceId,
			Identity: pubKey.Account(),
		}, nil)
		require.NoError(t, fx.MakeSpaceUnshareable(ctx, spaceId, headId))
	})
	t.Run("no record", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:     spaceId,
			Identity:    pubKey.Account(),
			IsShareable: true,
		}, nil)
		fx.acl.EXPECT().HasRecord(ctx, spaceId, headId).Return(false, nil)
		require.ErrorIs(t, fx.MakeSpaceUnshareable(ctx, spaceId, headId), coordinatorproto.ErrAclHeadIsMissing)
	})
	t.Run("non empty acl", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:     spaceId,
			Identity:    pubKey.Account(),
			IsShareable: true,
		}, nil)
		fx.acl.EXPECT().HasRecord(ctx, spaceId, headId).Return(true, nil)
		fx.acl.EXPECT().ReadState(ctx, spaceId, gomock.Any()).DoAndReturn(func(ctx context.Context, spaceId string, f func(s *list.AclState) error) error {
			a := list.NewAclExecutor("spaceId")
			cmds := []string{
				"a.init::a",
				"a.invite::invId",
				"b.join::invId",
				"a.revoke::invId",
				"a.approve::b,r",
			}
			for _, cmd := range cmds {
				err := a.Execute(cmd)
				require.NoError(t, err)
			}
			return f(a.ActualAccounts()["a"].Acl.AclState())
		})
		require.ErrorIs(t, fx.MakeSpaceUnshareable(ctx, spaceId, headId), coordinatorproto.ErrAclNonEmpty)
	})
	t.Run("success", func(t *testing.T) {
		/*
			TODO: list.NewTestAclStateWithUsers unavailable more
			fx := newFixture(t)
			defer fx.finish(t)
			fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
				SpaceId:     spaceId,
				Identity:    pubKey.Account(),
				IsShareable: true,
			}, nil)
			fx.acl.EXPECT().HasRecord(ctx, spaceId, headId).Return(true, nil)
			fx.acl.EXPECT().ReadState(ctx, spaceId, gomock.Any()).Do(func(ctx context.Context, spaceId string, f func(s *list.AclState) error) {
				s := list.NewTestAclStateWithUsers(1, 0, 0)
				require.NoError(t, f(s))
			})
			fx.aclEventLog.EXPECT().AddLog(ctx, gomock.Any()).Return(nil)

			fx.spaceStatus.EXPECT().MakeUnshareable(ctx, spaceId)
			require.NoError(t, fx.MakeSpaceUnshareable(ctx, spaceId, headId))

		*/
	})
}

func TestCoordinator_AclAddRecord(t *testing.T) {
	spaceId := "space.id"
	rec := &consensusproto.RawRecord{Payload: []byte("payload")}
	recBytes, _ := rec.MarshalVT()

	_, pubKey, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	pubKeyData, err := pubKey.Marshall()
	require.NoError(t, err)
	ctx = peer.CtxWithIdentity(ctx, pubKeyData)
	ctx = peer.CtxWithPeerId(ctx, "peer.addr")

	t.Run("not shareable", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId: spaceId,
		}, nil)

		_, err := fx.AclAddRecord(ctx, spaceId, recBytes)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceNotShareable)
	})
	t.Run("limit exceed", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:     spaceId,
			IsShareable: true,
		}, nil)
		fx.accountLimit.EXPECT().GetLimitsBySpace(ctx, spaceId).Return(accountlimit.SpaceLimits{
			SpaceMembersRead:  4,
			SpaceMembersWrite: 2,
		}, nil)
		fx.acl.EXPECT().AddRecord(ctx, spaceId, rec, acl.Limits{
			ReadMembers:  4,
			WriteMembers: 2,
		}, gomock.Any()).Return(nil, acl.ErrLimitExceed)

		_, err := fx.AclAddRecord(ctx, spaceId, recBytes)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceLimitReached)
	})
	t.Run("space deleted", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId: spaceId,
			Status:  spacestatus.SpaceStatusDeleted,
		}, nil)

		_, err := fx.AclAddRecord(ctx, spaceId, recBytes)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceIsDeleted)
	})
}

func TestCoordinator_AclAddRecordOwner(t *testing.T) {
	const spaceId = "space.id"
	owner, member := newAccount(t), newAccount(t)

	newSpace := func(t *testing.T) *fixture {
		fx := newAclFixture(t)
		t.Cleanup(func() { fx.finish(t) })
		fx.newAcl(t, spaceId, owner)
		fx.putMembers(t, spaceId, owner, list.AclPermissionsWriter, member)
		fx.expectSpace(spacestatus.StatusEntry{
			SpaceId:     spaceId,
			Identity:    owner.SignKey.GetPublic().Account(),
			IsShareable: true,
		})
		return fx
	}

	t.Run("a record that keeps the owner leaves the document as it is", func(t *testing.T) {
		fx := newSpace(t)
		fx.aclEventLog.EXPECT().AddLog(gomock.Any(), gomock.Any()).Return(nil)

		rec, err := fx.aclOf(t, spaceId, owner).RecordBuilder().BuildPermissionChange(list.PermissionChangePayload{
			Identity:    member.SignKey.GetPublic(),
			Permissions: list.AclPermissionsReader,
		})
		added, err := fx.addRecord(t, spaceId, owner, rec, err)
		require.NoError(t, err)
		assert.Equal(t, list.AclPermissionsReader, permissionsOf(fx.aclOf(t, spaceId, owner), member))
		assert.Equal(t, fx.consensus.log(spaceId)[2], added)
	})
	t.Run("the document takes the owner the record leads to before the network sends the record back", func(t *testing.T) {
		fx := newSpace(t)
		fx.aclEventLog.EXPECT().AddLog(gomock.Any(), gomock.Any()).Return(nil)

		rec, err := fx.aclOf(t, spaceId, owner).RecordBuilder().BuildOwnershipChange(list.OwnershipChangePayload{
			NewOwner:            member.SignKey.GetPublic(),
			OldOwnerPermissions: list.AclPermissionsAdmin,
		})
		require.NoError(t, err)
		payload, err := rec.MarshalVT()
		require.NoError(t, err)

		fx.spaceStatus.EXPECT().ChangeOwner(gomock.Any(), spaceId, member.SignKey.GetPublic().Account()).Return(nil)
		fx.deletionLog.EXPECT().AddOwnershipChange(gomock.Any(), spaceId, owner.SignKey.GetPublic().Account(), gomock.Any()).
			DoAndReturn(func(_ context.Context, _, _, recordId string) (string, error) {
				assert.Equal(t, fx.consensus.log(spaceId)[2].Id, recordId)
				return "logId", nil
			})
		// the acl service has not seen the record yet when the call returns
		changed, err := fx.AclAddRecord(accountCtx(t, owner.SignKey.GetPublic()), spaceId, payload)
		require.NoError(t, err)
		cachedOwner, err := fx.coordinator.acl.OwnerPubKey(context.Background(), spaceId)
		require.NoError(t, err)
		assert.True(t, cachedOwner.Equals(owner.SignKey.GetPublic()))
		assert.Equal(t, fx.consensus.log(spaceId)[2], changed)
	})
}

func TestCoordinator_AccountLimitsSet(t *testing.T) {
	const peerId = "payment.peer"
	peerCtx := peer.CtxWithPeerId(context.Background(), peerId)
	req := &coordinatorproto.AccountLimitsSetRequest{
		Identity:              "identity",
		Reason:                "reason",
		FileStorageLimitBytes: 1000,
		SpaceMembersRead:      10,
		SpaceMembersWrite:     5,
		SharedSpacesLimit:     3,
		ChildSpacesLimit:      7,
	}
	t.Run("only a payment node sets limits", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.nodeConf.EXPECT().NodeTypes(peerId).Return([]nodeconf.NodeType{nodeconf.NodeTypeTree})
		require.ErrorIs(t, fx.AccountLimitsSet(peerCtx, req), coordinatorproto.ErrForbidden)
	})
	t.Run("every limit of the request is stored", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.nodeConf.EXPECT().NodeTypes(peerId).Return([]nodeconf.NodeType{nodeconf.NodeTypePaymentProcessingNode})
		fx.accountLimit.EXPECT().SetLimits(peerCtx, accountlimit.Limits{
			Identity:          "identity",
			Reason:            "reason",
			FileStorageBytes:  1000,
			SpaceMembersRead:  10,
			SpaceMembersWrite: 5,
			SharedSpacesLimit: 3,
			ChildSpacesLimit:  7,
		}).Return(nil)
		require.NoError(t, fx.AccountLimitsSet(peerCtx, req))
	})
}

func TestCoordinator_AclGetRecords(t *testing.T) {
	var (
		spaceId = "space.id"
		aclHead = "aclHead"
	)
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId: spaceId,
		}, nil)
		fx.acl.EXPECT().RecordsAfter(ctx, spaceId, aclHead).Return(make([]*consensusproto.RawRecordWithId, 5), nil)

		res, err := fx.AclGetRecords(ctx, spaceId, aclHead)
		require.NoError(t, err)
		require.Len(t, res, 5)
	})
	t.Run("space deleted", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId: spaceId,
			Status:  spacestatus.SpaceStatusDeleted,
		}, nil)

		_, err := fx.AclGetRecords(ctx, spaceId, aclHead)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceIsDeleted)
	})
}

func newFixture(t *testing.T) *fixture {
	return buildFixture(t, nil)
}

// newAclFixture runs the coordinator with the real acl service over acl logs kept in memory
func newAclFixture(t *testing.T) *fixture {
	return buildFixture(t, newTestConsensus())
}

func buildFixture(t *testing.T, consensus *testConsensus) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		coordinator:  New().(*coordinator),
		nodeConf:     mock_nodeconf.NewMockService(ctrl),
		spaceStatus:  mock_spacestatus.NewMockSpaceStatus(ctrl),
		coordLog:     mock_coordinatorlog.NewMockCoordinatorLog(ctrl),
		aclEventLog:  mock_acleventlog.NewMockAclEventLog(ctrl),
		deletionLog:  mock_deletionlog.NewMockDeletionLog(ctrl),
		inbox:        mock_inbox.NewMockInboxService(ctrl),
		subscribe:    mock_subscribe.NewMockSubscribeService(ctrl),
		acl:          mock_acl.NewMockAclService(ctrl),
		accountLimit: mock_accountlimit.NewMockAccountLimit(ctrl),
		fileUsage:    mock_fileusage.NewMockFileUsage(ctrl),
		pool:         mock_pool.NewMockService(ctrl),
		inviteStore:  mock_invitestore.NewMockInviteStore(ctrl),
		consensus:    consensus,
		a:            new(app.App),
		ctrl:         ctrl,
	}

	anymock.ExpectComp(fx.nodeConf.EXPECT(), nodeconf.CName)
	anymock.ExpectComp(fx.spaceStatus.EXPECT(), spacestatus.CName)
	anymock.ExpectComp(fx.coordLog.EXPECT(), coordinatorlog.CName)
	anymock.ExpectComp(fx.deletionLog.EXPECT(), deletionlog.CName)
	anymock.ExpectComp(fx.inbox.EXPECT(), inbox.CName)
	anymock.ExpectComp(fx.subscribe.EXPECT(), subscribe.CName)
	if consensus == nil {
		anymock.ExpectComp(fx.acl.EXPECT(), acl.CName)
	} else {
		// without a network id the acl service asks for no acceptor signature on the records
		fx.nodeConf.EXPECT().Configuration().Return(nodeconf.Configuration{}).AnyTimes()
	}
	anymock.ExpectComp(fx.accountLimit.EXPECT(), accountlimit.CName)
	anymock.ExpectComp(fx.fileUsage.EXPECT(), fileusage.CName)
	anymock.ExpectComp(fx.aclEventLog.EXPECT(), acleventlog.CName)
	anymock.ExpectComp(fx.pool.EXPECT(), pool.CName)
	anymock.ExpectComp(fx.inviteStore.EXPECT(), invitestore.CName)

	fx.a.Register(fx.coordinator).
		Register(fx.nodeConf).
		Register(&config.Config{}).
		Register(&accounttest.AccountTestService{}).
		Register(fx.spaceStatus).
		Register(fx.coordLog).
		Register(fx.aclEventLog).
		Register(metric.New()).
		Register(fx.deletionLog).
		Register(fx.inbox).
		Register(fx.subscribe).
		Register(fx.accountLimit).
		Register(fx.fileUsage).
		Register(fx.pool).
		Register(fx.inviteStore).
		Register(rpctest.NewTestServer())
	if consensus == nil {
		fx.a.Register(fx.acl)
	} else {
		fx.a.Register(acl.New()).Register(consensus)
	}

	require.NoError(t, fx.a.Start(ctx))
	return fx
}

type fixture struct {
	*coordinator
	a            *app.App
	ctrl         *gomock.Controller
	nodeConf     *mock_nodeconf.MockService
	spaceStatus  *mock_spacestatus.MockSpaceStatus
	coordLog     *mock_coordinatorlog.MockCoordinatorLog
	aclEventLog  *mock_acleventlog.MockAclEventLog
	deletionLog  *mock_deletionlog.MockDeletionLog
	subscribe    *mock_subscribe.MockSubscribeService
	inbox        *mock_inbox.MockInboxService
	acl          *mock_acl.MockAclService
	accountLimit *mock_accountlimit.MockAccountLimit
	fileUsage    *mock_fileusage.MockFileUsage
	pool         *mock_pool.MockService
	inviteStore  *mock_invitestore.MockInviteStore
	// consensus is set when the coordinator runs with the real acl service
	consensus *testConsensus
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
}
