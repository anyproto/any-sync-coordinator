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
	"github.com/anyproto/any-sync-coordinator/inbox"
	"github.com/anyproto/any-sync-coordinator/inbox/mock_inbox"
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
		}).Return(nil, acl.ErrLimitExceed)

		_, err := fx.AclAddRecord(ctx, spaceId, recBytes)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceLimitReached)
	})
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:     spaceId,
			Identity:    pubKey.Account(),
			IsShareable: true,
		}, nil)
		fx.accountLimit.EXPECT().GetLimitsBySpace(ctx, spaceId).Return(accountlimit.SpaceLimits{
			SpaceMembersRead:  4,
			SpaceMembersWrite: 2,
		}, nil)
		rawRec := &consensusproto.RawRecordWithId{
			Payload: recBytes,
			Id:      "id",
		}
		fx.acl.EXPECT().AddRecord(ctx, spaceId, rec, acl.Limits{
			ReadMembers:  4,
			WriteMembers: 2,
		}).Return(rawRec, nil)
		fx.aclEventLog.EXPECT().AddLog(ctx, gomock.Any()).Return(nil)

		fx.acl.EXPECT().OwnerPubKey(ctx, spaceId).Return(pubKey, nil)

		res, err := fx.AclAddRecord(ctx, spaceId, recBytes)
		require.NoError(t, err)
		assert.Equal(t, rawRec, res)
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
	t.Run("change owner", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:     spaceId,
			Identity:    pubKey.Account(),
			IsShareable: true,
		}, nil)
		fx.accountLimit.EXPECT().GetLimitsBySpace(ctx, spaceId).Return(accountlimit.SpaceLimits{
			SpaceMembersRead:  4,
			SpaceMembersWrite: 2,
		}, nil)
		rawRec := &consensusproto.RawRecordWithId{
			Payload: recBytes,
			Id:      "id",
		}
		fx.acl.EXPECT().AddRecord(ctx, spaceId, rec, acl.Limits{
			ReadMembers:  4,
			WriteMembers: 2,
		}).Return(rawRec, nil)
		fx.aclEventLog.EXPECT().AddLog(ctx, gomock.Any()).Return(nil)

		_, newPubKey, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)

		fx.acl.EXPECT().OwnerPubKey(ctx, spaceId).Return(newPubKey, nil)
		fx.spaceStatus.EXPECT().ChangeOwner(ctx, spaceId, newPubKey.Account())
		fx.deletionLog.EXPECT().AddOwnershipChange(ctx, spaceId, rawRec.Id).Return("logId", nil)

		res, err := fx.AclAddRecord(ctx, spaceId, recBytes)
		require.NoError(t, err)
		assert.Equal(t, rawRec, res)
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
		pool:         mock_pool.NewMockService(ctrl),
		a:            new(app.App),
		ctrl:         ctrl,
	}

	anymock.ExpectComp(fx.nodeConf.EXPECT(), nodeconf.CName)
	anymock.ExpectComp(fx.spaceStatus.EXPECT(), spacestatus.CName)
	anymock.ExpectComp(fx.coordLog.EXPECT(), coordinatorlog.CName)
	anymock.ExpectComp(fx.deletionLog.EXPECT(), deletionlog.CName)
	anymock.ExpectComp(fx.inbox.EXPECT(), inbox.CName)
	anymock.ExpectComp(fx.subscribe.EXPECT(), subscribe.CName)
	anymock.ExpectComp(fx.acl.EXPECT(), acl.CName)
	anymock.ExpectComp(fx.accountLimit.EXPECT(), accountlimit.CName)
	anymock.ExpectComp(fx.aclEventLog.EXPECT(), acleventlog.CName)
	anymock.ExpectComp(fx.pool.EXPECT(), pool.CName)

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
		Register(fx.acl).
		Register(fx.accountLimit).
		Register(fx.pool).
		Register(rpctest.NewTestServer())

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
	pool         *mock_pool.MockService
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
}
