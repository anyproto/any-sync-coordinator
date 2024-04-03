package coordinator

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/acl"
	"github.com/anyproto/any-sync/acl/mock_acl"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/accountlimit/mock_accountlimit"
	"github.com/anyproto/any-sync-coordinator/config"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog/mock_coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/deletionlog"
	"github.com/anyproto/any-sync-coordinator/deletionlog/mock_deletionlog"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync-coordinator/spacestatus/mock_spacestatus"
)

var ctx = context.Background()

func TestCoordinator_MakeSpaceShareable(t *testing.T) {
	var spaceId = "space.id"

	_, pubKey, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	pubKeyData, err := pubKey.Marshall()
	require.NoError(t, err)
	ctx = peer.CtxWithIdentity(ctx, pubKeyData)

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
		}, nil)
		fx.accountLimit.EXPECT().GetLimitsBySpace(ctx, spaceId).Return(accountlimit.SpaceLimits{
			SharedSpacesLimit: 3,
		}, nil)
		fx.spaceStatus.EXPECT().MakeShareable(ctx, spaceId, uint32(3))
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
		fx.acl.EXPECT().ReadState(ctx, spaceId, gomock.Any()).Do(func(ctx context.Context, spaceId string, f func(s *list.AclState) error) {
			s := list.NewTestAclStateWithUsers(5, 5, 5)
			require.NoError(t, f(s))
		})
		require.ErrorIs(t, fx.MakeSpaceUnshareable(ctx, spaceId, headId), coordinatorproto.ErrAclNonEmpty)
	})
	t.Run("success", func(t *testing.T) {
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
		fx.spaceStatus.EXPECT().MakeUnshareable(ctx, spaceId)
		require.NoError(t, fx.MakeSpaceUnshareable(ctx, spaceId, headId))
	})
}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		coordinator:  New().(*coordinator),
		nodeConf:     mock_nodeconf.NewMockService(ctrl),
		spaceStatus:  mock_spacestatus.NewMockSpaceStatus(ctrl),
		coordLog:     mock_coordinatorlog.NewMockCoordinatorLog(ctrl),
		deletionLog:  mock_deletionlog.NewMockDeletionLog(ctrl),
		acl:          mock_acl.NewMockAclService(ctrl),
		accountLimit: mock_accountlimit.NewMockAccountLimit(ctrl),
		a:            new(app.App),
		ctrl:         ctrl,
	}

	anymock.ExpectComp(fx.nodeConf.EXPECT(), nodeconf.CName)
	anymock.ExpectComp(fx.spaceStatus.EXPECT(), spacestatus.CName)
	anymock.ExpectComp(fx.coordLog.EXPECT(), coordinatorlog.CName)
	anymock.ExpectComp(fx.deletionLog.EXPECT(), deletionlog.CName)
	anymock.ExpectComp(fx.acl.EXPECT(), acl.CName)
	anymock.ExpectComp(fx.accountLimit.EXPECT(), accountlimit.CName)

	fx.a.Register(fx.coordinator).
		Register(fx.nodeConf).
		Register(&config.Config{}).
		Register(&accounttest.AccountTestService{}).
		Register(fx.spaceStatus).
		Register(fx.coordLog).
		Register(metric.New()).
		Register(fx.deletionLog).
		Register(fx.acl).
		Register(fx.accountLimit).
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
	deletionLog  *mock_deletionlog.MockDeletionLog
	acl          *mock_acl.MockAclService
	accountLimit *mock_accountlimit.MockAccountLimit
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
}
