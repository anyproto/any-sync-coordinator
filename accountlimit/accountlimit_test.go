package accountlimit

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/peer/mock_peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync-coordinator/spacestatus/mock_spacestatus"
)

func TestAccountLimit_SetLimits(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	fx.nodeConf.EXPECT().FilePeers().Return([]string{"filePeer"}).Times(2)
	peer := mock_peer.NewMockPeer(fx.ctrl)
	peer.EXPECT().Id().Return("filePeer").AnyTimes()
	peer.EXPECT().DoDrpc(ctx, gomock.Any()).Times(2)
	fx.pool.AddPeer(ctx, peer)

	limits := Limits{
		Identity:          "123",
		Reason:            "reason",
		FileStorageBytes:  12345,
		SpaceMembersRead:  100,
		SpaceMembersWrite: 200,
		SharedSpacesLimit: 3,
	}

	// set
	require.NoError(t, fx.SetLimits(ctx, limits))
	result, err := fx.GetLimits(ctx, "123")
	require.NoError(t, err)
	result.UpdatedTime = time.Time{}
	assert.Equal(t, limits, result)

	// update
	limits.SpaceMembersRead = 1000
	limits.SpaceMembersWrite = 2000
	limits.Reason = "upsert"
	require.NoError(t, fx.SetLimits(ctx, limits))
	result, err = fx.GetLimits(ctx, "123")
	require.NoError(t, err)
	result.UpdatedTime = time.Time{}
	assert.Equal(t, limits, result)

}

func TestAccountLimit_GetLimits(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	// get default limits
	limits, err := fx.GetLimits(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, uint32(5), limits.SpaceMembersWrite)
	assert.Equal(t, uint32(10), limits.SpaceMembersRead)
}

func TestAccountLimit_GetLimitsBySpace(t *testing.T) {
	var (
		spaceId  = "spaceId"
		identity = "identity"
	)
	t.Run("personal", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:  spaceId,
			Identity: identity,
			Type:     spacestatus.SpaceTypePersonal,
		}, nil)

		limits, err := fx.GetLimitsBySpace(ctx, spaceId)
		require.NoError(t, err)
		assert.Equal(t, SpaceLimits{1, 1, 0}, limits)
	})

	t.Run("regular", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:  spaceId,
			Identity: identity,
			Type:     spacestatus.SpaceTypeRegular,
		}, nil)

		limits, err := fx.GetLimitsBySpace(ctx, spaceId)
		require.NoError(t, err)
		assert.Equal(t, SpaceLimits{10, 5, 3}, limits)
	})

	t.Run("chat", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId).Return(spacestatus.StatusEntry{
			SpaceId:  spaceId,
			Identity: identity,
			Type:     spacestatus.SpaceTypeChat,
		}, nil)

		limits, err := fx.GetLimitsBySpace(ctx, spaceId)
		require.NoError(t, err)
		assert.Equal(t, SpaceLimits{100, 50, 0}, limits)
	})

}

var ctx = context.Background()

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		AccountLimit: New(),
		nodeConf:     mock_nodeconf.NewMockService(ctrl),
		pool:         rpctest.NewTestPool(),
		spaceStatus:  mock_spacestatus.NewMockSpaceStatus(ctrl),
		a:            new(app.App),
		ctrl:         ctrl,
	}

	fx.nodeConf.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.nodeConf.EXPECT().Name().Return(nodeconf.CName).AnyTimes()
	fx.nodeConf.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.nodeConf.EXPECT().Close(gomock.Any()).AnyTimes()

	fx.spaceStatus.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.spaceStatus.EXPECT().Name().Return(spacestatus.CName).AnyTimes()
	fx.spaceStatus.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.spaceStatus.EXPECT().Close(gomock.Any()).AnyTimes()

	fx.a.Register(db.New()).
		Register(fx.AccountLimit).
		Register(fx.nodeConf).
		Register(fx.spaceStatus).
		Register(fx.pool).
		Register(&testConfig{})

	require.NoError(t, fx.a.Start(ctx))
	_ = fx.a.MustComponent(db.CName).(db.Database).Db().Collection(collName).Drop(ctx)
	return fx
}

type fixture struct {
	AccountLimit
	a           *app.App
	ctrl        *gomock.Controller
	nodeConf    *mock_nodeconf.MockService
	pool        *rpctest.TestPool
	spaceStatus *mock_spacestatus.MockSpaceStatus
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
}

type testConfig struct {
}

func (c *testConfig) Init(_ *app.App) error { return nil }
func (c *testConfig) Name() string          { return "config" }

func (c *testConfig) GetMongo() db.Mongo {
	return db.Mongo{
		Connect:  "mongodb://localhost:27017",
		Database: "coordinator_unittest",
	}
}

func (c *testConfig) GetAccountLimit() SpaceLimits {
	return SpaceLimits{
		SpaceMembersRead:  10,
		SpaceMembersWrite: 5,
		SharedSpacesLimit: 3,
	}
}

func (c *testConfig) GetChatSpaceLimit() SpaceLimits {
	return SpaceLimits{
		SpaceMembersRead:  100,
		SpaceMembersWrite: 50,
	}
}
