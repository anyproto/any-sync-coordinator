package fileusage

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/accountlimit/mock_accountlimit"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync-coordinator/spacestatus/mock_spacestatus"
)

var ctx = context.Background()

func row(spaceId, identity string, bytes, files uint64) *coordinatorproto.FileUsageRow {
	return &coordinatorproto.FileUsageRow{
		SpaceId:           spaceId,
		Identity:          identity,
		DurableBytes:      bytes,
		DurableFilesCount: files,
	}
}

func TestFileUsage_ReportAndGetLimits(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	fx.nodeConf.EXPECT().FileV2NodeIds(gomock.Any()).Return([]string{"node1", "node2"}).AnyTimes()
	fx.accountLimit.EXPECT().GetLimits(gomock.Any(), "alice").
		Return(accountlimit.Limits{Identity: "alice", FileStorageBytes: 1000}, nil).AnyTimes()

	// two spaces charge alice; both pair nodes report space1, MAX wins
	require.NoError(t, fx.Report(ctx, "node1", []*coordinatorproto.FileUsageRow{
		row("space1", "alice", 100, 2),
		row("space2", "alice", 50, 1),
	}))
	require.NoError(t, fx.Report(ctx, "node2", []*coordinatorproto.FileUsageRow{
		row("space1", "alice", 120, 3), // node2 is ahead
	}))

	resp, err := fx.GetLimits(ctx, "space1", "alice")
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), resp.AccountLimitBytes)
	assert.Equal(t, uint64(170), resp.AccountTotalUsageBytes) // max(100,120) + 50
	assert.Equal(t, uint64(0), resp.SpaceLimitBytes)

	// absolute re-report replaces, never accumulates (GC shrink on node2)
	require.NoError(t, fx.Report(ctx, "node2", []*coordinatorproto.FileUsageRow{
		row("space1", "alice", 80, 2),
	}))
	resp, err = fx.GetLimits(ctx, "space1", "alice")
	require.NoError(t, err)
	assert.Equal(t, uint64(150), resp.AccountTotalUsageBytes) // max(100,80) + 50
}

func TestFileUsage_NonMemberDropped(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	fx.nodeConf.EXPECT().FileV2NodeIds("space1").Return([]string{"node1", "node2"}).AnyTimes()
	fx.accountLimit.EXPECT().GetLimits(gomock.Any(), "alice").
		Return(accountlimit.Limits{Identity: "alice"}, nil).AnyTimes()

	require.NoError(t, fx.Report(ctx, "stranger", []*coordinatorproto.FileUsageRow{
		row("space1", "alice", 999, 9),
	}))
	resp, err := fx.GetLimits(ctx, "space1", "alice")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), resp.AccountTotalUsageBytes)
}

func TestFileUsage_SpaceRemoveHook(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	fx.nodeConf.EXPECT().FileV2NodeIds(gomock.Any()).Return([]string{"node1", "node2"}).AnyTimes()
	fx.accountLimit.EXPECT().GetLimits(gomock.Any(), "alice").
		Return(accountlimit.Limits{Identity: "alice"}, nil).AnyTimes()

	require.NoError(t, fx.Report(ctx, "node1", []*coordinatorproto.FileUsageRow{
		row("space1", "alice", 100, 2),
		row("space2", "alice", 50, 1),
	}))
	require.NotNil(t, fx.removeHook)
	require.NoError(t, fx.db.Tx(ctx, func(txCtx mongo.SessionContext) error {
		return fx.removeHook(txCtx, "space1")
	}))

	resp, err := fx.GetLimits(ctx, "", "alice")
	require.NoError(t, err)
	assert.Equal(t, uint64(50), resp.AccountTotalUsageBytes)
}

func TestEffectiveBytes(t *testing.T) {
	now := time.Now()
	members := []string{"node1", "node2"}
	fresh := func(nodeId string, bytes int64) nodeReport {
		return nodeReport{NodeId: nodeId, Bytes: bytes, UpdatedAt: now.Add(-time.Minute)}
	}
	stale := func(nodeId string, bytes int64, age time.Duration) nodeReport {
		return nodeReport{NodeId: nodeId, Bytes: bytes, UpdatedAt: now.Add(-age)}
	}

	t.Run("max over fresh members", func(t *testing.T) {
		doc := usageDoc{PerNode: []nodeReport{fresh("node1", 100), fresh("node2", 120)}}
		assert.Equal(t, uint64(120), effectiveBytes(doc, members, now))
	})
	t.Run("expired member loses to fresh member", func(t *testing.T) {
		doc := usageDoc{PerNode: []nodeReport{stale("node1", 500, 2*time.Hour), fresh("node2", 120)}}
		assert.Equal(t, uint64(120), effectiveBytes(doc, members, now))
	})
	t.Run("all expired retains newest", func(t *testing.T) {
		doc := usageDoc{PerNode: []nodeReport{stale("node1", 500, 3*time.Hour), stale("node2", 300, 2*time.Hour)}}
		assert.Equal(t, uint64(300), effectiveBytes(doc, members, now))
	})
	t.Run("resharded away retains newest non-member", func(t *testing.T) {
		doc := usageDoc{PerNode: []nodeReport{fresh("oldNode", 400)}}
		assert.Equal(t, uint64(400), effectiveBytes(doc, []string{"newNode1", "newNode2"}, now))
	})
	t.Run("member report beats fresher non-member", func(t *testing.T) {
		doc := usageDoc{PerNode: []nodeReport{stale("node1", 100, time.Minute), fresh("oldNode", 900)}}
		assert.Equal(t, uint64(100), effectiveBytes(doc, members, now))
	})
	t.Run("empty doc is zero", func(t *testing.T) {
		assert.Equal(t, uint64(0), effectiveBytes(usageDoc{}, members, now))
	})
}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		FileUsage:    New(),
		nodeConf:     mock_nodeconf.NewMockService(ctrl),
		accountLimit: mock_accountlimit.NewMockAccountLimit(ctrl),
		spaceStatus:  mock_spacestatus.NewMockSpaceStatus(ctrl),
		a:            new(app.App),
		ctrl:         ctrl,
	}

	fx.nodeConf.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.nodeConf.EXPECT().Name().Return(nodeconf.CName).AnyTimes()
	fx.nodeConf.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.nodeConf.EXPECT().Close(gomock.Any()).AnyTimes()

	fx.accountLimit.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.accountLimit.EXPECT().Name().Return(accountlimit.CName).AnyTimes()

	fx.spaceStatus.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.spaceStatus.EXPECT().Name().Return(spacestatus.CName).AnyTimes()
	fx.spaceStatus.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.spaceStatus.EXPECT().Close(gomock.Any()).AnyTimes()
	fx.spaceStatus.EXPECT().RegisterSpaceRemoveHook(gomock.Any()).
		Do(func(fn func(mongo.SessionContext, string) error) {
			fx.removeHook = fn
		})

	fx.a.Register(db.New()).
		Register(fx.FileUsage).
		Register(fx.nodeConf).
		Register(fx.accountLimit).
		Register(fx.spaceStatus).
		Register(&testConfig{})

	require.NoError(t, fx.a.Start(ctx))
	fx.db = fx.a.MustComponent(db.CName).(db.Database)
	_ = fx.db.Db().Collection(collName).Drop(ctx)
	return fx
}

type fixture struct {
	FileUsage
	a            *app.App
	ctrl         *gomock.Controller
	db           db.Database
	nodeConf     *mock_nodeconf.MockService
	accountLimit *mock_accountlimit.MockAccountLimit
	spaceStatus  *mock_spacestatus.MockSpaceStatus
	removeHook   func(txCtx mongo.SessionContext, spaceId string) error
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
}

type testConfig struct{}

func (c *testConfig) Init(_ *app.App) error { return nil }
func (c *testConfig) Name() string          { return "config" }

func (c *testConfig) GetMongo() db.Mongo {
	return db.Mongo{
		Connect:  "mongodb://localhost:27017",
		Database: "coordinator_unittest",
	}
}
