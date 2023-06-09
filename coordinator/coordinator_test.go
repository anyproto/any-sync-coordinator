package coordinator

import (
	"context"
	"github.com/anyproto/any-sync-coordinator/config"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/filelimit"
	"github.com/anyproto/any-sync-coordinator/nodeservice"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"testing"
)

var ctx = context.Background()

func TestCoordinator_FileLimitCheck(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	ts := rpctest.NewTestServer()
	fx := &fixture{
		coordinator: New().(*coordinator),
		a:           new(app.App),
		nodeConf:    mock_nodeconf.NewMockService(ctrl),
		account:     &accounttest.AccountTestService{},
		ctrl:        ctrl,
		db:          db.New(),
		pool:        rpctest.NewTestPool(),
	}
	fx.nodeConf.EXPECT().Name().Return(nodeconf.CName).AnyTimes()
	fx.nodeConf.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.nodeConf.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.nodeConf.EXPECT().Close(gomock.Any()).AnyTimes()

	conf := &config.Config{
		Mongo: db.Mongo{
			Connect:  "mongodb://localhost:27017",
			Database: "coordinator_unittest",
		},
	}

	fx.a.Register(conf).
		Register(fx.db).
		Register(fx.pool).
		Register(fx.nodeConf).
		Register(fx.account).
		Register(spacestatus.New()).
		Register(nodeservice.New()).
		Register(coordinatorlog.New()).
		Register(metric.New()).
		Register(filelimit.New()).
		Register(ts).
		Register(fx.coordinator)
	require.NoError(t, fx.a.Start(ctx))
	_ = fx.db.Db().Drop(ctx)
	return fx
}

type fixture struct {
	*coordinator
	a        *app.App
	nodeConf *mock_nodeconf.MockService
	account  *accounttest.AccountTestService
	ctrl     *gomock.Controller
	db       db.Database
	pool     *rpctest.TestPool
}

func (fx *fixture) finish(t *testing.T) {
	fx.ctrl.Finish()
	fx.a.Close(ctx)
}
