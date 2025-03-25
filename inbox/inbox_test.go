package inbox

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"storj.io/drpc"
	"storj.io/drpc/drpcerr"
)

var ctx = context.Background()

func TestInbox(t *testing.T) {
	var makeClientServer = func(t *testing.T) (fxC, fxS *fixture, peerId string) {
		fxC = newFixture(t)
		fxS = newFixture(t)
		peerId = "peer"
		mcS, mcC := rpctest.MultiConnPair(peerId, peerId+"client")
		pS, err := peer.NewPeer(mcS, fxC.ts)
		require.NoError(t, err)
		fxC.tp.AddPeer(ctx, pS)
		_, err = peer.NewPeer(mcC, fxS.ts)
		require.NoError(t, err)
		return
	}

	t.Run("sync", func(t *testing.T) {

		fxC, fxS, peerId := makeClientServer(t)
		defer fxC.Finish(t)
		defer fxS.Finish(t)
		peer, err := fxC.tp.GetOneOf(ctx, []string{peerId})
		require.NoError(t, err)
		peer.DoDrpc(ctx, func(conn drpc.Conn) error {
			client := coordinatorproto.NewDRPCCoordinatorClient(conn)
			resp, err := client.InboxFetch(ctx, &coordinatorproto.InboxFetchRequest{})
			require.NoError(t, err)
			fmt.Printf("%#v\n", resp)
			return nil
		})

		// writeFiles(t, fxS.store.StoreDir(spaceId), testFiles...)
		// fxC.store.EXPECT().
		// 	TryLockAndDo(gomock.Any(), gomock.Any()).AnyTimes().
		// 	DoAndReturn(func(spaceId string, do func() error) (err error) {
		// 		return do()
		// 	})
		// fxS.store.EXPECT().
		// 	TryLockAndDo(gomock.Any(), gomock.Any()).AnyTimes().
		// 	DoAndReturn(func(spaceId string, do func() error) (err error) {
		// 		return do()
		// 	})
		// fxC.store.EXPECT().SpaceExists(spaceId).Return(false)

		// require.NoError(t, fxC.Sync(ctx, "spaceId", peerId))

		// for _, tf := range testFiles {
		// 	cBytes, err := os.ReadFile(filepath.Join(fxC.store.StoreDir(spaceId), tf.name))
		// 	require.NoError(t, err)
		// 	sBytes, err := os.ReadFile(filepath.Join(fxS.store.StoreDir(spaceId), tf.name))
		// 	require.NoError(t, err)
		// 	assert.True(t, bytes.Equal(cBytes, sBytes))
		// }
	})

}

type config struct {
}

func (c config) Init(a *app.App) (err error) { return }
func (c config) Name() string                { return "config" }

func (c config) GetMongo() db.Mongo {
	return db.Mongo{
		Connect:  "mongodb://localhost:27017",
		Database: "coordinator_unittest",
	}
}

func newFixture(t *testing.T) (fx *fixture) {
	ts := rpctest.NewTestServer()
	fx = &fixture{
		InboxService: New(),
		db:           db.New(),
		ctrl:         gomock.NewController(t),
		a:            new(app.App),
		ts:           ts,
		tp:           rpctest.NewTestPool(),
	}

	// anymock.ExpectComp(fx.space.EXPECT(), nodespace.CName)
	fx.a.
		Register(config{}).
		Register(fx.db).
		Register(fx.InboxService).
		Register(fx.tp).
		Register(fx.ts)

	require.NoError(t, coordinatorproto.DRPCRegisterCoordinator(ts, &testServer{inbox: fx.InboxService}))
	require.NoError(t, fx.a.Start(ctx))

	return fx
}

type fixture struct {
	InboxService
	a    *app.App
	db   db.Database
	ctrl *gomock.Controller
	ts   *rpctest.TestServer
	tp   *rpctest.TestPool
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
}

type testServer struct {
	coordinatorproto.DRPCCoordinatorUnimplementedServer
	inbox InboxService
}

func (t *testServer) InboxFetch(context.Context, *coordinatorproto.InboxFetchRequest) (*coordinatorproto.InboxFetchResponse, error) {
	return nil, drpcerr.WithCode(errors.New("Unimplemented 1"), drpcerr.Unimplemented)
}

func (t *testServer) InboxNotifySubscribe(*coordinatorproto.InboxNotifySubscribeRequest, coordinatorproto.DRPCCoordinator_InboxNotifySubscribeStream) error {
	return drpcerr.WithCode(errors.New("Unimplemented 1"), drpcerr.Unimplemented)
}
