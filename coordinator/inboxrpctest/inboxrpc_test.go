package inboxrpctest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/accountlimit/mock_accountlimit"
	"github.com/anyproto/any-sync-coordinator/acleventlog"
	"github.com/anyproto/any-sync-coordinator/acleventlog/mock_acleventlog"
	"github.com/anyproto/any-sync-coordinator/config"
	"github.com/anyproto/any-sync-coordinator/coordinator"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog/mock_coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/deletionlog"
	"github.com/anyproto/any-sync-coordinator/deletionlog/mock_deletionlog"
	"github.com/anyproto/any-sync-coordinator/inbox"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync-coordinator/spacestatus/mock_spacestatus"
	"github.com/anyproto/any-sync/acl"
	"github.com/anyproto/any-sync/acl/mock_acl"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/coordinator/inboxclient"
	inboxclientmocks "github.com/anyproto/any-sync/coordinator/inboxclient/mocks"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/mock/gomock"
)

type fixtureServer struct {
	inbox       inbox.InboxService
	coordinator coordinator.Coordinator
	account     *accounttest.AccountTestService
	a           *app.App
	db          db.Database
	ctrl        *gomock.Controller
	ts          *rpctest.TestServer
	tp          *rpctest.TestPool

	spaceStatus  *mock_spacestatus.MockSpaceStatus
	coordLog     *mock_coordinatorlog.MockCoordinatorLog
	aclEventLog  *mock_acleventlog.MockAclEventLog
	deletionLog  *mock_deletionlog.MockDeletionLog
	acl          *mock_acl.MockAclService
	accountLimit *mock_accountlimit.MockAccountLimit
}

type fixtureClient struct {
	inboxclient  inboxclient.InboxClient
	mockReceiver *inboxclientmocks.MockMessageReceiverTest
	account      *accounttest.AccountTestService
	a            *app.App
	ctrl         *gomock.Controller
	ts           *rpctest.TestServer
	tp           *rpctest.TestPool
}

var ctx = context.Background()

func newFixtureServer(t *testing.T, nodeConf *mockNodeConf) (fxS *fixtureServer) {
	account := &accounttest.AccountTestService{}
	config := &config.Config{
		SpaceStatus: spacestatus.Config{
			DeletionPeriodDays: 42,
		},
		Mongo: db.Mongo{
			Connect:  "mongodb://localhost:27017",
			Database: "coordinator_unittest",
		},
	}
	ctrl := gomock.NewController(t)
	fxS = &fixtureServer{
		inbox:   inbox.New(),
		account: account,
		db:      db.New(),
		ctrl:    ctrl,
		ts:      rpctest.NewTestServer(),
		tp:      rpctest.NewTestPool(),

		a: new(app.App),

		coordinator:  coordinator.New(),
		spaceStatus:  mock_spacestatus.NewMockSpaceStatus(ctrl),
		coordLog:     mock_coordinatorlog.NewMockCoordinatorLog(ctrl),
		aclEventLog:  mock_acleventlog.NewMockAclEventLog(ctrl),
		deletionLog:  mock_deletionlog.NewMockDeletionLog(ctrl),
		acl:          mock_acl.NewMockAclService(ctrl),
		accountLimit: mock_accountlimit.NewMockAccountLimit(ctrl),
	}

	anymock.ExpectComp(fxS.spaceStatus.EXPECT(), spacestatus.CName)
	anymock.ExpectComp(fxS.coordLog.EXPECT(), coordinatorlog.CName)
	anymock.ExpectComp(fxS.deletionLog.EXPECT(), deletionlog.CName)
	anymock.ExpectComp(fxS.acl.EXPECT(), acl.CName)
	anymock.ExpectComp(fxS.accountLimit.EXPECT(), accountlimit.CName)
	anymock.ExpectComp(fxS.aclEventLog.EXPECT(), acleventlog.CName)

	fxS.a.
		Register(account).
		Register(nodeConf).
		Register(config).
		Register(fxS.db).
		Register(fxS.ts).
		Register(fxS.tp).
		Register(fxS.coordinator).
		Register(fxS.spaceStatus).
		Register(fxS.coordLog).
		Register(fxS.aclEventLog).
		Register(metric.New()).
		Register(fxS.deletionLog).
		Register(fxS.acl).
		Register(fxS.accountLimit).
		Register(fxS.inbox)

	require.NoError(t, fxS.a.Start(ctx))

	return fxS
}

func (fxS *fixtureServer) Finish(t *testing.T) {
	require.NoError(t, fxS.a.Close(ctx))
	fxS.ctrl.Finish()
}

func newFixtureClient(t *testing.T, nodeConf *mockNodeConf) (fxC *fixtureClient) {
	account := &accounttest.AccountTestService{}
	ic := inboxclient.New()

	ctrl := gomock.NewController(t)
	fxC = &fixtureClient{
		inboxclient:  ic,
		mockReceiver: inboxclientmocks.NewMockMessageReceiverTest(ctrl),
		account:      account,
		ctrl:         ctrl,
		ts:           rpctest.NewTestServer(),
		tp:           rpctest.NewTestPool(),
		a:            new(app.App),
	}

	fxC.inboxclient.SetMessageReceiver(fxC.mockReceiver.Receive)
	fxC.a.
		Register(account).
		Register(nodeConf).
		Register(fxC.ts).
		Register(fxC.tp).
		Register(fxC.inboxclient)

	require.NoError(t, fxC.a.Start(ctx))

	return fxC
}

func (fxC *fixtureClient) Finish(t *testing.T) {
	require.NoError(t, fxC.a.Close(ctx))
	fxC.ctrl.Finish()
}

func makeClientServer(t *testing.T) (fxC *fixtureClient, fxS *fixtureServer, peerId string) {
	nodeConf := &mockNodeConf{}
	fxC = newFixtureClient(t, nodeConf)
	fxS = newFixtureServer(t, nodeConf)
	peerId = "peer"
	identityS, err := fxS.account.Account().SignKey.GetPublic().Marshall()
	require.NoError(t, err)
	identityC, err := fxC.account.Account().SignKey.GetPublic().Marshall()
	require.NoError(t, err)

	mcS, mcC := rpctest.MultiConnPairWithClientServerIdentity(peerId, peerId+"client", identityS, identityC)
	pS, err := peer.NewPeer(mcS, fxC.ts)
	require.NoError(t, err)
	fxC.tp.AddPeer(ctx, pS)
	_, err = peer.NewPeer(mcC, fxS.ts)
	require.NoError(t, err)
	return
}

func makeMessage(pubkeyTo crypto.PubKey, privkeyFrom crypto.PrivKey) (msg *coordinatorproto.InboxMessage, err error) {
	body := []byte("hello")
	encrypted, err := pubkeyTo.Encrypt(body)
	if err != nil {
		return
	}

	signature, err := privkeyFrom.Sign(encrypted)
	if err != nil {
		return
	}

	msgInbox := &inbox.InboxMessage{
		PacketType: inbox.InboxPacketTypeDefault,
		Packet: inbox.InboxPacket{
			KeyType:          inbox.InboxKeyTypeEd25519,
			SenderSignature:  signature,
			SenderIdentity:   privkeyFrom.GetPublic().Account(),
			ReceiverIdentity: pubkeyTo.Account(),
			Payload: inbox.InboxPayload{
				PayloadType: inbox.InboxPayloadTypeSpaceInvite,
				Body:        encrypted,
			},
		},
	}
	msg = inbox.InboxMessageToResponse(msgInbox)
	return

}

func clearColl(t *testing.T, fxS *fixtureServer) {
	_, err := fxS.db.Db().Collection("inboxMessages").DeleteMany(ctx, bson.M{})
	require.NoError(t, err)
}

func TestInbox_Notifications(t *testing.T) {
	fxC, fxS, _ := makeClientServer(t)
	t.Run("InboxAddMessage creates a stream notification", func(t *testing.T) {
		clearColl(t, fxS)

		msg, _ := makeMessage(fxC.account.Account().SignKey.GetPublic(), fxC.account.Account().SignKey)
		pubKeyC := fxC.account.Account().SignKey.GetPublic()
		raw, _ := pubKeyC.Marshall()
		ictx := peer.CtxWithIdentity(ctx, raw)
		var wg sync.WaitGroup
		amount := 9
		wg.Add(amount)
		fxC.mockReceiver.EXPECT().
			Receive(gomock.Any()).
			Do(func(evt *coordinatorproto.InboxNotifySubscribeEvent) {
				defer wg.Done()
			}).
			Times(amount)

		// TODO: create something like stream.ready to avoid sleep?
		time.Sleep(2 * time.Second)
		msgs, err := fxC.inboxclient.InboxFetch(ictx, "")
		require.NoError(t, err)
		assert.Len(t, msgs, 0)

		for range amount {
			err = fxC.inboxclient.InboxAddMessage(ictx, pubKeyC, msg)
			require.NoError(t, err)
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// after notification, fetch once again
			msgs, err := fxC.inboxclient.InboxFetch(ictx, "")
			require.NoError(t, err)
			assert.Len(t, msgs, amount)

			return
		case <-time.After(3 * time.Second):
			t.Fatal("Receive callback was not triggered in time")
		}
	})

}
