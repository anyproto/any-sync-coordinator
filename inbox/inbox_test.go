package inbox

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var ctx = context.Background()

func newIdentityCtx() (rctx context.Context, identity string, sk crypto.PrivKey) {
	as := &accounttest.AccountTestService{}
	_ = as.Init(nil)
	raw, _ := as.Account().SignKey.GetPublic().Marshall()
	rctx = peer.CtxWithIdentity(ctx, raw)
	identity = as.Account().SignKey.GetPublic().Account()
	sk = as.Account().SignKey
	return
}

func defaultMessage(ctx context.Context, sk crypto.PrivKey) (msg *InboxMessage, err error) {
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	body := []byte("hello")
	encrypted, err := accountPubKey.Encrypt(body)
	if err != nil {
		return
	}

	signature, err := sk.Sign(encrypted)
	if err != nil {
		return
	}

	msg = &InboxMessage{
		PacketType: InboxPacketTypeDefault,
		Packet: InboxPacket{
			KeyType:          InboxKeyTypeEd25519,
			SenderSignature:  signature,
			SenderIdentity:   accountPubKey.Account(),
			ReceiverIdentity: accountPubKey.Account(),
			Payload: InboxPayload{
				PayloadType: InboxPayloadTypeSpaceInvite,
				Timestamp:   time.Now(),
				Body:        encrypted,
			},
		},
	}
	return
}

func TestInbox_AddMessageVerify(t *testing.T) {
	fxC := newFixture(t)
	defer fxC.Finish(t)
	ctx, _, sk := newIdentityCtx()
	msg, err := defaultMessage(ctx, sk)
	require.NoError(t, err)

	t.Run("message verify success", func(t *testing.T) {
		err = fxC.InboxAddMessage(ctx, msg)
		require.NoError(t, err)
	})

	t.Run("message verify signature verify fail", func(t *testing.T) {
		pk, _, _ := crypto.GenerateRandomEd25519KeyPair()
		signature, _ := pk.Sign(msg.Packet.Payload.Body)
		msg.Packet.SenderSignature = signature
		err = fxC.InboxAddMessage(ctx, msg)
		assert.ErrorIs(t, err, coordinatorproto.ErrInboxMessageVerifyFailed)
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
	fx = &fixture{
		InboxService: New(),
		db:           db.New(),
		ctrl:         gomock.NewController(t),
		a:            new(app.App),
	}

	fx.a.
		Register(config{}).
		Register(fx.db).
		Register(fx.InboxService)

	require.NoError(t, fx.a.Start(ctx))

	return fx
}

type fixture struct {
	InboxService
	a    *app.App
	db   db.Database
	ctrl *gomock.Controller
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
}
