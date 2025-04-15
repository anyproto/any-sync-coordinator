package inbox

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/subscribe"
	"github.com/anyproto/any-sync-coordinator/subscribe/mock_subscribe"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/mock/gomock"
)

var ctx = context.Background()

func newIdentityCtx() (rctx context.Context, pk crypto.PubKey, sk crypto.PrivKey) {
	as := &accounttest.AccountTestService{}
	_ = as.Init(nil)
	raw, _ := as.Account().SignKey.GetPublic().Marshall()
	rctx = peer.CtxWithIdentity(ctx, raw)
	pk = as.Account().SignKey.GetPublic()
	sk = as.Account().SignKey
	return
}

func makeMessage(pubkeyTo crypto.PubKey, privkeyFrom crypto.PrivKey) (msg *InboxMessage, err error) {
	body := []byte("hello")
	encrypted, err := pubkeyTo.Encrypt(body)
	if err != nil {
		return
	}

	signature, err := privkeyFrom.Sign(encrypted)
	if err != nil {
		return
	}

	msg = &InboxMessage{
		PacketType: InboxPacketTypeDefault,
		Packet: InboxPacket{
			KeyType:          InboxKeyTypeEd25519,
			SenderSignature:  signature,
			SenderIdentity:   privkeyFrom.GetPublic().Account(),
			ReceiverIdentity: pubkeyTo.Account(),
			Payload: InboxPayload{
				PayloadType: InboxPayloadTypeSpaceInvite,
				Body:        encrypted,
			},
		},
	}

	return

}

func dropColl(t *testing.T, fxC *fixture) {
	err := fxC.db.Db().Collection(collName).Drop(ctx)
	require.NoError(t, err)
}

func TestInbox_StrToObjId(t *testing.T) {
	t.Run("empty offset", func(t *testing.T) {
		_, err := strToObjId("")
		t.Log(err)
		assert.Error(t, err)
	})

}

func TestInbox_AddMessage(t *testing.T) {
	fxC := newFixture(t)
	defer fxC.Finish(t)
	ctx, pk, sk := newIdentityCtx()
	msg, err := makeMessage(pk, sk)
	require.NoError(t, err)

	t.Run("message verify success", func(t *testing.T) {
		dropColl(t, fxC)

		err = fxC.InboxAddMessage(ctx, msg)
		require.NoError(t, err)
	})

	t.Run("message verify signature verify fail", func(t *testing.T) {
		dropColl(t, fxC)

		msgFail, _ := makeMessage(pk, sk)
		pk, _, _ := crypto.GenerateRandomEd25519KeyPair()
		signature, _ := pk.Sign(msgFail.Packet.Payload.Body)
		msgFail.Packet.SenderSignature = signature
		err = fxC.InboxAddMessage(ctx, msgFail)
		assert.ErrorIs(t, err, coordinatorproto.ErrInboxMessageVerifyFailed)
	})

	t.Run("add messages and fetch them", func(t *testing.T) {
		dropColl(t, fxC)

		fxC2 := newFixture(t)
		defer fxC2.Finish(t)

		ctx2, pk2, sk2 := newIdentityCtx()
		msg, _ := makeMessage(pk2, sk)

		fxC2.subscribe.EXPECT().NotifyAllPeers(gomock.Any(), gomock.Any(), gomock.Any()).Times(10)

		for range 10 {
			err = fxC.InboxAddMessage(ctx, msg)
			require.NoError(t, err)
		}

		msgs, err := fxC.InboxFetch(ctx2, "")
		require.NoError(t, err)
		assert.Len(t, msgs.Messages, 10)

		for _, msg := range msgs.Messages {
			decrypted, err := sk2.Decrypt(msg.Packet.Payload.Body)
			require.NoError(t, err)
			assert.Equal(t, "hello", string(decrypted))
		}

	})

	t.Run("fetch with offset", func(t *testing.T) {
		dropColl(t, fxC)

		fxC2 := newFixture(t)
		defer fxC2.Finish(t)

		fxC2.subscribe.EXPECT().NotifyAllPeers(gomock.Any(), gomock.Any(), gomock.Any()).Times(15)

		ctx2, pk2, _ := newIdentityCtx()
		msg, _ := makeMessage(pk2, sk)
		for range 10 {
			err = fxC.InboxAddMessage(ctx, msg)
			require.NoError(t, err)
		}

		msgs, err := fxC.InboxFetch(ctx2, "")
		require.NoError(t, err)
		assert.Len(t, msgs.Messages, 10)

		offset := msgs.Messages[len(msgs.Messages)-1].Id

		for range 5 {
			err = fxC.InboxAddMessage(ctx, msg)
			require.NoError(t, err)
		}

		msgs, err = fxC.InboxFetch(ctx2, offset.Hex())
		require.NoError(t, err)
		assert.Len(t, msgs.Messages, 5)

	})

	t.Run("fetch returns hasMore", func(t *testing.T) {
		dropColl(t, fxC)

		fxC2 := newFixture(t)
		defer fxC2.Finish(t)

		ctx2, pk2, _ := newIdentityCtx()
		msg, _ := makeMessage(pk2, sk)

		preseed := make([]any, fetchLimit+5)
		msg.Packet.Payload.Timestamp = time.Now().Unix()

		// this test runs too long via AddMessage: pre-seed db
		for i := range preseed {
			m := *msg
			m.Id = primitive.NewObjectID()
			t := time.Now().Add(time.Duration(i) * time.Second)
			m.Packet.Payload.Timestamp = t.Unix()
			preseed[i] = m
		}

		fxC.db.Db().Collection(collName).InsertMany(ctx, preseed)

		msgs, err := fxC.InboxFetch(ctx2, "")
		require.NoError(t, err)
		assert.Len(t, msgs.Messages, fetchLimit)
		assert.True(t, msgs.HasMore)

		offset := msgs.Messages[len(msgs.Messages)-1].Id
		msgs, err = fxC.InboxFetch(ctx2, offset.Hex())
		require.NoError(t, err)
		assert.False(t, msgs.HasMore)
		assert.Len(t, msgs.Messages, 5)

	})

}

type config struct {
}

func (c config) Init(a *app.App) (err error) { return }
func (c config) Name() string                { return "config" }

func (c config) GetMongo() db.Mongo {
	return db.Mongo{
		Connect:  "mongodb://localhost:27017",
		Database: "coordinator_unittest_inbox",
	}
}

func newFixture(t *testing.T) (fx *fixture) {
	ctrl := gomock.NewController(t)
	fx = &fixture{
		InboxService: New(),
		subscribe:    mock_subscribe.NewMockSubscribeService(ctrl),
		db:           db.New(),
		ctrl:         ctrl,
		a:            new(app.App),
	}

	anymock.ExpectComp(fx.subscribe.EXPECT(), subscribe.CName)

	fx.a.
		Register(config{}).
		Register(fx.db).
		Register(fx.subscribe).
		Register(fx.InboxService)

	require.NoError(t, fx.a.Start(ctx))

	return fx
}

type fixture struct {
	InboxService
	subscribe *mock_subscribe.MockSubscribeService
	a         *app.App
	db        db.Database
	ctrl      *gomock.Controller
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
}
