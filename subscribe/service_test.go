package subscribe

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
)

var ctx = context.Background()

func TestSubscribe_AddStream(t *testing.T) {
	t.Run("add same peer returns PeerAlreadySubscribed error", func(t *testing.T) {
		fxC := newFixture(t)
		defer fxC.Finish(t)
		dummyStream := NewDummyStream()

		err := fxC.AddStream(coordinatorproto.NotifyEventType_NetworkConfigChangedEvent, "accountId", "peerId", dummyStream)
		require.NoError(t, err)

		err = fxC.AddStream(coordinatorproto.NotifyEventType_NetworkConfigChangedEvent, "accountId", "peerId", dummyStream)
		assert.ErrorIs(t, err, coordinatorproto.ErrSubscribePeerAlreadySubscribed)

		err = fxC.AddStream(coordinatorproto.NotifyEventType_NetworkConfigChangedEvent, "accountId", "peerId2", dummyStream)
		require.NoError(t, err)

	})
}

func newFixture(t *testing.T) (fx *fixture) {
	fx = &fixture{
		SubscribeService: New(),
		a:                new(app.App),
	}

	fx.a.
		Register(fx.SubscribeService)

	require.NoError(t, fx.a.Start(ctx))

	return fx
}

type dummyDrpcStream struct{}

func NewDummyStream() coordinatorproto.DRPCCoordinator_NotifySubscribeStream {
	return &dummyDrpcStream{}
}
func (*dummyDrpcStream) Context() context.Context {
	return context.TODO()
}

func (*dummyDrpcStream) MsgSend(msg drpc.Message, enc drpc.Encoding) error {
	return nil
}

func (*dummyDrpcStream) MsgRecv(msg drpc.Message, enc drpc.Encoding) error {
	return nil
}

func (*dummyDrpcStream) CloseSend() error {
	return nil
}

func (*dummyDrpcStream) Close() error {
	return nil
}

func (*dummyDrpcStream) Send(*coordinatorproto.NotifySubscribeEvent) error {
	return nil
}

type fixture struct {
	SubscribeService
	a *app.App
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
}
