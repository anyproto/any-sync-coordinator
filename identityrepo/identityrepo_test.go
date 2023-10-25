package identityrepo

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/identityrepo/identityrepoproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-coordinator/db"
)

var ctx = context.Background()

func newIdentityCtx(ctx context.Context) (rctx context.Context, identity string, sk crypto.PrivKey) {
	as := &accounttest.AccountTestService{}
	_ = as.Init(nil)
	raw, _ := as.Account().SignKey.GetPublic().Marshall()
	rctx = peer.CtxWithIdentity(ctx, raw)
	identity = as.Account().SignKey.GetPublic().Account()
	sk = as.Account().SignKey
	return
}

func newRandData(kind string, sk crypto.PrivKey, l int) *identityrepoproto.Data {
	data := make([]byte, l)
	_, _ = rand.Read(data)
	sign, _ := sk.Sign(data)
	return &identityrepoproto.Data{
		Kind:      kind,
		Data:      data,
		Signature: sign,
	}
}

func TestIdentityRepo_Push(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		rctx, idn, sk := newIdentityCtx(ctx)
		res, err := fx.handler.DataPut(rctx, &identityrepoproto.DataPutRequest{
			Identity: idn,
			Data: []*identityrepoproto.Data{
				newRandData("one", sk, 100),
				newRandData("two", sk, 100),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, okResp, res)
	})
	t.Run("invalid identity", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		rctx, idn, sk := newIdentityCtx(ctx)
		_, err := fx.handler.DataPut(rctx, &identityrepoproto.DataPutRequest{
			Identity: idn + "1",
			Data: []*identityrepoproto.Data{
				newRandData("one", sk, 100),
			},
		})
		assert.EqualError(t, err, ErrInvalidIdentity.Error())
	})
	t.Run("missing identity", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		_, idn, sk := newIdentityCtx(ctx)
		_, err := fx.handler.DataPut(ctx, &identityrepoproto.DataPutRequest{
			Identity: idn,
			Data: []*identityrepoproto.Data{
				newRandData("one", sk, 100),
			},
		})
		assert.EqualError(t, err, peer.ErrIdentityNotFoundInContext.Error())
	})
	t.Run("invalid signature", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		rctx, idn, sk := newIdentityCtx(ctx)
		data := newRandData("one", sk, 100)
		data.Data = append(data.Data, []byte("invalid")...)
		_, err := fx.handler.DataPut(rctx, &identityrepoproto.DataPutRequest{
			Identity: idn,
			Data: []*identityrepoproto.Data{
				data,
			},
		})
		assert.EqualError(t, err, ErrInvalidSignature.Error())
	})
	t.Run("threshold reached", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		rctx, idn, sk := newIdentityCtx(ctx)
		data := newRandData("one", sk, thresholdDataLen+1)
		_, err := fx.handler.DataPut(rctx, &identityrepoproto.DataPutRequest{
			Identity: idn,
			Data: []*identityrepoproto.Data{
				data,
			},
		})
		assert.EqualError(t, err, ErrThresholdReached.Error())
	})
}

func TestIdentityRepo_Delete(t *testing.T) {
	t.Run("missing identity", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		_, idn, _ := newIdentityCtx(ctx)
		_, err := fx.handler.DataDelete(ctx, &identityrepoproto.DataDeleteRequest{
			Identity: idn,
			Kinds:    []string{"one"},
		})
		assert.EqualError(t, err, peer.ErrIdentityNotFoundInContext.Error())
	})
	t.Run("partial", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		rctx, idn, sk := newIdentityCtx(ctx)
		_, err := fx.handler.DataPut(rctx, &identityrepoproto.DataPutRequest{
			Identity: idn,
			Data: []*identityrepoproto.Data{
				newRandData("one", sk, 100),
				newRandData("two", sk, 100),
			},
		})
		require.NoError(t, err)

		res, err := fx.handler.DataDelete(rctx, &identityrepoproto.DataDeleteRequest{
			Identity: idn,
			Kinds:    []string{"one"},
		})
		require.NoError(t, err)
		assert.Equal(t, okResp, res)

		e, err := fx.fetchOne(ctx, idn)
		require.NoError(t, err)
		assert.Len(t, e.Data, 1)
	})
	t.Run("full", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		rctx, idn, sk := newIdentityCtx(ctx)
		_, err := fx.handler.DataPut(rctx, &identityrepoproto.DataPutRequest{
			Identity: idn,
			Data: []*identityrepoproto.Data{
				newRandData("one", sk, 100),
				newRandData("two", sk, 100),
			},
		})
		require.NoError(t, err)

		res, err := fx.handler.DataDelete(rctx, &identityrepoproto.DataDeleteRequest{
			Identity: idn,
		})
		require.NoError(t, err)
		assert.Equal(t, okResp, res)

		e, err := fx.fetchOne(ctx, idn)
		require.NoError(t, err)
		assert.Len(t, e.Data, 0)
	})
}

func TestIdentityRepo_Pull(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	var identities []string

	for i := 0; i < 5; i++ {
		rctx, idn, sk := newIdentityCtx(ctx)
		_, err := fx.handler.DataPut(rctx, &identityrepoproto.DataPutRequest{
			Identity: idn,
			Data: []*identityrepoproto.Data{
				newRandData("one", sk, 100),
				newRandData("two", sk, 100),
			},
		})
		require.NoError(t, err)
		identities = append(identities, idn)
	}

	t.Run("all", func(t *testing.T) {
		resp, err := fx.handler.DataPull(ctx, &identityrepoproto.DataPullRequest{
			Identities: identities,
			Kinds:      []string{"one", "two"},
		})
		require.NoError(t, err)
		assert.Len(t, resp.Data, len(identities))
		for _, dt := range resp.Data {
			assert.Len(t, dt.Data, 2)
		}
	})
	t.Run("partial", func(t *testing.T) {
		resp, err := fx.handler.DataPull(ctx, &identityrepoproto.DataPullRequest{
			Identities: identities[1:],
			Kinds:      []string{"two"},
		})
		require.NoError(t, err)
		assert.Len(t, resp.Data, len(identities)-1)
		for _, dt := range resp.Data {
			assert.Len(t, dt.Data, 1)
		}
	})
	t.Run("threshold reached", func(t *testing.T) {
		_, err := fx.handler.DataPull(ctx, &identityrepoproto.DataPullRequest{
			Identities: make([]string, thresholdIdentityPerReq+1),
			Kinds:      []string{"two"},
		})
		assert.EqualError(t, err, ErrThresholdReached.Error())
	})

}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		identityRepo: New().(*identityRepo),
		db:           db.New(),
		ts:           rpctest.NewTestServer(),
		a:            new(app.App),
	}
	fx.a.Register(config{}).Register(fx.db).Register(fx.identityRepo).Register(fx.ts)
	require.NoError(t, fx.a.Start(ctx))
	_ = fx.db.Db().Collection(collName).Drop(ctx)
	time.Sleep(time.Second / 2)
	return fx
}

type fixture struct {
	*identityRepo
	a  *app.App
	db db.Database
	ts *rpctest.TestServer
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
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
