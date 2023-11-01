package filelimit

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync-coordinator/spacestatus/mock_spacestatus"
)

var ctx = context.Background()

func TestFileLimit_Get(t *testing.T) {
	spaceId := "s1"
	identity, identityPub := newTestIdentity()
	ss := spacestatus.StatusEntry{
		SpaceId:     spaceId,
		Identity:    identityPub.Account(),
		OldIdentity: "old",
		Status:      spacestatus.SpaceStatusCreated,
	}
	t.Run("default user", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		fx.spaceStatus.EXPECT().Status(ctx, spaceId, gomock.Any()).Return(ss, nil)

		limit, storeKey, err := fx.Get(ctx, identity, spaceId)
		require.NoError(t, err)
		assert.Equal(t, ss.Identity, storeKey)
		assert.Equal(t, uint64(100), limit)
	})

	t.Run("custom limit", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		require.NoError(t, fx.FileLimit.(*fileLimit).db.Set(ctx, identityPub.Account(), 123))

		fx.spaceStatus.EXPECT().Status(ctx, spaceId, gomock.Any()).Return(ss, nil)
		limit, storeKey, err := fx.Get(ctx, identity, spaceId)
		require.NoError(t, err)
		assert.Equal(t, ss.Identity, storeKey)
		assert.Equal(t, uint64(123), limit)
	})
	t.Run("empty spaceId", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		require.NoError(t, fx.FileLimit.(*fileLimit).db.Set(ctx, identityPub.Account(), 123))

		limit, storeKey, err := fx.Get(ctx, identity, "")
		require.NoError(t, err)
		assert.Equal(t, ss.Identity, storeKey)
		assert.Equal(t, uint64(123), limit)
	})
}

func newTestIdentity() ([]byte, crypto.PubKey) {
	_, pub, _ := crypto.GenerateRandomEd25519KeyPair()
	data, _ := pub.Marshall()
	return data, pub
}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		FileLimit:   New(),
		ctrl:        ctrl,
		a:           new(app.App),
		spaceStatus: mock_spacestatus.NewMockSpaceStatus(ctrl),
	}

	fx.spaceStatus.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.spaceStatus.EXPECT().Name().Return(spacestatus.CName).AnyTimes()
	fx.spaceStatus.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.spaceStatus.EXPECT().Close(gomock.Any()).AnyTimes()

	fx.a.Register(db.New()).
		Register(fx.FileLimit).
		Register(fx.spaceStatus).
		Register(&testConfig{})

	require.NoError(t, fx.a.Start(ctx))
	_ = fx.a.MustComponent(db.CName).(db.Database).Db().Collection(collName).Drop(ctx)
	return fx
}

type fixture struct {
	FileLimit
	a           *app.App
	ctrl        *gomock.Controller
	spaceStatus *mock_spacestatus.MockSpaceStatus
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
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

func (c *testConfig) GetFileLimit() Config {
	return Config{
		LimitDefault:      100,
		LimitAlphaUsers:   1000,
		LimitNightlyUsers: 10000,
	}
}
