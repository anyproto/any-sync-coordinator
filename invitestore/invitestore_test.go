package invitestore

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-coordinator/db"
)

var ctx = context.Background()

func entry(cid, spaceId string, key byte) Entry {
	return Entry{Cid: cid, SpaceId: spaceId, InvitePubKey: []byte{key}}
}

func TestInviteStore_Bind(t *testing.T) {
	t.Run("bind and get", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		require.NoError(t, fx.Bind(ctx, entry("cid1", "space1", 1)))

		got, err := fx.Get(ctx, "cid1")
		require.NoError(t, err)
		assert.Equal(t, entry("cid1", "space1", 1), got)
	})

	t.Run("rebinding the same invite is a no-op", func(t *testing.T) {
		// an upload retry must not fail
		fx := newFixture(t)
		defer fx.finish(t)
		require.NoError(t, fx.Bind(ctx, entry("cid1", "space1", 1)))
		require.NoError(t, fx.Bind(ctx, entry("cid1", "space1", 1)))
	})

	t.Run("rebinding to another space is refused", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		require.NoError(t, fx.Bind(ctx, entry("cid1", "space1", 1)))
		require.ErrorIs(t, fx.Bind(ctx, entry("cid1", "space2", 1)), ErrCidBound)
	})

	t.Run("rebinding to another invite key is refused", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		require.NoError(t, fx.Bind(ctx, entry("cid1", "space1", 1)))
		require.ErrorIs(t, fx.Bind(ctx, entry("cid1", "space1", 2)), ErrCidBound)
	})
}

func TestInviteStore_Get(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)
	_, err := fx.Get(ctx, "nope")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestInviteStore_Delete(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)
	require.NoError(t, fx.Bind(ctx, entry("cid1", "space1", 1)))
	require.NoError(t, fx.Delete(ctx, "cid1"))

	_, err := fx.Get(ctx, "cid1")
	require.ErrorIs(t, err, ErrNotFound)

	// deleting what is not there is fine: an upload rollback and a delete can race
	require.NoError(t, fx.Delete(ctx, "cid1"))
}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		InviteStore: New(),
		db:          db.New(),
		a:           new(app.App),
	}
	fx.a.Register(config{}).Register(fx.db).Register(fx.InviteStore)
	require.NoError(t, fx.a.Start(ctx))
	_ = fx.db.Db().Collection(collName).Drop(ctx)
	time.Sleep(time.Second / 2)
	return fx
}

type fixture struct {
	InviteStore
	a  *app.App
	db db.Database
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
}

type config struct{}

func (c config) Init(a *app.App) (err error) { return }
func (c config) Name() string                { return "config" }

func (c config) GetMongo() db.Mongo {
	return db.Mongo{
		Connect:  "mongodb://localhost:27017",
		Database: "coordinator_unittest_invitestore",
	}
}
