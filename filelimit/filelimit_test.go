package filelimit

import (
	"context"
	"fmt"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

var ctx = context.Background()

func TestFileLimit_Set(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)
	require.NoError(t, fx.Set(ctx, "spaceId", 123))
	lim, err := fx.Get(ctx, "spaceId")
	require.NoError(t, err)
	assert.Equal(t, uint64(123), lim)
	require.NoError(t, fx.Set(ctx, "spaceId", 1234))
	lim, err = fx.Get(ctx, "spaceId")
	require.NoError(t, err)
	assert.Equal(t, uint64(1234), lim)
}

func TestFileLimit_Get(t *testing.T) {
	t.Run("not found error", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		l, e := fx.Get(ctx, fmt.Sprint(rand.Uint64()))
		assert.Empty(t, l)
		assert.EqualError(t, e, ErrNotFound.Error())
	})

}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		FileLimit: New(),
		db:        db.New(),
		a:         new(app.App),
	}
	fx.a.Register(config{}).Register(fx.db).Register(fx.FileLimit)
	require.NoError(t, fx.a.Start(ctx))
	_ = fx.db.Db().Drop(ctx)
	return fx
}

type fixture struct {
	FileLimit
	a  *app.App
	db db.Database
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
