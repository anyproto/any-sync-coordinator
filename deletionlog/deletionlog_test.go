package deletionlog

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-coordinator/db"
)

var ctx = context.Background()

func TestDeletionLog_Add(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)
	id, err := fx.Add(ctx, "spaceId", "fileGroup", StatusOk)
	require.NoError(t, err)
	assert.NotEmpty(t, id)
	t.Log(id)
}

func TestDeletionLog_GetAfter(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	for i := 0; i < 10; i++ {
		fx.Add(ctx, fmt.Sprint(i), "fileGroup", StatusRemove)
	}

	t.Run("empty after if", func(t *testing.T) {
		res, hasMore, err := fx.GetAfter(ctx, "", 0)
		require.NoError(t, err)
		require.Len(t, res, 10)
		assert.False(t, hasMore)
	})
	t.Run("hasMore", func(t *testing.T) {
		res, hasMore, err := fx.GetAfter(ctx, "", 9)
		require.NoError(t, err)
		require.Len(t, res, 9)
		assert.True(t, hasMore)
	})
	t.Run("afterId", func(t *testing.T) {
		res, hasMore, err := fx.GetAfter(ctx, "", 5)
		require.NoError(t, err)
		require.Len(t, res, 5)
		assert.True(t, hasMore)
		lastId := res[4].Id.Hex()

		res2, hasMore2, err2 := fx.GetAfter(ctx, lastId, 0)
		require.NoError(t, err2)
		require.Len(t, res2, 5)
		assert.False(t, hasMore2)

		for i, r := range append(res, res2...) {
			assert.Equal(t, fmt.Sprint(i), r.SpaceId)
			assert.Equal(t, "fileGroup", r.FileGroup)
			assert.Equal(t, StatusRemove, r.Status)
			assert.NotEmpty(t, r.Id)
		}
	})

}

func TestDeletionLog_AddOwnershipChange(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)
	id, err := fx.AddOwnershipChange(ctx, "spaceId", "aclRecordId")
	require.NoError(t, err)
	assert.NotEmpty(t, id)

	res, _, err := fx.GetAfter(ctx, "", 0)
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, "spaceId", res[0].SpaceId)
	assert.Equal(t, "aclRecordId", res[0].AclRecordId)
	assert.Equal(t, StatusOwnershipChange, res[0].Status)
}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		DeletionLog: New(),
		db:          db.New(),
		a:           new(app.App),
	}
	fx.a.Register(config{}).Register(fx.db).Register(fx.DeletionLog)
	require.NoError(t, fx.a.Start(ctx))
	_ = fx.db.Db().Collection(collName).Drop(ctx)
	time.Sleep(time.Second / 2)
	return fx
}

type fixture struct {
	DeletionLog
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
		Database: "coordinator_unittest_deletionlog",
	}
}
