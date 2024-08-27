package acleventlog

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/anyproto/any-sync-coordinator/db"
)

var ctx = context.Background()

func TestEventLog_Add(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	id := primitive.NewObjectID()

	err := fx.AddLog(ctx, AclEventLogEntry{
		Id:        &id,
		SpaceId:   "space1",
		PeerId:    "peer1",
		Owner:     "identity1",
		Timestamp: time.Now().Unix(),
		EntryType: EntryTypeSpaceReceipt,
	})

	require.NoError(t, err)
}

func TestEventLog_GetAfter(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	for i := 0; i < 10; i++ {
		id := primitive.NewObjectID()

		err := fx.AddLog(ctx, AclEventLogEntry{
			Id:        &id,
			SpaceId:   "space1",
			PeerId:    "peerA",
			Owner:     "identity1",
			Timestamp: time.Now().Unix(),
			EntryType: EntryTypeSpaceReceipt,
		})

		require.NoError(t, err)
	}

	t.Run("no identity", func(t *testing.T) {
		// should return error
		_, _, err := fx.GetAfter(ctx, "", "", 0)
		require.Equal(t, ErrNoIdentity, err)
	})

	t.Run("success", func(t *testing.T) {
		res, hasMore, err := fx.GetAfter(ctx, "identity1", "", 0)

		require.NoError(t, err)
		require.Len(t, res, 10)
		assert.False(t, hasMore)
	})

	t.Run("hasMore for last item", func(t *testing.T) {
		res, hasMore, err := fx.GetAfter(ctx, "identity1", "", 9)
		require.NoError(t, err)
		require.Len(t, res, 9)
		assert.True(t, hasMore)
	})

	t.Run("afterId", func(t *testing.T) {
		res, hasMore, err := fx.GetAfter(ctx, "identity1", "", 5)
		require.NoError(t, err)
		require.Len(t, res, 5)
		assert.True(t, hasMore)
		lastId := res[4].Id.Hex()

		res2, hasMore2, err2 := fx.GetAfter(ctx, "identity1", lastId, 0)
		require.NoError(t, err2)
		require.Len(t, res2, 5)
		assert.False(t, hasMore2)
	})
}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		AclEventLog: New(),
		db:          db.New(),
		a:           new(app.App),
	}

	fx.a.Register(config{}).Register(fx.db).Register(fx.AclEventLog)

	require.NoError(t, fx.a.Start(ctx))
	_ = fx.db.Db().Collection(collName).Drop(ctx)

	time.Sleep(time.Second / 2)
	return fx
}

type fixture struct {
	AclEventLog

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
		Database: "coordinator_unittest_eventlog",
	}
}
