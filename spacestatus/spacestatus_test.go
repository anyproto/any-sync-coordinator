package spacestatus

import (
	"context"
	"github.com/anytypeio/any-sync-coordinator/config"
	"github.com/anytypeio/any-sync-coordinator/db"
	"github.com/anytypeio/any-sync/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var ctx = context.Background()

func TestSpaceStatus_ChangeStatus(t *testing.T) {
	t.Run("change status created to pending", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(context.Background(), spaceId, identity)
		require.NoError(t, err)
		raw := []byte{1}
		err = fx.ChangeStatus(context.Background(), spaceId, StatusChange{
			DeletePayload: raw,
			Identity:      identity,
			Status:        SpaceStatusDeletionPending,
		})
		require.NoError(t, err)
	})
	t.Run("can't create new two times", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(context.Background(), spaceId, identity)
		require.NoError(t, err)
		err = fx.NewStatus(context.Background(), spaceId, identity)
		require.Error(t, err)
	})
}

type fixture struct {
	SpaceStatus
	a      *app.App
	cancel context.CancelFunc
}

func newFixture(t *testing.T) *fixture {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	fx := fixture{
		SpaceStatus: New(),
		cancel:      cancel,
		a:           new(app.App),
	}
	fx.a.Register(&config.Config{
		Mongo: db.Mongo{
			Connect:          "mongodb://localhost:27017",
			Database:         "coordinator_test",
			SpacesCollection: "spaces",
		},
	})
	fx.a.Register(db.New())
	fx.a.Register(fx.SpaceStatus)
	err := fx.a.Start(ctx)
	if err != nil {
		fx.cancel()
	}
	require.NoError(t, err)
	return &fx
}

func (fx *fixture) Finish(t *testing.T) {
	if fx.cancel != nil {
		fx.cancel()
	}
	coll := fx.SpaceStatus.(*spaceStatus).spaces
	t.Log(coll.Drop(ctx))
	assert.NoError(t, fx.a.Close(ctx))
}
