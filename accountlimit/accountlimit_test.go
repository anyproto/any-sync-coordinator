package accountlimit

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-coordinator/db"
)

func TestAccountLimit_SetLimits(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	limits := Limits{
		Identity:          "123",
		Reason:            "reason",
		SpaceMembersRead:  100,
		SpaceMembersWrite: 200,
	}

	// set
	require.NoError(t, fx.SetLimits(ctx, limits))
	result, err := fx.GetLimits(ctx, "123")
	require.NoError(t, err)
	result.UpdatedTime = time.Time{}
	assert.Equal(t, limits, result)

	// update
	limits.SpaceMembersRead = 1000
	limits.SpaceMembersWrite = 2000
	limits.Reason = "upsert"
	require.NoError(t, fx.SetLimits(ctx, limits))
	result, err = fx.GetLimits(ctx, "123")
	require.NoError(t, err)
	result.UpdatedTime = time.Time{}
	assert.Equal(t, limits, result)

}

func TestAccountLimit_GetLimits(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	// get default limits
	limits, err := fx.GetLimits(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, uint32(5), limits.SpaceMembersWrite)
	assert.Equal(t, uint32(10), limits.SpaceMembersRead)
}

var ctx = context.Background()

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		AccountLimit: New(),
		a:            new(app.App),
	}

	fx.a.Register(db.New()).
		Register(fx.AccountLimit).
		Register(&testConfig{})

	require.NoError(t, fx.a.Start(ctx))
	_ = fx.a.MustComponent(db.CName).(db.Database).Db().Collection(collName).Drop(ctx)
	return fx
}

type fixture struct {
	AccountLimit
	a *app.App
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
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

func (c *testConfig) GetAccountLimit() Limits {
	return Limits{
		SpaceMembersWrite: 5,
		SpaceMembersRead:  10,
	}
}
