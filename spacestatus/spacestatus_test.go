package spacestatus

import (
	"context"
	"fmt"
	"github.com/anytypeio/any-sync-coordinator/db"
	"github.com/anytypeio/any-sync-coordinator/nodeservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
	"time"
)

var ctx = context.Background()

type mockVerifier struct {
	result bool
}

func (m *mockVerifier) Verify(rawDelete *treechangeproto.RawTreeChangeWithId, identity []byte, peerId string) (err error) {
	if m.result {
		return nil
	} else {
		return fmt.Errorf("failed to verify")
	}
}

type mockDelSender struct {
}

func (m *mockDelSender) Delete(ctx context.Context, spaceId string, raw *treechangeproto.RawTreeChangeWithId) (err error) {
	return nil
}

func (m *mockDelSender) Init(a *app.App) (err error) {
	return
}

func (m *mockDelSender) Name() (name string) {
	return nodeservice.CName
}

type mockConfig struct {
	db.Mongo
	Config
}

func (c mockConfig) GetMongo() db.Mongo {
	return c.Mongo
}

func (c mockConfig) GetSpaceStatus() Config {
	return c.Config
}

func (c mockConfig) Init(a *app.App) (err error) {
	return
}

func (c mockConfig) Name() (name string) {
	return "config"
}

type delayedDeleter struct {
	runCh chan struct{}
	SpaceDeleter
}

func (d *delayedDeleter) Run(spaces *mongo.Collection, delSender DelSender) {
	go func() {
		<-d.runCh
		d.SpaceDeleter.Run(spaces, delSender)
	}()
}

func (d *delayedDeleter) Close() {
	d.SpaceDeleter.Close()
}

func TestSpaceStatus_NewAndChangeStatusAndStatus(t *testing.T) {
	t.Run("new status", func(t *testing.T) {
		fx := newFixture(t)
		fx.Run()
		fx.verifier.result = true
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(context.Background(), spaceId, identity)
		require.NoError(t, err)
		res, err := fx.Status(ctx, spaceId, identity)
		require.NoError(t, err)
		require.Equal(t, StatusEntry{
			SpaceId:      spaceId,
			Identity:     identity,
			DeletionDate: time.Time{},
			Status:       SpaceStatusCreated,
		}, res)
	})
	t.Run("pending status", func(t *testing.T) {
		fx := newFixture(t)
		fx.Run()
		fx.verifier.result = true
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(context.Background(), spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		marshalled, _ := raw.Marshal()
		err = fx.ChangeStatus(context.Background(), spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeletionPending,
		})
		require.NoError(t, err)
		res, err := fx.Status(ctx, spaceId, identity)
		require.NoError(t, err)
		if time.Now().Sub(res.DeletionDate).Seconds() > 10 {
			t.Fatal("incorrect deletion date")
		}
		res.DeletionDate = time.Time{}
		require.Equal(t, StatusEntry{
			SpaceId:         spaceId,
			Identity:        identity,
			DeletionPayload: marshalled,
			Status:          SpaceStatusDeletionPending,
		}, res)
	})
	t.Run("change status pending to created", func(t *testing.T) {
		fx := newFixture(t)
		fx.Run()
		fx.verifier.result = true
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(context.Background(), spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		err = fx.ChangeStatus(context.Background(), spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeletionPending,
		})
		require.NoError(t, err)
		err = fx.ChangeStatus(context.Background(), spaceId, StatusChange{
			Identity: identity,
			Status:   SpaceStatusCreated,
		})
		require.NoError(t, err)
		res, err := fx.Status(ctx, spaceId, identity)
		require.NoError(t, err)
		require.Equal(t, StatusEntry{
			SpaceId:      spaceId,
			Identity:     identity,
			DeletionDate: time.Time{},
			Status:       SpaceStatusCreated,
		}, res)
	})
	t.Run("failed to verify change", func(t *testing.T) {
		fx := newFixture(t)
		fx.Run()
		fx.verifier.result = false
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(context.Background(), spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		err = fx.ChangeStatus(context.Background(), spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeletionPending,
		})
		require.Error(t, err)
	})
	t.Run("set incorrect status change", func(t *testing.T) {
		fx := newFixture(t)
		fx.Run()
		fx.verifier.result = false
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(context.Background(), spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		err = fx.ChangeStatus(context.Background(), spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeletionStarted,
		})
		require.Error(t, err)
		err = fx.ChangeStatus(context.Background(), spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeleted,
		})
		require.Error(t, err)
	})
	t.Run("set wrong identity", func(t *testing.T) {
		fx := newFixture(t)
		fx.Run()
		fx.verifier.result = false
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(context.Background(), spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		err = fx.ChangeStatus(context.Background(), spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        []byte("other"),
			Status:          SpaceStatusDeletionPending,
		})
		require.Error(t, err)
	})
}

type fixture struct {
	SpaceStatus
	a        *app.App
	cancel   context.CancelFunc
	verifier *mockVerifier
	sender   *mockDelSender
	delayed  *delayedDeleter
}

func newFixture(t *testing.T) *fixture {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	fx := fixture{
		SpaceStatus: New(),
		verifier:    &mockVerifier{true},
		sender:      &mockDelSender{},
		cancel:      cancel,
		a:           new(app.App),
	}
	getChangeVerifier = func() ChangeVerifier {
		return fx.verifier
	}
	getSpaceDeleter = func(runSeconds int, deletionPeriod time.Duration) SpaceDeleter {
		del := newSpaceDeleter(runSeconds, deletionPeriod)
		fx.delayed = &delayedDeleter{make(chan struct{}), del}
		return fx.delayed
	}
	fx.a.Register(mockConfig{
		Mongo: db.Mongo{
			Connect:          "mongodb://localhost:27017",
			Database:         "coordinator_test",
			SpacesCollection: "spaces",
		},
		Config: Config{
			RunSeconds:         100,
			DeletionPeriodDays: 1,
		},
	})
	fx.a.Register(db.New())
	fx.a.Register(fx.sender)
	fx.a.Register(fx.SpaceStatus)
	err := fx.a.Start(ctx)
	if err != nil {
		fx.cancel()
	}
	require.NoError(t, err)
	return &fx
}

func (fx *fixture) Run() {
	close(fx.delayed.runCh)
}

func (fx *fixture) Finish(t *testing.T) {
	if fx.cancel != nil {
		fx.cancel()
	}
	coll := fx.SpaceStatus.(*spaceStatus).spaces
	t.Log(coll.Drop(ctx))
	assert.NoError(t, fx.a.Close(ctx))
}
