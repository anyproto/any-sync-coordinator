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
	verify bool
}

func (m *mockVerifier) Verify(rawDelete *treechangeproto.RawTreeChangeWithId, identity []byte, peerId string) (err error) {
	if m.verify {
		return nil
	} else {
		return fmt.Errorf("failed to verify")
	}
}

type mockDelSender struct {
	send bool
}

func (m *mockDelSender) Delete(ctx context.Context, spaceId string, raw *treechangeproto.RawTreeChangeWithId) (err error) {
	if m.send {
		return nil
	} else {
		return fmt.Errorf("cannot send")
	}
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

func TestSpaceStatus_StatusOperations(t *testing.T) {
	t.Run("new status", func(t *testing.T) {
		fx := newFixture(t, 1)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(ctx, spaceId, identity)
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
		fx := newFixture(t, 1)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(ctx, spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		marshalled, _ := raw.Marshal()
		checkStatus := func(res StatusEntry, err error) {
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
		}
		res, err := fx.ChangeStatus(ctx, spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeletionPending,
		})
		checkStatus(res, err)
		res, err = fx.Status(ctx, spaceId, identity)
		checkStatus(res, err)
	})
	t.Run("change status pending to created", func(t *testing.T) {
		fx := newFixture(t, 1)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(ctx, spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		res, err := fx.ChangeStatus(ctx, spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeletionPending,
		})
		require.NoError(t, err)
		res, err = fx.ChangeStatus(ctx, spaceId, StatusChange{
			Identity: identity,
			Status:   SpaceStatusCreated,
		})
		require.NoError(t, err)
		require.Equal(t, StatusEntry{
			SpaceId:      spaceId,
			Identity:     identity,
			DeletionDate: time.Time{},
			Status:       SpaceStatusCreated,
		}, res)
	})
	t.Run("failed to verify change", func(t *testing.T) {
		fx := newFixture(t, 1)
		fx.Run()
		fx.verifier.verify = false
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(ctx, spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		_, err = fx.ChangeStatus(ctx, spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeletionPending,
		})
		require.Error(t, err)
	})
	t.Run("set incorrect status change", func(t *testing.T) {
		fx := newFixture(t, 1)
		fx.Run()
		fx.verifier.verify = false
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(ctx, spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		_, err = fx.ChangeStatus(ctx, spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeletionStarted,
		})
		require.Error(t, err)
		_, err = fx.ChangeStatus(ctx, spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        identity,
			Status:          SpaceStatusDeleted,
		})
		require.Error(t, err)
	})
	t.Run("set wrong identity", func(t *testing.T) {
		fx := newFixture(t, 1)
		fx.Run()
		fx.verifier.verify = false
		defer fx.Finish(t)
		spaceId := "spaceId"
		identity := []byte("identity")

		err := fx.NewStatus(ctx, spaceId, identity)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		_, err = fx.ChangeStatus(ctx, spaceId, StatusChange{
			DeletionPayload: raw,
			Identity:        []byte("other"),
			Status:          SpaceStatusDeletionPending,
		})
		require.Error(t, err)
	})
}

func TestSpaceStatus_Run(t *testing.T) {
	identity := []byte("identity")
	generateIds := func(ctx context.Context, fx *fixture, new int, pending int) {
		for i := 0; i < new+pending; i++ {
			spaceId := fmt.Sprintf("space%d", i)
			err := fx.NewStatus(ctx, spaceId, identity)
			require.NoError(t, err)
		}
		for i := new; i < new+pending; i++ {
			spaceId := fmt.Sprintf("space%d", i)
			raw := &treechangeproto.RawTreeChangeWithId{
				RawChange: []byte{1},
				Id:        "id",
			}
			_, err := fx.ChangeStatus(ctx, spaceId, StatusChange{
				DeletionPayload: raw,
				Identity:        identity,
				Status:          SpaceStatusDeletionPending,
			})
			require.NoError(t, err)
		}
	}
	getStatus := func(ctx context.Context, fx *fixture, index int) (status StatusEntry) {
		status, err := fx.Status(ctx, fmt.Sprintf("space%d", index), identity)
		require.NoError(t, err)
		return
	}
	t.Run("test run simple", func(t *testing.T) {
		fx := newFixture(t, 0)
		defer fx.Finish(t)
		new := 10
		pending := 10
		generateIds(ctx, fx, new, pending)
		fx.Run()
		time.Sleep(1 * time.Second)
		for i := 0; i < new; i++ {
			status := getStatus(ctx, fx, i)
			if status.Status != SpaceStatusCreated {
				t.Fatalf("should get status created for new ids")
			}
		}
		for i := new; i < new+pending; i++ {
			status := getStatus(ctx, fx, i)
			if status.Status != SpaceStatusDeleted {
				t.Fatalf("should get status deleted for pending ids")
			}
		}
	})
	t.Run("test run errors", func(t *testing.T) {
		fx := newFixture(t, 0)
		defer fx.Finish(t)
		fx.sender.send = false
		pending := 10
		generateIds(ctx, fx, 0, pending)
		fx.Run()
		time.Sleep(1 * time.Second)
		for i := 0; i < pending; i++ {
			status := getStatus(ctx, fx, i)
			if status.Status != SpaceStatusDeletionPending {
				t.Fatalf("should get status pending for pending ids")
			}
		}
	})
	t.Run("test run parallel", func(t *testing.T) {
		var otherFx *fixture
		mainFx := newFixture(t, 0)
		defer mainFx.Finish(t)
		pending := 10
		generateIds(ctx, mainFx, 0, pending)
		startCh := make(chan struct{})
		stopCh := make(chan struct{})

		go func() {
			otherFx = newFixture(t, 0)
			otherFx.deleteColl = false
			defer otherFx.Finish(t)
			close(startCh)
			<-stopCh
		}()

		<-startCh
		mainFx.Run()
		otherFx.Run()
		time.Sleep(1 * time.Second)
		close(stopCh)
		for i := 0; i < pending; i++ {
			status := getStatus(ctx, mainFx, i)
			if status.Status != SpaceStatusDeleted {
				t.Fatalf("should get status deleted for pending ids")
			}
		}
	})
}

type fixture struct {
	SpaceStatus
	a          *app.App
	cancel     context.CancelFunc
	verifier   *mockVerifier
	sender     *mockDelSender
	delayed    *delayedDeleter
	deleteColl bool
}

func newFixture(t *testing.T, deletionPeriod int) *fixture {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	fx := fixture{
		SpaceStatus: New(),
		verifier:    &mockVerifier{true},
		sender:      &mockDelSender{true},
		cancel:      cancel,
		deleteColl:  true,
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
			DeletionPeriodDays: deletionPeriod,
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
	if fx.deleteColl {
		coll := fx.SpaceStatus.(*spaceStatus).spaces
		t.Log(coll.Drop(ctx))
	}
	assert.NoError(t, fx.a.Close(ctx))
}
