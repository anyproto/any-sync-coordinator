package spacestatus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/deletionlog"
)

var ctx = context.Background()

type mockVerifier struct {
	verify bool
}

func (m *mockVerifier) Verify(change StatusChange) (err error) {
	if m.verify {
		return nil
	} else {
		return fmt.Errorf("failed to verify")
	}
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

func (d *delayedDeleter) Run(spaces *mongo.Collection, delSender Deleter) {
	go func() {
		<-d.runCh
		d.SpaceDeleter.Run(spaces, delSender)
	}()
}

func (d *delayedDeleter) Close() {
	d.SpaceDeleter.Close()
}

func TestSpaceStatus_StatusOperations(t *testing.T) {
	_, identity, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	encoded := identity.Account()
	t.Run("new status", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		spaceId := "spaceId"

		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		require.NoError(t, err)
		res, err := fx.Status(ctx, spaceId)
		require.NoError(t, err)
		require.Equal(t, StatusEntry{
			Type:     SpaceTypeRegular,
			SpaceId:  spaceId,
			Identity: encoded,
			Status:   SpaceStatusCreated,
		}, res)

		// no error for second call
		err = fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		assert.NoError(t, err)
	})
	t.Run("new status force", func(t *testing.T) {
		t.Run("err space deleted", func(t *testing.T) {
			fx := newFixture(t, 1, 0)
			fx.Run()
			fx.verifier.verify = true
			defer fx.Finish(t)
			spaceId := "spaceId"

			err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
			require.NoError(t, err)

			_, err = fx.SpaceStatus.(*spaceStatus).setStatus(ctx, StatusChange{
				Status:  SpaceStatusDeleted,
				SpaceId: spaceId,
			}, SpaceStatusCreated)
			require.NoError(t, err)

			err = fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
			assert.EqualError(t, err, coordinatorproto.ErrSpaceIsDeleted.Error())
		})
		t.Run("force create", func(t *testing.T) {
			fx := newFixture(t, 1, 0)
			fx.Run()
			fx.verifier.verify = true
			defer fx.Finish(t)
			spaceId := "spaceId"

			err = fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
			require.NoError(t, err)

			_, err = fx.SpaceStatus.(*spaceStatus).setStatus(ctx, StatusChange{
				Status:  SpaceStatusDeleted,
				SpaceId: spaceId,
			}, SpaceStatusCreated)
			require.NoError(t, err)

			err = fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, true)
			require.NoError(t, err)

			res, err := fx.Status(ctx, spaceId)
			require.NoError(t, err)
			require.Equal(t, StatusEntry{
				Type:     SpaceTypeRegular,
				SpaceId:  spaceId,
				Identity: encoded,
				Status:   SpaceStatusCreated,
			}, res)

		})
	})
	t.Run("pending status", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		spaceId := "spaceId"

		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		marshalled, _ := raw.MarshalVT()
		checkStatus := func(res StatusEntry, err error) {
			require.NoError(t, err)
			if time.Now().Unix()-res.DeletionTimestamp > 10*int64(time.Second) {
				t.Fatal("incorrect deletion date")
			}
			require.Equal(t, time.Hour*24,
				time.Unix(res.ToBeDeletedTimestamp, 0).Sub(time.Unix(res.DeletionTimestamp, 0)))
			res.DeletionTimestamp = 0
			res.ToBeDeletedTimestamp = 0
			require.Equal(t, StatusEntry{
				Type:            SpaceTypeRegular,
				SpaceId:         spaceId,
				Identity:        encoded,
				DeletionPayload: marshalled,
				Status:          SpaceStatusDeletionPending,
			}, res)
		}
		res, err := fx.ChangeStatus(ctx, StatusChange{
			DeletionPayloadType: coordinatorproto.DeletionPayloadType_Tree,
			DeletionPayload:     marshalled,
			Identity:            identity,
			SpaceId:             spaceId,
			Status:              SpaceStatusDeletionPending,
		})
		checkStatus(res, err)
		res, err = fx.Status(ctx, spaceId)
		checkStatus(res, err)
	})
	t.Run("space delete", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		spaceId := "spaceId"

		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		marshalled, _ := raw.MarshalVT()
		checkStatus := func(res StatusEntry, err error) {
			require.NoError(t, err)
			if time.Now().Unix()-res.DeletionTimestamp > 10*int64(time.Second) {
				t.Fatal("incorrect deletion date")
			}
			require.Equal(t, time.Hour*24,
				time.Unix(res.ToBeDeletedTimestamp, 0).Sub(time.Unix(res.DeletionTimestamp, 0)))
			res.DeletionTimestamp = 0
			res.ToBeDeletedTimestamp = 0
			require.Equal(t, StatusEntry{
				Type:            SpaceTypeRegular,
				SpaceId:         spaceId,
				Identity:        encoded,
				DeletionPayload: marshalled,
				Status:          SpaceStatusDeletionPending,
			}, res)
		}
		res, err := fx.ChangeStatus(ctx, StatusChange{
			DeletionPayloadType: coordinatorproto.DeletionPayloadType_Tree,
			DeletionPayload:     marshalled,
			Identity:            identity,
			SpaceId:             spaceId,
			Status:              SpaceStatusDeletionPending,
		})
		checkStatus(res, err)
		res, err = fx.Status(ctx, spaceId)
		checkStatus(res, err)
	})
	t.Run("change status pending to created", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		spaceId := "spaceId"

		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		marshaled, _ := raw.MarshalVT()
		res, err := fx.ChangeStatus(ctx, StatusChange{
			DeletionPayload:     marshaled,
			DeletionPayloadType: coordinatorproto.DeletionPayloadType_Confirm,
			Identity:            identity,
			SpaceId:             spaceId,
			Status:              SpaceStatusDeletionPending,
		})
		require.NoError(t, err)
		res, err = fx.ChangeStatus(ctx, StatusChange{
			Identity: identity,
			SpaceId:  spaceId,
			Status:   SpaceStatusCreated,
		})
		require.NoError(t, err)
		require.Equal(t, StatusEntry{
			Type:     SpaceTypeRegular,
			SpaceId:  spaceId,
			Identity: encoded,
			Status:   SpaceStatusCreated,
		}, res)
	})
	t.Run("failed to verify change", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = false
		defer fx.Finish(t)
		spaceId := "spaceId"

		err := fx.NewStatus(ctx, spaceId, identity, 0, false)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		marshaled, _ := raw.MarshalVT()
		_, err = fx.ChangeStatus(ctx, StatusChange{
			DeletionPayload: marshaled,
			SpaceId:         spaceId,
			Identity:        identity,
			Status:          SpaceStatusDeletionPending,
		})
		require.Error(t, err)
	})
	t.Run("set incorrect status change", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		spaceId := "spaceId"

		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		require.NoError(t, err)
		_, err = fx.ChangeStatus(ctx, StatusChange{
			Identity: identity,
			SpaceId:  spaceId,
			Status:   SpaceStatusCreated,
		})
		require.Equal(t, err, coordinatorproto.ErrSpaceIsCreated)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		marshaled, _ := raw.MarshalVT()
		_, err = fx.ChangeStatus(ctx, StatusChange{
			Identity:        identity,
			DeletionPayload: marshaled,
			SpaceId:         spaceId,
			Status:          SpaceStatusDeletionPending,
		})
		require.NoError(t, err)
		_, err = fx.ChangeStatus(ctx, StatusChange{
			Identity:        identity,
			DeletionPayload: marshaled,
			SpaceId:         spaceId,
			Status:          SpaceStatusDeletionPending,
		})
		require.Equal(t, err, coordinatorproto.ErrSpaceDeletionPending)
		_, err = fx.ChangeStatus(ctx, StatusChange{
			DeletionPayload: marshaled,
			SpaceId:         spaceId,
			Identity:        identity,
			Status:          SpaceStatusDeletionStarted,
		})
		require.Equal(t, err, coordinatorproto.ErrUnexpected)
		_, err = fx.ChangeStatus(ctx, StatusChange{
			DeletionPayload: marshaled,
			SpaceId:         spaceId,
			Identity:        identity,
			Status:          SpaceStatusDeleted,
		})
		require.Equal(t, err, coordinatorproto.ErrUnexpected)
		_, err = fx.SpaceStatus.(*spaceStatus).modifyStatus(ctx, StatusChange{
			DeletionPayload: []byte{1},
			Identity:        identity,
			Status:          SpaceStatusDeleted,
			SpaceId:         spaceId,
		}, SpaceStatusDeletionPending)

		require.NoError(t, err)
		_, err = fx.ChangeStatus(ctx, StatusChange{
			Identity: identity,
			SpaceId:  spaceId,
			Status:   SpaceStatusCreated,
		})
		require.Equal(t, err, coordinatorproto.ErrSpaceIsDeleted)
	})
	t.Run("set wrong identity", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = false
		defer fx.Finish(t)
		spaceId := "spaceId"
		_, other, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)

		err = fx.NewStatus(ctx, spaceId, identity, 0, false)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		marshaled, _ := raw.MarshalVT()
		_, err = fx.ChangeStatus(ctx, StatusChange{
			DeletionPayload: marshaled,
			Identity:        other,
			SpaceId:         spaceId,
			Status:          SpaceStatusDeletionPending,
		})
		require.Equal(t, err, coordinatorproto.ErrUnexpected)
	})
	t.Run("new status space limit", func(t *testing.T) {
		var limit = 3
		fx := newFixture(t, 0, limit)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)

		for i := 0; i < limit; i++ {
			err := fx.NewStatus(ctx, fmt.Sprint(i), identity, 0, false)
			require.NoError(t, err)
		}

		err := fx.NewStatus(ctx, "spaceId", identity, 0, false)
		require.EqualError(t, err, coordinatorproto.ErrSpaceLimitReached.Error())
	})
	t.Run("restore status limit", func(t *testing.T) {
		var limit = 3
		fx := newFixture(t, 0, limit)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		spaceId := "spaceId"

		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		require.NoError(t, err)
		raw := &treechangeproto.RawTreeChangeWithId{
			RawChange: []byte{1},
			Id:        "id",
		}
		marshaled, _ := raw.MarshalVT()
		_, err = fx.ChangeStatus(ctx, StatusChange{
			DeletionPayload: marshaled,
			Identity:        identity,
			SpaceId:         spaceId,
			Status:          SpaceStatusDeletionPending,
		})
		require.NoError(t, err)

		for i := 0; i < limit; i++ {
			err := fx.NewStatus(ctx, fmt.Sprint(i), identity, SpaceTypeRegular, false)
			require.NoError(t, err)
		}

		_, err = fx.ChangeStatus(ctx, StatusChange{
			Identity: identity,
			SpaceId:  spaceId,
			Status:   SpaceStatusCreated,
		})
		assert.EqualError(t, err, coordinatorproto.ErrSpaceLimitReached.Error())
	})
}

func TestSpaceStatus_Run(t *testing.T) {
	_, identity, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	generateIds := func(t *testing.T, fx *fixture, new int, pending int) {
		for i := 0; i < new+pending; i++ {
			spaceId := fmt.Sprintf("space%d", i)
			err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
			require.NoError(t, err)
		}
		for i := new; i < new+pending; i++ {
			spaceId := fmt.Sprintf("space%d", i)
			raw := &treechangeproto.RawTreeChangeWithId{
				RawChange: []byte{1},
				Id:        "id",
			}
			marshaled, _ := raw.MarshalVT()
			_, err := fx.ChangeStatus(ctx, StatusChange{
				DeletionPayload: marshaled,
				Identity:        identity,
				SpaceId:         spaceId,
				Status:          SpaceStatusDeletionPending,
			})
			require.NoError(t, err)
		}
	}
	getStatus := func(t *testing.T, fx *fixture, index int) (status StatusEntry) {
		status, err := fx.Status(ctx, fmt.Sprintf("space%d", index))
		require.NoError(t, err)
		return
	}
	t.Run("test run simple", func(t *testing.T) {
		fx := newFixture(t, 0, 0)
		defer fx.Finish(t)
		new := 10
		pending := 10
		generateIds(t, fx, new, pending)
		fx.Run()
		time.Sleep(1 * time.Second)
		for i := 0; i < new; i++ {
			status := getStatus(t, fx, i)
			if status.Status != SpaceStatusCreated {
				t.Fatalf("should get status created for new ids")
			}
		}
		for i := new; i < new+pending; i++ {
			status := getStatus(t, fx, i)
			if status.Status != SpaceStatusDeleted {
				t.Fatalf("should get status deleted for pending ids")
			}
		}
	})
	t.Run("test run parallel", func(t *testing.T) {
		var otherFx *fixture
		mainFx := newFixture(t, 0, 0)
		defer mainFx.Finish(t)
		pending := 10
		generateIds(t, mainFx, 0, pending)
		startCh := make(chan struct{})
		stopCh := make(chan struct{})

		go func() {
			otherFx = newFixture(t, 0, 0)
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
			status := getStatus(t, mainFx, i)
			if status.Status != SpaceStatusDeleted {
				t.Fatalf("should get status deleted for pending ids")
			}
		}
	})
}

func TestSpaceStatus_SpaceDelete(t *testing.T) {
	_, identity, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	encoded := identity.Account()
	spaceId := "spaceId"
	raw := &treechangeproto.RawTreeChangeWithId{
		RawChange: []byte{1},
		Id:        "id",
	}
	marshalled, _ := raw.MarshalVT()
	checkStatus := func(fx *fixture, timestamp int64, delPeriod time.Duration, err error) {
		require.NoError(t, err)
		res, err := fx.Status(ctx, spaceId)
		require.NoError(t, err)
		if time.Now().Unix()-res.DeletionTimestamp > 10*int64(time.Second) {
			t.Fatal("incorrect deletion date")
		}
		require.Equal(t, timestamp, res.ToBeDeletedTimestamp)
		require.Equal(t, delPeriod,
			time.Unix(res.ToBeDeletedTimestamp, 0).Sub(time.Unix(res.DeletionTimestamp, 0)))
		res.DeletionTimestamp = 0
		res.ToBeDeletedTimestamp = 0
		require.Equal(t, StatusEntry{
			Type:                SpaceTypeRegular,
			DeletionPayloadType: int(coordinatorproto.DeletionPayloadType_Confirm),
			SpaceId:             spaceId,
			Identity:            encoded,
			DeletionPayload:     marshalled,
			Status:              SpaceStatusDeletionPending,
		}, res)
	}
	t.Run("regular space delete", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		require.NoError(t, err)
		delPeriod := time.Hour
		res, err := fx.SpaceDelete(ctx, SpaceDeletion{
			DeletionPayload: marshalled,
			DeletionPeriod:  delPeriod,
			AccountInfo: AccountInfo{
				Identity: identity,
			},
			SpaceId: spaceId,
		})
		checkStatus(fx, res, delPeriod, err)
	})
	t.Run("personal space delete - no error", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypePersonal, false)
		require.NoError(t, err)
		delPeriod := time.Hour
		_, err = fx.SpaceDelete(ctx, SpaceDeletion{
			DeletionPayload: marshalled,
			DeletionPeriod:  delPeriod,
			AccountInfo: AccountInfo{
				Identity: identity,
			},
			SpaceId: spaceId,
		})
		require.NoError(t, err)
	})
	t.Run("tech space delete - error", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeTech, false)
		require.NoError(t, err)
		delPeriod := time.Hour
		_, err = fx.SpaceDelete(ctx, SpaceDeletion{
			DeletionPayload: marshalled,
			DeletionPeriod:  delPeriod,
			AccountInfo: AccountInfo{
				Identity: identity,
			},
			SpaceId: spaceId,
		})
		require.Error(t, err)
	})
	t.Run("onetoone space delete - error", func(t *testing.T) {
		fx := newFixture(t, 1, 0)
		fx.Run()
		fx.verifier.verify = true
		defer fx.Finish(t)
		err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeOneToOne, false)
		require.NoError(t, err)
		delPeriod := time.Hour
		_, err = fx.SpaceDelete(ctx, SpaceDeletion{
			DeletionPayload: marshalled,
			DeletionPeriod:  delPeriod,
			AccountInfo: AccountInfo{
				Identity: identity,
			},
			SpaceId: spaceId,
		})
		require.Error(t, err)
	})

}

func TestSpaceStatus_AccountDelete(t *testing.T) {
	_, identity, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	generateAccount := func(t *testing.T, fx *fixture, new int) {
		err := fx.NewStatus(ctx, "personal", identity, SpaceTypePersonal, false)
		require.NoError(t, err)
		err = fx.NewStatus(ctx, "tech", identity, SpaceTypeTech, false)
		require.NoError(t, err)
		for i := 0; i < new; i++ {
			spaceId := fmt.Sprintf("space%d", i)
			err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
			require.NoError(t, err)
		}
	}
	generateOldAccount := func(t *testing.T, fx *fixture, new int) {
		_, err := fx.SpaceStatus.(*spaceStatus).spaces.InsertOne(ctx, bson.M{
			"identity": identity.Account(),
			"status":   SpaceStatusCreated,
			"_id":      "personal",
		})
		require.NoError(t, err)
		err = fx.NewStatus(ctx, "tech", identity, SpaceTypeTech, false)
		require.NoError(t, err)
		for i := 0; i < new; i++ {
			spaceId := fmt.Sprintf("space%d", i)
			err := fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
			require.NoError(t, err)
		}
	}
	checkStatuses := func(t *testing.T, fx *fixture, new int, timestamp int64, checkStatus int) {
		allIds := []string{"personal", "tech"}
		for i := 0; i < new; i++ {
			allIds = append(allIds, fmt.Sprintf("space%d", i))
		}
		for _, spaceId := range allIds {
			status, err := fx.Status(ctx, spaceId)
			require.NoError(t, err)
			require.Equal(t, checkStatus, status.Status)
			require.Equal(t, timestamp, status.ToBeDeletedTimestamp)
			if checkStatus == SpaceStatusDeleted {
				require.NotNil(t, status.DeletionPayload)
			}
		}
	}
	t.Run("test account delete - pending", func(t *testing.T) {
		fx := newFixture(t, 0, 0)
		defer fx.Finish(t)
		new := 10
		generateAccount(t, fx, new)
		tm, err := fx.AccountDelete(ctx, AccountDeletion{
			DeletionPayload:   []byte("payload"),
			DeletionPayloadId: "id",
			AccountInfo: AccountInfo{
				Identity: identity,
			},
		})
		require.NoError(t, err)
		checkStatuses(t, fx, new, tm, SpaceStatusDeletionPending)
	})
	t.Run("test old delete - pending", func(t *testing.T) {
		fx := newFixture(t, 0, 0)
		defer fx.Finish(t)
		new := 10
		generateOldAccount(t, fx, new)
		tm, err := fx.AccountDelete(ctx, AccountDeletion{
			DeletionPayload:   []byte("payload"),
			DeletionPayloadId: "id",
			AccountInfo: AccountInfo{
				Identity: identity,
			},
		})
		require.NoError(t, err)
		checkStatuses(t, fx, new, tm, SpaceStatusDeletionPending)
	})
	t.Run("test account delete - pending - revert deletion", func(t *testing.T) {
		fx := newFixture(t, 0, 0)
		defer fx.Finish(t)
		new := 10
		generateAccount(t, fx, new)
		tm, err := fx.AccountDelete(ctx, AccountDeletion{
			DeletionPayload:   []byte("payload"),
			DeletionPayloadId: "id",
			AccountInfo: AccountInfo{
				Identity: identity,
			},
		})
		checkStatuses(t, fx, new, tm, SpaceStatusDeletionPending)
		spaceId := fmt.Sprintf("space%d", new)
		err = fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		require.Equal(t, coordinatorproto.ErrAccountIsDeleted, err)
		err = fx.AccountRevertDeletion(ctx, AccountInfo{
			Identity: identity,
		})
		require.NoError(t, err)
		checkStatuses(t, fx, new, 0, SpaceStatusCreated)
		err = fx.NewStatus(ctx, spaceId, identity, SpaceTypeRegular, false)
		require.NoError(t, err)
	})
	t.Run("test account delete - deleted", func(t *testing.T) {
		fx := newFixture(t, 0, 0)
		defer fx.Finish(t)
		new := 10
		generateAccount(t, fx, new)
		tm, err := fx.AccountDelete(ctx, AccountDeletion{
			DeletionPayload:   []byte("payload"),
			DeletionPayloadId: "id",
			AccountInfo: AccountInfo{
				Identity: identity,
			},
		})
		require.NoError(t, err)
		checkStatuses(t, fx, new, tm, SpaceStatusDeletionPending)
		fx.Run()
		time.Sleep(time.Second)
		checkStatuses(t, fx, new, tm, SpaceStatusDeleted)
	})
}

func TestSpaceStatus_Status(t *testing.T) {
	fx := newFixture(t, 0, 0)
	defer fx.Finish(t)

	var (
		spaceId   = "space.id"
		spaceType = SpaceTypeRegular
	)

	_, identity, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	require.NoError(t, fx.NewStatus(ctx, spaceId, identity, spaceType, false))

	status, err := fx.Status(ctx, spaceId)
	require.NoError(t, err)

	assert.Equal(t, spaceId, status.SpaceId)
	assert.Equal(t, SpaceStatusCreated, status.Status)
	assert.Equal(t, spaceType, status.Type)
}

func TestSpaceStatus_MakeShareable(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t, 0, 0)
		defer fx.Finish(t)

		var (
			spaceId   = "space.id.shareable"
			spaceType = SpaceTypeRegular
		)

		_, identity, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)

		require.NoError(t, fx.NewStatus(ctx, spaceId, identity, spaceType, false))

		require.NoError(t, fx.MakeShareable(ctx, spaceId, SpaceTypeRegular, 2))

		entry, err := fx.Status(ctx, spaceId)
		require.NoError(t, err)
		assert.True(t, entry.IsShareable)

		require.NoError(t, fx.MakeShareable(ctx, spaceId, SpaceTypeRegular, 2))
	})
	t.Run("limit exceed", func(t *testing.T) {
		fx := newFixture(t, 0, 0)
		defer fx.Finish(t)

		var (
			spaceType = SpaceTypeRegular
		)

		_, identity, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			require.NoError(t, fx.NewStatus(ctx, fmt.Sprintf("space.%d", i), identity, spaceType, false))
		}

		require.NoError(t, fx.MakeShareable(ctx, "space.0", SpaceTypeRegular, 2))
		require.NoError(t, fx.MakeShareable(ctx, "space.1", SpaceTypeRegular, 2))
		require.ErrorIs(t, fx.MakeShareable(ctx, "space.2", SpaceTypeRegular, 2), coordinatorproto.ErrSpaceLimitReached)

		require.NoError(t, fx.MakeShareable(ctx, "space.2", SpaceTypeRegular, 3))
	})
}

func TestSpaceStatus_MakeUnshareable(t *testing.T) {
	fx := newFixture(t, 0, 0)
	defer fx.Finish(t)

	var (
		spaceId   = "space.id.shareable"
		spaceType = SpaceTypeRegular
	)

	_, identity, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	require.NoError(t, fx.NewStatus(ctx, spaceId, identity, spaceType, false))

	require.NoError(t, fx.MakeShareable(ctx, spaceId, SpaceTypeRegular, 2))

	entry, err := fx.Status(ctx, spaceId)
	require.NoError(t, err)
	assert.True(t, entry.IsShareable)

	require.NoError(t, fx.MakeUnshareable(ctx, spaceId))
	entry, err = fx.Status(ctx, spaceId)
	require.NoError(t, err)
	assert.False(t, entry.IsShareable)
}

func TestSpaceStatus_ChangeOwner(t *testing.T) {
	fx := newFixture(t, 0, 0)
	defer fx.Finish(t)

	var (
		spaceId   = "space.id.shareable"
		spaceType = SpaceTypeRegular
	)

	_, identity, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	_, newIdentity, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	require.NoError(t, fx.NewStatus(ctx, spaceId, identity, spaceType, false))

	require.NoError(t, fx.ChangeOwner(ctx, spaceId, newIdentity.Account()))

	status, err := fx.Status(ctx, spaceId)
	require.NoError(t, err)

	assert.Equal(t, newIdentity.Account(), status.Identity)
}

type fixture struct {
	SpaceStatus
	a          *app.App
	cancel     context.CancelFunc
	verifier   *mockVerifier
	delayed    *delayedDeleter
	deleteColl bool
}

func newFixture(t *testing.T, deletionPeriod, spaceLimit int) *fixture {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	fx := fixture{
		SpaceStatus: New(),
		verifier:    &mockVerifier{true},
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
			Connect:  "mongodb://localhost:27017",
			Database: "coordinator_unittest_spacestatus",
		},
		Config: Config{
			RunSeconds:         100,
			DeletionPeriodDays: deletionPeriod,
			SpaceLimit:         spaceLimit,
		},
	})
	fx.a.Register(db.New())
	fx.a.Register(fx.SpaceStatus)
	fx.a.Register(deletionlog.New())
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
