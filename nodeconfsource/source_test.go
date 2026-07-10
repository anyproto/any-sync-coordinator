package nodeconfsource

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/anyproto/any-sync-coordinator/db"
)

var ctx = context.Background()

func TestNodeConfSource_GetLast(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	_, err := fx.GetLast(ctx, "")
	require.EqualError(t, err, nodeconf.ErrConfigurationNotFound.Error())

	conf := ConfModel{
		Id:            primitive.NewObjectID(),
		NetworkId:     "testnetwork",
		FileNetworkId: "NfleetReceiptSigningIdentity",
		Nodes: []nodeconf.Node{
			{
				PeerId:    "p1",
				Addresses: []string{"addr1", "addr2"},
				Types:     []nodeconf.NodeType{nodeconf.NodeTypeCoordinator, nodeconf.NodeTypeTree},
			},
			{
				PeerId:    "p2",
				Addresses: []string{"addr3", "addr4"},
				Types:     []nodeconf.NodeType{nodeconf.NodeTypeFile, nodeconf.NodeTypeTree},
			},
			{
				PeerId:    "p5",
				Addresses: []string{"addr5", "addr6"},
				Types:     []nodeconf.NodeType{nodeconf.NodeTypeFileV2},
			},
		},
		CreationTime: time.Now(),
		Enable:       false,
	}
	_, err = fx.db.Db().Collection(collName).InsertOne(ctx, conf)
	require.NoError(t, err)

	_, err = fx.GetLast(ctx, "")
	require.EqualError(t, err, nodeconf.ErrConfigurationNotFound.Error())

	_, err = fx.db.Db().Collection(collName).UpdateByID(ctx, conf.Id, bson.M{"$set": bson.M{"enable": true}})
	require.NoError(t, err)

	got, err := fx.GetLast(ctx, "")
	require.NoError(t, err)

	assert.Equal(t, conf.Id.Hex(), got.Id)
	assert.Equal(t, conf.NetworkId, got.NetworkId)
	assert.Equal(t, conf.FileNetworkId, got.FileNetworkId)
	assert.Equal(t, conf.Nodes, got.Nodes)

	conf2 := ConfModel{
		Id:        primitive.NewObjectID(),
		NetworkId: "testnetwork",
		Nodes: []nodeconf.Node{
			{
				PeerId:    "p3",
				Addresses: []string{"addr1", "addr2"},
				Types:     []nodeconf.NodeType{nodeconf.NodeTypeCoordinator, nodeconf.NodeTypeTree},
			},
			{
				PeerId:    "p4",
				Addresses: []string{"addr3", "addr4"},
				Types:     []nodeconf.NodeType{nodeconf.NodeTypeFile, nodeconf.NodeTypeTree},
			},
		},
		CreationTime: time.Now(),
		Enable:       true,
	}
	_, err = fx.db.Db().Collection(collName).InsertOne(ctx, conf2)
	require.NoError(t, err)

	got, err = fx.GetLast(ctx, "")
	require.NoError(t, err)

	assert.Equal(t, conf2.Id.Hex(), got.Id)
	assert.Equal(t, conf2.NetworkId, got.NetworkId)
	assert.Equal(t, conf2.Nodes, got.Nodes)

	_, err = fx.GetLast(ctx, got.Id)
	require.EqualError(t, err, nodeconf.ErrConfigurationNotChanged.Error())

	// Add (the confapply path) round-trips fileNetworkId through mongo.
	addedId, _, err := fx.Add(ctx, nodeconf.Configuration{
		NetworkId:     "testnetwork",
		FileNetworkId: "NfleetReceiptSigningIdentity2",
		Nodes:         conf2.Nodes,
	}, true, true)
	require.NoError(t, err)
	got, err = fx.GetLast(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, addedId, got.Id)
	assert.Equal(t, "NfleetReceiptSigningIdentity2", got.FileNetworkId)
}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		NodeConfSource: New(),
		db:             db.New(),
		a:              new(app.App),
	}
	fx.a.Register(config{}).Register(fx.db).Register(fx.NodeConfSource)
	require.NoError(t, fx.a.Start(ctx))
	_ = fx.db.Db().Collection(collName).Drop(ctx)
	time.Sleep(time.Second)
	return fx
}

type fixture struct {
	NodeConfSource
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

func treeNodes(peerIds ...string) []nodeconf.Node {
	nodes := make([]nodeconf.Node, 0, len(peerIds))
	for _, id := range peerIds {
		nodes = append(nodes, nodeconf.Node{
			PeerId:    id,
			Addresses: []string{"127.0.0.1:1000"},
			Types:     []nodeconf.NodeType{nodeconf.NodeTypeTree},
		})
	}
	return nodes
}

func TestNodeConfSource_Add(t *testing.T) {
	t.Run("epoch increments", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		_, epoch, err := fx.Add(ctx, nodeconf.Configuration{
			NetworkId: "testnetwork",
			Nodes:     treeNodes("p1", "p2", "p3"),
		}, true, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), epoch)

		// one node replaced: safe, epoch bumps
		id, epoch, err := fx.Add(ctx, nodeconf.Configuration{
			NetworkId: "testnetwork",
			Nodes:     treeNodes("p1", "p2", "p4"),
		}, true, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), epoch)

		got, err := fx.GetLast(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, id, got.Id)
		assert.Equal(t, uint64(2), got.Epoch)
	})
	t.Run("guardrail rejects replacing all replicas", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		_, _, err := fx.Add(ctx, nodeconf.Configuration{
			NetworkId: "testnetwork",
			Nodes:     treeNodes("p1", "p2", "p3"),
		}, true, false)
		require.NoError(t, err)

		// entirely new node set: every partition loses all replicas
		_, _, err = fx.Add(ctx, nodeconf.Configuration{
			NetworkId: "testnetwork",
			Nodes:     treeNodes("q1", "q2", "q3"),
		}, true, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsafe configuration")

		// force bypasses the guardrail
		_, epoch, err := fx.Add(ctx, nodeconf.Configuration{
			NetworkId: "testnetwork",
			Nodes:     treeNodes("q1", "q2", "q3"),
		}, true, true)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), epoch)
	})
	t.Run("disabled config skips validation but still gets an epoch", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		_, _, err := fx.Add(ctx, nodeconf.Configuration{
			NetworkId: "testnetwork",
			Nodes:     treeNodes("p1", "p2", "p3"),
		}, true, false)
		require.NoError(t, err)

		_, epoch, err := fx.Add(ctx, nodeconf.Configuration{
			NetworkId: "testnetwork",
			Nodes:     treeNodes("q1", "q2", "q3"),
		}, false, false)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), epoch)
	})
}

func TestValidateTransition(t *testing.T) {
	t.Run("add node ok", func(t *testing.T) {
		assert.NoError(t, validateTransition(treeNodes("p1", "p2", "p3"), treeNodes("p1", "p2", "p3", "p4")))
	})
	t.Run("remove node ok", func(t *testing.T) {
		assert.NoError(t, validateTransition(treeNodes("p1", "p2", "p3", "p4"), treeNodes("p1", "p2", "p3")))
	})
	t.Run("replace one of three ok", func(t *testing.T) {
		assert.NoError(t, validateTransition(treeNodes("p1", "p2", "p3"), treeNodes("p1", "p2", "p4")))
	})
	t.Run("replace all rejected", func(t *testing.T) {
		assert.Error(t, validateTransition(treeNodes("p1", "p2", "p3"), treeNodes("q1", "q2", "q3")))
	})
	t.Run("no tree nodes in proposed rejected", func(t *testing.T) {
		err := validateTransition(treeNodes("p1"), []nodeconf.Node{{PeerId: "c1", Types: []nodeconf.NodeType{nodeconf.NodeTypeCoordinator}}})
		assert.Error(t, err)
	})
	t.Run("first config with tree nodes ok", func(t *testing.T) {
		assert.NoError(t, validateTransition(nil, treeNodes("p1")))
	})
}
