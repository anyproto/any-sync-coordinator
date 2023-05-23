package nodeconfsource

import (
	"context"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
	"time"
)

var ctx = context.Background()

func TestNodeConfSource_GetLast(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	_, err := fx.GetLast(ctx, "")
	require.EqualError(t, err, nodeconf.ErrConfigurationNotFound.Error())

	conf := ConfModel{
		Id:        primitive.NewObjectID(),
		NetworkId: "testnetwork",
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
}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		NodeConfSource: New(),
		db:             db.New(),
		a:              new(app.App),
	}
	fx.a.Register(config{}).Register(fx.db).Register(fx.NodeConfSource)
	require.NoError(t, fx.a.Start(ctx))
	_ = fx.db.Db().Drop(ctx)
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
