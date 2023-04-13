package nodeconfsource

import (
	"context"
	"github.com/anytypeio/any-sync-coordinator/db"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/nodeconf"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const collName = "nodeConf"

func New() NodeConfSource {
	return new(nodeConfSource)
}

type NodeConfSource interface {
	nodeconf.Source
	app.Component
}

type ConfModel struct {
	Id           primitive.ObjectID `bson:"_id"`
	NetworkId    string             `bson:"networkId"`
	Nodes        []nodeconf.Node    `bson:"nodes"`
	CreationTime time.Time          `bson:"creationTime"`
	Enable       bool               `bson:"enable"`
}

type nodeConfSource struct {
	coll *mongo.Collection
}

func (n *nodeConfSource) Init(a *app.App) (err error) {
	n.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	return nil
}

func (n *nodeConfSource) Name() (name string) {
	return nodeconf.CNameSource
}

var getLastSort = options.FindOne().SetSort(bson.D{{"_id", -1}})
var getLastFilter = bson.D{{"enable", true}}

func (n *nodeConfSource) GetLast(ctx context.Context, currentId string) (c nodeconf.Configuration, err error) {
	var model ConfModel
	if err = n.coll.FindOne(ctx, getLastFilter, getLastSort).Decode(&model); err != nil {
		if err == mongo.ErrNoDocuments {
			err = nodeconf.ErrConfigurationNotFound
		}
		return
	}
	if model.Id.Hex() == currentId {
		err = nodeconf.ErrConfigurationNotChanged
		return
	}
	return nodeconf.Configuration{
		Id:           model.Id.Hex(),
		NetworkId:    model.NetworkId,
		Nodes:        model.Nodes,
		CreationTime: model.CreationTime,
	}, nil
}
