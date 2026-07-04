package nodeconfsource

import (
	"context"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/nodeconf"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/anyproto/any-sync-coordinator/db"
)

const collName = "nodeConf"

func New() NodeConfSource {
	return new(nodeConfSource)
}

type NodeConfSource interface {
	nodeconf.Source
	app.Component
	// Add stores a new network configuration assigning it the next epoch.
	// Unless force is set, an enabled configuration is validated against the
	// currently active one: every chash partition must keep at least one
	// surviving tree-node replica (see validateTransition).
	Add(ctx context.Context, conf nodeconf.Configuration, enable, force bool) (id string, epoch uint64, err error)
}

type ConfModel struct {
	Id           primitive.ObjectID `bson:"_id"`
	NetworkId    string             `bson:"networkId"`
	Nodes        []nodeconf.Node    `bson:"nodes"`
	CreationTime time.Time          `bson:"creationTime"`
	Enable       bool               `bson:"enable"`
	Epoch        uint64             `bson:"epoch"`
}

type nodeConfSource struct {
	coll *mongo.Collection
}

func (n *nodeConfSource) Init(a *app.App) (err error) {
	n.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	// epochs must be unique: concurrent Adds otherwise mint duplicates and
	// corrupt per-epoch config history on the nodes (index is partial to
	// tolerate pre-epoch documents, which all carry 0)
	_, err = n.coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.D{{"epoch", 1}},
		Options: options.Index().
			SetUnique(true).
			SetPartialFilterExpression(bson.M{"epoch": bson.M{"$gt": 0}}),
	})
	return err
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
		Epoch:        model.Epoch,
	}, nil
}

func (n *nodeConfSource) IsNetworkNeedsUpdate(ctx context.Context) (bool, error) {
	return false, nil
}

var lastEpochSort = options.FindOne().SetSort(bson.D{{"epoch", -1}})

func (n *nodeConfSource) Add(ctx context.Context, conf nodeconf.Configuration, enable, force bool) (id string, epoch uint64, err error) {
	if conf.NetworkId == "" {
		return "", 0, fmt.Errorf("network id not specified")
	}
	if len(conf.Nodes) == 0 {
		return "", 0, fmt.Errorf("you must provide at leat one node")
	}
	if enable && !force {
		var active nodeconf.Configuration
		active, err = n.GetLast(ctx, "")
		switch {
		case err == nil:
			if err = validateTransition(active.Nodes, conf.Nodes); err != nil {
				return "", 0, err
			}
		case err == nodeconf.ErrConfigurationNotFound:
			err = nil
		default:
			return "", 0, err
		}
	}
	// read-max-then-insert is racy; the unique epoch index turns a concurrent
	// Add into a duplicate-key error which we resolve by retrying
	for attempt := 0; attempt < 5; attempt++ {
		var last ConfModel
		if lastErr := n.coll.FindOne(ctx, bson.D{}, lastEpochSort).Decode(&last); lastErr != nil && lastErr != mongo.ErrNoDocuments {
			return "", 0, lastErr
		}
		m := ConfModel{
			Id:           primitive.NewObjectID(),
			NetworkId:    conf.NetworkId,
			Nodes:        conf.Nodes,
			CreationTime: time.Now(),
			Enable:       enable,
			Epoch:        last.Epoch + 1,
		}
		if _, err = n.coll.InsertOne(ctx, m); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				continue
			}
			return
		}
		return m.Id.Hex(), m.Epoch, nil
	}
	return "", 0, fmt.Errorf("can't assign a unique epoch after retries: %w", err)
}
