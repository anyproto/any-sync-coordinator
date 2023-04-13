package coordinatorlog

import (
	"context"
	"github.com/anytypeio/any-sync-coordinator/db"
	"github.com/anytypeio/any-sync/app"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

const CName = "coordinator.coordinatorlog"

const collName = "log"

type SpaceReceiptEntry struct {
	SignedSpaceReceipt []byte `bson:"receipt"`
	SpaceId            string `bson:"spaceId"`
	PeerId             string `bson:"peerId"`
	Identity           string `bson:"identity"`
	Timestamp          int64  `bson:"timestamp"`
}

type CoordinatorLog interface {
	app.Component
	SpaceReceipt(ctx context.Context, entry SpaceReceiptEntry) (err error)
}

func New() CoordinatorLog {
	return &coordinatorLog{}
}

type coordinatorLog struct {
	logColl *mongo.Collection
}

func (c *coordinatorLog) Init(a *app.App) (err error) {
	c.logColl = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	return
}

func (c *coordinatorLog) Name() (name string) {
	return CName
}

func (c *coordinatorLog) SpaceReceipt(ctx context.Context, entry SpaceReceiptEntry) (err error) {
	entry.Timestamp = time.Now().Unix()
	_, err = c.logColl.InsertOne(ctx, entry)
	return
}
