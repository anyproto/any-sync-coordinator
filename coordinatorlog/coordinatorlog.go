package coordinatorlog

import (
	"context"
	"github.com/anytypeio/any-sync-coordinator/db"
	"github.com/anytypeio/any-sync/app"
	"go.mongodb.org/mongo-driver/mongo"
)

const CName = "coordinator.coordinatorlog"

type SpaceReceiptEntry struct {
	SignedSpaceReceipt []byte `bson:"receipt"`
	SpaceId            string `bson:"spaceId"`
	PeerId             string `bson:"peerId"`
	Identity           []byte `bson:"identity"`
}

type CoordinatorLog interface {
	app.ComponentRunnable
	SpaceReceipt(ctx context.Context, entry SpaceReceiptEntry) (err error)
}

func New() CoordinatorLog {
	return &coordinatorLog{}
}

type coordinatorLog struct {
	db      db.Database
	logColl *mongo.Collection
}

func (c *coordinatorLog) Init(a *app.App) (err error) {
	c.db = a.MustComponent(db.CName).(db.Database)
	return
}

func (c *coordinatorLog) Name() (name string) {
	return CName
}

func (c *coordinatorLog) Run(ctx context.Context) (err error) {
	c.logColl = c.db.LogCollection()
	return
}

func (c *coordinatorLog) Close(ctx context.Context) (err error) {
	return
}

func (c *coordinatorLog) SpaceReceipt(ctx context.Context, entry SpaceReceiptEntry) (err error) {
	_, err = c.logColl.InsertOne(ctx, entry)
	return
}
