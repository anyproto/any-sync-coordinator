//go:generate mockgen -destination mock_coordinatorlog/mock_coordinatorlog.go github.com/anyproto/any-sync-coordinator/coordinatorlog CoordinatorLog
package coordinatorlog

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/app"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"

	"github.com/anyproto/any-sync-coordinator/db"
)

const CName = "coordinator.coordinatorlog"

const (
	collName     = "log"
	defaultLimit = 1000
)

type findIdGt struct {
	Identity string `bson:"identity"`

	Id struct {
		Gt primitive.ObjectID `bson:"$gt"`
	} `bson:"_id"`
}

type findIdentity struct {
	Identity string `bson:"identity"`
}

var sortById = bson.D{{"_id", 1}}

type SpaceReceiptEntryType uint8

const (
	EntryTypeSpaceReceipt          SpaceReceiptEntryType = 0
	EntryTypeSpaceSharedOrUnshared SpaceReceiptEntryType = 1
	EntryTypeSpaceAclAddRecord     SpaceReceiptEntryType = 2
)

type SpaceUpdateEntry struct {
	Id        *primitive.ObjectID   `bson:"_id,omitempty"`
	SpaceId   string                `bson:"spaceId"`
	PeerId    string                `bson:"peerId"`
	Identity  string                `bson:"identity"`
	Timestamp int64                 `bson:"timestamp"`
	EntryType SpaceReceiptEntryType `bson:"entryType"`
	// only for EntryTypeSpaceReceipt
	SignedSpaceReceipt []byte `bson:"receipt"`
	// TODO: ?
	//AclRecord consensusproto.RawRecordWithId `bson:"record"`
}

type CoordinatorLog interface {
	app.Component
	AddLog(ctx context.Context, entry SpaceUpdateEntry) (err error)
	GetAfter(ctx context.Context, identity string, afterId string, limit uint32) (records []SpaceUpdateEntry, hasMore bool, err error)
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

func (c *coordinatorLog) AddLog(ctx context.Context, entry SpaceUpdateEntry) (err error) {
	entry.Timestamp = time.Now().Unix()
	_, err = c.logColl.InsertOne(ctx, entry)
	return
}

type findStatusQuery struct {
	Identity string `bson:"identity"`
}

func (d *coordinatorLog) GetAfter(ctx context.Context, identity string, afterId string, limit uint32) (records []SpaceUpdateEntry, hasMore bool, err error) {
	if limit == 0 || limit > defaultLimit {
		limit = defaultLimit
	}
	// fetch one more item to detect a hasMore
	limit += 1

	var q any

	if afterId != "" {
		var qGt findIdGt
		if qGt.Id.Gt, err = primitive.ObjectIDFromHex(afterId); err != nil {
			return
		}
		qGt.Identity = identity

		q = qGt
	} else {
		var qId findIdentity
		qId.Identity = identity

		q = qId
	}
	it, err := d.logColl.Find(ctx, q, options.Find().SetSort(sortById).SetLimit(int64(limit)))
	if err != nil {
		return
	}
	defer func() {
		_ = it.Close(ctx)
	}()
	records = make([]SpaceUpdateEntry, 0, limit)
	for it.Next(ctx) {
		var rec SpaceUpdateEntry
		if err = it.Decode(&rec); err != nil {
			return
		}
		records = append(records, rec)
	}
	if len(records) == int(limit) {
		records = records[:len(records)-1]
		hasMore = true
	}
	return
}
