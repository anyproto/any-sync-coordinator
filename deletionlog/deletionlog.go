package deletionlog

import (
	"context"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const CName = "coordinator.deletionLog"

var log = logger.NewNamed(CName)

const (
	collName     = "deletionLog"
	defaultLimit = 1000
)

func New() DeletionLog {
	return new(deletionLog)
}

type DeletionLog interface {
	GetAfter(ctx context.Context, afterId string, limit uint32) (records []Record, hasMore bool, err error)
	Add(ctx context.Context, spaceId string, status Status) (id string, err error)
	app.Component
}

type Record struct {
	Id      *primitive.ObjectID `bson:"_id,omitempty"`
	SpaceId string              `bson:"spaceId"`
	Status  Status              `bson:"status"`
}

func (r Record) Timestamp() int64 {
	return r.Id.Timestamp().Unix()
}

type Status int32

const (
	StatusOk            = Status(coordinatorproto.DeletionLogRecordStatus_Ok)
	StatusRemovePrepare = Status(coordinatorproto.DeletionLogRecordStatus_RemovePrepare)
	StatusRemove        = Status(coordinatorproto.DeletionLogRecordStatus_Remove)
)

type deletionLog struct {
	coll *mongo.Collection
}

func (d *deletionLog) Init(a *app.App) (err error) {
	d.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	return
}

func (d *deletionLog) Name() (name string) {
	return CName
}

type findIdGt struct {
	Id struct {
		Gt primitive.ObjectID `bson:"$gt"`
	} `bson:"_id"`
}

var sortById = bson.D{{"_id", 1}}

func (d *deletionLog) GetAfter(ctx context.Context, afterId string, limit uint32) (records []Record, hasMore bool, err error) {
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
		q = qGt
	} else {
		q = bson.D{}
	}
	it, err := d.coll.Find(ctx, q, options.Find().SetSort(sortById).SetLimit(int64(limit)))
	if err != nil {
		return
	}
	defer func() {
		_ = it.Close(ctx)
	}()
	records = make([]Record, 0, limit)
	for it.Next(ctx) {
		var rec Record
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

func (d *deletionLog) Add(ctx context.Context, spaceId string, status Status) (id string, err error) {
	rec := Record{
		SpaceId: spaceId,
		Status:  status,
	}
	res, err := d.coll.InsertOne(ctx, rec)
	if err != nil {
		return
	}
	return res.InsertedID.(primitive.ObjectID).Hex(), nil
}
