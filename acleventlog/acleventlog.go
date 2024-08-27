//go:generate mockgen -destination mock_eventlog/mock_eventlog.go github.com/anyproto/any-sync-coordinator/acleventlog AclEventLog
package acleventlog

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/anyproto/any-sync-coordinator/db"
)

const CName = "coordinator.aclEventLog"

var log = logger.NewNamed(CName)

const (
	collName     = "aclEventLog"
	defaultLimit = 1000
)

var (
	ErrNoIdentity = errors.New("no identity")
)

func New() AclEventLog {
	return new(aclEventLog)
}

type EventLogEntryType uint8

const (
	EntryTypeSpaceReceipt      EventLogEntryType = 0
	EntryTypeSpaceShared       EventLogEntryType = 1
	EntryTypeSpaceUnshared     EventLogEntryType = 2
	EntryTypeSpaceAclAddRecord EventLogEntryType = 3
)

type AclEventLogEntry struct {
	Id        *primitive.ObjectID `bson:"_id,omitempty"`
	SpaceId   string              `bson:"spaceId"`
	PeerId    string              `bson:"peerId"`
	Owner     string              `bson:"owner"`
	Timestamp int64               `bson:"timestamp"`

	EntryType EventLogEntryType `bson:"entryType"`
	// only for EntryTypeSpaceAclAddRecord
	AclChangeId string `bson:"aclChangeId"`
}

type findIdGt struct {
	Owner string `bson:"owner"`

	Id struct {
		Gt primitive.ObjectID `bson:"$gt"`
	} `bson:"_id"`
}

type findOwner struct {
	Owner string `bson:"owner"`
}

var sortById = bson.D{{"_id", 1}}

type AclEventLog interface {
	AddLog(ctx context.Context, event AclEventLogEntry) (err error)
	GetAfter(ctx context.Context, identity, afterId string, limit uint32) (records []AclEventLogEntry, hasMore bool, err error)

	app.ComponentRunnable
}

type aclEventLog struct {
	coll *mongo.Collection
}

func (d *aclEventLog) Init(a *app.App) (err error) {
	d.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	return
}

func (d *aclEventLog) Name() (name string) {
	return CName
}

func (d *aclEventLog) Run(ctx context.Context) error {
	// create collection if doesn't exist
	_ = d.coll.Database().CreateCollection(ctx, collName)
	return nil
}

func (d *aclEventLog) Close(_ context.Context) (err error) {
	return nil
}

func (d *aclEventLog) GetAfter(ctx context.Context, identity string, afterId string, limit uint32) (records []AclEventLogEntry, hasMore bool, err error) {
	// if no identity provided, return error
	if identity == "" {
		err = ErrNoIdentity
		return
	}

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
		qGt.Owner = identity

		q = qGt
	} else {
		var qId findOwner
		qId.Owner = identity

		q = qId
	}

	it, err := d.coll.Find(ctx, q, options.Find().SetSort(sortById).SetLimit(int64(limit)))
	if err != nil {
		return
	}
	defer func() {
		_ = it.Close(ctx)
	}()
	records = make([]AclEventLogEntry, 0, limit)
	for it.Next(ctx) {
		var rec AclEventLogEntry
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

func (d *aclEventLog) AddLog(ctx context.Context, event AclEventLogEntry) (err error) {
	_, err = d.coll.InsertOne(ctx, event)
	if err != nil {
		return
	}
	return nil
}
