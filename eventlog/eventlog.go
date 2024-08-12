//go:generate mockgen -destination mock_eventlog/mock_eventlog.go github.com/anyproto/any-sync-coordinator/eventlog EventLog
package eventlog

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

const CName = "coordinator.eventLog"

var log = logger.NewNamed(CName)

const (
	collName     = "eventLog"
	defaultLimit = 1000
)

var (
	ErrNoIdentity = errors.New("no identity")
)

func New() EventLog {
	return new(eventLog)
}

type EventLogEntryType uint8

const (
	EntryTypeSpaceReceipt          EventLogEntryType = 0
	EntryTypeSpaceSharedOrUnshared EventLogEntryType = 1
	EntryTypeSpaceAclAddRecord     EventLogEntryType = 2
)

type EventLogEntry struct {
	Id        *primitive.ObjectID `bson:"_id,omitempty"`
	SpaceId   string              `bson:"spaceId"`
	PeerId    string              `bson:"peerId"`
	Identity  string              `bson:"identity"`
	Timestamp int64               `bson:"timestamp"`

	EntryType EventLogEntryType `bson:"entryType"`
	// only for EntryTypeSpaceReceipt
	SignedSpaceReceipt []byte `bson:"receipt"`
	// only for EntryTypeSpaceAclAddRecord
	AclChangeId string `bson:"aclChangeId"`
}

/*
	type findIdGt struct {
		Id struct {
			Gt primitive.ObjectID `bson:"$gt"`
		} `bson:"_id"`
	}
*/

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

type EventLog interface {
	AddLog(ctx context.Context, event EventLogEntry) (err error)
	GetAfter(ctx context.Context, identity, afterId string, limit uint32) (records []EventLogEntry, hasMore bool, err error)

	app.ComponentRunnable
}

type eventLog struct {
	coll *mongo.Collection
}

func (d *eventLog) Init(a *app.App) (err error) {
	d.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	return
}

func (d *eventLog) Name() (name string) {
	return CName
}

func (d *eventLog) Run(ctx context.Context) error {
	// create collection if doesn't exist
	_ = d.coll.Database().CreateCollection(ctx, collName)
	return nil
}

func (d *eventLog) Close(_ context.Context) (err error) {
	return nil
}

func (d *eventLog) GetAfter(ctx context.Context, identity string, afterId string, limit uint32) (records []EventLogEntry, hasMore bool, err error) {
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
		qGt.Identity = identity

		q = qGt
	} else {
		var qId findIdentity
		qId.Identity = identity

		q = qId
	}

	it, err := d.coll.Find(ctx, q, options.Find().SetSort(sortById).SetLimit(int64(limit)))
	if err != nil {
		return
	}
	defer func() {
		_ = it.Close(ctx)
	}()
	records = make([]EventLogEntry, 0, limit)
	for it.Next(ctx) {
		var rec EventLogEntry
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

func (d *eventLog) AddLog(ctx context.Context, event EventLogEntry) (err error) {
	_, err = d.coll.InsertOne(ctx, event)
	if err != nil {
		return
	}
	return nil
}
