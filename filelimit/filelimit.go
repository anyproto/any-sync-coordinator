package filelimit

import (
	"context"
	"errors"
	"github.com/anytypeio/any-sync-coordinator/db"
	"github.com/anytypeio/any-sync/app"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const CName = "coordinator.filelimit"

const collName = "fileLimit"

var ErrNotFound = errors.New("fileLimit not found")

func New() FileLimit {
	return new(fileLimit)
}

type FileLimit interface {
	Get(ctx context.Context, spaceId string) (limit uint64, err error)
	Set(ctx context.Context, spaceId string, limit uint64) (err error)
	app.Component
}

type Limit struct {
	SpaceId     string `bson:"_id"`
	Limit       uint64 `bson:"limit"`
	UpdatedTime int64  `bson:"updatedTime"`
}

type fileLimit struct {
	coll *mongo.Collection
}

func (f *fileLimit) Init(a *app.App) (err error) {
	f.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	return
}

func (f *fileLimit) Name() (name string) {
	return CName
}

type byIdFilter struct {
	SpaceId string `bson:"_id"`
}

func (f *fileLimit) Get(ctx context.Context, spaceId string) (limit uint64, err error) {
	var res Limit
	if err = f.coll.FindOne(ctx, byIdFilter{spaceId}).Decode(&res); err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, ErrNotFound
		}
		return
	}
	return res.Limit, nil
}

type upd struct {
	Lim Limit `bson:"$set"`
}

func (f *fileLimit) Set(ctx context.Context, spaceId string, limit uint64) (err error) {
	var obj = Limit{
		SpaceId:     spaceId,
		Limit:       limit,
		UpdatedTime: time.Now().Unix(),
	}
	opts := options.Update().SetUpsert(true)
	if _, err = f.coll.UpdateOne(ctx, byIdFilter{spaceId}, upd{obj}, opts); err != nil {
		return
	}
	return
}
