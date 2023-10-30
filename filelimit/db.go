package filelimit

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/anyproto/any-sync-coordinator/db"
)

const collName = "fileLimit"

var ErrNotFound = errors.New("fileLimit not found")

func newDb(db db.Database) *fileLimitDb {
	return &fileLimitDb{
		coll: db.Db().Collection(collName),
	}
}

type Limit struct {
	Key         string `bson:"_id"`
	Limit       uint64 `bson:"limit"`
	UpdatedTime int64  `bson:"updatedTime"`
}

type fileLimitDb struct {
	coll *mongo.Collection
}

type byIdFilter struct {
	SpaceId string `bson:"_id"`
}

func (f *fileLimitDb) Get(ctx context.Context, key string) (limit uint64, err error) {
	var res Limit
	if err = f.coll.FindOne(ctx, byIdFilter{key}).Decode(&res); err != nil {
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

func (f *fileLimitDb) Set(ctx context.Context, key string, limit uint64) (err error) {
	var obj = Limit{
		Key:         key,
		Limit:       limit,
		UpdatedTime: time.Now().Unix(),
	}
	opts := options.Update().SetUpsert(true)
	if _, err = f.coll.UpdateOne(ctx, byIdFilter{key}, upd{obj}, opts); err != nil {
		return
	}
	return
}
