package identityrepo

import (
	"context"
	"errors"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/net/peer"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const CName = "coordinator.identityrepo"

const collName = "identityRepo"

var log = logger.NewNamed(CName)

var (
	ErrInvalidIdentity  = errors.New("invalid identity")
	ErrInvalidSignature = errors.New("invalid signature")
)

func New() IdentityRepo {
	return new(identityRepo)
}

type IdentityRepo interface {
	app.Component
}

type identityRepo struct {
	coll *mongo.Collection
}

func (i *identityRepo) Init(a *app.App) (err error) {
	i.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	return
}

func (i *identityRepo) Name() (name string) {
	return CName
}

func (i *identityRepo) Push(ctx context.Context, e Entry) (err error) {
	if err = i.validateRequest(ctx, e.Id, e); err != nil {
		return
	}
	entry, err := i.fetchOne(ctx, e.Id)
	if err != nil {
		return err
	}
	for kind, data := range e.Data {
		entry.Data[kind] = data
	}
	upd := bson.M{"$set": bson.M{
		"data":    entry.Data,
		"updated": time.Now(),
	}}
	_, err = i.coll.UpdateOne(ctx, byId{entry.Id}, upd, options.Update().SetUpsert(true))
	return
}

type byIdIn struct {
	Ids []string `bson:"_id.$in"`
}

func (i *identityRepo) Pull(ctx context.Context, identities, kinds []string) (entries []Entry, err error) {
	cur, err := i.coll.Find(ctx, byIdIn{identities})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = cur.Close(ctx)
	}()
	for cur.Next(ctx) {
		var e Entry
		if err = cur.Decode(&e); err != nil {
			return nil, err
		}
		entries = append(entries, e.Kinds(kinds))
	}
	if cur.Err() != nil {
		return nil, cur.Err()
	}
	return entries, nil
}

func (i *identityRepo) Delete(ctx context.Context, identity string, kinds ...string) (err error) {
	if err = i.validateRequest(ctx, identity, Entry{}); err != nil {
		return
	}
	if len(kinds) == 0 {
		if _, err = i.coll.DeleteOne(ctx, byId{identity}); err != nil {
			return
		}
		return
	}

	entry, err := i.fetchOne(ctx, identity)
	if err != nil {
		return err
	}
	for _, kind := range kinds {
		delete(entry.Data, kind)
	}
	upd := bson.M{"$set": bson.M{
		"data":    entry.Data,
		"updated": time.Now(),
	}}
	_, err = i.coll.UpdateOne(ctx, byId{entry.Id}, upd, options.Update().SetUpsert(true))
	return
}

type byId struct {
	Id string `bson:"_id"`
}

func (i *identityRepo) fetchOne(ctx context.Context, id string) (e Entry, err error) {
	if err = i.coll.FindOne(ctx, byId{id}).Decode(&e); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return Entry{
				Id: id,
			}, nil
		}
		return
	}
	return e, nil
}

func (i *identityRepo) validateRequest(ctx context.Context, identity string, e Entry) error {
	pubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return err
	}
	if pubKey.Account() != identity {
		return ErrInvalidIdentity
	}
	if e.Id != "" {
		if e.Id != identity {
			return ErrInvalidIdentity
		}
		for _, data := range e.Data {
			ok, verr := pubKey.Verify(data.Data, data.Signature)
			if verr != nil {
				return verr
			}
			if !ok {
				return ErrInvalidSignature
			}
		}
	}
	return nil
}
