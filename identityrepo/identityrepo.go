package identityrepo

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/identityrepo/identityrepoproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/anyproto/any-sync-coordinator/db"
)

const CName = "coordinator.identityrepo"

const collName = "identityRepo"

var log = logger.NewNamed(CName)

var (
	ErrInvalidIdentity  = errors.New("invalid identity")
	ErrInvalidSignature = errors.New("invalid signature")

	ErrThresholdReached = errors.New("threshold reached")
)

const (
	thresholdDataLen        = 65 * 1024 // 65 kb
	thresholdIdentityPerReq = 350
)

func New() IdentityRepo {
	return new(identityRepo)
}

type IdentityRepo interface {
	app.Component
}

type identityRepo struct {
	coll    *mongo.Collection
	handler *rpcHandler
}

func (i *identityRepo) Init(a *app.App) (err error) {
	i.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	i.handler = &rpcHandler{repo: i}
	return identityrepoproto.DRPCRegisterIdentityRepo(a.MustComponent(server.CName).(server.DRPCServer), i.handler)
}

func (i *identityRepo) Name() (name string) {
	return CName
}

func (i *identityRepo) Push(ctx context.Context, identity string, data []*identityrepoproto.Data) (err error) {
	if err = i.validateRequest(ctx, identity, data); err != nil {
		return
	}
	upd := bson.M{}
	for _, d := range data {
		upd["data."+d.Kind] = Data{
			Data:      d.Data,
			Signature: d.Signature,
		}
	}
	upd["updated"] = time.Now()
	_, err = i.coll.UpdateOne(ctx, byId{identity}, bson.M{"$set": upd}, options.Update().SetUpsert(true))
	return
}

type byIdIn struct {
	Id struct {
		Ids []string `bson:"$in"`
	} `bson:"_id"`
}

func (i *identityRepo) Pull(ctx context.Context, identities, kinds []string) (res []*identityrepoproto.DataWithIdentity, err error) {
	if len(identities) > thresholdIdentityPerReq {
		return nil, ErrThresholdReached
	}

	var in byIdIn
	in.Id.Ids = identities
	cur, err := i.coll.Find(ctx, in)
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
		data := make([]*identityrepoproto.Data, 0, len(e.Data))
		for _, kind := range kinds {
			if v, ok := e.Data[kind]; ok {
				data = append(data, &identityrepoproto.Data{
					Kind:      kind,
					Data:      v.Data,
					Signature: v.Signature,
				})
			}
		}
		res = append(res, &identityrepoproto.DataWithIdentity{
			Identity: e.Id,
			Data:     data,
		})
	}
	if cur.Err() != nil {
		return nil, cur.Err()
	}
	return
}

func (i *identityRepo) Delete(ctx context.Context, identity string, kinds ...string) (err error) {
	if err = i.validateRequest(ctx, identity, nil); err != nil {
		return
	}
	if len(kinds) == 0 {
		if _, err = i.coll.DeleteOne(ctx, byId{identity}); err != nil {
			return
		}
		return
	}

	unset := bson.M{}
	for _, kind := range kinds {
		unset["data."+kind] = 1
	}

	upd := bson.M{
		"$set": bson.M{
			"updated": time.Now(),
		},
		"$unset": unset,
	}

	_, err = i.coll.UpdateOne(ctx, byId{identity}, upd)
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

func (i *identityRepo) validateRequest(ctx context.Context, identity string, data []*identityrepoproto.Data) error {
	pubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return err
	}
	if pubKey.Account() != identity {
		return ErrInvalidIdentity
	}

	for _, d := range data {
		if len(d.Data) > thresholdDataLen {
			return ErrThresholdReached
		}
		ok, verr := pubKey.Verify(d.Data, d.Signature)
		if verr != nil {
			return verr
		}
		if !ok {
			return ErrInvalidSignature
		}
	}
	return nil
}
