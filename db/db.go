package db

import (
	"context"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/coordinator/coordinatorproto"
	"github.com/anytypeio/any-sync/util/strkey"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const CName = "coordinator.db"

var log = logger.NewNamed(CName)

type Database interface {
	app.ComponentRunnable
	SpacesCollection() *mongo.Collection
	LogCollection() *mongo.Collection
}

func New() Database {
	return &database{}
}

type mongoProvider interface {
	GetMongo() Mongo
}

type database struct {
	conf   Mongo
	spaces *mongo.Collection
	log    *mongo.Collection
}

func (d *database) Init(a *app.App) (err error) {
	d.conf = a.MustComponent("config").(mongoProvider).GetMongo()
	return
}

func (d *database) Name() (name string) {
	return CName
}

func (d *database) Run(ctx context.Context) (err error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(d.conf.Connect))
	if err != nil {
		return err
	}
	db := d.conf.Database
	d.spaces = client.Database(db).Collection(d.conf.SpacesCollection)
	d.log = client.Database(db).Collection(d.conf.LogCollection)
	return
}

func (d *database) Close(ctx context.Context) (err error) {
	return nil
}

func (d *database) SpacesCollection() *mongo.Collection {
	return d.spaces
}

func (d *database) LogCollection() *mongo.Collection {
	return d.log
}

func EncodeIdentity(identity []byte) (stringId string, err error) {
	// TODO: maybe move to any-sync
	encodedIdentity, err := strkey.Encode(strkey.AccountAddressVersionByte, identity)
	if err != nil {
		log.Debug("failed to encode payload", zap.Error(err))
		return "", coordinatorproto.ErrUnexpected
	}
	return encodedIdentity, nil
}
