package db

import (
	"context"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

const CName = "coordinator.db"

var log = logger.NewNamed(CName)

type Database interface {
	app.Component
	Db() *mongo.Database
	GetInboxCollection() *mongo.Collection
	Tx(ctx context.Context, f func(txCtx mongo.SessionContext) error) error
}

func New() Database {
	return &database{}
}

type mongoProvider interface {
	GetMongo() Mongo
}

type database struct {
	inboxColl *mongo.Collection
	db        *mongo.Database
}

func (d *database) Init(a *app.App) (err error) {
	conf := a.MustComponent("config").(mongoProvider).GetMongo()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(conf.Connect))
	if err != nil {
		return err
	}
	d.db = client.Database(conf.Database)
	d.inboxColl = client.Database(conf.Database).Collection("tmp-inbox-coll")
	return
}

func (d *database) Name() (name string) {
	return CName
}

func (d *database) Db() *mongo.Database {
	return d.db
}

func (d *database) Tx(ctx context.Context, f func(txCtx mongo.SessionContext) error) error {
	client := d.db.Client()
	return client.UseSessionWithOptions(
		ctx,
		options.Session().SetDefaultReadConcern(readconcern.Majority()),
		func(txCtx mongo.SessionContext) error {
			if err := txCtx.StartTransaction(); err != nil {
				return err
			}

			if err := f(txCtx); err != nil {
				// Abort the transaction after an error. Use
				// context.Background() to ensure that the abort can complete
				// successfully even if the context passed to mongo.WithSession
				// is changed to have a timeout.
				_ = txCtx.AbortTransaction(context.Background())
				return err
			}

			// Use context.Background() to ensure that the commit can complete
			// successfully even if the context passed to mongo.WithSession is
			// changed to have a timeout.
			return txCtx.CommitTransaction(context.Background())
		})
}

func (d *database) GetInboxCollection() *mongo.Collection {
	fmt.Printf("inbox coll: %#v\n", d.inboxColl)
	return d.inboxColl
}
