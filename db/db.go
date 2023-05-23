package db

import (
	"context"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const CName = "coordinator.db"

var log = logger.NewNamed(CName)

type Database interface {
	app.Component
	Db() *mongo.Database
}

func New() Database {
	return &database{}
}

type mongoProvider interface {
	GetMongo() Mongo
}

type database struct {
	db *mongo.Database
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
	return
}

func (d *database) Name() (name string) {
	return CName
}

func (d *database) Db() *mongo.Database {
	return d.db
}
