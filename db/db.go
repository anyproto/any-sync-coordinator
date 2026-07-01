package db

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

// transientTransactionErrorLabel is attached by the mongo driver to errors
// that are safe to retry by re-running the whole transaction (e.g. a
// WriteConflict raised on a write inside a transaction). WithTransaction
// retries on this label automatically.
const transientTransactionErrorLabel = "TransientTransactionError"

// IsTransientTransactionError reports whether err is a transient mongo
// transaction error (such as a WriteConflict) that Tx/WithTransaction will
// retry. Callers that translate mongo errors into their own sentinel errors
// must propagate such errors unchanged, otherwise the retry cannot happen
// (see SYN-38).
func IsTransientTransactionError(err error) bool {
	if err == nil {
		return false
	}
	// Both mongo.CommandError and mongo.WriteException expose HasErrorLabel.
	var labeler interface{ HasErrorLabel(string) bool }
	if errors.As(err, &labeler) {
		return labeler.HasErrorLabel(transientTransactionErrorLabel)
	}
	return false
}

const CName = "coordinator.db"

var log = logger.NewNamed(CName)

type Database interface {
	app.Component
	Db() *mongo.Database
	Tx(ctx context.Context, f func(txCtx mongo.SessionContext) error) error
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

func (d *database) Tx(ctx context.Context, f func(txCtx mongo.SessionContext) error) error {
	client := d.db.Client()
	return client.UseSessionWithOptions(
		ctx,
		options.Session().SetDefaultReadConcern(readconcern.Majority()),
		func(sessCtx mongo.SessionContext) error {
			// WithTransaction transparently retries the whole callback on
			// transient errors (e.g. mongo WriteConflict, labeled
			// TransientTransactionError) and retries the commit on
			// UnknownTransactionCommitResult, with backoff and a safety
			// deadline. This is the mongo-recommended retry loop; without it
			// a WriteConflict under concurrency fails the caller instead of
			// being retried (see SYN-38).
			_, err := sessCtx.WithTransaction(ctx, func(txCtx mongo.SessionContext) (interface{}, error) {
				return nil, f(txCtx)
			})
			return err
		})
}
