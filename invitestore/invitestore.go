//go:generate mockgen -destination mock_invitestore/mock_invitestore.go github.com/anyproto/any-sync-coordinator/invitestore InviteStore
package invitestore

import (
	"bytes"
	"context"
	"errors"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/anyproto/any-sync-coordinator/db"
)

const CName = "coordinator.invitestore"

var log = logger.NewNamed(CName)

const collName = "invites"

var (
	// ErrCidBound is returned when the cid is already bound to a different space or invite.
	ErrCidBound = errors.New("invite cid is already bound")
	ErrNotFound = errors.New("invite binding not found")
)

func New() InviteStore {
	return new(inviteStore)
}

// Entry binds an invite file stored on the filenodes to the acl invite it belongs to.
// InvitePubKey is the public part of the invite key, so the binding never carries anything
// that would let the coordinator read the invite file itself.
type Entry struct {
	Cid          string `bson:"_id"`
	SpaceId      string `bson:"spaceId"`
	InvitePubKey []byte `bson:"invitePubKey"`
}

type InviteStore interface {
	// Bind is insert-only: a cid keeps the space and invite key it was first bound to.
	// Rebinding the same cid to the same invite is a no-op, so upload retries are safe.
	Bind(ctx context.Context, entry Entry) (err error)
	// Get returns ErrNotFound for an invite uploaded before the binding existed.
	Get(ctx context.Context, cid string) (entry Entry, err error)
	Delete(ctx context.Context, cid string) (err error)
	app.ComponentRunnable
}

type inviteStore struct {
	coll *mongo.Collection
}

func (i *inviteStore) Init(a *app.App) (err error) {
	i.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	return
}

func (i *inviteStore) Name() (name string) {
	return CName
}

func (i *inviteStore) Run(ctx context.Context) error {
	// the binding is looked up only by cid, which is the _id, so no extra index is needed
	_ = i.coll.Database().CreateCollection(ctx, collName)
	return nil
}

func (i *inviteStore) Bind(ctx context.Context, entry Entry) (err error) {
	_, err = i.coll.InsertOne(ctx, entry)
	if err == nil {
		return nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return err
	}
	// the cid is taken: only the very same binding may claim it again
	existing, err := i.Get(ctx, entry.Cid)
	if err != nil {
		return err
	}
	if existing.SpaceId != entry.SpaceId || !bytes.Equal(existing.InvitePubKey, entry.InvitePubKey) {
		return ErrCidBound
	}
	return nil
}

func (i *inviteStore) Get(ctx context.Context, cid string) (entry Entry, err error) {
	err = i.coll.FindOne(ctx, bson.M{"_id": cid}).Decode(&entry)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Entry{}, ErrNotFound
	}
	return
}

func (i *inviteStore) Delete(ctx context.Context, cid string) (err error) {
	_, err = i.coll.DeleteOne(ctx, bson.M{"_id": cid})
	return
}

func (i *inviteStore) Close(ctx context.Context) (err error) {
	return nil
}
