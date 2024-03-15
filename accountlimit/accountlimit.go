package accountlimit

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/anyproto/any-sync-coordinator/db"
)

func New() AccountLimit {
	return &accountLimit{}
}

const CName = "coordinator.accountLimit"

const collName = "accountLimit"

var log = logger.NewNamed(CName)

type configGetter interface {
	GetAccountLimit() Limits
}

type Limits struct {
	Identity          string    `bson:"_id"`
	Reason            string    `bson:"reason"`
	SpaceMembersRead  uint32    `bson:"spaceMembersRead" yaml:"spaceMembersRead"`
	SpaceMembersWrite uint32    `bson:"spaceMembersWrite" yaml:"spaceMembersWrite"`
	UpdatedTime       time.Time `bson:"updatedTime"`
}

type AccountLimit interface {
	SetLimits(ctx context.Context, limits Limits) (err error)
	GetLimits(ctx context.Context, identity string) (limits Limits, err error)
	app.Component
}
type accountLimit struct {
	coll          *mongo.Collection
	defaultLimits Limits
}

func (al *accountLimit) Init(a *app.App) (err error) {
	al.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	al.defaultLimits = a.MustComponent("config").(configGetter).GetAccountLimit()
	return nil
}

func (al *accountLimit) Name() (name string) {
	return CName
}

func (al *accountLimit) SetLimits(ctx context.Context, limits Limits) (err error) {
	limits.UpdatedTime = time.Now()
	_, err = al.coll.UpdateOne(
		ctx,
		bson.D{{"_id", limits.Identity}},
		bson.D{{"$set", limits}},
		options.Update().SetUpsert(true),
	)
	return
}

func (al *accountLimit) GetLimits(ctx context.Context, identity string) (limits Limits, err error) {
	err = al.coll.FindOne(ctx, bson.D{{"_id", identity}}).Decode(&limits)
	if err == nil || !errors.Is(err, mongo.ErrNoDocuments) {
		return
	}
	// default limit
	return Limits{
		Identity:          identity,
		SpaceMembersRead:  al.defaultLimits.SpaceMembersRead,
		SpaceMembersWrite: al.defaultLimits.SpaceMembersWrite,
		UpdatedTime:       time.Now(),
	}, nil
}
