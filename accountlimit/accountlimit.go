package accountlimit

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonfile/fileproto"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/nodeconf"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-coordinator/db"
)

func New() AccountLimit {
	return &accountLimit{}
}

const CName = "coordinator.accountLimit"

const collName = "accountLimit"

var log = logger.NewNamed(CName)

type configGetter interface {
	GetAccountLimit() SpaceLimits
}

type SpaceLimits struct {
	SpaceMembersRead  uint32 `yaml:"spaceMembersRead" bson:"spaceMembersRead"`
	SpaceMembersWrite uint32 `yaml:"spaceMembersWrite" bson:"spaceMembersWrite"`
}

type Limits struct {
	Identity          string    `bson:"_id"`
	Reason            string    `bson:"reason"`
	FileStorageBytes  uint64    `bson:"fileStorageBytes"`
	SpaceMembersRead  uint32    `bson:"spaceMembersRead"`
	SpaceMembersWrite uint32    `bson:"spaceMembersWrite"`
	UpdatedTime       time.Time `bson:"updatedTime"`
}

type AccountLimit interface {
	SetLimits(ctx context.Context, limits Limits) (err error)
	GetLimits(ctx context.Context, identity string) (limits Limits, err error)
	app.Component
}
type accountLimit struct {
	pool          pool.Pool
	nodeConf      nodeconf.Service
	coll          *mongo.Collection
	defaultLimits SpaceLimits
}

func (al *accountLimit) Init(a *app.App) (err error) {
	al.pool = a.MustComponent(pool.CName).(pool.Pool)
	al.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	al.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	al.defaultLimits = a.MustComponent("config").(configGetter).GetAccountLimit()
	return nil
}

func (al *accountLimit) Name() (name string) {
	return CName
}

func (al *accountLimit) SetLimits(ctx context.Context, limits Limits) (err error) {
	if err = al.updateFileLimits(ctx, limits); err != nil {
		return
	}

	limits.UpdatedTime = time.Now()
	_, err = al.coll.UpdateOne(
		ctx,
		bson.D{{"_id", limits.Identity}},
		bson.D{{"$set", limits}},
		options.Update().SetUpsert(true),
	)
	return
}

func (al *accountLimit) updateFileLimits(ctx context.Context, limits Limits) (err error) {
	filePeer, err := al.pool.GetOneOf(ctx, al.nodeConf.FilePeers())
	if err != nil {
		return
	}
	return filePeer.DoDrpc(ctx, func(conn drpc.Conn) error {
		_, err := fileproto.NewDRPCFileClient(conn).AccountLimitSet(ctx, &fileproto.AccountLimitSetRequest{
			Identity: limits.Identity,
			Limit:    limits.FileStorageBytes,
		})
		return err
	})
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
