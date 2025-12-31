//go:generate mockgen -destination mock_accountlimit/mock_accountlimit.go github.com/anyproto/any-sync-coordinator/accountlimit AccountLimit
package accountlimit

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonfile/fileproto"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/anyproto/any-sync/nodeconf"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
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
	SharedSpacesLimit uint32 `yaml:"sharedSpacesLimit" bson:"sharedSpacesLimit"`
}

type Limits struct {
	Identity          string    `bson:"_id"`
	Reason            string    `bson:"reason"`
	FileStorageBytes  uint64    `bson:"fileStorageBytes"`
	SpaceMembersRead  uint32    `bson:"spaceMembersRead"`
	SpaceMembersWrite uint32    `bson:"spaceMembersWrite"`
	SharedSpacesLimit uint32    `bson:"sharedSpacesLimit"`
	UpdatedTime       time.Time `bson:"updatedTime"`
}

type AccountLimit interface {
	SetLimits(ctx context.Context, limits Limits) (err error)
	GetLimits(ctx context.Context, identity string) (limits Limits, err error)
	GetLimitsBySpace(ctx context.Context, spaceId string) (limits SpaceLimits, err error)
	app.Component
}
type accountLimit struct {
	pool          pool.Pool
	nodeConf      nodeconf.Service
	coll          *mongo.Collection
	spaceStatus   spacestatus.SpaceStatus
	defaultLimits SpaceLimits
}

func (al *accountLimit) Init(a *app.App) (err error) {
	al.pool = a.MustComponent(pool.CName).(pool.Pool)
	al.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	al.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	al.spaceStatus = a.MustComponent(spacestatus.CName).(spacestatus.SpaceStatus)
	al.defaultLimits = a.MustComponent("config").(configGetter).GetAccountLimit()
	return nil
}

func (al *accountLimit) Name() (name string) {
	return CName
}

func (al *accountLimit) SetLimits(ctx context.Context, limits Limits) (err error) {
	limits.UpdatedTime = time.Now()

	if err = al.updateFileLimits(ctx, limits); err != nil {
		return
	}

	return al.updateDbLimits(ctx, limits)
}

func (al *accountLimit) updateDbLimits(ctx context.Context, limits Limits) (err error) {
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
		if err != nil {
			return errors.Join(rpcerr.Unwrap(err), err)
		}
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
		SharedSpacesLimit: al.defaultLimits.SharedSpacesLimit,
		UpdatedTime:       time.Now(),
	}, nil
}

func (al *accountLimit) GetLimitsBySpace(ctx context.Context, spaceId string) (sLimits SpaceLimits, err error) {
	entry, err := al.spaceStatus.Status(ctx, spaceId)
	if err != nil {
		return
	}

	// return 1-1 to personal and tech spaces
	switch entry.Type {
	case spacestatus.SpaceTypePersonal, spacestatus.SpaceTypeTech:
		return SpaceLimits{
			SpaceMembersRead:  1,
			SpaceMembersWrite: 1,
		}, nil
	case spacestatus.SpaceTypeOneToOne:
		return SpaceLimits{
			SharedSpacesLimit: 3000,
		}, nil
	}

	limits, err := al.GetLimits(ctx, entry.Identity)
	if err != nil {
		return
	}
	return SpaceLimits{
		SpaceMembersRead:  limits.SpaceMembersRead,
		SpaceMembersWrite: limits.SpaceMembersWrite,
		SharedSpacesLimit: limits.SharedSpacesLimit,
	}, nil
}
