//go:generate mockgen -destination mock_fileusage/mock_fileusage.go github.com/anyproto/any-sync-coordinator/fileusage FileUsage

// Package fileusage owns the files-v2 storage-limits state (SYN-19): fileV2
// brokers push absolute per-(space, identity) durable usage rows and pull the
// account-pool bounds back to authorize uploads locally. The identity total
// is aggregated on read; in-flight usage never reaches the coordinator.
package fileusage

import (
	"context"
	"slices"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/nodeconf"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

const CName = "coordinator.fileUsage"

const collName = "fileUsage"

// reportTTL bounds how long a node report can drive the MAX aggregation
// without a refresh. Expired reports fall back to the newest one regardless
// of age (retain-last-accepted), so the TTL only arbitrates between diverging
// live nodes — expiry never zeroes usage.
const reportTTL = time.Hour

var log = logger.NewNamed(CName)

func New() FileUsage {
	return &fileUsage{}
}

type FileUsage interface {
	// Report stores absolute per-(space, identity) durable usage rows reported
	// by nodeId. Rows for spaces nodeId is not responsible for under the
	// current chash configuration are dropped: membership is enforced here,
	// not trusted from the reporter, so a node with a stale nodeconf can never
	// pin a resharded space's usage.
	Report(ctx context.Context, nodeId string, rows []*coordinatorproto.FileUsageRow) error
	// GetLimits returns the absolute account-pool bounds for an uploader
	// identity: the cap, the aggregated durable usage across all the
	// identity's spaces, and the space-scoped cap (0 = unset). The caller
	// computes effective headroom locally.
	GetLimits(ctx context.Context, spaceId, identity string) (*coordinatorproto.FileLimitsGetResponse, error)
	app.ComponentRunnable
}

// nodeReport is one node's last absolute report for a (space, identity)
// slice. Sizes are int64 in bson (mongo has no uint64); file sizes fit.
type nodeReport struct {
	NodeId    string    `bson:"nodeId"`
	Bytes     int64     `bson:"bytes"`
	Files     int64     `bson:"files"`
	UpdatedAt time.Time `bson:"updatedAt"`
}

type usageDoc struct {
	Id       string       `bson:"_id"` // spaceId + "/" + identity
	SpaceId  string       `bson:"spaceId"`
	Identity string       `bson:"identity"`
	PerNode  []nodeReport `bson:"perNode"`
}

type fileUsage struct {
	coll         *mongo.Collection
	nodeConf     nodeconf.Service
	accountLimit accountlimit.AccountLimit
}

func (f *fileUsage) Name() string { return CName }

func (f *fileUsage) Init(a *app.App) error {
	f.coll = a.MustComponent(db.CName).(db.Database).Db().Collection(collName)
	f.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	f.accountLimit = app.MustComponent[accountlimit.AccountLimit](a)
	// deleted spaces must stop counting toward their uploaders' pools
	// immediately, not when the brokers' reports fade out
	app.MustComponent[spacestatus.SpaceStatus](a).RegisterSpaceRemoveHook(f.removeSpaceTx)
	return nil
}

func (f *fileUsage) Run(ctx context.Context) error {
	_ = f.coll.Database().CreateCollection(ctx, collName)
	_, err := f.coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "identity", Value: 1}}},
		{Keys: bson.D{{Key: "spaceId", Value: 1}}},
	})
	return err
}

func (f *fileUsage) Report(ctx context.Context, nodeId string, rows []*coordinatorproto.FileUsageRow) error {
	now := time.Now()
	models := make([]mongo.WriteModel, 0, len(rows)*2)
	var dropped int
	for _, row := range rows {
		if !slices.Contains(f.nodeConf.FileV2NodeIds(row.SpaceId), nodeId) {
			dropped++
			continue
		}
		id := row.SpaceId + "/" + row.Identity
		entry := nodeReport{
			NodeId:    nodeId,
			Bytes:     int64(row.DurableBytes),
			Files:     int64(row.DurableFilesCount),
			UpdatedAt: now,
		}
		// replace-by-node as pull+push: ordered bulk keeps the pair atomic enough
		// (reports are absolute and idempotent, a torn pair is healed by the next one)
		models = append(models,
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": id}).
				SetUpdate(bson.M{"$pull": bson.M{"perNode": bson.M{"nodeId": nodeId}}}),
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": id}).
				SetUpdate(bson.M{
					"$push": bson.M{"perNode": entry},
					"$set":  bson.M{"spaceId": row.SpaceId, "identity": row.Identity},
				}).
				SetUpsert(true),
		)
	}
	if dropped > 0 {
		log.Info("dropped usage rows from a non-responsible node",
			zap.String("nodeId", nodeId), zap.Int("rows", dropped))
	}
	if len(models) == 0 {
		return nil
	}
	_, err := f.coll.BulkWrite(ctx, models)
	return err
}

func (f *fileUsage) GetLimits(ctx context.Context, spaceId, identity string) (*coordinatorproto.FileLimitsGetResponse, error) {
	limits, err := f.accountLimit.GetLimits(ctx, identity)
	if err != nil {
		return nil, err
	}
	total, err := f.accountTotalUsage(ctx, identity)
	if err != nil {
		return nil, err
	}
	_ = spaceId // per-space caps are reserved: spaceLimitBytes stays 0 until they exist
	return &coordinatorproto.FileLimitsGetResponse{
		AccountLimitBytes:      limits.FileStorageBytes,
		AccountTotalUsageBytes: total,
		SpaceLimitBytes:        0,
	}, nil
}

func (f *fileUsage) accountTotalUsage(ctx context.Context, identity string) (total uint64, err error) {
	cur, err := f.coll.Find(ctx, bson.M{"identity": identity})
	if err != nil {
		return
	}
	defer func() {
		_ = cur.Close(ctx)
	}()
	now := time.Now()
	for cur.Next(ctx) {
		var doc usageDoc
		if err = cur.Decode(&doc); err != nil {
			return
		}
		total += effectiveBytes(doc, f.nodeConf.FileV2NodeIds(doc.SpaceId), now)
	}
	return total, cur.Err()
}

// effectiveBytes reconciles one slice's per-node reports: MAX over unexpired
// reports from the currently responsible pair (over-count, never under-count
// — a lagging node can't unlock headroom a fresher one already charged).
// When none qualify (resharding window before the new pair reports, long-idle
// space) the newest report is retained regardless of age or membership: a
// reshard must never make an identity's usage vanish.
func effectiveBytes(doc usageDoc, memberIds []string, now time.Time) uint64 {
	var best int64
	var found bool
	var newest nodeReport
	for _, r := range doc.PerNode {
		if r.UpdatedAt.After(newest.UpdatedAt) {
			newest = r
		}
		if now.Sub(r.UpdatedAt) < reportTTL && slices.Contains(memberIds, r.NodeId) {
			found = true
			if r.Bytes > best {
				best = r.Bytes
			}
		}
	}
	if !found {
		best = newest.Bytes
	}
	if best < 0 {
		return 0
	}
	return uint64(best)
}

func (f *fileUsage) removeSpaceTx(txCtx mongo.SessionContext, spaceId string) error {
	_, err := f.coll.DeleteMany(txCtx, bson.M{"spaceId": spaceId})
	return err
}

func (f *fileUsage) Close(_ context.Context) error { return nil }
