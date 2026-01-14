package coordinator

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/acl"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/acleventlog"
	"github.com/anyproto/any-sync-coordinator/config"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/deletionlog"
	"github.com/anyproto/any-sync-coordinator/inbox"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync-coordinator/subscribe"
)

var (
	spaceReceiptValidPeriod = time.Hour * 6
)

const (
	defaultFileLimit      uint64 = 1 << 30               // 1 GiB
	oldUsersFileLimit            = defaultFileLimit * 10 // 10 Gb
	nightlyUsersFileLimit        = defaultFileLimit * 50 // 10 Gb
)

const CName = "coordinator.coordinator"

var log = logger.NewNamed(CName)

func New() Coordinator {
	return new(coordinator)
}

type Coordinator interface {
	app.Component
}

type coordinator struct {
	account        *accountdata.AccountKeys
	nodeConf       nodeconf.Service
	spaceStatus    spacestatus.SpaceStatus
	coordinatorLog coordinatorlog.CoordinatorLog
	aclEventLog    acleventlog.AclEventLog
	deletionPeriod time.Duration
	metric         metric.Metric
	deletionLog    deletionlog.DeletionLog
	accountLimit   accountlimit.AccountLimit
	acl            acl.AclService
	inbox          inbox.InboxService
	subscribe      subscribe.SubscribeService
	drpcHandler    *rpcHandler
	pool           pool.Service
}

func (c *coordinator) Init(a *app.App) (err error) {
	c.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	delDays := a.MustComponent(config.CName).(*config.Config).SpaceStatus.DeletionPeriodDays
	c.deletionPeriod = time.Duration(delDays*24) * time.Hour
	c.drpcHandler = &rpcHandler{c: c}
	c.account = a.MustComponent(accountservice.CName).(accountservice.Service).Account()
	c.spaceStatus = a.MustComponent(spacestatus.CName).(spacestatus.SpaceStatus)
	c.coordinatorLog = a.MustComponent(coordinatorlog.CName).(coordinatorlog.CoordinatorLog)
	c.metric = a.MustComponent(metric.CName).(metric.Metric)
	c.subscribe = a.MustComponent(subscribe.CName).(subscribe.SubscribeService)
	c.deletionLog = app.MustComponent[deletionlog.DeletionLog](a)
	c.acl = app.MustComponent[acl.AclService](a)
	c.inbox = app.MustComponent[inbox.InboxService](a)
	c.accountLimit = app.MustComponent[accountlimit.AccountLimit](a)
	c.aclEventLog = app.MustComponent[acleventlog.AclEventLog](a)
	c.pool = a.MustComponent(pool.CName).(pool.Service)
	return coordinatorproto.DRPCRegisterCoordinator(a.MustComponent(server.CName).(drpc.Mux), c.drpcHandler)
}

func (c *coordinator) Name() (name string) {
	return CName
}

func (c *coordinator) StatusCheck(ctx context.Context, spaceId string) (status spacestatus.StatusEntry, err error) {
	defer func() {
		log.Debug("finished checking status", zap.Error(err), zap.String("spaceId", spaceId), zap.Error(err))
	}()
	status, err = c.spaceStatus.Status(ctx, spaceId)
	return
}

func (c *coordinator) AccountDelete(ctx context.Context, payload []byte, payloadId string) (toBeDeleted int64, err error) {
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	return c.spaceStatus.AccountDelete(ctx, spacestatus.AccountDeletion{
		DeletionPayload:   payload,
		DeletionPayloadId: payloadId,
		AccountInfo: spacestatus.AccountInfo{
			Identity:  accountPubKey,
			PeerId:    peerId,
			NetworkId: c.nodeConf.Configuration().NetworkId,
		},
	})
}

func (c *coordinator) AccountRevertDeletion(ctx context.Context) (err error) {
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	return c.spaceStatus.AccountRevertDeletion(ctx, spacestatus.AccountInfo{
		Identity:  accountPubKey,
		PeerId:    peerId,
		NetworkId: c.nodeConf.Configuration().NetworkId,
	})
}

func (c *coordinator) SpaceDelete(ctx context.Context, spaceId string, deletionDurationSecs int64, payload []byte, payloadId string) (toBeDeleted int64, err error) {
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	return c.spaceStatus.SpaceDelete(ctx, spacestatus.SpaceDeletion{
		DeletionPayload:   payload,
		DeletionPayloadId: payloadId,
		SpaceId:           spaceId,
		DeletionPeriod:    time.Duration(deletionDurationSecs) * time.Second,
		AccountInfo: spacestatus.AccountInfo{
			Identity:  accountPubKey,
			PeerId:    peerId,
			NetworkId: c.nodeConf.Configuration().NetworkId,
		},
	})
}

// StatusChange is deprecated use only for backwards compatibility
func (c *coordinator) StatusChange(ctx context.Context, spaceId string, deletionPeriod time.Duration, payloadType coordinatorproto.DeletionPayloadType, payload []byte, payloadId string) (entry spacestatus.StatusEntry, err error) {
	defer func() {
		log.Debug("finished changing status", zap.Error(err), zap.String("spaceId", spaceId), zap.Bool("isDelete", payload != nil))
	}()
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	status := spacestatus.SpaceStatusCreated
	if payload != nil {
		status = spacestatus.SpaceStatusDeletionPending
	}
	return c.spaceStatus.ChangeStatus(ctx, spacestatus.StatusChange{
		DeletionPayloadType: payloadType,
		DeletionPayload:     payload,
		DeletionPayloadId:   payloadId,
		Identity:            accountPubKey,
		Status:              status,
		PeerId:              peerId,
		SpaceId:             spaceId,
		NetworkId:           c.nodeConf.Configuration().NetworkId,
	})
}

func (c *coordinator) SpaceSign(ctx context.Context, spaceId string, spaceHeader []byte, force bool) (signedReceipt *coordinatorproto.SpaceReceiptWithSignature, err error) {
	// TODO: Think about how to make it more evident that account.SignKey is actually a network key
	//  on a coordinator level
	networkKey := c.account.SignKey
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	_, err = spacepayloads.ValidateSpaceHeader(&spacesyncproto.RawSpaceHeaderWithId{RawHeader: spaceHeader, Id: spaceId}, accountPubKey, nil, nil)
	if err != nil {
		return
	}
	spaceType, err := spacestatus.VerifySpaceHeader(accountPubKey, spaceHeader)
	if err != nil {
		return
	}
	err = c.spaceStatus.NewStatus(ctx, spaceId, accountPubKey, spaceType, force)
	if err != nil {
		return
	}
	signedReceipt, err = coordinatorproto.PrepareSpaceReceipt(spaceId, peerId, spaceReceiptValidPeriod, accountPubKey, networkKey)
	if err != nil {
		return
	}
	c.addCoordinatorLog(ctx, spaceId, peerId, accountPubKey, signedReceipt)
	return
}

func (c *coordinator) addCoordinatorLog(ctx context.Context, spaceId, peerId string, accountPubKey crypto.PubKey, signedReceipt *coordinatorproto.SpaceReceiptWithSignature) {
	var err error
	defer func() {
		if err != nil {
			log.Debug("failed to add space receipt log entry", zap.Error(err))
		}
	}()
	marshalledReceipt, err := signedReceipt.MarshalVT()
	if err != nil {
		return
	}
	err = c.coordinatorLog.SpaceReceipt(ctx, coordinatorlog.SpaceReceiptEntry{
		SignedSpaceReceipt: marshalledReceipt,
		SpaceId:            spaceId,
		PeerId:             peerId,
		Identity:           accountPubKey.Account(),
	})
	if err != nil {
		return
	}

	// add to event log too
	err = c.aclEventLog.AddLog(ctx, acleventlog.AclEventLogEntry{
		SpaceId:   spaceId,
		PeerId:    peerId,
		Owner:     accountPubKey.Account(),
		Timestamp: time.Now().Unix(),
		EntryType: acleventlog.EntryTypeSpaceReceipt,
	})
}

func (c *coordinator) AccountLimitsSet(ctx context.Context, req *coordinatorproto.AccountLimitsSetRequest) (err error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	if !slices.Contains(c.nodeConf.NodeTypes(peerId), nodeconf.NodeTypePaymentProcessingNode) {
		return coordinatorproto.ErrForbidden
	}
	return c.accountLimit.SetLimits(ctx, accountlimit.Limits{
		Identity:          req.Identity,
		Reason:            req.Reason,
		FileStorageBytes:  req.FileStorageLimitBytes,
		SpaceMembersRead:  req.SpaceMembersRead,
		SpaceMembersWrite: req.SpaceMembersWrite,
		SharedSpacesLimit: req.SharedSpacesLimit,
	})
}

func (c *coordinator) AclAddRecord(ctx context.Context, spaceId string, payload []byte) (result *consensusproto.RawRecordWithId, err error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return nil, err
	}

	rec := &consensusproto.RawRecord{}
	err = rec.UnmarshalVT(payload)
	if err != nil {
		return
	}

	statusEntry, err := c.spaceStatus.Status(ctx, spaceId)
	if err != nil {
		return
	}

	if statusEntry.Status != spacestatus.SpaceStatusCreated {
		err = coordinatorproto.ErrSpaceIsDeleted
		return
	}

	if !statusEntry.IsShareable {
		err = coordinatorproto.ErrSpaceNotShareable
		return
	}

	limits, err := c.accountLimit.GetLimitsBySpace(ctx, spaceId)
	if err != nil {
		return nil, err
	}

	rawRecordWithId, err := c.acl.AddRecord(ctx, spaceId, rec, acl.Limits{
		ReadMembers:  limits.SpaceMembersRead,
		WriteMembers: limits.SpaceMembersWrite,
	})
	if err != nil {
		if errors.Is(err, acl.ErrLimitExceed) {
			err = coordinatorproto.ErrSpaceLimitReached
		}
		return
	}

	log.Debug("ACL change ID:", zap.String("rawRecordWithId.Id", rawRecordWithId.Id))

	err = c.aclEventLog.AddLog(ctx, acleventlog.AclEventLogEntry{
		SpaceId:     spaceId,
		PeerId:      peerId,
		Owner:       statusEntry.Identity,
		Timestamp:   time.Now().Unix(),
		EntryType:   acleventlog.EntryTypeSpaceAclAddRecord,
		AclChangeId: rawRecordWithId.Id,
	})
	if err != nil {
		return
	}

	ownerPubKey, err := c.acl.OwnerPubKey(ctx, spaceId)
	if err != nil {
		return
	}
	if ownerPubKey.Account() != statusEntry.Identity {
		log.InfoCtx(ctx, "owner change detected", zap.String("spaceId", spaceId), zap.String("newOwner", ownerPubKey.Account()), zap.String("oldOwner", statusEntry.Identity))
		if err = c.spaceStatus.ChangeOwner(ctx, spaceId, ownerPubKey.Account()); err != nil {
			return
		}
		if _, err = c.deletionLog.AddOwnershipChange(ctx, spaceId, statusEntry.Identity, rawRecordWithId.Id); err != nil {
			return
		}
	}

	return rawRecordWithId, nil
}

func (c *coordinator) AclGetRecords(ctx context.Context, spaceId, aclHead string) (result []*consensusproto.RawRecordWithId, err error) {
	statusEntry, err := c.spaceStatus.Status(ctx, spaceId)
	if err != nil {
		return
	}

	if statusEntry.Status != spacestatus.SpaceStatusCreated {
		err = coordinatorproto.ErrSpaceIsDeleted
		return
	}

	return c.acl.RecordsAfter(ctx, spaceId, aclHead)
}

func (c *coordinator) MakeSpaceShareable(ctx context.Context, spaceId string) (err error) {
	pubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return coordinatorproto.ErrForbidden
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	statusEntry, err := c.spaceStatus.Status(ctx, spaceId)
	if err != nil {
		return
	}
	if statusEntry.Identity != pubKey.Account() {
		return coordinatorproto.ErrForbidden
	}
	if statusEntry.IsShareable {
		return nil
	}

	limits, err := c.accountLimit.GetLimitsBySpace(ctx, spaceId)
	if err != nil {
		return err
	}

	err = c.spaceStatus.MakeShareable(ctx, spaceId, statusEntry.Type, limits.SharedSpacesLimit)
	if err != nil {
		return
	}

	return c.aclEventLog.AddLog(ctx, acleventlog.AclEventLogEntry{
		SpaceId:   spaceId,
		PeerId:    peerId,
		Owner:     statusEntry.Identity,
		Timestamp: time.Now().Unix(),
		EntryType: acleventlog.EntryTypeSpaceShared,
	})
}

func (c *coordinator) MakeSpaceUnshareable(ctx context.Context, spaceId, aclHead string) (err error) {
	pubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return coordinatorproto.ErrForbidden
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	statusEntry, err := c.spaceStatus.Status(ctx, spaceId)
	if err != nil {
		return
	}
	if statusEntry.Identity != pubKey.Account() {
		return coordinatorproto.ErrForbidden
	}
	if !statusEntry.IsShareable {
		return nil
	}

	hasRecord, err := c.acl.HasRecord(ctx, spaceId, aclHead)
	if err != nil {
		return err
	}
	if !hasRecord {
		return coordinatorproto.ErrAclHeadIsMissing
	}

	if err = c.acl.ReadState(ctx, spaceId, func(s *list.AclState) error {
		if !s.IsEmpty() {
			return coordinatorproto.ErrAclNonEmpty
		}
		return nil
	}); err != nil {
		return
	}

	err = c.spaceStatus.MakeUnshareable(ctx, spaceId)
	if err != nil {
		return
	}

	return c.aclEventLog.AddLog(ctx, acleventlog.AclEventLogEntry{
		SpaceId:   spaceId,
		PeerId:    peerId,
		Owner:     statusEntry.Identity,
		Timestamp: time.Now().Unix(),
		EntryType: acleventlog.EntryTypeSpaceUnshared,
	})
}

func (c *coordinator) InboxAddMessage(ctx context.Context, message *inbox.InboxMessage) (err error) {
	err = c.inbox.InboxAddMessage(ctx, message)
	return
}

func (c *coordinator) AddStream(eventType coordinatorproto.NotifyEventType, accountId, peerId string, stream coordinatorproto.DRPCCoordinator_NotifySubscribeStream) error {
	return c.subscribe.AddStream(eventType, accountId, peerId, stream)
}
