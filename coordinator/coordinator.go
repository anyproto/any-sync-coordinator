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
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/ipfs/go-cid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/acleventlog"
	"github.com/anyproto/any-sync-coordinator/config"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/deletionlog"
	"github.com/anyproto/any-sync-coordinator/fileusage"
	"github.com/anyproto/any-sync-coordinator/inbox"
	"github.com/anyproto/any-sync-coordinator/invitestore"
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
	fileUsage      fileusage.FileUsage
	acl            acl.AclService
	inbox          inbox.InboxService
	subscribe      subscribe.SubscribeService
	drpcHandler    *rpcHandler
	pool           pool.Service
	inviteStore    invitestore.InviteStore

	// unboundInviteUploads counts invite files pushed by clients that predate the binding.
	// Those files are never collected; the counter tells us when it is safe to require it.
	unboundInviteUploads prometheus.Counter
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
	c.fileUsage = app.MustComponent[fileusage.FileUsage](a)
	c.aclEventLog = app.MustComponent[acleventlog.AclEventLog](a)
	c.pool = a.MustComponent(pool.CName).(pool.Service)
	c.inviteStore = app.MustComponent[invitestore.InviteStore](a)
	c.unboundInviteUploads = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "coordinator",
		Subsystem: "invite",
		Name:      "unbound_uploads_total",
		Help:      "invite files uploaded by clients that do not bind them to an acl invite",
	})
	return coordinatorproto.DRPCRegisterCoordinator(a.MustComponent(server.CName).(drpc.Mux), c.drpcHandler)
}

func (c *coordinator) Name() (name string) {
	return CName
}

func (c *coordinator) Run(ctx context.Context) (err error) {
	// not in Init: the metric component builds its registry there, and it is not guaranteed
	// to have been initialised before us
	if registry := c.metric.Registry(); registry != nil {
		registry.MustRegister(c.unboundInviteUploads)
	}
	return nil
}

func (c *coordinator) Close(ctx context.Context) (err error) {
	return nil
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
	ownerOfParent, err := c.ownerOfParent(ctx, spaceId, accountPubKey)
	if err != nil {
		return
	}
	return c.spaceStatus.SpaceDelete(ctx, spacestatus.SpaceDeletion{
		DeletionPayload:   payload,
		DeletionPayloadId: payloadId,
		SpaceId:           spaceId,
		DeletionPeriod:    time.Duration(deletionDurationSecs) * time.Minute,
		OwnerOfParent:     ownerOfParent,
		AccountInfo: spacestatus.AccountInfo{
			Identity:  accountPubKey,
			PeerId:    peerId,
			NetworkId: c.nodeConf.Configuration().NetworkId,
		},
	})
}

// ownerOfParent returns the parent of a child space for an account that deletes the child as the
// owner of that parent, and nothing for the owner of the child or for another space. An account that
// owns neither the child nor its parent is refused.
func (c *coordinator) ownerOfParent(ctx context.Context, spaceId string, account crypto.PubKey) (parentSpaceId string, err error) {
	entry, err := c.spaceStatus.Status(ctx, spaceId)
	if err != nil {
		if errors.Is(err, coordinatorproto.ErrSpaceNotExists) {
			return "", nil
		}
		return "", err
	}
	if entry.ParentSpaceId == "" || entry.Identity == account.Account() {
		return "", nil
	}
	parentOwner, err := c.acl.OwnerPubKey(ctx, entry.ParentSpaceId)
	if err != nil {
		if errors.Is(err, consensuserr.ErrLogNotFound) {
			return "", coordinatorproto.ErrParentSpaceNotAvailable
		}
		return "", err
	}
	if !parentOwner.Equals(account) {
		return "", coordinatorproto.ErrParentPermissionDenied
	}
	return entry.ParentSpaceId, nil
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

func (c *coordinator) SpaceSign(ctx context.Context, spaceId string, spaceHeader []byte, force bool) (signedReceipt *coordinatorproto.SpaceReceiptWithSignature, headerType string, err error) {
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
		if errors.Is(err, spacepayloads.ErrChildNeedsHeaderV1) {
			err = coordinatorproto.ErrParentSpaceInvalid
		}
		return
	}
	spaceType, headerType, err := spacestatus.VerifySpaceHeader(accountPubKey, spaceHeader)
	if err != nil {
		return
	}
	parentSpaceId, err := spacestatus.HeaderParent(spaceHeader, spaceType, headerType)
	if err != nil {
		return
	}
	if parentSpaceId == "" {
		err = c.spaceStatus.NewStatus(ctx, spaceId, accountPubKey, spaceType, headerType, force)
	} else {
		err = c.newChildStatus(ctx, spaceId, accountPubKey, spaceType, headerType, force, parentSpaceId)
	}
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

// newChildStatus registers a space whose header names a parent. A sign that takes a place, of a new
// child or a forced one of a finally deleted child, needs a parent that can take the child, a creator
// who is a Writer or above in the parent now, and room in the childSpacesLimit of the account that
// owns the parent. Any other sign is answered as the sign of a top-level space is, whatever changed in
// the parent since: a registered child is signed again, a deleted one answers that it is deleted.
func (c *coordinator) newChildStatus(ctx context.Context, spaceId string, creator crypto.PubKey, spaceType spacestatus.SpaceType, headerType string, force bool, parentSpaceId string) (err error) {
	// The deletion of a child can become final between the read of its status and the registration.
	// The registration then refuses a sign that takes a place unchecked, and the sign is decided again
	// from the new status. That happens once: a finally deleted child stays so until it is signed.
	for attempt := 0; attempt < 3; attempt++ {
		child, err := c.spaceStatus.Status(ctx, spaceId)
		registered := err == nil
		if err != nil && !errors.Is(err, coordinatorproto.ErrSpaceNotExists) {
			return err
		}
		takesPlace := spacestatus.TakesChildPlace(registered, child, force)
		if takesPlace {
			if err = c.checkChildCreator(ctx, creator, parentSpaceId); err != nil {
				return err
			}
		}
		err = c.spaceStatus.NewChildStatus(ctx, spaceId, creator, spaceType, headerType, force, parentSpaceId, c.childSpacesLimit, takesPlace)
		if !errors.Is(err, spacestatus.ErrChildTakesPlace) {
			return err
		}
	}
	return coordinatorproto.ErrUnexpected
}

// checkChildCreator tells whether the parent can take a child from this creator: the parent passes
// CheckParent and the creator is a Writer or above in its acl.
func (c *coordinator) checkChildCreator(ctx context.Context, creator crypto.PubKey, parentSpaceId string) (err error) {
	parent, err := c.spaceStatus.Status(ctx, parentSpaceId)
	if err != nil {
		if errors.Is(err, coordinatorproto.ErrSpaceNotExists) {
			return coordinatorproto.ErrParentSpaceNotAvailable
		}
		return err
	}
	if err = spacestatus.CheckParent(parent); err != nil {
		return err
	}
	permissions, err := c.acl.Permissions(ctx, creator, parentSpaceId)
	if err != nil {
		if errors.Is(err, consensuserr.ErrLogNotFound) {
			// the parent is signed, but its acl has not reached the network yet
			return coordinatorproto.ErrParentSpaceNotAvailable
		}
		return err
	}
	if !permissions.CanWrite() {
		return coordinatorproto.ErrParentPermissionDenied
	}
	return nil
}

func (c *coordinator) childSpacesLimit(ctx context.Context, identity string) (limit uint32, err error) {
	limits, err := c.accountLimit.GetLimits(ctx, identity)
	if err != nil {
		return 0, err
	}
	return limits.ChildSpacesLimit, nil
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
		ChildSpacesLimit:  req.ChildSpacesLimit,
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

	// a record of a child space is also checked against the parent of the space
	var child *childRecord
	if statusEntry.ParentSpaceId != "" {
		if child, err = c.newChildRecord(ctx, statusEntry.ParentSpaceId, rec); err != nil {
			return nil, err
		}
	}

	hasGuestRole := spacestatus.HasGuestRole(statusEntry.HeaderType)
	// the owner is taken from the state the record leads to: the cached acl takes the record in only
	// when the consensus node sends it back, which can be after this call returns
	var owner crypto.PubKey
	afterValid := func(before, after *list.AclState) (err error) {
		if owner, err = after.OwnerPubKey(); err != nil {
			return err
		}
		if !hasGuestRole {
			if err = checkNoNewGuest(before, after); err != nil {
				return err
			}
		}
		if child != nil {
			return child.afterValid(before, after)
		}
		return nil
	}

	opts := []acl.AddRecordOption{acl.WithAfterValid(afterValid)}
	if child != nil {
		// a top-level space has no parent to give anyone authority in it
		opts = append(opts, acl.WithParentAuthorizer(child.authorize))
	}

	rawRecordWithId, err := c.acl.AddRecord(ctx, spaceId, rec, acl.Limits{
		ReadMembers:  limits.SpaceMembersRead,
		WriteMembers: limits.SpaceMembersWrite,
	}, opts...)
	if err != nil {
		if errors.Is(err, acl.ErrLimitExceed) {
			err = coordinatorproto.ErrSpaceLimitReached
		} else if child != nil {
			err = child.refusal(err)
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

	if owner != nil && owner.Account() != statusEntry.Identity {
		log.InfoCtx(ctx, "owner change detected", zap.String("spaceId", spaceId), zap.String("newOwner", owner.Account()), zap.String("oldOwner", statusEntry.Identity))
		if err = c.spaceStatus.ChangeOwner(ctx, spaceId, owner.Account()); err != nil {
			return
		}
		if _, err = c.deletionLog.AddOwnershipChange(ctx, spaceId, statusEntry.Identity, rawRecordWithId.Id); err != nil {
			return
		}
	}

	return rawRecordWithId, nil
}

// checkNoNewGuest refuses a record that gives the guest role to an account that did not hold it.
func checkNoNewGuest(before, after *list.AclState) error {
	for _, account := range after.CurrentAccounts() {
		if account.Permissions.IsGuest() && !before.Permissions(account.PubKey).IsGuest() {
			return coordinatorproto.ErrGuestRoleNotAllowed
		}
	}
	return nil
}

// SpaceChildren lists the children of a space for an Admin or the Owner of that space. A member below
// that sees only the children it belongs to, in its own list of spaces.
func (c *coordinator) SpaceChildren(ctx context.Context, spaceId string) (children []spacestatus.StatusEntry, err error) {
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	statusEntry, err := c.spaceStatus.Status(ctx, spaceId)
	if err != nil {
		return
	}
	if statusEntry.Status != spacestatus.SpaceStatusCreated {
		return nil, coordinatorproto.ErrSpaceIsDeleted
	}
	permissions, err := c.acl.Permissions(ctx, accountPubKey, spaceId)
	if err != nil {
		if errors.Is(err, consensuserr.ErrLogNotFound) {
			// the space is signed, but its acl has not reached the network yet
			return nil, coordinatorproto.ErrParentSpaceNotAvailable
		}
		return
	}
	if !permissions.CanManageAccounts() {
		return nil, coordinatorproto.ErrParentPermissionDenied
	}
	return c.spaceStatus.Children(ctx, spaceId)
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

// AclDeleteInvite deletes a revoked invite's file from the filenodes.
//
// It looks the cid up in the binding collection first. A bound invite is checked against the
// acl and refused while it still grants access. An invite uploaded before the binding existed
// has no entry there, and is deleted as asked.
func (c *coordinator) AclDeleteInvite(ctx context.Context, req *coordinatorproto.AclDeleteInviteRequest) (err error) {
	pubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return coordinatorproto.ErrForbidden
	}
	perms, err := c.acl.Permissions(ctx, pubKey, req.SpaceId)
	if err != nil {
		return
	}
	if !perms.CanManageAccounts() {
		return coordinatorproto.ErrForbidden
	}
	inviteCid, err := cid.Cast(req.InviteCid)
	if err != nil {
		return
	}

	entry, err := c.inviteStore.Get(ctx, inviteCid.String())
	if errors.Is(err, invitestore.ErrNotFound) {
		// not bound: an invite from before the binding existed, so there is nothing to check
		// the file against
		return c.deleteInviteFile(ctx, inviteCid.Bytes())
	}
	if err != nil {
		return err
	}
	if entry.SpaceId != req.SpaceId {
		return coordinatorproto.ErrForbidden
	}
	dead, err := c.deadInvites(ctx, req.SpaceId, []invitestore.Entry{entry})
	if err != nil {
		return err
	}
	if len(dead) == 0 {
		// never delete the file of an invite that still grants access
		return coordinatorproto.ErrInviteStillActive
	}
	if err = c.deleteInviteFile(ctx, inviteCid.Bytes()); err != nil {
		return err
	}
	return c.inviteStore.Delete(ctx, entry.Cid)
}

func (c *coordinator) InboxAddMessage(ctx context.Context, message *inbox.InboxMessage) (err error) {
	err = c.inbox.InboxAddMessage(ctx, message)
	return
}

func (c *coordinator) AddStream(eventType coordinatorproto.NotifyEventType, accountId, peerId string, stream coordinatorproto.DRPCCoordinator_NotifySubscribeStream) error {
	return c.subscribe.AddStream(eventType, accountId, peerId, stream)
}
