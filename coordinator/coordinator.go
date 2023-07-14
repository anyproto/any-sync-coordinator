package coordinator

import (
	"context"
	"errors"
	"github.com/anyproto/any-sync-coordinator/config"
	"github.com/anyproto/any-sync-coordinator/coordinatorlog"
	"github.com/anyproto/any-sync-coordinator/filelimit"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"
	"storj.io/drpc"
	"time"
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

var ErrIncorrectAccountSignature = errors.New("incorrect account signature")

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
	deletionPeriod time.Duration
	metric         metric.Metric
	fileLimit      filelimit.FileLimit
}

func (c *coordinator) Init(a *app.App) (err error) {
	c.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	delDays := a.MustComponent(config.CName).(*config.Config).SpaceStatus.DeletionPeriodDays
	c.deletionPeriod = time.Duration(delDays*24) * time.Hour
	h := &rpcHandler{c: c}
	c.account = a.MustComponent(accountservice.CName).(accountservice.Service).Account()
	c.spaceStatus = a.MustComponent(spacestatus.CName).(spacestatus.SpaceStatus)
	c.coordinatorLog = a.MustComponent(coordinatorlog.CName).(coordinatorlog.CoordinatorLog)
	c.metric = a.MustComponent(metric.CName).(metric.Metric)
	c.fileLimit = a.MustComponent(filelimit.CName).(filelimit.FileLimit)
	return coordinatorproto.DRPCRegisterCoordinator(a.MustComponent(server.CName).(drpc.Mux), h)
}

func (c *coordinator) Name() (name string) {
	return CName
}

func (c *coordinator) StatusCheck(ctx context.Context, spaceId string) (status spacestatus.StatusEntry, err error) {
	defer func() {
		log.Debug("finished checking status", zap.Error(err), zap.String("spaceId", spaceId), zap.Error(err))
	}()
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	status, err = c.spaceStatus.Status(ctx, spaceId, accountPubKey)
	return
}

func (c *coordinator) StatusChange(ctx context.Context, spaceId string, raw *treechangeproto.RawTreeChangeWithId) (entry spacestatus.StatusEntry, err error) {
	defer func() {
		log.Debug("finished changing status", zap.Error(err), zap.String("spaceId", spaceId), zap.Bool("isDelete", raw != nil))
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
	if raw != nil {
		status = spacestatus.SpaceStatusDeletionPending
	}
	return c.spaceStatus.ChangeStatus(ctx, spaceId, spacestatus.StatusChange{
		DeletionPayload: raw,
		Identity:        accountPubKey,
		Status:          status,
		PeerId:          peerId,
	})
}

func (c *coordinator) SpaceSign(ctx context.Context, spaceId string, spaceHeader, oldIdentity, signature []byte) (signedReceipt *coordinatorproto.SpaceReceiptWithSignature, err error) {
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
	oldPubKey, err := crypto.UnmarshalEd25519PublicKeyProto(oldIdentity)
	if err != nil {
		return
	}
	err = c.verifyOldAccount(accountPubKey, oldPubKey, signature)
	if err != nil {
		return
	}
	err = commonspace.ValidateSpaceHeader(&spacesyncproto.RawSpaceHeaderWithId{RawHeader: spaceHeader, Id: spaceId}, accountPubKey)
	if err != nil {
		return
	}
	err = c.spaceStatus.NewStatus(ctx, spaceId, accountPubKey, oldPubKey)
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

func (c *coordinator) verifyOldAccount(newAccountKey, oldAccountKey crypto.PubKey, signature []byte) (err error) {
	rawPub, err := newAccountKey.Raw()
	if err != nil {
		return
	}
	verify, err := oldAccountKey.Verify(rawPub, signature)
	if err != nil {
		return
	}
	if !verify {
		return ErrIncorrectAccountSignature
	}
	return
}

func (c *coordinator) addCoordinatorLog(ctx context.Context, spaceId, peerId string, accountPubKey crypto.PubKey, signedReceipt *coordinatorproto.SpaceReceiptWithSignature) {
	var err error
	defer func() {
		if err != nil {
			log.Debug("failed to add space receipt log entry", zap.Error(err))
		}
	}()
	marshalledReceipt, err := signedReceipt.Marshal()
	if err != nil {
		return
	}
	err = c.coordinatorLog.SpaceReceipt(ctx, coordinatorlog.SpaceReceiptEntry{
		SignedSpaceReceipt: marshalledReceipt,
		SpaceId:            spaceId,
		PeerId:             peerId,
		Identity:           accountPubKey.Account(),
	})
	return
}
