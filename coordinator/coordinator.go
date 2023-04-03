package coordinator

import (
	"context"
	"errors"
	"github.com/anytypeio/any-sync-coordinator/config"
	"github.com/anytypeio/any-sync-coordinator/coordinatorlog"
	"github.com/anytypeio/any-sync-coordinator/spacestatus"
	"github.com/anytypeio/any-sync/accountservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace/object/accountdata"
	"github.com/anytypeio/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anytypeio/any-sync/commonspace/spacestorage"
	"github.com/anytypeio/any-sync/coordinator/coordinatorproto"
	"github.com/anytypeio/any-sync/net/peer"
	"github.com/anytypeio/any-sync/net/rpc/server"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/anytypeio/any-sync/util/crypto"
	"go.uber.org/zap"
	"storj.io/drpc"
	"time"
)

var (
	spaceReceiptValidPeriod        = time.Hour * 6
	defaultFileLimit        uint64 = 1 << 30 // 1 GiB
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
}

func (c *coordinator) Init(a *app.App) (err error) {
	c.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	delDays := a.MustComponent(config.CName).(*config.Config).SpaceStatus.DeletionPeriodDays
	c.deletionPeriod = time.Duration(delDays*24) * time.Hour
	h := &rpcHandler{c: c}
	c.account = a.MustComponent(accountservice.CName).(accountservice.Service).Account()
	c.spaceStatus = a.MustComponent(spacestatus.CName).(spacestatus.SpaceStatus)
	c.coordinatorLog = a.MustComponent(coordinatorlog.CName).(coordinatorlog.CoordinatorLog)
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
	err = spacestorage.ValidateSpaceHeader(spaceId, spaceHeader, accountPubKey)
	if err != nil {
		return
	}
	err = c.spaceStatus.NewStatus(ctx, spaceId, accountPubKey, oldPubKey)
	if err != nil {
		return
	}
	signedReceipt, err = c.prepareSpaceReceipt(spaceId, peerId, accountPubKey)
	if err != nil {
		return
	}
	c.addCoordinatorLog(ctx, spaceId, peerId, accountPubKey, signedReceipt)
	return
}

func (c *coordinator) FileLimitCheck(ctx context.Context, identity []byte, spaceId string) (limit uint64, err error) {
	// TODO: check identity and space here
	return defaultFileLimit, nil
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

func (c *coordinator) prepareSpaceReceipt(spaceId, peerId string, accountPubKey crypto.PubKey) (signedReceipt *coordinatorproto.SpaceReceiptWithSignature, err error) {
	marshalledAccount, err := accountPubKey.Marshall()
	if err != nil {
		return
	}
	marshalledNode, err := c.account.SignKey.GetPublic().Marshall()
	if err != nil {
		return
	}
	receipt := &coordinatorproto.SpaceReceipt{
		SpaceId:             spaceId,
		PeerId:              peerId,
		AccountIdentity:     marshalledAccount,
		ControlNodeIdentity: marshalledNode,
		ValidUntil:          uint64(time.Now().Add(spaceReceiptValidPeriod).Unix()),
	}
	receiptData, err := receipt.Marshal()
	if err != nil {
		return
	}
	sign, err := c.account.SignKey.Sign(receiptData)
	if err != nil {
		return
	}
	return &coordinatorproto.SpaceReceiptWithSignature{
		SpaceReceiptPayload: receiptData,
		Signature:           sign,
	}, nil
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
