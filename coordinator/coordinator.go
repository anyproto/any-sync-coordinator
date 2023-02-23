package coordinator

import (
	"context"
	"github.com/anytypeio/any-sync/accountservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace/object/accountdata"
	"github.com/anytypeio/any-sync/coordinator/coordinatorproto"
	"github.com/anytypeio/any-sync/net/peer"
	"github.com/anytypeio/any-sync/net/rpc/server"
	"github.com/anytypeio/any-sync/nodeconf"
	"storj.io/drpc"
	"time"
)

var (
	spaceReceiptValidPeriod        = time.Hour * 6
	defaultFileLimit        uint64 = 1 << 30 // 1 GiB
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
	account  *accountdata.AccountData
	nodeConf nodeconf.Service
}

func (c *coordinator) Init(a *app.App) (err error) {
	c.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	h := &rpcHandler{c: c}
	c.account = a.MustComponent(accountservice.CName).(accountservice.Service).Account()
	return coordinatorproto.DRPCRegisterCoordinator(a.MustComponent(server.CName).(drpc.Mux), h)
}

func (c *coordinator) Name() (name string) {
	return CName
}

func (c *coordinator) SpaceSign(ctx context.Context, spaceId string) (signedReceipt *coordinatorproto.SpaceReceiptWithSignature, err error) {
	accountIdentity, err := peer.CtxIdentity(ctx)
	if err != nil {
		return
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}

	// TODO: do security check here

	receipt := &coordinatorproto.SpaceReceipt{
		SpaceId:             spaceId,
		PeerId:              peerId,
		AccountIdentity:     accountIdentity,
		ControlNodeIdentity: c.account.Identity,
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

func (c *coordinator) FileLimitCheck(ctx context.Context, identity []byte, spaceId string) (limit uint64, err error) {
	// TODO: check identity and space here
	return defaultFileLimit, nil
}
