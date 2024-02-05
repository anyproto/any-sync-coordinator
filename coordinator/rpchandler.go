package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/nodeconf"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

type rpcHandler struct {
	c *coordinator
}

func (r *rpcHandler) SpaceDelete(ctx context.Context, request *coordinatorproto.SpaceDeleteRequest) (resp *coordinatorproto.SpaceDeleteResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.spaceDelete",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(request.SpaceId),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()
	ts, err := r.c.SpaceDelete(ctx, request.SpaceId, request.DeletionDuration, request.DeletionPayload, request.DeletionPayloadId)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.SpaceDeleteResponse{
		ToBeDeletedTimestamp: ts,
	}, nil
}

func (r *rpcHandler) AccountDelete(ctx context.Context, request *coordinatorproto.AccountDeleteRequest) (resp *coordinatorproto.AccountDeleteResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.accountDelete",
			metric.TotalDur(time.Since(st)),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()
	ts, err := r.c.AccountDelete(ctx, request.DeletionPayload, request.DeletionPayloadId)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.AccountDeleteResponse{
		ToBeDeletedTimestamp: ts,
	}, nil
}

func (r *rpcHandler) AccountRevertDeletion(ctx context.Context, request *coordinatorproto.AccountRevertDeletionRequest) (resp *coordinatorproto.AccountRevertDeletionResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.accountRevertDeletion",
			metric.TotalDur(time.Since(st)),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()
	err = r.c.AccountRevertDeletion(ctx)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.AccountRevertDeletionResponse{}, nil
}

func (r *rpcHandler) convertStatus(status spacestatus.StatusEntry) *coordinatorproto.SpaceStatusPayload {
	var timestamp int64
	if status.Status != spacestatus.SpaceStatusCreated {
		timestamp = time.Unix(status.DeletionTimestamp, 0).Add(r.c.deletionPeriod).Unix()
	}
	return &coordinatorproto.SpaceStatusPayload{
		Status:            coordinatorproto.SpaceStatus(status.Status),
		DeletionTimestamp: timestamp,
	}
}

func (r *rpcHandler) SpaceStatusCheck(ctx context.Context, req *coordinatorproto.SpaceStatusCheckRequest) (resp *coordinatorproto.SpaceStatusCheckResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.spaceStatusCheck",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(req.SpaceId),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()
	status, err := r.c.StatusCheck(ctx, req.SpaceId)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.SpaceStatusCheckResponse{
		Payload: r.convertStatus(status),
	}, nil
}

func (r *rpcHandler) SpaceStatusCheckMany(ctx context.Context, req *coordinatorproto.SpaceStatusCheckManyRequest) (resp *coordinatorproto.SpaceStatusCheckManyResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.spaceStatusCheckMany",
			metric.TotalDur(time.Since(st)),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()

	resp = &coordinatorproto.SpaceStatusCheckManyResponse{
		Payloads: make([]*coordinatorproto.SpaceStatusPayload, 0, len(req.SpaceIds)),
	}

	for _, spaceId := range req.SpaceIds {
		var status spacestatus.StatusEntry
		status, err = r.c.StatusCheck(ctx, spaceId)
		if err != nil {
			return nil, err
		}
		resp.Payloads = append(resp.Payloads, r.convertStatus(status))
	}
	return
}

func (r *rpcHandler) SpaceStatusChange(ctx context.Context, req *coordinatorproto.SpaceStatusChangeRequest) (resp *coordinatorproto.SpaceStatusChangeResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.spaceStatusChange",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(req.SpaceId),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()
	entry, err := r.c.StatusChange(ctx, req.SpaceId, r.c.deletionPeriod, req.DeletionPayloadType, req.DeletionPayload, req.DeletionPayloadId)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.SpaceStatusChangeResponse{
		Payload: r.convertStatus(entry),
	}, nil
}

func (r *rpcHandler) SpaceSign(ctx context.Context, req *coordinatorproto.SpaceSignRequest) (resp *coordinatorproto.SpaceSignResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.spaceSign",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(req.SpaceId),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()

	receipt, err := r.c.SpaceSign(ctx, req.SpaceId, req.Header, req.OldIdentity, req.NewIdentitySignature, req.ForceRequest)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.SpaceSignResponse{
		Receipt: receipt,
	}, nil
}

func (r *rpcHandler) FileLimitCheck(ctx context.Context, req *coordinatorproto.FileLimitCheckRequest) (resp *coordinatorproto.FileLimitCheckResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.fileLimitCheck",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(req.SpaceId),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()
	limit, storageKey, err := r.c.fileLimit.Get(ctx, req.AccountIdentity, req.SpaceId)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.FileLimitCheckResponse{
		Limit:      limit,
		StorageKey: storageKey,
	}, nil
}

func (r *rpcHandler) NetworkConfiguration(ctx context.Context, req *coordinatorproto.NetworkConfigurationRequest) (resp *coordinatorproto.NetworkConfigurationResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.networkConfiguration",
			metric.TotalDur(time.Since(st)),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()
	last := r.c.nodeConf.Configuration()
	var nodes []*coordinatorproto.Node
	if req.CurrentId != last.Id {
		nodes = make([]*coordinatorproto.Node, 0, len(last.Nodes))
		for _, n := range last.Nodes {
			types := make([]coordinatorproto.NodeType, 0, len(n.Types))
			for _, t := range n.Types {
				switch t {
				case nodeconf.NodeTypeCoordinator:
					types = append(types, coordinatorproto.NodeType_CoordinatorAPI)
				case nodeconf.NodeTypeFile:
					types = append(types, coordinatorproto.NodeType_FileAPI)
				case nodeconf.NodeTypeTree:
					types = append(types, coordinatorproto.NodeType_TreeAPI)
				case nodeconf.NodeTypeConsensus:
					types = append(types, coordinatorproto.NodeType_ConsensusAPI)
				case nodeconf.NodeTypeNamingNode:
					types = append(types, coordinatorproto.NodeType_NamingNodeAPI)
				case nodeconf.NodeTypePaymentProcessingNode:
					types = append(types, coordinatorproto.NodeType_PaymentProcessingAPI)
				}
			}
			nodes = append(nodes, &coordinatorproto.Node{
				PeerId:    n.PeerId,
				Addresses: n.Addresses,
				Types:     types,
			})
		}
	}
	return &coordinatorproto.NetworkConfigurationResponse{
		ConfigurationId:  last.Id,
		NetworkId:        last.NetworkId,
		Nodes:            nodes,
		CreationTimeUnix: uint64(last.CreationTime.Unix()),
	}, nil
}

func (r *rpcHandler) DeletionLog(ctx context.Context, req *coordinatorproto.DeletionLogRequest) (resp *coordinatorproto.DeletionLogResponse, err error) {
	st := time.Now()
	defer func() {
		r.c.metric.RequestLog(ctx, "coordinator.deletionLog",
			metric.TotalDur(time.Since(st)),
			zap.String("addr", peer.CtxPeerAddr(ctx)),
			zap.Error(err),
		)
	}()

	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return nil, err
	}
	if len(r.c.nodeConf.NodeTypes(peerId)) == 0 {
		return nil, fmt.Errorf("forbidden")
	}

	recs, hasMore, err := r.c.deletionLog.GetAfter(ctx, req.AfterId, req.Limit)
	if err != nil {
		return nil, err
	}
	resp = &coordinatorproto.DeletionLogResponse{
		Records: make([]*coordinatorproto.DeletionLogRecord, 0, len(recs)),
		HasMore: hasMore,
	}
	for _, rec := range recs {
		resp.Records = append(resp.Records, &coordinatorproto.DeletionLogRecord{
			Id:        rec.Id.Hex(),
			SpaceId:   rec.SpaceId,
			FileGroup: rec.FileGroup,
			Status:    coordinatorproto.DeletionLogRecordStatus(rec.Status),
			Timestamp: rec.Id.Timestamp().Unix(),
		})
	}
	return
}
