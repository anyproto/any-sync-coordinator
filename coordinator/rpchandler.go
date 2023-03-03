package coordinator

import (
	"context"
	"github.com/anytypeio/any-sync-coordinator/spacestatus"
	"github.com/anytypeio/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anytypeio/any-sync/coordinator/coordinatorproto"
)

type rpcHandler struct {
	c *coordinator
}

func (r *rpcHandler) convertStatus(status spacestatus.StatusEntry) *coordinatorproto.SpaceStatusPayload {
	return &coordinatorproto.SpaceStatusPayload{
		Status:            coordinatorproto.SpaceStatus(status.Status),
		DeletionTimestamp: status.DeletionDate.Add(r.c.deletionPeriod).UnixNano(),
	}
}

func (r *rpcHandler) SpaceStatusCheck(ctx context.Context, request *coordinatorproto.SpaceStatusCheckRequest) (*coordinatorproto.SpaceStatusCheckResponse, error) {
	status, err := r.c.StatusCheck(ctx, request.SpaceId)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.SpaceStatusCheckResponse{
		Payload: r.convertStatus(status),
	}, nil
}

func (r *rpcHandler) SpaceStatusChange(ctx context.Context, request *coordinatorproto.SpaceStatusChangeRequest) (*coordinatorproto.SpaceStatusChangeResponse, error) {
	var raw *treechangeproto.RawTreeChangeWithId
	if request.DeletionChangePayload != nil {
		raw = &treechangeproto.RawTreeChangeWithId{
			RawChange: request.DeletionChangePayload,
			Id:        request.DeletionChangeId,
		}
	}
	status, err := r.c.StatusChange(ctx, request.SpaceId, raw)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.SpaceStatusChangeResponse{
		Payload: r.convertStatus(status),
	}, nil
}

func (r *rpcHandler) SpaceSign(ctx context.Context, req *coordinatorproto.SpaceSignRequest) (*coordinatorproto.SpaceSignResponse, error) {
	receipt, err := r.c.SpaceSign(ctx, req.SpaceId, req.Header)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.SpaceSignResponse{
		Receipt: receipt,
	}, nil
}

func (r *rpcHandler) FileLimitCheck(ctx context.Context, req *coordinatorproto.FileLimitCheckRequest) (*coordinatorproto.FileLimitCheckResponse, error) {
	limit, err := r.c.FileLimitCheck(ctx, req.AccountIdentity, req.SpaceId)
	if err != nil {
		return nil, err
	}
	return &coordinatorproto.FileLimitCheckResponse{
		Limit: limit,
	}, nil
}
