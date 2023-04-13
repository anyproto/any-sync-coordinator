package coordinator

import (
	"context"
	"github.com/anytypeio/any-sync-coordinator/spacestatus"
	"github.com/anytypeio/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anytypeio/any-sync/coordinator/coordinatorproto"
	"github.com/anytypeio/any-sync/nodeconf"
	"time"
)

type rpcHandler struct {
	c *coordinator
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
	receipt, err := r.c.SpaceSign(ctx, req.SpaceId, req.Header, req.OldIdentity, req.NewIdentitySignature)
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

func (r *rpcHandler) NetworkConfiguration(ctx context.Context, req *coordinatorproto.NetworkConfigurationRequest) (*coordinatorproto.NetworkConfigurationResponse, error) {
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
