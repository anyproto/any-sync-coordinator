package identityrepo

import (
	"context"
	"github.com/anyproto/any-sync/identityrepo/identityrepoproto"
)

var okResp = &identityrepoproto.Ok{}

type rpcHandler struct {
	repo *identityRepo
}

func (r *rpcHandler) DataPut(ctx context.Context, req *identityrepoproto.DataPutRequest) (*identityrepoproto.Ok, error) {
	if err := r.repo.Push(ctx, req.Identity, req.Data); err != nil {
		return nil, err
	}
	return okResp, nil
}

func (r *rpcHandler) DataDelete(ctx context.Context, req *identityrepoproto.DataDeleteRequest) (*identityrepoproto.Ok, error) {
	if err := r.repo.Delete(ctx, req.Identity, req.Kinds...); err != nil {
		return nil, err
	}
	return okResp, nil
}

func (r *rpcHandler) DataPull(ctx context.Context, req *identityrepoproto.DataPullRequest) (*identityrepoproto.DataPullResponse, error) {
	res, err := r.repo.Pull(ctx, req.Identities, req.Kinds)
	if err != nil {
		return nil, err
	}
	return &identityrepoproto.DataPullResponse{
		Data: res,
	}, nil
}
