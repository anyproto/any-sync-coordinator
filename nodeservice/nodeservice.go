package nodeservice

import (
	"context"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/commonspace/object/tree/treechangeproto"
)

const CName = "coordinator.nodeservice"

type NodeService interface {
	app.ComponentRunnable
	Delete(ctx context.Context, spaceId string, raw *treechangeproto.RawTreeChangeWithId) (err error)
}

type nodeService struct {
}

func (n *nodeService) Init(a *app.App) (err error) {
	return nil
}

func (n *nodeService) Name() (name string) {
	return CName
}

func (n *nodeService) Run(ctx context.Context) (err error) {
	return nil
}

func (n *nodeService) Close(ctx context.Context) (err error) {
	return nil
}

func (n *nodeService) Delete(ctx context.Context, spaceId string, raw *treechangeproto.RawTreeChangeWithId) (err error) {
	return nil
}
