package inboxrpctest

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync-coordinator/inbox"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
)

type TestInboxService interface {
	inbox.InboxService
	IsRunning() <-chan bool
}

type TestInboxServiceWrapper struct {
	original  inbox.InboxService
	isRunning chan bool
}

func NewTestInboxServiceWrapper(original inbox.InboxService) *TestInboxServiceWrapper {
	return &TestInboxServiceWrapper{original: original}
}

func (w *TestInboxServiceWrapper) Init(a *app.App) error {
	w.isRunning = make(chan bool, 1)
	return w.original.Init(a)
}

func (w *TestInboxServiceWrapper) Name() string {
	return w.original.Name()
}

func (w *TestInboxServiceWrapper) Run(ctx context.Context) error {
	// TODO: have to move this to new subscribeclient mock to make this work?
	fmt.Printf("inbox wrapper run\n")
	w.isRunning <- true
	fmt.Printf("inbox wrapper run \n")
	return w.original.Run(ctx)
}

func (w *TestInboxServiceWrapper) Close(ctx context.Context) error {
	return w.original.Close(ctx)
}

func (w *TestInboxServiceWrapper) InboxAddMessage(ctx context.Context, msg *inbox.InboxMessage) error {
	return w.original.InboxAddMessage(ctx, msg)
}

func (w *TestInboxServiceWrapper) InboxFetch(ctx context.Context, offset string) (*inbox.InboxFetchResult, error) {
	return w.original.InboxFetch(ctx, offset)
}

func (w *TestInboxServiceWrapper) SubscribeClient(stream coordinatorproto.DRPCCoordinator_NotifySubscribeStream) error {
	return w.original.SubscribeClient(stream)
}

func (w *TestInboxServiceWrapper) IsRunning() <-chan bool {
	return w.isRunning
}
