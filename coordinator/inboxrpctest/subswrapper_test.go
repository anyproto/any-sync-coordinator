package inboxrpctest

import (
	"context"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/coordinator/subscribeclient"
)

type TestSubsClientService interface {
	subscribeclient.SubscribeClientService
	IsRunning() <-chan bool
}

type TestSubsClientServiceWrapper struct {
	original  subscribeclient.SubscribeClientService
	isRunning chan bool
}

func NewTestSubsClientServiceWrapper(original subscribeclient.SubscribeClientService) *TestSubsClientServiceWrapper {
	return &TestSubsClientServiceWrapper{original: original}
}

func (w *TestSubsClientServiceWrapper) Init(a *app.App) error {
	w.isRunning = make(chan bool, 1)
	return w.original.Init(a)
}

func (w *TestSubsClientServiceWrapper) Name() string {
	return w.original.Name()
}

func (w *TestSubsClientServiceWrapper) Run(ctx context.Context) error {
	w.isRunning <- true
	return w.original.Run(ctx)
}

func (w *TestSubsClientServiceWrapper) Close(ctx context.Context) error {
	return w.original.Close(ctx)
}

func (w *TestSubsClientServiceWrapper) Subscribe(eventType coordinatorproto.NotifyEventType, callback subscribeclient.EventCallback) error {
	return w.original.Subscribe(eventType, callback)
}

func (w *TestSubsClientServiceWrapper) IsRunning() <-chan bool {
	return w.isRunning
}
