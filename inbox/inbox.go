package inbox

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
)

const CName = "common.inbox.here"

var log = logger.NewNamed(CName)

var (
	ErrSomeError = errors.New("some error")
)

func New() InboxService {
	return new(inbox)
}

type InboxService interface {
	app.ComponentRunnable
}

type inbox struct {
}

func (s *inbox) Init(a *app.App) (err error) {
	log.Info("inbox service init")
	return
}

func (s *inbox) Name() (name string) {
	return CName
}

func (s *inbox) Run(ctx context.Context) error {
	log.Info("inbox service run")
	return nil
}

func (s *inbox) Close(_ context.Context) (err error) {
	return nil
}
