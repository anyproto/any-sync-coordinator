package inbox

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync-coordinator/db"
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
	InboxAddMessage(ctx context.Context, msg *InboxMessage) (err error)
	app.ComponentRunnable
}

type inbox struct {
	db db.Database
}

func (s *inbox) Init(a *app.App) (err error) {
	s.db = a.MustComponent(db.CName).(db.Database)
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

func (s *inbox) InboxAddMessage(ctx context.Context, msg *InboxMessage) (err error) {
	_, err = s.db.GetInboxCollection().InsertOne(ctx, msg)
	return err
}
