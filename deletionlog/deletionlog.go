package deletionlog

import (
	"context"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const CName = "coordinator.deletionLog"

var log = logger.NewNamed(CName)

const collName = "deletionLog"

type DeletionLog interface {
	GetAfter(ctx context.Context, afterId string, limit uint32) ([]Record, error)
	Add(ctx context.Context, spaceId string, status Status) (err error)
	app.Component
}

type Record struct {
	Id      *primitive.ObjectID `bson:"_id"`
	SpaceId string              `bson:"spaceId"`
	Status  Status              `bson:"status"`
}

func (r Record) Timestamp() int64 {
	return r.Id.Timestamp().Unix()
}

type Status int32

const (
	StatusOk            = Status(coordinatorproto.DeletionLogRecordStatus_Ok)
	StatusRemovePrepare = Status(coordinatorproto.DeletionLogRecordStatus_RemovePrepare)
	StatusRemove        = Status(coordinatorproto.DeletionLogRecordStatus_Remove)
)

type deletionLog struct {
}

func (d *deletionLog) Init(a *app.App) (err error) {
	return
}

func (d *deletionLog) Name() (name string) {
	return CName
}

func (d *deletionLog) GetAfter(ctx context.Context, afterId string, limit uint32) ([]Record, error) {
	//TODO implement me
	panic("implement me")
}

func (d *deletionLog) Add(ctx context.Context, spaceId string, status Status) (err error) {
	//TODO implement me
	panic("implement me")
}
