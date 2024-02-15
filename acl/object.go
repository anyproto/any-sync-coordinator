package acl

import (
	"context"
	"sync"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/liststorage"
	"github.com/anyproto/any-sync/consensus/consensusclient"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func newAclObject(ctx context.Context, cs consensusclient.Service, id string) (*aclObject, error) {
	obj := &aclObject{
		id:          id,
		consService: cs,
		ready:       make(chan struct{}),
	}
	select {
	case <-obj.ready:
		if obj.consErr != nil {
			_ = cs.UnWatch(id)
			return nil, obj.consErr
		}
		return obj, nil
	case <-ctx.Done():
		_ = cs.UnWatch(id)
		return nil, ctx.Err()
	}
}

type aclObject struct {
	id    string
	store liststorage.ListStorage
	list.AclList

	ready       chan struct{}
	consErr     error
	consService consensusclient.Service

	lastUsage atomic.Time

	mu sync.Mutex
}

func (a *aclObject) AddConsensusRecords(recs []*consensusproto.RawRecordWithId) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.store == nil {
		if a.store, a.consErr = liststorage.NewInMemoryAclListStorage(a.id, recs); a.consErr != nil {
			close(a.ready)
			return
		}
		if a.AclList, a.consErr = list.BuildAclList(a.store, list.NoOpAcceptorVerifier{}); a.consErr != nil {
			close(a.ready)
			return
		}
	} else {
		a.Lock()
		defer a.Unlock()
		if err := a.AddRawRecords(recs); err != nil {
			log.Warn("unable to add consensus records", zap.Error(err), zap.String("spaceId", a.id))
			return
		}
	}
}

func (a *aclObject) AddConsensusError(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.store == nil {
		a.consErr = err
		close(a.ready)
	} else {
		log.Warn("got consensus error", zap.Error(err))
	}
}

func (a *aclObject) Close() (err error) {
	return a.consService.UnWatch(a.id)
}

func (a *aclObject) TryClose(objectTTL time.Duration) (res bool, err error) {
	if a.lastUsage.Load().Before(time.Now().Add(-objectTTL)) {
		return true, a.Close()
	}
	return false, nil
}