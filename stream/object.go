package stream

import (
	"sync"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
)

// object is a cache entry that holds the actual log state and maintains added streams
type object struct {
	accountId string
	messages  []coordinatorproto.InboxMessage

	streams map[uint64]*Stream

	mu sync.Mutex
}

// Records returns all inbox messages
func (o *object) Messages() []coordinatorproto.InboxMessage {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.messages
}

// AddStream adds stream to the object
func (o *object) AddStream(s *Stream) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.streams[s.id] = s
	_ = s.AddMessages(o.accountId, o.messages)
	return
}

// RemoveStream remove stream from object
func (o *object) RemoveStream(id uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.streams, id)
}

func (o *object) TryClose(ttl time.Duration) (ok bool, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.streams) > 0 {
		return
	} else {
		return true, o.Close()
	}
}

func (o *object) Close() (err error) {
	return nil
}
