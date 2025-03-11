package stream

import (
	"context"
	"sync"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/cheggaaa/mb/v3"
)

// Stream is a buffer that receives updates from object and gives back to a client
type Stream struct {
	id uint64
	mu sync.Mutex
	mb *mb.MB[[]coordinatorproto.InboxMessage]
	s  *service
}

// AddRecords adds new records to stream, called by objects
func (s *Stream) AddMessages(accountId string, messages []coordinatorproto.InboxMessage) (err error) {
	return s.mb.Add(context.TODO(), messages)
}

// Close closes stream and unsubscribes all ids
func (s *Stream) Close() {
	_ = s.mb.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for logId := range s.logIds {
		_ = s.s.RemoveStream(context.TODO(), logId, s.id)
	}
}
