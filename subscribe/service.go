//go:generate mockgen -destination mock_subscribe/mock_subscribe.go github.com/anyproto/any-sync-coordinator/subscribe SubscribeService
package subscribe

import (
	"context"
	"fmt"
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"go.uber.org/zap"
)

const CName = "common.subscribe"

var log = logger.NewNamed(CName)

func New() SubscribeService {
	return new(subscribe)
}

type SubscribeService interface {
	app.ComponentRunnable
	AddStream(eventType coordinatorproto.NotifyEventType, accountId, peerId string, stream coordinatorproto.DRPCCoordinator_NotifySubscribeStream) error
	NotifyAllPeers(eventType coordinatorproto.NotifyEventType, accountId string, payload []byte)
}

type subscribe struct {

	// eventType -> accountId -> PeerId -> Stream
	notifyStreams map[coordinatorproto.NotifyEventType]map[string]map[string]coordinatorproto.DRPCCoordinator_NotifySubscribeStream
	mu            sync.Mutex
}

func (s *subscribe) Init(a *app.App) (err error) {
	s.notifyStreams = make(map[coordinatorproto.NotifyEventType]map[string]map[string]coordinatorproto.DRPCCoordinator_NotifySubscribeStream)
	// init first level, eventType beforehand
	for number := range coordinatorproto.NotifyEventType_name {
		eventType := coordinatorproto.NotifyEventType(number)
		s.notifyStreams[eventType] = make(map[string]map[string]coordinatorproto.DRPCCoordinator_NotifySubscribeStream)
	}
	return
}

func (s *subscribe) Name() (name string) {
	return CName
}

func (s *subscribe) Run(ctx context.Context) error {
	return nil
}

func (s *subscribe) Close(_ context.Context) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, accounts := range s.notifyStreams {
		for _, peers := range accounts {
			for _, stream := range peers {
				_ = stream.Close()
			}
		}
	}

	return nil
}

func (s *subscribe) AddStream(eventType coordinatorproto.NotifyEventType, accountId, peerId string, stream coordinatorproto.DRPCCoordinator_NotifySubscribeStream) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.notifyStreams[eventType][accountId]; !ok {
		s.notifyStreams[eventType][accountId] = make(map[string]coordinatorproto.DRPCCoordinator_NotifySubscribeStream)
	}

	if _, ok := s.notifyStreams[eventType][accountId][peerId]; ok {
		return fmt.Errorf("%w, peerId: %s", coordinatorproto.ErrSubscribePeerAlreadySubscribed, peerId)
	}

	s.notifyStreams[eventType][accountId][peerId] = stream
	go s.waitCloseStream(eventType, accountId, peerId, stream)

	return nil
}

func (s *subscribe) waitCloseStream(eventType coordinatorproto.NotifyEventType, accountId, peerId string, stream coordinatorproto.DRPCCoordinator_NotifySubscribeStream) {
	<-stream.Context().Done()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeStream(eventType, accountId, peerId)
}

func (s *subscribe) removeStream(eventType coordinatorproto.NotifyEventType, accountId, peerId string) {
	delete(s.notifyStreams[eventType][accountId], peerId)
	if len(s.notifyStreams[eventType][accountId]) == 0 {
		delete(s.notifyStreams[eventType], accountId)
	}
}

func (s *subscribe) NotifyAllPeers(eventType coordinatorproto.NotifyEventType, accountId string, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if receivers, ok := s.notifyStreams[eventType]; ok {
		if peers, ok := receivers[accountId]; ok {
			event := &coordinatorproto.NotifySubscribeEvent{
				EventType: eventType,
				Payload:   payload,
			}
			for peerId, stream := range peers {
				log.Debug("sending to notify stream", zap.String("receiver accountId", accountId), zap.String("peerId", peerId))
				err := stream.Send(event)
				if err != nil {
					log.Warn("error sending to notify stream", zap.String("receiver accountId", accountId), zap.String("peerId", peerId))
					s.removeStream(eventType, accountId, peerId)
				}
			}
		} else {
			log.Warn("no such recepient", zap.String("accountId", accountId))
		}

	} else {
		log.Warn("unknown notify event type", zap.Int("eventType", int(eventType)))
	}
}
