package inbox

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
)

type InboxPacketType int

const (
	InboxPacketTypeDefault InboxPacketType = iota
)

func (t InboxPacketType) String() string {
	return [...]string{"Default"}[t]
}

type InboxKeyType int

const (
	InboxKeyTypeEd25519 InboxKeyType = iota
)

func (t InboxKeyType) String() string {
	return [...]string{"Ed25519"}[t]
}

type InboxPayloadType int

const (
	InboxPayloadSpaceInvite InboxPayloadType = iota
)

func (t InboxPayloadType) String() string {
	return [...]string{"InboxPayloadSpaceInvite"}[t]
}

type InboxMessage struct {
	Id         string          `bson:"_id"`
	PacketType InboxPacketType `bson:"packetType"`
	Packet     InboxPacket     `bson:"packet"`
}

type InboxPacket struct {
	KeyType          InboxKeyType `bson:"keyType"`
	SenderIdentity   string       `bson:"senderIdentity"`
	ReceiverIdentity string       `bson:"receiverIdentity"`
	SenderSignature  []byte       `bson:"senderSignature"`
	Payload          InboxPayload `bson:"payload"`
}

type InboxPayload struct {
	PayloadType InboxPayloadType `bson:"payloadType"`
	Timestamp   time.Time        `bson:"timestamp"`
	Body        []byte           `bson:"body"`
}

func InboxMessageFromRequest(ctx context.Context, in *coordinatorproto.InboxAddMessageRequest) (message *InboxMessage) {
	accountPubKey, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}

	message = &InboxMessage{
		PacketType: InboxPacketType(in.Message.PacketType),
		Packet: InboxPacket{
			KeyType:          InboxKeyType(in.Message.Packet.KeyType),
			SenderSignature:  in.Message.Packet.SenderSignature,
			SenderIdentity:   accountPubKey.Account(),
			ReceiverIdentity: in.Message.Packet.ReceiverIdentity,
			Payload: InboxPayload{
				PayloadType: InboxPayloadType(in.Message.Packet.Payload.PayloadType),
				Timestamp:   time.Now(),
				Body:        in.Message.Packet.Payload.Body,
			},
		},
	}
	return
}
