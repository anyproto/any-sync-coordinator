package inbox

import "time"

type InboxPacketType int

const (
	Default InboxPacketType = iota
)

func (t InboxPacketType) String() string {
	return [...]string{"Default"}[t]
}

type InboxKeyType int

const (
	Ed25519 InboxKeyType = iota
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
	KeyType    InboxKeyType    `bson:"keyType"`
	PacketType InboxPacketType `bson:"packetType"`
	Packet     InboxPacket     `bson:"packet"`
}

type InboxPacket struct {
	SenderIdentity   []byte       `bson:"senderIdentity"`
	ReceiverIdentity []byte       `bson:"receiverIdentity"`
	SenderSignature  []byte       `bson:"senderSignature"`
	Payload          InboxPayload `bson:"payload"`
}

type InboxPayload struct {
	PayloadType InboxPayloadType `bson:"payloadType"`
	Timestamp   time.Time        `bson:"timestamp"`
	Body        []byte           `bson:"body"`
}
