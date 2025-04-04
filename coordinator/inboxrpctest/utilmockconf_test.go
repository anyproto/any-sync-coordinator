package inboxrpctest

import (
	"context"
	"time"

	accountService "github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/go-chash"
)

type mockNodeConf struct {
	id            string
	networkId     string
	configuration nodeconf.Configuration
}

func (m *mockNodeConf) NetworkCompatibilityStatus() nodeconf.NetworkCompatibilityStatus {
	return nodeconf.NetworkCompatibilityStatusOk
}

func (m *mockNodeConf) Init(a *app.App) (err error) {
	accountKeys := a.MustComponent(accountService.CName).(accountService.Service).Account()

	sk := accountKeys.SignKey
	networkId := sk.GetPublic().Network()
	node := nodeconf.Node{
		PeerId:    accountKeys.PeerId,
		Addresses: []string{"127.0.0.1:4430"},
		Types:     []nodeconf.NodeType{nodeconf.NodeTypeTree},
	}
	m.id = networkId
	m.networkId = networkId
	m.configuration = nodeconf.Configuration{
		Id:           networkId,
		NetworkId:    networkId,
		Nodes:        []nodeconf.Node{node},
		CreationTime: time.Now(),
	}
	return nil
}

func (m *mockNodeConf) Name() (name string) {
	return nodeconf.CName
}

func (m *mockNodeConf) Run(ctx context.Context) (err error) {
	return nil
}

func (m *mockNodeConf) Close(ctx context.Context) (err error) {
	return nil
}

func (m *mockNodeConf) Id() string {
	return ""
}

func (m *mockNodeConf) Configuration() nodeconf.Configuration {
	return m.configuration
}

func (m *mockNodeConf) NodeIds(spaceId string) []string {
	var nodeIds []string
	return nodeIds
}

func (m *mockNodeConf) IsResponsible(spaceId string) bool {
	return true
}

func (m *mockNodeConf) FilePeers() []string {
	return nil
}

func (m *mockNodeConf) ConsensusPeers() []string {
	return nil
}

func (m *mockNodeConf) CoordinatorPeers() []string {
	return []string{"peer"}
}

func (m *mockNodeConf) NamingNodePeers() []string {
	return nil
}

func (m *mockNodeConf) PaymentProcessingNodePeers() []string {
	return nil
}

func (m *mockNodeConf) PeerAddresses(peerId string) (addrs []string, ok bool) {
	return nil, false
}

func (m *mockNodeConf) CHash() chash.CHash {
	return nil
}

func (m *mockNodeConf) Partition(spaceId string) (part int) {
	return 0
}

func (m *mockNodeConf) NodeTypes(nodeId string) []nodeconf.NodeType {
	return nil
}
