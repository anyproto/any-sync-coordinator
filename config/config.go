package config

import (
	"os"

	commonaccount "github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/rpc"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"github.com/anyproto/any-sync/nodeconf"
	"gopkg.in/yaml.v3"

	"github.com/anyproto/any-sync-coordinator/accountlimit"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/inbox"
	"github.com/anyproto/any-sync-coordinator/spacestatus"
)

const CName = "config"

func NewFromFile(path string) (c *Config, err error) {
	c = &Config{}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err = yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}
	return
}

type Config struct {
	Account                  commonaccount.Config     `yaml:"account"`
	Drpc                     rpc.Config               `yaml:"drpc"`
	Metric                   metric.Config            `yaml:"metric"`
	Network                  nodeconf.Configuration   `yaml:"network"`
	NetworkStorePath         string                   `yaml:"networkStorePath"`
	NetworkUpdateIntervalSec int                      `yaml:"networkUpdateIntervalSec"`
	Mongo                    db.Mongo                 `yaml:"mongo"`
	SpaceStatus              spacestatus.Config       `yaml:"spaceStatus"`
	Inbox                    inbox.Config             `yaml:"inbox"`
	Yamux                    yamux.Config             `yaml:"yamux"`
	Quic                     quic.Config              `yaml:"quic"`
	AccountLimits            accountlimit.SpaceLimits `yaml:"defaultLimits"`
}

func (c *Config) Init(a *app.App) (err error) {
	return
}

func (c Config) Name() (name string) {
	return CName
}

func (c Config) GetAccount() commonaccount.Config {
	return c.Account
}

func (c Config) GetDrpc() rpc.Config {
	return c.Drpc
}

func (c Config) GetMetric() metric.Config {
	return c.Metric
}

func (c Config) GetNodeConf() nodeconf.Configuration {
	return c.Network
}

func (c Config) GetMongo() db.Mongo {
	return c.Mongo
}

func (c Config) GetSpaceStatus() spacestatus.Config {
	return c.SpaceStatus
}

func (c Config) GetInbox() inbox.Config {
	return c.Inbox
}

func (c Config) GetNodeConfStorePath() string {
	return c.NetworkStorePath
}

func (c Config) GetNodeConfUpdateInterval() int {
	return c.NetworkUpdateIntervalSec
}

func (c Config) GetYamux() yamux.Config {
	return c.Yamux
}

func (c Config) GetQuic() quic.Config {
	return c.Quic
}

func (c Config) GetAccountLimit() accountlimit.SpaceLimits {
	return c.AccountLimits
}
