package config

import (
	"github.com/anytypeio/any-sync-coordinator/db"
	"github.com/anytypeio/any-sync-coordinator/spacestatus"
	commonaccount "github.com/anytypeio/any-sync/accountservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/metric"
	"github.com/anytypeio/any-sync/net"
	"github.com/anytypeio/any-sync/nodeconf"
	"gopkg.in/yaml.v3"
	"os"
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
	Account           commonaccount.Config   `yaml:"account"`
	GrpcServer        net.Config             `yaml:"grpcServer"`
	Metric            metric.Config          `yaml:"metric"`
	NetConf           nodeconf.Configuration `yaml:"netConf"`
	NodeConfStorePath string                 `yaml:"nodeConfStorePath"`
	Mongo             db.Mongo               `yaml:"mongo"`
	SpaceStatus       spacestatus.Config     `yaml:"spaceStatus"`
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

func (c Config) GetNet() net.Config {
	return c.GrpcServer
}

func (c Config) GetMetric() metric.Config {
	return c.Metric
}

func (c Config) GetNodeConf() nodeconf.Configuration {
	return c.NetConf
}

func (c Config) GetMongo() db.Mongo {
	return c.Mongo
}

func (c Config) GetSpaceStatus() spacestatus.Config {
	return c.SpaceStatus
}

func (c Config) GetNodeConfStorePath() string {
	return c.NodeConfStorePath
}
