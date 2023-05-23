package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/anyproto/any-sync-coordinator/config"
	"github.com/anyproto/any-sync-coordinator/db"
	"github.com/anyproto/any-sync-coordinator/nodeconfsource"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/nodeconf"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"os"
)

var log = logger.NewNamed("main")

var (
	flagConfigFile = flag.String("c", "etc/any-sync-coordinator.yml", "path to config file")
	flagVersion    = flag.Bool("v", false, "show version and exit")
	flagHelp       = flag.Bool("h", false, "show help and exit")

	flagNetwork       = flag.String("n", "", "path to the yml network configuration file")
	flagNetworkEnable = flag.Bool("e", false, "enable new configuration")
)

func main() {
	flag.Parse()
	if *flagHelp {
		flag.PrintDefaults()
		return
	}

	// create app
	ctx := context.Background()
	a := new(app.App)

	// open config file
	conf, err := config.NewFromFile(*flagConfigFile)
	if err != nil {
		log.Fatal("can't open config file", zap.Error(err))
	}

	ncs := nodeconfsource.New()

	// bootstrap components
	a.Register(conf).Register(db.New()).Register(ncs)

	if err = a.Start(ctx); err != nil {
		log.Fatal("can't start app", zap.Error(err))
	}

	data, err := os.ReadFile(*flagNetwork)
	if err != nil {
		log.Fatal("can't open network configuration file", zap.Error(err))
	}
	var nodeConf nodeconf.Configuration
	if err = yaml.Unmarshal(data, &nodeConf); err != nil {
		log.Fatal("can't parse network configuration file", zap.Error(err))
	}

	id, err := ncs.Add(nodeConf, *flagNetworkEnable)
	if err != nil {
		log.Fatal("can't save configuration", zap.Error(err))
	}
	fmt.Println(id)
}
