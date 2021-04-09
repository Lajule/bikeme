package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/bmizerany/pat"
	"github.com/hashicorp/raft"
	"github.com/spf13/viper"
)

type config struct {
	NodeID            string        `mapstructure:"node_id"`
	TrailingLogs      uint64        `mapstructure:"trailing_logs"`
	LogStoreFile      string        `mapstructure:"log_store_file"`
	LogCacheSize      int           `mapstructure:"log_cache_size"`
	SnapshotDir       string        `mapstructure:"snapshot_dir"`
	SnapshotInterval  time.Duration `mapstructure:"snapshot_interval"`
	SnapshotThreshold uint64        `mapstructure:"snapshot_threshold"`
	SnapshotRetain    int           `mapstructure:"snapshot_retain"`
	RaftAddr          string        `mapstructure:"raft_addr"`
	MaxPool           int           `mapstructure:"max_pool"`
	TCPTimeout        time.Duration `mapstructure:"tcp_timeout"`
	BikeStoreFile     string        `mapstructure:"bike_store_file"`
	APIAddr           string        `mapstructure:"api_addr"`
	Graceful          time.Duration `mapstructure:"graceful"`
}

var (
	configFile string
	bootstrap  bool
)

func init() {
	flag.StringVar(&configFile, "config", "config.yaml", "Config filename")
	flag.BoolVar(&bootstrap, "bootstrap", false, "Bootstrap cluster")
}

func main() {
	flag.Parse()

	viper.SetConfigName(configFile)
	viper.SetConfigType("yaml")
	viper.AddConfigPath("/etc/bikeme/")
	viper.AddConfigPath("$HOME/.bikeme")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(err)
	}

	var c config
	if err := viper.Unmarshal(&c); err != nil {
		log.Fatal(err)
	}

	logger, err := newSTDLogger()
	if err != nil {
		log.Fatal(err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(c.NodeID)
	rc.Logger = logger

	rc.TrailingLogs = c.TrailingLogs

	store, err := newLogStore(c.LogStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	cacheStore, err := raft.NewLogCache(c.LogCacheSize, store)
	if err != nil {
		log.Fatal(err)
	}

	rc.SnapshotInterval = c.SnapshotInterval
	rc.SnapshotThreshold = c.SnapshotThreshold

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(c.SnapshotDir, c.SnapshotRetain, logger)
	if err != nil {
		log.Fatal(err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", c.RaftAddr)
	if err != nil {
		log.Fatal(err)
	}

	transport, err := raft.NewTCPTransportWithLogger(c.RaftAddr, tcpAddr, c.MaxPool, c.TCPTimeout, logger)
	if err != nil {
		log.Fatal(err)
	}

	bikeStore, err := newBikeStore(c.BikeStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	fsmStore, err := newFSM(bikeStore)
	if err != nil {
		log.Fatal(err)
	}

	cluster, err := raft.NewRaft(rc, fsmStore, cacheStore, store, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(c.NodeID),
					Address: transport.LocalAddr(),
				},
			},
		}

		f := cluster.BootstrapCluster(configuration)
		if err = f.Error(); err != nil {
			log.Fatal(err)
		}
	}

	m := pat.New()
	m.Get("/hello/:name", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		io.WriteString(w, "hello, "+req.URL.Query().Get(":name")+"!\n")
	}))

	srv := &http.Server{
		Addr:    c.APIAddr,
		Handler: m,
	}

	go func() {
		log.Print("Server is running")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Print("Server shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), c.Graceful)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
