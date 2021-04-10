package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/bmizerany/pat"
	"github.com/hashicorp/raft"
)

type config struct {
	LocalID           string             `json:"local_id"`
	TrailingLogs      uint64             `json:"trailing_logs"`
	LogStoreFile      string             `json:"log_store_file"`
	LogCacheSize      int                `json:"log_cache_size"`
	SnapshotDir       string             `json:"snapshot_dir"`
	SnapshotInterval  string             `json:"snapshot_interval"`
	SnapshotThreshold uint64             `json:"snapshot_threshold"`
	SnapshotRetain    int                `json:"snapshot_retain"`
	RAFTAddr          string             `json:"raft_addr"`
	MaxPool           int                `json:"max_pool"`
	TCPTimeout        string             `json:"tcp_timeout"`
	Configuration     raft.Configuration `json:"configuration"`
	BikeStoreFile     string             `json:"bike_store_file"`
	APIAddr           string             `json:"api_addr"`
	Graceful          string             `json:"graceful"`
}

type app struct {
	config  *config
	cluster *raft.Raft
	store   *bikeStore
}

var (
	cfgFile string
	bikeme  *app
)

func main() {
	flag.StringVar(&cfgFile, "config", "config.json", "Config filename")
	flag.Parse()

	data, err := os.ReadFile(cfgFile)
	if err != nil {
		log.Fatal(err)
	}

	cfg := config{}
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatal(err)
	}

	bikeme := &app{
		config: &cfg,
	}

	bikeme.store, err = newBikeStore(cfg.BikeStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	logger, err := newSTDLogger()
	if err != nil {
		log.Fatal(err)
	}

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(cfg.LocalID)
	c.Logger = logger
	c.TrailingLogs = cfg.TrailingLogs

	store, err := newLogStore(cfg.LogStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	cacheStore, err := raft.NewLogCache(cfg.LogCacheSize, store)
	if err != nil {
		log.Fatal(err)
	}

	c.SnapshotInterval = parseDuration(cfg.SnapshotInterval)
	c.SnapshotThreshold = cfg.SnapshotThreshold

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(cfg.SnapshotDir, cfg.SnapshotRetain, logger)
	if err != nil {
		log.Fatal(err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", cfg.RAFTAddr)
	if err != nil {
		log.Fatal(err)
	}

	transport, err := raft.NewTCPTransportWithLogger(cfg.RAFTAddr, tcpAddr, cfg.MaxPool, parseDuration(cfg.TCPTimeout), logger)
	if err != nil {
		log.Fatal(err)
	}

	f, err := newFSM(bikeme.store)
	if err != nil {
		log.Fatal(err)
	}

	bikeme.cluster, err = raft.NewRaft(c, f, cacheStore, store, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	if len(cfg.Configuration.Servers) > 0 {
		log.Print("Cluster is bootstraping")

		future := bikeme.cluster.BootstrapCluster(cfg.Configuration)

		if err := future.Error(); err != nil {
			log.Fatal(err)
		}
	}

	m := pat.New()
	m.Get("/bikes", &getBikesHandler{
		s: bikeme.store,
	})
	m.Get("/bikes/:id", &getBikeHandler{
		s: bikeme.store,
	})
	m.Post("/bikes", &postBikeHandler{
		s: bikeme.store,
	})

	srv := &http.Server{
		Addr:    cfg.APIAddr,
		Handler: m,
	}

	go func() {
		log.Printf("Server is listening: %s\n", cfg.APIAddr)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Print("Server is shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), parseDuration(cfg.Graceful))
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}

func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Fatal(err)
	}

	return d
}
