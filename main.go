package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
	NodeID            string `json:"node_id"`
	TrailingLogs      uint64 `json:"trailing_logs"`
	LogStoreFile      string `json:"log_store_file"`
	LogCacheSize      int    `json:"log_cache_size"`
	SnapshotDir       string `json:"snapshot_dir"`
	SnapshotInterval  string `json:"snapshot_interval"`
	SnapshotThreshold uint64 `json:"snapshot_threshold"`
	SnapshotRetain    int    `json:"snapshot_retain"`
	RaftAddr          string `json:"raft_addr"`
	MaxPool           int    `json:"max_pool"`
	TCPTimeout        string `json:"tcp_timeout"`
	BikeStoreFile     string `json:"bike_store_file"`
	APIAddr           string `json:"api_addr"`
	Graceful          string `json:"graceful"`
}

var (
	cfg     *config
	cluster *raft.Raft
	bs      *bikeStore
	cfgFile string
)

func init() {
	flag.StringVar(&cfgFile, "config", "config.json", "Config filename")
	flag.Usage = func() {
		fmt.Printf("Usage: %s [followerNodeID,followerRaftAddr]...\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	data, err := os.ReadFile(cfgFile)
	if err != nil {
		log.Fatal(err)
	}

	cfg = &config{}
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatal(err)
	}

	bs, err = newBikeStore(cfg.BikeStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	logger, err := newSTDLogger()
	if err != nil {
		log.Fatal(err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.Logger = logger
	rc.TrailingLogs = cfg.TrailingLogs

	ls, err := newLogStore(cfg.LogStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	cacheStore, err := raft.NewLogCache(cfg.LogCacheSize, ls)
	if err != nil {
		log.Fatal(err)
	}

	rc.SnapshotInterval = parseDuration(cfg.SnapshotInterval)
	rc.SnapshotThreshold = cfg.SnapshotThreshold

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(cfg.SnapshotDir, cfg.SnapshotRetain, logger)
	if err != nil {
		log.Fatal(err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		log.Fatal(err)
	}

	transport, err := raft.NewTCPTransportWithLogger(cfg.RaftAddr, tcpAddr, cfg.MaxPool, parseDuration(cfg.TCPTimeout), logger)
	if err != nil {
		log.Fatal(err)
	}

	fs, err := newFSM(bs)
	if err != nil {
		log.Fatal(err)
	}

	cluster, err = raft.NewRaft(rc, fs, cacheStore, ls, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	if flag.NArg() > 0 {
		bootstrapCluster()
	}

	m := pat.New()
	m.Get("/bikes", http.HandlerFunc(getBikes))
	m.Get("/bikes/:id", http.HandlerFunc(getBikes))
	m.Post("/bikes", http.HandlerFunc(postBike))

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

func bootstrapCluster() {
	servers := []raft.Server{
		{
			ID:      raft.ServerID(cfg.NodeID),
			Address: raft.ServerAddress(cfg.RaftAddr),
		},
	}

	for _, follower := range flag.Args() {
		var followerNodeID string
		var followerRaftAddr string

		if n, _ := fmt.Sscanf(follower, "%s,%s", &followerNodeID, &followerRaftAddr); n != 2 {
			log.Fatal("invalid follower parameter")
		}

		servers = append(servers, raft.Server{
			ID:      raft.ServerID(followerNodeID),
			Address: raft.ServerAddress(followerRaftAddr),
		})
	}

	f := cluster.BootstrapCluster(raft.Configuration{
		Servers: servers,
	})
	if err := f.Error(); err != nil {
		log.Fatal(err)
	}
}
