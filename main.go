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

type configuration struct {
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

type application struct {
	config  *configuration
	cluster *raft.Raft
	store   *bikeStore
}

var (
	configFile string
	app        *application
)

func main() {
	flag.StringVar(&configFile, "config", "config.json", "Config filename")
	flag.Parse()

	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	config := configuration{}
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatal(err)
	}

	app := &application{
		config: &config,
	}

	app.store, err = newBikeStore(config.BikeStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	logger, err := newSTDLogger()
	if err != nil {
		log.Fatal(err)
	}

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(config.LocalID)
	c.Logger = logger
	c.TrailingLogs = config.TrailingLogs

	store, err := newLogStore(config.LogStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	cacheStore, err := raft.NewLogCache(config.LogCacheSize, store)
	if err != nil {
		log.Fatal(err)
	}

	c.SnapshotInterval = parseDuration(config.SnapshotInterval)
	c.SnapshotThreshold = config.SnapshotThreshold

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(config.SnapshotDir, config.SnapshotRetain, logger)
	if err != nil {
		log.Fatal(err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", config.RAFTAddr)
	if err != nil {
		log.Fatal(err)
	}

	transport, err := raft.NewTCPTransportWithLogger(config.RAFTAddr, tcpAddr, config.MaxPool, parseDuration(config.TCPTimeout), logger)
	if err != nil {
		log.Fatal(err)
	}

	f, err := newFSM(app.store)
	if err != nil {
		log.Fatal(err)
	}

	app.cluster, err = raft.NewRaft(c, f, cacheStore, store, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	if len(config.Configuration.Servers) > 0 {
		log.Print("Cluster is bootstraping")

		future := app.cluster.BootstrapCluster(config.Configuration)
		if err := future.Error(); err != nil {
			log.Fatal(err)
		}
	}

	m := pat.New()
	m.Get("/bikes", &getBikesHandler{
		a: app,
	})
	m.Get("/bikes/:id", &getBikeHandler{
		a: app,
	})
	m.Post("/bikes", &postBikeHandler{
		a: app,
	})

	srv := &http.Server{
		Addr:    config.APIAddr,
		Handler: m,
	}

	go func() {
		log.Printf("Server is listening: %s\n", config.APIAddr)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Print("Server is shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), parseDuration(config.Graceful))
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
