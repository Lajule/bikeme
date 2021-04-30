package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"
	"github.com/hashicorp/raft"
)

// Configuration is used to load the configuration file.
type Configuration struct {
	LocalID                  string        `json:"local_id"`
	Hostname                 string        `json:"hostname"`
	TrailingLogs             uint64        `json:"trailing_logs"`
	LogStoreFile             string        `json:"log_store_file"`
	LogCacheSize             int           `json:"log_cache_size"`
	SnapshotDir              string        `json:"snapshot_dir"`
	SnapshotInterval         string        `json:"snapshot_interval"`
	SnapshotThreshold        uint64        `json:"snapshot_threshold"`
	SnapshotRetain           int           `json:"snapshot_retain"`
	NoSnapshotRestoreOnStart bool          `json:"no_snapshot_restore_on_start"`
	RAFTPort                 int           `json:"raft_port"`
	MaxPool                  int           `json:"max_pool"`
	TCPTimeout               string        `json:"tcp_timeout"`
	Servers                  []raft.Server `json:"servers"`
	BikeStoreFile            string        `json:"bike_store_file"`
	APIPort                  int           `json:"api_port"`
	Graceful                 string        `json:"graceful"`
}

// Application gives access to the configuration, the Raft cluster and the bike store.
type Application struct {
	Config    *Configuration
	Cluster   *raft.Raft
	BikeStore *BikeStore
}

var (
	// Version contains the program version.
	Version = "development"

	// Bootstrap is used to bootstrap the Raft cluster.
	Bootstrap = flag.Bool("b", false, "Bootstrap cluster")

	// ConfigFile contains the configuration filename.
	ConfigFile = flag.String("c", "config.json", "Config filename")
)

//go:embed *.tmpl
var content embed.FS

func main() {
	log.Printf("Starting bikeme %s\n", Version)

	flag.Parse()

	app := &Application{
		Config: &Configuration{
			Hostname:                 "127.0.0.1",
			TrailingLogs:             5,
			LogStoreFile:             "logs.db",
			LogCacheSize:             16,
			SnapshotDir:              "snapshots",
			SnapshotInterval:         "20s",
			SnapshotThreshold:        10,
			SnapshotRetain:           1,
			NoSnapshotRestoreOnStart: false,
			RAFTPort:                 3001,
			MaxPool:                  3,
			TCPTimeout:               "1s",
			APIPort:                  8001,
			Graceful:                 "5s",
		},
	}

	data, err := os.ReadFile(*ConfigFile)
	if err != nil {
		log.Fatal(err)
	}

	if err := json.Unmarshal(data, app.Config); err != nil {
		log.Fatal(err)
	}

	app.BikeStore, err = NewBikeStore(app.Config.BikeStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(app.Config.LocalID)
	raftConfig.TrailingLogs = app.Config.TrailingLogs
	raftConfig.SnapshotInterval = parseDuration(app.Config.SnapshotInterval)
	raftConfig.SnapshotThreshold = app.Config.SnapshotThreshold
	raftConfig.NoSnapshotRestoreOnStart = app.Config.NoSnapshotRestoreOnStart

	raftConfig.Logger, err = NewSTDLogger()
	if err != nil {
		log.Fatal(err)
	}

	logStore, err := NewLogStore(app.Config.LogStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	cacheStore, err := raft.NewLogCache(app.Config.LogCacheSize, logStore)
	if err != nil {
		log.Fatal(err)
	}

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(app.Config.SnapshotDir, app.Config.SnapshotRetain, raftConfig.Logger)
	if err != nil {
		log.Fatal(err)
	}

	bindAddr := fmt.Sprintf("%s:%d", app.Config.Hostname, app.Config.RAFTPort)
	advertise, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		log.Fatal(err)
	}

	transport, err := raft.NewTCPTransportWithLogger(bindAddr, advertise, app.Config.MaxPool, parseDuration(app.Config.TCPTimeout), raftConfig.Logger)
	if err != nil {
		log.Fatal(err)
	}

	fsm, err := NewFSM(app.BikeStore)
	if err != nil {
		log.Fatal(err)
	}

	app.Cluster, err = raft.NewRaft(raftConfig, fsm, cacheStore, logStore, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	if *Bootstrap {
		log.Print("Bootstrapping cluster")

		future := app.Cluster.BootstrapCluster(raft.Configuration{
			Servers: app.Config.Servers,
		})
		if err := future.Error(); err != nil {
			log.Fatal(err)
		}
	}

	tmpl, err := template.ParseFS(content, "*.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	r := mux.NewRouter()
	r.Use(Logger)
	r.Use(CORS)

	r.Handle("/", &IndexHandler{
		Application: app,
		Template:    tmpl,
	}).Methods(http.MethodGet)

	r.Handle("/bikes", &GetBikesHandler{
		Application: app,
	}).Methods(http.MethodGet)

	r.Handle("/bikes/{id:[0-9]+}", &GetBikeHandler{
		Application: app,
	}).Methods(http.MethodGet)

	r.Handle("/bikes", &PostBikeHandler{
		Application: app,
	}).Methods(http.MethodPost)

	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", app.Config.Hostname, app.Config.APIPort),
		Handler: r,
	}

	go func() {
		log.Printf("Listening %d\n", app.Config.APIPort)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Print("Shutting down server")

	ctx, cancel := context.WithTimeout(context.Background(), parseDuration(app.Config.Graceful))
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}

	shutdown := app.Cluster.Shutdown()
	if err := shutdown.Error(); err != nil {
		log.Fatal(err)
	}

	log.Print("Bye bye")
}

func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Fatal(err)
	}

	return d
}
