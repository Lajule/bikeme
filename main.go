package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"html/template"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/bmizerany/pat"
	"github.com/hashicorp/raft"
)

// Configuration is used to load the configuration file.
type Configuration struct {
	LocalID           string        `json:"local_id"`
	TrailingLogs      uint64        `json:"trailing_logs"`
	LogStoreFile      string        `json:"log_store_file"`
	LogCacheSize      int           `json:"log_cache_size"`
	SnapshotDir       string        `json:"snapshot_dir"`
	SnapshotInterval  string        `json:"snapshot_interval"`
	SnapshotThreshold uint64        `json:"snapshot_threshold"`
	SnapshotRetain    int           `json:"snapshot_retain"`
	RAFTAddr          string        `json:"raft_addr"`
	MaxPool           int           `json:"max_pool"`
	TCPTimeout        string        `json:"tcp_timeout"`
	Servers           []raft.Server `json:"servers"`
	BikeStoreFile     string        `json:"bike_store_file"`
	APIAddr           string        `json:"api_addr"`
	Graceful          string        `json:"graceful"`
}

// Application gives access to the configuration, the Raft cluster and the bike store.
type Application struct {
	Configuration *Configuration
	Cluster       *raft.Raft
	BikeStore     *BikeStore
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

	data, err := os.ReadFile(*ConfigFile)
	if err != nil {
		log.Fatal(err)
	}

	configuration := Configuration{}
	if err := json.Unmarshal(data, &configuration); err != nil {
		log.Fatal(err)
	}

	application := &Application{
		Configuration: &configuration,
	}

	application.BikeStore, err = NewBikeStore(configuration.BikeStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	logger, err := NewSTDLogger()
	if err != nil {
		log.Fatal(err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(configuration.LocalID)
	raftConfig.Logger = logger
	raftConfig.TrailingLogs = configuration.TrailingLogs

	logStore, err := NewLogStore(configuration.LogStoreFile)
	if err != nil {
		log.Fatal(err)
	}

	cacheStore, err := raft.NewLogCache(configuration.LogCacheSize, logStore)
	if err != nil {
		log.Fatal(err)
	}

	raftConfig.SnapshotInterval = parseDuration(configuration.SnapshotInterval)
	raftConfig.SnapshotThreshold = configuration.SnapshotThreshold

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(configuration.SnapshotDir, configuration.SnapshotRetain, logger)
	if err != nil {
		log.Fatal(err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", configuration.RAFTAddr)
	if err != nil {
		log.Fatal(err)
	}

	transport, err := raft.NewTCPTransportWithLogger(configuration.RAFTAddr, tcpAddr, configuration.MaxPool, parseDuration(configuration.TCPTimeout), logger)
	if err != nil {
		log.Fatal(err)
	}

	fsm, err := NewFSM(application.BikeStore)
	if err != nil {
		log.Fatal(err)
	}

	application.Cluster, err = raft.NewRaft(raftConfig, fsm, cacheStore, logStore, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	if *Bootstrap {
		log.Print("Bootstrapping cluster")

		future := application.Cluster.BootstrapCluster(raft.Configuration{
			Servers: configuration.Servers,
		})
		if err := future.Error(); err != nil {
			log.Fatal(err)
		}
	}

	tmpl, err := template.ParseFS(content, "*.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	m := pat.New()

	m.Get("/", &IndexHandler{
		Application: application,
		Template:    tmpl,
	})

	m.Get("/bikes", &GetBikesHandler{
		Application: application,
	})

	m.Get("/bikes/:id", &GetBikeHandler{
		Application: application,
	})

	m.Post("/bikes", &PostBikeHandler{
		Application: application,
	})

	srv := &http.Server{
		Addr:    configuration.APIAddr,
		Handler: CORS(m),
	}

	go func() {
		log.Printf("Listening %s\n", configuration.APIAddr)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Print("Shutting down server")

	ctx, cancel := context.WithTimeout(context.Background(), parseDuration(configuration.Graceful))
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
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
