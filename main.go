package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/viper"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type Config struct {
	Measurements []Measurement `mapstructure:"measurements"`
}

type Measurement struct {
	Name          string        `mapstructure:"name"`
	ClusterId     string        `mapstructure:"cluster-id"`
	Client        ClientServer  `mapstructure:"client"`
	LoadGenerator LoadGenerator `mapstructure:"load-generator"`
	Turncat       Turncat       `mapstructure:"turncat"`
}

type ClientServer struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type LoadGenerator struct {
	Command string   `mapstructure:"command"`
	Args    []string `mapstructure:"args"`
}

type Turncat struct {
	Log               string `mapstructure:"log"`
	ClientAddress     string `mapstructure:"clientAddress"`
	TurnServerAddress string `mapstructure:"turnServerAddress"`
	PeerHostAddress   string `mapstructure:"peerHostAddress"`
}

func main() {
	// Initialize logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	//logger := slog.New(logutils.NewCopyHandler(slog.NewTextHandler(os.Stdout, nil)))
	slog.SetDefault(logger)

	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	ctx := context.Background()

	// Set up viper to read the config file
	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	var cfg Config

	// Find and read the config file
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	// Unmarshal the config into the struct
	err = viper.Unmarshal(&cfg)
	if err != nil {
		panic(fmt.Errorf("fatal error unmarshaling file: %w", err))
	}

	// Separate the measurements based on the cluster id
	clusterMeasurements := make(map[string][]Measurement)
	for _, measurement := range cfg.Measurements {
		clusterMeasurements[measurement.ClusterId] = append(clusterMeasurements[measurement.ClusterId], measurement)
	}

	clusterIds := make([]string, 0, len(clusterMeasurements))
	for id := range clusterMeasurements {
		clusterIds = append(clusterIds, id)
	}

	var wg sync.WaitGroup

	loadingRules := &clientcmd.ClientConfigLoadingRules{
		ExplicitPath: *kubeconfig,
	}

	config, err := loadingRules.Load()
	if err != nil {
		panic(err)
	}

	for clusterId, measurements := range clusterMeasurements {
		go startSameClusterMeasurements(clusterId, measurements, *config, ctx, &wg)
	}
	wg.Wait()
}
