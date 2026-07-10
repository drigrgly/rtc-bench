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
	Clusters []Cluster `mapstructure:"clusters"`
}

type Cluster struct {
	ClusterId     string            `mapstructure:"cluster-id"`
	Host          string            `mapstructure:"host"`
	TurncatClient TurncatClient     `mapstructure:"turncat-client"`
	TurnServer    TurnServer        `mapstructure:"turn-server"`
	Queries       []PrometheusQuery `mapstructure:"queries"`
	Measurements  []Measurement     `mapstructure:"measurements"`
	Peer          Peer              `mapstructure:"peer"`
}

type TurncatClient struct {
	Log  string `mapstructure:"log"`
	Host string `mapstructure:"host"`
	Port string `mapstructure:"port"`
}

type TurnServer struct {
	Host string `mapstructure:"host"`
	Port string `mapstructure:"port"`
}

type Peer struct {
	Host string `mapstructure:"host"`
	Port string `mapstructure:"port"`
}

type PrometheusQuery struct {
	Name  string `mapstructure:"name"`
	Query string `mapstructure:"query"`
}

type Measurement struct {
	Name          string        `mapstructure:"name"`
	Repeat        int           `mapstructure:"repeat"`
	LoadGenerator LoadGenerator `mapstructure:"load-generator"`
	Offloading    string        `mapstructure:"offloading"`
}

type ClientServer struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type LoadGenerator struct {
	Command string   `mapstructure:"command"`
	Args    []string `mapstructure:"args"`
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

	var wg sync.WaitGroup

	loadingRules := &clientcmd.ClientConfigLoadingRules{
		ExplicitPath: *kubeconfig,
	}

	config, err := loadingRules.Load()
	if err != nil {
		panic(err)
	}

	// Start measurements for each cluster concurrently
	for _, cluster := range cfg.Clusters {
		wg.Add(1)
		go startSameClusterMeasurements(cluster, *config, ctx, &wg)
	}
	wg.Wait()
}
