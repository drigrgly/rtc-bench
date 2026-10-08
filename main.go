package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/viper"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type Config struct {
	Clusters []Cluster `mapstructure:"clusters"`
}

type Cluster struct {
	ClusterId     string        `mapstructure:"cluster-id"`
	Host          string        `mapstructure:"host"`
	TurncatClient TurncatClient `mapstructure:"turncat-client"`
	TurnServer    TurnServer    `mapstructure:"turn-server"` //Debug and documentation purposes
	Measurements  []Measurement `mapstructure:"measurements"`
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

type PrometheusQuery struct {
	Name  string `mapstructure:"name"`
	Query string `mapstructure:"query"`
}

type Measurement struct {
	Name          string            `mapstructure:"name"`
	Repeat        int               `mapstructure:"repeat"`
	LoadGenerator LoadGenerator     `mapstructure:"load-generator"`
	Offloading    string            `mapstructure:"offloading"`
	Queries       []PrometheusQuery `mapstructure:"queries"`
	BufferSeconds int               `mapstructure:"buffer-seconds"`
	Resolution    float32           `mapstructure:"resolution"`
}

type LoadGenerator struct {
	Command string   `mapstructure:"command"`
	Args    []string `mapstructure:"args"`
}

type MeasurementMetaData struct {
	Measurement            *Measurement
	CollectionOutputDir    string
	TurncatClientAddress   string
	TurnServerAddress      string
	InitialStartTime       time.Time
	IndividualMeasurements []IndividualMeasurementMetaData
}

type IndividualMeasurementMetaData struct {
	Count               int
	StartTime           time.Time
	EndTime             time.Time
	BufferSeconds       time.Duration
	Resolution          time.Duration
	LoadGeneratorOutput bytes.Buffer
}

func main() {
	// Configure logging
	logDir := "logs"
	filename := time.Now().Format("rtc-bench-2006-01-02-150305.log")

	if err := os.MkdirAll(logDir, 0o755); err != nil {
		slog.Error("failed to create log directory", "error", err)
	}

	logFile, err := os.OpenFile(
		filepath.Join(logDir, filename),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		slog.Error("failed to create log file", "error", err)
	}

	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, logFile), nil))
	slog.SetDefault(logger)

	// Load configurations
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

	slog.Info("Reading config file...")
	// Find and read the config file
	err = viper.ReadInConfig()
	if err != nil {
		slog.Error("fatal error config file: %w", "error", err)
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	// Unmarshal the config into the struct
	err = viper.Unmarshal(&cfg)
	if err != nil {
		slog.Error("fatal error unmarshaling file: %w", "error", err)
		panic(fmt.Errorf("fatal error unmarshaling file: %w", err))
	}

	var wg sync.WaitGroup

	loadingRules := &clientcmd.ClientConfigLoadingRules{
		ExplicitPath: *kubeconfig,
	}

	config, err := loadingRules.Load()
	if err != nil {
		slog.Error("Error loading kubeconfig", "error", err)
		panic(err)
	}

	// Start measurements for each cluster concurrently
	for _, cluster := range cfg.Clusters {
		wg.Add(1)
		go StartSameClusterMeasurements(&cluster, config, ctx, &wg)
	}
	wg.Wait()
}
