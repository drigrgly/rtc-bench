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

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type Config struct {
	Measurements []Measurement `mapstructure:"measurements"`
}

type Measurement struct {
	Name          string        `mapstructure:"name"`
	Client        ClientServer  `mapstructure:"client"`
	LoadGenerator LoadGenerator `mapstructure:"loadGenerator"`
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

	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// Initialize logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	//logger := slog.New(logutils.NewCopyHandler(slog.NewTextHandler(os.Stdout, nil)))
	slog.SetDefault(logger)

	// use the current context in kubeconfig
	kubeCfg, kubeErr := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if kubeErr != nil {
		panic(kubeErr.Error())
	}

	// create the clientset
	clientset, kubeErr := kubernetes.NewForConfig(kubeCfg)
	if kubeErr != nil {
		panic(kubeErr.Error())
	}

	// Dynamic client (for Gateway CRD)
	dynClient, err := dynamic.NewForConfig(kubeCfg)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// Set up viper to read the config file
	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	var cfg Config

	// Find and read the config file
	err = viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	// Unmarshal the config into the struct
	err = viper.Unmarshal(&cfg)
	if err != nil {
		panic(fmt.Errorf("fatal error unmarshaling file: %w", err))
	}

	var wg sync.WaitGroup

	// Loop through the measurements and get the necessary information for each measurement
	for _, measurement := range cfg.Measurements {

		turnServerAddress := "turn://user-1:pass-1@"

		// Get the TURN server IP and port
		turnIP, err := getTurnServerIP(clientset, ctx)
		if err != nil {
			panic(err)
		}

		turnPort, err := getTurnServerPortFromGateway(dynClient, ctx)
		if err != nil {
			panic(err)
		}

		turnServerAddress += fmt.Sprintf("%s:", turnIP)
		turnServerAddress += fmt.Sprintf("%s?transport=udp", turnPort)

		// Get Iperf cluster peer host address
		peerHostAddress, err := getIperfServerClusterIP(clientset, ctx, true)
		if err != nil {
			panic(err)
		}

		measurement.Turncat.PeerHostAddress = peerHostAddress
		measurement.Turncat.TurnServerAddress = turnServerAddress

		// Start in a separate goroutine to allow multiple measurements to run concurrently
		wg.Add(1)
		go startMeasurement(measurement, &wg)
	}

	wg.Wait()
}
