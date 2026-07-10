package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	promApi "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	kubeApi "k8s.io/client-go/tools/clientcmd/api"
)

func startSameClusterMeasurements(cluster Cluster, config kubeApi.Config, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// clusterId <=> context name mapping
	contextName := cluster.ClusterId
	configOverrides := &clientcmd.ConfigOverrides{
		CurrentContext: contextName,
	}

	clientConfig := clientcmd.NewNonInteractiveClientConfig(
		config,
		contextName,
		configOverrides,
		nil,
	)

	// Use the selected context (based on id)
	kubeCfg, err := clientConfig.ClientConfig()
	if err != nil {
		panic(err)
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

	// Get Iperf cluster peer address
	peerHost, peerPort, err := getIperfServerClusterIP(clientset, ctx, true)
	if err != nil {
		panic(err)
	}

	cluster.Peer.Host = peerHost
	cluster.Peer.Port = peerPort

	// Get the TURN server IP and port
	turnIP, err := getTurnServerIP(clientset, ctx)
	if err != nil {
		panic(err)
	}

	turnPort, err := getTurnServerPortFromGateway(dynClient, ctx)
	if err != nil {
		panic(err)
	}

	cluster.TurnServer.Host = turnIP
	cluster.TurnServer.Port = turnPort

	for _, measurement := range cluster.Measurements {
		startMeasurement(cluster, measurement)
	}

}

func startMeasurement(cluster Cluster, measurement Measurement) {

	// Construct the client server address
	turncatClientAddress := fmt.Sprintf("udp://%s:%s", cluster.TurncatClient.Host, cluster.TurncatClient.Port)

	//turnServerAddress := fmt.Sprintf("turn://%s:%s@%s:%s?transport=udp", turncatUsername, turncatPassword, cluster.TurnServer.Host, cluster.TurnServer.Port)

	// Redact the password in the log
	redactedTurnServerAddress := fmt.Sprintf("turn://%s:%s@%s:%s?transport=udp", "***", "***", cluster.TurnServer.Host, cluster.TurnServer.Port)

	// Construct the peer address
	peerAddress := fmt.Sprintf("udp://%s:%s", cluster.Peer.Host, cluster.Peer.Port)

	slog.Info("Measurement configuration", "name", measurement.Name, "Turncat client", turncatClientAddress, "peer", peerAddress, "Turn server", redactedTurnServerAddress)

	// Save start time
	startTime := time.Now()
	slog.Info("Measurement start time", "time", startTime.Format(time.RFC3339))

	// Print loadGenerator
	slog.Info("Starting load generator", "command", measurement.LoadGenerator.Command, "args", measurement.LoadGenerator.Args)

	loadGenerator := exec.Command(measurement.LoadGenerator.Command, measurement.LoadGenerator.Args...)
	loadGenerator.Stdout = os.Stdout

	err := loadGenerator.Start()
	if err != nil {
		panic(fmt.Errorf("fatal error starting load generator '%s': %w", measurement.LoadGenerator.Command, err))
	}

	loadGenerator.Wait()

	// Save end time
	endTime := time.Now()
	slog.Info("Measurement end time", "time", endTime.Format(time.RFC3339))

	// Get data from prometheus
	slog.Info("Fetching prometheus data", "measurement", measurement.Name)
	bufferTime := 5 * time.Minute
	savePrometheusData(cluster.Host, measurement.Name, startTime, endTime, bufferTime)

	slog.Info("Measurement completed", "measurement", measurement.Name)
}

func savePrometheusData(clusterHost string, measurementName string, startTime, endTime time.Time, bufferTime time.Duration) {
	// Create output directory if it doesn't exist
	outputDir := filepath.Join("results", measurementName)
	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		slog.Error("Error creating output directory", "error", err)
		return
	}

	// Add buffer time before start and after end to capture metrics
	bufferedStart := startTime.Add(-bufferTime)

	// Format start time for styx
	startStr := bufferedStart.Format("2006-01-02T15:04:05")

	// Calculate duration
	duration := endTime.Sub(startTime)
	durationStr := duration.String()

	slog.Info("Running styx query", "start", startStr, "duration", durationStr)

	// CPU query
	cpuQuery := "sum(rate(container_cpu_usage_seconds_total[5m])) by (namespace)"
	cpuFile := filepath.Join(outputDir, "cpu_by_namespace.csv")
	runPrometheusQuery(clusterHost, cpuQuery, cpuFile, startTime, endTime)
	// err = runStyxQuery(cpuQuery, startStr, durationStr, cpuFile)
	// if err != nil {
	// 	slog.Error("Error fetching CPU data", "error", err)
	// }

	// Memory query
	memQuery := "sum(container_memory_working_set_bytes) by (namespace) / 1024 / 1024 / 1024"
	memFile := filepath.Join(outputDir, "memory_by_namespace.csv")
	runPrometheusQuery(clusterHost, memQuery, memFile, bufferedStart, endTime)
	// if err != nil {
	// 	slog.Error("Error fetching memory data", "error", err)
	// }

	slog.Info("Prometheus data saved", "directory", outputDir)
}

func runPrometheusQuery(host, query, outputFile string, start, end time.Time) {
	// --- Config ---
	prometheusURL := fmt.Sprintf("http://%s:9090", host)
	step := 1 * time.Second

	// --- Prometheus client ---
	client, err := promApi.NewClient(promApi.Config{Address: prometheusURL})
	if err != nil {
		panic(fmt.Sprintf("error creating prometheus client: %v", err))
	}
	prometheusAPI := v1.NewAPI(client)

	// --- Query range ---
	result, warnings, err := prometheusAPI.QueryRange(context.Background(), query, v1.Range{
		Start: start,
		End:   end,
		Step:  step,
	})
	if err != nil {
		panic(fmt.Sprintf("error querying prometheus: %v", err))
	}
	if len(warnings) > 0 {
		fmt.Printf("warnings: %v\n", warnings)
	}

	matrix, ok := result.(model.Matrix)
	if !ok {
		panic(fmt.Sprintf("expected matrix result, got %T", result))
	}

	// --- Collect all label names for dynamic headers ---
	labelSet := map[string]struct{}{}
	for _, series := range matrix {
		for name := range series.Metric {
			labelSet[string(name)] = struct{}{}
		}
	}

	// Sort label names for consistent column order
	labelNames := make([]string, 0, len(labelSet))
	for name := range labelSet {
		labelNames = append(labelNames, name)
	}

	slog.Info(strings.Join(labelNames, ","))
	//sort.Strings(labelNames)

	namespaces := []string{}

	// Get the namespaces
	for _, series := range matrix {
		slog.Info(string(series.Metric[model.LabelName("namespace")]))
		namespaces = append(namespaces, string(series.Metric[model.LabelName("namespace")]))
	}

	headers := append([]string{"timestamp"}, namespaces...)

	slog.Info(strings.Join(headers, ", "))

	//create a map of namespace to values
	namespaceValues := map[string][]model.SamplePair{}

	for _, series := range matrix {
		namespace := string(series.Metric[model.LabelName("namespace")])
		namespaceValues[namespace] = append(namespaceValues[namespace], series.Values...)
	}

	maxValueNumbers := 0

	for _, values := range namespaceValues {
		if len(values) > maxValueNumbers {
			maxValueNumbers = len(values)
		}
	}

	slog.Info(strings.Join(headers, ","))

	// --- Write CSV ---
	f, err := os.Create(outputFile)
	if err != nil {
		panic(fmt.Sprintf("error creating output file: %v", err))
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	if err := writer.Write(headers); err != nil {
		panic(fmt.Sprintf("error writing headers: %v", err))
	}

	for i := 0; i < maxValueNumbers; i++ {
		row := make([]string, len(headers))
		for j, namespace := range namespaces {
			if i < len(namespaceValues[namespace]) {
				sample := namespaceValues[namespace][i]
				row[j+1] = sample.Value.String()
				if j == 0 {
					row[0] = strconv.FormatInt(sample.Timestamp.Unix(), 10)
				}
			}
		}

		if err := writer.Write(row); err != nil {
			panic(fmt.Sprintf("error writing row: %v", err))
		}

	}

}
