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

func StartSameClusterMeasurements(cluster *Cluster, config *kubeApi.Config, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// clusterId <=> context name mapping
	contextName := cluster.ClusterId
	configOverrides := &clientcmd.ConfigOverrides{
		CurrentContext: contextName,
	}

	clientConfig := clientcmd.NewNonInteractiveClientConfig(
		*config,
		contextName,
		configOverrides,
		nil,
	)

	// Use the selected context (based on id)
	kubeCfg, err := clientConfig.ClientConfig()
	if err != nil {
		slog.Error("Error creating client config", "error", err)
		panic(err)
	}

	// create the clientset
	clientset, kubeErr := kubernetes.NewForConfig(kubeCfg)
	if kubeErr != nil {
		slog.Error("Error creating clientset", "error", kubeErr)
		panic(kubeErr.Error())
	}

	// Dynamic client (for Gateway CRD)
	dynClient, err := dynamic.NewForConfig(kubeCfg)
	if err != nil {
		slog.Error("Error creating dynamic client", "error", err)
		panic(err)
	}

	// Get Iperf cluster peer address
	peerHost, peerPort, err := getIperfServerClusterIP(clientset, ctx, true)
	if err != nil {
		slog.Error("Error fetching Iperf server cluster IP", "error", err)
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

func startMeasurement(cluster *Cluster, measurement Measurement) {
	metaData := MeasurementMetaData{
		InitialStartTime: time.Now(),
		Measurement:      &measurement,
	}

	// Construct the client server address
	turncatClientAddress := fmt.Sprintf("udp://%s:%s", cluster.TurncatClient.Host, cluster.TurncatClient.Port)

	// Redact the password in the log
	redactedTurnServerAddress := fmt.Sprintf("turn://%s:%s@%s:%s?transport=udp", "***", "***", cluster.TurnServer.Host, cluster.TurnServer.Port)

	// Construct the peer address
	peerAddress := fmt.Sprintf("udp://%s:%s", cluster.Peer.Host, cluster.Peer.Port)

	//Save information about the measurement
	metaData.TurncatClientAddress = turncatClientAddress
	metaData.TurnServerAddress = redactedTurnServerAddress
	metaData.PeerAddress = peerAddress

	LogFormatted(FormatConnectionInfo(turncatClientAddress, redactedTurnServerAddress, peerAddress))

	for _, i := 0, 0; i < measurement.Repeat; i++ {
		// Save information about each individual measurement
		individualMeasurementMetaData := IndividualMeasurementMetaData{
			Count: i,
		}

		// Save start time
		individualMeasurementMetaData.StartTime = time.Now()
		slog.Info("Measurement start time", "time", individualMeasurementMetaData.StartTime)

		slog.Info("Starting load generator", "command", measurement.LoadGenerator.Command, "args", measurement.LoadGenerator.Args)
		loadGenerator := exec.Command(measurement.LoadGenerator.Command, measurement.LoadGenerator.Args...)

		loadGenerator.Stdout = os.Stdout
		err := loadGenerator.Start()
		if err != nil {
			panic(fmt.Errorf("fatal error starting load generator '%s': %w", measurement.LoadGenerator.Command, err))
		}
		loadGenerator.Wait()

		// Save end time
		individualMeasurementMetaData.EndTime = time.Now()

		slog.Info("Measurement end time", "time", individualMeasurementMetaData.EndTime)

		// Get data from prometheus
		slog.Info("Fetching prometheus data", "measurement", measurement.Name)
		individualMeasurementMetaData.BufferTime = 5 * time.Minute

		clusterCollectionName := fmt.Sprintf("%s-%s", cluster.ClusterId, metaData.InitialStartTime.Format("2006-01-01"))
		hours, minutes, sec := metaData.InitialStartTime.Clock()
		measurementCollectionName := fmt.Sprintf("%s-%s", metaData.Measurement.Name, fmt.Sprintf("%02d%02d%02d", hours, minutes, sec))

		collectionOutputDir := filepath.Join("results", clusterCollectionName, measurementCollectionName)
		metaData.CollectionOutputDir = collectionOutputDir

		metaData.IndividualMeasurements = append(metaData.IndividualMeasurements, individualMeasurementMetaData)

		savePrometheusData(cluster, &metaData, i)
	}

	slog.Info("Measurement completed", "measurement", measurement.Name)
}

func savePrometheusData(cluster *Cluster, measurementMetaData *MeasurementMetaData, count int) {
	err := os.MkdirAll(measurementMetaData.CollectionOutputDir, 0755)
	if err != nil {
		slog.Error("Error creating output directory", "error", err)
		return
	}

	currentRepeatData := measurementMetaData.IndividualMeasurements[count]
	startTime := currentRepeatData.StartTime
	endTime := currentRepeatData.EndTime
	bufferTime := currentRepeatData.BufferTime

	// Add buffer time before start and after end to capture metrics
	bufferedStart := startTime.Add(-bufferTime)

	// Format start time for styx
	startStr := bufferedStart.Format("2006-01-02T15:04:05")

	// Calculate duration
	duration := endTime.Sub(startTime)
	durationStr := duration.String()

	slog.Info("Running styx query", "start", startStr, "duration", durationStr)

	queries := measurementMetaData.Measurement.Queries

	for _, query := range queries {
		outputDir := filepath.Join(measurementMetaData.CollectionOutputDir, query.Name)
		err := os.MkdirAll(outputDir, 0755)
		if err != nil {
			slog.Error("Error creating output directory for query", "error", err)
			return
		}

		outputFile := filepath.Join(outputDir, fmt.Sprintf("%s-%d.csv", query.Name, count))
		runPrometheusQuery(cluster.Host, query.Query, outputFile, startTime, endTime)
	}

	saveMetadata(measurementMetaData)
	slog.Info("Prometheus data saved", "directory", measurementMetaData.CollectionOutputDir)
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

func saveMetadata(MeasurementMetaData *MeasurementMetaData) {
	formattedInfo := FormatMeasurementInfo(MeasurementMetaData)

	f, err := os.Create(filepath.Join(MeasurementMetaData.CollectionOutputDir, "metadata.txt"))
	if err != nil {
		panic(fmt.Sprintf("error creating metadata file: %v", err))
	}
	defer f.Close()

	_, err = f.WriteString(formattedInfo)
	if err != nil {
		panic(fmt.Sprintf("error writing metadata: %v", err))
	}
}
