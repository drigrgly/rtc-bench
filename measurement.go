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

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

func startMeasurement(measurement Measurement, wg *sync.WaitGroup) {
	defer wg.Done()

	// This function will start the measurement based on the configuration
	// It will start turncat and the load generator with the appropriate parameters
	clientAddress := fmt.Sprintf("udp://%s:%d", measurement.Client.Host, measurement.Client.Port)

	slog.Info("Measurement configuration", "name", measurement.Name, "client", fmt.Sprintf("%s:%d", measurement.Client.Host, measurement.Client.Port), "peer", measurement.Turncat.PeerHostAddress, "turn_server", measurement.Turncat.TurnServerAddress)

	slog.Info("Starting turncat")

	// Get the authentication information for turncat
	turncat := exec.Command("turncat", "--log=all:INFO", clientAddress, measurement.Turncat.TurnServerAddress, measurement.Turncat.PeerHostAddress)
	turncat.Stdout = os.Stdout

	// Save start time
	startTime := time.Now()
	slog.Info("Measurement start time", "time", startTime.Format(time.RFC3339))

	// Print loadGenerator
	slog.Info("Starting load generator", "command", measurement.LoadGenerator.Command, "args", measurement.LoadGenerator.Args)

	loadGenerator := exec.Command(measurement.LoadGenerator.Command, measurement.LoadGenerator.Args...)
	loadGenerator.Stdout = os.Stdout

	err := turncat.Start()
	if err != nil {
		panic(fmt.Errorf("fatal error starting turncat: %w", err))
	}

	err = loadGenerator.Start()
	if err != nil {
		panic(fmt.Errorf("fatal error starting load generator '%s': %w", measurement.LoadGenerator.Command, err))
	}

	loadGenerator.Wait()

	// Save end time
	endTime := time.Now()
	slog.Info("Measurement end time", "time", endTime.Format(time.RFC3339))

	slog.Info("Shutting down turncat", "measurement", measurement.Name)
	turncat.Process.Kill()

	// Run styx to save prometheus data
	slog.Info("Fetching prometheus data", "measurement", measurement.Name)
	bufferTime := 5 * time.Minute
	savePrometheusData(measurement.Name, startTime, endTime, bufferTime)

	slog.Info("Measurement completed", "measurement", measurement.Name)
}

func savePrometheusData(measurementName string, startTime, endTime time.Time, bufferTime time.Duration) {
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
	runPrometheusQuery(cpuQuery, cpuFile, startTime, endTime)
	// err = runStyxQuery(cpuQuery, startStr, durationStr, cpuFile)
	// if err != nil {
	// 	slog.Error("Error fetching CPU data", "error", err)
	// }

	// Memory query
	memQuery := "sum(container_memory_working_set_bytes) by (namespace) / 1024 / 1024 / 1024"
	memFile := filepath.Join(outputDir, "memory_by_namespace.csv")
	runPrometheusQuery(memQuery, memFile, bufferedStart, endTime)
	// if err != nil {
	// 	slog.Error("Error fetching memory data", "error", err)
	// }

	slog.Info("Prometheus data saved", "directory", outputDir)
}

func runPrometheusQuery(query, outputFile string, start, end time.Time) {
	// --- Config ---
	prometheusURL := "http://localhost:9090"
	step := 1 * time.Second

	// --- Prometheus client ---
	client, err := api.NewClient(api.Config{Address: prometheusURL})
	if err != nil {
		panic(fmt.Sprintf("error creating prometheus client: %v", err))
	}
	promAPI := v1.NewAPI(client)

	// --- Query range ---
	result, warnings, err := promAPI.QueryRange(context.Background(), query, v1.Range{
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
