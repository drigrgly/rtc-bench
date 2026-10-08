package main

import (
	"fmt"
	"log/slog"
	"strings"
)

func FormatConnectionInfo(turncatClientAddress, turnServerAddress string) string {

	var sb strings.Builder

	sb.WriteString("---- Connection Information ---\n")
	sb.WriteString(fmt.Sprintf("Turncat Client Address: %s\n", turncatClientAddress))
	sb.WriteString(fmt.Sprintf("Turn Server Address: %s\n", turnServerAddress))
	sb.WriteString("-------------------------------\n")
	return sb.String()
}

func LogFormatted(formattedString string) {
	lines := strings.Split(formattedString, "\n")
	for _, line := range lines {
		slog.Info(line)
	}
}

func FormatMeasurementInfo(measurementMetaData *MeasurementMetaData) string {
	var sb strings.Builder
	connectionInfo := FormatConnectionInfo(measurementMetaData.TurncatClientAddress, measurementMetaData.TurnServerAddress)

	sb.WriteString("--- Measurement Information ---\n")
	sb.WriteString(fmt.Sprintf("Measurement name: %s\n", measurementMetaData.Measurement.Name))
	sb.WriteString(fmt.Sprintf("Measurement start: %s\n", measurementMetaData.InitialStartTime.Format("2006.01.02 15:04:05")))
	sb.WriteString(fmt.Sprintf("Offloading: %s\n", measurementMetaData.Measurement.Offloading))
	sb.WriteString(fmt.Sprintf("Repeats: %d\n", measurementMetaData.Measurement.Repeat))
	sb.WriteString("-------------------------------\n")

	sb.WriteString("\n")

	sb.WriteString(connectionInfo)

	sb.WriteString("\n")

	sb.WriteString("---- Load generator output ----\n")
	sb.WriteString(fmt.Sprintf("Command: %s\nArgs: %s", measurementMetaData.Measurement.LoadGenerator.Command, strings.Join(measurementMetaData.Measurement.LoadGenerator.Args, " ")))
	for run, measurement := range measurementMetaData.IndividualMeasurements {
		sb.WriteString(fmt.Sprintf("\nIteration: %d\n", run+1))
		sb.WriteString(measurement.LoadGeneratorOutput.String())
	}
	sb.WriteString(measurementMetaData.IndividualMeasurements[0].LoadGeneratorOutput.String())
	sb.WriteString("-------------------------------\n")

	return sb.String()
}
