package main

import (
	"fmt"
	"log/slog"
	"strings"
)

func FormatConnectionInfo(turncatClientAddress, turnServerAddress, peerAddress string) string {

	var sb strings.Builder

	sb.WriteString("---- Connection Information ---\n")
	sb.WriteString(fmt.Sprintf("Turncat Client Address: %s\n", turncatClientAddress))
	sb.WriteString(fmt.Sprintf("Turn Server Address: %s\n", turnServerAddress))
	sb.WriteString(fmt.Sprintf("Peer Address: %s\n", peerAddress))
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
	connectionInfo := FormatConnectionInfo(measurementMetaData.TurncatClientAddress, measurementMetaData.TurnServerAddress, measurementMetaData.PeerAddress)

	sb.WriteString("--- Measurement Information ---\n")
	sb.WriteString(fmt.Sprintf("Measurement name: %s\n", measurementMetaData.Measurement.Name))
	sb.WriteString(fmt.Sprintf("Measurement start: %s\n", measurementMetaData.InitialStartTime.Format("2006.01.02 15:04:05")))
	sb.WriteString(fmt.Sprintf("Repeats: %d\n", measurementMetaData.Measurement.Repeat))
	sb.WriteString("-------------------------------\n")

	sb.WriteString("\n")
	sb.WriteString("------ Query Information ------\n")
	sb.WriteString("Queries:\n")
	for _, query := range measurementMetaData.Measurement.Queries {
		sb.WriteString(fmt.Sprintf("  - %s: %s\n", query.Name, query.Query))
	}
	sb.WriteString("-------------------------------\n")
	sb.WriteString("\n")
	sb.WriteString(connectionInfo)
	sb.WriteString("\n")

	return sb.String()
}
