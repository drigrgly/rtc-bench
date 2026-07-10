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
	sb.WriteString("Queries:\n")
	for _, query := range measurementMetaData.Measurement.Queries {
		sb.WriteString(fmt.Sprintf("  - %s\n", query.Name))
	}
	sb.WriteString("-------------------------------\n")
	sb.WriteString("\n")
	sb.WriteString(connectionInfo)
	sb.WriteString("\n")

	return sb.String()
}
