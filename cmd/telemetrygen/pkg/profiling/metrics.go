// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profiling

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/config"
)

const (
	profileMetricsPath           = "/v1/metrics"
	profileLogRecordsMetric      = "telemetrygen.profiling.log_records"
	profileExportErrorsMetric    = "telemetrygen.profiling.export_errors"
	partialSuccessRejectedSuffix = " logs rejected)"
)

type exportMetrics struct {
	logRecords metric.Int64Counter
	exportErrs metric.Int64Counter
}

func newExportMetrics(meter metric.Meter) (*exportMetrics, error) {
	logRecords, err := meter.Int64Counter(
		profileLogRecordsMetric,
		metric.WithDescription("Number of profile log records acknowledged, rejected, or part of an unacknowledged export request."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create log records counter: %w", err)
	}
	exportErrs, err := meter.Int64Counter(
		profileExportErrorsMetric,
		metric.WithDescription("Number of failed or partially rejected OTLP log export requests, classified by error type."),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create export errors counter: %w", err)
	}
	return &exportMetrics{logRecords: logRecords, exportErrs: exportErrs}, nil
}

func (m *exportMetrics) recordLogExport(recordCount int, err error) {
	ctx := context.Background()
	if err == nil {
		m.logRecords.Add(ctx, int64(recordCount), metric.WithAttributes(attribute.String("status", "sent")))
		return
	}
	if rejected, ok := partialSuccessRejectedCount(err); ok {
		if rejected > recordCount {
			rejected = recordCount
		}
		accepted := recordCount - rejected
		if accepted > 0 {
			m.logRecords.Add(ctx, int64(accepted), metric.WithAttributes(attribute.String("status", "sent")))
		}
		if rejected > 0 {
			m.logRecords.Add(ctx, int64(rejected), metric.WithAttributes(attribute.String("status", "rejected")))
		}
		m.exportErrs.Add(ctx, 1, metric.WithAttributes(attribute.String("error.type", "rejected")))
		return
	}
	m.logRecords.Add(ctx, int64(recordCount), metric.WithAttributes(attribute.String("status", "failed")))
	m.exportErrs.Add(ctx, 1, metric.WithAttributes(attribute.String("error.type", classifyExportError(err))))
}

func partialSuccessRejectedCount(err error) (int, bool) {
	message := err.Error()
	suffix := strings.LastIndex(message, partialSuccessRejectedSuffix)
	if suffix < 0 {
		return 0, false
	}
	openParen := strings.LastIndex(message[:suffix], "(")
	if openParen < 0 {
		return 0, false
	}
	rejected, parseErr := strconv.Atoi(message[openParen+1 : suffix])
	return rejected, parseErr == nil && rejected >= 0
}

func classifyExportError(err error) string {
	if _, ok := partialSuccessRejectedCount(err); ok {
		return "rejected"
	}
	switch status.Code(err) {
	case codes.ResourceExhausted:
		return "busy"
	case codes.Unavailable, codes.DeadlineExceeded:
		return "network"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "network"
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return "network"
	}

	message := strings.ToLower(err.Error())
	for _, response := range []string{"429 too many requests", "502 bad gateway", "503 service unavailable", "504 gateway timeout"} {
		if strings.Contains(message, response) {
			return "busy"
		}
	}
	for _, fragment := range []string{"connection refused", "connection reset", "broken pipe", "no route to host", "malformed http response", "i/o timeout"} {
		if strings.Contains(message, fragment) {
			return "network"
		}
	}
	return "other"
}

func createMetricsExporter(cfg *Config, logger *zap.Logger) (sdkmetric.Exporter, error) {
	if cfg.UseHTTP {
		options := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(cfg.Endpoint()),
			otlpmetrichttp.WithURLPath(profileMetricsPath),
			otlpmetrichttp.WithTimeout(cfg.Timeout),
		}
		if cfg.Insecure {
			options = append(options, otlpmetrichttp.WithInsecure())
		} else {
			tlsConfig, err := config.GetTLSCredentialsForHTTPExporter(cfg.CaFile, cfg.ClientAuth, cfg.InsecureSkipVerify)
			if err != nil {
				return nil, fmt.Errorf("failed to configure OTLP HTTP metrics TLS: %w", err)
			}
			options = append(options, otlpmetrichttp.WithTLSClientConfig(tlsConfig))
		}
		if len(cfg.Headers) > 0 {
			options = append(options, otlpmetrichttp.WithHeaders(cfg.GetHeaders()))
		}
		return otlpmetrichttp.New(context.Background(), options...)
	}

	options := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(cfg.Endpoint()),
		otlpmetricgrpc.WithTimeout(cfg.Timeout),
	}
	if cfg.Insecure {
		options = append(options, otlpmetricgrpc.WithInsecure())
	} else {
		credentials, err := config.GetTLSCredentialsForGRPCExporter(cfg.CaFile, cfg.ClientAuth, cfg.InsecureSkipVerify)
		if err != nil {
			return nil, fmt.Errorf("failed to configure OTLP gRPC metrics TLS: %w", err)
		}
		options = append(options, otlpmetricgrpc.WithTLSCredentials(credentials))
	}
	if len(cfg.Headers) > 0 {
		options = append(options, otlpmetricgrpc.WithHeaders(cfg.GetHeaders()))
	}
	logger.Debug("creating OTLP gRPC metrics exporter")
	return otlpmetricgrpc.New(context.Background(), options...)
}

func newMetricsProvider(cfg *Config, res *resource.Resource, logger *zap.Logger) (*sdkmetric.MeterProvider, *exportMetrics, error) {
	exporter, err := createMetricsExporter(cfg, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create profiling metrics exporter: %w", err)
	}
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(cfg.ReportingInterval))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(res))
	metrics, err := newExportMetrics(provider.Meter(profilingScope))
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, nil, err
	}
	return provider, metrics, nil
}
