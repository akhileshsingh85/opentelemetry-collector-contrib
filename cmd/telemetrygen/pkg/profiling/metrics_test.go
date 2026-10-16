// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profiling

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecordLogExportMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { require.NoError(t, provider.Shutdown(t.Context())) }()
	metrics, err := newExportMetrics(provider.Meter(profilingScope))
	require.NoError(t, err)

	metrics.recordLogExport(3, nil)
	metrics.recordLogExport(2, status.Error(codes.ResourceExhausted, "collector is busy"))
	metrics.recordLogExport(1, &url.Error{Op: "Post", URL: "http://collector/v1/logs", Err: errors.New("connection refused")})
	metrics.recordLogExport(2, errors.New("OTLP partial success: profile rejected (1 logs rejected)"))

	var result metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &result))
	values := make(map[string]map[string]int64)
	for _, scopeMetrics := range result.ScopeMetrics {
		for _, metric := range scopeMetrics.Metrics {
			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %q should be a sum", metric.Name)
			for _, point := range sum.DataPoints {
				var label string
				if metric.Name == profileLogRecordsMetric {
					value, ok := point.Attributes.Value("status")
					require.True(t, ok)
					label = value.AsString()
				} else {
					value, ok := point.Attributes.Value("error.type")
					require.True(t, ok)
					label = value.AsString()
				}
				if values[metric.Name] == nil {
					values[metric.Name] = make(map[string]int64)
				}
				values[metric.Name][label] = point.Value
			}
		}
	}
	require.Equal(t, map[string]map[string]int64{
		profileLogRecordsMetric:   {"sent": 4, "failed": 3, "rejected": 1},
		profileExportErrorsMetric: {"busy": 1, "network": 1, "rejected": 1},
	}, values)
}

func TestClassifyExportError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "gRPC busy", err: status.Error(codes.ResourceExhausted, "busy"), want: "busy"},
		{name: "HTTP busy", err: errors.New("failed to send logs: 503 Service Unavailable"), want: "busy"},
		{name: "network unavailable", err: status.Error(codes.Unavailable, "unavailable"), want: "network"},
		{name: "network error", err: errors.New("connection reset by peer"), want: "network"},
		{name: "partial rejection", err: errors.New("OTLP partial success: invalid profile (1 logs rejected)"), want: "rejected"},
		{name: "other", err: errors.New("invalid request"), want: "other"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, classifyExportError(test.err))
		})
	}
}

func TestProfileMetricsUseConfiguredResource(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.NewWithAttributes("", attribute.String("service.name", "profile-loadgen"))),
	)
	defer func() { require.NoError(t, provider.Shutdown(t.Context())) }()

	metrics, err := newExportMetrics(provider.Meter(profilingScope))
	require.NoError(t, err)
	metrics.recordLogExport(1, nil)
	var result metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &result))
	value, ok := result.Resource.Set().Value(attribute.Key("service.name"))
	require.True(t, ok)
	require.Equal(t, "profile-loadgen", value.AsString())
}

func TestMetricsExporterSendsCountersToOTLPMetricsPath(t *testing.T) {
	var mu sync.Mutex
	var received pmetricotlp.ExportRequest
	var handlerErr error
	setHandlerErr := func(err error) {
		mu.Lock()
		handlerErr = err
		mu.Unlock()
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != profileMetricsPath {
			err := errors.New("unexpected request path: " + r.URL.Path)
			setHandlerErr(err)
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			setHandlerErr(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		request := pmetricotlp.NewExportRequest()
		if err = request.UnmarshalProto(body); err != nil {
			setHandlerErr(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = request
		mu.Unlock()
		response, err := pmetricotlp.NewExportResponse().MarshalProto()
		if err != nil {
			setHandlerErr(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(response)
	}))
	t.Cleanup(server.Close)
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	cfg := NewConfig()
	cfg.CustomEndpoint = serverURL.Host
	cfg.Insecure = true
	exporter, err := createMetricsExporter(cfg, zap.NewNop())
	require.NoError(t, err)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Hour))))
	metrics, err := newExportMetrics(provider.Meter(profilingScope))
	require.NoError(t, err)
	metrics.recordLogExport(4, nil)
	metrics.recordLogExport(2, status.Error(codes.ResourceExhausted, "collector busy"))
	require.NoError(t, provider.ForceFlush(t.Context()))
	defer func() { require.NoError(t, provider.Shutdown(t.Context())) }()

	mu.Lock()
	defer mu.Unlock()
	require.NoError(t, handlerErr)
	metricNames := make(map[string]bool)
	for _, resourceMetrics := range received.Metrics().ResourceMetrics().All() {
		for _, scopeMetrics := range resourceMetrics.ScopeMetrics().All() {
			for _, metric := range scopeMetrics.Metrics().All() {
				metricNames[metric.Name()] = true
			}
		}
	}
	require.Equal(t, map[string]bool{profileLogRecordsMetric: true, profileExportErrorsMetric: true}, metricNames)
}
