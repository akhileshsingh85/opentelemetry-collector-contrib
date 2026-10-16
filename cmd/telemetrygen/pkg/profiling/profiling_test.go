// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profiling

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type captureExporter struct {
	requests [][]sdklog.Record
}

func (e *captureExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.requests = append(e.requests, append([]sdklog.Record(nil), records...))
	return nil
}

func (*captureExporter) Shutdown(context.Context) error   { return nil }
func (*captureExporter) ForceFlush(context.Context) error { return nil }

func TestEncodeProfile(t *testing.T) {
	for _, dataType := range []string{cpuProfileType, allocationProfileType} {
		t.Run(dataType, func(t *testing.T) {
			encoded, frameCount, err := encodeProfile(dataType, 8, 4, 2, time.Unix(100, 0))
			require.NoError(t, err)
			require.Equal(t, 32, frameCount)

			compressed, err := base64.StdEncoding.DecodeString(encoded)
			require.NoError(t, err)
			parsedProfile, err := profile.ParseData(compressed)
			require.NoError(t, err)
			require.NoError(t, parsedProfile.CheckValid())
			require.Len(t, parsedProfile.Sample, 8)
			require.Len(t, parsedProfile.Location, 8)
			require.Len(t, parsedProfile.Function, 8)
			require.Equal(t, dataType == allocationProfileType, parsedProfile.SampleType[0].Type == "allocationSize")
			require.Len(t, parsedProfile.Sample[0].Location, 4)
			expectedEvent := "jdk.ExecutionSample"
			if dataType == allocationProfileType {
				expectedEvent = "jdk.ObjectAllocationSample"
				require.Equal(t, "bytes", parsedProfile.SampleType[0].Unit)
			}
			require.Equal(t, expectedEvent, parsedProfile.Sample[0].Label[profileSamplesAttr][0])
		})
	}
}

func TestNewProfileLogRecord(t *testing.T) {
	res := resource.NewWithAttributes("", attribute.String("service.name", "test-service"))
	record, err := newProfileLogRecord(cpuProfileType, 2, 3, 1, time.Unix(100, 0), res)
	require.NoError(t, err)
	require.Equal(t, profilingScope, record.InstrumentationScope().Name)
	require.Equal(t, profilingScopeVersion, record.InstrumentationScope().Version)
	require.Equal(t, 5, record.AttributesLen())
	compressed, err := base64.StdEncoding.DecodeString(record.Body().AsString())
	require.NoError(t, err)
	_, err = profile.ParseData(compressed)
	require.NoError(t, err)

	attributes := make(map[string]attribute.Value)
	record.WalkAttributes(func(kv attribute.KeyValue) bool {
		attributes[string(kv.Key)] = kv.Value
		return true
	})
	require.Equal(t, profilingSource, attributes[profilingSourceTypeAttr].AsString())
	require.Equal(t, cpuProfileType, attributes[profilingDataTypeAttr].AsString())
	require.Equal(t, profilingFormat, attributes[profilingDataFormatAttr].AsString())
	require.EqualValues(t, 6, attributes[profilingFrameCountAttr].AsInt64())
	require.Equal(t, profilingContinuous, attributes[profilingSourceAttr].AsString())
}

func TestHTTPExporterSendsProfilingLogs(t *testing.T) {
	var mu sync.Mutex
	var received plogotlp.ExportRequest
	var handlerErr error
	setHandlerErr := func(err error) {
		mu.Lock()
		handlerErr = err
		mu.Unlock()
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			setHandlerErr(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		request := plogotlp.NewExportRequest()
		if err = request.UnmarshalProto(body); err != nil {
			setHandlerErr(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = request
		mu.Unlock()
		response, err := plogotlp.NewExportResponse().MarshalProto()
		if err != nil {
			setHandlerErr(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		if _, err = w.Write(response); err != nil {
			setHandlerErr(err)
		}
	}))
	t.Cleanup(server.Close)
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	cfg := NewConfig()
	cfg.CPUEnabled = true
	cfg.CustomEndpoint = serverURL.Host
	cfg.HTTPPath = "/v1/logs"
	cfg.Insecure = true
	exporter, err := createExporter(cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, exporter.Shutdown(t.Context())) }()

	cpuRecord, err := newProfileLogRecord(cpuProfileType, 2, 3, 1, time.Unix(100, 0), resource.Empty())
	require.NoError(t, err)
	allocationRecord, err := newProfileLogRecord(allocationProfileType, 2, 3, 1, time.Unix(100, 0), resource.Empty())
	require.NoError(t, err)
	require.NoError(t, exporter.Export(t.Context(), []sdklog.Record{cpuRecord, allocationRecord}))
	mu.Lock()
	defer mu.Unlock()
	require.NoError(t, handlerErr)
	require.Equal(t, 1, received.Logs().ResourceLogs().Len())
	records := received.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	require.Equal(t, 2, records.Len())
	dataTypes := make(map[string]bool)
	for i := 0; i < records.Len(); i++ {
		attributes := records.At(i).Attributes()
		format, ok := attributes.Get(profilingDataFormatAttr)
		require.True(t, ok)
		require.Equal(t, profilingFormat, format.Str())
		dataType, ok := attributes.Get(profilingDataTypeAttr)
		require.True(t, ok)
		dataTypes[dataType.Str()] = true
	}
	require.Equal(t, map[string]bool{cpuProfileType: true, allocationProfileType: true}, dataTypes)
}

func TestConfigUsesProfilerEnvironment(t *testing.T) {
	t.Setenv(profilerEnabledEnv, "true")
	t.Setenv(profilerMemoryEnabledEnv, "true")
	t.Setenv(profilerLogsEndpointEnv, "http://collector.example:4318/v1/logs")
	cfg := NewConfig()

	require.True(t, cfg.CPUEnabled)
	require.True(t, cfg.MemoryEnabled)
	require.True(t, cfg.UseHTTP)
	require.True(t, cfg.Insecure)
	require.Equal(t, "collector.example:4318", cfg.CustomEndpoint)
	require.Equal(t, "/v1/logs", cfg.HTTPPath)
	require.Equal(t, 5*time.Minute, cfg.ReportingInterval)
	require.NoError(t, cfg.Validate())
}

func TestConfigRequiresCPUProfiler(t *testing.T) {
	t.Setenv(profilerEnabledEnv, "false")
	t.Setenv(profilerMemoryEnabledEnv, "true")
	t.Setenv(profilerLogsEndpointEnv, "")
	cfg := NewConfig()
	require.ErrorContains(t, cfg.Validate(), profilerEnabledEnv)
}

func TestConfigRejectsInvalidProfilerEnvironment(t *testing.T) {
	t.Setenv(profilerEnabledEnv, "on")
	cfg := NewConfig()
	require.ErrorContains(t, cfg.Validate(), profilerEnabledEnv)
}

func TestConfigValidatesErrors(t *testing.T) {
	cfg := NewConfig()
	cfg.CPUEnabled = true
	cfg.Errors = 101
	require.ErrorContains(t, cfg.Validate(), "errors must be between 0 and 100")
	cfg.Errors = 10
	cfg.BatchSize = 0
	require.ErrorContains(t, cfg.Validate(), "batch-size must be greater than 0")
}

func TestGenerateBatchesRecordsAndAppliesConfiguredErrorPercentage(t *testing.T) {
	cfg := NewConfig()
	cfg.CPUEnabled = true
	cfg.MemoryEnabled = true
	cfg.NumProfiles = 100
	cfg.Rate = 0
	cfg.Errors = 10
	cfg.BatchSize = 30
	cfg.Timeout = time.Second

	provider := sdkmetric.NewMeterProvider()
	defer func() { require.NoError(t, provider.Shutdown(t.Context())) }()
	profileMetrics, err := newExportMetrics(provider.Meter(profilingScope))
	require.NoError(t, err)

	exporter := &captureExporter{}
	var generated atomic.Int64
	err = generate(t.Context(), cfg, resource.Empty(), exporter, profileMetrics, rate.NewLimiter(rate.Inf, 1), 0, &generated, zap.NewNop())
	require.NoError(t, err)
	require.EqualValues(t, 100, generated.Load())
	require.Len(t, exporter.requests, 4)
	require.Equal(t, []int{30, 30, 30, 10}, []int{
		len(exporter.requests[0]), len(exporter.requests[1]), len(exporter.requests[2]), len(exporter.requests[3]),
	})

	var errorRecords int
	dataTypes := map[string]int{}
	for _, request := range exporter.requests {
		for _, record := range request {
			if record.Severity() == log.SeverityError {
				errorRecords++
				encoded := record.Body().AsString()
				compressed, decodeErr := base64.StdEncoding.DecodeString(encoded)
				if decodeErr == nil {
					_, decodeErr = profile.ParseData(compressed)
				}
				require.Error(t, decodeErr, "error-severity records must contain invalid profile payloads")
			}
			var dataType string
			record.WalkAttributes(func(kv attribute.KeyValue) bool {
				if string(kv.Key) == profilingDataTypeAttr {
					dataType = kv.Value.AsString()
				}
				return true
			})
			dataTypes[dataType]++
		}
	}
	require.Equal(t, 10, errorRecords)
	require.Equal(t, map[string]int{cpuProfileType: 50, allocationProfileType: 50}, dataTypes)
}

func TestErrorRecordGeneratorProducesRequestedShare(t *testing.T) {
	generator := errorRecordGenerator{percentage: 10}
	count := 0
	for range 100 {
		if generator.next() {
			count++
		}
	}
	require.Equal(t, 10, count)
}

func TestConfigEndpointEnvironmentDoesNotLeak(t *testing.T) {
	t.Setenv(profilerEnabledEnv, "true")
	t.Setenv(profilerLogsEndpointEnv, "not-a-url")
	cfg := NewConfig()
	require.ErrorContains(t, cfg.Validate(), profilerLogsEndpointEnv)
}
