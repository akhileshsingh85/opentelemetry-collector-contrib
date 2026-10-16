// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profiling

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/pprof/profile"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/log/logtest"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/config"
	internalLog "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/log"
)

const (
	profilingScope          = "otel.profiling"
	profilingScopeVersion   = "0.1.0"
	profilingSource         = "otel.profiling"
	profilingFormat         = "pprof-gzip-base64"
	profilingContinuous     = "continuous"
	cpuProfileType          = "cpu"
	allocationProfileType   = "allocation"
	profilingSourceTypeAttr = "com.splunk.sourcetype"
	profilingDataTypeAttr   = "profiling.data.type"
	profilingDataFormatAttr = "profiling.data.format"
	profilingFrameCountAttr = "profiling.data.total.frame.count"
	profilingSourceAttr     = "profiling.instrumentation.source"
	profileSamplesAttr      = "source.event.name"
	profileSampleTimeAttr   = "source.event.time"
	profileSamplePeriodAttr = "source.event.period"
	profileThreadIDAttr     = "thread.id"
	profileThreadNameAttr   = "thread.name"
	profileThreadStateAttr  = "thread.state"
)

// Start runs the AlwaysOn Profiling log generator.
func Start(cfg *Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	logger, err := internalLog.CreateLogger(cfg.SkipSettingGRPCLogger)
	if err != nil {
		return err
	}
	logger.Info("starting the profiling generator", zap.Any("config", cfg))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil {
		return err
	}
	return nil
}

func run(ctx context.Context, cfg *Config, logger *zap.Logger) error {
	res := resource.NewWithAttributes("", cfg.GetAttributes()...)
	meterProvider, profileMetrics, err := newMetricsProvider(cfg, res, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			logger.Error("failed to shut down profiling metrics exporter", zap.Error(err))
		}
	}()

	lim := rate.Limit(cfg.Rate)
	if cfg.Rate == 0 {
		lim = rate.Inf
		logger.Info("generation of profiles isn't being throttled")
	} else {
		logger.Info("generation of profile intervals is limited", zap.Float64("per-second-per-worker", cfg.Rate))
	}

	var generated atomic.Int64
	var wg sync.WaitGroup
	workerErrors := make(chan error, cfg.WorkerCount)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for workerID := range cfg.WorkerCount {
		exporter, err := createExporter(cfg)
		if err != nil {
			cancel()
			wg.Wait()
			return err
		}
		wg.Add(1)
		go func(workerID int, exp sdklog.Exporter) {
			defer wg.Done()
			defer func() {
				if err := exp.Shutdown(context.Background()); err != nil {
					logger.Error("failed to stop profiling exporter", zap.Int("worker", workerID), zap.Error(err))
				}
			}()
			if err := generate(workerCtx, cfg, res, exp, profileMetrics, rate.NewLimiter(lim, 1), workerID, &generated, logger); err != nil {
				workerErrors <- err
				cancel()
			}
		}(workerID, exporter)
	}

	if cfg.TotalDuration.Duration() > 0 && !cfg.TotalDuration.IsInf() {
		timer := time.NewTimer(cfg.TotalDuration.Duration())
		select {
		case <-ctx.Done():
		case <-timer.C:
		case err := <-workerErrors:
			cancel()
			wg.Wait()
			return err
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		cancel()
	} else if cfg.TotalDuration.IsInf() {
		select {
		case <-ctx.Done():
		case err := <-workerErrors:
			cancel()
			wg.Wait()
			return err
		}
		cancel()
	}

	wg.Wait()
	select {
	case err := <-workerErrors:
		return err
	default:
	}
	logger.Info("profiling data generated", zap.Int64("profile_log_records", generated.Load()))
	return nil
}

func generate(ctx context.Context, cfg *Config, res *resource.Resource, exporter sdklog.Exporter, profileMetrics *exportMetrics, limiter *rate.Limiter, workerID int, generated *atomic.Int64, logger *zap.Logger) error {
	dataTypes := []string{cpuProfileType}
	if cfg.MemoryEnabled {
		dataTypes = append(dataTypes, allocationProfileType)
	}

	// Reuse encoded profile bodies for a whole export batch. Encoding a pprof
	// profile for every log record is expensive and the synthetic stack shape is
	// identical for each record.
	payloads, err := buildProfilePayloads(cfg, time.Now(), dataTypes)
	if err != nil {
		return fmt.Errorf("failed to build profile payloads: %w", err)
	}

	batchCapacity := cfg.BatchSize
	if cfg.Rate > 0 && cfg.Rate < float64(batchCapacity) {
		batchCapacity = max(1, int(cfg.Rate))
	}
	records := make([]sdklog.Record, 0, batchCapacity)
	var recordCount int
	var invalidPayloadSequence uint64
	errorGenerator := errorRecordGenerator{percentage: cfg.Errors}

	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		exportCtx := ctx
		if ctx.Err() != nil {
			exportCtx = context.Background()
		}
		exportCtx, cancel := context.WithTimeout(exportCtx, cfg.Timeout)
		defer cancel()
		batch := records
		exportErr := exporter.Export(exportCtx, batch)
		if exportErr != nil {
			profileMetrics.recordLogExport(len(batch), exportErr)
			logger.Error("failed to export profiling logs", zap.Int("worker", workerID), zap.Error(exportErr))
			return fmt.Errorf("worker %d failed to export profiling logs: %w", workerID, exportErr)
		}
		profileMetrics.recordLogExport(len(batch), nil)
		generated.Add(int64(len(batch)))
		records = make([]sdklog.Record, 0, batchCapacity)
		payloads, err = buildProfilePayloads(cfg, time.Now(), dataTypes)
		if err != nil {
			return fmt.Errorf("failed to build profile payloads: %w", err)
		}
		return nil
	}

	for ctx.Err() == nil {
		if !cfg.TotalDuration.IsInf() && cfg.TotalDuration.Duration() <= 0 && recordCount >= cfg.NumProfiles {
			break
		}
		if err := limiter.Wait(ctx); err != nil {
			break
		}

		now := time.Now()
		dataType := dataTypes[recordCount%len(dataTypes)]
		encoded := payloads[dataType].encoded
		invalid := errorGenerator.next()
		if invalid {
			encoded = invalidProfilePayload(invalidPayloadSequence)
			invalidPayloadSequence++
		}
		record, err := newProfileLogRecordFromPayload(dataType, encoded, payloads[dataType].frameCount, now, res, invalid)
		if err != nil {
			return fmt.Errorf("failed to create profile log record: %w", err)
		}
		records = append(records, record)
		recordCount++

		if len(records) >= batchCapacity || (!cfg.TotalDuration.IsInf() && cfg.TotalDuration.Duration() <= 0 && recordCount >= cfg.NumProfiles) {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if err := flush(); err != nil {
		return err
	}
	return nil
}

type profilePayload struct {
	encoded    string
	frameCount int
}

func buildProfilePayloads(cfg *Config, timestamp time.Time, dataTypes []string) (map[string]profilePayload, error) {
	payloads := make(map[string]profilePayload, len(dataTypes))
	for _, dataType := range dataTypes {
		encoded, frameCount, err := encodeProfile(dataType, cfg.Samples, cfg.StackDepth, cfg.UniqueStacks, timestamp)
		if err != nil {
			return nil, err
		}
		payloads[dataType] = profilePayload{encoded: encoded, frameCount: frameCount}
	}
	return payloads, nil
}

type errorRecordGenerator struct {
	percentage  int
	accumulator int
}

func (g *errorRecordGenerator) next() bool {
	g.accumulator += g.percentage
	if g.accumulator < 100 {
		return false
	}
	g.accumulator -= 100
	return true
}

var (
	invalidPayloadsOnce sync.Once
	invalidPayloads     []string
)

func invalidProfilePayload(sequence uint64) string {
	invalidPayloadsOnce.Do(func() {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, _ = writer.Write([]byte("not a pprof profile"))
		_ = writer.Close()
		invalidPayloads = []string{
			"%%%invalid-base64%%%",
			base64.StdEncoding.EncodeToString([]byte("not a gzip stream")),
			base64.StdEncoding.EncodeToString(compressed.Bytes()),
		}
	})
	return invalidPayloads[sequence%uint64(len(invalidPayloads))]
}

func newProfileLogRecord(dataType string, samples, stackDepth, uniqueStacks int, timestamp time.Time, res *resource.Resource) (sdklog.Record, error) {
	encoded, frameCount, err := encodeProfile(dataType, samples, stackDepth, uniqueStacks, timestamp)
	if err != nil {
		return sdklog.Record{}, err
	}
	return newProfileLogRecordFromPayload(dataType, encoded, frameCount, timestamp, res, false)
}

func newProfileLogRecordFromPayload(dataType, encoded string, frameCount int, timestamp time.Time, res *resource.Resource, invalid bool) (sdklog.Record, error) {
	attrs := []attribute.KeyValue{
		attribute.String(profilingSourceTypeAttr, profilingSource),
		attribute.String(profilingDataTypeAttr, dataType),
		attribute.String(profilingDataFormatAttr, profilingFormat),
		attribute.Int64(profilingFrameCountAttr, int64(frameCount)),
		attribute.String(profilingSourceAttr, profilingContinuous),
	}
	severity := log.SeverityInfo
	severityText := "INFO"
	if invalid {
		severity = log.SeverityError
		severityText = "ERROR"
	}
	factory := logtest.RecordFactory{
		Timestamp:            timestamp,
		Severity:             severity,
		SeverityText:         severityText,
		Body:                 attribute.StringValue(encoded),
		Attributes:           attrs,
		Resource:             res,
		InstrumentationScope: &instrumentation.Scope{Name: profilingScope, Version: profilingScopeVersion},
	}
	record := factory.NewRecord()
	return record, nil
}

func encodeProfile(dataType string, samples, stackDepth, uniqueStacks int, timestamp time.Time) (string, int, error) {
	if samples <= 0 || stackDepth <= 0 || uniqueStacks <= 0 {
		return "", 0, errors.New("samples, stack-depth, and unique-stacks must be greater than 0")
	}

	p := &profile.Profile{
		TimeNanos: timestamp.UnixNano(),
		SampleType: []*profile.ValueType{{
			Type: "cpu",
			Unit: "nanoseconds",
		}},
	}
	if dataType == allocationProfileType {
		p.SampleType[0] = &profile.ValueType{Type: "allocationSize", Unit: "bytes"}
	}
	frameCount := 0
	locations := make(map[uint64]*profile.Location, uniqueStacks*stackDepth)
	functions := make(map[uint64]*profile.Function, uniqueStacks*stackDepth)
	for sampleIndex := range samples {
		stackID := sampleIndex % uniqueStacks
		sample := &profile.Sample{Value: []int64{10_000_000}, Label: map[string][]string{}, NumLabel: map[string][]int64{}}
		if dataType == allocationProfileType {
			sample.Value[0] = int64((stackID + 1) * 1024)
		}
		sample.Label[profileSamplesAttr] = []string{"jdk.ExecutionSample"}
		if dataType == allocationProfileType {
			sample.Label[profileSamplesAttr] = []string{"jdk.ObjectAllocationSample"}
		}
		sample.NumLabel[profileSampleTimeAttr] = []int64{timestamp.UnixMilli()}
		sample.NumLabel[profileSamplePeriodAttr] = []int64{10_000}
		sample.NumLabel[profileThreadIDAttr] = []int64{int64(sampleIndex + 1)}
		sample.Label[profileThreadNameAttr] = []string{fmt.Sprintf("profile-worker-%d", stackID)}
		sample.Label[profileThreadStateAttr] = []string{"RUNNABLE"}
		for depth := range stackDepth {
			locationID := uint64(stackID*stackDepth + depth + 1)
			location := locations[locationID]
			if location == nil {
				function := functions[locationID]
				if function == nil {
					function = &profile.Function{
						ID:         locationID,
						Name:       fmt.Sprintf("com.example.ProfileWork.method%d", depth),
						SystemName: fmt.Sprintf("com.example.ProfileWork.method%d", depth),
						Filename:   "ProfileWork.java",
						StartLine:  int64(depth + 1),
					}
					functions[locationID] = function
					p.Function = append(p.Function, function)
				}
				location = &profile.Location{
					ID:   locationID,
					Line: []profile.Line{{Function: function, Line: int64(depth + 1)}},
				}
				locations[locationID] = location
				p.Location = append(p.Location, location)
			}
			sample.Location = append(sample.Location, location)
			frameCount++
		}
		p.Sample = append(p.Sample, sample)
	}

	if err := p.CheckValid(); err != nil {
		return "", 0, fmt.Errorf("generated pprof profile is invalid: %w", err)
	}
	var buf bytes.Buffer
	if err := p.Write(&buf); err != nil {
		return "", 0, fmt.Errorf("failed to encode pprof profile: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), frameCount, nil
}

func createExporter(cfg *Config) (sdklog.Exporter, error) {
	if cfg.UseHTTP {
		options := []otlploghttp.Option{
			otlploghttp.WithEndpoint(cfg.Endpoint()),
			otlploghttp.WithURLPath(cfg.HTTPPath),
			otlploghttp.WithTimeout(cfg.Timeout),
		}
		if cfg.Insecure {
			options = append(options, otlploghttp.WithInsecure())
		} else {
			tlsConfig, err := config.GetTLSCredentialsForHTTPExporter(cfg.CaFile, cfg.ClientAuth, cfg.InsecureSkipVerify)
			if err != nil {
				return nil, fmt.Errorf("failed to configure OTLP HTTP TLS: %w", err)
			}
			options = append(options, otlploghttp.WithTLSClientConfig(tlsConfig))
		}
		if len(cfg.Headers) > 0 {
			options = append(options, otlploghttp.WithHeaders(cfg.GetHeaders()))
		}
		return otlploghttp.New(context.Background(), options...)
	}

	options := []otlploggrpc.Option{
		otlploggrpc.WithEndpoint(cfg.Endpoint()),
		otlploggrpc.WithTimeout(cfg.Timeout),
	}
	if cfg.Insecure {
		options = append(options, otlploggrpc.WithInsecure())
	} else {
		credentials, err := config.GetTLSCredentialsForGRPCExporter(cfg.CaFile, cfg.ClientAuth, cfg.InsecureSkipVerify)
		if err != nil {
			return nil, fmt.Errorf("failed to configure OTLP gRPC TLS: %w", err)
		}
		options = append(options, otlploggrpc.WithTLSCredentials(credentials))
	}
	if len(cfg.Headers) > 0 {
		options = append(options, otlploggrpc.WithHeaders(cfg.GetHeaders()))
	}
	return otlploggrpc.New(context.Background(), options...)
}
