// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profiles

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/log"
)

// Start generates profiles and exports them to an OTLP/gRPC endpoint.
func Start(cfg *Config) error {
	logger, err := log.CreateLogger(cfg.SkipSettingGRPCLogger)
	if err != nil {
		return err
	}
	logger.Info("starting the profiles generator with configuration", zap.Any("config", cfg))
	return run(cfg, logger)
}

func run(cfg *Config, logger *zap.Logger) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if cfg.TotalDuration.Duration() > 0 || cfg.TotalDuration.IsInf() {
		cfg.NumProfiles = 0
	}
	if err := exportRunConfiguration(cfg, logger); err != nil {
		if !cfg.AllowExportFailures {
			return err
		}
		logger.Error("failed to export profile run configuration metrics; continuing due to --allow-export-failures", zap.Error(err))
	}
	stopReporting := reportRunConfigurationPeriodically(cfg, logger)
	defer stopReporting()

	limit := rate.Limit(cfg.Rate)
	if cfg.Rate == 0 {
		limit = rate.Inf
		logger.Info("generation of profiles isn't being throttled")
	} else {
		logger.Info("generation of profiles is limited", zap.Float64("per-second", float64(limit)))
	}

	var sequence atomic.Uint64
	workerList := make([]worker, cfg.WorkerCount)
	for i := 0; i < cfg.WorkerCount; i++ {
		conn, client, err := newGRPCClient(cfg)
		if err != nil {
			for previous := 0; previous < i; previous++ {
				_ = workerList[previous].conn.Close()
			}
			return fmt.Errorf("failed to create profile exporter: %w", err)
		}
		workerList[i] = worker{
			config:   cfg,
			client:   client,
			conn:     conn,
			limit:    limit,
			sequence: &sequence,
			logger:   logger.With(zap.Int("worker", i)),
		}
	}
	running := &atomic.Bool{}
	running.Store(true)
	var wg sync.WaitGroup
	for i := range workerList {
		wg.Add(1)
		workerList[i].running = running
		workerList[i].wg = &wg
		go workerList[i].generate()
	}
	if cfg.TotalDuration.Duration() > 0 && !cfg.TotalDuration.IsInf() {
		time.Sleep(cfg.TotalDuration.Duration())
		running.Store(false)
	}
	wg.Wait()
	return nil
}

func reportRunConfigurationPeriodically(cfg *Config, logger *zap.Logger) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	ticker := time.NewTicker(cfg.ReportingInterval)
	go func() {
		defer close(done)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := exportRunConfiguration(cfg, logger); err != nil {
					if !cfg.AllowExportFailures {
						logger.Fatal("failed to export profile run configuration metrics", zap.Error(err))
					}
					logger.Error("failed to export profile run configuration metrics; continuing due to --allow-export-failures", zap.Error(err))
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func exportRunConfiguration(cfg *Config, logger *zap.Logger) error {
	conn, err := newGRPCConnection(cfg)
	if err != nil {
		return fmt.Errorf("failed to create metrics exporter: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			logger.Error("failed to close metrics exporter connection", zap.Error(closeErr))
		}
	}()

	client := pmetricotlp.NewGRPCClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	if headers := cfg.GetHeaders(); len(headers) > 0 {
		ctx = metadataContext(ctx, headers)
	}
	response, err := client.Export(ctx, buildRunConfigurationMetrics(cfg))
	if err != nil {
		return fmt.Errorf("failed to export profile run configuration metrics: %w", err)
	}
	if rejected := response.PartialSuccess().RejectedDataPoints(); rejected > 0 {
		return fmt.Errorf("collector rejected %d profile run configuration metric data points: %s", rejected, response.PartialSuccess().ErrorMessage())
	}
	return nil
}

func buildRunConfigurationMetrics(cfg *Config) pmetricotlp.ExportRequest {
	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	for _, attr := range cfg.GetAttributes() {
		if err := resourceMetrics.Resource().Attributes().PutEmpty(string(attr.Key)).FromRaw(attr.Value.AsInterface()); err != nil {
			continue
		}
	}
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName("telemetrygen")
	scopeMetrics.Scope().SetVersion("1.0.0")

	now := pcommon.NewTimestampFromTime(time.Now())
	addIntGauge(scopeMetrics.Metrics(), "telemetrygen_profiles_workers", "Number of profile generator workers", "{worker}", int64(cfg.WorkerCount), now)
	addDoubleGauge(scopeMetrics.Metrics(), "telemetrygen_profiles_rate", "Configured profile generation rate per worker; zero means unlimited", "{profile}/s", cfg.Rate, now)
	batchEnabled := int64(0)
	batchSize := 1
	if cfg.Batch {
		batchEnabled = 1
		batchSize = cfg.BatchSize
	}
	addIntGauge(scopeMetrics.Metrics(), "telemetrygen_profiles_batch_enabled", "Whether profile batching is enabled", "1", batchEnabled, now)
	addIntGauge(scopeMetrics.Metrics(), "telemetrygen_profiles_batch_size", "Configured number of profiles to batch per OTLP export request", "{profile}", int64(cfg.BatchSize), now)
	addIntGauge(scopeMetrics.Metrics(), "telemetrygen_profiles_effective_batch_size", "Number of profiles in each OTLP export request", "{profile}", int64(batchSize), now)
	addIntGauge(scopeMetrics.Metrics(), "telemetrygen_profiles_invalid_profiles", "Number of intentionally invalid profiles in each OTLP export request", "{profile}", int64(cfg.InvalidProfiles), now)
	return pmetricotlp.NewExportRequestFromMetrics(metrics)
}

func addIntGauge(metrics pmetric.MetricSlice, name, description, unit string, value int64, timestamp pcommon.Timestamp) {
	metric := metrics.AppendEmpty()
	metric.SetName(name)
	metric.SetDescription(description)
	metric.SetUnit(unit)
	point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetTimestamp(timestamp)
	point.SetIntValue(value)
}

func addDoubleGauge(metrics pmetric.MetricSlice, name, description, unit string, value float64, timestamp pcommon.Timestamp) {
	metric := metrics.AppendEmpty()
	metric.SetName(name)
	metric.SetDescription(description)
	metric.SetUnit(unit)
	point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetTimestamp(timestamp)
	point.SetDoubleValue(value)
}

type worker struct {
	config   *Config
	client   pprofileotlp.GRPCClient
	conn     *grpc.ClientConn
	limit    rate.Limit
	running  *atomic.Bool
	sequence *atomic.Uint64
	logger   *zap.Logger
	wg       *sync.WaitGroup
}

func (w *worker) generate() {
	defer w.wg.Done()
	defer func() {
		if err := w.conn.Close(); err != nil {
			w.logger.Error("failed to close gRPC connection", zap.Error(err))
		}
	}()

	limiter := rate.NewLimiter(w.limit, 1)
	generated := 0
	for w.running.Load() && (w.config.NumProfiles == 0 || generated < w.config.NumProfiles) {
		batchSize := 1
		if w.config.Batch {
			batchSize = w.config.BatchSize
			if w.config.NumProfiles > 0 && generated+batchSize > w.config.NumProfiles {
				batchSize = w.config.NumProfiles - generated
			}
		}
		for i := 0; i < batchSize; i++ {
			if err := limiter.Wait(context.Background()); err != nil {
				w.logger.Fatal("rate limiter wait failed", zap.Error(err))
			}
		}

		profiles := buildProfiles(w.config, batchSize, w.config.InvalidProfiles, w.sequence)
		request := pprofileotlp.NewExportRequestFromProfiles(profiles)
		ctx, cancel := context.WithTimeout(context.Background(), w.config.Timeout)
		if headers := w.config.GetHeaders(); len(headers) > 0 {
			ctx = metadataContext(ctx, headers)
		}
		response, err := w.client.Export(ctx, request)
		cancel()
		if err != nil {
			if w.config.AllowExportFailures {
				w.logger.Error("profile export failed; continuing due to --allow-export-failures", zap.Error(err))
			} else {
				w.logger.Fatal("profile export failed", zap.Error(err))
			}
		} else if rejected := response.PartialSuccess().RejectedProfiles(); rejected > 0 {
			w.logger.Warn("Collector rejected profiles", zap.Int64("rejected", rejected), zap.String("message", response.PartialSuccess().ErrorMessage()))
		}
		generated += batchSize
	}
	w.logger.Info("profiles generated", zap.Int("profiles", generated))
}

func buildProfiles(cfg *Config, count, invalidCount int, sequence *atomic.Uint64) pprofile.Profiles {
	profiles := pprofile.NewProfiles()
	dictionary := profiles.Dictionary()
	dictionary.MappingTable().AppendEmpty()
	dictionary.LocationTable().AppendEmpty()
	dictionary.FunctionTable().AppendEmpty()
	dictionary.LinkTable().AppendEmpty()
	dictionary.StackTable().AppendEmpty()
	dictionary.AttributeTable().AppendEmpty()
	dictionary.StringTable().Append("")

	cpuType := appendString(dictionary, "cpu")
	nanoseconds := appendString(dictionary, "nanoseconds")
	periodType := appendString(dictionary, "cpu")
	periodUnit := appendString(dictionary, "nanoseconds")
	uniqueStacks := min(cfg.UniqueStacks, cfg.NumSamples)
	stackIndices := buildStacks(dictionary, uniqueStacks, cfg.StackDepth)

	resourceProfiles := profiles.ResourceProfiles().AppendEmpty()
	resource := resourceProfiles.Resource()
	for _, attr := range cfg.GetAttributes() {
		putAttribute(resource.Attributes(), attr)
	}
	scopeProfiles := resourceProfiles.ScopeProfiles().AppendEmpty()
	scopeProfiles.Scope().SetName("telemetrygen")
	scopeProfiles.Scope().SetVersion("1.0.0")

	createdAt := time.Now()
	for i := 0; i < count; i++ {
		profile := scopeProfiles.Profiles().AppendEmpty()
		profile.SampleType().SetTypeStrindex(cpuType)
		profile.SampleType().SetUnitStrindex(nanoseconds)
		profile.PeriodType().SetTypeStrindex(periodType)
		profile.PeriodType().SetUnitStrindex(periodUnit)
		profile.SetPeriod(10_000_000)
		profile.SetTime(pcommon.NewTimestampFromTime(createdAt))
		profile.SetDurationNano(uint64(time.Second))
		profile.SetProfileID(nextProfileID(sequence))
		for _, attr := range cfg.GetTelemetryAttributes() {
			attributeIndex := dictionary.AttributeTable().Len()
			kv := dictionary.AttributeTable().AppendEmpty()
			kv.SetKeyStrindex(appendString(dictionary, string(attr.Key)))
			if err := kv.Value().FromRaw(attr.Value.AsInterface()); err != nil {
				continue
			}
			profile.AttributeIndices().Append(int32(attributeIndex))
		}
		if cfg.LoadSize > 0 {
			attributeIndex := dictionary.AttributeTable().Len()
			kv := dictionary.AttributeTable().AppendEmpty()
			kv.SetKeyStrindex(appendString(dictionary, "load-data"))
			kv.Value().SetStr(strings.Repeat("x", cfg.LoadSize*1024*1024))
			profile.AttributeIndices().Append(int32(attributeIndex))
		}
		for sampleIndex := 0; sampleIndex < cfg.NumSamples; sampleIndex++ {
			sample := profile.Samples().AppendEmpty()
			sample.SetStackIndex(stackIndices[sampleIndex%len(stackIndices)])
			sample.Values().Append(10_000_000)
		}
		if i < invalidCount {
			// Keep the protobuf encodable but make its sample type reference a missing string.
			profile.SampleType().SetTypeStrindex(int32(dictionary.StringTable().Len()))
		}
	}
	return profiles
}

func buildStacks(dictionary pprofile.ProfilesDictionary, count, depth int) []int32 {
	stackIndices := make([]int32, 0, count)
	for stackIndex := 0; stackIndex < count; stackIndex++ {
		stack := dictionary.StackTable().AppendEmpty()
		for frameIndex := 0; frameIndex < depth; frameIndex++ {
			nameIndex := appendString(dictionary, fmt.Sprintf("work_%d_frame_%d", stackIndex, frameIndex))
			fileIndex := appendString(dictionary, fmt.Sprintf("/telemetrygen/stack_%d.go", stackIndex))
			function := dictionary.FunctionTable().AppendEmpty()
			function.SetNameStrindex(nameIndex)
			function.SetSystemNameStrindex(nameIndex)
			function.SetFilenameStrindex(fileIndex)
			function.SetStartLine(int64(frameIndex + 1))

			location := dictionary.LocationTable().AppendEmpty()
			location.SetAddress(uint64(stackIndex*depth + frameIndex + 1))
			line := location.Lines().AppendEmpty()
			line.SetFunctionIndex(int32(dictionary.FunctionTable().Len() - 1))
			line.SetLine(int64(frameIndex + 1))
			stack.LocationIndices().Append(int32(dictionary.LocationTable().Len() - 1))
		}
		stackIndices = append(stackIndices, int32(dictionary.StackTable().Len()-1))
	}
	return stackIndices
}

func appendString(dictionary pprofile.ProfilesDictionary, value string) int32 {
	index := int32(dictionary.StringTable().Len())
	dictionary.StringTable().Append(value)
	return index
}

func nextProfileID(sequence *atomic.Uint64) pprofile.ProfileID {
	var id pprofile.ProfileID
	binary.BigEndian.PutUint64(id[:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(id[8:], sequence.Add(1))
	return id
}

func putAttribute(attrs pcommon.Map, attr attribute.KeyValue) {
	if err := attrs.PutEmpty(string(attr.Key)).FromRaw(attr.Value.AsInterface()); err != nil {
		return
	}
}
