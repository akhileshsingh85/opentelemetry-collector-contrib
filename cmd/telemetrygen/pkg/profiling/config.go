// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profiling

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/spf13/pflag"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/config"
	types "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/pkg"
)

const (
	profilerEnabledEnv       = "SPLUNK_PROFILER_ENABLED"
	profilerMemoryEnabledEnv = "SPLUNK_PROFILER_MEMORY_ENABLED"
	profilerLogsEndpointEnv  = "SPLUNK_PROFILER_LOGS_ENDPOINT"
	defaultLogsEndpoint      = "http://localhost:4318/v1/logs"
	defaultErrorLogFile      = "telemetrygen-profiling-errors.log"
)

// Config describes the profiling log workload.
type Config struct {
	config.Config
	NumProfiles      int
	Errors           int
	Samples          int
	StackDepth       int
	UniqueStacks     int
	ErrorLogFile     string
	CPUEnabled       bool
	MemoryEnabled    bool
	environmentError error
}

// NewConfig creates a configuration using the same environment variable names as the Splunk
// instrumentation libraries. Profiling remains disabled unless SPLUNK_PROFILER_ENABLED is true.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.Rate = 1
	cfg.TotalDuration = types.DurationWithInf(0)
	cfg.ReportingInterval = 5 * time.Minute
	cfg.UseHTTP = true
	cfg.HTTPPath = "/v1/logs"
	cfg.NumProfiles = 1
	cfg.Samples = 100
	cfg.StackDepth = 32
	cfg.UniqueStacks = 20
	cfg.ErrorLogFile = defaultErrorLogFile

	var err error
	cfg.CPUEnabled, err = environmentBool(profilerEnabledEnv)
	if err != nil {
		cfg.environmentError = err
	}
	cfg.MemoryEnabled, err = environmentBool(profilerMemoryEnabledEnv)
	if err != nil && cfg.environmentError == nil {
		cfg.environmentError = err
	}
	if endpoint := os.Getenv(profilerLogsEndpointEnv); endpoint != "" {
		if err := cfg.setLogsEndpoint(endpoint); err != nil && cfg.environmentError == nil {
			cfg.environmentError = fmt.Errorf("invalid %s: %w", profilerLogsEndpointEnv, err)
		}
	} else {
		_ = cfg.setLogsEndpoint(defaultLogsEndpoint)
	}
	return cfg
}

// Flags registers the profiling flags.
func (c *Config) Flags(fs *pflag.FlagSet) {
	c.CommonFlags(fs)
	fs.Lookup("interval").Usage = "Interval for exporting profiling delivery metrics"
	for _, name := range []string{"batch", "size"} {
		fs.Lookup(name).Hidden = true
	}
	fs.Lookup("rate").Usage = "Profile log records per second per worker; zero means no throttling"
	fs.Lookup("batch-size").Usage = "Maximum number of profile log records per OTLP export request"
	fs.StringVar(&c.HTTPPath, "otlp-http-url-path", c.HTTPPath, "OTLP HTTP path for profiling log requests")
	fs.StringVar(&c.ErrorLogFile, "error-log-file", c.ErrorLogFile, "File for profiling export errors")
	fs.IntVar(&c.NumProfiles, "profiles", c.NumProfiles, "Number of profile log records to generate per worker (ignored when duration is set)")
	fs.IntVar(&c.Errors, "errors", c.Errors, "Percentage of generated profile log records with intentionally invalid profile payloads (0-100)")
	fs.IntVar(&c.Samples, "samples", c.Samples, "Number of stack samples in each generated profile")
	fs.IntVar(&c.StackDepth, "stack-depth", c.StackDepth, "Number of frames in each generated stack")
	fs.IntVar(&c.UniqueStacks, "unique-stacks", c.UniqueStacks, "Number of distinct stacks to cycle through")
	fs.BoolVar(&c.CPUEnabled, "profiler-enabled", c.CPUEnabled, "Generate CPU profiles; defaults from SPLUNK_PROFILER_ENABLED")
	fs.BoolVar(&c.MemoryEnabled, "memory-profiler-enabled", c.MemoryEnabled, "Generate allocation profiles when CPU profiling is enabled; defaults from SPLUNK_PROFILER_MEMORY_ENABLED")
}

// Validate validates the profiling workload.
func (c *Config) Validate() error {
	if c.environmentError != nil {
		return c.environmentError
	}
	if !c.CPUEnabled {
		return fmt.Errorf("profiling is disabled; set %s=true or pass --profiler-enabled", profilerEnabledEnv)
	}
	if c.TotalDuration.Duration() <= 0 && !c.TotalDuration.IsInf() && c.NumProfiles <= 0 {
		return errors.New("either --profiles or --duration must be greater than 0")
	}
	if c.WorkerCount <= 0 {
		return errors.New("workers must be greater than 0")
	}
	if c.Rate < 0 || math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) {
		return errors.New("rate must be a finite non-negative number")
	}
	if c.Errors < 0 || c.Errors > 100 {
		return errors.New("errors must be between 0 and 100")
	}
	if c.BatchSize <= 0 {
		return errors.New("batch-size must be greater than 0")
	}
	if c.ReportingInterval <= 0 {
		return errors.New("interval must be greater than 0")
	}
	if c.Samples <= 0 {
		return errors.New("samples must be greater than 0")
	}
	if c.StackDepth <= 0 {
		return errors.New("stack-depth must be greater than 0")
	}
	if c.UniqueStacks <= 0 {
		return errors.New("unique-stacks must be greater than 0")
	}
	if c.HTTPPath == "" {
		return errors.New("OTLP HTTP path must not be empty")
	}
	return nil
}

func environmentBool(name string) (bool, error) {
	value := os.Getenv(name)
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	return parsed, nil
}

func (c *Config) setLogsEndpoint(raw string) error {
	endpoint, err := url.ParseRequestURI(raw)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return fmt.Errorf("expected an http or https URL, got %q", raw)
	}
	c.CustomEndpoint = endpoint.Host
	if endpoint.Path != "" && endpoint.Path != "/" {
		c.HTTPPath = endpoint.Path
	}
	c.UseHTTP = true
	c.Insecure = endpoint.Scheme == "http"
	return nil
}
