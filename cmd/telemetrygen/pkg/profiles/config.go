// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profiles

import (
	"errors"
	"math"
	"time"

	"github.com/spf13/pflag"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/config"
	types "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/pkg"
)

// Config describes the profile generation scenario.
type Config struct {
	config.Config
	NumProfiles     int
	InvalidProfiles int
	NumSamples      int
	StackDepth      int
	UniqueStacks    int
	MaxMessageMiB   int
}

func NewConfig() *Config {
	cfg := &Config{}
	cfg.SetDefaults()
	return cfg
}

// Flags registers configuration flags.
func (c *Config) Flags(fs *pflag.FlagSet) {
	c.CommonFlags(fs)
	fs.IntVar(&c.NumProfiles, "profiles", c.NumProfiles, "Number of profiles to generate in each worker (ignored if duration is provided)")
	fs.IntVar(&c.InvalidProfiles, "invalid-profiles", c.InvalidProfiles, "Number of intentionally invalid profiles to include in each export batch")
	fs.IntVar(&c.NumSamples, "samples", c.NumSamples, "Number of samples in each generated profile")
	fs.IntVar(&c.StackDepth, "stack-depth", c.StackDepth, "Number of locations in each unique stack")
	fs.IntVar(&c.UniqueStacks, "unique-stacks", c.UniqueStacks, "Number of unique stacks in each profile; capped at the sample count")
	fs.IntVar(&c.MaxMessageMiB, "max-message-mib", c.MaxMessageMiB, "Maximum OTLP/gRPC request size in MiB")
}

// SetDefaults sets defaults before command line flags are parsed.
func (c *Config) SetDefaults() {
	c.Config.SetDefaults()
	c.Rate = 1
	c.TotalDuration = types.DurationWithInf(0)
	c.ReportingInterval = 5 * time.Minute
	c.NumProfiles = 1
	c.NumSamples = 100
	c.StackDepth = 32
	c.UniqueStacks = 100
	c.MaxMessageMiB = 4
}

// Validate checks that the profile scenario can run.
func (c *Config) Validate() error {
	if c.InvalidProfiles < 0 {
		return errors.New("invalid-profiles must be non-negative")
	}
	maxInvalidProfiles := 1
	if c.Batch {
		maxInvalidProfiles = c.BatchSize
	}
	if c.InvalidProfiles > maxInvalidProfiles {
		return errors.New("invalid-profiles cannot exceed the number of profiles in each export request")
	}
	if c.ReportingInterval <= 0 {
		return errors.New("reporting interval must be greater than 0")
	}
	if c.TotalDuration.Duration() <= 0 && c.NumProfiles <= 0 && !c.TotalDuration.IsInf() {
		return errors.New("either `profiles` or `duration` must be greater than 0")
	}
	if c.Rate < 0 || math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) {
		return errors.New("rate must be a finite non-negative number")
	}
	if c.WorkerCount <= 0 {
		return errors.New("workers must be greater than 0")
	}
	if c.Batch && c.BatchSize <= 0 {
		return errors.New("batch-size must be greater than 0")
	}
	if c.NumSamples <= 0 {
		return errors.New("samples must be greater than 0")
	}
	if c.StackDepth <= 0 {
		return errors.New("stack-depth must be greater than 0")
	}
	if c.UniqueStacks <= 0 {
		return errors.New("unique-stacks must be greater than 0")
	}
	if c.MaxMessageMiB <= 0 || c.MaxMessageMiB > int(^uint(0)>>1)/(1024*1024) {
		return errors.New("max-message-mib must be positive and fit in an integer number of bytes")
	}
	if c.UseHTTP {
		return errors.New("profiles are currently sent over OTLP/gRPC; --otlp-http is not supported")
	}
	if c.LoadSize < 0 || c.LoadSize > int(^uint(0)>>1)/(1024*1024) {
		return errors.New("size must be non-negative and fit in an integer number of bytes")
	}
	return nil
}
