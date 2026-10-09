// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profiles

import (
	"iter"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pprofile"
)

func TestBuildProfilesRandomizesInvalidFields(t *testing.T) {
	const invalidCount = 18
	cfg := NewConfig()
	cfg.NumSamples = 1
	cfg.UniqueStacks = 1
	cfg.StackDepth = 1

	sequence := &atomic.Uint64{}
	mutations := &invalidProfileMutationGenerator{}
	seen := make(map[string]int)
	for range invalidCount {
		profiles := buildProfiles(cfg, 1, 1, sequence, mutations)
		profile := profiles.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(0)
		kinds := invalidProfileKinds(profile, profiles.Dictionary())
		require.Len(t, kinds, 1, "each marked profile should have one invalid field")
		seen[kinds[0]]++
	}

	require.Len(t, seen, 9, "every supported invalid field should be used before the order repeats")
	for kind, count := range seen {
		require.Equal(t, 2, count, "invalid field %q should repeat after one complete cycle", kind)
	}
}

func invalidProfileKinds(profile pprofile.Profile, dictionary pprofile.ProfilesDictionary) []string {
	var kinds []string
	stringTableLen := dictionary.StringTable().Len()
	if int(profile.SampleType().TypeStrindex()) >= stringTableLen {
		kinds = append(kinds, "sample type string")
	}
	if int(profile.SampleType().UnitStrindex()) >= stringTableLen {
		kinds = append(kinds, "sample unit string")
	}
	if int(profile.PeriodType().TypeStrindex()) >= stringTableLen {
		kinds = append(kinds, "period type string")
	}
	if int(profile.PeriodType().UnitStrindex()) >= stringTableLen {
		kinds = append(kinds, "period unit string")
	}
	if hasInvalidAttributeIndex(profile.AttributeIndices().All(), dictionary.AttributeTable().Len()) {
		kinds = append(kinds, "profile attribute")
	}
	if profile.Samples().Len() == 0 {
		return kinds
	}
	sample := profile.Samples().At(0)
	if int(sample.StackIndex()) >= dictionary.StackTable().Len() {
		kinds = append(kinds, "sample stack")
	}
	if hasInvalidAttributeIndex(sample.AttributeIndices().All(), dictionary.AttributeTable().Len()) {
		kinds = append(kinds, "sample attribute")
	}
	if sample.LinkIndex() < 0 || int(sample.LinkIndex()) >= dictionary.LinkTable().Len() {
		kinds = append(kinds, "sample link")
	}
	if sample.Values().Len() > 1 && sample.TimestampsUnixNano().Len() == 0 {
		kinds = append(kinds, "sample values shape")
	}
	return kinds
}

func hasInvalidAttributeIndex(indices iter.Seq2[int, int32], tableLen int) bool {
	for _, index := range indices {
		if int(index) >= tableLen {
			return true
		}
	}
	return false
}
