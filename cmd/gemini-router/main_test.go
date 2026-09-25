package main

import (
	"strings"
	"testing"
)

func TestUsageTextDescribesCurrentStatsAndQueueBehavior(t *testing.T) {
	for _, want := range []string{
		"queue depth",
		"predicted wait",
		"5h/weekly quotas",
		"--queue-unbounded",
		"status [--json]",
		"quota [--json]",
		"all provider quota",
	} {
		if !strings.Contains(usageText, want) {
			t.Fatalf("usageText missing %q:\n%s", want, usageText)
		}
	}
}
