package control

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"
)

type ErrorClass string

const (
	Success                  ErrorClass = "success"
	ShortThrottle            ErrorClass = "short_throttle"
	LongQuotaExhausted       ErrorClass = "long_quota_exhausted"
	TransientUpstreamFailure ErrorClass = "transient_upstream_failure"
	AuthOrEligibilityFailure ErrorClass = "auth_or_eligibility_failure"
	InvalidRequestOrModel    ErrorClass = "invalid_request_or_model"
	LocalPreparationFailure  ErrorClass = "local_preparation_failure"
	Timeout                  ErrorClass = "timeout"
	ClientCancelled          ErrorClass = "client_cancelled"
	RouterOverloaded         ErrorClass = "router_overloaded"
)

type ClassifiedError struct {
	Class         ErrorClass
	Message       string
	ResetDuration time.Duration
}

var resetRE = regexp.MustCompile(`(?i)resets in\s+([0-9hms]+)`)

func ClassifyProcessOutput(exitErr error, stdout, stderr, ownedLog string) ClassifiedError {
	if errors.Is(exitErr, context.Canceled) {
		return ClassifiedError{Class: ClientCancelled, Message: "client cancelled"}
	}
	if errors.Is(exitErr, context.DeadlineExceeded) {
		return ClassifiedError{Class: Timeout, Message: "request timeout"}
	}
	diagnosticParts := []string{stderr, ownedLog}
	if exitErr != nil {
		diagnosticParts = append(diagnosticParts, stdout)
	}
	combined := strings.TrimSpace(strings.Join(diagnosticParts, "\n"))
	lower := strings.ToLower(combined)
	reset := time.Duration(0)
	if m := resetRE.FindStringSubmatch(combined); len(m) == 2 {
		reset, _ = time.ParseDuration(m[1])
	}
	if strings.Contains(lower, "individual quota reached") || strings.Contains(lower, "weekly quota") || strings.Contains(lower, "five-hour quota") {
		return ClassifiedError{Class: LongQuotaExhausted, Message: combined, ResetDuration: reset}
	}
	if strings.Contains(lower, "account ineligible") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "authentication failed") || strings.Contains(lower, "forbidden") {
		return ClassifiedError{Class: AuthOrEligibilityFailure, Message: combined}
	}
	if strings.Contains(lower, "invalid model") || strings.Contains(lower, "invalid request") || strings.Contains(lower, "unknown model") {
		return ClassifiedError{Class: InvalidRequestOrModel, Message: combined}
	}
	if strings.Contains(lower, "resource_exhausted") || strings.Contains(lower, "too many requests") || strings.Contains(lower, "rate limit exceeded") || strings.Contains(lower, "rate_limit_exceeded") {
		return ClassifiedError{Class: ShortThrottle, Message: combined}
	}
	if exitErr != nil {
		return ClassifiedError{Class: TransientUpstreamFailure, Message: combined}
	}
	return ClassifiedError{Class: Success, Message: strings.TrimSpace(stdout)}
}
