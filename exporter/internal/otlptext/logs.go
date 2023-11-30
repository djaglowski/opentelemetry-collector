// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlptext // import "go.opentelemetry.io/collector/exporter/internal/otlptext"

import (
	"go.opentelemetry.io/collector/pdata/plog"
)

// NewTextLogsMarshaler returns a plog.Marshaler to encode to OTLP text bytes.
func NewTextLogsMarshaler() plog.Marshaler {
	return textLogsMarshaler{}
}

type textLogsMarshaler struct{}

// MarshalLogs plog.Logs to OTLP text.
func (textLogsMarshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	buf := dataBuffer{}
	ld.ResourceLogs().Range(func(i int, rl plog.ResourceLogs) {
		buf.logEntry("ResourceLog #%d", i)
		buf.logEntry("Resource SchemaURL: %s", rl.SchemaUrl())
		buf.logAttributes("Resource attributes", rl.Resource().Attributes())
		rl.ScopeLogs().Range(func(j int, ils plog.ScopeLogs) {
			buf.logEntry("ScopeLogs #%d", j)
			buf.logEntry("ScopeLogs SchemaURL: %s", ils.SchemaUrl())
			buf.logInstrumentationScope(ils.Scope())
			ils.LogRecords().Range(func(k int, lr plog.LogRecord) {
				buf.logEntry("LogRecord #%d", k)
				buf.logEntry("ObservedTimestamp: %s", lr.ObservedTimestamp())
				buf.logEntry("Timestamp: %s", lr.Timestamp())
				buf.logEntry("SeverityText: %s", lr.SeverityText())
				buf.logEntry("SeverityNumber: %s(%d)", lr.SeverityNumber(), lr.SeverityNumber())
				buf.logEntry("Body: %s", valueToString(lr.Body()))
				buf.logAttributes("Attributes", lr.Attributes())
				buf.logEntry("Trace ID: %s", lr.TraceID())
				buf.logEntry("Span ID: %s", lr.SpanID())
				buf.logEntry("Flags: %d", lr.Flags())
			})
		})
	})
	return buf.buf.Bytes(), nil
}
