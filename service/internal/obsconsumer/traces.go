// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package obsconsumer // import "go.opentelemetry.io/collector/service/internal/obsconsumer"

import (
	"context"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric"
)

var _ consumer.Traces = traces{}

func NewTraces(consumer consumer.Traces, callbacks ...TracesCallback) consumer.Traces {
	return traces{
		consumer:  consumer,
		callbacks: callbacks,
	}
}

type traces struct {
	consumer  consumer.Traces
	callbacks []TracesCallback
}

func (c traces) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var err error
	for _, callback := range c.callbacks {
		defer callback(ctx, td)(&err)
	}
	err = c.consumer.ConsumeTraces(ctx, td)
	return err
}

func (c traces) Capabilities() consumer.Capabilities {
	return c.consumer.Capabilities()
}

// TracesCallback is a function that is called prior to ConsumeTraces.
// It returns another callback that will be called with the result of the ConsumeTraces call.
// It MUST NOT modify the ptrace.Traces object in any way.
type TracesCallback func(ctx context.Context, td ptrace.Traces) func(*error)

// CountTraces returns a TracesCallback that counts the number of traces.
func CountTraces(itemCounter metric.Int64Counter, opts ...CallbackOption) TracesCallback {
	cbos := &callbackOptions{}
	for _, opt := range opts {
		opt.apply(cbos)
	}
	compiledOptions := cbos.compile()
	return func(ctx context.Context, td ptrace.Traces) func(*error) {
		itemCount := td.SpanCount()
		return func(err *error) {
			if *err == nil {
				itemCounter.Add(ctx, int64(itemCount), compiledOptions.withSuccessAttrs)
			} else {
				itemCounter.Add(ctx, int64(itemCount), compiledOptions.withFailureAttrs)
			}
		}
	}
}
