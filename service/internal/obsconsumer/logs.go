// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package obsconsumer // import "go.opentelemetry.io/collector/service/internal/obsconsumer"

import (
	"context"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/metric"
)

var _ consumer.Logs = logs{}

func NewLogs(consumer consumer.Logs, callbacks ...LogsCallback) consumer.Logs {
	return logs{
		consumer:  consumer,
		callbacks: callbacks,
	}
}

type logs struct {
	consumer  consumer.Logs
	callbacks []LogsCallback
}

func (c logs) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	var err error
	for _, callback := range c.callbacks {
		defer callback(ctx, ld)(&err)
	}
	err = c.consumer.ConsumeLogs(ctx, ld)
	return err
}

func (c logs) Capabilities() consumer.Capabilities {
	return c.consumer.Capabilities()
}

// LogsCallback is a function that is called prior to ConsumeLogs.
// It returns another callback that will be called with the result of the ConsumeLogs call.
// It MUST NOT modify the plog.Logs object in any way.
type LogsCallback func(ctx context.Context, ld plog.Logs) func(*error)

// CountLogs returns a LogsCallback that counts the number of logs.
func CountLogs(itemCounter metric.Int64Counter, opts ...CallbackOption) LogsCallback {
	cbos := &callbackOptions{}
	for _, opt := range opts {
		opt.apply(cbos)
	}
	compiledOptions := cbos.compile()
	return func(ctx context.Context, ld plog.Logs) func(*error) {
		itemCount := ld.LogRecordCount()
		return func(err *error) {
			if *err == nil {
				itemCounter.Add(ctx, int64(itemCount), compiledOptions.withSuccessAttrs)
			} else {
				itemCounter.Add(ctx, int64(itemCount), compiledOptions.withFailureAttrs)
			}
		}
	}
}
