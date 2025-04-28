// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package obsconsumer // import "go.opentelemetry.io/collector/service/internal/obsconsumer"

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var _ consumer.Metrics = metrics{}

func NewMetrics(consumer consumer.Metrics, callbacks ...MetricsCallback) consumer.Metrics {
	return metrics{
		consumer:  consumer,
		callbacks: callbacks,
	}
}

type metrics struct {
	consumer  consumer.Metrics
	callbacks []MetricsCallback
}

func (c metrics) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	var err error
	for _, callback := range c.callbacks {
		defer callback(ctx, md)(&err)
	}
	err = c.consumer.ConsumeMetrics(ctx, md)
	return err
}

func (c metrics) Capabilities() consumer.Capabilities {
	return c.consumer.Capabilities()
}

// MetricsCallback is a function that is called prior to ConsumeMetrics.
// It returns another callback that will be called with the result of the ConsumeMetrics call.
// It MUST NOT modify the pmetric.Metrics object in any way.
type MetricsCallback func(ctx context.Context, md pmetric.Metrics) func(*error)

// CountMetrics returns a MetricsCallback that counts the number of metrics.
func CountMetrics(itemCounter metric.Int64Counter, opts ...CallbackOption) MetricsCallback {
	cbos := &callbackOptions{}
	for _, opt := range opts {
		opt.apply(cbos)
	}
	compiledOptions := cbos.compile()
	return func(ctx context.Context, md pmetric.Metrics) func(*error) {
		itemCount := md.DataPointCount()
		return func(err *error) {
			if *err == nil {
				itemCounter.Add(ctx, int64(itemCount), compiledOptions.withSuccessAttrs)
			} else {
				itemCounter.Add(ctx, int64(itemCount), compiledOptions.withFailureAttrs)
			}
		}
	}
}
