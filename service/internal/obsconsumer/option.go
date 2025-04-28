// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package obsconsumer // import "go.opentelemetry.io/collector/service/internal/obsconsumer"

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// CallbackOption modifies the consumer behavior.
type CallbackOption interface {
	apply(*callbackOptions)
}

type callbackOptions struct {
	staticDataPointAttributes []attribute.KeyValue
}

// WithStaticDataPointAttribute returns an Option that adds a static attribute to data points.
func WithStaticDataPointAttribute(attr attribute.KeyValue) CallbackOption {
	return staticDataPointAttributeOption(attr)
}

type staticDataPointAttributeOption attribute.KeyValue

func (o staticDataPointAttributeOption) apply(opts *callbackOptions) {
	opts.staticDataPointAttributes = append(opts.staticDataPointAttributes, attribute.KeyValue(o))
}

type compiledOptions struct {
	withSuccessAttrs metric.AddOption
	withFailureAttrs metric.AddOption
}

func (o *callbackOptions) compile() compiledOptions {
	successAttrs := make([]attribute.KeyValue, 0, 1+len(o.staticDataPointAttributes))
	successAttrs = append(successAttrs, attribute.String("outcome", "success"))
	successAttrs = append(successAttrs, o.staticDataPointAttributes...)

	failureAttrs := make([]attribute.KeyValue, 0, 1+len(o.staticDataPointAttributes))
	failureAttrs = append(failureAttrs, attribute.String("outcome", "failure"))
	failureAttrs = append(failureAttrs, o.staticDataPointAttributes...)

	return compiledOptions{
		withSuccessAttrs: metric.WithAttributes(successAttrs...),
		withFailureAttrs: metric.WithAttributes(failureAttrs...),
	}
}
