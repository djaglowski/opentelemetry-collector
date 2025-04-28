// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package obsconsumer // import "go.opentelemetry.io/collector/service/internal/obsconsumer"

import (
	"context"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/otel/metric"
)

var _ xconsumer.Profiles = profiles{}

func NewProfiles(consumer xconsumer.Profiles, callbacks ...ProfilesCallback) xconsumer.Profiles {
	return profiles{
		consumer:  consumer,
		callbacks: callbacks,
	}
}

type profiles struct {
	consumer  xconsumer.Profiles
	callbacks []ProfilesCallback
}

func (c profiles) ConsumeProfiles(ctx context.Context, pd pprofile.Profiles) error {
	var err error
	for _, callback := range c.callbacks {
		defer callback(ctx, pd)(&err)
	}
	err = c.consumer.ConsumeProfiles(ctx, pd)
	return err
}

func (c profiles) Capabilities() consumer.Capabilities {
	return c.consumer.Capabilities()
}

// ProfilesCallback is a function that is called prior to ConsumeProfiles.
// It returns another callback that will be called with the result of the ConsumeProfiles call.
// It MUST NOT modify the pprofile.Profiles object in any way.
type ProfilesCallback func(ctx context.Context, pd pprofile.Profiles) func(*error)

// CountProfiles returns a ProfilesCallback that counts the number of profiles.
func CountProfiles(itemCounter metric.Int64Counter, opts ...CallbackOption) ProfilesCallback {
	cbos := &callbackOptions{}
	for _, opt := range opts {
		opt.apply(cbos)
	}
	compiledOptions := cbos.compile()
	return func(ctx context.Context, pd pprofile.Profiles) func(*error) {
		itemCount := pd.SampleCount()
		return func(err *error) {
			if *err == nil {
				itemCounter.Add(ctx, int64(itemCount), compiledOptions.withSuccessAttrs)
			} else {
				itemCounter.Add(ctx, int64(itemCount), compiledOptions.withFailureAttrs)
			}
		}
	}
}
