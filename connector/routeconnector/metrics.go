// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package routeconnector // import "go.opentelemetry.io/collector/connector/routeconnector"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type MetricsRoute struct {
	MetricsCondition `mapstructure:",squash"`
	Pipelines        []component.ID `mapstructure:"pipelines"`
}

type metricsConsumerRoute struct {
	MetricsCondition
	consumer.Metrics
}

// TODO this is a placeholder for a real grammar
type MetricsCondition struct {
	MatchAll bool `mapstructure:"match_all"`
	MinInt   *int `mapstructure:"min_int"`
	MaxInt   *int `mapstructure:"max_int"`
}

func (c MetricsCondition) Validate() error {
	if c.MatchAll {
		return nil
	}
	if c.MinInt != nil || c.MaxInt != nil {
		return nil
	}
	return fmt.Errorf("must specify a match condition")
}

func (c MetricsCondition) Match(md pmetric.Metrics) bool {
	if c.MatchAll {
		return true
	}

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				dps := ms.At(k).Sum().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					if c.MinInt != nil && dps.At(l).IntValue() >= int64(*c.MinInt) {
						return true
					}
					if c.MaxInt != nil && dps.At(l).IntValue() <= int64(*c.MaxInt) {
						return true
					}
				}
			}
		}
	}

	return false
}

// createMetrics creates a metric router that may emit to only a
// subset of its downstream consumers. Rather than giving it a fanout
// consumer, we need to give it the means to decide which consumer
// should be called. In some cases, it may emit to more than one
// consumer, in which case we want to make sure they use a fanoutconsumer,
// but we don't want them to have to build it every time.
func (f *routeFactory) createMetrics(
	_ context.Context,
	set connector.CreateSettings,
	cfg component.Config,
	nextConsumers *connector.MetricsConsumerMap,
) (connector.Metrics, error) {
	comp, err := f.GetOrAdd(cfg, func() (component.Component, error) {
		rCfg, ok := cfg.(*Config)
		if !ok {
			return nil, fmt.Errorf("not a route config")
		}

		r := &router{metricsTable: []metricsConsumerRoute{}}
		for _, tr := range rCfg.MetricsTable {
			cons, err := nextConsumers.FanoutToPipelines(tr.Pipelines)
			if err != nil {
				return nil, err
			}
			r.metricsTable = append(r.metricsTable, metricsConsumerRoute{
				MetricsCondition: tr.MetricsCondition,
				Metrics:          cons,
			})
		}
		return r, nil
	})
	if err != nil {
		return nil, err
	}

	conn := comp.Unwrap().(*router)
	return conn, nil
}

func (r *router) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	for _, route := range r.metricsTable {
		if route.MetricsCondition.Match(md) {
			return route.ConsumeMetrics(ctx, md)
		}
	}
	return nil // No match, just drop
}
