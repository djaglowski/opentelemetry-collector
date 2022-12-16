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

package finitestaterouterconnector // import "go.opentelemetry.io/collector/connector/finitestaterouterconnector"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/pdata/plog"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type MetricsRoute struct {
	MetricsCondition `mapstructure:",squash"`
	Pipelines        []component.ID `mapstructure:"pipelines"`
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
func (f *finiteStateRouterFactory) createMetrics(
	_ context.Context,
	set connector.CreateSettings,
	cfg component.Config,
	nextConsumers *connector.MetricsConsumerMap,
) (connector.Metrics, error) {
	comp, err := f.GetOrAdd(cfg, createFiniteStateRouter(set, cfg))
	if err != nil {
		return nil, err
	}
	conn := comp.Unwrap().(*finiteStateRouter)
	conn.MetricsConsumerMap = nextConsumers
	return conn, nil
}

func (r *finiteStateRouter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	r.stateMux.Lock()
	defer r.stateMux.Unlock()

	for _, trigger := range r.currentState.Triggers {
		var triggered bool
		mt := trigger.MetricThreshold
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if m.Name() != mt.MetricName {
						continue
					}
					dps := m.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						v := dps.At(l).IntValue()
						if (mt.Above != nil && int(v) > *mt.Above) || (mt.Below != nil && int(v) < *mt.Below) {
							mt.count++
							if mt.count >= mt.Times {
								triggered = true
							}
						}
					}
				}
			}
		}

		if !triggered {
			continue
		}

		for _, action := range trigger.Actions {
			switch {
			case action.Alert != nil:
				alertConsumer, err := r.LogsConsumerMap.FanoutToPipelines(action.Alert.Pipelines)
				if err != nil {
					return err
				}
				alertLog := plog.NewLogs()
				lr := alertLog.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
				lr.Body().SetStr(action.Alert.Message)
				lr.SetTimestamp(lr.ObservedTimestamp())
				if err := alertConsumer.ConsumeLogs(ctx, alertLog); err != nil {
					return err
				}
			case action.SetState != "":
				r.Logger.Info(fmt.Sprintf("changing state to: %s", action.SetState))
				r.currentState = r.cfg.States[action.SetState]
			}
		}
		mt.count = 0 // reset threshold violation count
	}

	for _, route := range r.currentState.MetricsTable {
		if route.Match(md) {
			cons, err := r.MetricsConsumerMap.FanoutToPipelines(route.Pipelines)
			if err != nil {
				return err
			}
			return cons.ConsumeMetrics(ctx, md)
		}
	}
	return nil // No match, just drop
}
