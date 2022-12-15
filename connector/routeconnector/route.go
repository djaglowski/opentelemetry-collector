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
	"go.opentelemetry.io/collector/internal/sharedcomponent"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	typeStr = "route"
)

type Config struct {
	Default []component.ID `mapstructure:"default"`
	Table   []Route        `mapstructure:"table"`
}

func (c *Config) Validate() error {
	numRoutes := len(c.Table)
	if c.Default != nil && len(c.Default) > 0 {
		numRoutes++
	}
	if numRoutes < 2 {
		return fmt.Errorf("route connector must have at least two routes")
	}

	for _, r := range c.Table {
		if r.Condition == "" {
			return fmt.Errorf("table route missing condition")
		}
		if len(r.Pipelines) == 0 {
			return fmt.Errorf("condition must have at least one pipeline: %s", r.Condition)
		}
	}

	return nil
}

// TODO how does this work
type Condition string

// TODO placeholder
func (c Condition) MatchLogs(ld plog.Logs) bool {
	return c == "true"
}
func (c Condition) MatchMetrics(md pmetric.Metrics) bool {
	return c == "true"
}

type Route struct {
	Condition `mapstructure:"condition"`
	Pipelines []component.ID `mapstructure:"pipelines"`
}

type routeFactory struct {
	// This is the map of already created forward connectors for particular configurations.
	// We maintain this map because the Factory is asked trace, metric, and log receivers
	// separately but they must not create separate objects. When the connector is shutdown
	// it should be removed from this map so the same configuration can be recreated successfully.
	*sharedcomponent.SharedComponents
}

// NewFactory returns a connector.Factory.
func NewFactory() connector.Factory {
	f := &routeFactory{sharedcomponent.NewSharedComponents()}
	return connector.NewFactory(
		typeStr,
		createDefaultConfig,
		// connector.WithTracesToTraces(f.createTracesToTraces, component.StabilityLevelDevelopment),
		connector.WithMetricsToMetrics(f.createMetrics, component.StabilityLevelDevelopment),
		connector.WithLogsToLogs(f.createLogs, component.StabilityLevelDevelopment),
	)
}

// createDefaultConfig creates the default configuration.
func createDefaultConfig() component.Config {
	return &Config{}
}

// // createTracesToTraces creates a trace receiver based on provided config.
// func (f *routeFactory) createTracesToTraces(
// 	_ context.Context,
// 	set connector.CreateSettings,
// 	cfg component.Config,
// 	nextConsumer consumer.Traces,
// ) (connector.Traces, error) {
// 	comp, _ := f.GetOrAdd(cfg, func() (component.Component, error) {
// 		return &forward{}, nil
// 	})

// 	conn := comp.Unwrap().(*forward)
// 	conn.Traces = nextConsumer
// 	return conn, nil
// }

type metricsConsumerRoute struct {
	Condition
	consumer.Metrics
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
	comp, _ := f.GetOrAdd(cfg, func() (component.Component, error) {
		rCfg, ok := cfg.(*Config)
		if !ok {
			return nil, fmt.Errorf("not a route config")
		}

		r := &router{metricRoutes: []metricsConsumerRoute{}}
		for _, tr := range rCfg.Table {
			cons, err := nextConsumers.FanoutToPipelines(tr.Pipelines)
			if err != nil {
				return nil, err
			}
			r.metricRoutes = append(r.metricRoutes, metricsConsumerRoute{
				Condition: tr.Condition,
				Metrics:   cons,
			})
		}

		if rCfg.Default != nil {
			cons, err := nextConsumers.FanoutToPipelines(rCfg.Default)
			if err != nil {
				return nil, err
			}
			r.metricRoutes = append(r.metricRoutes, metricsConsumerRoute{
				Condition: "true",
				Metrics:   cons,
			})
		}
		return r, nil
	})

	conn := comp.Unwrap().(*router)
	return conn, nil
}

type logsConsumerRoute struct {
	Condition
	consumer.Logs
}

// createLogs creates a log router that may emit to only a
// subset of its downstream consumers. Rather than giving it a fanout
// consumer, we need to give it the means to decide which consumer
// should be called. In some cases, it may emit to more than one
// consumer, in which case we want to make sure they use a fanoutconsumer,
// but we don't want them to have to build it every time.
func (f *routeFactory) createLogs(
	_ context.Context,
	set connector.CreateSettings,
	cfg component.Config,
	nextConsumers *connector.LogsConsumerMap,
) (connector.Logs, error) {
	comp, _ := f.GetOrAdd(cfg, func() (component.Component, error) {
		rCfg, ok := cfg.(*Config)
		if !ok {
			return nil, fmt.Errorf("not a route config")
		}

		r := &router{logRoutes: []logsConsumerRoute{}}
		for _, tr := range rCfg.Table {
			cons, err := nextConsumers.FanoutToPipelines(tr.Pipelines)
			if err != nil {
				return nil, err
			}
			r.logRoutes = append(r.logRoutes, logsConsumerRoute{
				Condition: tr.Condition,
				Logs:      cons,
			})
		}

		if rCfg.Default != nil {
			cons, err := nextConsumers.FanoutToPipelines(rCfg.Default)
			if err != nil {
				return nil, err
			}
			r.logRoutes = append(r.logRoutes, logsConsumerRoute{
				Condition: "true",
				Logs:      cons,
			})
		}
		return r, nil
	})

	conn := comp.Unwrap().(*router)
	return conn, nil
}

// router is used to pass signals directly from one pipeline to another.
// This is useful when there is a need to replicate data and process it in more
// than one way. It can also be used to join pipelines together.
type router struct {
	// consumer.Traces
	metricRoutes []metricsConsumerRoute
	logRoutes    []logsConsumerRoute
	component.StartFunc
	component.ShutdownFunc
	// TODO state tracking here - (requires sharedcomponent)
}

func (r *router) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	for _, route := range r.metricRoutes {
		if route.Condition.MatchMetrics(md) {
			if err := route.ConsumeMetrics(ctx, md); err != nil {
				return err
			}
		}
	}
	// TODO error? for now, drop logs
	return nil
}

func (r *router) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	for _, route := range r.logRoutes {
		if route.Condition.MatchLogs(ld) {
			if err := route.ConsumeLogs(ctx, ld); err != nil {
				return err
			}
		}
	}
	// TODO error? for now, drop logs
	return nil
}

func (r *router) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
