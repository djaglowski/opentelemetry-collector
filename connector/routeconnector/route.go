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
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/internal/sharedcomponent"
)

const (
	typeStr = "route"
)

type Config struct {
	MetricsTable []MetricsRoute `mapstructure:"metrics"`
	LogsTable    []LogsRoute    `mapstructure:"logs"`
	// TODO default routes
}

func (c *Config) Validate() error {
	numRoutes := len(c.MetricsTable) + len(c.LogsTable)
	if numRoutes < 2 {
		return fmt.Errorf("route connector must have at least two routes")
	}

	for _, r := range c.MetricsTable {
		if err := r.MetricsCondition.Validate(); err != nil {
			return err
		}
		if len(r.Pipelines) == 0 {
			return fmt.Errorf("condition must have at least one pipeline: %v", r.MetricsCondition)
		}
	}
	for _, r := range c.LogsTable {
		if err := r.LogsCondition.Validate(); err != nil {
			return err
		}
		if len(r.Pipelines) == 0 {
			return fmt.Errorf("condition must have at least one pipeline: %v", r.LogsCondition)
		}
	}

	return nil
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

// router is used to pass signals directly from one pipeline to another.
// This is useful when there is a need to replicate data and process it in more
// than one way. It can also be used to join pipelines together.
type router struct {
	// consumer.Traces
	metricsTable []metricsConsumerRoute
	logsTable    []logsConsumerRoute
	component.StartFunc
	component.ShutdownFunc
	// TODO state tracking here - (requires sharedcomponent)
}

func (r *router) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
