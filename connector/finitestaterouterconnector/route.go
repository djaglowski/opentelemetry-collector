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
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/internal/sharedcomponent"
	"go.uber.org/zap"
)

const (
	typeStr = "finitestaterouter"
)

type Config struct {
	InitialState string           `mapstructure:"initial_state"`
	States       map[string]State `mapstructure:"states"`
}

type State struct {
	MetricsTable []MetricsRoute `mapstructure:"metrics"`
	LogsTable    []LogsRoute    `mapstructure:"logs"`
	Triggers     []Trigger      `mapstructure:"triggers"`
}

type Trigger struct {
	MetricThreshold *MetricThreshold `mapstructure:"metric_threshold"`
	Actions         []Action         `mapstructure:"actions"`
}

type MetricThreshold struct {
	MetricName string `mapstructure:"metric_name"`
	Above      *int   `mapstructure:"above"`
	Below      *int   `mapstructure:"below"`
	Times      int    `mapstructure:"times"`
	count      int
}

type Action struct {
	Alert    *Alert `mapstructure:"alert"`
	SetState string `mapstructure:"set_state"`
}

type Alert struct {
	Message   string
	Pipelines []component.ID
}

func (c *Config) Validate() error {
	if len(c.States) < 2 {
		return fmt.Errorf("must define at least two states")
	}

	if _, ok := c.States[c.InitialState]; !ok {
		return fmt.Errorf("initial_state not found: %s", c.InitialState)
	}

	for stateName, state := range c.States {
		for _, r := range state.MetricsTable {
			if err := r.MetricsCondition.Validate(); err != nil {
				return err
			}
			if len(r.Pipelines) == 0 {
				return fmt.Errorf("condition must have at least one pipeline: %v", r.MetricsCondition)
			}
		}
		for _, r := range state.LogsTable {
			if err := r.LogsCondition.Validate(); err != nil {
				return err
			}
			if len(r.Pipelines) == 0 {
				return fmt.Errorf("condition must have at least one pipeline: %v", r.LogsCondition)
			}
		}

		if len(state.Triggers) == 0 {
			return fmt.Errorf("state must have at least one trigger: %s", stateName)
		}
		if len(state.LogsTable)+len(state.MetricsTable) == 0 {
			return fmt.Errorf("state must have at least one route: %s", stateName)
		}

		for _, trigger := range state.Triggers {
			if trigger.MetricThreshold == nil {
				return fmt.Errorf("trigger must use metric_threshold") // TODO add more types of triggers (timer, etc)
			}
			if trigger.MetricThreshold.Above == nil && trigger.MetricThreshold.Below == nil {
				return fmt.Errorf("metric_threshold must specify above or below")
			}

			if len(trigger.Actions) == 0 {
				return fmt.Errorf("trigger must have at least one associated action")
			}
			for _, action := range trigger.Actions {
				if action.Alert != nil && action.SetState != "" {
					return fmt.Errorf("action may only be one of 'alert' or 'set_state'")
				}
				if action.Alert != nil {
					if action.Alert.Message == "" {
						return fmt.Errorf("alert must include a 'message'")
					}
					if len(action.Alert.Pipelines) == 0 {
						return fmt.Errorf("alert must include at least one pipeline")
					}
				}
				if action.SetState != "" {
					if _, ok := c.States[action.SetState]; !ok {
						return fmt.Errorf("set_state refers to an undefined state: %s", action.SetState)
					}
				}
			}
		}
	}

	return nil
}

type finiteStateRouterFactory struct {
	// This is the map of already created forward connectors for particular configurations.
	// We maintain this map because the Factory is asked trace, metric, and log receivers
	// separately but they must not create separate objects. When the connector is shutdown
	// it should be removed from this map so the same configuration can be recreated successfully.
	*sharedcomponent.SharedComponents
}

// NewFactory returns a connector.Factory.
func NewFactory() connector.Factory {
	f := &finiteStateRouterFactory{sharedcomponent.NewSharedComponents()}
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

// finiteStateRouter is used to pass signals directly from one pipeline to another.
// This is useful when there is a need to replicate data and process it in more
// than one way. It can also be used to join pipelines together.
type finiteStateRouter struct {
	*zap.Logger
	cfg *Config

	*connector.MetricsConsumerMap
	*connector.LogsConsumerMap

	currentState State
	stateMux     sync.Mutex

	component.StartFunc
	component.ShutdownFunc
}

func (r *finiteStateRouter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func createFiniteStateRouter(set connector.CreateSettings, cfg component.Config) func() (component.Component, error) {
	rCfg, ok := cfg.(*Config)
	return func() (component.Component, error) {
		if !ok {
			return nil, fmt.Errorf("not a finitestateroute config")
		}

		initialState, ok := rCfg.States[rCfg.InitialState]
		if !ok {
			return nil, fmt.Errorf("invalid initial state: %s", rCfg.InitialState)
		}

		r := &finiteStateRouter{
			Logger:       set.TelemetrySettings.Logger,
			cfg:          rCfg,
			currentState: initialState,
		}

		r.Logger.Info(fmt.Sprintf("using initial state: %s", rCfg.InitialState))
		return r, nil
	}
}
