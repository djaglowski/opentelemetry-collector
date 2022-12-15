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
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
)

type LogsRoute struct {
	LogsCondition `mapstructure:",squash"`
	Pipelines     []component.ID `mapstructure:"pipelines"`
}

type logsConsumerRoute struct {
	LogsCondition
	consumer.Logs
}

// TODO this is a placeholder for a real grammar
type LogsCondition struct {
	MatchAll    bool   `mapstructure:"match_all"`
	MinSeverity string `mapstructure:"min_severity"`
}

func (c LogsCondition) Validate() error {
	if c.MatchAll {
		return nil
	}
	_, ok := sevMap[strings.ToLower(c.MinSeverity)]
	if !ok {
		return fmt.Errorf("must specify min_severity")
	}
	return nil
}

func (c LogsCondition) Match(ld plog.Logs) bool {
	if c.MatchAll {
		return true
	}

	minSev := sevMap[strings.ToLower(c.MinSeverity)]
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				if lrs.At(k).SeverityNumber() >= minSev {
					return true
				}
			}
		}
	}

	return false
}

var sevMap = func() map[string]plog.SeverityNumber {
	sm := make(map[string]plog.SeverityNumber)
	for i := plog.SeverityNumberTrace; i <= plog.SeverityNumberFatal4; i++ {
		sm[strings.ToLower(i.String())] = i
	}
	return sm
}()

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
	comp, err := f.GetOrAdd(cfg, func() (component.Component, error) {
		rCfg, ok := cfg.(*Config)
		if !ok {
			return nil, fmt.Errorf("not a route config")
		}

		r := &router{logsTable: []logsConsumerRoute{}}
		for _, tr := range rCfg.LogsTable {
			cons, err := nextConsumers.FanoutToPipelines(tr.Pipelines)
			if err != nil {
				return nil, err
			}
			r.logsTable = append(r.logsTable, logsConsumerRoute{
				LogsCondition: tr.LogsCondition,
				Logs:          cons,
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

func (r *router) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	for _, route := range r.logsTable {
		if route.LogsCondition.Match(ld) {
			return route.ConsumeLogs(ctx, ld)
		}
	}
	return nil // No match, just drop
}
