// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package routeconnector

import (
	"testing"

	"go.opentelemetry.io/collector/component"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	testCases := []struct {
		name string
		cfg  *Config
		err  bool
	}{
		{
			name: "default_config",
			cfg:  NewFactory().CreateDefaultConfig().(*Config),
			err:  true,
		},
		{
			name: "logs/missing_pipelines",
			cfg: &Config{
				LogsTable: []LogsRoute{
					{
						LogsCondition: LogsCondition{MinSeverity: "debug"},
					},
					{
						LogsCondition: LogsCondition{MatchAll: true},
						Pipelines:     []component.ID{component.NewIDWithName("logs", "file")},
					},
				},
			},
			err: true,
		},
		{
			name: "logs/missing_condition",
			cfg: &Config{
				LogsTable: []LogsRoute{
					{
						Pipelines: []component.ID{component.NewID("logs")},
					},
					{
						LogsCondition: LogsCondition{MatchAll: true},
						Pipelines:     []component.ID{component.NewIDWithName("logs", "file")},
					},
				},
			},
			err: true,
		},
		{
			name: "logs/invalid_sev",
			cfg: &Config{
				LogsTable: []LogsRoute{
					{
						LogsCondition: LogsCondition{MinSeverity: "derp"},
						Pipelines:     []component.ID{component.NewID("logs")},
					},
					{
						LogsCondition: LogsCondition{MatchAll: true},
						Pipelines:     []component.ID{component.NewIDWithName("logs", "file")},
					},
				},
			},
			err: true,
		},
		{
			name: "logs/min_sev/one_out",
			cfg: &Config{
				LogsTable: []LogsRoute{
					{
						LogsCondition: LogsCondition{MinSeverity: "debug"},
						Pipelines:     []component.ID{component.NewID("logs")},
					},
					{
						LogsCondition: LogsCondition{MatchAll: true},
						Pipelines:     []component.ID{component.NewIDWithName("logs", "file")},
					},
				},
			},
		},
		{
			name: "logs/min_sev/two_outs",
			cfg: &Config{
				LogsTable: []LogsRoute{
					{
						LogsCondition: LogsCondition{MinSeverity: "debug"},
						Pipelines:     []component.ID{component.NewID("logs"), component.NewIDWithName("logs", "1")},
					},
					{
						LogsCondition: LogsCondition{MatchAll: true},
						Pipelines:     []component.ID{component.NewIDWithName("logs", "file")},
					},
				},
			},
		},
		{
			name: "logs/multiple_condition",
			cfg: &Config{
				LogsTable: []LogsRoute{
					{
						LogsCondition: LogsCondition{MinSeverity: "error"},
						Pipelines:     []component.ID{component.NewIDWithName("logs", "err"), component.NewIDWithName("logs", "all")},
					},
					{
						LogsCondition: LogsCondition{MinSeverity: "debug"},
						Pipelines:     []component.ID{component.NewIDWithName("logs", "all")},
					},
					{
						LogsCondition: LogsCondition{MatchAll: true},
						Pipelines:     []component.ID{component.NewIDWithName("logs", "file")},
					},
				},
			},
		},
		{
			name: "metrics/missing_pipelines",
			cfg: &Config{
				MetricsTable: []MetricsRoute{
					{
						MetricsCondition: MetricsCondition{MinInt: func() *int {
							i := 100
							return &i
						}()},
					},
					{
						MetricsCondition: MetricsCondition{MatchAll: true},
						Pipelines:        []component.ID{component.NewIDWithName("metrics", "file")},
					},
				},
			},
			err: true,
		},
		{
			name: "metrics/missing_condition",
			cfg: &Config{
				MetricsTable: []MetricsRoute{
					{
						Pipelines: []component.ID{component.NewID("metrics")},
					},
					{
						MetricsCondition: MetricsCondition{MatchAll: true},
						Pipelines:        []component.ID{component.NewIDWithName("metrics", "file")},
					},
				},
			},
			err: true,
		},
		{
			name: "metrics/min_max",
			cfg: &Config{
				MetricsTable: []MetricsRoute{
					{
						MetricsCondition: MetricsCondition{MinInt: func() *int {
							i := 70
							return &i
						}()},
						Pipelines: []component.ID{component.NewIDWithName("metrics", "high")},
					},
					{
						MetricsCondition: MetricsCondition{MaxInt: func() *int {
							i := 30
							return &i
						}()},
						Pipelines: []component.ID{component.NewIDWithName("metrics", "low")},
					},
				},
			},
		},
		{
			name: "metrics/multiple_condition",
			cfg: &Config{
				MetricsTable: []MetricsRoute{
					{
						MetricsCondition: MetricsCondition{MinInt: func() *int {
							i := 70
							return &i
						}()},
						Pipelines: []component.ID{component.NewIDWithName("metrics", "high")},
					},
					{
						MetricsCondition: MetricsCondition{MaxInt: func() *int {
							i := 30
							return &i
						}()},
						Pipelines: []component.ID{component.NewIDWithName("metrics", "low")},
					},
					{
						MetricsCondition: MetricsCondition{MatchAll: true},
						Pipelines:        []component.ID{component.NewIDWithName("metrics", "file")},
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
