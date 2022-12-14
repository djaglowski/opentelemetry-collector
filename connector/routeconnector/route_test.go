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
			name: "only_default_one",
			cfg: &Config{
				Default: []component.ID{component.NewID("logs")},
			},
			err: true,
		},
		{
			name: "only_default_two",
			cfg: &Config{
				Default: []component.ID{component.NewID("logs"), component.NewIDWithName("logs", "1")},
			},
			err: true,
		},
		{
			name: "only_one_route_one",
			cfg: &Config{
				Table: []Route{
					{
						Condition: "true",
						Pipelines: []component.ID{component.NewID("logs")},
					},
				},
			},
			err: true,
		},
		{
			name: "only_one_route_two",
			cfg: &Config{
				Table: []Route{
					{
						Condition: "true",
						Pipelines: []component.ID{component.NewID("logs"), component.NewIDWithName("logs", "1")},
					},
				},
			},
			err: true,
		},
		{
			name: "missing_condition",
			cfg: &Config{
				Table: []Route{
					{
						Condition: "true",
						Pipelines: []component.ID{component.NewID("logs")},
					},
					{
						Pipelines: []component.ID{component.NewIDWithName("logs", "1")},
					},
				},
			},
			err: true,
		},
		{
			name: "two_routes",
			cfg: &Config{
				Table: []Route{
					{
						Condition: "false",
						Pipelines: []component.ID{component.NewID("logs")},
					},
					{
						Condition: "true",
						Pipelines: []component.ID{component.NewIDWithName("logs", "1")},
					},
				},
			},
		},
		{
			name: "default_and_one_route",
			cfg: &Config{
				Table: []Route{
					{
						Condition: "false",
						Pipelines: []component.ID{component.NewID("logs")},
					},
				},
				Default: []component.ID{component.NewID("logs")},
			},
		},
		{
			name: "default_and_two_routes",
			cfg: &Config{
				Table: []Route{
					{
						Condition: "false",
						Pipelines: []component.ID{component.NewIDWithName("logs", "1")},
					},
					{
						Condition: "true",
						Pipelines: []component.ID{component.NewIDWithName("logs", "2")},
					},
				},
				Default: []component.ID{component.NewID("logs")},
			},
		},
		{
			name: "default_multi_and_two_routes",
			cfg: &Config{
				Table: []Route{
					{
						Condition: "false",
						Pipelines: []component.ID{component.NewIDWithName("logs", "1")},
					},
					{
						Condition: "true",
						Pipelines: []component.ID{component.NewIDWithName("logs", "2")},
					},
				},
				Default: []component.ID{component.NewID("logs"), component.NewIDWithName("logs", "3")},
			},
		},
		{
			name: "default_and_multi_routes",
			cfg: &Config{
				Table: []Route{
					{
						Condition: "false",
						Pipelines: []component.ID{component.NewIDWithName("logs", "1"), component.NewIDWithName("logs", "3")},
					},
					{
						Condition: "true",
						Pipelines: []component.ID{component.NewIDWithName("logs", "2")},
					},
				},
				Default: []component.ID{component.NewID("logs")},
			},
		},
		{
			name: "multi_multi",
			cfg: &Config{
				Table: []Route{
					{
						Condition: "false",
						Pipelines: []component.ID{component.NewIDWithName("logs", "1"), component.NewIDWithName("logs", "3")},
					},
					{
						Condition: "also_false",
						Pipelines: []component.ID{component.NewIDWithName("logs", "2"), component.NewIDWithName("logs", "3")},
					},
					{
						Condition: "true",
						Pipelines: []component.ID{component.NewIDWithName("logs", "2")},
					},
				},
				Default: []component.ID{component.NewID("logs"), component.NewIDWithName("logs", "3")},
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
