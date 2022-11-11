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

package configunmarshaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
)

func TestConnectorsUnmarshal(t *testing.T) {
	factories, err := componenttest.NopFactories()
	require.NoError(t, err)

	conns := NewConnectors(factories.Connectors)
	conf := confmap.NewFromStringMap(map[string]interface{}{
		"nop":             nil,
		"nop/myconnector": nil,
	})
	require.NoError(t, conns.Unmarshal(conf))

	cfgWithName := factories.Connectors["nop"].CreateDefaultConfig()
	cfgWithName.SetIDName("myconnector")
	assert.Equal(t, map[component.ID]component.ConnectorConfig{
		component.NewID("nop"):                        factories.Connectors["nop"].CreateDefaultConfig(),
		component.NewIDWithName("nop", "myconnector"): cfgWithName,
	}, conns.GetConnectors())
}

func TestConnectorsUnmarshalError(t *testing.T) {
	var testCases = []struct {
		name string
		conf *confmap.Conf
		// string that the error must contain
		expectedError string
	}{
		{
			name: "invalid-connector-type",
			conf: confmap.NewFromStringMap(map[string]interface{}{
				"nop":     nil,
				"/custom": nil,
			}),
			expectedError: "the part before / should not be empty",
		},
		{
			name: "invalid-connector-name-after-slash",
			conf: confmap.NewFromStringMap(map[string]interface{}{
				"nop":  nil,
				"nop/": nil,
			}),
			expectedError: "the part after / should not be empty",
		},
		{
			name: "unknown-connector-type",
			conf: confmap.NewFromStringMap(map[string]interface{}{
				"nosuchconnector": nil,
			}),
			expectedError: "unknown connectors type: \"nosuchconnector\"",
		},
		{
			name: "duplicate-connector",
			conf: confmap.NewFromStringMap(map[string]interface{}{
				"nop /exp ": nil,
				" nop/ exp": nil,
			}),
			expectedError: "duplicate name",
		},
		{
			name: "invalid-connector-section",
			conf: confmap.NewFromStringMap(map[string]interface{}{
				"nop": map[string]interface{}{
					"unknown_section": "connector",
				},
			}),
			expectedError: "error reading connectors configuration for \"nop\"",
		},
		{
			name: "invalid-connector-sub-config",
			conf: confmap.NewFromStringMap(map[string]interface{}{
				"nop": "tests",
			}),
			expectedError: "'[nop]' expected a map, got 'string'",
		},
	}

	factories, err := componenttest.NopFactories()
	assert.NoError(t, err)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			exps := NewConnectors(factories.Connectors)
			err = exps.Unmarshal(tt.conf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedError)
		})
	}
}
