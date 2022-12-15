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

package otelcoltest // import "go.opentelemetry.io/collector/otelcol/otelcoltest"

import (
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/otelcol"
)

// NopFactories returns a otelcol.Factories with all nop factories.
func NopFactories() (otelcol.Factories, error) {
	var factories otelcol.Factories
	var err error
	if factories.Factories, err = componenttest.NopFactories(); err != nil { // nolint:staticcheck
		return otelcol.Factories{}, err
	}
	if factories.Connectors, err = connector.MakeFactoryMap(connectortest.NewNopFactory()); err != nil {
		return otelcol.Factories{}, err
	}
	return factories, nil
}