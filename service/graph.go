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

package service // import "go.opentelemetry.io/collector/service"

import (
	"context"
	"net/http"

	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"

	"go.opentelemetry.io/collector/component"
)

var _ pipelines = (*pipelinesGraph)(nil)

type pipelinesGraph struct {

	// All component instances represented as nodes, with directed edges indicating data flow.
	componentGraph *simple.DirectedGraph
}

func (g *pipelinesGraph) StartAll(ctx context.Context, host component.Host) error {
	nodes, err := topo.Sort(g.componentGraph)
	if err != nil {
		return err
	}

	// Start exporters first, and work towards receivers
	for i := len(nodes) - 1; i >= 0; i-- {
		comp, ok := nodes[i].(component.Component)
		if !ok {
			continue
		}
		if compErr := comp.Start(ctx, host); compErr != nil {
			return compErr
		}
	}
	return nil
}

func (g *pipelinesGraph) ShutdownAll(ctx context.Context) error {
	nodes, err := topo.Sort(g.componentGraph)
	if err != nil {
		return err
	}

	// Stop receivers first, and work towards exporters
	for i := 0; i < len(nodes); i++ {
		comp, ok := nodes[i].(component.Component)
		if !ok {
			continue
		}
		if compErr := comp.Shutdown(ctx); compErr != nil {
			return compErr
		}
	}
	return nil
}

func (g *pipelinesGraph) GetExporters() map[component.DataType]map[component.ID]component.Component {
	// TODO actual implemenation
	return make(map[component.DataType]map[component.ID]component.Component)
}

func (g *pipelinesGraph) HandleZPages(w http.ResponseWriter, r *http.Request) {
	// TODO actual implemenation
}
