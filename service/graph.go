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
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/service/internal/components"
	"go.opentelemetry.io/collector/service/internal/zpages"
)

var _ pipelines = (*pipelinesGraph)(nil)

type pipelinesGraph struct {

	// All component instances represented as nodes, with directed edges indicating data flow.
	componentGraph *simple.DirectedGraph

	// Keep track of how nodes relate to pipelines, so we can declare edges in the graph.
	pipelineGraphs map[component.ID]*pipelineGraph
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

func buildPipelinesGraph(ctx context.Context, set pipelinesSettings) (Pipelines, error) {
	pipelines := &pipelinesGraph{
		componentGraph: simple.NewDirectedGraph(),
		pipelineGraphs: make(map[component.ID]*pipelineGraph, len(set.PipelineConfigs)),
	}
	for pipelineID := range set.PipelineConfigs {
		pipelines.pipelineGraphs[pipelineID] = newPipelineGraph()
	}

	if err := pipelines.createNodes(set); err != nil {
		return nil, err
	}

	pipelines.createEdges()

	if err := pipelines.buildNodes(ctx, set); err != nil {
		return nil, err
	}

	return pipelines, nil
}

// Creates a node for each instance of a component and adds it to the graph
func (g *pipelinesGraph) createNodes(set pipelinesSettings) error {

	// map[connectorID]pipelineIDs
	// Keep track of connectors and where they are used.
	connectorsAsExporter := make(map[component.ID][]component.ID)
	connectorsAsReceiver := make(map[component.ID][]component.ID)

	for pipelineID, pipelineCfg := range set.PipelineConfigs {
		for _, recvID := range pipelineCfg.Receivers {
			if connCfg := set.Connectors.Config(recvID); connCfg != nil {
				connectorsAsReceiver[recvID] = append(connectorsAsReceiver[recvID], pipelineID)
				continue
			}
			if rcvrCfg := set.Receivers.Config(recvID); rcvrCfg == nil {
				return fmt.Errorf("receiver %q is not configured", recvID)
			}
			g.addReceiver(pipelineID, recvID)
		}
		for _, procID := range pipelineCfg.Processors {
			if procCfg := set.Processors.Config(procID); procCfg == nil {
				return fmt.Errorf("processor %q is not configured", procID)
			}
			g.addProcessor(pipelineID, procID)
		}
	}

	// All exporters added after all receivers to ensure deterministic error when a connector is not configured
	for pipelineID, pipelineCfg := range set.PipelineConfigs {
		for _, exprID := range pipelineCfg.Exporters {
			if connCfg := set.Connectors.Config(exprID); connCfg != nil {
				connectorsAsExporter[exprID] = append(connectorsAsExporter[exprID], pipelineID)
				continue
			}
			if exprCfg := set.Exporters.Config(exprID); exprCfg == nil {
				return fmt.Errorf("exporter %q is not configured", exprID)
			}
			g.addExporter(pipelineID, exprID)
		}
	}

	return g.addConnectors(connectorsAsExporter, connectorsAsReceiver)
}

func (g *pipelinesGraph) addReceiver(pipelineID, recvID component.ID) {
	node := components.NewReceiverNode(pipelineID, recvID)
	if rcvrNode := g.componentGraph.Node(node.ID()); rcvrNode != nil {
		g.pipelineGraphs[pipelineID].addReceiver(rcvrNode)
		return
	}
	g.pipelineGraphs[pipelineID].addReceiver(node)
	g.componentGraph.AddNode(node)
}

func (g *pipelinesGraph) addProcessor(pipelineID, procID component.ID) {
	node := components.NewProcessorNode(pipelineID, procID)
	g.pipelineGraphs[pipelineID].addProcessor(node)
	g.componentGraph.AddNode(node)
}

func (g *pipelinesGraph) addExporter(pipelineID, exprID component.ID) {
	node := components.NewExporterNode(pipelineID, exprID)
	if expNode := g.componentGraph.Node(node.ID()); expNode != nil {
		g.pipelineGraphs[pipelineID].addExporter(expNode)
		return
	}
	g.pipelineGraphs[pipelineID].addExporter(node)
	g.componentGraph.AddNode(node)
}

func (g *pipelinesGraph) addConnectors(asExporter, asReceiver map[component.ID][]component.ID) error {
	if len(asExporter) != len(asReceiver) {
		return fmt.Errorf("each connector must be used as both receiver and exporter")
	}
	for connID, exprPipelineIDs := range asExporter {
		rcvrPipelineIDs, ok := asReceiver[connID]
		if !ok {
			return fmt.Errorf("connector %q must be used as receiver, only found as exporter", connID)
		}
		for _, eID := range exprPipelineIDs {
			for _, rID := range rcvrPipelineIDs {
				g.addConnector(eID, rID, connID)
			}
		}
	}
	return nil
}

func (g *pipelinesGraph) addConnector(exprPipelineID, rcvrPipelineID, connID component.ID) {
	node := components.NewConnectorNode(exprPipelineID.Type(), rcvrPipelineID.Type(), connID)
	if connNode := g.componentGraph.Node(node.ID()); connNode != nil {
		g.pipelineGraphs[exprPipelineID].addExporter(connNode)
		g.pipelineGraphs[rcvrPipelineID].addReceiver(connNode)
		return
	}
	g.pipelineGraphs[exprPipelineID].addExporter(node)
	g.pipelineGraphs[rcvrPipelineID].addReceiver(node)
	g.componentGraph.AddNode(node)
}

func (g *pipelinesGraph) createEdges() {
	for pipelineID, pg := range g.pipelineGraphs {
		fanOutToExporters := components.NewFanOutNode(pipelineID)

		for _, exporter := range pg.exporters {
			g.componentGraph.SetEdge(g.componentGraph.NewEdge(fanOutToExporters, exporter))
		}

		if len(pg.processors) == 0 {
			for _, receiver := range pg.receivers {
				g.componentGraph.SetEdge(g.componentGraph.NewEdge(receiver, fanOutToExporters))
			}
			continue
		}

		fanInToProcessors := components.NewFanInNode(pipelineID)

		for _, receiver := range pg.receivers {
			g.componentGraph.SetEdge(g.componentGraph.NewEdge(receiver, fanInToProcessors))
		}

		g.componentGraph.SetEdge(g.componentGraph.NewEdge(fanInToProcessors, pg.processors[0]))

		for i := 0; i+1 < len(pg.processors); i++ {
			g.componentGraph.SetEdge(g.componentGraph.NewEdge(pg.processors[i], pg.processors[i+1]))
		}

		g.componentGraph.SetEdge(g.componentGraph.NewEdge(pg.processors[len(pg.processors)-1], fanOutToExporters))
	}
}

func (g *pipelinesGraph) buildNodes(ctx context.Context, set pipelinesSettings) error {
	nodes, err := topo.Sort(g.componentGraph)
	if err != nil {
		var topoErr topo.Unorderable
		if !errors.As(err, &topoErr) {
			return topoErr
		}

		// It is possible to have multiple cycles, but it is enough to report the first cycle
		cycle := topoErr[0]
		nodeCycle := make([]string, 0, len(cycle)+1)
		for _, node := range cycle {
			switch n := node.(type) {
			case *components.ReceiverNode:
				nodeCycle = append(nodeCycle, fmt.Sprintf("receiver \"%s\"", n.ComponentID()))
			case *components.ProcessorNode:
				nodeCycle = append(nodeCycle, fmt.Sprintf("processor \"%s\"", n.ComponentID()))
			case *components.ExporterNode:
				nodeCycle = append(nodeCycle, fmt.Sprintf("exporter \"%s\"", n.ComponentID()))
			case *components.ConnectorNode:
				nodeCycle = append(nodeCycle, fmt.Sprintf("connector \"%s\"", n.ComponentID()))
			}
		}
		// Prepend the last node to clarify the cycle
		nodeCycle = append([]string{nodeCycle[len(nodeCycle)-1]}, nodeCycle...)
		return fmt.Errorf("cycle detected: %s", strings.Join(nodeCycle, ", "))
	}

	for i := len(nodes) - 1; i >= 0; i-- {
		node := nodes[i]
		switch n := node.(type) {
		case *components.ReceiverNode:
			nexts := g.nextConsumers(n.ID())
			if len(nexts) == 0 {
				return fmt.Errorf("receiver %q has no next consumer: %w", n.ComponentID(), err)
			}
			if err = n.Build(ctx, set.Telemetry, set.BuildInfo, set.Receivers, nexts); err != nil {
				return err
			}
		case *components.ProcessorNode:
			nexts := g.nextConsumers(n.ID())
			if len(nexts) == 0 {
				return fmt.Errorf("processor %q has no next consumer: %w", n.ComponentID(), err)
			}
			if len(nexts) > 1 {
				return fmt.Errorf("processor %q has multiple consumers", n.ComponentID())
			}
			if err = n.Build(ctx, set.Telemetry, set.BuildInfo, set.Processors, nexts[0]); err != nil {
				return err
			}
		case *components.ConnectorNode:
			nexts := g.nextConsumers(n.ID())
			if len(nexts) == 0 {
				return fmt.Errorf("connector %q has no next consumer: %w", n.ComponentID(), err)
			}
			if err = n.Build(ctx, set.Telemetry, set.BuildInfo, set.Connectors, nexts); err != nil {
				return err
			}
		case *components.FanInNode:
			nexts := g.nextConsumers(n.ID())
			if len(nexts) != 1 {
				return fmt.Errorf("fan-in in pipeline %q must have one consumer: %w", n.PipelineID(), err)
			}
			n.Build(nexts[0], g.nextProcessors(n.ID()))
		case *components.FanOutNode:
			nexts := g.nextConsumers(n.ID())
			if len(nexts) == 0 {
				return fmt.Errorf("fan-out in pipeline %q has no next consumer: %w", n.PipelineID(), err)
			}
			if err = n.Build(nexts); err != nil {
				return err
			}
		case *components.ExporterNode:
			if err = n.Build(ctx, set.Telemetry, set.BuildInfo, set.Exporters); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *pipelinesGraph) nextConsumers(nodeID int64) []consumer.Consumer {
	nextNodes := g.componentGraph.From(nodeID)
	nextConsumers := make([]consumer.Consumer, 0, nextNodes.Len())
	for nextNodes.Next() {
		switch next := nextNodes.Node().(type) {
		case *components.ProcessorNode:
			nextConsumers = append(nextConsumers, next.Component.(consumer.Consumer))
		case *components.ExporterNode:
			nextConsumers = append(nextConsumers, next.Component.(consumer.Consumer))
		case *components.ConnectorNode:
			nextConsumers = append(nextConsumers, next.Component.(consumer.Consumer))
		case *components.FanInNode:
			nextConsumers = append(nextConsumers, next.Consumer)
		case *components.FanOutNode:
			nextConsumers = append(nextConsumers, next.Consumer)
		default:
			panic(fmt.Sprintf("type cannot be consumer: %T", next))
		}
	}
	return nextConsumers
}

func (g *pipelinesGraph) nextProcessors(nodeID int64) []*components.ProcessorNode {
	nextProcessors := make([]*components.ProcessorNode, 0)
	for {
		nextNodes := g.componentGraph.From(nodeID)
		if nextNodes.Len() != 1 {
			break
		}
		procNode, ok := nextNodes.Node().(*components.ProcessorNode)
		if !ok {
			break
		}
		nextProcessors = append(nextProcessors, procNode)
	}
	return nextProcessors
}

// A node-based representation of a pipeline configuration.
type pipelineGraph struct {

	// Use maps for receivers and exporters to assist with deduplication of connector instances.
	receivers map[int64]graph.Node
	exporters map[int64]graph.Node

	// The order of processors is very important. Therefore use a slice for processors.
	processors []graph.Node
}

func newPipelineGraph() *pipelineGraph {
	return &pipelineGraph{
		receivers: make(map[int64]graph.Node),
		exporters: make(map[int64]graph.Node),
	}
}

func (p *pipelineGraph) addReceiver(node graph.Node) {
	p.receivers[node.ID()] = node
}
func (p *pipelineGraph) addProcessor(node graph.Node) {
	p.processors = append(p.processors, node)
}
func (p *pipelineGraph) addExporter(node graph.Node) {
	p.exporters[node.ID()] = node
}

func (g *pipelinesGraph) GetExporters() map[component.DataType]map[component.ID]component.Component {
	exportersMap := make(map[component.DataType]map[component.ID]component.Component)
	exportersMap[component.DataTypeTraces] = make(map[component.ID]component.Component)
	exportersMap[component.DataTypeMetrics] = make(map[component.ID]component.Component)
	exportersMap[component.DataTypeLogs] = make(map[component.ID]component.Component)

	for _, pg := range g.pipelineGraphs {
		for _, expNode := range pg.exporters {
			expOrConnNode := g.componentGraph.Node(expNode.ID())
			expNode, ok := expOrConnNode.(*components.ExporterNode)
			if !ok {
				continue
			}
			exportersMap[expNode.PipelineType()][expNode.ComponentID()] = expNode.Component
		}
	}
	return exportersMap
}

func (g *pipelinesGraph) HandleZPages(w http.ResponseWriter, r *http.Request) {
	qValues := r.URL.Query()
	pipelineName := qValues.Get(zPipelineName)
	componentName := qValues.Get(zComponentName)
	componentKind := qValues.Get(zComponentKind)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	zpages.WriteHTMLPageHeader(w, zpages.HeaderData{Title: "Pipelines"})
	zpages.WriteHTMLPipelinesSummaryTable(w, g.getPipelinesSummaryTableData())
	if pipelineName != "" && componentName != "" && componentKind != "" {
		fullName := componentName
		if componentKind == "processor" {
			fullName = pipelineName + "/" + componentName
		}
		zpages.WriteHTMLComponentHeader(w, zpages.ComponentHeaderData{
			Name: componentKind + ": " + fullName,
		})
		// TODO: Add config + status info.
	}
	zpages.WriteHTMLPageFooter(w)
}

func (g *pipelinesGraph) getPipelinesSummaryTableData() zpages.SummaryPipelinesTableData {
	sumData := zpages.SummaryPipelinesTableData{}
	sumData.Rows = make([]zpages.SummaryPipelinesTableRowData, 0, len(g.pipelineGraphs))

	for pipelineID, pipelineGraph := range g.pipelineGraphs {
		row := zpages.SummaryPipelinesTableRowData{
			FullName:   pipelineID.String(),
			InputType:  string(pipelineID.Type()),
			Receivers:  make([]string, len(pipelineGraph.receivers)),
			Processors: make([]string, len(pipelineGraph.processors)),
			Exporters:  make([]string, len(pipelineGraph.exporters)),
		}
		for _, recvNode := range pipelineGraph.receivers {
			switch node := recvNode.(type) {
			case *components.ReceiverNode:
				row.Receivers = append(row.Receivers, node.ComponentID().String())
			case *components.ConnectorNode:
				row.Receivers = append(row.Receivers, node.ComponentID().String()+" (connector)")
			}
		}
		for _, procNode := range pipelineGraph.processors {
			node := procNode.(*components.ProcessorNode)
			row.Processors = append(row.Processors, node.ComponentID().String())
			row.MutatesData = row.MutatesData || node.Component.(consumer.Consumer).Capabilities().MutatesData
		}
		for _, expNode := range pipelineGraph.exporters {
			switch node := expNode.(type) {
			case *components.ExporterNode:
				row.Exporters = append(row.Exporters, node.ComponentID().String())
			case *components.ConnectorNode:
				row.Exporters = append(row.Exporters, node.ComponentID().String()+" (connector)")
			}
		}
		sumData.Rows = append(sumData.Rows, row)
	}

	sort.Slice(sumData.Rows, func(i, j int) bool {
		return sumData.Rows[i].FullName < sumData.Rows[j].FullName
	})
	return sumData
}
