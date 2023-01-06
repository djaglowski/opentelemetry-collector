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

package components // import "go.opentelemetry.io/collector/service/internal/components"

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"

	"gonum.org/v1/gonum/graph"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service/internal/fanoutconsumer"
)

type nodeID int64

func (n nodeID) ID() int64 {
	return int64(n)
}

func newNodeID(parts ...string) nodeID {
	h := fnv.New64a()
	h.Write([]byte(strings.Join(parts, "|")))
	return nodeID(int64(h.Sum64()))
}

// ComponentNode represents a component instance within a directed graph.
type ComponentNode interface {
	ComponentID() component.ID
	component.Component
	graph.Node
}

var _ ComponentNode = &ReceiverNode{}

// ReceiverNode represents a receiver instance which can be shared by multiple pipelines of the same type.
// Therefore, nodeID is derived from "pipeline type" and "component ID".
type ReceiverNode struct {
	nodeID
	componentID  component.ID
	pipelineType component.DataType
	component.Component
}

func NewReceiverNode(pipelineID component.ID, recvID component.ID) *ReceiverNode {
	return &ReceiverNode{
		nodeID:       newNodeID("receiver", string(pipelineID.Type()), recvID.String()),
		componentID:  recvID,
		pipelineType: pipelineID.Type(),
	}
}

func (n *ReceiverNode) ComponentID() component.ID {
	return n.componentID
}

func (n *ReceiverNode) PipelineType() component.DataType {
	return n.pipelineType
}

func (n *ReceiverNode) Build(
	ctx context.Context,
	tel component.TelemetrySettings,
	info component.BuildInfo,
	builder *receiver.Builder,
	nexts []consumer.Consumer,
) error {
	set := receiver.CreateSettings{ID: n.ComponentID(), TelemetrySettings: tel, BuildInfo: info}
	set.TelemetrySettings.Logger = ReceiverLogger(set.TelemetrySettings.Logger, n.componentID, n.pipelineType)

	var err error
	switch n.pipelineType {
	case component.DataTypeTraces:
		var consumers []consumer.Traces
		for _, next := range nexts {
			tracesConsumer, ok := next.(consumer.Traces)
			if !ok {
				return fmt.Errorf("next component is not a traces consumer: %s", n.componentID)
			}
			consumers = append(consumers, tracesConsumer)
		}
		n.Component, err = builder.CreateTraces(ctx, set, fanoutconsumer.NewTraces(consumers))
	case component.DataTypeMetrics:
		var consumers []consumer.Metrics
		for _, next := range nexts {
			metricsConsumer, ok := next.(consumer.Metrics)
			if !ok {
				return fmt.Errorf("next component is not a metrics consumer: %s", n.componentID)
			}
			consumers = append(consumers, metricsConsumer)
		}
		n.Component, err = builder.CreateMetrics(ctx, set, fanoutconsumer.NewMetrics(consumers))
	case component.DataTypeLogs:
		var consumers []consumer.Logs
		for _, next := range nexts {
			logsConsumer, ok := next.(consumer.Logs)
			if !ok {
				return fmt.Errorf("next component is not a logs consumer: %s", n.componentID)
			}
			consumers = append(consumers, logsConsumer)
		}
		n.Component, err = builder.CreateLogs(ctx, set, fanoutconsumer.NewLogs(consumers))
	default:
		return fmt.Errorf("error creating receiver %q, data type %q is not supported", n.componentID, n.pipelineType)
	}
	return err
}

var _ ComponentNode = &ProcessorNode{}

// ProcessorNode represents a processor instance unique to one pipeline.
// Therefore, nodeID is derived from "pipeline ID" and "component ID".
type ProcessorNode struct {
	nodeID
	componentID component.ID
	pipelineID  component.ID
	component.Component
}

func NewProcessorNode(pipelineID, procID component.ID) *ProcessorNode {
	return &ProcessorNode{
		nodeID:      newNodeID("processor", pipelineID.String(), procID.String()),
		componentID: procID,
		pipelineID:  pipelineID,
	}
}

func (n *ProcessorNode) ComponentID() component.ID {
	return n.componentID
}

func (n *ProcessorNode) Build(
	ctx context.Context,
	tel component.TelemetrySettings,
	info component.BuildInfo,
	builder *processor.Builder,
	next consumer.Consumer,
) error {
	set := processor.CreateSettings{ID: n.ComponentID(), TelemetrySettings: tel, BuildInfo: info}
	set.TelemetrySettings.Logger = ProcessorLogger(set.TelemetrySettings.Logger, n.componentID, n.pipelineID)

	var err error
	switch n.pipelineID.Type() {
	case component.DataTypeTraces:
		tracesConsumer, ok := next.(consumer.Traces)
		if !ok {
			return fmt.Errorf("next component is not a traces consumer: %s", n.componentID)
		}
		n.Component, err = builder.CreateTraces(ctx, set, tracesConsumer)
	case component.DataTypeMetrics:
		metricsConsumer, ok := next.(consumer.Metrics)
		if !ok {
			return fmt.Errorf("next component is not a metrics consumer: %s", n.componentID)
		}
		n.Component, err = builder.CreateMetrics(ctx, set, metricsConsumer)
	case component.DataTypeLogs:
		logsConsumer, ok := next.(consumer.Logs)
		if !ok {
			return fmt.Errorf("next component is not a logs consumer: %s", n.componentID)
		}
		n.Component, err = builder.CreateLogs(ctx, set, logsConsumer)
	default:
		return fmt.Errorf("error creating processor %q, data type %q is not supported", n.componentID, n.pipelineID.Type())
	}
	return err
}

var _ ComponentNode = &ExporterNode{}

// ExporterNode represents an exporter instance which can be shared by multiple pipelines of the same type.
// Therefore, nodeID is derived from "pipeline type" and "component ID".
type ExporterNode struct {
	nodeID
	componentID  component.ID
	pipelineType component.DataType
	component.Component
}

func NewExporterNode(pipelineID component.ID, exprID component.ID) *ExporterNode {
	return &ExporterNode{
		nodeID:       newNodeID("exporter", string(pipelineID.Type()), exprID.String()),
		componentID:  exprID,
		pipelineType: pipelineID.Type(),
	}
}

func (n *ExporterNode) ComponentID() component.ID {
	return n.componentID
}

func (n *ExporterNode) PipelineType() component.DataType {
	return n.pipelineType
}

func (n *ExporterNode) Build(
	ctx context.Context,
	tel component.TelemetrySettings,
	info component.BuildInfo,
	builder *exporter.Builder,
) error {
	set := exporter.CreateSettings{ID: n.ComponentID(), TelemetrySettings: tel, BuildInfo: info}
	set.TelemetrySettings.Logger = ExporterLogger(set.TelemetrySettings.Logger, n.componentID, n.pipelineType)

	var err error
	switch n.pipelineType {
	case component.DataTypeTraces:
		n.Component, err = builder.CreateTraces(ctx, set)
	case component.DataTypeMetrics:
		n.Component, err = builder.CreateMetrics(ctx, set)
	case component.DataTypeLogs:
		n.Component, err = builder.CreateLogs(ctx, set)
	default:
		return fmt.Errorf("error creating exporter %q, data type %q is not supported", n.componentID, n.pipelineType)
	}
	return err
}

var _ ComponentNode = &ConnectorNode{}

// ConnectorNode represents a connector instance which connects one pipeline type to one other pipeline type.
// Therefore, nodeID is derived from "exporter pipeline type", "receiver pipeline type", and "component ID".
type ConnectorNode struct {
	nodeID
	componentID      component.ID
	exprPipelineType component.DataType
	rcvrPipelineType component.DataType
	component.Component
}

func NewConnectorNode(exprPipelineType, rcvrPipelineType component.DataType, connID component.ID) *ConnectorNode {
	return &ConnectorNode{
		nodeID:           newNodeID("connector", connID.String(), string(exprPipelineType), string(rcvrPipelineType)),
		componentID:      connID,
		exprPipelineType: exprPipelineType,
		rcvrPipelineType: rcvrPipelineType,
	}
}

func (n *ConnectorNode) ComponentID() component.ID {
	return n.componentID
}

func (n *ConnectorNode) Build(
	ctx context.Context,
	tel component.TelemetrySettings,
	info component.BuildInfo,
	builder *connector.Builder,
	nexts []consumer.Consumer,
) error {
	set := connector.CreateSettings{ID: n.ComponentID(), TelemetrySettings: tel, BuildInfo: info}
	set.TelemetrySettings.Logger = ConnectorLogger(set.TelemetrySettings.Logger, n.componentID, n.exprPipelineType, n.rcvrPipelineType)

	var err error
	switch n.rcvrPipelineType {
	case component.DataTypeTraces:
		var consumers []consumer.Traces
		for _, next := range nexts {
			tracesConsumer, ok := next.(consumer.Traces)
			if !ok {
				return fmt.Errorf("next component is not a traces consumer: %s", n.componentID)
			}
			consumers = append(consumers, tracesConsumer)
		}
		fanoutConsumer := fanoutconsumer.NewTraces(consumers)
		switch n.exprPipelineType {
		case component.DataTypeTraces:
			n.Component, err = builder.CreateTracesToTraces(ctx, set, fanoutConsumer)
		case component.DataTypeMetrics:
			n.Component, err = builder.CreateMetricsToTraces(ctx, set, fanoutConsumer)
		case component.DataTypeLogs:
			n.Component, err = builder.CreateLogsToTraces(ctx, set, fanoutConsumer)
		}
	case component.DataTypeMetrics:
		var consumers []consumer.Metrics
		for _, next := range nexts {
			metricsConsumer, ok := next.(consumer.Metrics)
			if !ok {
				return fmt.Errorf("next component is not a metrics consumer: %s", n.componentID)
			}
			consumers = append(consumers, metricsConsumer)
		}
		fanoutConsumer := fanoutconsumer.NewMetrics(consumers)
		switch n.exprPipelineType {
		case component.DataTypeTraces:
			n.Component, err = builder.CreateTracesToMetrics(ctx, set, fanoutConsumer)
		case component.DataTypeMetrics:
			n.Component, err = builder.CreateMetricsToMetrics(ctx, set, fanoutConsumer)
		case component.DataTypeLogs:
			n.Component, err = builder.CreateLogsToMetrics(ctx, set, fanoutConsumer)
		}
	case component.DataTypeLogs:
		var consumers []consumer.Logs
		for _, next := range nexts {
			logsConsumer, ok := next.(consumer.Logs)
			if !ok {
				return fmt.Errorf("next component is not a logs consumer: %s", n.componentID)
			}
			consumers = append(consumers, logsConsumer)
		}
		fanoutConsumer := fanoutconsumer.NewLogs(consumers)
		switch n.exprPipelineType {
		case component.DataTypeTraces:
			n.Component, err = builder.CreateTracesToLogs(ctx, set, fanoutConsumer)
		case component.DataTypeMetrics:
			n.Component, err = builder.CreateMetricsToLogs(ctx, set, fanoutConsumer)
		case component.DataTypeLogs:
			n.Component, err = builder.CreateLogsToLogs(ctx, set, fanoutConsumer)
		}
	}
	return err
}

// If a pipeline has any processors, a fan-in is added before the first one.
// The main purpose of this node is to present aggregated capabilities to receivers,
// such as whether the pipeline mutates data.
// The nodeID is derived from "pipeline ID".
type FanInNode struct {
	nodeID
	pipelineID component.ID
	consumer.Consumer
	consumer.Capabilities
}

func NewFanInNode(pipelineID component.ID) *FanInNode {
	return &FanInNode{
		nodeID:       newNodeID("fanin_to_processors", pipelineID.String()),
		pipelineID:   pipelineID,
		Capabilities: consumer.Capabilities{},
	}
}

func (n *FanInNode) PipelineID() component.ID {
	return n.pipelineID
}

func (n *FanInNode) Build(nextConsumer consumer.Consumer, processors []*ProcessorNode) {
	n.Consumer = nextConsumer
	for _, proc := range processors {
		n.Capabilities.MutatesData = n.Capabilities.MutatesData ||
			proc.Component.(consumer.Consumer).Capabilities().MutatesData
	}
}

// Each pipeline has one fan-out node before exporters.
// Therefore, nodeID is derived from "pipeline ID".
type FanOutNode struct {
	nodeID
	pipelineID component.ID
	consumer.Consumer
}

func NewFanOutNode(pipelineID component.ID) *FanOutNode {
	return &FanOutNode{
		nodeID:     newNodeID("fanout_to_exporters", pipelineID.String()),
		pipelineID: pipelineID,
	}
}

func (n *FanOutNode) PipelineID() component.ID {
	return n.pipelineID
}

func (n *FanOutNode) Build(nextConsumers []consumer.Consumer) error {
	switch n.pipelineID.Type() {
	case component.DataTypeTraces:
		consumers := make([]consumer.Traces, 0, len(nextConsumers))
		for _, next := range nextConsumers {
			consumers = append(consumers, next.(consumer.Traces))
		}
		n.Consumer = fanoutconsumer.NewTraces(consumers)
	case component.DataTypeMetrics:
		consumers := make([]consumer.Metrics, 0, len(nextConsumers))
		for _, next := range nextConsumers {

			consumers = append(consumers, next.(consumer.Metrics))
		}
		n.Consumer = fanoutconsumer.NewMetrics(consumers)
	case component.DataTypeLogs:
		consumers := make([]consumer.Logs, 0, len(nextConsumers))
		for _, next := range nextConsumers {
			consumers = append(consumers, next.(consumer.Logs))
		}
		n.Consumer = fanoutconsumer.NewLogs(consumers)
	default:
		return fmt.Errorf("create fan-out exporter in pipeline %q, data type %q is not supported", n.pipelineID, n.pipelineID.Type())
	}
	return nil
}
