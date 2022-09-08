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

package pipelines

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/internal/testdata"
	"go.opentelemetry.io/collector/service/internal/testcomponents"
)

// pipelineSpec is designed for easy definition of expected pipeline structure
// It represents the structure of an individual pipeline, including instanced connectors
type pipelineSpec struct {
	id           component.ID
	receiverIDs  map[int64]component.ID
	processorIDs map[int64]component.ID
	exporterIDs  map[int64]component.ID
}

func newPipelineSpec(id component.ID) *pipelineSpec {
	return &pipelineSpec{
		id:           id,
		receiverIDs:  make(map[int64]component.ID),
		processorIDs: make(map[int64]component.ID),
		exporterIDs:  make(map[int64]component.ID),
	}
}

func (ps *pipelineSpec) withExampleReceiver(name string) *pipelineSpec {
	rID := component.NewIDWithName(component.Type("examplereceiver"), name)
	ps.receiverIDs[newReceiverNodeID(ps.id.Type(), rID).ID()] = rID
	return ps
}

func (ps *pipelineSpec) withExampleProcessor(name string) *pipelineSpec {
	pID := component.NewIDWithName(component.Type("exampleprocessor"), name)
	ps.processorIDs[newProcessorNodeID(ps.id, pID).ID()] = pID
	return ps
}

func (ps *pipelineSpec) withExampleExporter(name string) *pipelineSpec {
	eID := component.NewIDWithName(component.Type("exampleexporter"), name)
	ps.exporterIDs[newExporterNodeID(ps.id.Type(), eID).ID()] = eID
	return ps
}

func (ps *pipelineSpec) withExampleConnectorAsReceiver(name string, fromType component.Type) *pipelineSpec {
	cID := component.NewIDWithName(component.Type("exampleconnector"), name)
	ps.receiverIDs[newConnectorNodeID(fromType, ps.id.Type(), cID).ID()] = cID
	return ps
}

func (ps *pipelineSpec) withExampleConnectorAsExporter(name string, toType component.Type) *pipelineSpec {
	cID := component.NewIDWithName(component.Type("exampleconnector"), name)
	ps.exporterIDs[newConnectorNodeID(ps.id.Type(), toType, cID).ID()] = cID
	return ps
}

type pipelineSpecSlice []*pipelineSpec

func (psSlice pipelineSpecSlice) toMap() map[component.ID]*pipelineSpec {
	psMap := make(map[component.ID]*pipelineSpec, len(psSlice))
	for _, ps := range psSlice {
		psMap[ps.id] = ps
	}
	return psMap
}

type statefulComponentPipeline struct {
	receivers  map[int64]testcomponents.StatefulComponent
	processors map[int64]testcomponents.StatefulComponent
	exporters  map[int64]testcomponents.StatefulComponent
}

// Extract the component from each node in the pipeline and make it available as StatefulComponent
func (pg *pipelineGraph) toStatefulComponentPipeline() *statefulComponentPipeline {
	statefulPipeline := &statefulComponentPipeline{
		receivers:  make(map[int64]testcomponents.StatefulComponent),
		processors: make(map[int64]testcomponents.StatefulComponent),
		exporters:  make(map[int64]testcomponents.StatefulComponent),
	}

	for _, r := range pg.receivers {
		switch c := r.(type) {
		case *receiverNode:
			statefulPipeline.receivers[c.ID()] = c.Component.(*testcomponents.ExampleReceiver)
		case *connectorNode:
			// connector needs to be unwrapped to access component as ExampleConnector
			switch ct := c.Component.(type) {
			case connectorConsumerTraces:
				statefulPipeline.receivers[c.ID()] = ct.Component.(*testcomponents.ExampleConnector)
			case connectorConsumerMetrics:
				statefulPipeline.receivers[c.ID()] = ct.Component.(*testcomponents.ExampleConnector)
			case connectorConsumerLogs:
				statefulPipeline.receivers[c.ID()] = ct.Component.(*testcomponents.ExampleConnector)
			}
		}
	}

	for _, p := range pg.processors {
		pn := p.(*processorNode)
		statefulPipeline.processors[pn.ID()] = pn.Component.(*testcomponents.ExampleProcessor)
	}

	for _, e := range pg.exporters {
		switch c := e.(type) {
		case *exporterNode:
			statefulPipeline.exporters[c.ID()] = c.Component.(*testcomponents.ExampleExporter)
		case *connectorNode:
			// connector needs to be unwrapped to access component as ExampleConnector
			switch ct := c.Component.(type) {
			case connectorConsumerTraces:
				statefulPipeline.exporters[c.ID()] = ct.Component.(*testcomponents.ExampleConnector)
			case connectorConsumerMetrics:
				statefulPipeline.exporters[c.ID()] = ct.Component.(*testcomponents.ExampleConnector)
			case connectorConsumerLogs:
				statefulPipeline.exporters[c.ID()] = ct.Component.(*testcomponents.ExampleConnector)
			}
		}
	}

	return statefulPipeline
}

func TestConnectorPipelinesGraph(t *testing.T) {
	// Even though we have a graph of nodes, it is very important that the notion of
	// pipelines is fully respected. Therefore, test expectations are defined on a
	// per-pipeline basis. Validation is also focused on each pipeline, but the net
	// effect is that the entire graph is carefully validated.
	tests := []struct {
		name                string
		pipelines           pipelineSpecSlice
		expectedPerExporter int // requires symmetry in pipelines
	}{
		// Same test cases as old test
		{
			name: "pipelines_simple.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleExporter(""),
			},
			expectedPerExporter: 1,
		},
		{
			name: "pipelines_simple_multi_proc.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "")).
					withExampleReceiver("").
					withExampleProcessor("").withExampleProcessor("1").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "")).
					withExampleReceiver("").
					withExampleProcessor("").withExampleProcessor("1").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "")).
					withExampleReceiver("").
					withExampleProcessor("").withExampleProcessor("1").
					withExampleExporter(""),
			},
			expectedPerExporter: 1,
		},
		{
			name: "pipelines_simple_no_proc.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "")).
					withExampleReceiver("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "")).
					withExampleReceiver("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "")).
					withExampleReceiver("").
					withExampleExporter(""),
			},
			expectedPerExporter: 1,
		},
		{
			name: "pipelines_multi.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "")).
					withExampleReceiver("").withExampleReceiver("1").
					withExampleProcessor("").withExampleProcessor("1").
					withExampleExporter("").withExampleExporter("1"),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "")).
					withExampleReceiver("").withExampleReceiver("1").
					withExampleProcessor("").withExampleProcessor("1").
					withExampleExporter("").withExampleExporter("1"),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "")).
					withExampleReceiver("").withExampleReceiver("1").
					withExampleProcessor("").withExampleProcessor("1").
					withExampleExporter("").withExampleExporter("1"),
			},
			expectedPerExporter: 2,
		},
		{
			name: "pipelines_multi_no_proc.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "")).
					withExampleReceiver("").withExampleReceiver("1").
					withExampleExporter("").withExampleExporter("1"),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "")).
					withExampleReceiver("").withExampleReceiver("1").
					withExampleExporter("").withExampleExporter("1"),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "")).
					withExampleReceiver("").withExampleReceiver("1").
					withExampleExporter("").withExampleExporter("1"),
			},
			expectedPerExporter: 2,
		},
		{
			name: "pipelines_exporter_multi_pipeline.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "1")).
					withExampleReceiver("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "1")).
					withExampleReceiver("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "1")).
					withExampleReceiver("").
					withExampleExporter(""),
			},
			expectedPerExporter: 2,
		},
		// New test cases involving connectors
		{
			name: "pipelines_conn_simple_traces.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "in")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleConnectorAsExporter("", component.DataTypeTraces),
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "out")).
					withExampleConnectorAsReceiver("", component.DataTypeTraces).
					withExampleProcessor("").
					withExampleExporter(""),
			},
			expectedPerExporter: 1,
		},
		{
			name: "pipelines_conn_simple_metrics.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "in")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleConnectorAsExporter("", component.DataTypeMetrics),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "out")).
					withExampleConnectorAsReceiver("", component.DataTypeMetrics).
					withExampleProcessor("").
					withExampleExporter(""),
			},
			expectedPerExporter: 1,
		},
		{
			name: "pipelines_conn_simple_logs.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "in")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleConnectorAsExporter("", component.DataTypeLogs),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "out")).
					withExampleConnectorAsReceiver("", component.DataTypeLogs).
					withExampleProcessor("").
					withExampleExporter(""),
			},
			expectedPerExporter: 1,
		},
		{
			name: "pipelines_conn_fork_merge_traces.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "in")).
					withExampleReceiver("").
					withExampleProcessor("prefork").
					withExampleConnectorAsExporter("fork", component.DataTypeTraces),
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "type0")).
					withExampleConnectorAsReceiver("fork", component.DataTypeTraces).
					withExampleProcessor("type0").
					withExampleConnectorAsExporter("merge", component.DataTypeTraces),
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "type1")).
					withExampleConnectorAsReceiver("fork", component.DataTypeTraces).
					withExampleProcessor("type1").
					withExampleConnectorAsExporter("merge", component.DataTypeTraces),
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "out")).
					withExampleConnectorAsReceiver("merge", component.DataTypeTraces).
					withExampleProcessor("postmerge").
					withExampleExporter(""),
			},
			expectedPerExporter: 2,
		},
		{
			name: "pipelines_conn_fork_merge_metrics.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "in")).
					withExampleReceiver("").
					withExampleProcessor("prefork").
					withExampleConnectorAsExporter("fork", component.DataTypeMetrics),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "type0")).
					withExampleConnectorAsReceiver("fork", component.DataTypeMetrics).
					withExampleProcessor("type0").
					withExampleConnectorAsExporter("merge", component.DataTypeMetrics),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "type1")).
					withExampleConnectorAsReceiver("fork", component.DataTypeMetrics).
					withExampleProcessor("type1").
					withExampleConnectorAsExporter("merge", component.DataTypeMetrics),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "out")).
					withExampleConnectorAsReceiver("merge", component.DataTypeMetrics).
					withExampleProcessor("postmerge").
					withExampleExporter(""),
			},
			expectedPerExporter: 2,
		},
		{
			name: "pipelines_conn_fork_merge_logs.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "in")).
					withExampleReceiver("").
					withExampleProcessor("prefork").
					withExampleConnectorAsExporter("fork", component.DataTypeLogs),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "type0")).
					withExampleConnectorAsReceiver("fork", component.DataTypeLogs).
					withExampleProcessor("type0").
					withExampleConnectorAsExporter("merge", component.DataTypeLogs),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "type1")).
					withExampleConnectorAsReceiver("fork", component.DataTypeLogs).
					withExampleProcessor("type1").
					withExampleConnectorAsExporter("merge", component.DataTypeLogs),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "out")).
					withExampleConnectorAsReceiver("merge", component.DataTypeLogs).
					withExampleProcessor("postmerge").
					withExampleExporter(""),
			},
			expectedPerExporter: 2,
		},
		{
			name: "pipelines_conn_translate_from_traces.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleConnectorAsExporter("", component.DataTypeMetrics).
					withExampleConnectorAsExporter("", component.DataTypeLogs),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "")).
					withExampleConnectorAsReceiver("", component.DataTypeTraces).
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "")).
					withExampleConnectorAsReceiver("", component.DataTypeTraces).
					withExampleProcessor("").
					withExampleExporter(""),
			},
			expectedPerExporter: 1,
		},
		{
			name: "pipelines_conn_translate_from_metrics.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleConnectorAsExporter("", component.DataTypeTraces).
					withExampleConnectorAsExporter("", component.DataTypeLogs),
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "")).
					withExampleConnectorAsReceiver("", component.DataTypeMetrics).
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "")).
					withExampleConnectorAsReceiver("", component.DataTypeMetrics).
					withExampleProcessor("").
					withExampleExporter(""),
			},
			expectedPerExporter: 1,
		},
		{
			name: "pipelines_conn_translate_from_logs.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleConnectorAsExporter("", component.DataTypeTraces).
					withExampleConnectorAsExporter("", component.DataTypeMetrics),
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "")).
					withExampleConnectorAsReceiver("", component.DataTypeLogs).
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "")).
					withExampleConnectorAsReceiver("", component.DataTypeLogs).
					withExampleProcessor("").
					withExampleExporter(""),
			},
			expectedPerExporter: 1,
		},
		{
			name: "pipelines_conn_matrix.yaml",
			pipelines: pipelineSpecSlice{
				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "in")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleConnectorAsExporter("", component.DataTypeTraces).
					withExampleConnectorAsExporter("", component.DataTypeMetrics).
					withExampleConnectorAsExporter("", component.DataTypeLogs),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "in")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleConnectorAsExporter("", component.DataTypeTraces).
					withExampleConnectorAsExporter("", component.DataTypeMetrics).
					withExampleConnectorAsExporter("", component.DataTypeLogs),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "in")).
					withExampleReceiver("").
					withExampleProcessor("").
					withExampleConnectorAsExporter("", component.DataTypeTraces).
					withExampleConnectorAsExporter("", component.DataTypeMetrics).
					withExampleConnectorAsExporter("", component.DataTypeLogs),

				newPipelineSpec(component.NewIDWithName(component.DataTypeTraces, "out")).
					withExampleConnectorAsReceiver("", component.DataTypeTraces).
					withExampleConnectorAsReceiver("", component.DataTypeMetrics).
					withExampleConnectorAsReceiver("", component.DataTypeLogs).
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeMetrics, "out")).
					withExampleConnectorAsReceiver("", component.DataTypeTraces).
					withExampleConnectorAsReceiver("", component.DataTypeMetrics).
					withExampleConnectorAsReceiver("", component.DataTypeLogs).
					withExampleProcessor("").
					withExampleExporter(""),
				newPipelineSpec(component.NewIDWithName(component.DataTypeLogs, "out")).
					withExampleConnectorAsReceiver("", component.DataTypeTraces).
					withExampleConnectorAsReceiver("", component.DataTypeMetrics).
					withExampleConnectorAsReceiver("", component.DataTypeLogs).
					withExampleProcessor("").
					withExampleExporter(""),
			},
			expectedPerExporter: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factories, err := testcomponents.ExampleComponents()
			assert.NoError(t, err)

			cfg := loadConfig(t, filepath.Join("testdata", test.name), factories)

			// Build the pipeline
			pipelinesInterface, err := NewPipelinesGraph(context.Background(), toSettings(factories, cfg))
			require.NoError(t, err)

			allPipelines, ok := pipelinesInterface.(*pipelinesGraph)
			require.True(t, ok)

			assert.Equal(t, len(test.pipelines), len(allPipelines.pipelineGraphs))

			// The entire graph of components is started topologically
			assert.NoError(t, allPipelines.StartAll(context.Background(), componenttest.NewNopHost()))

			// Check each pipeline individually, ensuring that all components are started
			// and that they have observed no signals yet.
			for pipelineID, pipeSpec := range test.pipelines.toMap() {
				pipeline, ok := allPipelines.pipelineGraphs[pipelineID]
				require.True(t, ok, "expected to find pipeline: %s", pipelineID.String())

				require.Equal(t, len(pipeSpec.receiverIDs), len(pipeline.receivers))
				require.Equal(t, len(pipeSpec.processorIDs), len(pipeline.processors))
				require.Equal(t, len(pipeSpec.exporterIDs), len(pipeline.exporters))

				// The pipelineGraph is cumbersome to work with in this context because
				// several type assertions & switches are necessary in order to access the
				// validation functions included on the example components (Started, Stopped, etc)
				// This gets a representation where all components are testcomponent.StatefulComponent
				actualPipeline := pipeline.toStatefulComponentPipeline()

				for expNodeID := range pipeSpec.exporterIDs {
					exp, ok := actualPipeline.exporters[expNodeID]
					require.True(t, ok)
					require.True(t, exp.Started())
					require.Equal(t, 0, len(exp.RecallTraces()))
					require.Equal(t, 0, len(exp.RecallMetrics()))
					require.Equal(t, 0, len(exp.RecallLogs()))
				}

				for procNodeID := range pipeSpec.processorIDs {
					proc, ok := actualPipeline.processors[procNodeID]
					require.True(t, ok)
					require.True(t, proc.Started())
				}

				for rcvrNodeID := range pipeSpec.receiverIDs {
					rcvr, ok := actualPipeline.receivers[rcvrNodeID]
					require.True(t, ok)
					require.True(t, rcvr.Started())
				}
			}

			// Push data into the pipelines. The list of receivers is retrieved directly from the overall
			// component graph because we do not want to duplicate signal inputs to receivers that are
			// shared between pipelines. The `allReceivers` function also excludes connectors, which we do
			// not want to directly inject with signals.
			allReceivers := allPipelines.getReceivers()
			for _, rcvr := range allReceivers[component.DataTypeTraces] {
				tracesReceiver := rcvr.(*testcomponents.ExampleReceiver)
				assert.NoError(t, tracesReceiver.ConsumeTraces(context.Background(), testdata.GenerateTraces(1)))
			}
			for _, rcvr := range allReceivers[component.DataTypeMetrics] {
				metricsReceiver := rcvr.(*testcomponents.ExampleReceiver)
				assert.NoError(t, metricsReceiver.ConsumeMetrics(context.Background(), testdata.GenerateMetrics(1)))
			}
			for _, rcvr := range allReceivers[component.DataTypeLogs] {
				logsReceiver := rcvr.(*testcomponents.ExampleReceiver)
				assert.NoError(t, logsReceiver.ConsumeLogs(context.Background(), testdata.GenerateLogs(1)))
			}

			// Shut down the entire component graph
			assert.NoError(t, allPipelines.ShutdownAll(context.Background()))

			// Check each pipeline individually, ensuring that all components are stopped.
			for pipelineID, pipeSpec := range test.pipelines.toMap() {
				pipeline, ok := allPipelines.pipelineGraphs[pipelineID]
				require.True(t, ok, "expected to find pipeline: %s", pipelineID.String())

				actualPipeline := pipeline.toStatefulComponentPipeline()

				for rcvrNodeID := range pipeSpec.receiverIDs {
					rcvr, ok := actualPipeline.receivers[rcvrNodeID]
					require.True(t, ok)
					require.True(t, rcvr.Stopped())
				}

				for procNodeID := range pipeSpec.processorIDs {
					proc, ok := actualPipeline.processors[procNodeID]
					require.True(t, ok)
					require.True(t, proc.Stopped())
				}

				for expNodeID := range pipeSpec.exporterIDs {
					exp, ok := actualPipeline.exporters[expNodeID]
					require.True(t, ok)
					require.True(t, exp.Stopped())
				}
			}

			// Get the list of exporters directly from the overall component graph. Like receivers,
			// exclude connectors and validate each exporter once regardless of sharing between pipelines.
			allExporters := allPipelines.GetExporters()
			for _, exp := range allExporters[component.DataTypeTraces] {
				tracesExporter := exp.(*testcomponents.ExampleExporter)
				assert.Equal(t, test.expectedPerExporter, len(tracesExporter.RecallTraces()))
				for i := 0; i < test.expectedPerExporter; i++ {
					assert.EqualValues(t, testdata.GenerateTraces(1), tracesExporter.RecallTraces()[0])
				}
			}
			for _, exp := range allExporters[component.DataTypeMetrics] {
				metricsExporter := exp.(*testcomponents.ExampleExporter)
				assert.Equal(t, test.expectedPerExporter, len(metricsExporter.RecallMetrics()))
				for i := 0; i < test.expectedPerExporter; i++ {
					assert.EqualValues(t, testdata.GenerateMetrics(1), metricsExporter.RecallMetrics()[0])
				}
			}
			for _, exp := range allExporters[component.DataTypeLogs] {
				logsExporter := exp.(*testcomponents.ExampleExporter)
				assert.Equal(t, test.expectedPerExporter, len(logsExporter.RecallLogs()))
				for i := 0; i < test.expectedPerExporter; i++ {
					assert.EqualValues(t, testdata.GenerateLogs(1), logsExporter.RecallLogs()[0])
				}
			}
		})
	}
}

// This is a near-copy of the same test against the old struct.
// TODO this is superseded by the test above, but is included initially
// for direct comparison to prior implementation.
func TestNonConnectorPipelinesGraph(t *testing.T) {
	tests := []struct {
		name             string
		receiverIDs      []component.ID
		processorIDs     []component.ID
		exporterIDs      []component.ID
		expectedRequests int
	}{
		{
			name:             "pipelines_simple.yaml",
			receiverIDs:      []component.ID{component.NewID("examplereceiver")},
			processorIDs:     []component.ID{component.NewID("exampleprocessor")},
			exporterIDs:      []component.ID{component.NewID("exampleexporter")},
			expectedRequests: 1,
		},
		{
			name:             "pipelines_simple_multi_proc.yaml",
			receiverIDs:      []component.ID{component.NewID("examplereceiver")},
			processorIDs:     []component.ID{component.NewID("exampleprocessor"), component.NewIDWithName("exampleprocessor", "1")},
			exporterIDs:      []component.ID{component.NewID("exampleexporter")},
			expectedRequests: 1,
		},
		{
			name:             "pipelines_simple_no_proc.yaml",
			receiverIDs:      []component.ID{component.NewID("examplereceiver")},
			exporterIDs:      []component.ID{component.NewID("exampleexporter")},
			expectedRequests: 1,
		},
		{
			name:             "pipelines_multi.yaml",
			receiverIDs:      []component.ID{component.NewID("examplereceiver"), component.NewIDWithName("examplereceiver", "1")},
			processorIDs:     []component.ID{component.NewID("exampleprocessor"), component.NewIDWithName("exampleprocessor", "1")},
			exporterIDs:      []component.ID{component.NewID("exampleexporter"), component.NewIDWithName("exampleexporter", "1")},
			expectedRequests: 2,
		},
		{
			name:             "pipelines_multi_no_proc.yaml",
			receiverIDs:      []component.ID{component.NewID("examplereceiver"), component.NewIDWithName("examplereceiver", "1")},
			exporterIDs:      []component.ID{component.NewID("exampleexporter"), component.NewIDWithName("exampleexporter", "1")},
			expectedRequests: 2,
		},
		{
			name:             "pipelines_exporter_multi_pipeline.yaml",
			receiverIDs:      []component.ID{component.NewID("examplereceiver")},
			exporterIDs:      []component.ID{component.NewID("exampleexporter")},
			expectedRequests: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factories, err := testcomponents.ExampleComponents()
			assert.NoError(t, err)

			cfg := loadConfig(t, filepath.Join("testdata", test.name), factories)

			// Build the pipeline
			pipelinesInterface, err := NewPipelinesGraph(context.Background(), toSettings(factories, cfg))
			assert.NoError(t, err)

			pipelines, ok := pipelinesInterface.(*pipelinesGraph)
			require.True(t, ok)

			assert.NoError(t, pipelines.StartAll(context.Background(), componenttest.NewNopHost()))

			// Verify exporters created, started and empty.
			for _, expID := range test.exporterIDs {
				traceExporter := pipelines.GetExporters()[component.DataTypeTraces][expID].(*testcomponents.ExampleExporter)
				assert.True(t, traceExporter.Started())
				assert.Zero(t, len(traceExporter.RecallTraces()))

				// Validate metrics.
				metricsExporter := pipelines.GetExporters()[component.DataTypeMetrics][expID].(*testcomponents.ExampleExporter)
				assert.True(t, metricsExporter.Started())
				assert.Zero(t, len(metricsExporter.RecallMetrics()))

				// Validate logs.
				logsExporter := pipelines.GetExporters()[component.DataTypeLogs][expID].(*testcomponents.ExampleExporter)
				assert.True(t, logsExporter.Started())
				assert.Zero(t, len(logsExporter.RecallLogs()))
			}

			// Verify processors created in the given order and started.
			for i, procID := range test.processorIDs {
				tracesNode := pipelines.pipelineGraphs[component.NewID(component.DataTypeTraces)].processors[i]
				tracesProcessor := tracesNode.(*processorNode)
				assert.Equal(t, procID, tracesProcessor.componentID)
				assert.True(t, tracesProcessor.Component.(*testcomponents.ExampleProcessor).Started())

				// Validate metrics.
				metricsNode := pipelines.pipelineGraphs[component.NewID(component.DataTypeMetrics)].processors[i]
				metricsProcessor := metricsNode.(*processorNode)
				assert.Equal(t, procID, metricsProcessor.componentID)
				assert.True(t, metricsProcessor.Component.(*testcomponents.ExampleProcessor).Started())

				// Validate logs.
				logsNode := pipelines.pipelineGraphs[component.NewID(component.DataTypeLogs)].processors[i]
				logsProcessor := logsNode.(*processorNode)
				assert.Equal(t, procID, logsProcessor.componentID)
				assert.True(t, logsProcessor.Component.(*testcomponents.ExampleProcessor).Started())
			}

			// Verify receivers created, started and send data to confirm pipelines correctly connected.
			for _, recvID := range test.receiverIDs {
				traceReceiver := pipelines.getReceivers()[component.DataTypeTraces][recvID].(*testcomponents.ExampleReceiver)
				assert.True(t, traceReceiver.Started())
				// Send traces.
				assert.NoError(t, traceReceiver.ConsumeTraces(context.Background(), testdata.GenerateTraces(1)))

				metricsReceiver := pipelines.getReceivers()[component.DataTypeMetrics][recvID].(*testcomponents.ExampleReceiver)
				assert.True(t, metricsReceiver.Started())
				// Send metrics.
				assert.NoError(t, metricsReceiver.ConsumeMetrics(context.Background(), testdata.GenerateMetrics(1)))

				logsReceiver := pipelines.getReceivers()[component.DataTypeLogs][recvID].(*testcomponents.ExampleReceiver)
				assert.True(t, logsReceiver.Started())
				// Send logs.
				assert.NoError(t, logsReceiver.ConsumeLogs(context.Background(), testdata.GenerateLogs(1)))
			}

			assert.NoError(t, pipelines.ShutdownAll(context.Background()))

			// Verify receivers shutdown.
			for _, recvID := range test.receiverIDs {
				traceReceiver := pipelines.getReceivers()[component.DataTypeTraces][recvID].(*testcomponents.ExampleReceiver)
				assert.True(t, traceReceiver.Stopped())

				metricsReceiver := pipelines.getReceivers()[component.DataTypeMetrics][recvID].(*testcomponents.ExampleReceiver)
				assert.True(t, metricsReceiver.Stopped())

				logsReceiver := pipelines.getReceivers()[component.DataTypeLogs][recvID].(*testcomponents.ExampleReceiver)
				assert.True(t, logsReceiver.Stopped())
			}

			// Verify processors shutdown.
			for i := range test.processorIDs {
				traceNode := pipelines.pipelineGraphs[component.NewID(component.DataTypeTraces)].processors[i]
				traceProcessor := traceNode.(*processorNode)
				assert.True(t, traceProcessor.Component.(*testcomponents.ExampleProcessor).Stopped())

				// Validate metrics.
				metricsNode := pipelines.pipelineGraphs[component.NewID(component.DataTypeMetrics)].processors[i]
				metricsProcessor := metricsNode.(*processorNode)
				assert.True(t, metricsProcessor.Component.(*testcomponents.ExampleProcessor).Stopped())

				// Validate logs.
				logsNode := pipelines.pipelineGraphs[component.NewID(component.DataTypeLogs)].processors[i]
				logsProcessor := logsNode.(*processorNode)
				assert.True(t, logsProcessor.Component.(*testcomponents.ExampleProcessor).Stopped())
			}

			// Now verify that exporters received data, and are shutdown.
			for _, expID := range test.exporterIDs {
				// Validate traces.
				traceExporter := pipelines.GetExporters()[component.DataTypeTraces][expID].(*testcomponents.ExampleExporter)
				require.Len(t, traceExporter.RecallTraces(), test.expectedRequests)
				assert.EqualValues(t, testdata.GenerateTraces(1), traceExporter.RecallTraces()[0])
				assert.True(t, traceExporter.Stopped())

				// Validate metrics.
				metricsExporter := pipelines.GetExporters()[component.DataTypeMetrics][expID].(*testcomponents.ExampleExporter)
				require.Len(t, metricsExporter.RecallMetrics(), test.expectedRequests)
				assert.EqualValues(t, testdata.GenerateMetrics(1), metricsExporter.RecallMetrics()[0])
				assert.True(t, metricsExporter.Stopped())

				// Validate logs.
				logsExporter := pipelines.GetExporters()[component.DataTypeLogs][expID].(*testcomponents.ExampleExporter)
				require.Len(t, logsExporter.RecallLogs(), test.expectedRequests)
				assert.EqualValues(t, testdata.GenerateLogs(1), logsExporter.RecallLogs()[0])
				assert.True(t, logsExporter.Stopped())
			}
		})
	}
}

// This includes all tests from the previous implmentation, plus many new ones
// relevant only to the new graph-based implementation.
func TestGraphBuildErrors(t *testing.T) {
	nopReceiverFactory := componenttest.NewNopReceiverFactory()
	nopProcessorFactory := componenttest.NewNopProcessorFactory()
	nopExporterFactory := componenttest.NewNopExporterFactory()
	nopConnectorFactory := componenttest.NewNopConnectorFactory()
	badReceiverFactory := newBadReceiverFactory()
	badProcessorFactory := newBadProcessorFactory()
	badExporterFactory := newBadExporterFactory()
	badConnectorFactory := newBadConnectorFactory()

	tests := []struct {
		configFile string
	}{
		{configFile: "not_supported_exporter_logs.yaml"},
		{configFile: "not_supported_exporter_metrics.yaml"},
		{configFile: "not_supported_exporter_traces.yaml"},
		{configFile: "not_supported_processor_logs.yaml"},
		{configFile: "not_supported_processor_metrics.yaml"},
		{configFile: "not_supported_processor_traces.yaml"},
		{configFile: "not_supported_receiver_traces.yaml"},
		{configFile: "not_supported_receiver_metrics.yaml"},
		{configFile: "not_supported_receiver_traces.yaml"},
		{configFile: "not_supported_connector_traces_traces.yaml"},
		{configFile: "not_supported_connector_traces_metrics.yaml"},
		{configFile: "not_supported_connector_traces_logs.yaml"},
		{configFile: "not_supported_connector_metrics_traces.yaml"},
		{configFile: "not_supported_connector_metrics_metrics.yaml"},
		{configFile: "not_supported_connector_metrics_logs.yaml"},
		{configFile: "not_supported_connector_logs_traces.yaml"},
		{configFile: "not_supported_connector_logs_metrics.yaml"},
		{configFile: "not_supported_connector_logs_logs.yaml"},
		{configFile: "not_allowed_conn_omit_recv_traces.yaml"},
		{configFile: "not_allowed_conn_omit_recv_metrics.yaml"},
		{configFile: "not_allowed_conn_omit_recv_logs.yaml"},
		{configFile: "not_allowed_conn_omit_exp_traces.yaml"},
		{configFile: "not_allowed_conn_omit_exp_metrics.yaml"},
		{configFile: "not_allowed_conn_omit_exp_logs.yaml"},
		{configFile: "not_allowed_simple_cycle_traces.yaml"},
		{configFile: "not_allowed_simple_cycle_metrics.yaml"},
		{configFile: "not_allowed_simple_cycle_logs.yaml"},
		{configFile: "not_allowed_deep_cycle_traces.yaml"},
		{configFile: "not_allowed_deep_cycle_metrics.yaml"},
		{configFile: "not_allowed_deep_cycle_logs.yaml"},
		{configFile: "not_allowed_deep_cycle_multi_signal.yaml"},
		{configFile: "unknown_exporter_config.yaml"},
		{configFile: "unknown_exporter_factory.yaml"},
		{configFile: "unknown_processor_config.yaml"},
		{configFile: "unknown_processor_factory.yaml"},
		{configFile: "unknown_receiver_config.yaml"},
		{configFile: "unknown_receiver_factory.yaml"},
		{configFile: "unknown_connector_config.yaml"},
		{configFile: "unknown_connector_factory.yaml"},
	}

	for _, test := range tests {
		t.Run(test.configFile, func(t *testing.T) {
			factories := component.Factories{
				Receivers: map[component.Type]component.ReceiverFactory{
					nopReceiverFactory.Type(): nopReceiverFactory,
					"unknown":                 nopReceiverFactory,
					badReceiverFactory.Type(): badReceiverFactory,
				},
				Processors: map[component.Type]component.ProcessorFactory{
					nopProcessorFactory.Type(): nopProcessorFactory,
					"unknown":                  nopProcessorFactory,
					badProcessorFactory.Type(): badProcessorFactory,
				},
				Exporters: map[component.Type]component.ExporterFactory{
					nopExporterFactory.Type(): nopExporterFactory,
					"unknown":                 nopExporterFactory,
					badExporterFactory.Type(): badExporterFactory,
				},
				Connectors: map[component.Type]component.ConnectorFactory{
					nopConnectorFactory.Type(): nopConnectorFactory,
					"unknown":                  nopConnectorFactory,
					badConnectorFactory.Type(): badConnectorFactory,
				},
			}

			// Need the unknown factories to do unmarshalling.
			cfg := loadConfig(t, filepath.Join("testdata", test.configFile), factories)

			// Remove the unknown factories, so they are NOT available during building.
			delete(factories.Exporters, "unknown")
			delete(factories.Processors, "unknown")
			delete(factories.Receivers, "unknown")
			delete(factories.Connectors, "unknown")

			_, err := NewPipelinesGraph(context.Background(), toSettings(factories, cfg))
			assert.Error(t, err)
		})
	}
}

// This includes all tests from the previous implmentation, plus a new one
// relevant only to the new graph-based implementation.
func TestGraphFailToStartAndShutdown(t *testing.T) {
	errReceiverFactory := newErrReceiverFactory()
	errProcessorFactory := newErrProcessorFactory()
	errExporterFactory := newErrExporterFactory()
	errConnectorFactory := newErrConnectorFactory()
	nopReceiverFactory := componenttest.NewNopReceiverFactory()
	nopProcessorFactory := componenttest.NewNopProcessorFactory()
	nopExporterFactory := componenttest.NewNopExporterFactory()
	nopConnectorFactory := componenttest.NewNopConnectorFactory()

	set := Settings{
		Telemetry: componenttest.NewNopTelemetrySettings(),
		BuildInfo: component.NewDefaultBuildInfo(),
		ReceiverFactories: map[component.Type]component.ReceiverFactory{
			nopReceiverFactory.Type(): nopReceiverFactory,
			errReceiverFactory.Type(): errReceiverFactory,
		},
		ReceiverConfigs: map[component.ID]component.Config{
			component.NewID(nopReceiverFactory.Type()): nopReceiverFactory.CreateDefaultConfig(),
			component.NewID(errReceiverFactory.Type()): errReceiverFactory.CreateDefaultConfig(),
		},
		ProcessorFactories: map[component.Type]component.ProcessorFactory{
			nopProcessorFactory.Type(): nopProcessorFactory,
			errProcessorFactory.Type(): errProcessorFactory,
		},
		ProcessorConfigs: map[component.ID]component.Config{
			component.NewID(nopProcessorFactory.Type()): nopProcessorFactory.CreateDefaultConfig(),
			component.NewID(errProcessorFactory.Type()): errProcessorFactory.CreateDefaultConfig(),
		},
		ExporterFactories: map[component.Type]component.ExporterFactory{
			nopExporterFactory.Type(): nopExporterFactory,
			errExporterFactory.Type(): errExporterFactory,
		},
		ExporterConfigs: map[component.ID]component.Config{
			component.NewID(nopExporterFactory.Type()): nopExporterFactory.CreateDefaultConfig(),
			component.NewID(errExporterFactory.Type()): errExporterFactory.CreateDefaultConfig(),
		},
		ConnectorFactories: map[component.Type]component.ConnectorFactory{
			nopConnectorFactory.Type(): nopConnectorFactory,
			errConnectorFactory.Type(): errConnectorFactory,
		},
		ConnectorConfigs: map[component.ID]component.Config{
			component.NewIDWithName(nopConnectorFactory.Type(), "conn"): nopConnectorFactory.CreateDefaultConfig(),
			component.NewIDWithName(errConnectorFactory.Type(), "conn"): errConnectorFactory.CreateDefaultConfig(),
		},
	}

	dataTypes := []component.DataType{component.DataTypeTraces, component.DataTypeMetrics, component.DataTypeLogs}
	for _, dt := range dataTypes {
		t.Run(string(dt)+"/receiver", func(t *testing.T) {
			set.PipelineConfigs = map[component.ID]*config.Pipeline{
				component.NewID(dt): {
					Receivers:  []component.ID{component.NewID("nop"), component.NewID("err")},
					Processors: []component.ID{component.NewID("nop")},
					Exporters:  []component.ID{component.NewID("nop")},
				},
			}
			pipelines, err := NewPipelinesGraph(context.Background(), set)
			assert.NoError(t, err)
			assert.Error(t, pipelines.StartAll(context.Background(), componenttest.NewNopHost()))
			assert.Error(t, pipelines.ShutdownAll(context.Background()))
		})

		t.Run(string(dt)+"/processor", func(t *testing.T) {
			set.PipelineConfigs = map[component.ID]*config.Pipeline{
				component.NewID(dt): {
					Receivers:  []component.ID{component.NewID("nop")},
					Processors: []component.ID{component.NewID("nop"), component.NewID("err")},
					Exporters:  []component.ID{component.NewID("nop")},
				},
			}
			pipelines, err := NewPipelinesGraph(context.Background(), set)
			assert.NoError(t, err)
			assert.Error(t, pipelines.StartAll(context.Background(), componenttest.NewNopHost()))
			assert.Error(t, pipelines.ShutdownAll(context.Background()))
		})

		t.Run(string(dt)+"/exporter", func(t *testing.T) {
			set.PipelineConfigs = map[component.ID]*config.Pipeline{
				component.NewID(dt): {
					Receivers:  []component.ID{component.NewID("nop")},
					Processors: []component.ID{component.NewID("nop")},
					Exporters:  []component.ID{component.NewID("nop"), component.NewID("err")},
				},
			}
			pipelines, err := NewPipelinesGraph(context.Background(), set)
			assert.NoError(t, err)
			assert.Error(t, pipelines.StartAll(context.Background(), componenttest.NewNopHost()))
			assert.Error(t, pipelines.ShutdownAll(context.Background()))
		})

		for _, dt2 := range dataTypes {
			t.Run(string(dt)+"/"+string(dt2)+"/connector", func(t *testing.T) {
				set.PipelineConfigs = map[component.ID]*config.Pipeline{
					component.NewIDWithName(dt, "in"): {
						Receivers:  []component.ID{component.NewID("nop")},
						Processors: []component.ID{component.NewID("nop")},
						Exporters:  []component.ID{component.NewID("nop"), component.NewIDWithName("err", "conn")},
					},
					component.NewIDWithName(dt2, "out"): {
						Receivers:  []component.ID{component.NewID("nop"), component.NewIDWithName("err", "conn")},
						Processors: []component.ID{component.NewID("nop")},
						Exporters:  []component.ID{component.NewID("nop")},
					},
				}
				pipelines, err := NewPipelinesGraph(context.Background(), set)
				assert.NoError(t, err)
				assert.Error(t, pipelines.StartAll(context.Background(), componenttest.NewNopHost()))
				assert.Error(t, pipelines.ShutdownAll(context.Background()))
			})
		}
	}
}

func (g *pipelinesGraph) getReceivers() map[component.DataType]map[component.ID]component.Component {
	receiversMap := make(map[component.DataType]map[component.ID]component.Component)
	receiversMap[component.DataTypeTraces] = make(map[component.ID]component.Component)
	receiversMap[component.DataTypeMetrics] = make(map[component.ID]component.Component)
	receiversMap[component.DataTypeLogs] = make(map[component.ID]component.Component)

	for _, pg := range g.pipelineGraphs {
		for _, rcvrNode := range pg.receivers {
			rcvrOrConnNode := g.componentGraph.Node(rcvrNode.ID())
			rcvrNode, ok := rcvrOrConnNode.(*receiverNode)
			if !ok {
				continue
			}
			receiversMap[rcvrNode.pipelineType][rcvrNode.componentID] = rcvrNode.Component
		}
	}
	return receiversMap
}
