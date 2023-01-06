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
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
)

// LogStabilityLevel logs the stability level of a component. The log level is set to info for
// undefined, unmaintained, deprecated and development. The log level is set to debug
// for alpha, beta and stable.
func LogStabilityLevel(logger *zap.Logger, sl component.StabilityLevel) {
	if sl >= component.StabilityLevelAlpha {
		logger.Debug(sl.LogMessage(), zap.String(ZapStabilityKey, sl.String()))
	} else {
		logger.Info(sl.LogMessage(), zap.String(ZapStabilityKey, sl.String()))
	}
}

func ReceiverLogger(logger *zap.Logger, id component.ID, dt component.DataType) *zap.Logger {
	return logger.With(
		zap.String(ZapKindKey, ZapKindReceiver),
		zap.String(ZapNameKey, id.String()),
		zap.String(ZapKindPipeline, string(dt)))
}

func ProcessorLogger(logger *zap.Logger, procID component.ID, pipelineID component.ID) *zap.Logger {
	return logger.With(
		zap.String(ZapKindKey, ZapKindProcessor),
		zap.String(ZapNameKey, procID.String()),
		zap.String(ZapKindPipeline, pipelineID.String()))
}

func ExporterLogger(logger *zap.Logger, id component.ID, dt component.DataType) *zap.Logger {
	return logger.With(
		zap.String(ZapKindKey, ZapKindExporter),
		zap.String(ZapDataTypeKey, string(dt)),
		zap.String(ZapNameKey, id.String()))
}

func ConnectorLogger(logger *zap.Logger, connID component.ID, expPipelineType, rcvrPipelineType component.DataType) *zap.Logger {
	return logger.With(
		zap.String(ZapKindKey, ZapKindExporter),
		zap.String(ZapNameKey, connID.String()),
		zap.String(ZapRoleExporterInPipeline, string(expPipelineType)),
		zap.String(ZapRoleReceiverInPipeline, string(rcvrPipelineType)))
}
