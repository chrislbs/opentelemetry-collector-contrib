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

package logstransformprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstransformprocessor"

import (
	"context"
	"errors"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"go.opentelemetry.io/collector/extension/experimental/storage"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/adapter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/pipeline"
)

type outputType struct {
	logs plog.Logs
	err  error
}

type logsTransformProcessor struct {
	logger *zap.Logger
	config *Config
}

func (ltp *logsTransformProcessor) buildOperatorPipeline(emitter *ChannelEmitter) (*pipeline.DirectedPipeline, error) {
	baseCfg := ltp.config.BaseConfig

	return pipeline.Config{
		Operators:     baseCfg.Operators,
		DefaultOutput: emitter,
	}.Build(ltp.logger.Sugar())
}

func (ltp *logsTransformProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {

	numToProcess := ld.LogRecordCount()

	// create pipeline components
	fromConverter := adapter.NewFromPdataConverter(1, ltp.logger)
	emitter := NewChannelEmitter(ltp.logger.Sugar())
	converter := adapter.NewConverter(ltp.logger)
	pipe, err := ltp.buildOperatorPipeline(emitter)

	if err != nil {
		return ld, err
	}

	firstOp := pipe.Operators()[0]

	outputChannel := make(chan outputType)
	pipelineContext, pipelineCancel := context.WithCancel(ctx)
	defer pipelineCancel()

	// setup stanza pipeline processing
	go ltp.converterLoop(pipelineContext, fromConverter, firstOp, outputChannel)
	go ltp.emitterLoop(pipelineContext, emitter, converter, numToProcess, outputChannel)
	go ltp.consumerLoop(pipelineContext, converter, outputChannel)

	// start pipeline components
	err = pipe.Start(storage.NewNopClient())
	defer pipe.Stop()
	if err != nil {
		return ld, err
	}
	fromConverter.Start()
	defer fromConverter.Stop()
	converter.Start()
	defer converter.Stop()

	// push all items to be processed
	err = fromConverter.Batch(ld)
	if err != nil {
		return ld, err
	}

	doneChan := ctx.Done()
	select {
	case <-doneChan:
		return ld, errors.New("processor interrupted")
	case output, ok := <-outputChannel:
		if !ok {
			return ld, errors.New("processor encountered an issue receiving logs")
		}
		if output.err != nil {
			return ld, output.err
		}
		return output.logs, nil
	}
}

// converterLoop reads the log entries produced by the fromConverter and sends them
// into the pipeline
func (ltp *logsTransformProcessor) converterLoop(
	ctx context.Context,
	fromConverter *adapter.FromPdataConverter,
	firstOperator operator.Operator,
	outputChannel chan<- outputType) {

	//TODO: defer ltp.wg.Done()
	for {
		select {
		case <-ctx.Done():
			ltp.logger.Debug("converter loop stopped")
			return

		case entries, ok := <-fromConverter.OutChannel():
			if !ok {
				ltp.logger.Debug("fromConverter channel got closed")
				return
			}

			for _, e := range entries {
				// Add item to the first operator of the pipeline manually
				if err := firstOperator.Process(ctx, e); err != nil {
					outputChannel <- outputType{err: fmt.Errorf("processor encountered an issue with the pipeline: %w", err)}
					break
				}
			}
		}
	}
}

// emitterLoop reads the log entries produced by the emitter and batches them
// in converter.
func (ltp *logsTransformProcessor) emitterLoop(
	ctx context.Context,
	emitter *ChannelEmitter,
	converter *adapter.Converter,
	numExpected int,
	outputChannel chan<- outputType) {

	var entries []*entry.Entry
	for i := 0; i < numExpected; i++ {
		select {
		case <-ctx.Done():
			ltp.logger.Debug("emitter loop stopped")
			return

		case e, ok := <-emitter.OutChannel():
			if !ok {
				ltp.logger.Debug("emitter channel got closed")
				return
			}

			entries = append(entries, e)
		}
	}
	if err := converter.Batch(entries); err != nil {
		outputChannel <- outputType{err: fmt.Errorf("processor encountered an issue with the converter: %w", err)}
	}
}

// consumerLoop reads converter log entries and calls the consumer to consumer them.
func (ltp *logsTransformProcessor) consumerLoop(ctx context.Context,
	converter *adapter.Converter,
	outputChannel chan<- outputType) {

	for {
		select {
		case <-ctx.Done():
			ltp.logger.Debug("consumer loop stopped")
			return

		case pLogs, ok := <-converter.OutChannel():
			if !ok {
				ltp.logger.Debug("converter channel got closed")
				return
			}

			outputChannel <- outputType{logs: pLogs, err: nil}
		}
	}
}
