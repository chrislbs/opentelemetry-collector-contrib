package logstransformprocessor

import (
	"context"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
)

// ChannelEmitter is a stanza operator that logs entries immediately to an output channel
type ChannelEmitter struct {
	helper.OutputOperator
	logChan chan *entry.Entry
}

// NewChannelEmitter creates a new receiver output
func NewChannelEmitter(logger *zap.SugaredLogger) *ChannelEmitter {
	return &ChannelEmitter{
		OutputOperator: helper.OutputOperator{
			BasicOperator: helper.BasicOperator{
				OperatorID:    "channel_emitter",
				OperatorType:  "channel_emitter",
				SugaredLogger: logger,
			},
		},
		logChan: make(chan *entry.Entry),
	}
}

// Start starts the goroutine(s) required for this operator
func (e *ChannelEmitter) Start(_ operator.Persister) error {
	return nil
}

// Stop will close the log channel and stop running goroutines
func (e *ChannelEmitter) Stop() error {
	return nil
}

// OutChannel returns the channel on which entries will be sent to.
func (e *ChannelEmitter) OutChannel() <-chan *entry.Entry {
	return e.logChan
}

// Process will emit an entry to the output channel
func (e *ChannelEmitter) Process(ctx context.Context, ent *entry.Entry) error {
	doneChan := ctx.Done()
	select {
	case e.logChan <- ent:
	case <-doneChan:
	}

	return nil
}
