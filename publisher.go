package elasticplugin

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"sync"
	"time"
)

var (
	ErrPublisherClosed = errors.New("publisher is closed")
)

// Publisher inserts the Messages as rows into SQL table.
type Publisher struct {
	Topic             string
	schemaAdapter     Adapter
	publishWg         *sync.WaitGroup
	closeCh           chan struct{}
	closed            bool
	initializedTopics sync.Map
	logger            watermill.LoggerAdapter
	client     *http.Client
	ElasticURL string

}

func (c *Publisher) setDefaults() {
	var schema scyllaSchema
	c.schemaAdapter = schema
}

// NewPublisher crete pub module
func (c *Publisher) NewPublisher(schema Adapter, logger watermill.LoggerAdapter) (*Publisher, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	if schema == nil {
		var schemaAdapter scyllaSchema
		schema = schemaAdapter
	}

	return &Publisher{
		schemaAdapter: schema,
		publishWg:     new(sync.WaitGroup),
		closeCh:       make(chan struct{}),
		closed:        false,
		logger:        logger,
	}, nil
}

// Publish inserts the messages as rows into the MessagesTable.
func (c *Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if c.closed {
		return ErrPublisherClosed
	}
	// Setup tracing
	tr := otel.Tracer("scylla")
	// Setup metrics
	meter := global.Meter("scylla")
	publishTracker := metric.Must(meter).NewInt64ValueRecorder("watermill_scylla_publish_ms")
	// Init context
	c.publishWg.Add(1)
	defer c.publishWg.Done()
	for _, msg := range messages {
		ctx := msg.Context()
		_, pubAttr := buildAttrs(ctx)
		spanCtx, span := tr.Start(ctx, "publish messages", trace.WithSpanKind(trace.SpanKindProducer))
		span.SetAttributes(pubAttr...)
		// Track processing complete
		publishStart := time.Now()
		err = c.query(spanCtx, msg)
		if err != nil {
			span.SetStatus(codes.Error, "failed to insert query into scylla")
			span.RecordError(err)
			return
		}
		span.End()
		meter.RecordBatch(
			ctx,
			pubAttr,
			publishTracker.Measurement(time.Since(publishStart).Milliseconds()),
		)
	}

	return nil
}
func (c *Publisher) query(ctx context.Context, msg *message.Message) error {
	c.logger.Trace("Inserting message to Elastic", watermill.LogFields{
		"query": string(msg.Payload),
	})
	err := c.sendData(msg.Payload)
	if err != nil {
		c.logger.Error("could not insert message as row", err, watermill.LogFields{
			"topic": c.Topic,
		})
		return err
	}
	return nil
}

// buildAttrs build otel attributes from watermill context data
func buildAttrs(ctx context.Context) (processor, publisher []attribute.KeyValue) {
	handler := attribute.String("watermill_handler", message.HandlerNameFromCtx(ctx))
	proAttrs := []attribute.KeyValue{handler}
	pubAttrs := []attribute.KeyValue{handler, attribute.String("kafka_topic", message.PublishTopicFromCtx(ctx))}
	return proAttrs, pubAttrs
}

func (c *Publisher) sendData(body []byte) error {
	var resp, err = c.client.Post(c.ElasticURL+"/"+c.Topic+"/_doc", "application/json", bytes.NewReader(body))
	defer resp.Body.Close()
	if err != nil {
		return err
	}
	var res map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return err
	}
	return nil
}



func (c *Publisher) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	//implement
	return nil, nil
}
// Close closes the publisher, which means that all Publish calls called before are finished.
func (c *Publisher) Close() error {
	if c.closed {
		return nil
	}

	c.closed = true

	close(c.closeCh)
	c.publishWg.Wait()

	return nil
}
