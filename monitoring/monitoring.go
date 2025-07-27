package monitoring

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
	gormprometheus "gorm.io/plugin/prometheus"
)

// Config holds monitoring configuration
type Config struct {
	ServiceName          string
	DBMetricsEnabled     bool
	DBMetricsInterval    int
	KafkaMetricsEnabled  bool
	KafkaMetricsInterval int
}

// Monitoring holds all monitoring components
type Monitoring struct {
	KafkaMetrics *KafkaMetrics
	config       Config
}

// KafkaMetrics holds client-side Kafka metrics
type KafkaMetrics struct {
	// Producer client metrics
	ProducerMessagesTotal   prometheus.CounterVec
	ProducerDurationSeconds prometheus.HistogramVec
	ProducerErrorsTotal     prometheus.CounterVec

	// Consumer client metrics
	ConsumerMessagesTotal   prometheus.CounterVec
	ConsumerDurationSeconds prometheus.HistogramVec
	ConsumerErrorsTotal     prometheus.CounterVec
}

// Init initializes monitoring components
func Init(cfg Config) *Monitoring {
	m := &Monitoring{
		config: cfg,
	}

	if cfg.KafkaMetricsEnabled {
		m.KafkaMetrics = newKafkaMetrics(cfg.ServiceName)
	}

	return m
}

// newKafkaMetrics creates and registers client-side Kafka metrics with Prometheus
func newKafkaMetrics(serviceName string) *KafkaMetrics {
	return &KafkaMetrics{
		// Producer client metrics
		ProducerMessagesTotal: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_producer_messages_total",
				Help: "Total number of messages produced to Kafka",
			},
			[]string{"service", "topic", "status"},
		),
		ProducerDurationSeconds: *promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "kafka_producer_duration_seconds",
				Help:    "Time spent producing messages to Kafka",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"service", "topic"},
		),
		ProducerErrorsTotal: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_producer_errors_total",
				Help: "Total number of producer errors",
			},
			[]string{"service", "topic", "error_type"},
		),

		// Consumer client metrics
		ConsumerMessagesTotal: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_consumer_messages_total",
				Help: "Total number of messages consumed from Kafka",
			},
			[]string{"service", "topic", "group", "status"},
		),
		ConsumerDurationSeconds: *promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "kafka_consumer_duration_seconds",
				Help:    "Time spent consuming messages from Kafka",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"service", "topic", "group"},
		),
		ConsumerErrorsTotal: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_consumer_errors_total",
				Help: "Total number of consumer errors",
			},
			[]string{"service", "topic", "group", "error_type"},
		),
	}
}

// SetupDatabaseMetrics configures GORM with Prometheus metrics
func (m *Monitoring) SetupDatabaseMetrics(db *gorm.DB, dbName string) error {
	if !m.config.DBMetricsEnabled {
		return nil
	}

	return db.Use(gormprometheus.New(gormprometheus.Config{
		DBName:          dbName,
		RefreshInterval: uint32(m.config.DBMetricsInterval),
		StartServer:     false, // Use existing metrics endpoint
		MetricsCollector: []gormprometheus.MetricsCollector{
			&gormprometheus.MySQL{
				VariableNames: []string{"Threads_running", "Threads_connected", "Slow_queries"},
			},
		},
	}))
}

// InstrumentedProducer wraps kafka.Writer with metrics
type InstrumentedProducer struct {
	writer      *kafka.Writer
	metrics     *KafkaMetrics
	serviceName string
}

// NewInstrumentedProducer creates a new instrumented Kafka producer
func (m *Monitoring) NewInstrumentedProducer(writer *kafka.Writer) *InstrumentedProducer {
	if m.KafkaMetrics == nil {
		return &InstrumentedProducer{writer: writer}
	}

	return &InstrumentedProducer{
		writer:      writer,
		metrics:     m.KafkaMetrics,
		serviceName: m.config.ServiceName,
	}
}

// WriteMessages writes messages with metrics instrumentation
func (p *InstrumentedProducer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	start := time.Now()
	topic := p.writer.Topic

	err := p.writer.WriteMessages(ctx, msgs...)

	// Record metrics only if available
	if p.metrics != nil {
		duration := time.Since(start)
		status := "success"
		if err != nil {
			status = "error"
			p.metrics.ProducerErrorsTotal.WithLabelValues(p.serviceName, topic, "write_error").Inc()
		}

		p.metrics.ProducerMessagesTotal.WithLabelValues(p.serviceName, topic, status).Add(float64(len(msgs)))
		p.metrics.ProducerDurationSeconds.WithLabelValues(p.serviceName, topic).Observe(duration.Seconds())
	}

	return err
}

// Close closes the underlying writer
func (p *InstrumentedProducer) Close() error {
	return p.writer.Close()
}

// InstrumentedConsumer wraps kafka.Reader with metrics
type InstrumentedConsumer struct {
	reader      *kafka.Reader
	metrics     *KafkaMetrics
	serviceName string
	groupID     string
}

// NewInstrumentedConsumer creates a new instrumented Kafka consumer
func (m *Monitoring) NewInstrumentedConsumer(reader *kafka.Reader, groupID string) *InstrumentedConsumer {
	if m.KafkaMetrics == nil {
		return &InstrumentedConsumer{reader: reader, groupID: groupID}
	}

	return &InstrumentedConsumer{
		reader:      reader,
		metrics:     m.KafkaMetrics,
		serviceName: m.config.ServiceName,
		groupID:     groupID,
	}
}

// ReadMessage reads a message with metrics instrumentation
func (c *InstrumentedConsumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	start := time.Now()

	msg, err := c.reader.ReadMessage(ctx)

	// Record metrics only if available
	if c.metrics != nil {
		duration := time.Since(start)
		topic := msg.Topic
		if topic == "" && c.reader.Config().Topic != "" {
			topic = c.reader.Config().Topic
		}

		status := "success"
		if err != nil {
			status = "error"
			errorType := "read_error"
			if ctx.Err() != nil {
				errorType = "context_error"
			}
			c.metrics.ConsumerErrorsTotal.WithLabelValues(c.serviceName, topic, c.groupID, errorType).Inc()
		} else {
			c.metrics.ConsumerMessagesTotal.WithLabelValues(c.serviceName, topic, c.groupID, status).Inc()
		}

		c.metrics.ConsumerDurationSeconds.WithLabelValues(c.serviceName, topic, c.groupID).Observe(duration.Seconds())
	}

	return msg, err
}

// Close closes the underlying reader
func (c *InstrumentedConsumer) Close() error {
	return c.reader.Close()
}

// Stats returns the current reader stats
func (c *InstrumentedConsumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}
