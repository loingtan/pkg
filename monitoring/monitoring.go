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

type Config struct {
	ServiceName          string
	DBMetricsEnabled     bool
	DBMetricsInterval    int
	KafkaMetricsEnabled  bool
	KafkaMetricsInterval int
}

type Monitoring struct {
	KafkaMetrics *KafkaMetrics
	config       Config
}

type KafkaMetrics struct {
	ProducerMessagesTotal   prometheus.CounterVec
	ProducerDurationSeconds prometheus.HistogramVec
	ProducerErrorsTotal     prometheus.CounterVec

	ConsumerMessagesTotal   prometheus.CounterVec
	ConsumerDurationSeconds prometheus.HistogramVec
	ConsumerErrorsTotal     prometheus.CounterVec
}

func Init(cfg Config) *Monitoring {
	m := &Monitoring{
		config: cfg,
	}

	if cfg.KafkaMetricsEnabled {
		m.KafkaMetrics = newKafkaMetrics(cfg.ServiceName)
	}

	return m
}

func newKafkaMetrics(serviceName string) *KafkaMetrics {
	return &KafkaMetrics{

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

func (m *Monitoring) SetupDatabaseMetrics(db *gorm.DB, dbName string) error {
	if !m.config.DBMetricsEnabled {
		return nil
	}

	return db.Use(gormprometheus.New(gormprometheus.Config{
		DBName:          dbName,
		RefreshInterval: uint32(m.config.DBMetricsInterval),
		StartServer:     false,
		MetricsCollector: []gormprometheus.MetricsCollector{
			&gormprometheus.MySQL{
				VariableNames: []string{"Threads_running", "Threads_connected", "Slow_queries"},
			},
		},
	}))
}

type InstrumentedProducer struct {
	writer      *kafka.Writer
	metrics     *KafkaMetrics
	serviceName string
}

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

func (p *InstrumentedProducer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	start := time.Now()
	topic := p.writer.Topic

	err := p.writer.WriteMessages(ctx, msgs...)

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

func (p *InstrumentedProducer) Close() error {
	return p.writer.Close()
}

type InstrumentedConsumer struct {
	reader      *kafka.Reader
	metrics     *KafkaMetrics
	serviceName string
	groupID     string
}

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

func (c *InstrumentedConsumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	start := time.Now()

	msg, err := c.reader.ReadMessage(ctx)

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

func (c *InstrumentedConsumer) Close() error {
	return c.reader.Close()
}

func (c *InstrumentedConsumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}
