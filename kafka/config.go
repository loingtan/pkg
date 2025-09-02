package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// ProducerConfig holds configuration for Kafka producers with at-least-once delivery
type ProducerConfig struct {
	Brokers      []string
	Topic        string
	RequiredAcks kafka.RequiredAcks
	Async        bool
	BatchSize    int
	BatchTimeout time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Balancer     kafka.Balancer
	Compression  kafka.Compression
	MaxAttempts  int
}

// ConsumerConfig holds configuration for Kafka consumers with at-least-once delivery
type ConsumerConfig struct {
	Brokers        []string
	Topic          string
	GroupID        string
	MinBytes       int
	MaxBytes       int
	MaxWait        time.Duration
	CommitInterval time.Duration
	StartOffset    int64
	MaxAttempts    int
}

// DefaultProducerConfig returns a producer configuration optimized for at-least-once delivery
func DefaultProducerConfig(brokers []string, topic string) ProducerConfig {
	return ProducerConfig{
		Brokers:      brokers,
		Topic:        topic,
		RequiredAcks: kafka.RequireAll, // Wait for all in-sync replicas to acknowledge
		Async:        false,            // Synchronous writes for reliability
		BatchSize:    100,              // Reasonable batch size for performance
		BatchTimeout: 10 * time.Millisecond,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Balancer:     &kafka.Hash{}, // Use hash balancer for consistent partitioning
		Compression:  kafka.Snappy,  // Enable compression for efficiency
		MaxAttempts:  3,             // Retry failed writes
	}
}

// DefaultConsumerConfig returns a consumer configuration optimized for at-least-once delivery
func DefaultConsumerConfig(brokers []string, topic, groupID string) ConsumerConfig {
	return ConsumerConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,                // Read immediately when data is available
		MaxBytes:       10e6,             // 10MB max message size
		MaxWait:        1 * time.Second,  // Don't wait too long for batching
		CommitInterval: 1 * time.Second,  // Commit offsets frequently
		StartOffset:    kafka.LastOffset, // Start from latest for new consumers
		MaxAttempts:    3,                // Retry failed reads
	}
}

// ReliableProducerConfig returns a producer configuration with maximum reliability
func ReliableProducerConfig(brokers []string, topic string) ProducerConfig {
	return ProducerConfig{
		Brokers:      brokers,
		Topic:        topic,
		RequiredAcks: kafka.RequireAll, // Wait for all in-sync replicas
		Async:        false,            // Synchronous writes only
		BatchSize:    1,                // No batching for maximum reliability
		BatchTimeout: 0,
		ReadTimeout:  30 * time.Second, // Longer timeouts for reliability
		WriteTimeout: 30 * time.Second,
		Balancer:     &kafka.Hash{},
		Compression:  0, // No compression to avoid potential issues
		MaxAttempts:  5, // More retry attempts
	}
}

// ReliableConsumerConfig returns a consumer configuration with maximum reliability
func ReliableConsumerConfig(brokers []string, topic, groupID string) ConsumerConfig {
	return ConsumerConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       1e6,                    // Smaller max message size for reliability
		MaxWait:        100 * time.Millisecond, // Shorter wait for immediate processing
		CommitInterval: 100 * time.Millisecond, // Commit very frequently
		StartOffset:    kafka.FirstOffset,      // Start from beginning for reliability
		MaxAttempts:    5,                      // More retry attempts
	}
}

// NewProducerWriter creates a kafka.Writer with the given configuration
func NewProducerWriter(config ProducerConfig) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Balancer:     config.Balancer,
		RequiredAcks: config.RequiredAcks,
		Async:        config.Async,
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		Compression:  config.Compression,
		MaxAttempts:  config.MaxAttempts,
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			// Log errors for debugging
			// You can integrate with your logging framework here
		}),
	}
}

// NewConsumerReader creates a kafka.Reader with the given configuration
func NewConsumerReader(config ConsumerConfig) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.Brokers,
		Topic:          config.Topic,
		GroupID:        config.GroupID,
		MinBytes:       config.MinBytes,
		MaxBytes:       config.MaxBytes,
		MaxWait:        config.MaxWait,
		CommitInterval: config.CommitInterval,
		StartOffset:    config.StartOffset,
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			// Log errors for debugging
			// You can integrate with your logging framework here
		}),
	})
}

// AtLeastOnceProducerConfig returns configuration for at-least-once delivery semantics
func AtLeastOnceProducerConfig(brokers []string, topic string) ProducerConfig {
	return ProducerConfig{
		Brokers:      brokers,
		Topic:        topic,
		RequiredAcks: kafka.RequireOne, // At least one replica must acknowledge
		Async:        false,            // Synchronous for error handling
		BatchSize:    50,               // Moderate batching for performance
		BatchTimeout: 5 * time.Millisecond,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		Balancer:     &kafka.LeastBytes{}, // Distribute load evenly
		Compression:  kafka.Snappy,        // Good compression ratio
		MaxAttempts:  3,                   // Reasonable retry count
	}
}

// AtLeastOnceConsumerConfig returns configuration for at-least-once delivery semantics
func AtLeastOnceConsumerConfig(brokers []string, topic, groupID string) ConsumerConfig {
	return ConsumerConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3,                   // 10KB min for efficiency
		MaxBytes:       10e6,                   // 10MB max
		MaxWait:        500 * time.Millisecond, // Reasonable wait time
		CommitInterval: 1 * time.Second,        // Commit after processing
		StartOffset:    kafka.LastOffset,
		MaxAttempts:    3,
	}
}

// IdempotentProducerConfig returns configuration for idempotent producers
func IdempotentProducerConfig(brokers []string, topic string) ProducerConfig {
	config := AtLeastOnceProducerConfig(brokers, topic)
	// For idempotent producers, we can be more aggressive with batching
	// since duplicates will be handled by the outbox pattern
	config.BatchSize = 100
	config.BatchTimeout = 10 * time.Millisecond
	return config
}

// DuplicateTolerantConsumerConfig returns configuration for consumers that can handle duplicates
func DuplicateTolerantConsumerConfig(brokers []string, topic, groupID string) ConsumerConfig {
	config := AtLeastOnceConsumerConfig(brokers, topic, groupID)
	// For duplicate-tolerant consumers (using inbox pattern), we can optimize for throughput
	config.MinBytes = 1e3  // 1KB min
	config.MaxBytes = 50e6 // 50MB max for higher throughput
	config.MaxWait = 1 * time.Second
	return config
}
