package inbox

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

// MessageHandler defines the interface for handling processed messages
type MessageHandler interface {
	Handle(ctx context.Context, event *InboxEvent) error
}

// MessageHandlerFunc is a function adapter for MessageHandler
type MessageHandlerFunc func(ctx context.Context, event *InboxEvent) error

func (f MessageHandlerFunc) Handle(ctx context.Context, event *InboxEvent) error {
	return f(ctx, event)
}

// KafkaReader interface for reading messages from Kafka
type KafkaReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

// Consumer handles consuming messages with inbox pattern for exactly-once processing
type Consumer struct {
	repo         *Repository
	db           *gorm.DB
	kafkaReader  KafkaReader
	handler      MessageHandler
	batchSize    int
	pollInterval time.Duration
	running      bool
	stopCh       chan struct{}
	wg           sync.WaitGroup
	mu           sync.RWMutex
}

// ConsumerConfig holds configuration for the inbox consumer
type ConsumerConfig struct {
	BatchSize    int           // Number of events to process in each batch
	PollInterval time.Duration // How often to poll for new events to retry
}

// DefaultConsumerConfig returns default configuration
func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		BatchSize:    50,
		PollInterval: 10 * time.Second,
	}
}

// NewConsumer creates a new inbox consumer
func NewConsumer(repo *Repository, db *gorm.DB, kafkaReader KafkaReader, handler MessageHandler, config ConsumerConfig) *Consumer {
	if config.BatchSize <= 0 {
		config.BatchSize = 50
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 10 * time.Second
	}

	return &Consumer{
		repo:         repo,
		db:           db,
		kafkaReader:  kafkaReader,
		handler:      handler,
		batchSize:    config.BatchSize,
		pollInterval: config.PollInterval,
		stopCh:       make(chan struct{}),
	}
}

// Start begins the inbox consumer
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("consumer is already running")
	}

	c.running = true
	c.wg.Add(2) // One for Kafka consumer, one for retry processor

	// Start Kafka message consumer
	go func() {
		defer c.wg.Done()
		c.consumeKafkaMessages(ctx)
	}()

	// Start retry processor for failed messages
	go func() {
		defer c.wg.Done()
		c.processRetries(ctx)
	}()

	log.Println("Inbox consumer started")
	return nil
}

// Stop gracefully stops the inbox consumer
func (c *Consumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}

	c.running = false
	close(c.stopCh)
	c.wg.Wait()

	if c.kafkaReader != nil {
		c.kafkaReader.Close()
	}

	log.Println("Inbox consumer stopped")
	return nil
}

// IsRunning returns true if the consumer is currently running
func (c *Consumer) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}

// consumeKafkaMessages consumes messages from Kafka and stores them in inbox
func (c *Consumer) consumeKafkaMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer context cancelled")
			return
		case <-c.stopCh:
			log.Println("Kafka consumer stop signal received")
			return
		default:
			if err := c.processKafkaMessage(ctx); err != nil {
				log.Printf("Error processing Kafka message: %v", err)
				// Add a small delay to prevent tight loop on persistent errors
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// processKafkaMessage processes a single Kafka message
func (c *Consumer) processKafkaMessage(ctx context.Context) error {
	msg, err := c.kafkaReader.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to read Kafka message: %w", err)
	}

	// Extract message metadata
	messageID := c.extractMessageID(msg)
	eventType := c.extractEventType(msg)
	headers := c.extractHeaders(msg)

	// Create inbox event
	inboxEvent, err := NewInboxEvent(
		messageID,
		msg.Topic,
		int32(msg.Partition),
		msg.Offset,
		eventType,
		msg.Value,
		headers,
	)
	if err != nil {
		return fmt.Errorf("failed to create inbox event: %w", err)
	}

	// Store and process the event atomically
	return c.storeAndProcessEvent(ctx, inboxEvent)
}

// storeAndProcessEvent stores the event in inbox and processes it atomically
func (c *Consumer) storeAndProcessEvent(ctx context.Context, event *InboxEvent) error {
	// Start database transaction
	tx := c.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("failed to start transaction: %w", tx.Error)
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Store event in inbox (with deduplication)
	isNew, err := c.repo.StoreWithTransaction(ctx, tx, event)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to store inbox event: %w", err)
	}

	if !isNew {
		// Duplicate message, just commit and return
		tx.Commit()
		log.Printf("Duplicate message ignored: %s", event.MessageID)
		return nil
	}

	// Process the event
	if err := c.handler.Handle(ctx, event); err != nil {
		tx.Rollback()
		// Mark as failed and store the error
		event.MarkAsFailed(err)
		if updateErr := c.repo.UpdateEventStatus(ctx, event); updateErr != nil {
			log.Printf("Failed to update event status after processing error: %v", updateErr)
		}
		return fmt.Errorf("failed to process event: %w", err)
	}

	// Mark as processed
	event.MarkAsProcessed()
	if err := c.repo.UpdateEventStatus(ctx, event); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update event status: %w", err)
	}

	// Commit transaction
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Successfully processed message: %s (type: %s)", event.MessageID, event.EventType)
	return nil
}

// processRetries processes failed events that are ready for retry
func (c *Consumer) processRetries(ctx context.Context) {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Retry processor context cancelled")
			return
		case <-c.stopCh:
			log.Println("Retry processor stop signal received")
			return
		case <-ticker.C:
			if err := c.processRetryBatch(ctx); err != nil {
				log.Printf("Error processing retry batch: %v", err)
			}
		}
	}
}

// processRetryBatch processes a batch of events ready for retry
func (c *Consumer) processRetryBatch(ctx context.Context) error {
	events, err := c.repo.GetPendingEvents(ctx, c.batchSize)
	if err != nil {
		return fmt.Errorf("failed to get pending events: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	log.Printf("Processing %d retry events", len(events))

	for _, event := range events {
		if !event.CanRetry() {
			continue
		}

		if err := c.retryEvent(ctx, event); err != nil {
			log.Printf("Failed to retry event %s: %v", event.ID, err)
		}
	}

	return nil
}

// retryEvent retries processing a single event
func (c *Consumer) retryEvent(ctx context.Context, event *InboxEvent) error {
	// Process the event
	if err := c.handler.Handle(ctx, event); err != nil {
		event.MarkAsFailed(err)
		return c.repo.UpdateEventStatus(ctx, event)
	}

	// Mark as processed
	event.MarkAsProcessed()
	return c.repo.UpdateEventStatus(ctx, event)
}

// extractMessageID extracts a unique message ID from Kafka message
func (c *Consumer) extractMessageID(msg kafka.Message) string {
	// Try to get from headers first
	for _, header := range msg.Headers {
		if header.Key == "message_id" || header.Key == "event_id" {
			return string(header.Value)
		}
	}

	// Fallback to topic-partition-offset
	return fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
}

// extractEventType extracts event type from Kafka message
func (c *Consumer) extractEventType(msg kafka.Message) string {
	for _, header := range msg.Headers {
		if header.Key == "event_type" {
			return string(header.Value)
		}
	}
	return "unknown"
}

// extractHeaders extracts headers from Kafka message
func (c *Consumer) extractHeaders(msg kafka.Message) map[string]string {
	headers := make(map[string]string)
	for _, header := range msg.Headers {
		headers[header.Key] = string(header.Value)
	}
	return headers
}
