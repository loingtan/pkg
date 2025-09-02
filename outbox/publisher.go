package outbox

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaPublisher interface for publishing messages to Kafka
type KafkaPublisher interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Publisher handles publishing outbox events to Kafka
type Publisher struct {
	repo           *Repository
	kafkaPublisher KafkaPublisher
	batchSize      int
	pollInterval   time.Duration
	running        bool
	stopCh         chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
}

// PublisherConfig holds configuration for the outbox publisher
type PublisherConfig struct {
	BatchSize    int           // Number of events to process in each batch
	PollInterval time.Duration // How often to poll for new events
}

// DefaultPublisherConfig returns default configuration
func DefaultPublisherConfig() PublisherConfig {
	return PublisherConfig{
		BatchSize:    100,
		PollInterval: 5 * time.Second,
	}
}

// NewPublisher creates a new outbox publisher
func NewPublisher(repo *Repository, kafkaPublisher KafkaPublisher, config PublisherConfig) *Publisher {
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 5 * time.Second
	}

	return &Publisher{
		repo:           repo,
		kafkaPublisher: kafkaPublisher,
		batchSize:      config.BatchSize,
		pollInterval:   config.PollInterval,
		stopCh:         make(chan struct{}),
	}
}

// Start begins the outbox publisher background process
func (p *Publisher) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("publisher is already running")
	}

	p.running = true
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()
		p.run(ctx)
	}()

	log.Println("Outbox publisher started")
	return nil
}

// Stop gracefully stops the outbox publisher
func (p *Publisher) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return nil
	}

	p.running = false
	close(p.stopCh)
	p.wg.Wait()

	log.Println("Outbox publisher stopped")
	return nil
}

// IsRunning returns true if the publisher is currently running
func (p *Publisher) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

// PublishPendingEvents manually triggers publishing of pending events
func (p *Publisher) PublishPendingEvents(ctx context.Context) error {
	return p.processBatch(ctx)
}

// run is the main loop for the outbox publisher
func (p *Publisher) run(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Outbox publisher context cancelled")
			return
		case <-p.stopCh:
			log.Println("Outbox publisher stop signal received")
			return
		case <-ticker.C:
			if err := p.processBatch(ctx); err != nil {
				log.Printf("Error processing outbox events: %v", err)
			}
		}
	}
}

// processBatch processes a batch of pending outbox events
func (p *Publisher) processBatch(ctx context.Context) error {
	events, err := p.repo.GetPendingEvents(ctx, p.batchSize)
	if err != nil {
		return fmt.Errorf("failed to get pending events: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	log.Printf("Processing %d outbox events", len(events))

	// Group events by topic for efficient publishing
	eventsByTopic := make(map[string][]*OutboxEvent)
	for _, event := range events {
		eventsByTopic[event.Topic] = append(eventsByTopic[event.Topic], event)
	}

	// Publish events topic by topic
	for topic, topicEvents := range eventsByTopic {
		if err := p.publishEventsForTopic(ctx, topic, topicEvents); err != nil {
			log.Printf("Failed to publish events for topic %s: %v", topic, err)
			// Continue with other topics even if one fails
		}
	}

	return nil
}

// publishEventsForTopic publishes all events for a specific topic
func (p *Publisher) publishEventsForTopic(ctx context.Context, topic string, events []*OutboxEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Convert outbox events to Kafka messages
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		message := kafka.Message{
			Topic: topic,
			Key:   []byte(event.PartitionKey),
			Value: event.EventData,
			Time:  event.CreatedAt,
			Headers: []kafka.Header{
				{Key: "event_id", Value: []byte(event.ID)},
				{Key: "event_type", Value: []byte(event.EventType)},
				{Key: "aggregate_id", Value: []byte(event.AggregateID)},
			},
		}
		messages = append(messages, message)
	}

	// Publish to Kafka
	if err := p.kafkaPublisher.WriteMessages(ctx, messages...); err != nil {
		// Mark events as failed
		for _, event := range events {
			event.MarkAsFailed(err)
		}
		if updateErr := p.repo.UpdateEventsStatus(ctx, events); updateErr != nil {
			log.Printf("Failed to update event status after Kafka error: %v", updateErr)
		}
		return fmt.Errorf("failed to publish messages to Kafka: %w", err)
	}

	// Mark events as published
	for _, event := range events {
		event.MarkAsPublished()
	}

	if err := p.repo.UpdateEventsStatus(ctx, events); err != nil {
		log.Printf("Failed to update event status after successful publish: %v", err)
		return fmt.Errorf("failed to update event status: %w", err)
	}

	log.Printf("Successfully published %d events to topic %s", len(events), topic)
	return nil
}

// CleanupOldEvents removes old published events to prevent table growth
func (p *Publisher) CleanupOldEvents(ctx context.Context, olderThan time.Duration) error {
	return p.repo.DeleteOldEvents(ctx, olderThan)
}
