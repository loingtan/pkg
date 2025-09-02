package outbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// OutboxEvent represents an event stored in the outbox table
type OutboxEvent struct {
	ID          string          `json:"id" gorm:"primaryKey;type:varchar(36)" db:"id"`
	AggregateID string          `json:"aggregate_id" gorm:"type:varchar(36);not null;index" db:"aggregate_id"`
	EventType   string          `json:"event_type" gorm:"type:varchar(100);not null;index" db:"event_type"`
	EventData   json.RawMessage `json:"event_data" gorm:"type:json;not null" db:"event_data"`
	Topic       string          `json:"topic" gorm:"type:varchar(100);not null" db:"topic"`
	PartitionKey string         `json:"partition_key" gorm:"type:varchar(100)" db:"partition_key"`
	Status      EventStatus     `json:"status" gorm:"type:varchar(20);not null;default:'PENDING';index" db:"status"`
	Attempts    int             `json:"attempts" gorm:"default:0" db:"attempts"`
	MaxAttempts int             `json:"max_attempts" gorm:"default:3" db:"max_attempts"`
	LastError   string          `json:"last_error" gorm:"type:text" db:"last_error"`
	CreatedAt   time.Time       `json:"created_at" gorm:"autoCreateTime" db:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at" gorm:"autoUpdateTime" db:"updated_at"`
	ProcessedAt *time.Time      `json:"processed_at" gorm:"index" db:"processed_at"`
	NextRetryAt *time.Time      `json:"next_retry_at" gorm:"index" db:"next_retry_at"`
}

func (OutboxEvent) TableName() string {
	return "outbox_events"
}

// EventStatus represents the status of an outbox event
type EventStatus string

const (
	EventStatusPending   EventStatus = "PENDING"
	EventStatusPublished EventStatus = "PUBLISHED"
	EventStatusFailed    EventStatus = "FAILED"
	EventStatusSkipped   EventStatus = "SKIPPED"
)

// NewOutboxEvent creates a new outbox event
func NewOutboxEvent(aggregateID, eventType string, eventData interface{}, topic, partitionKey string) (*OutboxEvent, error) {
	data, err := json.Marshal(eventData)
	if err != nil {
		return nil, err
	}

	return &OutboxEvent{
		ID:           uuid.New().String(),
		AggregateID:  aggregateID,
		EventType:    eventType,
		EventData:    data,
		Topic:        topic,
		PartitionKey: partitionKey,
		Status:       EventStatusPending,
		Attempts:     0,
		MaxAttempts:  3,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}, nil
}

// MarkAsPublished marks the event as successfully published
func (e *OutboxEvent) MarkAsPublished() {
	now := time.Now()
	e.Status = EventStatusPublished
	e.ProcessedAt = &now
	e.UpdatedAt = now
}

// MarkAsFailed marks the event as failed and schedules retry if attempts remain
func (e *OutboxEvent) MarkAsFailed(err error) {
	now := time.Now()
	e.Attempts++
	e.LastError = err.Error()
	e.UpdatedAt = now

	if e.Attempts >= e.MaxAttempts {
		e.Status = EventStatusFailed
	} else {
		// Exponential backoff: 2^attempts minutes
		retryDelay := time.Duration(1<<e.Attempts) * time.Minute
		nextRetry := now.Add(retryDelay)
		e.NextRetryAt = &nextRetry
	}
}

// CanRetry returns true if the event can be retried
func (e *OutboxEvent) CanRetry() bool {
	if e.Status == EventStatusPublished || e.Status == EventStatusFailed {
		return false
	}
	if e.NextRetryAt != nil && time.Now().Before(*e.NextRetryAt) {
		return false
	}
	return e.Attempts < e.MaxAttempts
}

// GetEventData unmarshals the event data into the provided interface
func (e *OutboxEvent) GetEventData(v interface{}) error {
	return json.Unmarshal(e.EventData, v)
}
