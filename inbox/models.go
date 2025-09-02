package inbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// InboxEvent represents an event received and processed through the inbox pattern
type InboxEvent struct {
	ID          string          `json:"id" gorm:"primaryKey;type:varchar(36)" db:"id"`
	MessageID   string          `json:"message_id" gorm:"type:varchar(100);not null;uniqueIndex" db:"message_id"`
	Topic       string          `json:"topic" gorm:"type:varchar(100);not null;index" db:"topic"`
	Partition   int32           `json:"partition" gorm:"not null" db:"partition"`
	Offset      int64           `json:"offset" gorm:"not null" db:"offset"`
	EventType   string          `json:"event_type" gorm:"type:varchar(100);not null;index" db:"event_type"`
	EventData   json.RawMessage `json:"event_data" gorm:"type:json;not null" db:"event_data"`
	Headers     json.RawMessage `json:"headers" gorm:"type:json" db:"headers"`
	Status      ProcessStatus   `json:"status" gorm:"type:varchar(20);not null;default:'PENDING';index" db:"status"`
	Attempts    int             `json:"attempts" gorm:"default:0" db:"attempts"`
	MaxAttempts int             `json:"max_attempts" gorm:"default:3" db:"max_attempts"`
	LastError   string          `json:"last_error" gorm:"type:text" db:"last_error"`
	CreatedAt   time.Time       `json:"created_at" gorm:"autoCreateTime" db:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at" gorm:"autoUpdateTime" db:"updated_at"`
	ProcessedAt *time.Time      `json:"processed_at" gorm:"index" db:"processed_at"`
	NextRetryAt *time.Time      `json:"next_retry_at" gorm:"index" db:"next_retry_at"`
}

func (InboxEvent) TableName() string {
	return "inbox_events"
}

// ProcessStatus represents the processing status of an inbox event
type ProcessStatus string

const (
	ProcessStatusPending   ProcessStatus = "PENDING"
	ProcessStatusProcessed ProcessStatus = "PROCESSED"
	ProcessStatusFailed    ProcessStatus = "FAILED"
	ProcessStatusSkipped   ProcessStatus = "SKIPPED"
)

// NewInboxEvent creates a new inbox event from Kafka message
func NewInboxEvent(messageID, topic string, partition int32, offset int64, eventType string, eventData []byte, headers map[string]string) (*InboxEvent, error) {
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return nil, err
	}

	return &InboxEvent{
		ID:          uuid.New().String(),
		MessageID:   messageID,
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		EventType:   eventType,
		EventData:   eventData,
		Headers:     headersJSON,
		Status:      ProcessStatusPending,
		Attempts:    0,
		MaxAttempts: 3,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}, nil
}

// MarkAsProcessed marks the event as successfully processed
func (e *InboxEvent) MarkAsProcessed() {
	now := time.Now()
	e.Status = ProcessStatusProcessed
	e.ProcessedAt = &now
	e.UpdatedAt = now
}

// MarkAsFailed marks the event as failed and schedules retry if attempts remain
func (e *InboxEvent) MarkAsFailed(err error) {
	now := time.Now()
	e.Attempts++
	e.LastError = err.Error()
	e.UpdatedAt = now

	if e.Attempts >= e.MaxAttempts {
		e.Status = ProcessStatusFailed
	} else {
		// Exponential backoff: 2^attempts minutes
		retryDelay := time.Duration(1<<e.Attempts) * time.Minute
		nextRetry := now.Add(retryDelay)
		e.NextRetryAt = &nextRetry
	}
}

// MarkAsSkipped marks the event as skipped (e.g., duplicate or invalid)
func (e *InboxEvent) MarkAsSkipped(reason string) {
	now := time.Now()
	e.Status = ProcessStatusSkipped
	e.LastError = reason
	e.ProcessedAt = &now
	e.UpdatedAt = now
}

// CanRetry returns true if the event can be retried
func (e *InboxEvent) CanRetry() bool {
	if e.Status == ProcessStatusProcessed || e.Status == ProcessStatusFailed || e.Status == ProcessStatusSkipped {
		return false
	}
	if e.NextRetryAt != nil && time.Now().Before(*e.NextRetryAt) {
		return false
	}
	return e.Attempts < e.MaxAttempts
}

// GetEventData unmarshals the event data into the provided interface
func (e *InboxEvent) GetEventData(v interface{}) error {
	return json.Unmarshal(e.EventData, v)
}

// GetHeaders unmarshals the headers into a map
func (e *InboxEvent) GetHeaders() (map[string]string, error) {
	var headers map[string]string
	if len(e.Headers) == 0 {
		return make(map[string]string), nil
	}
	err := json.Unmarshal(e.Headers, &headers)
	return headers, err
}

// GetMessageKey generates a unique key for message deduplication
func (e *InboxEvent) GetMessageKey() string {
	return e.MessageID
}

// IsProcessed returns true if the event has been successfully processed
func (e *InboxEvent) IsProcessed() bool {
	return e.Status == ProcessStatusProcessed
}

// IsFailed returns true if the event has permanently failed
func (e *InboxEvent) IsFailed() bool {
	return e.Status == ProcessStatusFailed
}

// IsSkipped returns true if the event was skipped
func (e *InboxEvent) IsSkipped() bool {
	return e.Status == ProcessStatusSkipped
}
