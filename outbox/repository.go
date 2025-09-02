package outbox

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Repository handles outbox event persistence
type Repository struct {
	db *gorm.DB
}

// NewRepository creates a new outbox repository
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// Store saves an outbox event within a transaction
func (r *Repository) Store(ctx context.Context, tx *gorm.DB, event *OutboxEvent) error {
	if tx == nil {
		tx = r.db.WithContext(ctx)
	}
	return tx.Create(event).Error
}

// StoreEvents saves multiple outbox events within a transaction
func (r *Repository) StoreEvents(ctx context.Context, tx *gorm.DB, events []*OutboxEvent) error {
	if len(events) == 0 {
		return nil
	}
	
	if tx == nil {
		tx = r.db.WithContext(ctx)
	}
	return tx.Create(events).Error
}

// GetPendingEvents retrieves events that are ready to be published
func (r *Repository) GetPendingEvents(ctx context.Context, limit int) ([]*OutboxEvent, error) {
	var events []*OutboxEvent
	
	query := r.db.WithContext(ctx).
		Where("status = ? OR (status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?))", 
			EventStatusPending, EventStatusPending, time.Now()).
		Where("attempts < max_attempts").
		Order("created_at ASC").
		Limit(limit)
	
	err := query.Find(&events).Error
	return events, err
}

// UpdateEventStatus updates the status of an outbox event
func (r *Repository) UpdateEventStatus(ctx context.Context, event *OutboxEvent) error {
	return r.db.WithContext(ctx).Save(event).Error
}

// UpdateEventsStatus updates the status of multiple outbox events
func (r *Repository) UpdateEventsStatus(ctx context.Context, events []*OutboxEvent) error {
	if len(events) == 0 {
		return nil
	}
	
	// Use batch update for better performance
	for _, event := range events {
		if err := r.UpdateEventStatus(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// GetEventByID retrieves an outbox event by ID
func (r *Repository) GetEventByID(ctx context.Context, id string) (*OutboxEvent, error) {
	var event OutboxEvent
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&event).Error
	if err != nil {
		return nil, err
	}
	return &event, nil
}

// GetEventsByAggregateID retrieves outbox events by aggregate ID
func (r *Repository) GetEventsByAggregateID(ctx context.Context, aggregateID string) ([]*OutboxEvent, error) {
	var events []*OutboxEvent
	err := r.db.WithContext(ctx).
		Where("aggregate_id = ?", aggregateID).
		Order("created_at ASC").
		Find(&events).Error
	return events, err
}

// DeleteOldEvents deletes events older than the specified duration
func (r *Repository) DeleteOldEvents(ctx context.Context, olderThan time.Duration) error {
	cutoff := time.Now().Add(-olderThan)
	return r.db.WithContext(ctx).
		Where("status = ? AND processed_at < ?", EventStatusPublished, cutoff).
		Delete(&OutboxEvent{}).Error
}

// GetFailedEvents retrieves events that have failed permanently
func (r *Repository) GetFailedEvents(ctx context.Context, limit int) ([]*OutboxEvent, error) {
	var events []*OutboxEvent
	err := r.db.WithContext(ctx).
		Where("status = ?", EventStatusFailed).
		Order("created_at ASC").
		Limit(limit).
		Find(&events).Error
	return events, err
}

// ResetFailedEvent resets a failed event to pending status for retry
func (r *Repository) ResetFailedEvent(ctx context.Context, eventID string) error {
	now := time.Now()
	return r.db.WithContext(ctx).
		Model(&OutboxEvent{}).
		Where("id = ? AND status = ?", eventID, EventStatusFailed).
		Updates(map[string]interface{}{
			"status":        EventStatusPending,
			"attempts":      0,
			"last_error":    "",
			"next_retry_at": nil,
			"updated_at":    now,
		}).Error
}

// GetEventStats returns statistics about outbox events
func (r *Repository) GetEventStats(ctx context.Context) (map[string]int64, error) {
	stats := make(map[string]int64)
	
	// Count by status
	var statusCounts []struct {
		Status string
		Count  int64
	}
	
	err := r.db.WithContext(ctx).
		Model(&OutboxEvent{}).
		Select("status, count(*) as count").
		Group("status").
		Find(&statusCounts).Error
	
	if err != nil {
		return nil, err
	}
	
	for _, sc := range statusCounts {
		stats[fmt.Sprintf("status_%s", sc.Status)] = sc.Count
	}
	
	// Count events pending retry
	var pendingRetryCount int64
	err = r.db.WithContext(ctx).
		Model(&OutboxEvent{}).
		Where("status = ? AND next_retry_at > ?", EventStatusPending, time.Now()).
		Count(&pendingRetryCount).Error
	
	if err != nil {
		return nil, err
	}
	
	stats["pending_retry"] = pendingRetryCount
	
	return stats, nil
}
