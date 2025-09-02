package inbox

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Repository handles inbox event persistence
type Repository struct {
	db *gorm.DB
}

// NewRepository creates a new inbox repository
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// Store saves an inbox event, returns true if it's a new event, false if duplicate
func (r *Repository) Store(ctx context.Context, event *InboxEvent) (bool, error) {
	err := r.db.WithContext(ctx).Create(event).Error
	if err != nil {
		// Check if it's a duplicate key error
		if isDuplicateKeyError(err) {
			return false, nil // Not an error, just a duplicate
		}
		return false, err
	}
	return true, nil
}

// StoreWithTransaction saves an inbox event within a transaction
func (r *Repository) StoreWithTransaction(ctx context.Context, tx *gorm.DB, event *InboxEvent) (bool, error) {
	if tx == nil {
		tx = r.db.WithContext(ctx)
	}
	
	err := tx.Create(event).Error
	if err != nil {
		if isDuplicateKeyError(err) {
			return false, nil // Not an error, just a duplicate
		}
		return false, err
	}
	return true, nil
}

// GetByMessageID retrieves an inbox event by message ID
func (r *Repository) GetByMessageID(ctx context.Context, messageID string) (*InboxEvent, error) {
	var event InboxEvent
	err := r.db.WithContext(ctx).Where("message_id = ?", messageID).First(&event).Error
	if err != nil {
		return nil, err
	}
	return &event, nil
}

// IsMessageProcessed checks if a message has already been processed
func (r *Repository) IsMessageProcessed(ctx context.Context, messageID string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&InboxEvent{}).
		Where("message_id = ? AND status IN (?)", messageID, []ProcessStatus{ProcessStatusProcessed, ProcessStatusSkipped}).
		Count(&count).Error
	
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetPendingEvents retrieves events that are ready to be processed
func (r *Repository) GetPendingEvents(ctx context.Context, limit int) ([]*InboxEvent, error) {
	var events []*InboxEvent
	
	query := r.db.WithContext(ctx).
		Where("status = ? OR (status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?))", 
			ProcessStatusPending, ProcessStatusPending, time.Now()).
		Where("attempts < max_attempts").
		Order("created_at ASC").
		Limit(limit)
	
	err := query.Find(&events).Error
	return events, err
}

// UpdateEventStatus updates the status of an inbox event
func (r *Repository) UpdateEventStatus(ctx context.Context, event *InboxEvent) error {
	return r.db.WithContext(ctx).Save(event).Error
}

// UpdateEventsStatus updates the status of multiple inbox events
func (r *Repository) UpdateEventsStatus(ctx context.Context, events []*InboxEvent) error {
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

// GetEventByID retrieves an inbox event by ID
func (r *Repository) GetEventByID(ctx context.Context, id string) (*InboxEvent, error) {
	var event InboxEvent
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&event).Error
	if err != nil {
		return nil, err
	}
	return &event, nil
}

// GetEventsByTopic retrieves inbox events by topic
func (r *Repository) GetEventsByTopic(ctx context.Context, topic string, limit int) ([]*InboxEvent, error) {
	var events []*InboxEvent
	err := r.db.WithContext(ctx).
		Where("topic = ?", topic).
		Order("created_at ASC").
		Limit(limit).
		Find(&events).Error
	return events, err
}

// DeleteOldEvents deletes events older than the specified duration
func (r *Repository) DeleteOldEvents(ctx context.Context, olderThan time.Duration) error {
	cutoff := time.Now().Add(-olderThan)
	return r.db.WithContext(ctx).
		Where("status IN (?) AND processed_at < ?", 
			[]ProcessStatus{ProcessStatusProcessed, ProcessStatusSkipped}, cutoff).
		Delete(&InboxEvent{}).Error
}

// GetFailedEvents retrieves events that have failed permanently
func (r *Repository) GetFailedEvents(ctx context.Context, limit int) ([]*InboxEvent, error) {
	var events []*InboxEvent
	err := r.db.WithContext(ctx).
		Where("status = ?", ProcessStatusFailed).
		Order("created_at ASC").
		Limit(limit).
		Find(&events).Error
	return events, err
}

// ResetFailedEvent resets a failed event to pending status for retry
func (r *Repository) ResetFailedEvent(ctx context.Context, eventID string) error {
	now := time.Now()
	return r.db.WithContext(ctx).
		Model(&InboxEvent{}).
		Where("id = ? AND status = ?", eventID, ProcessStatusFailed).
		Updates(map[string]interface{}{
			"status":        ProcessStatusPending,
			"attempts":      0,
			"last_error":    "",
			"next_retry_at": nil,
			"updated_at":    now,
		}).Error
}

// GetEventStats returns statistics about inbox events
func (r *Repository) GetEventStats(ctx context.Context) (map[string]int64, error) {
	stats := make(map[string]int64)
	
	// Count by status
	var statusCounts []struct {
		Status string
		Count  int64
	}
	
	err := r.db.WithContext(ctx).
		Model(&InboxEvent{}).
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
		Model(&InboxEvent{}).
		Where("status = ? AND next_retry_at > ?", ProcessStatusPending, time.Now()).
		Count(&pendingRetryCount).Error
	
	if err != nil {
		return nil, err
	}
	
	stats["pending_retry"] = pendingRetryCount
	
	return stats, nil
}

// isDuplicateKeyError checks if the error is a duplicate key constraint violation
func isDuplicateKeyError(err error) bool {
	// This is database-specific. For MySQL, check for duplicate entry error
	// You might need to adjust this based on your database
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	// MySQL duplicate entry error
	return contains(errStr, "Duplicate entry") || 
		   contains(errStr, "duplicate key") ||
		   contains(errStr, "UNIQUE constraint failed")
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && 
		   (s == substr || 
		    (len(s) > len(substr) && 
		     (s[:len(substr)] == substr || 
		      s[len(s)-len(substr):] == substr || 
		      indexOfSubstring(s, substr) >= 0)))
}

// indexOfSubstring finds the index of a substring in a string
func indexOfSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
