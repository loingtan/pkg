package idempotency

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// Repository handles idempotency record persistence
type Repository struct {
	db *gorm.DB
}

// NewRepository creates a new idempotency repository
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// Store saves an idempotency record
func (r *Repository) Store(ctx context.Context, record *IdempotencyRecord) error {
	return r.db.WithContext(ctx).Create(record).Error
}

// StoreWithTransaction saves an idempotency record within a transaction
func (r *Repository) StoreWithTransaction(ctx context.Context, tx *gorm.DB, record *IdempotencyRecord) error {
	if tx == nil {
		tx = r.db.WithContext(ctx)
	}
	return tx.Create(record).Error
}

// GetByIdempotencyKey retrieves a record by idempotency key
func (r *Repository) GetByIdempotencyKey(ctx context.Context, idempotencyKey string) (*IdempotencyRecord, error) {
	var record IdempotencyRecord
	err := r.db.WithContext(ctx).
		Where("idempotency_key = ?", idempotencyKey).
		First(&record).Error
	
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// GetByIdempotencyKeyAndUser retrieves a record by idempotency key and user ID
func (r *Repository) GetByIdempotencyKeyAndUser(ctx context.Context, idempotencyKey, userID string) (*IdempotencyRecord, error) {
	var record IdempotencyRecord
	err := r.db.WithContext(ctx).
		Where("idempotency_key = ? AND user_id = ?", idempotencyKey, userID).
		First(&record).Error
	
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// Update updates an idempotency record
func (r *Repository) Update(ctx context.Context, record *IdempotencyRecord) error {
	return r.db.WithContext(ctx).Save(record).Error
}

// UpdateWithTransaction updates an idempotency record within a transaction
func (r *Repository) UpdateWithTransaction(ctx context.Context, tx *gorm.DB, record *IdempotencyRecord) error {
	if tx == nil {
		tx = r.db.WithContext(ctx)
	}
	return tx.Save(record).Error
}

// DeleteExpired deletes expired idempotency records
func (r *Repository) DeleteExpired(ctx context.Context) error {
	return r.db.WithContext(ctx).
		Where("expires_at < ?", time.Now()).
		Delete(&IdempotencyRecord{}).Error
}

// GetExpiredRecords retrieves expired records for cleanup
func (r *Repository) GetExpiredRecords(ctx context.Context, limit int) ([]*IdempotencyRecord, error) {
	var records []*IdempotencyRecord
	err := r.db.WithContext(ctx).
		Where("expires_at < ?", time.Now()).
		Limit(limit).
		Find(&records).Error
	return records, err
}

// GetProcessingRecords retrieves records that are still processing (for cleanup of stuck requests)
func (r *Repository) GetProcessingRecords(ctx context.Context, olderThan time.Duration, limit int) ([]*IdempotencyRecord, error) {
	var records []*IdempotencyRecord
	cutoff := time.Now().Add(-olderThan)
	
	err := r.db.WithContext(ctx).
		Where("status = ? AND created_at < ?", RequestStatusProcessing, cutoff).
		Limit(limit).
		Find(&records).Error
	
	return records, err
}

// MarkStuckRequestsAsFailed marks processing requests that are older than specified duration as failed
func (r *Repository) MarkStuckRequestsAsFailed(ctx context.Context, olderThan time.Duration) error {
	cutoff := time.Now().Add(-olderThan)
	now := time.Now()
	
	return r.db.WithContext(ctx).
		Model(&IdempotencyRecord{}).
		Where("status = ? AND created_at < ?", RequestStatusProcessing, cutoff).
		Updates(map[string]interface{}{
			"status":       RequestStatusFailed,
			"error_message": "Request timed out",
			"completed_at": now,
			"updated_at":   now,
		}).Error
}

// GetStats returns statistics about idempotency records
func (r *Repository) GetStats(ctx context.Context) (map[string]int64, error) {
	stats := make(map[string]int64)
	
	// Count by status
	var statusCounts []struct {
		Status string
		Count  int64
	}
	
	err := r.db.WithContext(ctx).
		Model(&IdempotencyRecord{}).
		Select("status, count(*) as count").
		Group("status").
		Find(&statusCounts).Error
	
	if err != nil {
		return nil, err
	}
	
	for _, sc := range statusCounts {
		stats[sc.Status] = sc.Count
	}
	
	// Count expired records
	var expiredCount int64
	err = r.db.WithContext(ctx).
		Model(&IdempotencyRecord{}).
		Where("expires_at < ?", time.Now()).
		Count(&expiredCount).Error
	
	if err != nil {
		return nil, err
	}
	
	stats["expired"] = expiredCount
	
	return stats, nil
}

// GetRecordsByUser retrieves idempotency records for a specific user
func (r *Repository) GetRecordsByUser(ctx context.Context, userID string, limit int) ([]*IdempotencyRecord, error) {
	var records []*IdempotencyRecord
	err := r.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("created_at DESC").
		Limit(limit).
		Find(&records).Error
	return records, err
}

// GetRecordsByPath retrieves idempotency records for a specific path
func (r *Repository) GetRecordsByPath(ctx context.Context, path string, limit int) ([]*IdempotencyRecord, error) {
	var records []*IdempotencyRecord
	err := r.db.WithContext(ctx).
		Where("path = ?", path).
		Order("created_at DESC").
		Limit(limit).
		Find(&records).Error
	return records, err
}

// CountRecordsByUserAndTimeRange counts records for a user within a time range
func (r *Repository) CountRecordsByUserAndTimeRange(ctx context.Context, userID string, start, end time.Time) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&IdempotencyRecord{}).
		Where("user_id = ? AND created_at BETWEEN ? AND ?", userID, start, end).
		Count(&count).Error
	return count, err
}
