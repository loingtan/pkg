package idempotency

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// IdempotencyRecord represents a stored idempotency record
type IdempotencyRecord struct {
	ID             string          `json:"id" gorm:"primaryKey;type:varchar(36)" db:"id"`
	IdempotencyKey string          `json:"idempotency_key" gorm:"type:varchar(255);not null;uniqueIndex" db:"idempotency_key"`
	RequestHash    string          `json:"request_hash" gorm:"type:varchar(64);not null" db:"request_hash"`
	Method         string          `json:"method" gorm:"type:varchar(10);not null" db:"method"`
	Path           string          `json:"path" gorm:"type:varchar(500);not null" db:"path"`
	UserID         string          `json:"user_id" gorm:"type:varchar(36);index" db:"user_id"`
	Status         RequestStatus   `json:"status" gorm:"type:varchar(20);not null;default:'PROCESSING';index" db:"status"`
	StatusCode     int             `json:"status_code" gorm:"default:0" db:"status_code"`
	ResponseBody   json.RawMessage `json:"response_body" gorm:"type:json" db:"response_body"`
	ResponseHeaders json.RawMessage `json:"response_headers" gorm:"type:json" db:"response_headers"`
	ErrorMessage   string          `json:"error_message" gorm:"type:text" db:"error_message"`
	CreatedAt      time.Time       `json:"created_at" gorm:"autoCreateTime" db:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at" gorm:"autoUpdateTime" db:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at" gorm:"index" db:"completed_at"`
	ExpiresAt      time.Time       `json:"expires_at" gorm:"index" db:"expires_at"`
}

func (IdempotencyRecord) TableName() string {
	return "idempotency_records"
}

// RequestStatus represents the status of an idempotent request
type RequestStatus string

const (
	RequestStatusProcessing RequestStatus = "PROCESSING"
	RequestStatusCompleted  RequestStatus = "COMPLETED"
	RequestStatusFailed     RequestStatus = "FAILED"
)

// NewIdempotencyRecord creates a new idempotency record
func NewIdempotencyRecord(idempotencyKey, requestHash, method, path, userID string, ttl time.Duration) *IdempotencyRecord {
	now := time.Now()
	return &IdempotencyRecord{
		ID:             uuid.New().String(),
		IdempotencyKey: idempotencyKey,
		RequestHash:    requestHash,
		Method:         method,
		Path:           path,
		UserID:         userID,
		Status:         RequestStatusProcessing,
		CreatedAt:      now,
		UpdatedAt:      now,
		ExpiresAt:      now.Add(ttl),
	}
}

// MarkAsCompleted marks the record as completed with response data
func (r *IdempotencyRecord) MarkAsCompleted(statusCode int, responseBody []byte, responseHeaders map[string]string) error {
	now := time.Now()
	r.Status = RequestStatusCompleted
	r.StatusCode = statusCode
	r.ResponseBody = responseBody
	r.CompletedAt = &now
	r.UpdatedAt = now

	if responseHeaders != nil {
		headersJSON, err := json.Marshal(responseHeaders)
		if err != nil {
			return err
		}
		r.ResponseHeaders = headersJSON
	}

	return nil
}

// MarkAsFailed marks the record as failed with error message
func (r *IdempotencyRecord) MarkAsFailed(errorMessage string) {
	now := time.Now()
	r.Status = RequestStatusFailed
	r.ErrorMessage = errorMessage
	r.CompletedAt = &now
	r.UpdatedAt = now
}

// IsExpired returns true if the record has expired
func (r *IdempotencyRecord) IsExpired() bool {
	return time.Now().After(r.ExpiresAt)
}

// IsCompleted returns true if the request was completed successfully
func (r *IdempotencyRecord) IsCompleted() bool {
	return r.Status == RequestStatusCompleted
}

// IsFailed returns true if the request failed
func (r *IdempotencyRecord) IsFailed() bool {
	return r.Status == RequestStatusFailed
}

// IsProcessing returns true if the request is still being processed
func (r *IdempotencyRecord) IsProcessing() bool {
	return r.Status == RequestStatusProcessing
}

// GetResponseHeaders unmarshals the response headers
func (r *IdempotencyRecord) GetResponseHeaders() (map[string]string, error) {
	if len(r.ResponseHeaders) == 0 {
		return make(map[string]string), nil
	}
	
	var headers map[string]string
	err := json.Unmarshal(r.ResponseHeaders, &headers)
	return headers, err
}

// IdempotencyConfig holds configuration for idempotency middleware
type IdempotencyConfig struct {
	// TTL for idempotency records
	TTL time.Duration
	
	// Header name for idempotency key
	HeaderName string
	
	// Methods that require idempotency
	Methods []string
	
	// Paths that require idempotency (can use wildcards)
	Paths []string
	
	// Whether to include user ID in uniqueness check
	IncludeUserID bool
	
	// Whether to include request body hash in uniqueness check
	IncludeRequestHash bool
}

// DefaultIdempotencyConfig returns default configuration
func DefaultIdempotencyConfig() IdempotencyConfig {
	return IdempotencyConfig{
		TTL:                24 * time.Hour,
		HeaderName:         "Idempotency-Key",
		Methods:            []string{"POST", "PUT"},
		Paths:              []string{"/api/v1/orders", "/api/v1/coupons"},
		IncludeUserID:      true,
		IncludeRequestHash: true,
	}
}
