package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// Middleware provides idempotency functionality for HTTP requests
type Middleware struct {
	repo   *Repository
	config IdempotencyConfig
}

// NewMiddleware creates a new idempotency middleware
func NewMiddleware(db *gorm.DB, config IdempotencyConfig) *Middleware {
	return &Middleware{
		repo:   NewRepository(db),
		config: config,
	}
}

// Handler returns a Gin middleware function for idempotency
func (m *Middleware) Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if this request requires idempotency
		if !m.requiresIdempotency(c) {
			c.Next()
			return
		}

		// Get idempotency key from header
		idempotencyKey := c.GetHeader(m.config.HeaderName)
		if idempotencyKey == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Missing %s header for idempotent operation", m.config.HeaderName),
			})
			c.Abort()
			return
		}

		// Validate idempotency key format
		if !m.isValidIdempotencyKey(idempotencyKey) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Invalid idempotency key format",
			})
			c.Abort()
			return
		}

		// Get user ID from context
		userID := m.getUserID(c)

		// Check for existing record
		ctx := c.Request.Context()
		existingRecord, err := m.getExistingRecord(ctx, idempotencyKey, userID)
		if err != nil && err != gorm.ErrRecordNotFound {
			log.Printf("Error checking existing idempotency record: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Internal server error",
			})
			c.Abort()
			return
		}

		if existingRecord != nil {
			m.handleExistingRecord(c, existingRecord)
			return
		}

		// Create new idempotency record
		requestHash, err := m.calculateRequestHash(c)
		if err != nil {
			log.Printf("Error calculating request hash: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Internal server error",
			})
			c.Abort()
			return
		}

		record := NewIdempotencyRecord(
			idempotencyKey,
			requestHash,
			c.Request.Method,
			c.Request.URL.Path,
			userID,
			m.config.TTL,
		)

		// Store the record
		if err := m.repo.Store(ctx, record); err != nil {
			// Check if it's a duplicate key error (race condition)
			if m.isDuplicateKeyError(err) {
				// Another request with same key was processed, try to get it
				existingRecord, getErr := m.getExistingRecord(ctx, idempotencyKey, userID)
				if getErr == nil && existingRecord != nil {
					m.handleExistingRecord(c, existingRecord)
					return
				}
			}

			log.Printf("Error storing idempotency record: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Internal server error",
			})
			c.Abort()
			return
		}

		// Store record in context for completion handling
		c.Set("idempotency_record", record)

		// Wrap response writer to capture response
		writer := &responseWriter{
			ResponseWriter: c.Writer,
			body:           &bytes.Buffer{},
			headers:        make(map[string]string),
		}
		c.Writer = writer

		// Process the request
		c.Next()

		// Complete the idempotency record
		m.completeRecord(ctx, record, writer)
	}
}

// requiresIdempotency checks if the request requires idempotency
func (m *Middleware) requiresIdempotency(c *gin.Context) bool {
	// Check method
	methodMatch := false
	for _, method := range m.config.Methods {
		if c.Request.Method == method {
			methodMatch = true
			break
		}
	}
	if !methodMatch {
		return false
	}

	// Check path
	requestPath := c.Request.URL.Path
	for _, pattern := range m.config.Paths {
		if m.pathMatches(requestPath, pattern) {
			return true
		}
	}

	return false
}

// pathMatches checks if a request path matches a pattern (supports wildcards)
func (m *Middleware) pathMatches(requestPath, pattern string) bool {
	matched, err := filepath.Match(pattern, requestPath)
	if err != nil {
		// Fallback to exact match if pattern is invalid
		return requestPath == pattern
	}
	return matched
}

// isValidIdempotencyKey validates the format of an idempotency key
func (m *Middleware) isValidIdempotencyKey(key string) bool {
	// Basic validation: non-empty, reasonable length, alphanumeric + hyphens
	if len(key) == 0 || len(key) > 255 {
		return false
	}

	for _, r := range key {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}

	return true
}

// getUserID extracts user ID from the request context
func (m *Middleware) getUserID(c *gin.Context) string {
	if !m.config.IncludeUserID {
		return ""
	}

	// Try to get user from context (set by auth middleware)
	if user, exists := c.Get("user"); exists {
		if userCtx, ok := user.(interface{ GetID() string }); ok {
			return userCtx.GetID()
		}
		// Try different user context structures
		if userMap, ok := user.(map[string]interface{}); ok {
			if id, exists := userMap["id"]; exists {
				if idStr, ok := id.(string); ok {
					return idStr
				}
			}
		}
	}

	// Fallback to header
	return c.GetHeader("X-User-ID")
}

// getExistingRecord retrieves an existing idempotency record
func (m *Middleware) getExistingRecord(ctx context.Context, idempotencyKey, userID string) (*IdempotencyRecord, error) {
	if m.config.IncludeUserID && userID != "" {
		return m.repo.GetByIdempotencyKeyAndUser(ctx, idempotencyKey, userID)
	}
	return m.repo.GetByIdempotencyKey(ctx, idempotencyKey)
}

// handleExistingRecord handles a request with an existing idempotency record
func (m *Middleware) handleExistingRecord(c *gin.Context, record *IdempotencyRecord) {
	if record.IsExpired() {
		c.JSON(http.StatusConflict, gin.H{
			"error": "Idempotency key has expired",
		})
		c.Abort()
		return
	}

	if record.IsProcessing() {
		c.JSON(http.StatusConflict, gin.H{
			"error": "Request is already being processed",
		})
		c.Abort()
		return
	}

	if record.IsFailed() {
		c.JSON(http.StatusConflict, gin.H{
			"error": "Previous request with this idempotency key failed",
		})
		c.Abort()
		return
	}

	if record.IsCompleted() {
		// Return the cached response
		headers, _ := record.GetResponseHeaders()
		for key, value := range headers {
			c.Header(key, value)
		}

		c.Data(record.StatusCode, "application/json", record.ResponseBody)
		c.Abort()
		return
	}
}

// calculateRequestHash calculates a hash of the request body for comparison
func (m *Middleware) calculateRequestHash(c *gin.Context) (string, error) {
	if !m.config.IncludeRequestHash {
		return "", nil
	}

	// Read the request body
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return "", err
	}

	// Restore the request body for downstream handlers
	c.Request.Body = io.NopCloser(bytes.NewBuffer(body))

	// Calculate hash
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), nil
}

// completeRecord completes an idempotency record with the response
func (m *Middleware) completeRecord(ctx context.Context, record *IdempotencyRecord, writer *responseWriter) {
	if writer.statusCode >= 400 {
		// Request failed
		record.MarkAsFailed(fmt.Sprintf("HTTP %d", writer.statusCode))
	} else {
		// Request succeeded
		err := record.MarkAsCompleted(writer.statusCode, writer.body.Bytes(), writer.headers)
		if err != nil {
			log.Printf("Error marking idempotency record as completed: %v", err)
			return
		}
	}

	// Update the record
	if err := m.repo.Update(ctx, record); err != nil {
		log.Printf("Error updating idempotency record: %v", err)
	}
}

// isDuplicateKeyError checks if the error is a duplicate key constraint violation
func (m *Middleware) isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "duplicate") ||
		strings.Contains(errStr, "unique constraint")
}

// responseWriter wraps gin.ResponseWriter to capture response data
type responseWriter struct {
	gin.ResponseWriter
	body       *bytes.Buffer
	headers    map[string]string
	statusCode int
}

func (w *responseWriter) Write(data []byte) (int, error) {
	w.body.Write(data)
	return w.ResponseWriter.Write(data)
}

func (w *responseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

// CleanupExpiredRecords removes expired idempotency records
func (m *Middleware) CleanupExpiredRecords(ctx context.Context) error {
	return m.repo.DeleteExpired(ctx)
}

// CleanupStuckRequests marks stuck processing requests as failed
func (m *Middleware) CleanupStuckRequests(ctx context.Context, timeout time.Duration) error {
	return m.repo.MarkStuckRequestsAsFailed(ctx, timeout)
}
