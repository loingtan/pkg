package validation

import (
	"context"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HTTPMiddleware provides HTTP validation middleware
type HTTPMiddleware struct {
	validator *Validator
}

// NewHTTPMiddleware creates a new HTTP validation middleware
func NewHTTPMiddleware() *HTTPMiddleware {
	return &HTTPMiddleware{
		validator: NewValidator(),
	}
}

// ValidateJSON validates JSON request body
func (m *HTTPMiddleware) ValidateJSON(obj interface{}) gin.HandlerFunc {
	return func(c *gin.Context) {
		if err := c.ShouldBindJSON(obj); err != nil {
			m.handleValidationError(c, err)
			return
		}

		if validationErrors := m.validator.Validate(obj); len(validationErrors) > 0 {
			m.handleValidationErrors(c, validationErrors)
			return
		}

		c.Next()
	}
}

// ValidatePathParam validates path parameters
func (m *HTTPMiddleware) ValidatePathParam(paramName, validationType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		value := c.Param(paramName)
		if value == "" {
			err := NewRequiredFieldError(paramName)
			m.handleValidationErrors(c, ValidationErrors{err})
			return
		}

		if err := m.validatePathParamValue(paramName, value, validationType); err != nil {
			m.handleValidationErrors(c, ValidationErrors{*err})
			return
		}

		c.Next()
	}
}

// ValidateQueryParams validates query parameters
func (m *HTTPMiddleware) ValidateQueryParams() gin.HandlerFunc {
	return func(c *gin.Context) {
		var errors ValidationErrors

		// Validate page parameter
		if pageStr := c.Query("page"); pageStr != "" {
			page, err := strconv.Atoi(pageStr)
			if err != nil || page < 1 || page > 1000 {
				errors = append(errors, NewInvalidRangeError("page", 1, 1000, pageStr))
			}
		}

		// Validate page_size parameter
		if pageSizeStr := c.Query("page_size"); pageSizeStr != "" {
			pageSize, err := strconv.Atoi(pageSizeStr)
			if err != nil || pageSize < 1 || pageSize > 100 {
				errors = append(errors, NewInvalidRangeError("page_size", 1, 100, pageSizeStr))
			}
		}

		// Validate filter parameter
		if filter := c.Query("filter"); filter != "" && len(filter) > 100 {
			errors = append(errors, NewInvalidRangeError("filter", 0, 100, len(filter)))
		}

		if len(errors) > 0 {
			m.handleValidationErrors(c, errors)
			return
		}

		c.Next()
	}
}

// handleValidationError handles single validation error
func (m *HTTPMiddleware) handleValidationError(c *gin.Context, err error) {
	traceID := getTraceID(c)

	// Try to extract validation errors from binding error
	if validationErrors := extractBindingErrors(err); len(validationErrors) > 0 {
		m.handleValidationErrors(c, validationErrors)
		return
	}

	// Generic validation error
	appErr := NewValidationAppError(ValidationErrors{
		NewValidationError("request", err.Error(), nil, ErrorCodeValidation),
	}, traceID)

	c.JSON(appErr.HTTPStatus, appErr.ToHTTPResponse())
	c.Abort()
}

// handleValidationErrors handles multiple validation errors
func (m *HTTPMiddleware) handleValidationErrors(c *gin.Context, errors ValidationErrors) {
	traceID := getTraceID(c)
	appErr := NewValidationAppError(errors, traceID)

	c.JSON(appErr.HTTPStatus, appErr.ToHTTPResponse())
	c.Abort()
}

// validatePathParamValue validates a path parameter value
func (m *HTTPMiddleware) validatePathParamValue(paramName, value, validationType string) *ValidationError {
	switch validationType {
	case "uuid":
		if _, err := uuid.Parse(value); err != nil {
			ve := NewInvalidFormatError(paramName, "UUID", value)
			return &ve
		}
	case "coupon_code":
		if len(value) < 3 || len(value) > 50 {
			ve := NewInvalidRangeError(paramName, 3, 50, len(value))
			return &ve
		}
		matched, _ := regexp.MatchString("^[A-Z0-9_-]+$", value)
		if !matched {
			ve := NewInvalidFormatError(paramName, "coupon code", value)
			return &ve
		}
	case "positive_int":
		if val, err := strconv.Atoi(value); err != nil || val <= 0 {
			ve := NewBusinessRuleError(paramName, "must be a positive integer", value)
			return &ve
		}
	}

	return nil
}

// extractBindingErrors extracts validation errors from Gin binding errors
func extractBindingErrors(err error) ValidationErrors {
	// This would need to be implemented based on the specific binding error types
	// For now, return a generic error
	return ValidationErrors{
		NewValidationError("request", "Invalid request format", nil, ErrorCodeValidation),
	}
}

// getTraceID extracts trace ID from context
func getTraceID(c *gin.Context) string {
	if traceID, exists := c.Get("trace_id"); exists {
		if id, ok := traceID.(string); ok {
			return id
		}
	}
	if traceID := c.GetHeader("X-Trace-ID"); traceID != "" {
		return traceID
	}
	return uuid.New().String()
}

// GRPCInterceptor provides gRPC validation interceptor
type GRPCInterceptor struct {
	validator *Validator
}

// NewGRPCInterceptor creates a new gRPC validation interceptor
func NewGRPCInterceptor() *GRPCInterceptor {
	return &GRPCInterceptor{
		validator: NewValidator(),
	}
}

// UnaryServerInterceptor returns a gRPC unary server interceptor for validation
func (i *GRPCInterceptor) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Validate the request
		if validationErrors := i.validator.Validate(req); len(validationErrors) > 0 {
			return nil, validationErrors.ToGRPCStatus().Err()
		}

		// Call the handler
		resp, err := handler(ctx, req)

		// Handle any validation errors in the response
		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.InvalidArgument {
					// This is already a validation error, pass it through
					return nil, err
				}
			}
		}

		return resp, err
	}
}

// StreamServerInterceptor returns a gRPC stream server interceptor for validation
func (i *GRPCInterceptor) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// For stream interceptors, validation would typically be done per message
		// This is a basic implementation
		return handler(srv, ss)
	}
}

// ValidateGRPCRequest validates a gRPC request and returns appropriate error
func (i *GRPCInterceptor) ValidateGRPCRequest(req interface{}) error {
	if validationErrors := i.validator.Validate(req); len(validationErrors) > 0 {
		return validationErrors.ToGRPCStatus().Err()
	}
	return nil
}

// SanitizeString removes potentially harmful characters from string
func SanitizeString(s string) string {
	// Remove null bytes and control characters
	s = strings.ReplaceAll(s, "\x00", "")
	s = regexp.MustCompile(`[\x00-\x1f\x7f]`).ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

// ValidateStringLength validates string length with custom error messages
func ValidateStringLength(value, fieldName string, minLen, maxLen int) *ValidationError {
	if len(value) < minLen {
		ve := NewInvalidRangeError(fieldName, minLen, maxLen, len(value))
		return &ve
	}
	if len(value) > maxLen {
		ve := NewInvalidRangeError(fieldName, minLen, maxLen, len(value))
		return &ve
	}
	return nil
}

// IsValidUUID checks if a string is a valid UUID
func IsValidUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

// IsValidEmail checks if a string is a valid email
func IsValidEmail(email string) bool {
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	return emailRegex.MatchString(email)
}

// ErrorHandler creates a standardized error handler middleware
func ErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// Handle any errors that occurred during request processing
		if len(c.Errors) > 0 {
			err := c.Errors.Last().Err

			// Check if it's already an AppError
			if appErr, ok := err.(*AppError); ok {
				if !c.Writer.Written() {
					c.JSON(appErr.HTTPStatus, appErr.ToHTTPResponse())
				}
				return
			}

			// Create a generic internal error
			traceID := getTraceID(c)
			appErr := &AppError{
				Code:       ErrorCodeInternalError,
				Message:    "Internal server error",
				HTTPStatus: http.StatusInternalServerError,
				TraceID:    traceID,
				Cause:      err,
			}

			if !c.Writer.Written() {
				c.JSON(appErr.HTTPStatus, appErr.ToHTTPResponse())
			}
		}
	}
}
