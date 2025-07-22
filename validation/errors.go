package validation

import (
	"fmt"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ValidationError represents a single validation error
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Value   any    `json:"value,omitempty"`
	Code    string `json:"code,omitempty"`
}

// ValidationErrors represents multiple validation errors
type ValidationErrors []ValidationError

// Error implements the error interface
func (ve ValidationErrors) Error() string {
	if len(ve) == 0 {
		return "validation failed"
	}
	if len(ve) == 1 {
		return ve[0].Message
	}
	return fmt.Sprintf("validation failed with %d errors", len(ve))
}

// HTTPResponse represents the standardized HTTP error response format
type HTTPResponse struct {
	Success bool              `json:"success"`
	Error   string            `json:"error"`
	Details []ValidationError `json:"details,omitempty"`
	Code    string            `json:"code,omitempty"`
	TraceID string            `json:"trace_id,omitempty"`
}

// GRPCError represents standardized gRPC error information
type GRPCError struct {
	Code    codes.Code
	Message string
	Details []ValidationError
}

// ToGRPCStatus converts validation errors to gRPC status
func (ve ValidationErrors) ToGRPCStatus() *status.Status {
	if len(ve) == 0 {
		return status.New(codes.InvalidArgument, "validation failed")
	}
	
	message := ve.Error()
	return status.New(codes.InvalidArgument, message)
}

// ToHTTPResponse converts validation errors to HTTP response
func (ve ValidationErrors) ToHTTPResponse(traceID string) HTTPResponse {
	return HTTPResponse{
		Success: false,
		Error:   "Validation failed",
		Details: ve,
		Code:    "VALIDATION_ERROR",
		TraceID: traceID,
	}
}

// ErrorCodes defines standard error codes
const (
	ErrorCodeValidation     = "VALIDATION_ERROR"
	ErrorCodeRequired       = "REQUIRED_FIELD"
	ErrorCodeInvalidFormat  = "INVALID_FORMAT"
	ErrorCodeInvalidRange   = "INVALID_RANGE"
	ErrorCodeInvalidEnum    = "INVALID_ENUM"
	ErrorCodeBusinessRule   = "BUSINESS_RULE_VIOLATION"
	ErrorCodeDuplicate      = "DUPLICATE_VALUE"
	ErrorCodeNotFound       = "NOT_FOUND"
	ErrorCodeUnauthorized   = "UNAUTHORIZED"
	ErrorCodeForbidden      = "FORBIDDEN"
	ErrorCodeInternalError  = "INTERNAL_ERROR"
)

// HTTPStatusCodes maps error codes to HTTP status codes
var HTTPStatusCodes = map[string]int{
	ErrorCodeValidation:    http.StatusBadRequest,
	ErrorCodeRequired:      http.StatusBadRequest,
	ErrorCodeInvalidFormat: http.StatusBadRequest,
	ErrorCodeInvalidRange:  http.StatusBadRequest,
	ErrorCodeInvalidEnum:   http.StatusBadRequest,
	ErrorCodeBusinessRule:  http.StatusBadRequest,
	ErrorCodeDuplicate:     http.StatusConflict,
	ErrorCodeNotFound:      http.StatusNotFound,
	ErrorCodeUnauthorized:  http.StatusUnauthorized,
	ErrorCodeForbidden:     http.StatusForbidden,
	ErrorCodeInternalError: http.StatusInternalServerError,
}

// GRPCStatusCodes maps error codes to gRPC status codes
var GRPCStatusCodes = map[string]codes.Code{
	ErrorCodeValidation:    codes.InvalidArgument,
	ErrorCodeRequired:      codes.InvalidArgument,
	ErrorCodeInvalidFormat: codes.InvalidArgument,
	ErrorCodeInvalidRange:  codes.InvalidArgument,
	ErrorCodeInvalidEnum:   codes.InvalidArgument,
	ErrorCodeBusinessRule:  codes.FailedPrecondition,
	ErrorCodeDuplicate:     codes.AlreadyExists,
	ErrorCodeNotFound:      codes.NotFound,
	ErrorCodeUnauthorized:  codes.Unauthenticated,
	ErrorCodeForbidden:     codes.PermissionDenied,
	ErrorCodeInternalError: codes.Internal,
}

// NewValidationError creates a new validation error
func NewValidationError(field, message string, value any, code string) ValidationError {
	return ValidationError{
		Field:   field,
		Message: message,
		Value:   value,
		Code:    code,
	}
}

// NewRequiredFieldError creates a required field validation error
func NewRequiredFieldError(field string) ValidationError {
	return NewValidationError(field, fmt.Sprintf("Field '%s' is required", field), nil, ErrorCodeRequired)
}

// NewInvalidFormatError creates an invalid format validation error
func NewInvalidFormatError(field, expectedFormat string, value any) ValidationError {
	return NewValidationError(field, fmt.Sprintf("Field '%s' must be in %s format", field, expectedFormat), value, ErrorCodeInvalidFormat)
}

// NewInvalidRangeError creates an invalid range validation error
func NewInvalidRangeError(field string, min, max any, value any) ValidationError {
	return NewValidationError(field, fmt.Sprintf("Field '%s' must be between %v and %v", field, min, max), value, ErrorCodeInvalidRange)
}

// NewInvalidEnumError creates an invalid enum validation error
func NewInvalidEnumError(field string, validValues []string, value any) ValidationError {
	return NewValidationError(field, fmt.Sprintf("Field '%s' must be one of: %v", field, validValues), value, ErrorCodeInvalidEnum)
}

// NewBusinessRuleError creates a business rule validation error
func NewBusinessRuleError(field, message string, value any) ValidationError {
	return NewValidationError(field, message, value, ErrorCodeBusinessRule)
}

// AppError represents a standardized application error
type AppError struct {
	Code       string            `json:"code"`
	Message    string            `json:"message"`
	Details    []ValidationError `json:"details,omitempty"`
	HTTPStatus int               `json:"-"`
	GRPCCode   codes.Code        `json:"-"`
	TraceID    string            `json:"trace_id,omitempty"`
	Cause      error             `json:"-"`
}

// Error implements the error interface
func (e *AppError) Error() string {
	return e.Message
}

// Unwrap returns the underlying cause
func (e *AppError) Unwrap() error {
	return e.Cause
}

// NewAppError creates a new application error
func NewAppError(code, message string, cause error) *AppError {
	httpStatus, exists := HTTPStatusCodes[code]
	if !exists {
		httpStatus = http.StatusInternalServerError
	}
	
	grpcCode, exists := GRPCStatusCodes[code]
	if !exists {
		grpcCode = codes.Internal
	}
	
	return &AppError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
		GRPCCode:   grpcCode,
		Cause:      cause,
	}
}

// NewValidationAppError creates an application error from validation errors
func NewValidationAppError(errors ValidationErrors, traceID string) *AppError {
	return &AppError{
		Code:       ErrorCodeValidation,
		Message:    "Validation failed",
		Details:    errors,
		HTTPStatus: http.StatusBadRequest,
		GRPCCode:   codes.InvalidArgument,
		TraceID:    traceID,
	}
}

// ToHTTPResponse converts AppError to HTTP response
func (e *AppError) ToHTTPResponse() HTTPResponse {
	return HTTPResponse{
		Success: false,
		Error:   e.Message,
		Details: e.Details,
		Code:    e.Code,
		TraceID: e.TraceID,
	}
}

// ToGRPCStatus converts AppError to gRPC status
func (e *AppError) ToGRPCStatus() *status.Status {
	return status.New(e.GRPCCode, e.Message)
}
