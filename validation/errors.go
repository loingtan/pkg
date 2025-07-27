package validation

import (
	"fmt"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Value   any    `json:"value,omitempty"`
	Code    string `json:"code,omitempty"`
}

type ValidationErrors []ValidationError

func (ve ValidationErrors) Error() string {
	if len(ve) == 0 {
		return "validation failed"
	}
	if len(ve) == 1 {
		return ve[0].Message
	}
	return fmt.Sprintf("validation failed with %d errors", len(ve))
}

type HTTPResponse struct {
	Success bool              `json:"success"`
	Error   string            `json:"error"`
	Details []ValidationError `json:"details,omitempty"`
	Code    string            `json:"code,omitempty"`
	TraceID string            `json:"trace_id,omitempty"`
}

type GRPCError struct {
	Code    codes.Code
	Message string
	Details []ValidationError
}

func (ve ValidationErrors) ToGRPCStatus() *status.Status {
	if len(ve) == 0 {
		return status.New(codes.InvalidArgument, "validation failed")
	}

	message := ve.Error()
	return status.New(codes.InvalidArgument, message)
}

func (ve ValidationErrors) ToHTTPResponse(traceID string) HTTPResponse {
	return HTTPResponse{
		Success: false,
		Error:   "Validation failed",
		Details: ve,
		Code:    "VALIDATION_ERROR",
		TraceID: traceID,
	}
}

const (
	ErrorCodeValidation    = "VALIDATION_ERROR"
	ErrorCodeRequired      = "REQUIRED_FIELD"
	ErrorCodeInvalidFormat = "INVALID_FORMAT"
	ErrorCodeInvalidRange  = "INVALID_RANGE"
	ErrorCodeInvalidEnum   = "INVALID_ENUM"
	ErrorCodeBusinessRule  = "BUSINESS_RULE_VIOLATION"
	ErrorCodeDuplicate     = "DUPLICATE_VALUE"
	ErrorCodeNotFound      = "NOT_FOUND"
	ErrorCodeUnauthorized  = "UNAUTHORIZED"
	ErrorCodeForbidden     = "FORBIDDEN"
	ErrorCodeInternalError = "INTERNAL_ERROR"
)

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

func NewValidationError(field, message string, value any, code string) ValidationError {
	return ValidationError{
		Field:   field,
		Message: message,
		Value:   value,
		Code:    code,
	}
}

func NewRequiredFieldError(field string) ValidationError {
	return NewValidationError(field, fmt.Sprintf("Field '%s' is required", field), nil, ErrorCodeRequired)
}

func NewInvalidFormatError(field, expectedFormat string, value any) ValidationError {
	return NewValidationError(field, fmt.Sprintf("Field '%s' must be in %s format", field, expectedFormat), value, ErrorCodeInvalidFormat)
}

func NewInvalidRangeError(field string, min, max any, value any) ValidationError {
	return NewValidationError(field, fmt.Sprintf("Field '%s' must be between %v and %v", field, min, max), value, ErrorCodeInvalidRange)
}

func NewInvalidEnumError(field string, validValues []string, value any) ValidationError {
	return NewValidationError(field, fmt.Sprintf("Field '%s' must be one of: %v", field, validValues), value, ErrorCodeInvalidEnum)
}

func NewBusinessRuleError(field, message string, value any) ValidationError {
	return NewValidationError(field, message, value, ErrorCodeBusinessRule)
}

type AppError struct {
	Code       string            `json:"code"`
	Message    string            `json:"message"`
	Details    []ValidationError `json:"details,omitempty"`
	HTTPStatus int               `json:"-"`
	GRPCCode   codes.Code        `json:"-"`
	TraceID    string            `json:"trace_id,omitempty"`
	Cause      error             `json:"-"`
}

func (e *AppError) Error() string {
	return e.Message
}

func (e *AppError) Unwrap() error {
	return e.Cause
}

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

func (e *AppError) ToHTTPResponse() HTTPResponse {
	return HTTPResponse{
		Success: false,
		Error:   e.Message,
		Details: e.Details,
		Code:    e.Code,
		TraceID: e.TraceID,
	}
}

func (e *AppError) ToGRPCStatus() *status.Status {
	return status.New(e.GRPCCode, e.Message)
}
