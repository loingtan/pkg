package validation

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

type Validator struct {
	validator *validator.Validate
}

func NewValidator() *Validator {
	v := validator.New()

	v.RegisterValidation("uuid", validateUUID)
	v.RegisterValidation("coupon_code", validateCouponCode)
	v.RegisterValidation("discount_type", validateDiscountType)
	v.RegisterValidation("usage_type", validateUsageType)
	v.RegisterValidation("future_time", validateFutureTime)
	v.RegisterValidation("end_after_start", validateEndAfterStart)
	v.RegisterValidation("percentage_range", validatePercentageRange)
	v.RegisterValidation("positive_amount", validatePositiveAmount)
	v.RegisterValidation("discount_value", validateDiscountValue)
	v.RegisterValidation("phone_number", validatePhoneNumber)
	v.RegisterValidation("strong_password", validateStrongPassword)
	v.RegisterValidation("order_status", validateOrderStatus)
	v.RegisterValidation("payment_method", validatePaymentMethod)
	v.RegisterValidation("notification_type", validateNotificationType)

	return &Validator{validator: v}
}

func (v *Validator) Validate(s interface{}) ValidationErrors {
	err := v.validator.Struct(s)
	if err == nil {
		return nil
	}

	var validationErrors ValidationErrors
	if ve, ok := err.(validator.ValidationErrors); ok {
		for _, fe := range ve {
			validationErrors = append(validationErrors, convertFieldError(fe))
		}
	}

	return validationErrors
}

func (v *Validator) ValidateVar(field interface{}, tag string) error {
	return v.validator.Var(field, tag)
}

func validateUUID(fl validator.FieldLevel) bool {
	value := fl.Field().String()
	_, err := uuid.Parse(value)
	return err == nil
}

func validateCouponCode(fl validator.FieldLevel) bool {
	code := fl.Field().String()
	if len(code) < 3 || len(code) > 50 {
		return false
	}
	matched, _ := regexp.MatchString("^[A-Z0-9_-]+$", code)
	if !matched {
		return false
	}

	return !strings.HasPrefix(code, "_") && !strings.HasPrefix(code, "-") &&
		!strings.HasSuffix(code, "_") && !strings.HasSuffix(code, "-")
}

func validateDiscountType(fl validator.FieldLevel) bool {
	discountType := fl.Field().String()
	validTypes := []string{"PERCENT", "FIXED_PRICE", "FIXED"}
	return slices.Contains(validTypes, discountType)
}

func validateUsageType(fl validator.FieldLevel) bool {
	usageType := fl.Field().String()
	validTypes := []string{"MANUAL", "AUTO"}
	return slices.Contains(validTypes, usageType)
}

func validateFutureTime(fl validator.FieldLevel) bool {
	t, ok := fl.Field().Interface().(time.Time)
	if !ok {
		return false
	}
	return t.After(time.Now().Add(-time.Minute))
}

func validateEndAfterStart(fl validator.FieldLevel) bool {
	endTime, ok := fl.Field().Interface().(time.Time)
	if !ok {
		return false
	}

	parent := fl.Parent()
	if !parent.IsValid() {
		return false
	}

	startTimeField := parent.FieldByName("StartTime")
	if !startTimeField.IsValid() {
		return false
	}

	startTime, ok := startTimeField.Interface().(time.Time)
	if !ok {
		return false
	}

	return endTime.After(startTime)
}

func validatePercentageRange(fl validator.FieldLevel) bool {
	value := fl.Field().Float()
	return value >= 0 && value <= 100
}

func validatePositiveAmount(fl validator.FieldLevel) bool {
	value := fl.Field().Float()
	return value > 0
}

func validateDiscountValue(fl validator.FieldLevel) bool {
	discountVal := fl.Field().Float()

	parent := fl.Parent()
	if !parent.IsValid() {
		return false
	}

	discountTypeField := parent.FieldByName("DiscountType")
	if !discountTypeField.IsValid() {
		return false
	}

	discountType := discountTypeField.String()

	switch discountType {
	case "PERCENT":
		return discountVal >= 0 && discountVal <= 100
	case "FIXED", "FIXED_PRICE":
		return discountVal >= 1000
	default:
		return false
	}
}

func validatePhoneNumber(fl validator.FieldLevel) bool {
	phone := fl.Field().String()

	matched, _ := regexp.MatchString(`^\+?[1-9]\d{1,14}$`, phone)
	return matched
}

func validateStrongPassword(fl validator.FieldLevel) bool {
	password := fl.Field().String()
	if len(password) < 8 {
		return false
	}

	hasUpper, _ := regexp.MatchString(`[A-Z]`, password)
	hasLower, _ := regexp.MatchString(`[a-z]`, password)
	hasDigit, _ := regexp.MatchString(`\d`, password)
	hasSpecial, _ := regexp.MatchString(`[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]`, password)

	return hasUpper && hasLower && hasDigit && hasSpecial
}

func validateOrderStatus(fl validator.FieldLevel) bool {
	status := fl.Field().String()
	validStatuses := []string{"PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"}
	for _, validStatus := range validStatuses {
		if status == validStatus {
			return true
		}
	}
	return false
}

func validatePaymentMethod(fl validator.FieldLevel) bool {
	method := fl.Field().String()
	validMethods := []string{"CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "CASH_ON_DELIVERY", "DIGITAL_WALLET"}
	for _, validMethod := range validMethods {
		if method == validMethod {
			return true
		}
	}
	return false
}

func validateNotificationType(fl validator.FieldLevel) bool {
	notificationType := fl.Field().String()
	validTypes := []string{"EMAIL", "SMS", "PUSH", "IN_APP"}
	for _, validType := range validTypes {
		if notificationType == validType {
			return true
		}
	}
	return false
}

func convertFieldError(fe validator.FieldError) ValidationError {
	field := strings.ToLower(fe.Field())
	value := fe.Value()

	switch fe.Tag() {
	case "required":
		return NewRequiredFieldError(field)
	case "email":
		return NewInvalidFormatError(field, "email", value)
	case "uuid":
		return NewInvalidFormatError(field, "UUID", value)
	case "min":
		return NewInvalidRangeError(field, fe.Param(), "∞", value)
	case "max":
		return NewInvalidRangeError(field, "0", fe.Param(), value)
	case "gte":
		return NewInvalidRangeError(field, fe.Param(), "∞", value)
	case "gt":
		return NewInvalidRangeError(field, fmt.Sprintf(">%s", fe.Param()), "∞", value)
	case "lte":
		return NewInvalidRangeError(field, "0", fe.Param(), value)
	case "lt":
		return NewInvalidRangeError(field, "0", fmt.Sprintf("<%s", fe.Param()), value)
	case "oneof":
		validValues := strings.Split(fe.Param(), " ")
		return NewInvalidEnumError(field, validValues, value)
	case "coupon_code":
		return NewInvalidFormatError(field, "coupon code (3-50 chars, A-Z0-9_- only)", value)
	case "discount_type":
		return NewInvalidEnumError(field, []string{"PERCENT", "FIXED_PRICE", "FIXED"}, value)
	case "usage_type":
		return NewInvalidEnumError(field, []string{"MANUAL", "AUTO"}, value)
	case "future_time":
		return NewBusinessRuleError(field, "must be a future date and time", value)
	case "end_after_start":
		return NewBusinessRuleError(field, "must be after the start time", value)
	case "percentage_range":
		return NewInvalidRangeError(field, "0", "100", value)
	case "positive_amount":
		return NewBusinessRuleError(field, "must be a positive amount", value)
	case "discount_value":
		return NewBusinessRuleError(field, "invalid discount value for the selected discount type", value)
	case "phone_number":
		return NewInvalidFormatError(field, "phone number", value)
	case "strong_password":
		return NewBusinessRuleError(field, "must be at least 8 characters with uppercase, lowercase, digit, and special character", value)
	case "order_status":
		return NewInvalidEnumError(field, []string{"PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"}, value)
	case "payment_method":
		return NewInvalidEnumError(field, []string{"CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "CASH_ON_DELIVERY", "DIGITAL_WALLET"}, value)
	case "notification_type":
		return NewInvalidEnumError(field, []string{"EMAIL", "SMS", "PUSH", "IN_APP"}, value)
	default:
		return NewValidationError(field, fmt.Sprintf("Field '%s' is invalid", field), value, ErrorCodeValidation)
	}
}
