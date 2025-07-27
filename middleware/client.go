package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v4"
	telemetry "github.com/loingtan/pkg/provider"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type ServiceClientVerifier interface {
	VerifyServiceClient(ctx context.Context, clientID, clientKey string) (bool, error)
	VerifyServiceToken(ctx context.Context, token string) (bool, error)
}

type httpServiceAuthClient struct {
	baseURL   string
	client    *http.Client
	jwtSecret string
}

func NewServiceAuthClient(baseURL, jwtSecret string) ServiceClientVerifier {
	return &httpServiceAuthClient{
		baseURL:   baseURL,
		client:    telemetry.HTTPClient(),
		jwtSecret: jwtSecret,
	}
}

func (c *httpServiceAuthClient) VerifyServiceClient(ctx context.Context, clientID, clientKey string) (bool, error) {
	payload := map[string]string{
		"client_id":  clientID,
		"client_key": clientKey,
	}
	payloadBytes, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/api/v1/services/verify", c.baseURL), strings.NewReader(string(payloadBytes)))
	if err != nil {
		return false, err
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("Error: %v", err)
		log.Printf("Service client verification failed: status=%d, body=%s", resp.StatusCode, string(bodyBytes))
		return false, fmt.Errorf("verification failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var ginResponse struct {
		Success bool `json:"success"`
		Data    struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			ClientID string `json:"client_id"`
			Active   bool   `json:"active"`
		} `json:"data"`
		Error   string `json:"error,omitempty"`
		TraceID string `json:"trace_id,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&ginResponse); err != nil {
		return false, fmt.Errorf("failed to decode response: %w", err)
	}

	if !ginResponse.Success {
		return false, fmt.Errorf("service verification failed: %s", ginResponse.Error)
	}

	return ginResponse.Data.Active, nil
}

func (c *httpServiceAuthClient) VerifyServiceToken(ctx context.Context, tokenString string) (bool, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(c.jwtSecret), nil
	})

	if err != nil {
		return false, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		if _, ok := claims["service_id"]; !ok {
			return false, errors.New("missing service_id claim")
		}
		if _, ok := claims["client_id"]; !ok {
			return false, errors.New("missing client_id claim")
		}
		return true, nil
	}
	return false, errors.New("invalid token claims")
}
