package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v4"
	telemetry "github.com/loingtan/pkg/provider"
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
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("verification failed with status %d", resp.StatusCode)
	}

	var result struct {
		Verified bool `json:"verified"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, err
	}

	return result.Verified, nil
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
