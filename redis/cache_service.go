package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrCacheMiss    = errors.New("cache miss")
	ErrInvalidValue = errors.New("invalid cached value")
)

type CacheService struct {
	client     *redis.Client
	keyPrefix  string
	defaultTTL time.Duration
}

func NewCacheService(client *redis.Client, serviceName string, defaultTTL time.Duration) *CacheService {
	keyPrefix := fmt.Sprintf("service:%s:", serviceName)
	return &CacheService{
		client:     client,
		keyPrefix:  keyPrefix,
		defaultTTL: defaultTTL,
	}
}
func (c *CacheService) prefixKey(key string) string {
	return c.keyPrefix + key
}

func (c *CacheService) Set(ctx context.Context, key string, value interface{}) error {
	return c.SetWithTTL(ctx, key, value, c.defaultTTL)
}

func (c *CacheService) SetWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	prefixedKey := c.prefixKey(key)
	return c.client.Set(ctx, prefixedKey, data, ttl).Err()
}
func (c *CacheService) Get(ctx context.Context, key string, dest interface{}) error {
	prefixedKey := c.prefixKey(key)
	data, err := c.client.Get(ctx, prefixedKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return ErrCacheMiss
		}
		return err
	}

	if err := json.Unmarshal(data, dest); err != nil {
		return ErrInvalidValue
	}

	return nil
}

func (c *CacheService) Delete(ctx context.Context, key string) error {
	prefixedKey := c.prefixKey(key)
	return c.client.Del(ctx, prefixedKey).Err()
}

func (c *CacheService) DeleteWithPrefix(ctx context.Context, prefix string) error {
	prefixedKey := c.prefixKey(prefix) + "*"
	iter := c.client.Scan(ctx, 0, prefixedKey, 0).Iterator()
	var keys []string

	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return err
	}
	if len(keys) > 0 {
		return c.client.Del(ctx, keys...).Err()
	}

	return nil
}

func (c *CacheService) SetHash(ctx context.Context, key string, values map[string]interface{}) error {
	if len(values) == 0 {
		return nil
	}
	stringValues := make(map[string]interface{}, len(values))
	for k, v := range values {
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to marshal hash value: %w", err)
		}
		stringValues[k] = string(data)
	}

	prefixedKey := c.prefixKey(key)
	pipeline := c.client.Pipeline()
	pipeline.HSet(ctx, prefixedKey, stringValues)
	pipeline.Expire(ctx, prefixedKey, c.defaultTTL)
	_, err := pipeline.Exec(ctx)
	return err
}

// GetHash retrieves a field from a hash
func (c *CacheService) GetHash(ctx context.Context, key, field string, dest interface{}) error {
	prefixedKey := c.prefixKey(key)
	data, err := c.client.HGet(ctx, prefixedKey, field).Bytes()
	if err != nil {
		if err == redis.Nil {
			return ErrCacheMiss
		}
		return err
	}

	if err := json.Unmarshal(data, dest); err != nil {
		return ErrInvalidValue
	}

	return nil
}

func (c *CacheService) GetAllHash(ctx context.Context, key string) (map[string]string, error) {
	prefixedKey := c.prefixKey(key)
	return c.client.HGetAll(ctx, prefixedKey).Result()
}

func (c *CacheService) Exists(ctx context.Context, key string) (bool, error) {
	prefixedKey := c.prefixKey(key)
	result, err := c.client.Exists(ctx, prefixedKey).Result()
	return result > 0, err
}
func (c *CacheService) Expire(ctx context.Context, key string, ttl time.Duration) error {
	prefixedKey := c.prefixKey(key)
	return c.client.Expire(ctx, prefixedKey, ttl).Err()
}

func (c *CacheService) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) (bool, error) {
	prefixedKey := c.prefixKey(key)
	return c.client.SetNX(ctx, prefixedKey, value, ttl).Result()
}

func (c *CacheService) Del(ctx context.Context, key string) error {
	prefixedKey := c.prefixKey(key)
	return c.client.Del(ctx, prefixedKey).Err()
}

func (c *CacheService) HIncrBy(ctx context.Context, key, field string, incr int64) error {
	prefixedKey := c.prefixKey(key)
	return c.client.HIncrBy(ctx, prefixedKey, field, incr).Err()
}

func (c *CacheService) RedisClient() *redis.Client {
	return c.client
}
