package caches

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/redis/go-redis/v9"
)

var (
	_ Cache = &redisCache{}
	_ Cache = &memcacheCache{}
)

type (
	// SingleDataRecord represents a single data item that can be stored in cache.
	SingleDataRecord interface{}
	// MultipleDataRecord represents multiple data items that can be stored in cache.
	MultipleDataRecord []interface{}

	// Cache defines the interface for cache operations supporting both single and multiple data records.
	Cache interface {
		// SetSingle stores a single data record in the cache with the specified key.
		SetSingle(ctx context.Context, key string, value SingleDataRecord) (err error)
		// GetSingle retrieves a single data record from the cache using the specified key.
		GetSingle(ctx context.Context, key string) (result SingleDataRecord, err error)

		// SetMultiple stores multiple data records in the cache with the specified key.
		SetMultiple(ctx context.Context, key string, value MultipleDataRecord) (err error)
		// GetMultiple retrieves multiple data records from the cache using the specified key.
		GetMultiple(ctx context.Context, key string) (result MultipleDataRecord, err error)
	}

	// redisCache implements the Cache interface using Redis as the backend.
	redisCache struct {
		client *redis.Client
	}

	// memcacheCache implements the Cache interface using Memcache as the backend.
	memcacheCache struct {
		client *memcache.Client
	}

	// cacheStruct wraps a Cache implementation.
	cacheStruct struct {
		Cache
	}
)

// SetSingle stores a single data record in Memcache with the specified key.
// The value is JSON marshaled before storage.
// Returns an error if marshaling or storage fails.
func (m *memcacheCache) SetSingle(ctx context.Context, key string, value SingleDataRecord) (err error) {
	result, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return m.client.Set(&memcache.Item{
		Key:   key,
		Value: result,
	})
}

// GetSingle retrieves a single data record from Memcache using the specified key.
// Returns the raw byte data from the cache.
// Returns an error if the key is not found or retrieval fails.
func (m *memcacheCache) GetSingle(ctx context.Context, key string) (result SingleDataRecord, err error) {
	resp, err := m.client.Get(key)
	if err != nil {
		return nil, err
	}
	response := resp.Value

	return response, nil
}

// SetMultiple stores multiple data records in Memcache with the specified key.
// The value is JSON marshaled before storage.
// Returns an error if marshaling or storage fails.
func (m *memcacheCache) SetMultiple(ctx context.Context, key string, value MultipleDataRecord) (err error) {
	result, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return m.client.Set(&memcache.Item{
		Key:   key,
		Value: result,
	})
}

// GetMultiple retrieves multiple data records from Memcache using the specified key.
// The data is JSON unmarshaled into a MultipleDataRecord.
// Returns an error if the key is not found, retrieval fails, or unmarshaling fails.
func (m *memcacheCache) GetMultiple(ctx context.Context, key string) (result MultipleDataRecord, err error) {
	resp, err := m.client.Get(key)
	if err != nil {
		return nil, err
	}
	response := resp.Value
	err = json.Unmarshal(response, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// SetSingle stores a single data record in Redis with the specified key.
// The value is stored with no expiration (0 TTL).
// Returns an error if the storage operation fails.
func (r *redisCache) SetSingle(ctx context.Context, key string, value SingleDataRecord) (err error) {
	return r.client.Set(ctx, key, value, 0).Err()
}

// GetSingle retrieves a single data record from Redis using the specified key.
// The data is JSON unmarshaled into a SingleDataRecord.
// Returns an error if the key is not found, retrieval fails, or unmarshaling fails.
func (r *redisCache) GetSingle(ctx context.Context, key string) (result SingleDataRecord, err error) {
	resultStr, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(resultStr), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SetMultiple stores multiple data records in Redis with the specified key.
// The value is stored with no expiration (0 TTL).
// Returns an error if the storage operation fails.
func (r *redisCache) SetMultiple(ctx context.Context, key string, value MultipleDataRecord) (err error) {
	return r.client.Set(ctx, key, value, 0).Err()
}

// GetMultiple retrieves multiple data records from Redis using the specified key.
// The data is JSON unmarshaled into a MultipleDataRecord.
// Returns an error if the key is not found, retrieval fails, or unmarshaling fails.
func (r *redisCache) GetMultiple(ctx context.Context, key string) (result MultipleDataRecord, err error) {
	resultStr, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(resultStr), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// NewRedis creates a new Redis cache client with the specified host and port.
// It initializes a Redis client with default settings (no password, database 0).
// Returns a Cache interface implementation using Redis as the backend.
func NewRedis(
	host, port string,
) Cache {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Password: "",
		DB:       0,
	})
	return &redisCache{
		client: client,
	}
}

// NewMemcache creates a new Memcache client with the specified host and port.
// It initializes a Memcache client and returns a Cache interface implementation.
// Returns a Cache interface implementation using Memcache as the backend.
func NewMemcache(
	host, port string,
) Cache {
	client := memcache.New(fmt.Sprintf("%s:%s", host, port))
	return &memcacheCache{
		client: client,
	}
}

// NewCache wraps an existing Cache implementation in a cacheStruct.
// This function provides a way to wrap any Cache implementation.
// Returns the wrapped Cache implementation.
func NewCache(cache Cache) Cache {
	return cacheStruct{
		cache,
	}
}
