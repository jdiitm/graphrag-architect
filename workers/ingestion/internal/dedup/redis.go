package dedup

import (
	"log"
	"time"
)

type RedisClient interface {
	SetNX(key string, ttl time.Duration) (bool, error)
	Exists(key string) (bool, error)
}

type RedisStore struct {
	client RedisClient
	prefix string
	ttl    time.Duration
}

func NewRedisStore(client RedisClient, prefix string, ttl time.Duration) *RedisStore {
	return &RedisStore{client: client, prefix: prefix, ttl: ttl}
}

func (r *RedisStore) IsDuplicate(key string) bool {
	if key == "" {
		return false
	}
	exists, err := r.client.Exists(r.prefix + key)
	if err != nil {
		log.Printf("dedup redis exists error key=%s: %v", key, err)
		return false
	}
	return exists
}

func (r *RedisStore) Mark(key string) {
	if key == "" {
		return
	}
	_, err := r.client.SetNX(r.prefix+key, r.ttl)
	if err != nil {
		log.Printf("dedup redis setnx error key=%s: %v", key, err)
	}
}

