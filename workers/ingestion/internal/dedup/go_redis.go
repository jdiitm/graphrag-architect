package dedup

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type goRedisWrapper struct {
	client *redis.Client
}

func newGoRedisClient(url string) RedisClient {
	opts, err := redis.ParseURL(url)
	if err != nil {
		opts = &redis.Options{Addr: url}
	}
	return &goRedisWrapper{client: redis.NewClient(opts)}
}

func (w *goRedisWrapper) SetNX(key string, ttl time.Duration) (bool, error) {
	result, err := w.client.SetArgs(context.Background(), key, "1", redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	}).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return result == "OK", nil
}

func (w *goRedisWrapper) Exists(key string) (bool, error) {
	n, err := w.client.Exists(context.Background(), key).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
