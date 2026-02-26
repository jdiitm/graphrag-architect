package dedup

import (
	"log"
	"time"
)

func NewStoreFromEnv(storeType, redisURL string, capacity int, ttlHours int) Store {
	switch storeType {
	case "redis":
		if redisURL == "" {
			log.Fatal("DEDUP_REDIS_URL required when DEDUP_STORE_TYPE=redis")
		}
		ttl := time.Duration(ttlHours) * time.Hour
		if ttl <= 0 {
			ttl = 7 * 24 * time.Hour
		}
		client := newGoRedisClient(redisURL)
		log.Printf("dedup: using redis backend url=%s ttl=%v", redisURL, ttl)
		return NewRedisStore(client, "dedup:", ttl)
	case "memory":
		if capacity <= 0 {
			capacity = 10000
		}
		log.Printf("dedup: using in-memory LRU backend capacity=%d", capacity)
		return NewLRUStore(capacity)
	default:
		log.Println("dedup: using noop backend (deduplication disabled)")
		return NoopStore{}
	}
}
