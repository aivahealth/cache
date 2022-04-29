package cache

import (
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
)

// TODO: generic cache interface in case we want to switch away from Redis

var (
	redisClient     *redis.Client
	redisClientLock sync.Mutex

	redisClientNoevict     *redis.Client
	redisClientNoevictLock sync.Mutex
)

func RedisClientEvict() *redis.Client {
	redisClientLock.Lock()
	defer redisClientLock.Unlock()
	if redisClient == nil {
		redisClient = redis.NewClient(&redis.Options{
			Addr: os.Getenv("REDIS_DEDUP_ADDRESS"),
		})
	}
	return redisClient
}

func RedisClientNoevict() *redis.Client {
	redisClientNoevictLock.Lock()
	defer redisClientNoevictLock.Unlock()
	if redisClientNoevict == nil {
		maybeAddress := os.Getenv("REDIS_NO_EVICT_ADDRESS")
		if maybeAddress == "" {
			maybeAddress = os.Getenv("REDIS_NOEVICT_ADDRESS")
		}
		redisClientNoevict = redis.NewClient(&redis.Options{
			Addr: maybeAddress,
		})
	}
	return redisClientNoevict
}
