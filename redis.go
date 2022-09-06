package cache

import (
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
)

// TODO: generic cache interface in case we want to switch away from Redis

var (
	redisClientEvict     *redis.Client
	redisClientEvictLock sync.Mutex

	redisClientEvictReadonly     *redis.Client
	redisClientEvictReadonlyLock sync.Mutex

	redisClientNoevict     *redis.Client
	redisClientNoevictLock sync.Mutex

	redisClientNoevictReadonly     *redis.Client
	redisClientNoevictReadonlyLock sync.Mutex
)

func RedisClientEvict() *redis.Client {
	redisClientEvictLock.Lock()
	defer redisClientEvictLock.Unlock()
	if redisClientEvict == nil {
		redisClientEvict = redis.NewClient(&redis.Options{
			Addr: RedisAddressEvict(),
		})
	}
	return redisClientEvict
}

func RedisClientEvictReadonly() *redis.Client {
	redisClientEvictReadonlyLock.Lock()
	defer redisClientEvictReadonlyLock.Unlock()
	if redisClientEvictReadonly == nil {
		redisClientEvictReadonly = redis.NewClient(&redis.Options{
			Addr: RedisAddressEvictReadonly(),
		})
	}
	return redisClientEvictReadonly
}

func RedisClientNoevict() *redis.Client {
	redisClientNoevictLock.Lock()
	defer redisClientNoevictLock.Unlock()
	if redisClientNoevict == nil {
		redisClientNoevict = redis.NewClient(&redis.Options{
			Addr: RedisAddressNoevict(),
		})
	}
	return redisClientNoevict
}

func RedisClientNoevictReadonly() *redis.Client {
	redisClientNoevictReadonlyLock.Lock()
	defer redisClientNoevictReadonlyLock.Unlock()
	if redisClientNoevictReadonly == nil {
		redisClientNoevictReadonly = redis.NewClient(&redis.Options{
			Addr: RedisAddressNoevictReadonly(),
		})
	}
	return redisClientNoevictReadonly
}

func RedisAddressEvict() string {
	return os.Getenv("REDIS_DEDUP_ADDRESS")
}

func RedisAddressNoevict() string {
	maybeAddress := os.Getenv("REDIS_NO_EVICT_ADDRESS")
	if maybeAddress == "" {
		maybeAddress = os.Getenv("REDIS_NOEVICT_ADDRESS")
	}
	return maybeAddress
}

func RedisAddressEvictReadonly() string {
	return os.Getenv("REDIS_DEDUP_ADDRESS_READONLY")
}

func RedisAddressNoevictReadonly() string {
	return os.Getenv("REDIS_NOEVICT_ADDRESS_READONLY")
}
