package cache

import (
	"fmt"
	"sync"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

var (
	redsyncInternal *redsync.Redsync
	mutexes         map[string]*redsync.Mutex
	mutexesLock     sync.Mutex
)

// Return a distributed lock for the given key (uses evictable cache storage)
func DLock(key string) *redsync.Mutex {
	mutexesLock.Lock()
	defer mutexesLock.Unlock()

	if mutexes == nil {
		mutexes = map[string]*redsync.Mutex{}
		pool := goredis.NewPool(RedisClientEvict())
		redsyncInternal = redsync.New(pool)
	}
	if maybe, ok := mutexes[key]; ok {
		return maybe
	}
	actualKey := fmt.Sprintf("dlock:%s", key)
	mut := redsyncInternal.NewMutex(actualKey)
	mutexes[key] = mut
	return mut
}