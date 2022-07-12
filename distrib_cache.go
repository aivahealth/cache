package cache

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aquilax/truncate"
	"go.uber.org/zap"
)

//
//
// Helpers for managing shared resources in a distributed cache.
//
// Uses evictable storage.
//

const (
	dcacheRedisField_Expiration = "expiration"
	dcacheRedisField_RawBody    = "raw_body"
)

type DEntry struct {
	RawBody []byte
	GoBody  interface{}

	expiration time.Time
}

func (de *DEntry) Valid() bool {
	return de != nil && de.expiration.After(time.Now())
}

func (de *DEntry) IsValidAfter(t time.Time) bool {
	return de != nil && de.expiration.After(t)
}

var (
	dEntryLocalCache     = map[string]*DEntry{}
	dEntryLocalCacheLock sync.Mutex
)

func redisEntryKey(keyIn string) string {
	return fmt.Sprintf("dcache_entry:%s", keyIn)
}

func DLoad(ctx context.Context, keyIn string) (*DEntry, error) {
	truncKey := truncate.Truncate(keyIn, 30, "...", truncate.PositionMiddle)

	// Check local
	dEntryLocalCacheLock.Lock()
	localDe, ok := dEntryLocalCache[keyIn]
	dEntryLocalCacheLock.Unlock()
	if ok && localDe.Valid() {
		logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: local cache hit (exp: %v)", truncKey, time.Until(localDe.expiration)))
		return localDe, nil
	}

	redisKey := redisEntryKey(keyIn)

	// DLock!
	dlock := DLock(redisKey)
	if err := dlock.LockContext(ctx); err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DLoad failed: dlock acquisition failed", truncKey), zap.Error(err))
		return nil, err
	}
	defer func() {
		dlock.UnlockContext(ctx)
		logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: dlock released", truncKey))
	}()
	logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: acquired dlock", truncKey))

	// Get from shared storage
	exists, err := RedisClientEvict().Exists(ctx, redisKey).Result()
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DLoad failed: redis EXISTS failed", truncKey), zap.Error(err))
		return nil, err
	}
	if exists == 0 {
		// Nothing cached
		logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: data not found in shared storage", truncKey))
		return nil, nil
	}
	entryRedis, err := RedisClientEvict().HGetAll(ctx, redisKey).Result()
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DLoad failed: redis HGETALL failed", truncKey), zap.Error(err))
		return nil, err
	}

	expirationUnixString, ok := entryRedis[dcacheRedisField_Expiration]
	if !ok {
		err := fmt.Errorf("Malformed distributed cache entry structure in redis storage (missing %q field)", dcacheRedisField_Expiration)
		logger().Error(fmt.Sprintf("[dcache:%s] DLoad failed", truncKey), zap.Error(err))
		return nil, err
	}
	expirationUnix, err := strconv.ParseInt(expirationUnixString, 10, 64)
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DLoad failed", truncKey), zap.Error(err))
		return nil, err
	}
	expiration := time.Unix(expirationUnix, 0)

	// Short-circuit if expired
	if expiration.Before(time.Now()) {
		logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: data found in shared storage, but it's expired", truncKey))
		return nil, nil
	}

	bodyString, ok := entryRedis[dcacheRedisField_RawBody]
	if !ok {
		err := fmt.Errorf("Malformed distributed cache entry structure in redis storage (missing %q field)", dcacheRedisField_RawBody)
		logger().Error(fmt.Sprintf("[dcache:%s] DLoad failed", truncKey), zap.Error(err))
		return nil, err
	}

	// Cache the loaded record locally for quicker future lookups
	newEntry := &DEntry{
		expiration: expiration,
		RawBody:    []byte(bodyString),
	}
	dEntryLocalCacheLock.Lock()
	dEntryLocalCache[keyIn] = newEntry
	dEntryLocalCacheLock.Unlock()

	logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: data loaded from shared storage, saved to local cache (exp: %v)", truncKey, time.Until(newEntry.expiration)))

	return newEntry, nil
}

func DSave(ctx context.Context, keyIn string, body []byte, expiration time.Time) error {
	truncKey := truncate.Truncate(keyIn, 30, "...", truncate.PositionMiddle)

	redisKey := redisEntryKey(keyIn)

	dlock := DLock(redisKey)
	if err := dlock.LockContext(ctx); err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DSave failed: dlock acquisition failed", truncKey), zap.Error(err))
		return err
	}
	defer func() {
		dlock.UnlockContext(ctx)
		logger().Debug(fmt.Sprintf("[dcache:%s] DSave: dlock released", truncKey))
	}()
	logger().Debug(fmt.Sprintf("[dcache:%s] DSave: acquired dlock", truncKey))

	expiryUnix := expiration.Unix()
	expiryUnixString := strconv.FormatInt(expiryUnix, 10)

	_, err := RedisClientEvict().HSet(
		ctx,
		redisKey,
		dcacheRedisField_Expiration, expiryUnixString,
		dcacheRedisField_RawBody, string(body),
	).Result()
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DSave failed: redis HSET failed", truncKey), zap.Error(err))
		return err
	}

	// Cache the loaded record locally for quicker future lookups
	newEntry := &DEntry{
		expiration: expiration,
		RawBody:    body,
	}
	dEntryLocalCacheLock.Lock()
	dEntryLocalCache[keyIn] = newEntry
	dEntryLocalCacheLock.Unlock()

	logger().Debug(fmt.Sprintf("[dcache:%s] DSave finished", truncKey))

	return nil
}

// Persist a parsed Go result to local cache
// Useful for saving processing time on future successful local cache hits
func DSaveLocalGoBody(key string, goBody interface{}) {
	dEntryLocalCacheLock.Lock()
	defer dEntryLocalCacheLock.Unlock()

	if localEntry, ok := dEntryLocalCache[key]; ok {
		localEntry.GoBody = goBody
	}
}
