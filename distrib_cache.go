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
//

const (
	dcacheRedisField_Expiration = "expiration"
	dcacheRedisField_RawBody    = "raw_body"
)

type DEntry struct {
	RawBody []byte
	GoBody  any

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

// helper for logging purposes
func truncKey(keyIn string) string {
	return truncate.Truncate(keyIn, 30, "...", truncate.PositionMiddle)
}

//
//
// Loading

func dLoadCheckLocal(ctx context.Context, keyIn string) *DEntry {
	dEntryLocalCacheLock.Lock()
	localDe, ok := dEntryLocalCache[keyIn]
	dEntryLocalCacheLock.Unlock()
	if ok && localDe.Valid() {
		logger().Debug(fmt.Sprintf("[dcache:%s] dLoadCheckLocal: local cache hit (exp: %v)", truncKey(keyIn), time.Until(localDe.expiration)))
		return localDe
	}
	return nil
}

// Load data from cache (local, then distributed), controlling access via distributed lock
func DLoad(ctx context.Context, keyIn string) (*DEntry, error) {
	// Check process-local cache first
	if localDe := dLoadCheckLocal(ctx, keyIn); localDe != nil {
		return localDe, nil
	}

	// DLock!
	dlock := DLock(redisEntryKey(keyIn))
	if err := dlock.LockContext(ctx); err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DLoad failed: dlock acquisition failed", truncKey(keyIn)), zap.Error(err))
		return nil, err
	}
	defer func() {
		dlock.UnlockContext(ctx)
		logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: dlock released", truncKey(keyIn)))
	}()
	logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: acquired dlock", truncKey(keyIn)))

	bodyString, expiration, err := dLoadData(ctx, keyIn)
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DLoad failed: dLoadData failed", truncKey(keyIn)), zap.Error(err))
		return nil, err
	}

	// Short-circuit if expired
	if expiration.Before(time.Now()) {
		logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: data found in shared storage, but it's expired", truncKey(keyIn)))
		return nil, nil
	}

	// Cache the loaded record locally for quicker future lookups
	newEntry := &DEntry{
		expiration: expiration,
		RawBody:    []byte(bodyString),
	}
	dEntryLocalCacheLock.Lock()
	dEntryLocalCache[keyIn] = newEntry
	dEntryLocalCacheLock.Unlock()

	logger().Debug(fmt.Sprintf("[dcache:%s] DLoad: data loaded from shared storage, saved to local cache (exp: %v)", truncKey(keyIn), time.Until(newEntry.expiration)))

	return newEntry, nil
}

// Load data from cache (local, then distributed) without using a distributed lock
func DLoadLockless(ctx context.Context, keyIn string) (*DEntry, error) {
	// Check process-local cache first
	if localDe := dLoadCheckLocal(ctx, keyIn); localDe != nil {
		return localDe, nil
	}

	bodyString, expiration, err := dLoadData(ctx, keyIn)
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DLoadLockless failed: dLoadData failed", truncKey(keyIn)), zap.Error(err))
		return nil, err
	}

	// Short-circuit if expired
	if expiration.Before(time.Now()) {
		logger().Debug(fmt.Sprintf("[dcache:%s] DLoadLockless: data found in shared storage, but it's expired", truncKey(keyIn)))
		return nil, nil
	}

	// Cache the loaded record locally for quicker future lookups
	newEntry := &DEntry{
		expiration: expiration,
		RawBody:    []byte(bodyString),
	}
	dEntryLocalCacheLock.Lock()
	dEntryLocalCache[keyIn] = newEntry
	dEntryLocalCacheLock.Unlock()

	logger().Debug(fmt.Sprintf("[dcache:%s] DLoadLockless: data loaded from shared storage, saved to local cache (exp: %v)", truncKey(keyIn), time.Until(newEntry.expiration)))

	return newEntry, nil
}

//
//
// Saving

func DSave(ctx context.Context, keyIn string, body []byte, expiration time.Time) error {
	dlock := DLock(redisEntryKey(keyIn))
	if err := dlock.LockContext(ctx); err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DSave failed: dlock acquisition failed", truncKey(keyIn)), zap.Error(err))
		return err
	}
	defer func() {
		dlock.UnlockContext(ctx)
		logger().Debug(fmt.Sprintf("[dcache:%s] DSave: dlock released", truncKey(keyIn)))
	}()
	logger().Debug(fmt.Sprintf("[dcache:%s] DSave: acquired dlock", truncKey(keyIn)))

	err := dPersistData(ctx, keyIn, body, expiration)
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DSave failed", truncKey(keyIn)), zap.Error(err))
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

	logger().Debug(fmt.Sprintf("[dcache:%s] DSave finished", truncKey(keyIn)))

	return nil
}

func DSaveLockless(ctx context.Context, keyIn string, body []byte, expiration time.Time) error {
	err := dPersistData(ctx, keyIn, body, expiration)
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] DSaveLockless failed", truncKey(keyIn)), zap.Error(err))
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

	logger().Debug(fmt.Sprintf("[dcache:%s] DSaveLockless finished", truncKey(keyIn)))

	return nil
}

// Persist a parsed Go result to local cache
// Useful for saving processing time on future successful local cache hits
func DSaveLocalGoBody(key string, goBody any) {
	dEntryLocalCacheLock.Lock()
	defer dEntryLocalCacheLock.Unlock()

	if localEntry, ok := dEntryLocalCache[key]; ok {
		localEntry.GoBody = goBody
	}
}

//
//
// Primitives

func dPersistData(ctx context.Context, keyIn string, body []byte, expiration time.Time) error {
	redisKey := redisEntryKey(keyIn)

	expiryUnix := expiration.Unix()
	expiryUnixString := strconv.FormatInt(expiryUnix, 10)

	_, err := RedisClientEvict().HSet(
		ctx,
		redisKey,
		dcacheRedisField_Expiration, expiryUnixString,
		dcacheRedisField_RawBody, string(body),
	).Result()
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] dPersistData failed: redis HSET failed", truncKey(keyIn)), zap.Error(err))
		return err
	}

	return nil
}

func dLoadData(ctx context.Context, keyIn string) (body string, exp time.Time, err error) {
	redisKey := redisEntryKey(keyIn)

	// Get from shared storage
	exists, err := RedisClientEvict().Exists(ctx, redisKey).Result()
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] dLoadData failed: redis EXISTS failed", truncKey(keyIn)), zap.Error(err))
		return
	}
	if exists == 0 {
		// Nothing cached
		logger().Debug(fmt.Sprintf("[dcache:%s] dLoadData failed: data not found in shared storage", truncKey(keyIn)))
		return
	}
	entryRedis, err := RedisClientEvict().HGetAll(ctx, redisKey).Result()
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] dLoadData failed: redis HGETALL failed", truncKey(keyIn)), zap.Error(err))
		return
	}

	expirationUnixString, ok := entryRedis[dcacheRedisField_Expiration]
	if !ok {
		err = fmt.Errorf("Malformed distributed cache entry structure in redis storage (missing %q field)", dcacheRedisField_Expiration)
		logger().Error(fmt.Sprintf("[dcache:%s] dLoadData failed", truncKey(keyIn)), zap.Error(err))
		return
	}
	expirationUnix, err := strconv.ParseInt(expirationUnixString, 10, 64)
	if err != nil {
		logger().Error(fmt.Sprintf("[dcache:%s] dLoadData failed", truncKey(keyIn)), zap.Error(err))
		return
	}
	exp = time.Unix(expirationUnix, 0)

	body, ok = entryRedis[dcacheRedisField_RawBody]
	if !ok {
		err = fmt.Errorf("Malformed distributed cache entry structure in redis storage (missing %q field)", dcacheRedisField_RawBody)
		logger().Error(fmt.Sprintf("[dcache:%s] dLoadData failed", truncKey(keyIn)), zap.Error(err))
		return
	}

	return
}
