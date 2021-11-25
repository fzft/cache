package cache

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"time"
)

type Redis struct {
	rds *redis.Client
}

func newRedis(opts *redis.Options) *Redis {
	return &Redis{
		rds: redis.NewClient(opts),
	}
}

func (r *Redis) RGet(ctx context.Context, key string) (val interface{}, err error) {
	mVal, err := r.HGetAll(ctx, key)
	if err == redis.Nil || len(mVal) == 0 {
		val, err = r.Get(ctx, key)
		if err == redis.Nil {
			return
		} else if err != nil {
			return
		}

		if val == notFoundPlaceholder {
			err = ErrPlaceholder
			return
		}
		return

	} else if err != nil {
		return
	}

	val = mVal

	return
}

func (r *Redis) Get(ctx context.Context, key string) (val string, err error) {
	val, err = r.rds.Get(ctx, key).Result()
	return
}

func (r *Redis) Del(ctx context.Context, keys ...string) (val int64, err error) {
	val, err = r.rds.Del(ctx, keys...).Result()
	return
}

func (r *Redis) HGetAll(ctx context.Context, key string) (val map[string]string, err error) {
	val, err = r.rds.HGetAll(ctx, key).Result()
	return
}

func (r *Redis) HGet(ctx context.Context, key, field string) (val interface{}, err error) {
	val, err = r.rds.HMGet(ctx, key, field).Result()
	return
}

func (r *Redis) SetWithExpire(ctx context.Context, key string, v interface{}, expire time.Duration) (err error) {
	data, err := json.Marshal(v)
	_, err = r.rds.SetEX(ctx, key, data, expire).Result()
	return err
}

func (r *Redis) HMSet(ctx context.Context, key string, val map[string]interface{}) error {
	_, err := r.rds.HMSet(ctx, key, val).Result()
	return err
}

func (r *Redis) Exists(ctx context.Context, key string) (bool, error) {
	val, err := r.rds.Exists(ctx, key).Result()
	return val == 1, err
}

func (r *Redis) LPush(ctx context.Context, key, v string) (bool, error) {
	val, err := r.rds.LPush(ctx, key, v).Result()
	return val == 1, err
}

func (r *Redis) RPop(ctx context.Context, key string) (string, error) {
	val, err := r.rds.RPop(ctx, key).Result()
	return val, err
}
