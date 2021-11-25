package cache

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/fatih/structtag"
	"github.com/go-redis/redis/v8"
	"github.com/mitchellh/mapstructure"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
	"reflect"
	"strconv"
	"time"
)

const (
	notFoundPlaceholder = "*"
)

var (
	ErrRedisKeyNotExist = errors.New("redis key not exist")
	ErrPlaceholder      = errors.New("placeholder error")
	ErrNotFound         = errors.New("not found")
	ErrUnKnown          = errors.New("unknown error")
)

type cacheNode struct {
	sfGroup        singleflight.Group
	rds            *Redis
	notFoundExpiry time.Duration
	expiry         time.Duration
	cacheUsages    *Usage
}

func newCacheNode(opts *redis.Options) cacheNode {
	return cacheNode{
		expiry:         time.Second * 60,
		notFoundExpiry: time.Second * 60,
		rds:            newRedis(opts),
		cacheUsages:    &Usage{r: metrics.NewRegistry()},
	}
}

func (c cacheNode) Take(ctx context.Context, v interface{}, key string, query func(v interface{}) error) error {
	return c.doTake(ctx, v, key, query, func(v interface{}) {
		err := c.doSetCache(ctx, v, key)
		if err != nil {
			log.Errorf("take set expiry error: %v\n", err)
		}
		return
	})
}

func (c cacheNode) doSetCache(ctx context.Context, v interface{}, key string) error {
	var (
		hashMap map[string]interface{}
		val     string
	)

	hashMap = make(map[string]interface{})

	elemsType := reflect.TypeOf(v)
	if elemsType.Kind() == reflect.Ptr {
		elemsType = elemsType.Elem()
	}

	for i := 0; i < elemsType.NumField(); i++ {
		tag := elemsType.Field(i).Tag
		tags, err := structtag.Parse(string(tag))
		if err != nil {
			panic(err)
		}
		cacheTag, err := tags.Get("cache")
		if err != nil {
			continue
		}

		name := cacheTag.Name
		t := elemsType.Field(i).Type.String()

		if t == "int64" {
			val = strconv.Itoa(int(reflect.ValueOf(v).Elem().Field(i).Int()))
		} else if t == "string" {
			val = reflect.ValueOf(v).Elem().Field(i).String()
		}

		hashMap[name] = val
		if contains(cacheTag.Options, "indexed") {

		}

	}
	err := c.rds.HMSet(ctx, key, hashMap)
	return err
}

func (c cacheNode) doTake(ctx context.Context, v interface{}, key string, query func(v interface{}) error, cacheVal func(v interface{})) error {

	// singleflight
	val, err, _ := c.sfGroup.Do(key, func() (interface{}, error) {
		if err := c.doGetCache(ctx, key, v); err != nil {
			if err == ErrPlaceholder {
				log.Infof("do get cache with placeholder")
				return nil, ErrNotFound
			} else if err != ErrNotFound {
				log.Infof("do get cache with not found: %v", err)
				return nil, err
			}

			if err := query(v); err == ErrNotFound {
				c.setCacheWithNotFound(ctx, key)
				return nil, err
			} else if err != nil {
				return nil, err
			}

			cacheVal(v)
		}

		return json.Marshal(v)
	})

	if err != nil {
		return err
	}

	return json.Unmarshal(val.([]byte), v)
}

func (c cacheNode) setCacheWithNotFound(ctx context.Context, key string) error {
	return c.rds.SetWithExpire(ctx, key, notFoundPlaceholder, c.notFoundExpiry)
}

func (c cacheNode) doGetCache(ctx context.Context, key string, v interface{}) error {
	val, err := c.rds.RGet(ctx, key)
	if err == redis.Nil {
		c.cacheUsages.IncrMiss(key)
		return ErrNotFound
	} else if err != nil {
		c.cacheUsages.IncrMiss(key)
		return err
	}

	if data, ok := val.(string); ok {
		if data == notFoundPlaceholder {
			return ErrPlaceholder
		}
	} else if data, ok := val.(map[string]string); ok {
		c.cacheUsages.IncrHit(key)
		return c.processCache(ctx, key, data, v)
	}

	return ErrUnKnown
}

func (c cacheNode) processCache(ctx context.Context, key string, data map[string]string, v interface{}) error {
	err := mapstructure.WeakDecode(data, v)
	if err == nil {
		return err
	}
	c.rds.Del(ctx, key)
	return ErrNotFound
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
