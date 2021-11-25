package cache

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	fakedb = map[int64]User{}
)

type User struct {
	Id   int64 `json:"id" cache:"id,id" mapstructure:""`
	Name string `json:"name" cache:"name,indexed"`
}

func getCacheNode() cacheNode {
	opts := &redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}

	c := newCacheNode(opts)
	return c
}

func generateFakeDb() map[int64]User {
	// fake a db
	fakedb = make(map[int64]User)

	fakedb[1] = User{Id: 1, Name: "a"}
	fakedb[2] = User{Id: 2, Name: "b"}
	fakedb[3] = User{Id: 3, Name: "c"}
	fakedb[4] = User{Id: 4, Name: "d"}

	return fakedb
}

func TestCacheNode_SingleThread(t *testing.T) {
	ctx := context.Background()
	c := getCacheNode()
	db := generateFakeDb()

	var u User
	c.Take(ctx, &u, "user:1", func(v interface{}) error {
		*v.(*User) = db[1]
		return nil
	})
	hitCount := c.cacheUsages.HitCount("user:1")
	fmt.Println("hitCount:", hitCount)
	assert.Equal(t, u.Name, "a")
}

func TestCacheNode_SingleFlight(t *testing.T) {
	ctx := context.Background()
	c := getCacheNode()
	db := generateFakeDb()

	var u User
	c.Take(ctx, &u, "user:1", func(v interface{}) error {
		*v.(*User) = db[1]
		return nil
	})

	assert.Equal(t, u.Name, "b")
}
