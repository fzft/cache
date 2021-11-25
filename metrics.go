package cache

import (
	"fmt"
	"github.com/rcrowley/go-metrics"
	"sync"
)

type Usage struct {
	mu sync.Mutex
	r  metrics.Registry
}

func (u *Usage) register(key string) {
	hitName := fmt.Sprintf("%s_hits_total", key)
	missName := fmt.Sprintf("%s_misses_total", key)
	hitCount := metrics.NewCounter()
	missCount := metrics.NewCounter()
	u.r.Register(hitName, hitCount)
	u.r.Register(missName, missCount)
	return
}

func (u *Usage) IncrHit(key string) {
	hitName := fmt.Sprintf("%s_hits_total", key)
	hi := u.r.Get(hitName)
	if hi == nil {
		u.register(key)
		hi = u.r.Get(hitName)
	}

	hit := hi.(metrics.Counter)
	hit.Inc(1)
}

func (u *Usage) IncrMiss(key string) {
	misName := fmt.Sprintf("%s_misses_total", key)
	mi := u.r.Get(misName)
	if mi == nil {
		u.register(key)
		mi = u.r.Get(misName)
	}

	miss := mi.(metrics.Counter)
	miss.Inc(1)
}

func (u *Usage) HitCount(key string) int64 {
	hitName := fmt.Sprintf("%s_hits_total", key)
	hi := u.r.Get(hitName)
	if hi == nil {
		return 0
	}
	hit := hi.(metrics.Counter)
	return hit.Count()

}

func (u *Usage) MissCount(key string) int64 {
	missName := fmt.Sprintf("%s_misses_total", key)
	mi := u.r.Get(missName)
	if mi == nil {
		return 0
	}
	miss := mi.(metrics.Counter)
	return miss.Count()
}

func (u *Usage) HitPct(key string) int64 {
	hit := u.HitCount(key)
	miss := u.MissCount(key)
	total := hit + miss
	if total == 0 {
		return 0
	}
	return hit / total
}
