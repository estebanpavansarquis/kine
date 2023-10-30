package osclient

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

type KV struct {
	kv sync.Map
}

func (c *KV) Get(key string) (value *server.KeyValue, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "osclient.Get, key=%v => kv=%v, err=%v, duration=%s"
		logrus.Infof(fStr, key, value, err, dur)
	}()

	if v, ok := c.kv.Load(key); ok {
		if v, ok := v.(server.KeyValue); ok {
			return &v, nil
		} else {
			err = ErrKeyCastingFailed
		}
	} else {
		err = ErrKeyNotFound
	}

	return
}

func (c *KV) Add(key string, value *server.KeyValue) (err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "osclient.Add, key=%v => kv=%v, err=%v, duration=%s"
		logrus.Infof(fStr, key, value, err, dur)
	}()

	if _, ok := c.kv.Load(key); !ok {
		c.kv.Store(key, *value)
		return
	}

	return ErrKeyAlreadyExists
}

func (c *KV) Update(key string, value *server.KeyValue) (err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "osclient.Update, key=%v => kv=%v, err=%v, duration=%s"
		logrus.Infof(fStr, key, value, err, dur)
	}()

	if _, ok := c.kv.Load(key); ok {
		c.kv.Store(key, *value)
		return
	}

	return ErrKeyNotFound
}

func (c *KV) Delete(key string) (err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "osclient.Delete uodates, key=%s  duration=%s, err=%v"
		logrus.Infof(fStr, key, dur, err)
	}()

	if _, ok := c.kv.Load(key); ok {
		c.kv.Delete(key)
		return nil
	}

	return ErrKeyNotFound
}

func (c *KV) Keys() (keyList []string) {
	c.kv.Range(func(key, value interface{}) bool {
		keyList = append(keyList, fmt.Sprint(key))
		return true
	})
	return
}

func (c *KV) List(prefix string) (valueList []*server.KeyValue) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "osclient.List, prefix=%s => kvs=%+v, duration=%s"
		logrus.Infof(fStr, prefix, valueList, dur)
	}()

	c.kv.Range(func(key, value interface{}) bool {
		var (
			ok bool
			v  server.KeyValue
		)

		if !strings.HasPrefix(fmt.Sprint(key), prefix) {
			//logrus.Infof("stringz '%s' do not starts with '%s'", key, prefix)
			return true
		}

		if v, ok = value.(server.KeyValue); !ok {
			//logrus.Infof("key '%s' coud not be casto int keyvalue", key)
			return true
		}

		//logrus.Infof("appending key '%s' with value: %+v", key, v)
		valueList = append(valueList, &v)

		return true
	})

	//logrus.Infof("returning valueList with %d values", len(valueList))
	return
}
