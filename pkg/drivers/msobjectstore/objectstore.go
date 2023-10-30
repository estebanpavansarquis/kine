package msobjectstore

import (
	"context"
	"fmt"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/msobjectstore/osclient"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/sirupsen/logrus"
)

const (
	MockRev           = 1
	defaultBucket     = "kine"
	defaultReplicas   = 1
	defaultRevHistory = 10
	defaultSlowMethod = 500 * time.Millisecond
)

type Driver struct {
	//conn *nats.Conn
	kv osclient.KV

	slowThreshold time.Duration
}

func New(_ context.Context, _ string, _ tls.Config) (server.Backend, error) {
	logrus.Info("msobjectstore Driver.New()")
	return &Driver{slowThreshold: defaultSlowMethod}, nil
}

func (d *Driver) Start(ctx context.Context) error {
	logrus.Info("msobjectstore Driver.Start()")

	if _, err := d.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}

	return nil
}

func (d *Driver) Get(_ context.Context, key, _ string, _, revision int64) (rev int64, val *server.KeyValue, err error) {
	logrus.Infof("msobjectstore Driver.Get(key:%s, revision:%d)\n", key, revision)
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		size := 0
		if val != nil {
			size = len(val.Value)
		}
		fStr := "msobjectstore Driver.Get %s, rev=%d => revRet=%d, kv=%v, size=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, rev, val != nil, size, err, dur)
	}()

	rev = 1
	val, err = d.kv.Get(key)

	if err == osclient.ErrKeyNotFound {
		err = nil
	}

	return
}

func (d *Driver) Create(_ context.Context, key string, value []byte, lease int64) (rev int64, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "CREATE %s, size=%d, lease=%d => rev=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), lease, rev, err, dur)
	}()

	createValue := getServerKeyValue(key, value, lease)

	rev = 0
	err = d.kv.Add(key, createValue)

	return
}

func (d *Driver) Update(_ context.Context, key string, value []byte, revision, lease int64) (rev int64, val *server.KeyValue, success bool, err error) {
	logrus.Infof("msobjectstoreDriver.Update(key:%s, value:%+v, lease:%d, revision:%d)\n", key, value, lease, revision)
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		kvRev := int64(0)
		if val != nil {
			kvRev = val.ModRevision
		}
		fStr := "msobjectstoreDriver.Update posta %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), revision, lease, rev, kvRev, nil, err, dur)
	}()

	success = false
	rev = revision
	val = getServerKeyValue(key, value, lease)

	if err = d.kv.Update(key, val); err != nil {
		if err == osclient.ErrKeyNotFound {
			err = nil
		}
		val = nil
		return
	}

	success = true

	return

}

func (d *Driver) Delete(_ context.Context, key string, revision int64) (rev int64, val *server.KeyValue, success bool, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore Driver.Delete %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, rev, val != nil, success, err, dur)
	}()

	rev = 1
	success = false

	if err = d.kv.Delete(key); err != nil {
		if err == osclient.ErrKeyNotFound {
			success = true
			err = nil
		}
		return
	}

	success = true

	return
}

func (d *Driver) List(_ context.Context, prefix, startKey string, limit, revision int64) (rev int64, kvs []*server.KeyValue, err error) {
	//TODO implement me
	logrus.Infof("msobjectstoreDriver.List(prefix:%s, startKey:%s, limit:%d, revision:%d)\n", prefix, startKey, limit, revision)
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "LIST req-prefix%s, req-start=%s, req-limit=%d, req-rev=%d => res-rev=%d, res-kvs=%d, res-err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, startKey, limit, revision, rev, len(kvs), err, dur)
	}()

	rev = 1
	kvs = d.kv.List(prefix)

	return
}

func (d *Driver) Count(_ context.Context, prefix string) (rev int64, count int64, e error) {
	logrus.Infof("msobjectstoreDriver.Count(prefix:%s)\n", prefix)

	rev = MockRev
	count = 123

	return
}

func (d *Driver) Watch(_ context.Context, key string, revision int64) <-chan []*server.Event {
	logrus.Infof("msobjectstoreDriver.Watch(key:%s, value:%+v, revision:%d)\n", key, revision)

	watchChan := make(chan []*server.Event, 100)

	return watchChan
}

func (d *Driver) DbSize(_ context.Context) (int64, error) {
	fmt.Println("msobjectstore Driver.DbSize()")

	return int64(len(d.kv.Keys())), nil

}

func (d *Driver) logMethod(dur time.Duration, str string, args ...any) {
	//if dur > d.slowThreshold {
	//	logrus.Warnf(str, args...)
	//} else {
	//	logrus.Tracef(str, args...)
	//}
	logrus.Infof(str, args...)
}

func getServerKeyValue(key string, value []byte, lease int64) *server.KeyValue {
	return &server.KeyValue{
		Key:            key,
		CreateRevision: MockRev,
		ModRevision:    MockRev,
		Value:          value,
		Lease:          lease,
	}
}
