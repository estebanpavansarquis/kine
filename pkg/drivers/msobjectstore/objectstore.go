package msobjectstore

import (
	"context"
	"strings"
	"time"

	"github.com/estebanpavansarquis/kine/pkg/drivers/msobjectstore/osclient"
	"github.com/estebanpavansarquis/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

const (
	DefaultLease      = int64(1)
	defaultSlowMethod = 500 * time.Millisecond

	apiServerResourceKeyPrefix = "/registry/sample-apiserver/gateway.mulesoft.com/"
)

type driver struct {
	store         osclient.ObjectStoreConsumer
	slowThreshold time.Duration
	//mut           sync.RWMutex
}

func New() (be server.Backend, err error) {
	logrus.Info("msobjectstore driver.New()")

	var c osclient.ObjectStoreConsumer
	if c, err = osclient.NewConsumer(); err != nil {
		return
	}

	return &driver{
		store:         c,
		slowThreshold: defaultSlowMethod,
	}, nil
}

func (d *driver) Start(_ context.Context) (err error) {
	logrus.Info("MS-ObjectStore Driver is starting...")

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.START: err=%v, duration=%s"
		d.logMethod(dur, fStr, err, dur)
	}()

	if _, _, err = d.store.Status(); err != nil {
		return
	}

	_, err = d.store.IncrementRevision()

	return
}

func (d *driver) Get(_ context.Context, key, rangeEnd string, limit, revision int64) (rev int64, val *server.KeyValue, err error) {
	//d.mut.RLock()
	//defer d.mut.RUnlock()

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		size := 0
		if val != nil {
			size = len(val.Value)
		}
		fStr := "msobjectstore driver.GET %s, rev=%d, ignored:rangeEnd=%s, ignored:limit=%d => revRet=%d, kv=%t, size=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, rangeEnd, limit, rev, val != nil, size, err, dur)
	}()

	//rev = d.revision.Load()
	//rev = DefaultRevision

	var resourceType, resourceKey string
	if resourceType, resourceKey, err = parseKey(key); err != nil {
		return
	}

	if val, err = d.store.Get(resourceType, resourceKey); err != nil {
		if err == osclient.ErrKeyNotFound {
			if rev, err = d.store.Revision(); err != nil {
				return
			}
			err = nil
		}
		return
	}

	rev = val.ModRevision

	return
}

func (d *driver) List(_ context.Context, prefix, startKey string, limit, revision int64) (rev int64, kvs []*server.KeyValue, err error) {
	//d.mut.RLock()
	//defer d.mut.RUnlock()

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.LIST prefix=%s, ignored:req-start=%s, req-limit=%d, ignored:req-rev=%d => res-rev=%d, size-kvs=%t, res-err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, startKey, limit, revision, rev, len(kvs), err, dur)
	}()

	if rev, err = d.store.Revision(); err != nil {
		return
	}

	resourceBundleKey, resourceNamePrefix := parsePrefix(prefix)

	kvs, err = d.store.List(resourceBundleKey, resourceNamePrefix, limit)

	return
}

func (d *driver) Create(_ context.Context, key string, value []byte, lease int64) (rev int64, err error) {
	//d.mut.Lock()
	//defer d.mut.Unlock()

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.CREATE %s, size=%d, lease=%d => rev=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), lease, rev, err, dur)
	}()

	if rev, err = d.store.IncrementRevision(); err != nil {
		return
	}

	resourceType, resourceKey, err := parseKey(key)
	if err != nil {
		return
	}

	kv := newKeyValue(key, value, rev, rev)
	err = d.store.Create(resourceType, resourceKey, kv)

	return
}

func (d *driver) Delete(_ context.Context, key string, revision int64) (rev int64, kv *server.KeyValue, success bool, err error) {
	start := time.Now()
	//d.mut.Lock()
	//defer d.mut.Unlock()

	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.DELETE %s, ignored:revision=%d => rev=%d, kv=%v, success=%t, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, rev, kv, err, dur)
	}()

	var resType, resKey string
	if resType, resKey, err = parseKey(key); err != nil {
		return
	}

	if rev, err = d.store.IncrementRevision(); err != nil {
		return
	}

	if kv, err = d.store.Delete(resType, resKey); err != nil {
		return
	}

	success = true

	return
}

func (d *driver) Update(_ context.Context, key string, value []byte, revision, lease int64) (rev int64, val *server.KeyValue, success bool, err error) {
	start := time.Now()
	//d.mut.Lock()
	//defer d.mut.Unlock()

	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.UPDATE %s, size=%d, ignored:revision=%d, ignored:lease=%d => rev=%d, success=%t, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), revision, lease, rev, success, err, dur)
	}()

	var resourceType, resourceKey string
	if resourceType, resourceKey, err = parseKey(key); err != nil {
		return

	}

	if rev, err = d.store.IncrementRevision(); err != nil {
		return
	}
	kv := newKeyValue(key, value, 0, rev)
	success, err = d.store.Update(resourceType, resourceKey, kv)

	return
}

func (d *driver) Count(ctx context.Context, prefix string) (rev int64, count int64, err error) {
	//d.mut.RLock()
	//defer d.mut.RUnlock()

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.COUNT %s => rev=%d, count=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, rev, count, err, dur)
	}()

	if rev, err = d.store.Revision(); err != nil {
		return
	}

	var list []*server.KeyValue
	if rev, list, err = d.List(ctx, prefix, "", 0, 0); err != nil {
		return
	}
	count = int64(len(list))

	return
}

func (d *driver) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {
	logrus.Infof("msobjectstoreDriver.Watch prefix=%s, ignored:revision=%d", prefix, revision)

	ch := make(chan []*server.Event, 100)

	bundleKey, _ := parsePrefix(prefix)
	watcher, err := d.store.Watch(ctx, bundleKey, ch)
	if err != nil {
		return nil
	}
	watcher.Updates(revision)
	/*
		go func() {
			watcher.Updates()
			for {
				select {
				case u := <-updates:
					if len(u) > 0 {
						logrus.Infof("watcher %s has an update incoming ", bundleKey)
						ch <- u
					}
				case <-ctx.Done():
					logrus.Infof("watcher: %s context cancelled", prefix)
					if err := watcher.Stop(); err != nil {
						logrus.Warnf("error stopping %s watcher: %v", prefix, err)
					}
					return
				}
			}
		}()
	*/

	return ch
}

func (d *driver) DbSize(_ context.Context) (size int64, err error) {
	return d.store.Size()
}

func (d *driver) logMethod(dur time.Duration, str string, args ...any) {
	if dur > d.slowThreshold {
		logrus.Warnf(str, args...)
	} else {
		logrus.Infof(str, args...)
	}
}

func formatKey(s string) (key string) {
	key = strings.TrimPrefix(s, "/")
	key = strings.TrimSuffix(key, "/")
	return strings.ReplaceAll(key, "/", "_")
}

func newKeyValue(key string, value []byte, cRev, mRev int64) *server.KeyValue {
	return &server.KeyValue{
		Key:            key,
		CreateRevision: cRev,
		ModRevision:    mRev,
		Value:          value,
		Lease:          DefaultLease,
	}
}

/*
parseKey parses ETCD key into MS Object Store keys
key example:

	/registry/sample-apiserver/gateway.mulesoft.com/apiinstances/default/object-store-api

returns:

	store: controlNodeStore
	partition: controlNodePartition
	resourceType: apiinstances
	resourceKey: default_object-store-api = <resource-namespace>_<resource-name>

	/registry/sample-apiserver/gateway.mulesoft.com/apiinstances/default/object-store-api-01
*/
func parseKey(key string) (resourceType, resourceKey string, err error) {
	/*
		start := time.Now()
		defer func() {
			dur := time.Since(start)
			fStr := "msobjectstore driver.parseKey %s => resourceType=%s, resourceKey=%s, err=%v, duration=%s"
			logrus.Infof(fStr, key, resourceType, resourceKey, err, dur)
		}()
	*/

	if !strings.HasPrefix(key, apiServerResourceKeyPrefix) {
		err = osclient.ErrInvalidKey
		logrus.Errorf("%s: string %s has not prefix %s", err.Error(), key, apiServerResourceKeyPrefix)
		return
	}

	tmpStr := strings.ReplaceAll(key, apiServerResourceKeyPrefix, "")
	tmpSlc := strings.Split(tmpStr, "/")
	if len(tmpSlc) != 3 {
		err = osclient.ErrInvalidKey
		logrus.Errorf("%s: slice %+v has size %v and not size %v", err.Error(), tmpSlc, len(tmpSlc), 3)
		return
	}

	resourceType = tmpSlc[0]
	resourceKey = tmpSlc[1] + "_" + tmpSlc[2]

	return
}

func parsePrefix(prefix string) (bundleKey, resourceNamePrefix string) {
	auxStr := strings.Replace(prefix, apiServerResourceKeyPrefix, "", 1)
	auxSlc := strings.SplitN(auxStr, "/", 2)

	bundleKey = auxSlc[0]
	if len(auxSlc) == 2 {
		resourceNamePrefix = formatKey(auxSlc[1])
	}

	return
}
