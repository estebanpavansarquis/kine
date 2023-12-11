package osclient

import (
	"context"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

const (
	updateInterval   = 10 * time.Second
	updateBufferSize = 100
	noPrefix         = ""
	noLimit          = 0
	initCAS          = "0" // check and save, optimistic locking mechanism
)

type Watcher interface {
	Updates() //<-chan []*server.Event
	Stop() error
}

type resourceWatcher struct {
	key      string
	consumer ObjectStoreConsumer
	actual   Bundled
	index    map[string]string
	cas      string
	ctx      context.Context
	cancel   context.CancelFunc
	chEvents chan []*server.Event
}

func NewResourceWatcher(ctx context.Context, c ObjectStoreConsumer, key string, ch chan []*server.Event) (w Watcher, err error) {
	ctx, cancel := context.WithCancel(ctx)
	return &resourceWatcher{
		consumer: c,
		key:      key,
		ctx:      ctx,
		cancel:   cancel,
		index:    make(map[string]string, 0), // metadata
		actual:   NewBundle(),
		cas:      initCAS, // cas
		chEvents: ch,
	}, nil
}

func (r *resourceWatcher) Stop() error {
	r.cancel()
	return nil
}

func (r *resourceWatcher) Updates() { // <-chan []*server.Event {
	var (
		err     error
		updates []*server.Event
	)

	t := time.NewTicker(updateInterval)
	//ch := make(chan []*server.Event, updateBufferSize)

	if updates, err = r.currentState(); err != nil {
		if err != ErrKeyNotFound {
			logrus.Errorf("error checking current state at watcher %s with cas %s: %s", r.key, r.cas, err.Error())
			return
		}
		err = nil
	}

	go func() {
		if len(updates) > 0 {
			logrus.Infof("Updates: current state updates %d", len(updates))
			r.chEvents <- updates
		}

		for {
			select {
			case <-r.ctx.Done():
				logrus.Infof("context cancelled at watcher %s with cas %s", r.key, r.cas)
				t.Stop()
				return
			case <-t.C:
				logrus.Infof("ticker says it is time for checking for updates")

				if updates, err = r.checkForUpdates(); err != nil {
					logrus.Errorf("error checking for updates at watcher %s with cas %s: %s", r.key, r.cas, err.Error())
				} else {
					if len(updates) > 0 {

						for i, u := range updates {
							logrus.Warnf("posting update %d: %s ", i, u.String())
						}

						r.chEvents <- updates
					}
				}
			}
		}
	}()

	return
}

func (r *resourceWatcher) currentState() (e []*server.Event, err error) {
	var (
		newBundle   Bundled
		newIndex    map[string]string
		newRevision string
	)
	if newBundle, newIndex, newRevision, err = r.consumer.GetBundle(r.key); err != nil {
		return
	}

	r.actual, r.index, r.cas = newBundle, newIndex, newRevision

	e = make([]*server.Event, 0)
	for _, kv := range r.actual.List(noPrefix, noLimit) {
		kv := kv
		e = append(e, creationEvent(kv))
	}

	return
}

func (r *resourceWatcher) checkForUpdates() (e []*server.Event, err error) {
	var (
		newBundle   Bundled
		newIndex    map[string]string
		newRevision string
	)
	if newBundle, newIndex, newRevision, err = r.consumer.GetBundle(r.key); err != nil {
		if err != ErrKeyNotFound {
			logrus.Errorf("error getting bundle watcher %s with cas %s: %s", r.key, r.cas, err.Error())
		} else {
			err = nil
		}
		return
	}

	if r.cas == newRevision {
		logrus.Infof("GetRevision is up to date: " + newRevision)
		return
	}
	logrus.Infof("GetRevision: " + r.cas + " was outdated, new cas found: " + newRevision)

	e = make([]*server.Event, 0)
	if added, modified, deleted, diff := r.diff(newIndex); diff {
		for _, a := range added {
			if kv, ok := newBundle.Get(a); ok {
				logrus.Infof("Appending addition: " + a)
				e = append(e, creationEvent(kv))
			}
		}

		for _, m := range modified {
			kv, ok1 := newBundle.Get(m)
			kvOld, ok2 := r.actual.Get(m)
			if ok1 && ok2 {
				logrus.Infof("Appending update: " + m)
				e = append(e, modificationEvent(kvOld, kv))
			}
		}

		for _, d := range deleted {
			if kv, ok := r.actual.Get(d); ok {
				logrus.Infof("Appending deletion: " + d)
				e = append(e, deletionEvent(kv))
			}
		}
	}

	r.actual, r.index, r.cas = newBundle, newIndex, newRevision

	return
}

func (r *resourceWatcher) diff(i2 map[string]string) (added, modified, deleted []string, diff bool) {
	added = make([]string, 0, 0)
	modified = make([]string, 0, 0)
	deleted = make([]string, 0, 0)

	for resourceName, actualHash := range r.index {
		if newHash, exist := i2[resourceName]; !exist {
			logrus.Infof("Deletion found in new index: " + resourceName)
			deleted = append(deleted, resourceName)
		} else if newHash != actualHash {
			logrus.Infof("Modification found in new index: " + resourceName)
			modified = append(modified, resourceName)
		}
	}

	for newResourceName, _ := range i2 {
		if _, exist := r.index[newResourceName]; !exist {
			logrus.Infof("Addition found in new index: " + newResourceName)
			added = append(added, newResourceName)
		}
	}

	diff = len(added)+len(modified)+len(deleted) > 0

	return
}

func creationEvent(kv *server.KeyValue) *server.Event {
	return &server.Event{
		Create: true,
		Delete: false,
		KV:     kv,
		PrevKV: nil,
	}
}

func modificationEvent(kvOld, kv *server.KeyValue) *server.Event {
	return &server.Event{
		Create: false,
		Delete: false,
		KV:     kv,
		PrevKV: kvOld,
	}
}

func deletionEvent(kv *server.KeyValue) *server.Event {
	return &server.Event{
		Create: false,
		Delete: true,
		KV:     kv,
		PrevKV: kv,
	}
}
