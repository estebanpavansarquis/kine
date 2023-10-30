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

	mockBinaryValue = "eyJraW5kIjoiQXBpSW5zdGFuY2UiLCJhcGlWZXJzaW9uIjoiZ2F0ZXdheS5tdWxlc29mdC5jb20vdjFhbHBoYTEiLCJtZXRhZGF0YSI6eyJuYW1lIjoib2JqZWN0LXN0b3JlLWFwaS0wMiIsIm5hbWVzcGFjZSI6ImRlZmF1bHQiLCJ1aWQiOiI1Y2U2NWY1Ny1hNDE5LTQ1NGMtYWNkZC03NGE4NzMzNTUxM2QiLCJjcmVhdGlvblRpbWVzdGFtcCI6IjIwMjMtMTEtMjdUMTU6NDk6NDVaIiwiYW5ub3RhdGlvbnMiOnsia3ViZWN0bC5rdWJlcm5ldGVzLmlvL2xhc3QtYXBwbGllZC1jb25maWd1cmF0aW9uIjoie1wiYXBpVmVyc2lvblwiOlwiZ2F0ZXdheS5tdWxlc29mdC5jb20vdjFhbHBoYTFcIixcImtpbmRcIjpcIkFwaUluc3RhbmNlXCIsXCJtZXRhZGF0YVwiOntcImFubm90YXRpb25zXCI6e30sXCJuYW1lXCI6XCJvYmplY3Qtc3RvcmUtYXBpLTAyXCIsXCJuYW1lc3BhY2VcIjpcImRlZmF1bHRcIn0sXCJzcGVjXCI6e1wiYWRkcmVzc1wiOlwiaHR0cDovL2xvY2FsaG9zdDo0MDAwL29iamVjdC1zdG9yZVwiLFwic2VydmljZXNcIjp7XCJ1cHN0cmVhbVwiOntcImFkZHJlc3NcIjpcImxvY2FsaG9zdDo0MDAwL2FwaS92MS9cIn19fX1cbiJ9LCJtYW5hZ2VkRmllbGRzIjpbeyJtYW5hZ2VyIjoia3ViZWN0bC1jbGllbnQtc2lkZS1hcHBseSIsIm9wZXJhdGlvbiI6IlVwZGF0ZSIsImFwaVZlcnNpb24iOiJnYXRld2F5Lm11bGVzb2Z0LmNvbS92MWFscGhhMSIsInRpbWUiOiIyMDIzLTExLTI3VDE1OjQ5OjQ1WiIsImZpZWxkc1R5cGUiOiJGaWVsZHNWMSIsImZpZWxkc1YxIjp7ImY6bWV0YWRhdGEiOnsiZjphbm5vdGF0aW9ucyI6eyIuIjp7fSwiZjprdWJlY3RsLmt1YmVybmV0ZXMuaW8vbGFzdC1hcHBsaWVkLWNvbmZpZ3VyYXRpb24iOnt9fX0sImY6c3BlYyI6eyJmOmFkZHJlc3MiOnt9LCJmOnNlcnZpY2VzIjp7Ii4iOnt9LCJmOnVwc3RyZWFtIjp7Ii4iOnt9LCJmOmFkZHJlc3MiOnt9fX19fX1dfSwic3BlYyI6eyJhZGRyZXNzIjoiaHR0cDovL2xvY2FsaG9zdDo0MDAwL29iamVjdC1zdG9yZSIsInNlcnZpY2VzIjp7InVwc3RyZWFtIjp7ImFkZHJlc3MiOiJsb2NhbGhvc3Q6NDAwMC9hcGkvdjEvIn19fSwic3RhdHVzIjp7fX0K"
)

type Watcher interface {
	Updates() //<-chan []*server.Event
	Stop() error
}

type resourceWatcher struct {
	consumer ObjectStoreConsumer
	revision string
	actual   Bundled
	index    map[string]string
	key      string
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
		revision: "0", // cas
		chEvents: ch,
	}, nil
}

func (r *resourceWatcher) Updates() { // <-chan []*server.Event {
	var (
		err     error
		updates []*server.Event
	)

	t := time.NewTicker(updateInterval)
	//ch := make(chan []*server.Event, updateBufferSize)

	if updates, err = r.currentState(); err != nil {
		logrus.Errorf("error checking current state at watcher %s with revision %s: %s", r.key, r.revision, err.Error())
		if err != ErrKeyNotFound {
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
				logrus.Infof("context cancelled at watcher %s with revision %s", r.key, r.revision)
				t.Stop()
				return
			case <-t.C:
				logrus.Infof("ticker says it is time for checking for updates")

				if updates, err = r.checkForUpdates(); err != nil {
					logrus.Errorf("error checking for updates at watcher %s with revision %s: %s", r.key, r.revision, err.Error())
				} else {
					if len(updates) > 0 {
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

	r.actual, r.index, r.revision = newBundle, newIndex, newRevision

	e = make([]*server.Event, 0)
	for _, kv := range r.actual.List(noPrefix, noLimit) {
		kv := kv
		e = append(e, creationEvent(kv))
	}

	return
}

func (r *resourceWatcher) mockUpdate() (e []*server.Event) {
	e = make([]*server.Event, 1, 1)

	b, _ := NewFrom([]byte(mockBinaryValue))
	res, _ := b.Get("object-store-api-02")
	e = append(e, creationEvent(res))

	return
}

func (r *resourceWatcher) checkForUpdates() (e []*server.Event, err error) {
	var (
		newBundle   Bundled
		newIndex    map[string]string
		newRevision string
	)
	if newBundle, newIndex, newRevision, err = r.consumer.GetBundle(r.key); err != nil {
		return
	}

	if r.revision == newRevision {
		logrus.Infof("Revision is up to date: " + newRevision)
		return
	}
	logrus.Infof("Revision: " + r.revision + " was ourdated, new revision found: " + newRevision)

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

	r.actual, r.index, r.revision = newBundle, newIndex, newRevision

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

func (r *resourceWatcher) Stop() error {
	r.cancel()
	return nil
}

func creationEvent(kv *server.KeyValue) *server.Event {
	kv.CreateRevision = 10
	return &server.Event{
		Create: true,
		Delete: false,
		KV:     kv,
		PrevKV: nil,
	}
}

func modificationEvent(kvOld, kv *server.KeyValue) *server.Event {
	kv.ModRevision = 10
	return &server.Event{
		Create: false,
		Delete: false,
		KV:     kv,
		PrevKV: kvOld,
	}
}

func deletionEvent(kv *server.KeyValue) *server.Event {
	kv.ModRevision = 10
	return &server.Event{
		Create: false,
		Delete: true,
		KV:     kv,
		PrevKV: kv,
	}
}
