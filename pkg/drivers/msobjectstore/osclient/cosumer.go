package osclient

import (
	"context"
	"sync"
	"time"

	types "github.com/estebanpavansarquis/kine/pkg/drivers/msobjectstore/flex"
	"github.com/estebanpavansarquis/kine/pkg/drivers/msobjectstore/flex/stubs"
	"github.com/estebanpavansarquis/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

const (
	controlNodeStore       = "control-node-store"
	controlNodePartition   = "control-node-partition"
	defaultTTLSeconds      = 1209600
	defaultMaximumEntries  = time.Hour * 24
	defaultConfirmationTTL = time.Second * 10
)

type ObjectStoreConsumer interface {
	Get(bundleKey, resourceKey string) (kvs *server.KeyValue, err error)
	List(bundleKey, resourcePrefix string, limit int64) (kvs []*server.KeyValue, err error)
	Create(bundleKey, resourceKey string, kv *server.KeyValue) (err error)
	Update(bundleKey, resourceKey string, kv *server.KeyValue) (ok bool, err error)
	Delete(bundleKey, resourceKey string) (old *server.KeyValue, err error)
	GetBundle(bundleKey string) (Bundled, map[string]string, string, error)
	Size() (size int64, err error)
	Watch(ctx context.Context, prefix string, ch chan []*server.Event) (Watcher, error)
	Status() (status Status, cas string, err error)
	Revision() (rev int64, err error)
	IncrementRevision() (rev int64, err error)
	//Lock() (success bool, lockingKey string, err error)
	//Unlock() (success bool, err error)
}

type consumer struct {
	store             string
	partition         string
	objectStoreClient types.Storage
	watchers          []Watcher
	muRev             sync.RWMutex
}

func NewConsumer() (osc ObjectStoreConsumer, err error) {
	osCli := NewClient()

	c := &consumer{
		store:             controlNodeStore,
		partition:         controlNodePartition,
		objectStoreClient: osCli,
	}

	var s Status
	if s, _, err = c.Status(); err != nil {
		return
	}

	logrus.Infof("osclient NewConsumer: ObjectStoreConsumer was succefully created with status %s", s.String())

	return c, nil
}

func (c *consumer) Get(bundleKey, resourceKey string) (res *server.KeyValue, err error) {
	getCommand := c.newGetCmd(bundleKey)

	var getResult types.GetResult
	if getResult, err = c.objectStoreClient.GetValue(getCommand); err != nil {
		return
	}

	return parseKeyValue(getResult, resourceKey)
}

func (c *consumer) List(bundleKey, resourcePrefix string, limit int64) (kvs []*server.KeyValue, err error) {
	var getResult types.GetResult
	getCommand := c.newGetCmd(bundleKey)
	if getResult, err = c.objectStoreClient.GetValue(getCommand); err != nil {
		if err == ErrKeyNotFound {
			err = nil
		}
		if err != nil {
			logrus.Error("msobjectstore driver.LIST %s failed when store.GetValue(): ", bundleKey, err.Error())
		}
		return
	}

	var resBundle Bundled
	if resBundle, err = parseBundle(getResult); err != nil {
		logrus.Error("msobjectstore driver.LIST failed when parseBundleFromObjectValue: ", err.Error())
		return
	}

	kvs = resBundle.List(resourcePrefix, limit)

	return
}

func (c *consumer) Create(bundleKey, resourceKey string, kv *server.KeyValue) (err error) {
	var getResult types.GetResult
	resBundle := NewBundle()

	var mustCreateBundle bool
	getCommand := c.newGetCmd(bundleKey)
	if getResult, err = c.objectStoreClient.GetValue(getCommand); err != nil {
		if err != ErrKeyNotFound {
			return
		}
		err = nil
		mustCreateBundle = true
	} else {
		if _, ok := getResult.GetValue().Metadata[resourceKey]; ok {
			err = ErrKeyAlreadyExists
			return
		}
		if resBundle, err = parseBundle(getResult); err != nil {
			return
		}
		if ok := resBundle.Contains(resourceKey); ok {
			err = ErrKeyAlreadyExists
			return
		}
	}

	resBundle.Upsert(resourceKey, kv)

	var data string
	if data, err = resBundle.Encode(); err != nil {
		return
	}

	cas := mustCreateNewCAS
	if !mustCreateBundle {
		cas = getResult.GetVersion()
	}
	storeCommand := c.newStoreCmd(bundleKey,
		stubs.NewObject(
			stubs.WithKey(bundleKey),
			stubs.WithBinaryValue(data),
			stubs.WithMetadata(resBundle.Index()),
		),
		cas,
		mustCreateBundle,
	)

	return c.objectStoreClient.Store(storeCommand)
}

func (c *consumer) Update(bundleKey, resourceKey string, kv *server.KeyValue) (ok bool, err error) {
	var getResult types.GetResult
	getCommand := c.newGetCmd(bundleKey)
	if getResult, err = c.objectStoreClient.GetValue(getCommand); err != nil {
		if err == ErrKeyNotFound {
			err = nil
		}
		ok = false
		return
	} else {
		if _, ok = getResult.GetValue().Metadata[resourceKey]; !ok {
			ok = false
			return
		}
	}

	var resBundle Bundled
	if resBundle, err = parseBundle(getResult); err != nil {
		return
	}

	var oldkv *server.KeyValue
	if oldkv, ok = resBundle.Get(resourceKey); !ok {
		ok = false
		return
	}

	kv.CreateRevision = oldkv.CreateRevision
	resBundle.Upsert(resourceKey, kv)

	var data string
	if data, err = resBundle.Encode(); err != nil {
		return
	}

	storeCommand := c.newStoreCmd(
		bundleKey,
		stubs.NewObject(
			stubs.WithKey(bundleKey),
			stubs.WithBinaryValue(data),
			stubs.WithMetadata(resBundle.Index()),
		),
		getResult.GetVersion(),
		false,
	)
	err = c.objectStoreClient.Store(storeCommand)

	return
}

func (c *consumer) Delete(bundleKey, resourceKey string) (kv *server.KeyValue, err error) {
	var (
		getResult types.GetResult
		ok        bool
	)

	getCommand := c.newGetCmd(bundleKey)
	if getResult, err = c.objectStoreClient.GetValue(getCommand); err != nil {
		if err != ErrKeyNotFound {
			err = nil
			ok = true
		}
		return
	} else {
		if _, ok = getResult.GetValue().Metadata[resourceKey]; !ok {
			return
		}
	}

	var resBundle Bundled
	if resBundle, err = parseBundle(getResult); err != nil {
		return
	}

	if kv, ok = resBundle.Get(resourceKey); !ok {
		return
	}

	resBundle.Remove(resourceKey)
	data, err := resBundle.Encode()
	if err != nil {
		return
	}

	metadata := getResult.GetValue().Metadata
	delete(metadata, resourceKey)

	storeCommand := c.newStoreCmd(
		bundleKey,
		stubs.NewObject(
			stubs.WithKey(bundleKey),
			stubs.WithBinaryValue(data),
			stubs.WithMetadata(metadata),
		),
		getResult.GetVersion(),
		false,
	)
	err = c.objectStoreClient.Store(storeCommand)

	return
}

func (c *consumer) Watch(ctx context.Context, key string, ch chan []*server.Event) (w Watcher, err error) {
	return NewResourceWatcher(ctx, c, key, ch)
}

// TODO: should return size in bytes
func (c *consumer) Size() (size int64, err error) {
	var keys *types.Keys
	if keys, err = c.objectStoreClient.GetKeys(controlNodeStore, controlNodePartition); err != nil {
		return
	}

	size = int64(len(keys.Values))

	return
}

func (c *consumer) GetBundle(bundleKey string) (resBundle Bundled, index map[string]string, rev string, err error) {
	getCommand := c.newGetCmd(bundleKey)

	var getResult types.GetResult
	if getResult, err = c.objectStoreClient.GetValue(getCommand); err != nil {
		return
	}

	if resBundle, err = parseBundle(getResult); err == ErrKeyNotFound {
		err = nil
	}

	index = getResult.GetValue().Metadata

	rev = getResult.GetVersion()

	return
}

func (c *consumer) newGetCmd(key string) types.GetCmd {
	return types.NewGetCommand(
		c.store,
		c.partition,
		key,
	)
}

func (c *consumer) newStoreCmd(key string, value *types.Object, cas string, mustCreate bool) types.StoreCmd {
	if mustCreate || cas == mustCreateNewCAS {
		return types.NewStoreCmd(
			c.store,
			c.partition,
			key,
			value,
			types.WithNoneMatch("*"),
		)
	}
	return types.NewStoreCmd(
		c.store,
		c.partition,
		key,
		value,
		types.WithMatches(cas),
	)
}
