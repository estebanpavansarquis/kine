package osclient

import (
	"context"
	"time"

	types "github.com/k3s-io/kine/pkg/drivers/msobjectstore/flex"
	"github.com/k3s-io/kine/pkg/drivers/msobjectstore/flex/stubs"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

const (
	NotFoundIgnored    = true
	NotFoundNotIgnored = false

	controlNodeStore       = "control-node-store"
	controlNodePartition   = "control-node-partition"
	defaultTTLSeconds      = 1209600
	defaultMaximumEntries  = time.Hour * 24
	defaultConfirmationTTL = time.Second * 10

	healthCheckKey   = "health"
	healthCheckValue = "{\"health\":\"true\"}"
)

type ObjectStoreConsumer interface {
	Get(bundleKey, resourceKey string, ignoreNotFound bool) (kvs *server.KeyValue, err error)
	List(bundleKey, resourcePrefix string, limit int64) (kvs []*server.KeyValue, err error)
	Create(bundleKey, resourceKey string, kv *server.KeyValue) (err error)
	Update(bundleKey, resourceKey string, kv *server.KeyValue) (ok bool, err error)
	Delete(bundleKey, resourceKey string) (old *server.KeyValue, err error)
	HealthCheck() (ok bool, err error)
	Watch(ctx context.Context, prefix string, ch chan []*server.Event) (Watcher, error)
	Size() (size int64, err error)
	GetBundle(bundleKey string) (Bundled, map[string]string, string, error)
}

type consumer struct {
	store             string
	partition         string
	objectStoreClient types.Storage
	watchers          []Watcher
}

func NewConsumer() ObjectStoreConsumer {
	st := types.Store{
		StoreID:                       controlNodeStore,
		DefaultTTLSeconds:             defaultTTLSeconds,
		MaximumEntries:                int(defaultMaximumEntries.Seconds()),
		DefaultConfirmationTTLSeconds: int(defaultConfirmationTTL.Seconds()),
	}

	osCli := NewClient()
	// TODO: store can be already created by another replica
	for {
		err := osCli.UpsertStore(&st)
		if err == nil {
			break
		}
		time.Sleep(3 * time.Second)
	}

	return &consumer{
		store:             controlNodeStore,
		partition:         controlNodePartition,
		objectStoreClient: osCli,
	}
}

func (c *consumer) HealthCheck() (ok bool, err error) {
	getHealthCommand := c.newGetCmd(healthCheckKey)

	var healthGetResult types.GetResult
	if healthGetResult, err = c.objectStoreClient.GetValue(getHealthCommand); err != nil {
		if err == ErrKeyNotFound {
			currentTime := time.Now().String()
			healthStoreCommand := c.newStoreCmd(
				healthCheckKey,
				stubs.NewObject(
					stubs.WithKey(healthCheckKey),
					stubs.WithBinaryValue(healthCheckValue),
					stubs.WithMetadata(map[string]string{
						"health":  "true",
						"created": currentTime,
						"updated": currentTime,
					}),
				),
			)
			if err = c.objectStoreClient.Store(healthStoreCommand); err != nil {
				return
			}
			logrus.Info("msobjectstore driver.Start: health check entry successfully stored")

			return c.HealthCheck()
		}
		return
	}

	if healthGetResult.GetValue().BinaryValue != healthCheckValue {
		err = ErrHealthCheckFailed
	}

	return
}

func (c *consumer) Get(bundleKey, resourceKey string, ignoreNotFound bool) (res *server.KeyValue, err error) {
	getCommand := c.newGetCmd(bundleKey)

	var getResult types.GetResult
	if getResult, err = c.objectStoreClient.GetValue(getCommand); err != nil {
		if err == ErrKeyNotFound && ignoreNotFound {
			err = nil
		}
		return
	}

	if res, err = parseKeyValue(getResult, resourceKey); err == ErrKeyNotFound && ignoreNotFound {
		err = nil
	}

	return
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
	resourceBundle := NewBundle()

	getCommand := c.newGetCmd(bundleKey)
	if getResult, err = c.objectStoreClient.GetValue(getCommand); err != nil {
		if err != ErrKeyNotFound {
			return
		}
		err = nil
	} else {
		if _, ok := getResult.GetValue().Metadata[resourceKey]; ok {
			err = ErrKeyAlreadyExists
			return
		}
		if resourceBundle, err = parseBundle(getResult); err != nil {
			return
		}
	}

	resourceBundle.Upsert(resourceKey, kv)

	var data string
	if data, err = resourceBundle.Encode(); err != nil {
		return
	}

	var metadata map[string]string
	if metadata, err = resourceBundle.Index(); err != nil {
		return
	}
	currentTime := time.Now().String()
	metadata["created"] = currentTime
	metadata["updated"] = currentTime

	storeCommand := c.newStoreCmd(
		bundleKey,
		stubs.NewObject(
			stubs.WithKey(bundleKey),
			stubs.WithBinaryValue(data),
			stubs.WithMetadata(metadata),
		))
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
	resBundle.Upsert(resourceKey, kv)

	var data string
	if data, err = resBundle.Encode(); err != nil {
		return
	}

	var metadata map[string]string
	if metadata, err = resBundle.Index(); err != nil {
		return
	}
	currentTime := time.Now().String()
	metadata["created"] = getResult.GetValue().Metadata["created"]
	metadata["updated"] = currentTime

	storeCommand := c.newStoreCmd(
		bundleKey,
		stubs.NewObject(
			stubs.WithKey(bundleKey),
			stubs.WithBinaryValue(data),
			stubs.WithMetadata(metadata),
		))
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
	metadata["updated"] = time.Now().String()

	storeCommand := c.newStoreCmd(
		bundleKey,
		stubs.NewObject(
			stubs.WithKey(bundleKey),
			stubs.WithBinaryValue(data),
			stubs.WithMetadata(metadata),
		))
	err = c.objectStoreClient.Store(storeCommand)

	return
}

func (c *consumer) Watch(ctx context.Context, key string, ch chan []*server.Event) (w Watcher, err error) {
	return NewResourceWatcher(ctx, c, key, ch)
}

func (c *consumer) Size() (size int64, err error) {
	var keys *types.Keys
	if keys, err = c.objectStoreClient.GetKeys(controlNodeStore, controlNodePartition); err != nil {
		return
	}

	size = int64(len(keys.Values))

	return
}

func (c *consumer) newGetCmd(key string) types.GetCmd {
	return types.NewGetCommand(
		c.store,
		c.partition,
		key,
	)
}

func (c *consumer) newStoreCmd(key string, value *types.Object) types.StoreCmd {
	return types.NewStoreCmd(
		c.store,
		c.partition,
		key,
		value,
	)
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
