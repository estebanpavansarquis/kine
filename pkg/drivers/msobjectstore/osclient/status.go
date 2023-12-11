package osclient

import (
	types "github.com/k3s-io/kine/pkg/drivers/msobjectstore/flex"
	"github.com/k3s-io/kine/pkg/drivers/msobjectstore/flex/stubs"
	"github.com/sirupsen/logrus"
)

const (
	statusObjectKey  = "object-store-status"
	mustCreateNewCAS = ""
)

func (c *consumer) Status() (s Status, cas string, err error) {
	getStatusCommand := c.newGetCmd(statusObjectKey)

	var statusGetResult types.GetResult
	if statusGetResult, err = c.objectStoreClient.GetValue(getStatusCommand); err != nil {
		if err != ErrKeyNotFound {
			return
		}

		if s, err = c.initStatus(); err != nil {
			return
		}

		return
	}

	if statusGetResult.GetValue().ValueType == types.ObjectTypeBinary {
		err = ErrStatusCheckFailed
	}

	s, err = NewStatusObjectFrom(statusGetResult.GetValue().BinaryValue)
	cas = statusGetResult.GetVersion()

	return
}

func (c *consumer) initStore() (err error) {
	var stores *types.Stores
	if stores, err = c.objectStoreClient.GetStores(); err != nil {
		return
	}

	if stores != nil {
		for _, st := range stores.Values {
			if st.StoreID == controlNodeStore {
				logrus.Info("osclient initStore: store was already created")
				return
			}
		}
	}

	logrus.Info("osclient initStore: creating store " + controlNodeStore)
	st := &types.Store{
		StoreID:                       controlNodeStore,
		DefaultTTLSeconds:             defaultTTLSeconds,
		MaximumEntries:                int(defaultMaximumEntries.Seconds()),
		DefaultConfirmationTTLSeconds: int(defaultConfirmationTTL.Seconds()),
	}

	return c.objectStoreClient.UpsertStore(st)
}

func (c *consumer) initStatus() (s Status, err error) {
	if err = c.initStore(); err != nil {
		return
	}

	s = NewStatusObject()
	logrus.Info("osclient initStatus: creating new status object %s", s.String())

	if err = c.storeStatus(s, mustCreateNewCAS, true); err != nil {
		s = nil
		return
	}

	return
}

func (c *consumer) storeStatus(s Status, cas string, mustCreate bool) (err error) {
	var data string
	if data, err = s.Encode(); err != nil {
		return
	}

	statusStoreCommand := c.newStoreCmd(
		statusObjectKey,
		stubs.NewObject(
			stubs.WithKey(statusObjectKey),
			stubs.WithBinaryValue(data),
		),
		cas,
		mustCreate,
	)

	return c.objectStoreClient.Store(statusStoreCommand)
}
