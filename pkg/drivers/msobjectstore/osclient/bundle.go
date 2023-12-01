package osclient

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

type Bundled interface {
	Contains(string) bool
	Upsert(string, *server.KeyValue) bool
	Remove(string) bool
	Get(string) (*server.KeyValue, bool)
	List(string, int64) []*server.KeyValue
	Encode() (string, error)
	Index() map[string]string
	//Diff(Bundled) (added, modified, deleted []server.KeyValue, diff bool)
}

type resourceBundle struct {
	Data map[string]*server.KeyValue `json:"data"`
}

//func (b *resourceBundle) Diff(b2 Bundled) (added, modified, deleted []server.KeyValue, diff bool) {
//
//	return nil, nil, nil, false
//}

func NewBundle() Bundled {
	return &resourceBundle{Data: make(map[string]*server.KeyValue, 1)}
}

func NewFrom(bin []byte) (Bundled, error) {
	var rb *resourceBundle
	if e := json.Unmarshal(bin, &rb); e != nil {
		logrus.Errorf("error while unmarshalling binary value from store: %s", e.Error())
		return nil, ErrKeyCastingFailed
	}
	return rb, nil
}

func (b *resourceBundle) Encode() (data string, err error) {
	var kvb []byte
	if kvb, err = json.Marshal(b); err != nil {
		err = ErrMarshallFailed
		return
	}

	data = string(kvb)

	return
}

// Index returns a map with every resource name as keys
func (b *resourceBundle) Index() (index map[string]string) {
	index = make(map[string]string, len(b.Data))

	for k, v := range b.Data {
		index[k] = fmt.Sprint(crc32.ChecksumIEEE(v.Value))
	}

	return
}

func (b *resourceBundle) Get(resourceName string) (data *server.KeyValue, ok bool) {
	if b.Data == nil || len(b.Data) == 0 {
		return
	}

	data, ok = b.Data[resourceName]

	return
}

func (b *resourceBundle) List(filter string, limit int64) (kvs []*server.KeyValue) {
	if len(b.Data) == 0 {
		return
	}

	size := int64(len(b.Data))
	if limit > 0 && limit < size {
		size = limit
	}

	kvs = make([]*server.KeyValue, 0, size)

	for k, v := range b.Data {
		if filter == "" || strings.HasPrefix(k, filter) {
			kvs = append(kvs, v)
		}

		if int64(len(kvs)) == size {
			break
		}
	}

	return
}

func (b *resourceBundle) Contains(resourceName string) (ok bool) {
	if b.Data == nil || len(b.Data) == 0 {
		ok = false
		return
	}

	_, ok = b.Data[resourceName]

	return
}

func (b *resourceBundle) Upsert(resourceName string, data *server.KeyValue) (ok bool) {
	if b.Data == nil || len(b.Data) == 0 {
		b.Data = map[string]*server.KeyValue{}
	}

	b.Data[resourceName] = data
	ok = true

	return
}

func (b *resourceBundle) Remove(resourceName string) (ok bool) {
	if b.Data == nil || len(b.Data) == 0 {
		ok = true
		return
	}

	delete(b.Data, resourceName)
	ok = true

	return
}
