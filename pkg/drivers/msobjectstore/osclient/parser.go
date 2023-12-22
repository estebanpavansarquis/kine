package osclient

import (
	types "github.com/estebanpavansarquis/kine/pkg/drivers/msobjectstore/flex"
	"github.com/estebanpavansarquis/kine/pkg/server"
)

func parseKeyValue(res types.GetResult, key string) (kv *server.KeyValue, err error) {
	var (
		resBundle Bundled
		ok        bool
	)

	if _, ok = res.GetValue().Metadata[key]; !ok {
		err = ErrKeyNotFound
		return
	}

	resBundle, err = parseBundle(res)

	if kv, ok = resBundle.Get(key); !ok {
		err = ErrKeyNotFound
		return
	}

	return
}

func parseBundle(res types.GetResult) (resBundle Bundled, err error) {
	if res.GetValue().ValueType != types.ObjectTypeBinary {
		err = ErrNotBinaryValue
		return
	}

	if res.GetValue().BinaryValue == "" {
		err = ErrNullValue
		return
	}

	resBundle, err = NewFrom([]byte(res.GetValue().BinaryValue))

	return
}
