package osclient

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	types "github.com/estebanpavansarquis/kine/pkg/drivers/msobjectstore/flex"
	"github.com/sirupsen/logrus"
)

const (
	defaultSlowThreshold = 500 * time.Millisecond
	defaultTimeOut       = 2 * time.Second

	versionHeader           = "etag"     // cas id
	ifMatchHeader           = "if-match" //  cas should match
	requestContentType      = "Content-Type"
	requestContentTypeValue = "application/json"
)

type store struct {
	client        HTTPClient
	slowThreshold time.Duration
}

func NewClient() types.Storage {
	return &store{
		client:        NewHTTPClient(defaultTimeOut),
		slowThreshold: defaultSlowThreshold,
	}
}

func (s *store) UpsertStore(store *types.Store) (err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fmt := "msobjectstore Client.UpsertStore %s err=%v, duration=%s"
		s.logMethod(dur, fmt, store.StoreID, err, dur)
	}()

	var body []byte
	if body, err = json.Marshal(store); err != nil {
		err = ErrClientOperationFailed
		return
	}

	headers := map[string]string{
		requestContentType: requestContentTypeValue,
	}

	var r *http.Response
	if r, err = s.client.Put(
		upsertStoreURL(store.StoreID),
		bytes.NewBuffer(body),
		headers,
	); err != nil {
		return
	}
	defer func() {
		if e := r.Body.Close(); e != nil {
			logrus.Errorf("msobjectstore Client.UpsertStore ERROR when closing response body:%v", e)
		}
	}()

	if r.StatusCode != http.StatusCreated {
		err = ErrClientResponseFailed
		logrus.Errorf("ErrClientResponseFailed: response status expected %d but was %s", http.StatusCreated, r.Status)
	}

	return
}

func (s *store) GetStores() (stores *types.Stores, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		var l int
		if stores != nil {
			l = len(stores.Values)
		}
		fmt := "msobjectstore Client.GetStores => len=%d, err=%v, duration=%s"
		s.logMethod(dur, fmt, l, err, dur)
	}()

	var r *http.Response
	if r, err = s.client.Get(getStoresURL()); err != nil {
		return
	}

	defer func() {
		if e := r.Body.Close(); e != nil {
			logrus.Errorf("msobjectstore Client.UpsertStore ERROR when closing response body:%v", e)
		}
	}()

	if r.StatusCode != http.StatusOK {
		err = ErrClientResponseFailed
		return
	}

	stores = new(types.Stores)
	if err = json.NewDecoder(r.Body).Decode(&stores); err != nil {
		logrus.Errorf("msobjectstore Client.GetKeys ERROR decoding response keys:%+v; error;%v", r.Body, err)
		return
	}

	return
}

func (s *store) GetPartitions(store string) (*types.Partitions, error) {
	// TODO: define store and partition correctly
	panic("implement me")
}

func (s *store) GetKeys(store, partition string) (keys *types.Keys, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		keyCount := 0
		if keys != nil {
			keyCount = len(keys.Values)
		}
		fmt := "msobjectstore Client.GetKeys store=%s, partition=%s => keys=%v, err=%v, duration=%s"
		s.logMethod(dur, fmt, store, partition, keyCount, err, dur)
	}()

	var r *http.Response
	if r, err = s.client.Get(
		getKeysURL(store, partition),
	); err != nil {
		return
	}
	defer func() {
		if e := r.Body.Close(); e != nil {
			logrus.Errorf("msobjectstore Client.Store ERROR when closing response body:%v", e.Error())
		}
	}()

	if r.StatusCode != http.StatusOK {
		err = ErrClientResponseFailed

		return
	}

	keys = new(types.Keys)
	if err = json.NewDecoder(r.Body).Decode(&keys); err != nil {
		logrus.Errorf("msobjectstore Client.GetKeys ERROR decoding response keys:%+v; error;%v", r.Body, err)
		return
	}

	return
}

func (s *store) Store(cmd types.StoreCmd) (err error) {
	start := time.Now()
	var r *http.Response
	defer func() {
		dur := time.Since(start)
		status := "failed"
		if r != nil {
			status = r.Status
		}
		fmt := "msobjectstore Client.Store %s, store=%s, partition:%s => status=%s, err=%v, duration=%s"
		s.logMethod(dur, fmt, cmd.GetKey(), cmd.GetStore(), cmd.GetPartition(), status, err, dur)
	}()

	var body []byte
	if body, err = json.Marshal(cmd.GetValue()); err != nil {
		err = ErrClientOperationFailed
		return
	}

	/*
		var prettyJSON bytes.Buffer
		_ = json.Indent(&prettyJSON, body, "", "\t")
		logrus.Infof("msobjectstore Client.Store JSON: %s, url: %s", string(prettyJSON.Bytes()), crudValueURL(cmd.GetStore(), cmd.GetPartition(), cmd.GetKey()))
	*/

	headers := map[string]string{
		requestContentType: requestContentTypeValue,
	}
	if cmd.IsMatch() {
		headers[ifMatchHeader] = cmd.GetMatchVersion()
	}

	if r, err = s.client.Post(
		crudValueURL(cmd.GetStore(), cmd.GetPartition(), cmd.GetKey()),
		bytes.NewBuffer(body),
		headers,
	); err != nil {
		logrus.Errorf("msobjectstore Client.Store ERROR: %v", err.Error())
		return
	}
	defer func() {
		if e := r.Body.Close(); e != nil {
			logrus.Errorf("msobjectstore Client.Store ERROR when closing response body:%v", e.Error())
		}
	}()

	if r.StatusCode != http.StatusCreated {
		err = ErrClientResponseFailed
		logrus.Errorf("ErrClientResponseFailed: response status expected %d but was %s", http.StatusCreated, r.Status)
	}

	return
}

func (s *store) GetValue(cmd types.GetCmd) (res types.GetResult, err error) {

	var r *http.Response

	/*
		start := time.Now()
		defer func() {
			dur := time.Since(start)
			fmt := "msobjectstore Client.GetValue %s store=%s, partition=%s => key=%s, version=%s, status=%q, err=%v, duration=%s"
			resKey := ""
			resVer := ""
			if res != nil && res.GetValue() != nil {
				resKey = res.GetValue().KeyID
				resVer = res.GetVersion()
			}
			status := "failed"
			if r != nil {
				status = r.Status
			}
			s.logMethod(dur, fmt, cmd.GetKey(), cmd.GetStore(), cmd.GetPartition(), resKey, resVer, status, err, dur)
		}()
	*/

	if r, err = s.client.Get(
		crudValueURL(cmd.GetStore(), cmd.GetPartition(), cmd.GetKey()),
	); err != nil {
		logrus.Errorf("msobjectstore Client.GetValue ERROR: %v", err.Error())
		return
	}
	defer func() {
		if e := r.Body.Close(); e != nil {
			logrus.Errorf("msobjectstore Client.Store ERROR when closing response body:%v", e.Error())
		}
	}()

	if r.StatusCode != http.StatusOK {
		if r.StatusCode == http.StatusNotFound {
			err = ErrKeyNotFound
		} else {
			logrus.Errorf("msobjectstore Client.GetValue status:%d, error:%s", r.StatusCode, err.Error())
			err = ErrClientResponseFailed
		}
		return
	}

	rObject := new(types.Object)
	if err = json.NewDecoder(r.Body).Decode(&rObject); err != nil {
		logrus.Errorf("msobjectstore Client.GetValue ERROR decoding response object, error:%s", err.Error())
		return
	}

	version := r.Header.Get(versionHeader)
	res = types.NewGetResult(rObject, version)

	return
}

// DeleteValue method not used
func (s *store) DeleteValue(store, partition, key string) (err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fmt := "msobjectstore Client.DeleteValue %s, store=%s, partition:%s => err=%v, duration=%s"
		s.logMethod(dur, fmt, key, store, partition, err, dur)
	}()

	var r *http.Response
	if r, err = s.client.Delete(
		crudValueURL(store, partition, key),
	); err != nil {
		logrus.Errorf("msobjectstore Client.DeleteValue ERROR: %v", err.Error())
		return
	}
	defer func() {
		if e := r.Body.Close(); e != nil {
			logrus.Errorf("msobjectstore Client.Store ERROR when closing response body:%v", e.Error())
		}
	}()

	if r.StatusCode != http.StatusNoContent {
		logrus.Errorf("msobjectstore Client.DeleteValue STATUS: %v", r.StatusCode)
		err = ErrClientResponseFailed
	}

	return
}

// DeletePartition method not used
func (s *store) DeletePartition(store, partition string) error {
	//TODO implement me
	panic("implement me")
}

func (s *store) logMethod(dur time.Duration, str string, args ...any) {
	//if dur > d.slowThreshold {
	//	logrus.Warnf(str, args...)
	//} else {
	//	logrus.Tracef(str, args...)
	//}
	logrus.Infof(str, args...)
}
