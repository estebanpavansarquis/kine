package osclient

import (
	"io"
	"net/http"
	"time"
)

type HTTPClient interface {
	Get(url string) (resp *http.Response, err error)
	Put(url string, body io.Reader, headers map[string]string) (resp *http.Response, err error)
	Post(url string, body io.Reader, headers map[string]string) (resp *http.Response, err error)
	Delete(url string) (resp *http.Response, err error)
}

type httpClient struct {
	client *http.Client
}

func NewHTTPClient(timeOut time.Duration) HTTPClient {
	c := new(httpClient)
	c.client = &http.Client{Timeout: timeOut}

	return c
}

func (c *httpClient) Get(url string) (resp *http.Response, err error) {
	return c.client.Get(url)
}

func (c *httpClient) Post(url string, body io.Reader, headers map[string]string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return c.client.Do(req)
}

func (c *httpClient) Put(url string, body io.Reader, headers map[string]string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return c.client.Do(req)
}

func (c *httpClient) Delete(url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}

	return c.client.Do(req)
}
