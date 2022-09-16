package http

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	reqTimeoutError = errors.New("Request Timeout")
)

type HTTPClient struct {
	clients     []*http.Client
	clientIndex uint64
	clientCount uint64
}

type ClientConfig struct {
	MaxIdleConnsPerHost uint
	MaxConnsPerHost     uint
	IdleConnTimeoutSec  uint
	RequestTimeoutMS    uint
	ConnectionTimeoutMS uint
	KeepAliveSec        uint
	MaxHTTPClient       uint
}

func NewHTTPClient(cfg ClientConfig) (*HTTPClient, error) {
	clients := make([]*http.Client, cfg.MaxHTTPClient)
	var idx uint = 0
	for ; idx < cfg.MaxHTTPClient; idx++ {
		clients[idx] = &http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   (time.Duration)(cfg.ConnectionTimeoutMS) * time.Millisecond,
					KeepAlive: (time.Duration)(cfg.KeepAliveSec) * time.Second,
				}).Dial,
				DisableKeepAlives:   false,
				IdleConnTimeout:     time.Duration(cfg.IdleConnTimeoutSec) * time.Second,
				MaxIdleConnsPerHost: (int)(cfg.MaxIdleConnsPerHost / (uint)(cfg.MaxHTTPClient)),
				MaxConnsPerHost:     (int)(cfg.MaxConnsPerHost / (uint)(cfg.MaxHTTPClient)),
				TLSHandshakeTimeout: 10 * time.Second,
			},
			Timeout: time.Duration(cfg.RequestTimeoutMS) * time.Millisecond,
		}
	}
	return &HTTPClient{
		clientCount: (uint64)(cfg.MaxHTTPClient),
		clientIndex: 0,
	}, nil
}

func (client *HTTPClient) GetClient() *http.Client {
	index := atomic.AddUint64(&client.clientIndex, 1)
	if index > client.clientCount {
		atomic.SwapUint64(&client.clientIndex, 0)
	}

	return client.clients[index%client.clientCount]
}

type HTTPRequest struct {
	request    *http.Request
	response   *http.Response
	timedout   bool
	err        error
	respTimeMS int64
}

// create and return the HTTPRequest object
func NewHTTPRequest(ctx context.Context, url string, data []byte) (*HTTPRequest, error) {
	var request *http.Request
	var err error

	if nil == data {
		request, err = http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	} else {
		postBytes := bytes.NewBuffer(data)
		request, err = http.NewRequestWithContext(ctx, http.MethodPost, url, postBytes)
	}

	if nil != err {
		return nil, err
	}

	httpRequest := &HTTPRequest{
		request: request,
	}

	return httpRequest, nil
}

func (req *HTTPRequest) AddHeader(key, value string) {
	req.request.Header.Add(key, value)
}

func (req *HTTPRequest) Close() {
	if nil != req.request {
		req.Close()
	}

	if nil != req.response {
		req.response.Body.Close()
	}
}

func (req *HTTPRequest) GetResponseBody() ([]byte, error) {
	if nil == req.response {
		return nil, errors.New("http.Response is null")
	}

	body, err := ioutil.ReadAll(req.response.Body)
	if nil != err {
		return nil, err
	}

	return body, nil
}

func (req *HTTPRequest) GetResponseTimeMS() int64 {
	return req.respTimeMS
}

type MultiHTTPRequestContext struct {
	client       *http.Client
	httpRequests []*HTTPRequest
}

func NewMultiHTTPRequestContext(client *HTTPClient) *MultiHTTPRequestContext {
	return &MultiHTTPRequestContext{
		httpRequests: make([]*HTTPRequest, 0),
		client:       client.GetClient(),
	}
}

func (mCtx *MultiHTTPRequestContext) AddHTTPRequest(request *HTTPRequest) {
	mCtx.httpRequests = append(mCtx.httpRequests, request)
}

func cleanResponse(respChan chan *http.Response) {
	response := <-respChan
	if nil != response {
		response.Body.Close()
	}
}

func (req *HTTPRequest) execute(
	httpClient *http.Client,
	wg *sync.WaitGroup,
	startTime time.Time,
	timeoutCtx context.Context) {
	ctx, cancel := context.WithCancel(timeoutCtx)

	defer func() {
		cancel()
		wg.Done()
	}()

	respChan := make(chan *http.Response)
	var response *http.Response
	var err error
	go func() {
		response, err = httpClient.Do(req.request)
		respChan <- response
	}()

	select {
	case response := <-respChan:
		req.response = response
		req.err = err
		req.respTimeMS = time.Since(startTime).Milliseconds()

	case <-ctx.Done():
		req.timedout = true
		req.err = reqTimeoutError
		go cleanResponse(respChan)
	}
}

// make call to all request present in MultiHTTPRequestContext
func (mCtx *MultiHTTPRequestContext) Execute(ctx context.Context) {
	var wg sync.WaitGroup
	startTime := time.Now()
	for _, req := range mCtx.httpRequests {
		wg.Add(1)
		go req.execute(mCtx.client, &wg, startTime, ctx)
	}

	wg.Wait()
}
