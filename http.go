package blip

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

func blipToHTTPRequest(blipRequest *Message) (*http.Request, error) {
	body, err := blipRequest.BodyReader()
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(body)
	request, err := http.ReadRequest(reader)
	if err != nil {
		return nil, err
	}
	if request.Header["Content-Length"] == nil {
		// If there is no Content-Length, the HTTP parser won't read any of the body.
		// So instead, assign the remainder of the message to the HTTP body.
		request.Body = ioutil.NopCloser(reader)
	}
	return request, nil
}

//////// RESPONSE WRITER:

// Implements http.ResponseWriter interface
type responseWriter struct {
	blipResponse *Message
	httpResponse http.Response
	body         bytes.Buffer
}

func makeResponseWriter(blipResponse *Message, httpRequest *http.Request) *responseWriter {
	return &responseWriter{
		blipResponse: blipResponse,
		httpResponse: http.Response{
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     http.Header{},
			Request:    httpRequest,
		},
	}
}

func (r *responseWriter) Header() http.Header {
	return r.httpResponse.Header
}

func (r *responseWriter) WriteHeader(status int) {
	r.httpResponse.StatusCode = status
}

func (r *responseWriter) finishHeader(contentType string) {
	if r.httpResponse.StatusCode == 0 {
		if _, exists := r.httpResponse.Header["Content-Type"]; !exists && len(contentType) > 0 {
			r.httpResponse.Header["Content-Type"] = []string{contentType}
		}
		r.WriteHeader(http.StatusOK)
	}
}

func (r *responseWriter) Write(data []byte) (bytesWritten int, err error) {
	r.finishHeader(http.DetectContentType(data))
	return r.body.Write(data)
}

func (r *responseWriter) Close() {
	r.finishHeader("")
	body := r.body.Bytes()
	r.httpResponse.ContentLength = int64(len(body))
	r.httpResponse.Body = ioutil.NopCloser(bytes.NewReader(body))

	var out bytes.Buffer
	r.httpResponse.Write(&out)
	r.blipResponse.SetBody(out.Bytes())
	r.blipResponse.SetProfile("HTTP")
}

//////// REQUEST HANDLER:

// Registers a handler for HTTP requests with a BLIP Context and returns the ServeMux that
// routes the requests. You should register your HTTP handlers with the ServeMux.
func addHTTPHandler(context *Context) *http.ServeMux {
	mux := http.NewServeMux()
	handler := func(request *Message) {
		httpReq, _ := blipToHTTPRequest(request) //FIX: Handle error
		httpRes := makeResponseWriter(request.Response(), httpReq)
		mux.ServeHTTP(httpRes, httpReq)
		httpRes.Close()
		//log.Printf("Response = %v", request.Response())
	}
	context.HandlerForProfile["HTTP"] = handler
	return mux
}

//////// CLIENT SIDE:

func httpToBLIPRequest(httpReq *http.Request) *Message {
	req := NewRequest()
	req.SetProfile("HTTP")
	var body bytes.Buffer
	httpReq.Write(&body)
	req.SetBody(body.Bytes())
	return req
}

func blipToHTTPResponse(blipResponse *Message, httpRequest *http.Request) (*http.Response, error) {
	if blipResponse.Type() == ErrorType {
		return nil, fmt.Errorf("BLIP error!") //FIX: IMPLEMENT
	}
	body, err := blipResponse.BodyReader()
	if err != nil {
		return nil, err
	}
	return http.ReadResponse(bufio.NewReader(body), httpRequest)
}

type blipTransport struct {
	sender *Sender
}

// Creates an HTTP Client that will send its requests over the given BLIP connection.
func newHTTPClient(sender *Sender) *http.Client {
	return &http.Client{Transport: &blipTransport{sender}}
}

func (bt blipTransport) RoundTrip(httpReq *http.Request) (resp *http.Response, err error) {
	blipReq := httpToBLIPRequest(httpReq)
	bt.sender.Send(blipReq)
	return blipToHTTPResponse(blipReq.Response(), httpReq) // This blocks till the response arrives
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
