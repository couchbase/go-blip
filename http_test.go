package blip

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func Test_HTTPToBLIPRequest(t *testing.T) {
	body := "This is the body"
	bodyReader := bytes.NewBufferString(body)
	req, err := http.NewRequest("GET", "http://foo.com/db", bodyReader)
	assert.Equals(t, err, nil)
	req.Header.Add("Content-Type", "text/plain")

	msg := HTTPToBLIPRequest(req)

	assert.Equals(t, msg.Type(), RequestType)
	assert.Equals(t, msg.Profile(), "HTTP")
	assert.Equals(t, msg.Outgoing, true)
	gotBody, err := msg.Body()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(gotBody),
		"GET /db HTTP/1.1\r\n"+
			"Host: foo.com\r\n"+
			"User-Agent: Go 1.1 package http\r\n"+
			"Content-Length: 16\r\n"+
			"Content-Type: text/plain\r\n"+
			"\r\n"+
			"This is the body")
}

func Test_BLIPToHTTPRequest(t *testing.T) {
	msg := NewRequest()
	msg.SetProfile("HTTP")
	msg.SetBody([]byte("GET /db HTTP/1.1\r\n" +
		"Host: foo.com\r\n" +
		"User-Agent: Go 1.1 package http\r\n" +
		"Content-Length: 16\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"This is the body"))

	req, err := BLIPToHTTPRequest(msg)
	assert.Equals(t, err, nil)

	assert.Equals(t, req.Method, "GET")
	assert.Equals(t, req.URL.Path, "/db")
	assert.DeepEquals(t, req.Header["Content-Type"], []string{"text/plain"})
	body, err := ioutil.ReadAll(req.Body)
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, body, []byte("This is the body"))
}

func Test_ResponseWriter(t *testing.T) {
	// Make an incoming request:
	props := Properties{"Content-Type": "text/plain"}
	msg := NewParsedIncomingMessage(RequestType, props, []byte("Request data"))

	req, _ := BLIPToHTTPRequest(msg)
	responseMsg := msg.Response()
	r := makeResponseWriter(responseMsg, req)

	// Send an HTTP response:
	r.Header().Add("MyHeader", "17")
	r.Write([]byte("Response data"))
	r.Close()

	assert.Equals(t, responseMsg.Type(), ResponseType)
	assert.Equals(t, responseMsg.Profile(), "HTTP")
	assert.Equals(t, responseMsg.Outgoing, true)
	gotBody, err := responseMsg.Body()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(gotBody),
		"HTTP/1.1 200 OK\r\n"+
			"Content-Length: 13\r\n"+
			"Content-Type: text/plain; charset=utf-8\r\n"+
			"Myheader: 17\r\n"+
			"\r\n"+
			"Response data")

}
