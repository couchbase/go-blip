package bliphttp

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/snej/go-blip"
)

func PropertiesToHeader(properties blip.Properties, header http.Header) {
	for prop, value := range properties {
		if strings.HasPrefix(prop, "HTTP-") {
			header[prop[5:]] = []string{value}
		}
	}
}

func BLIPToHTTPRequest(blipRequest *blip.Message) (*http.Request, error) {
	body, err := blipRequest.BodyReader()
	if err != nil {
		return nil, err
	}
	if body == nil {
		panic("nil body")
	}
	return http.ReadRequest(bufio.NewReader(body))
	/*
		method := blipRequest.Properties["Method"]
		if method == "" {
			method = "GET"
		}
		request, err := http.NewRequest(method, blipRequest.Properties["URI"], body)
		if err != nil {
			return nil, err
		}
		PropertiesToHeader(blipRequest.Properties, request.Header)
		return request, nil
	*/
}

//////// RESPONSE WRITER:

// Implements http.ResponseWriter interface
type responseWriter struct {
	blipResponse *blip.Message
	httpResponse http.Response
	body         bytes.Buffer
}

func MakeResponseWriter(blipResponse *blip.Message, httpRequest *http.Request) http.ResponseWriter {
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

	/*
		for prop, values := range r.header {
			r.blipResponse.Properties["HTTP-"+prop] = strings.Join(values, ", ")
		}
		if status != http.StatusOK {
			r.blipResponse.Properties["Status"] = fmt.Sprintf("%d", status)
		}
	*/
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
func AddHTTPHandler(context *blip.Context) *http.ServeMux {
	mux := http.NewServeMux()
	handler := func(request *blip.Message) {
		httpReq, _ := BLIPToHTTPRequest(request)//FIX: Handle error
		httpRes := MakeResponseWriter(request.Response(), httpReq)
		mux.ServeHTTP(httpRes, httpReq)
	}
	context.HandlerForProfile["HTTP"] = handler
	return mux
}
