package bliphttp

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"

	"github.com/snej/go-blip"
)

func HTTPToBLIPRequest(httpReq *http.Request) *blip.Message {
	req := blip.NewRequest()
	req.SetProfile("HTTP")
	var body bytes.Buffer
	httpReq.Write(&body)
	req.SetBody(body.Bytes())
	return req
}

func BLIPToHTTPResponse(blipResponse *blip.Message, httpRequest *http.Request) (*http.Response, error) {
	if blipResponse.Type() == blip.ErrorType {
		return nil, fmt.Errorf("BLIP error!") //FIX: IMPLEMENT
	}
	body, err := blipResponse.BodyReader()
	if err != nil {
		return nil, err
	}
	return http.ReadResponse(bufio.NewReader(body), httpRequest)
}

// Creates an HTTP Client that will send its requests over the given BLIP connection.
func NewHTTPClient(sender *blip.Sender) *http.Client {
	return &http.Client{Transport: &blipTransport{sender}}
}

type blipTransport struct {
	sender *blip.Sender
}

func (bt blipTransport) RoundTrip(httpReq *http.Request) (resp *http.Response, err error) {
	blipReq := HTTPToBLIPRequest(httpReq)
	bt.sender.Send(blipReq)
	return BLIPToHTTPResponse(blipReq.Response(), httpReq) // This blocks till the response arrives
}
