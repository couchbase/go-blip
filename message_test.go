package blip

import (
	"testing"
)

func BenchmarkMessageEncoding(b *testing.B) {
	msg := NewRequest()
	msg.SetProfile("BLIPTest/EchoData")
	msg.Properties["Content-Type"] = "application/octet-stream"
	body := make([]byte, 10000)
	for i := 0; i < len(body); i++ {
		body[i] = byte(i % 256)
	}
	msg.Body = body
	msg.number = 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.encoded = nil
		for {
			_, flags := msg.nextFrameToSend(4090)
			if flags&kMoreComing == 0 {
				break
			}
		}
	}
}
