module github.com/couchbase/go-blip/example

go 1.23

require (
	github.com/couchbase/go-blip v0.0.0-00010101000000-000000000000
	github.com/gorilla/mux v1.8.0
	github.com/spf13/cobra v1.4.0
)

require (
	github.com/coder/websocket v1.8.12 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
)

replace github.com/couchbase/go-blip => ../
