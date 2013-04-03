# BLIP-Over-WebSocket Implementation for Go

This is a [Go][GO] language [golang] implementation of the [BLIP][BLIP] messaging protocol running over [WebSockets][WEBSOCKET].

## Why?

BLIP adds several useful features that aren't supported directly by WebSocket:

* Request/response: Messages can have responses, and the responses don't have to be sent in the same order as the original messages. Responses are optional; a message can be sent in no-reply mode if it doesn't need one, otherwise a response (even an empty one) will always be sent after the message is handled.
* Metadata: Messages are structured, with a set of key/value headers and a binary body, much like HTTP or MIME messages. Peers can use the metadata to route incoming messages to different handlers, effectively creating multiple independent channels on the same connection.
* Multiplexing: Large messages are broken into fragments, and if multiple messages are ready to send their fragments will be interleaved on the connection, so they're sent in parallel. This prevents huge messages from blocking the connection.
* Priorities: Messages can be marked Urgent, which gives them higher priority in the multiplexing (but without completely starving normal-priority messages.) This is very useful for streaming media.

## Status

This is still pretty early; I wouldn't recommend using it yet. Only listener/server mode is implemented yet: you can't actively open a new connection, and you can't send requests, only reply to them. --Jens, 3 April 2013

[GO]: http://golang.org
[BLIP]: https://bitbucket.org/snej/mynetwork/wiki/BLIP/Overview
[WEBSOCKET]: http://www.websocket.org
