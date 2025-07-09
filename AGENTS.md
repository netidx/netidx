## Project Structure

- netidx-core

Core libraries

- netidx-derive

proc macro for deriving Pack automatically

- netidx-netproto

The wire protocol built using netidx-derive. Also the home of Value,
the core type of netidx values.

- netidx

The implementation of the publisher, subscriber, resolver server, and resolver client

- netidx-protocols

Higher level protocols such as RPC, channels, clustering, etc built on top of netidx

- netidx-archive

The netidx on disk archive format as well as the recorder implementation

- netidx-container

The netidx container server implementation, as simple nosql database for netidx

- netidx-wsproxy

Websocket proxy for using netidx from jawascript. uuuuutini

- netidx-bscript

The bscript language compiler and runtime implementation

- netidx-tools

The main binary containing all the command line tools including the
bscript shell/browser, publiser, subscriber, resolver server, archive,
container, etc
