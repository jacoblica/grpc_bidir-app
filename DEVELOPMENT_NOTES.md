# Development Notes

## Project Overview

`grpc_bidir-app` is a C++ gRPC example demonstrating a **bidirectional streaming** RPC.

- `service.proto` — defines `BidirService.Stream` (bidirectional stream) plus `Request`/`Response` messages. Each message has 5 fields: `Opcode` enum, `int32`, `string`, `bool`, `float`.
- `server.cpp` — gRPC server, default listen address `0.0.0.0:50051`.
- `client.cpp` — gRPC client that opens a stream and sends PING + DATA requests.
- `CMakeLists.txt` / `build.sh` — build (protoc codegen + CMake).

Both sides intentionally leave placeholder blocks where the opcode is extracted and dispatched.

## Build & Run

```bash
./build.sh                                   # cleans build/, runs cmake + make
./build/client 0.0.0.0:50051 0.0.0.0:50052   # client LISTENS (one listener per address)
./build/server localhost:50051 localhost:50052 # server DIALS the clients
```

`client` and `server` both accept any number of addresses; with no arguments `client`
listens on `0.0.0.0:50051` and `server` dials `localhost:50051`. Start the client first.

## Session Changes

### 1. Client: multiple server support

- `main` now parses all CLI arguments as server addresses.
- One `BidirClient` per address, each driven on its own `std::thread`.
- Responses are tagged with the originating address.

### 2. Server: configurable listen address

- `RunServer` takes a `listen_address` parameter.
- `main` uses `argv[1]` when present, otherwise `0.0.0.0:50051`.

This was needed to run two server instances locally for testing.

### 3. Conversion to the asynchronous callback API

Previously both sides used the blocking/synchronous API (client `stub_->Stream(&context)` on a thread; server overriding `BidirService::Service::Stream`).

**Client**

- Uses a heap-allocated `ClientBidiReactor<Request, Response>`, bound with
  `stub->experimental_async()->Stream(context, reactor)` followed by `StartCall()`.
- `OnReadDone` dispatches responses and re-arms `StartRead`; `OnWriteDone` chains the queued writes; `OnDone` reports status and signals the waiting caller.
- The caller blocks on a `std::mutex` + `std::condition_variable` until `OnDone`.
- Messages in flight are held in member buffers so their lifetime spans the async operation.

**Server**

- `BidirServiceImpl` derives from `BidirService::CallbackService`.
- Overrides `Stream(CallbackServerContext*)` returning a heap-allocated `ServerBidiReactor<Request, Response>`.
- `StreamReactor` flow: `StartRead` -> `OnReadDone` (dispatch by opcode, build response) -> `StartWrite` -> `OnWriteDone` -> `StartRead`.
- `Finish(Status::OK)` when the client half-closes (`ok == false` on read); `OnCancel` finishes with `CANCELLED`; `OnDone` deletes the reactor.
- A `finished_` flag guards against calling `Finish` more than once.

### 4. Roles reversed: client listens, server dials

The two processes swapped gRPC roles while keeping the application behaviour identical
(client sends PING/DATA, server answers PONG/DATA RECEIVED).

- `client.cpp` is now the **gRPC server role**: it binds ports, accepts calls, and drives
  the request/response exchange from a `ServerBidiReactor`.
- `server.cpp` is now the **gRPC client role**: it dials each address and handles the
  exchange from a `ClientBidiReactor`.
- Each file still accepts several addresses: the client opens one listener per address,
  the server opens one call per address.

**`service.proto` had to be inverted** to `rpc Stream(stream Response) returns (stream Request)`.
gRPC fixes the wire direction: the client role always sends the method's request type. Since
the *listener* is now the gRPC server role, it cannot send `Request` messages unless the
declaration is reversed. After inverting, the reactor template arguments swap too:

- Listener: `ServerBidiReactor<Response, Request>` (reads `Response`, writes `Request`).
- Dialer: `ClientBidiReactor<Response, Request>` (writes `Response`, reads `Request`).

Note the differing convention between the two reactor templates: for
`ServerBidiReactor<A, B>` the first argument is inbound, for `ClientBidiReactor<A, B>` the
first is outbound.


## Gotchas / Lessons Learned

- **The server role has no `WritesDone`.** Only the gRPC client role can half-close its
  write side. A listening (server role) peer must instead end the call with `Finish(...)`,
  so the listener here counts responses and closes the call once every request has been
  answered and no write is still in flight (`MaybeFinish()`).
- **Do not use `StartWriteLast` for a request that must be delivered immediately.** It
  lets gRPC defer the message so it can be coalesced with the trailing metadata of
  `Finish`. Used for the second request it deadlocked the exchange: the peer waited forever
  for a message that never left the process, and killing the peer then tripped an internal
  `assertion failed: call_ == nullptr` in gRPC's callback tag handling. Plain `StartWrite`
  for every request fixes it.
- **Never call `Finish` with an operation still in flight**, and never call it twice; both
  trip the same internal gRPC assertion. `write_in_flight_` plus `finished_` guard the
  close path.
- **Client reactors have no operation backlog.** Unlike `ServerBidiReactor`, whose
  `StartRead`/`StartWrite` queue work even before the stream is bound, `ClientBidiReactor::StartRead`
  calls `stream_->Read(...)` directly. Calling `StartRead` in the reactor constructor (before
  `experimental_async()->Stream(...)`) dereferences a null `stream_` and segfaults. Post the
  first read **after** binding, before `StartCall()`.
- **`StartCall()` is mandatory** for every client reactor, even if the RPC is cancelled.
- **Message lifetime:** a message passed to `StartRead`/`StartWrite` must remain valid and
  unmodified until the corresponding `OnReadDone`/`OnWriteDone` fires. Member buffers are
  used for this reason.
- **Reactor template argument order differs** between the two callback reactors
  (`ServerBidiReactor` is `<inbound, outbound>`, `ClientBidiReactor` is `<outbound, inbound>`).
- Interleaved stdout from concurrent threads is expected; each line is tagged with the
  connection or server id, but is not serialized.
- Not used but available: completion-queue async (`BidirService::AsyncService::RequestStream`).

## Testing Performed

- Built cleanly via `./build.sh`.
- Client listening on `:50051`, server dialing it: client sent PING (opcode 1) and DATA
  (opcode 2); server received both and returned `PONG` / `DATA RECEIVED`; server exited 0.
- Two listeners (`:50051`, `:50052`) with one server dialing both concurrently: both
  streams completed with `OK`.
- 25 sequential connections: all 25 received both responses, no assertion failures, no
  hangs.
