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
./build/server 0.0.0.0:50051                 # listen address is optional (default port 50051)
./build/client localhost:50051 localhost:50052
```

`client` accepts any number of server addresses; with no arguments it defaults to `localhost:50051`.

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

**Server**

- `BidirServiceImpl` now derives from `BidirService::CallbackService`.
- Overrides `Stream(CallbackServerContext*)` returning a heap-allocated `ServerBidiReactor<Request, Response>`.
- `StreamReactor` flow: `StartRead` -> `OnReadDone` (dispatch by opcode, build response) -> `StartWrite` -> `OnWriteDone` -> `StartRead`.
- `Finish(Status::OK)` when the client half-closes (`ok == false` on read); `OnCancel` finishes with `CANCELLED`; `OnDone` deletes the reactor.
- A `finished_` flag guards against calling `Finish` more than once.

**Client**

- Uses a heap-allocated `ClientBidiReactor<Request, Response>`, bound with
  `stub->experimental_async()->Stream(context, reactor)` followed by `StartCall()`.
- `OnReadDone` dispatches responses and re-arms `StartRead`; `OnWriteDone` chains the queued writes; `OnDone` reports status and signals the waiting caller.
- The caller blocks on a `std::mutex` + `std::condition_variable` until `OnDone`.
- Messages in flight are held in member buffers so their lifetime spans the async operation.

## Gotchas / Lessons Learned

- **Client reactors have no operation backlog.** Unlike `ServerBidiReactor`, whose `StartRead`/`StartWrite` queue work even before the stream is bound, `ClientBidiReactor::StartRead` calls `stream_->Read(...)` directly. Calling `StartRead` in the reactor constructor (before `experimental_async()->Stream(...)`) dereferences a null `stream_` and segfaults. Post the first read **after** binding, before `StartCall()`.
- **`StartCall()` is mandatory** for every client reactor, even if the RPC is cancelled.
- **Message lifetime:** a message passed to `StartRead`/`StartWrite` must remain valid and unmodified until the corresponding `OnReadDone`/`OnWriteDone` fires. Member buffers are used for this reason.
- **`Finish` may only be called once** on a server reactor; the `finished_` guard handles the read-end vs. cancel race.
- **Reactor lifetime:** heap-allocate the reactor and `delete this` in `OnDone` (called after all other reactions, and after all holds are removed).
- Interleaved stdout from concurrent client threads is expected; each line is address-tagged but not serialized.
- Not used but available: completion-queue async (`BidirService::AsyncService::RequestStream`).

## Testing Performed

- Built cleanly via `./build.sh`.
- Ran two callback servers (`:50051`, `:50052`) and one client against both.
- Verified each server received PING (opcode 1) and DATA (opcode 2) and returned `PONG` / `DATA RECEIVED`.
- Repeated single-server runs to confirm stability (exit code 0).
