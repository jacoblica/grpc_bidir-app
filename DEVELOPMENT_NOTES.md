# Development Notes

## Project Overview

`grpc_bidir-app` is a C++ gRPC example demonstrating a **bidirectional streaming** RPC with the
connection direction inverted: the `client` process is the gRPC **server** (it listens) and the
`server` process is the gRPC **client** (it dials out and pushes).

- `service.proto` — defines `BidirService.Stream` (bidirectional stream) plus `Request`/`Response` messages. Each message has 5 fields: `Opcode` enum, `int32`, `string`, `bool`, `float`.
- `client.cpp` — the **listener**: hosts the gRPC server (default listen address `0.0.0.0:50051`) and only receives.
- `server.cpp` — the **pusher**: dials one or more listeners and writes a formatted date/time string every 3 seconds.
- `CMakeLists.txt` / `build.sh` — build (protoc codegen + CMake).

Both sides use the **synchronous/blocking API** (`BidirService::Service::Stream` on the listener,
`Stub::Stream(&context)` returning a `ClientReaderWriter` on the pusher). There are no reactors.

The wire direction is fixed by the `Stream` signature: whoever acts as the gRPC client writes
`Request` messages and whoever acts as the gRPC server reads them. The date/time therefore travels
in `Request.payload` (`opcode = STATUS`, `id` = per-connection push counter).

## Build & Run

```bash
./build.sh                       # cleans build/, runs cmake + make
./build/client                   # listener; listen address is optional (default 0.0.0.0:50051)
./build/server localhost:50051   # pusher; accepts any number of listener addresses
```

Start the listener first, then the pusher. Either process can be stopped with Ctrl-C.

## Session Changes

### 1. Client: multiple server support

- `main` parses all CLI arguments as server addresses.
- One pushing connection per address, each driven on its own `std::thread`.
- Messages are tagged with the originating address.

### 2. Server: configurable listen address

- The listen address is a parameter of `RunListener`; `main` uses `argv[1]` when present,
  otherwise `0.0.0.0:50051`.

### 3. Conversion to the asynchronous callback API

(Superseded by change 6; kept for history. Both sides now use the blocking API again.)

Previously both sides used the blocking/synchronous API (client `stub_->Stream(&context)` on a thread; server overriding `BidirService::Service::Stream`).

**Listener (gRPC server role)**

- `BidirServiceImpl` derives from `BidirService::CallbackService` and overrides
  `Stream(CallbackServerContext*)`, returning a heap-allocated `ServerBidiReactor<Request, Response>`.
- `StreamReactor` flow: `StartRead` -> `OnReadDone` (print the pushed date/time) -> `StartRead`.
- `Finish(Status::OK)` when the push source half-closes (`ok == false` on read); `OnCancel` finishes
  with `CANCELLED`; `OnDone` deletes the reactor. A `finished_` flag guards against a double `Finish`.

**Pusher (gRPC client role)**

- Heap-allocated `ClientBidiReactor<Request, Response>`, bound with
  `stub->experimental_async()->Stream(context, reactor)` followed by `StartCall()`.
- `OnReadDone` re-arms `StartRead` (the listener never writes, so the read only detects the end of
  the stream); `OnDone` reports status and signals the waiting caller.
- The caller blocks on a `std::mutex` + `std::condition_variable` until `OnDone`.
- Messages in flight are held in member buffers so their lifetime spans the async operation.

### 4. Server-push stream with an idle reader

- The pusher's reactor owns a per-connection `std::thread` ticker that waits on a
  `std::condition_variable` for 3 seconds and then `StartWrite`s the timestamped `Request`.
- A `std::mutex` serializes the ticker's `StartWrite` against `OnWriteDone`; `request_` is only
  mutated when no write is in flight (`write_in_progress_`).
- `OnDone` sets `stopped_`, notifies, joins the ticker, then `delete this`, so the ticker can never
  touch a freed reactor.
- Pusher: `PushClient::Run` blocks on a timed `cv_.wait_for` loop; SIGINT/SIGTERM flip `g_stop`,
  which triggers `ClientContext::TryCancel()` and then waits for `OnDone` so the reactor's
  back-pointer to `PushClient` stays valid.
- Listener: SIGINT/SIGTERM are blocked in `main` and consumed by a `sigwait` thread that calls
  `Server::Shutdown()` (calling gRPC from a real signal handler is not safe).

### 5. Roles swapped: client listens, server connects

- `client.cpp` is now the gRPC server (`ServerBuilder` + `CallbackService`); `server.cpp` is now the
  gRPC client (`BidirService::Stub` + `ClientBidiReactor`).
- The timestamp moved from `Response.message` to `Request.payload`, because the pushing process is
  the gRPC client and can therefore only write `Request` messages.
- The listener prints `Push source connected` / `Push source disconnected` per stream and supports
  any number of simultaneous push sources.
- Added a `Log()` helper in the pusher so concurrent connection threads cannot interleave on stdout.

### 6. Back to the synchronous/blocking API

- `client.cpp` (listener) derives from `BidirService::Service` and implements
  `Status Stream(ServerContext*, ServerReaderWriter<Response, Request>*)` with a blocking
  `while (stream->Read(&request))` loop. Each stream is served on its own gRPC sync thread.
- `server.cpp` (pusher) uses `stub_->Stream(&context)`; the loop is a blocking `stream->Write(...)`
  every 3 seconds, then `WritesDone()` + `Finish()`. No reactor, no per-connection ticker thread.
- `WaitInterval()` sleeps the 3s interval in 200ms slices, because a signal handler cannot touch the
  mutex/condition variable that a `cv_.wait_for` would need; Ctrl-C is therefore honoured within
  ~200ms instead of up to a full interval.
- The listener's `Server::Shutdown()` now takes a 1s deadline: the in-flight blocking `Read` has to be
  cancelled for `Wait()` to return.
- Added `Log()` (mutex-guarded `std::cout`) to the listener as well, since each handler runs on a
  different thread there.

## Gotchas / Lessons Learned

- **Sync bidi handler signature is `ServerReaderWriter<W, R>`, not `<R, W>`.** protoc generates
  `Service::Stream(ServerContext*, ServerReaderWriter<Response, Request>*)` for
  `rpc Stream(stream Request) returns (stream Response)` — the *write* type is the first template
  argument, so `Read()` fills a `Request` and `Write()` takes a `Response`.
- **Client reactors have no operation backlog.** Unlike `ServerBidiReactor`, whose `StartRead`/`StartWrite` queue work even before the stream is bound, `ClientBidiReactor::StartRead` calls `stream_->Read(...)` directly. Calling `StartRead` in the reactor constructor (before `experimental_async()->Stream(...)`) dereferences a null `stream_` and segfaults. Post the first read **after** binding, before `StartCall()`.
- **`StartCall()` is mandatory** for every client reactor, even if the RPC is cancelled.
- **Message lifetime:** a message passed to `StartRead`/`StartWrite` must remain valid and unmodified until the corresponding `OnReadDone`/`OnWriteDone` fires. Member buffers are used for this reason.
- **`Finish` may only be called once** on a server reactor; the `finished_` guard handles the read-end vs. cancel race.
- **Reactor lifetime:** heap-allocate the reactor and `delete this` in `OnDone` (called after all other reactions, and after all holds are removed). Any thread the reactor owns must be joined there.
- **Callback reactors are not thread-safe.** Only one `StartWrite` may be in flight per call; a
  ticker thread must serialize with the gRPC callback thread (mutex) and must not re-arm the read.
  This is safe for the client callback API too: every client reaction tag is registered with
  `can_inline=false`, so a reaction can never run on the thread that called `StartWrite` (that would
  self-deadlock on the reactor's mutex). `ClientCallbackReaderWriterImpl` has no backlog, so the
  first read must still be posted after binding.
- **Method signatures fix the wire direction.** Swapping who listens and who dials also swaps who
  writes `Request` and who writes `Response`; the payload has to move to the message the pushing
  side is allowed to write.
- Interleaved stdout from concurrent pusher threads is expected unless serialized (`Log()` does this).
- Not used but available: completion-queue async (`BidirService::AsyncService::RequestStream`).

## Testing Performed

- Built cleanly via `./build.sh`.
- One pusher against one listener: timestamps arrived at 3s intervals (`18:58:57`, `18:59:00`).
- Two concurrent pushers against one listener: both streamed with independent counters, and their
  clean-exit messages no longer interleaved.
- `SIGKILL`'d both pushers mid-stream: the listener logged `Push source disconnected` twice, stayed
  up, and a new pusher connected and streamed normally.
- Ctrl-C on the pusher -> `Stream finished: OK` / `All streams finished`; Ctrl-C on the
  listener -> `Received signal 2, shutting down` / `Listener stopped`. Both exit code 0.
- Re-ran the whole suite after the sync rewrite: same behaviour, no interleaved log lines.
