# Development Notes

## Project Overview

`grpc_bidir-app` is a C++ gRPC example demonstrating a **bidirectional streaming** RPC where the
server is the sole writer: it listens, and every connected client receives a date/time string
every 3 seconds.

- `service.proto` — defines `TimePubService.Stream` (bidirectional stream) with `TimePublisher` (what a publisher writes: `Opcode`, `int32 id`, `string message`, `bool success`, `float result`) and `TimeSubscriber` (what a subscriber would write: `Opcode`, `int32 id`, `bool active`, `float value`).
- `time_susbscriber.cpp` — the gRPC **server** ("time subscriber"): listens (default `0.0.0.0:50051`) and only receives.
- `time_publisher.cpp` — the gRPC **client** ("time publisher"): dials one or more subscribers and pushes a formatted date/time string every 3 seconds.
- `CMakeLists.txt` / `build.sh` — build (protoc codegen + CMake).

Both sides use the **synchronous/blocking API** (`TimePubService::Service::Stream` on the subscriber,
`Stub::Stream(&context)` returning a `ClientReaderWriter` on the publisher). There are no reactors.

The date/time travels in `TimePublisher.message` (`opcode = STATUS`, `id` = per-connection push
counter). Because the publisher is the dialing side, the rpc is declared
`rpc Stream(stream TimePublisher) returns (stream TimeSubscriber)`: a gRPC client can only write the
rpc's first message type.

## Build & Run

```bash
./build.sh                                # cleans build/, runs cmake + make
./build/time_susbscriber 0.0.0.0:50051    # listener; listen address is optional
./build/time_publisher localhost:50051    # pusher; accepts any number of subscriber addresses
```

Start the subscriber first, then the publisher. Either process can be stopped with Ctrl-C.

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

Previously both sides used the blocking/synchronous API (client `stub_->Stream(&context)` on a thread; server overriding `TimePubService::Service::Stream`).

**Listener (gRPC server role)**

- `TimePubServiceImpl` derives from `TimePubService::CallbackService` and overrides
  `Stream(CallbackServerContext*)`, returning a heap-allocated `ServerBidiReactor<TimeSubscriber, TimePublisher>`.
- `StreamReactor` flow: `StartRead` -> `OnReadDone` (print the pushed date/time) -> `StartRead`.
- `Finish(Status::OK)` when the push source half-closes (`ok == false` on read); `OnCancel` finishes
  with `CANCELLED`; `OnDone` deletes the reactor. A `finished_` flag guards against a double `Finish`.

**Pusher (gRPC client role)**

- Heap-allocated `ClientBidiReactor<TimeSubscriber, TimePublisher>`, bound with
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
  gRPC client (`TimePubService::Stub` + `ClientBidiReactor`).
- The timestamp moved from `Response.message` to `Request.payload`, because the pushing process is
  the gRPC client and can therefore only write `Request` messages.
- The listener prints `Push source connected` / `Push source disconnected` per stream and supports
  any number of simultaneous push sources.
- Added a `Log()` helper in the pusher so concurrent connection threads cannot interleave on stdout.

### 6. Back to the synchronous/blocking API

- `time_publisher.cpp` (listener) derives from `TimePubService::Service` and implements
  `Status Stream(ServerContext*, ServerReaderWriter<TimePublisher, TimeSubscriber>*)` with a blocking
  `while (stream->Read(&request))` loop. Each stream is served on its own gRPC sync thread.
- `time_susbscriber.cpp` (pusher) uses `stub_->Stream(&context)`; the loop is a blocking `stream->Write(...)`
  every 3 seconds, then `WritesDone()` + `Finish()`. No reactor, no per-connection ticker thread.
- `WaitInterval()` sleeps the 3s interval in 200ms slices, because a signal handler cannot touch the
  mutex/condition variable that a `cv_.wait_for` would need; Ctrl-C is therefore honoured within
  ~200ms instead of up to a full interval.
- The listener's `Server::Shutdown()` now takes a 1s deadline: the in-flight blocking `Read` has to be
  cancelled for `Wait()` to return.
- Added `Log()` (mutex-guarded `std::cout`) to the listener as well, since each handler runs on a
  different thread there.

### 7. Server listens, server pushes; client connects and receives

- `time_publisher.cpp` is the gRPC server *and* the only writer: its handler runs a blocking
  `stream->Write(response)` every 3 seconds (`TimePublisher.message` = timestamp, `id` = per-connection
  counter) and treats `Write() == false` as the disconnect signal, since the handler never reads.
  A client half-close is therefore noticed within one interval.
- `time_susbscriber.cpp` is a pure reader: `while (stream->Read(&response))` + `Finish()`.
- A blocking `Read` cannot be interrupted by a flag, so the client keeps a small registry of active
  `ClientContext`s; a `sigwait` thread cancels all of them on SIGINT/SIGTERM, which unblocks the
  pending `Read` and lets `Finish()` report `CANCELLED`.
- The server's `sigwait` thread sets `g_stop` (so the handler's interval wait returns early) and
  then calls `Shutdown(deadline)`.

### 8. Proto renames: `BidirService` -> `TimePubService`, `Request`/`Response` -> `TimeSubscriber`/`TimePublisher`

- `service.proto` now defines `TimePubService.Stream(stream TimeSubscriber) returns (stream TimePublisher)`.
- `TimeSubscriber` dropped its `string payload` field (it is the type only the writing side would
  send, and the server never sends one), keeping `opcode`/`id`/`active`/`value`.
- Both sources updated to the new names; the wire behaviour is unchanged. Sections 3-7 above refer
  to the old `BidirService`/`Request`/`Response` names and describe superseded states.

### 9. File/binary renames

- `client.cpp` -> `time_susbscriber.cpp` (gRPC client / subscriber), `server.cpp` ->
  `time_publisher.cpp` (gRPC server / publisher). `CMakeLists.txt` targets renamed to match, so the
  binaries are `build/time_publisher` and `build/time_susbscriber`.
- Note the subscriber filename keeps the requested spelling `time_susbscriber` (sic).

### 10. Roles swapped again: subscriber listens, publisher dials and pushes

- `service.proto` now declares `rpc Stream(stream TimePublisher) returns (stream TimeSubscriber)`
  (was the other way round). A gRPC client can only write the rpc's *first* message type, and
  `TimeSubscriber` has no string field, so the timestamp needed a type it can actually send.
- `time_susbscriber.cpp` became the gRPC server: `TimePubServiceImpl::Stream` blocks in
  `while (stream->Read(&message))` on a `TimePublisher`. Publishers never write, so the read just
  parks until they half-close or vanish; received lines are tagged with `context->peer()`.
- `time_publisher.cpp` became the gRPC client: `TimePubClient::Run` blocks in `stream->Write(...)`
  every 3 seconds, then `WritesDone()` + `Finish()`. `Write() == false` is the disconnect signal.
- The blocking-API support code moved with the roles: the `ClientContext` registry + `sigwait`
  thread that cancels streams now lives in the publisher, and the `Shutdown(deadline)` logic in the
  subscriber.
- protoc regenerates the handler signature as `ServerReaderWriter<TimeSubscriber, TimePublisher>`
  (still `<return, request>`), which now matches "read TimePublisher, would write TimeSubscriber".

### 11. Publisher retry and connection recovery

`time_publisher.cpp` no longer exits when a subscriber is missing or a stream dies. `TimePubClient::Run` is now a
retry loop around a single attempt (`PushOnce`) and only returns once `g_stop` is set.

- **Failure detection in three places.** (a) *Subscriber unreachable*: the channel is polled via
  `GetState`/`WaitForStateChange` until `GRPC_CHANNEL_READY` or a `kConnectTimeout` budget runs out.
  (b) *Connection lost*: the blocking `stream->Write` returns false, or `Finish` reports a non-OK status.
  (c) *Network lost*: `GRPC_ARG_KEEPALIVE_TIME_MS`/`GRPC_ARG_KEEPALIVE_TIMEOUT_MS` (30s/10s) let gRPC reap a
  silently dead transport, which fails the blocking `Write` with `UNAVAILABLE keepalive watchdog timeout`
  instead of hanging on a dead TCP connection for minutes. 30s/10s is deliberately conservative — see
  gotcha "the keepalive timer is reset by *incoming* bytes only" below; the detection budget is
  `kKeepAliveTimeMs + kKeepAliveTimeoutMs` = 40s.
- **Backoff** is `BackoffDelay(consecutive_failures)`: jittered exponential from `kRetryInitial` (1s),
  doubling up to `kRetryMax` (15s). The generator is a `thread_local std::mt19937` because each publisher
  retries on its own thread. A stream that came up at least once resets the counter, so a recovered
  connection is retried within ~1s instead of after the grown penalty.
- **A fresh channel per attempt** (`CreateChannel`), via `grpc::CreateCustomChannel` because only that
  overload takes `ChannelArguments`. A new subscriber process or a restored link is then redialled
  immediately instead of waiting out the backoff of a channel that already gave up;
  `GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS`/`MIN`/`MAX` are shortened to match.
- **Resume, not restart**: `pushed_` moved into the class and survives reconnects, so the resumed stream
  logs `Connection recovered after N failed attempt(s), resuming date/time at #<n>` and the subscriber can
  see it is the same publisher.
- `WaitInterval()` became `WaitInterruptible(duration)` and now also serves as the retry wait, so Ctrl-C is
  honoured both while pushing and while backing off.
- Unchanged: one `ClientContext` per attempt, registered/unregistered around the stream so the `sigwait`
  thread can still cancel an in-flight blocking `Write`; the signal path still exits with code 0.

## Gotchas / Lessons Learned

- **A gRPC client can only write the rpc's first message type.** If the pushing process is the
  dialing one, the string-carrying message must be the *request* type, or the rpc signature has to
  be swapped; check which fields the writing type actually has before putting a payload in it.
- **Sync bidi handler signature is `ServerReaderWriter<W, R>`, not `<R, W>`.** protoc generates
  `Service::Stream(ServerContext*, ServerReaderWriter<TimeSubscriber, TimePublisher>*)` for
  `rpc Stream(stream TimePublisher) returns (stream TimeSubscriber)` — the *write* type is the first
  template argument, so `Read()` fills a `TimePublisher` and `Write()` would take a `TimeSubscriber`.
- **A blocking `Read` or `Write` needs a cancellation path.** A flag cannot unblock either; register
  the `ClientContext` and call `TryCancel()` from a `sigwait` thread when Ctrl-C must be graceful.
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
- **A blocking `Read` needs a cancellation path.** A flag cannot unblock it; register the
  `ClientContext` and call `TryCancel()` from a `sigwait` thread when Ctrl-C must be graceful.
- **A blocking `Write` also needs a liveness timeout, not just a cancellation path.** Cancellation only helps
  for local shutdown. If the peer's network dies silently the kernel keeps ACKing the TCP connection, so `Write`
  blocks indefinitely; HTTP/2 keepalive (`GRPC_ARG_KEEPALIVE_TIME_MS` + `GRPC_ARG_KEEPALIVE_TIMEOUT_MS`) is
  what turns that into a failed RPC that can be retried.
- **The keepalive timer is reset by *incoming* bytes only, so a write-only publisher pings on schedule.**
  In `chttp2_transport.cc` the timer is reset in the read path ("Since we have read a byte, reset the
  keepalive timer") and after a ping/BDP ping completes — never by our own outgoing DATA frames. Since
  `time_publisher.cpp` is the sole writer and the subscriber never writes, the connection receives nothing,
  so a ping is emitted every `GRPC_ARG_KEEPALIVE_TIME_MS` however often date/time is pushed. Pushing faster
  does **not** suppress keepalives.
- **An aggressive client keepalive tears down a perfectly healthy stream.** At 5s/3s this demo killed a live
  connection every ~15s with `UNAVAILABLE keepalive watchdog timeout`: a ping went out, no ACK arrived within
  3s, and `keepalive_watchdog_fired_locked` closed the transport (`GRPC_TRACE=http_keepalive` shows
  `Start keepalive ping` then `Keepalive watchdog fired` exactly `keepalive_timeout` later). A few-second
  cadence also runs into the peer side: a stock server's strike policy is `max_ping_strikes = 2` and
  `min_recv_ping_interval_without_data_ms = 300000` (5 min) — `frame_ping.cc` strikes any ping that arrives
  sooner, and strikes are only reset by `grpc_chttp2_reset_ping_clock` at stream close. Measured: 5s/3s died
  at ~15s; 20s/10s and 60s/10s stayed clean over 150s. Keep the ping cadence an order of magnitude above the
  push cadence and re-measure before tightening it.
- **Channel args need `grpc::CreateCustomChannel`.** `grpc::CreateChannel` has no `ChannelArguments`
  overload, so keepalive/reconnect tuning is a compile error until the custom variant is used.
- **`Channel::WaitForStateChange` only accepts `std::chrono::system_clock::time_point`** (or `gpr_timespec`);
  a `steady_clock` deadline does not compile.
- **An unreachable channel loops IDLE/CONNECTING/TRANSIENT_FAILURE internally.** Logging every transition
  floods stdout (gRPC re-dials within a single attempt); log the final verdict only.
- **`Server::Shutdown()` without a deadline waits for in-flight RPCs**, so a handler parked in
  `Read` would keep `Wait()` blocked until the client left on its own.
- Interleaved stdout from concurrent handler/client threads is expected; `Log()` serializes it.
- Not used but available: completion-queue async (`TimePubService::AsyncService::RequestStream`).

## Testing Performed

- Clean `./build.sh` after the rename/signature swap; protoc regenerated
  `ServerReaderWriter<TimeSubscriber, TimePublisher>`.
- Publisher -> subscriber at 3s intervals (`20:30:27`, `20:30:30`), subscriber tagging lines with
  `context->peer()`.
- Two publishers at once: both connected, each with its own per-connection counter and cadence.
- `kill -9` on a publisher: subscriber logged `Time publisher disconnected` and kept serving; a
  reconnecting publisher streamed normally (`#1 20:30:36`).
- Ctrl-C on the publisher -> `Stream finished: CANCELLED` / `All streams finished`; Ctrl-C on the
  subscriber -> `Received signal 2, shutting down` / `Subscriber stopped`. Both exit code 0.
- **Retry: subscriber started late.** Publisher alone logged `localhost:50051 not reachable, channel state
  TRANSIENT_FAILURE` / `Retrying in 625ms` / `Retrying in 3622ms` (backoff growing), then `Connected` and
  pushed `#1`, `#2` as soon as the subscriber came up.
- **Retry: `kill -9` on the subscriber mid-stream.** Publisher logged `Connection lost: 14 keepalive watchdog
  timeout` (14 = UNAVAILABLE), retried, and after the subscriber was restarted: `Connection recovered after 1
  failed attempt(s), resuming date/time at #6` -> subscriber received `#6`, `#7`, `#8` on a new peer port. The
  id counter continuing proves the resume, not a new stream.
- **Network lost:** `kill -STOP` on the subscriber (link up, no traffic). Detected within the
  `kKeepAliveTimeMs + kKeepAliveTimeoutMs` budget, retried through `TRANSIENT_FAILURE` while the peer was
  frozen, and recovered on `kill -CONT` -> `Connection recovered after N failed attempt(s), resuming
  date/time at #<n>`.
- **Spurious `Connection lost: 14 keepalive watchdog timeout` (regression, caused by 5s/3s keepalive):**
  reproduced every ~15s on an idle-but-healthy connection. Fixed by raising the keepalive to 30s/10s; a
  600s soak (200 pushes, ~20 pings) logged **zero** `Connection lost`/`Retrying`/`not reachable` lines and
  zero subscriber-side disconnects, and the `kill -STOP` case still recovered.
- Keepalive parameter sweep (experimental harness, env-configurable, `grpc.keepalive_*` + trace
  `http_keepalive`): 5s/3s fails at ~15s; `GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA` of 0 vs the default 2 makes
  no difference; 20s/10s and 60s/10s clean over 150s; no `GRPC_ARG_TCP_USER_TIMEOUT` / TCP keepalive args
  exist in gRPC 1.51.1, so HTTP/2 keepalive is the only bounded dead-peer mechanism available here.
- **Multiple addresses:** publisher dialing one live and one dead port kept pushing on the live one while the
  dead one retried independently; Ctrl-C stopped both (`All streams finished`, exit code 0).
- **Ctrl-C during the retry loop** (no subscriber, no registered context) exited promptly with code 0.
- clang-tidy: no new *kinds* of `readability-identifier-naming` violation; the new identifiers follow the
  file's existing Google-style conventions (`kXxx` constants, `PascalCase` functions), which the
  `.clang-tidy` config disagrees with for pre-existing code as well.
- Re-ran the whole suite after each role/API change.
- Note: `pkill -x time_susbscriber` matches nothing (Linux truncates `comm` at 15 chars); use
  `pkill -f`, and beware that `-f` patterns also match the invoking shell.
