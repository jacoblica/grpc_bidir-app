# Testing Plan / Test Record

Covers two questions:

1. How the retry/recovery implementation in `time_publisher.cpp` was tested for **unreachable**,
   **connection lost** and **network lost**.
2. How **high latency** and **frequent packet drop** should be tested.

## Under test — current parameters

All values live in `time_publisher.cpp`.

| Constant | Value | Purpose |
| --- | --- | --- |
| `kPushInterval` | 3s | date/time push cadence |
| `kConnectTimeout` | 2s | budget for one connection attempt to reach `GRPC_CHANNEL_READY` |
| `kConnectPollSlice` | 200ms | connectivity polling slice |
| `kRetryInitial` | 1s | first retry delay |
| `kRetryMax` | 15s | retry delay ceiling |
| `kMaxBackoffShift` | 4 | backoff doublings before the ceiling |
| `kKeepAliveTimeMs` | 30s | HTTP/2 keepalive ping interval |
| `kKeepAliveTimeoutMs` | 10s | ping ACK timeout |

Environment these were tested in: Ubuntu, gRPC **1.51.1-4.1build5**, uid 1000, **no root and no
passwordless sudo** (so `tc netem`, `ip link set down` and `iptables DROP` are unavailable),
`net.ipv4.tcp_retries2 = 15`.

---

# Part 1 — What was actually done

Everything below was run manually from the shell with `sleep`-based sequencing against
`127.0.0.1`. There is **no automated test suite**; see [Gaps](#gaps-and-caveats).

## 1. Unreachable

Publisher started with **no subscriber running**, plus a two-address run where one port was live and
one was closed (`localhost:50077`, later `localhost:50099`).

```
[localhost:50051] localhost:50051 not reachable, channel state TRANSIENT_FAILURE
[localhost:50051] Retrying in 625ms
[localhost:50051] localhost:50051 not reachable, channel state TRANSIENT_FAILURE
[localhost:50051] Retrying in 3622ms
[localhost:50051] Connected, pushing date/time every 3s; press Ctrl-C to stop.
```

The last two lines appeared on their own ~7s in, when the subscriber was started. This confirms the
backoff grows (625ms -> 3622ms) and that recovery is automatic. Subscriber then received `#1`, `#2`.

## 2. Connection lost

`kill -9` on the subscriber mid-stream, then restarted it.

```
[localhost:50070] Connection lost: 14 Socket closed
[localhost:50070] Retrying in 195ms
[localhost:50070] Connection recovered, resuming date/time at #5
```

Subscriber side logged a **new peer port** and received `#5`, `#6`, `#7`. The continuing message id is
what proves the stream resumed rather than being recreated.

## 3. Network lost

`kill -STOP` on the subscriber — the TCP connection stays `ESTABLISHED` and the kernel keeps ACKing,
but no data flows — then `kill -CONT` to restore.

Detected within the keepalive budget, retried through `TRANSIENT_FAILURE` while the peer was frozen,
then `Connection recovered after N failed attempt(s), resuming date/time at #18`, after which pushes
continued (`#21`, `#22`, `#23`).

## Also verified

- **Multi-address independence**: one live + one dead address; the live one kept pushing while the dead
  one retried on its own counter.
- **Ctrl-C while connected**: `Stream finished: CANCELLED` / `All streams finished`, **exit code 0**.
- **Ctrl-C during the retry loop** (no subscriber, no registered `ClientContext`): exit code 0.
- **600s healthy soak** (200 pushes, ~20 keepalive pings): zero `Connection lost` / `Retrying` /
  `not reachable` lines and zero subscriber-side disconnects.
- **Keepalive regression** (the `Connection lost: 14 keepalive watchdog timeout` bug): reproduced every
  ~15s at 5s/3s keepalive, fixed at 30s/10s. See `DEVELOPMENT_NOTES.md` section 11 and the gotchas.

## Gaps and caveats

1. **Everything ran over loopback.** `127.0.0.1` never drops a packet and ACKs instantly, so it cannot
   produce a genuine network fault. This is why the keepalive bug survived initially: the "healthy"
   tests were loopback too, and the broken config only surfaced on a 45s idle soak.
2. **"Network lost" was a frozen peer process, not a failed path.** `kill -STOP` models "peer hung",
   which has the right property (connection stays up, no data moves) but is not packet loss, and it
   freezes the peer's whole process including its HTTP/2 transport.
3. **Real packet loss was never tested.** `tc qdisc add ... netem loss 100%` returns
   `Operation not permitted` without root; `iptables DROP` and `ip link set down` likewise.
4. **Only the `ECONNREFUSED` path was exercised for connect failure** (a closed port replies
   immediately). A **blackholed SYN** — non-routable address, firewall drop, connect that hangs — was
   never tested, so the `kConnectTimeout` expiry path itself is unverified.
5. **DNS failure was never tested** (bogus hostname).
6. **Two publishers against one subscriber** was not re-verified after the rewrite.
7. **Flapping** (peer accepting then immediately dying, repeatedly) was never tested.
8. **No fd/socket leak check** over thousands of reconnects, even though a fresh
   `shared_ptr<Channel>` is built per attempt.
9. **Not repeatable.** Ad-hoc shell plus `sleep`, no assertions, no regression protection. A test that
   simply sleeps 15s is exactly the test that would have caught the keepalive bug automatically.

---

# Part 2 — Proposed tests for high latency and packet drop

## Harnesses

| Option | Fidelity | Requires |
| --- | --- | --- |
| **A. `tc netem` on a veth pair between two network namespaces** (publisher in ns1, subscriber in ns2) | Highest — real kernel path, real drops, directional loss | `sudo` |
| **B. User-space relay** extending the `netproxy.py` prototype: delay / jitter / directional drop / HTTP/2 frame-level drop | Sufficient for app-level behaviour; unprivileged; deterministic seeds | nothing |

Option B is viable today. Its main advantage over netem here is **directional loss**: this application
is a pure writer, so publisher -> subscriber loss is the only path that matters, and a relay isolates it
cleanly.

## 1. High latency network

### Setups

- B: relay with `--delay-ms 300` in each direction (~600ms RTT). Sweep 100 / 300 / 500 / 1000 / 2000ms.
- B: jitter — `--delay-ms 300 --jitter-ms 150`.
- A: `tc qdisc add dev veth0 root netem delay 300ms 100ms distribution normal`
- **DNS variant**: dial by hostname rather than `127.0.0.1`, so `getaddrinfo` cost lands inside the
  connect budget. A **fresh channel is built per attempt**, so name resolution repeats on every retry.
- **Asymmetric variant**: delay only subscriber -> publisher, proving the write path is insensitive to it.

### Assertions

- Time to first push after the subscriber appears: expect backoff + 2-3 RTT, and it must not grow without bound.
- **Cadence drift**: are pushes still ~3s apart, or does blocking `Write` stretch under latency?
- `Finish()` after `WritesDone()` is unbounded — measure whether it stalls.
- Recovery latency once latency returns to normal.

### Expected failure

`kConnectTimeout` is **2s**, while a gRPC connect costs TCP handshake (1 RTT) + HTTP/2
preface/SETTINGS (1 RTT) = ~2-3 RTT. **At RTT above roughly 700ms the publisher should fail to
connect while the subscriber is entirely healthy** — and that failure is indistinguishable from the
genuine "subscriber unreachable" case at the log level. Same budget applies to DNS resolution.

This is the highest-value test in this document and is expected to find a real bug. The likely fix is
an RTT-aware budget (e.g. a floor of 5-10s) rather than a fixed 2s.

## 2. Frequent packet drop

### Setups

- B: `--drop-pct 1 / 5 / 20`, publisher -> subscriber only, then both directions.
- B: **burst loss** — drop 100% for 10s every 60s. Flaky WiFi / cell-handover shape; much harder on the
  backoff logic than uniform loss.
- B: **frame-level drop of HTTP/2 PING only** (parse frame headers, drop 9-byte PINGs). A direct
  regression test for the keepalive bug: "if the path eats our keepalive pings, do we spuriously print
  `Connection lost`?" — very close to the original failure mode.
- B: small-packet-selective loss (PINGs, small DATA frames) vs large-frame loss.
- B: TCP **RST** mid-frame vs clean **FIN**, to cover both the `write_failed` and `Stream closed` log branches.
- **Setup loss**: drop the SYN or the SETTINGS exchange, so failure arrives as
  `TRANSIENT_FAILURE` / `kConnectTimeout` rather than `ECONNREFUSED`.
- A: `tc qdisc ... netem loss 5%`, and `loss 0.1%` for long-tail behaviour.

### Assertions

- Spurious `Connection lost` / `Retrying` lines on a lossy-but-alive link must be ~0.
- Recovery time after loss stops must track the backoff ceiling (15s), not drift upward.
- Log invariants (below): push ids strictly increasing, never reset.
- Does the backoff actually reach and hold the 15s ceiling?
- **fd / socket leak**: after thousands of reconnects, check `ss -s` and the process fd count. A
  `shared_ptr<Channel>` is created per attempt, so teardown should be verified.

## 3. Cross-cutting

- **Log invariant checker** — a script parsing the publisher log and asserting: ids strictly increasing
  with no resets; no `Connection lost` inside a window declared healthy; recovery bounded by X. This is
  the piece that makes every test above meaningful, and it is what is missing today.
- **Flapping subscriber** — accepts the TCP connection then dies immediately, repeating. Suspicion:
  `BackoffDelay` jitters uniformly over `[0, ceiling)`, and a stream that *does* establish resets
  `consecutive_failures` to 0, giving a 1s ceiling and possibly a sub-millisecond hot reconnect loop.
  Proposed check: count reconnect attempts over 30s and look for a spin.
- **Latency x loss combined** — the realistic case. Separately they are unlikely to interact badly.
- **Soak duration** — 0.1% loss over multi-hour runs is where rare races appear. The 600s soak run so
  far only covers the coarse case.
- **Reproducibility** — seed the relay's drop decisions, otherwise failures are not repeatable.
