# Changelog

All notable changes to nhttpc will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-10-21

Initial public release.

### Public surface

- `nhttpc`: request / response API, pool lifecycle, async / streaming
  envelope, WebSocket, opts vocabulary, error taxonomy.
- `nhttpc_cookie_jar`: opaque-ref cookie jar with `start_link/0,1`,
  `stop/1`, `get/2`, `set/3`, `set_from_headers/3`, `clear/1,2`,
  `all/1`, `persist/1`.

Everything else is an implementation module.

### Features

- **Protocols**: HTTP/1.1, HTTP/2 (ALPN), HTTP/3 (QUIC), WebSocket
  (RFC 6455).
- **Dispatch modes**: `via => oneshot | {pool, Ref} | {conn, Conn}`.
  Illegal combinations unrepresentable.
- **Async / streaming envelope**: every async surface delivers
  `{nhttpc, Handle, Event}`. `cancel/1` returns only after draining
  the owner's mailbox of `{nhttpc, Handle, _}`.
- **Streaming backpressure**: explicit consumer credit via
  `consume/2`; `{pause, _}` / `{resume, _}` events on the same
  envelope.
- **Sane defaults**: TLS verify on with system CAs, decompression
  on, redirect-following on with a cap, retries off. Once enabled,
  retries of a non-idempotent method fire only on `connection_refused`.
- **TLS key exchange groups**: `tls => #{supported_groups => Groups}`
  and the `supported_groups` key of `nhttpc_conn:conn_opts()` set the
  groups the client offers, so a client on OTP 29 can reach a server
  without ML-KEM.
- **Sealed error taxonomy**: every public spec returns
  `{error, nhttpc:error()}`; no upstream `term()` leaks except the
  documented `{connect_error, term()}` arm for QUIC. An HTTP/2 GOAWAY
  with debug data reaches the caller as
  `{protocol_error, http2, {h2, Code, DebugData}}`.
- **Telemetry**: `telemetry` events and OpenTelemetry metrics and
  traces for the request, connection and pool lifecycle, gated by
  `otel => true | #{events, metrics, traces}`. Request `stop` and
  `error` events carry the full request metadata, and `start` events
  measure `monotonic_time`. `[nhttpc, pool, checkout]` and
  `[nhttpc, pool, checkin]` fire for every pooled request, and a
  checkout served by the lock-free path reports a `queue_time` of 0.
  `[nhttpc, pool, credit_starved]`, `[nhttpc, connection, credit]` and
  `[nhttpc, connection, goaway]` report HTTP/2 flow control and
  connection shutdown. See the Telemetry section of the `nhttpc` docs.

### HTTP/2 pooling and flow control

- The pool option `stream_selection` takes `pack` (default) or
  `spread`. Under `pack` the pool starts one multiplexed connection and
  opens another only when every connection sits at its concurrent
  stream cap, bounded by `size + max_overflow`, so `size` is an upper
  bound and not a target. Under `spread` the pool opens `size`
  connections at start. An HTTP/1.1 pool always fills to `size`.
- Pooled HTTP/2 and HTTP/3 checkout and checkin are lock-free and do
  not pass through the pool manager. A checkout reserves a stream slot
  and the send credit for its body with one compare-and-swap each.
- A request is admitted only to a connection that can carry its body,
  under both `stream_selection` policies. When none can, the pool opens
  the next connection. The `nhttpc` docs describe, next to `max_overflow`,
  how to size a pool for a peer that credits on processing.
- The concurrent stream cap of each connection follows the peer's
  `SETTINGS_MAX_CONCURRENT_STREAMS`, default 100.
- A body that fits a fresh stream window is sent whole against the
  credit reserved at checkout, so it never waits in the send queue
  behind its HEADERS frame. A connection that cannot send it whole
  refuses it with `refused_stream`.
- A body above the peer's credit waits in the codec send queue, bounded
  by the pool options `max_send_buffer` and `max_queued_streams`, and
  is sent as WINDOW_UPDATE frames arrive. Poolless requests, streaming
  uploads and streaming downloads use the same queue.
- A request the peer refuses before its stream opens, or that meets a
  send queue bound, fails with
  `{stream_error, {protocol, refused_stream}}`. The pool re-dispatches
  it onto another connection whatever the method and the caller's
  retry options, since no frame reached the peer, at most twice and
  within the caller's deadline.
- A request or streaming upload that times out with octets still
  queued sends RST_STREAM and fails with the retryable
  `refused_stream` reason, because the peer never saw its END_STREAM.
- In a streaming upload `chunk_size` is the read size of a file
  source, not a frame size, and progress reports the octets written to
  the socket.
- Receive credit is returned in batches: the connection window at
  32 KiB of consumed octets and on the last frame of every stream, each
  stream window at half its size.

### Quality bar

- `rebar3 check` clean (compile + format + xref + dialyzer + hank,
  warnings treated as errors).
- `nhttpc_unified_async_SUITE` enforces the cancel-no-leak contract
  across HTTP/1.1, HTTP/2, HTTP/3, and WebSocket, plus five hardening
  scenarios: TLS-handshake cancellation, pool race after conn death,
  H3 mid-drain cancellation, upload mid-file-read cancellation, and
  the send / socket-flush race.
- Full CT suite green (`rebar3 test`).
