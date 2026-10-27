-module(nhttpc).

-moduledoc """
Client public API for nhttpc.

HTTP/1.1, HTTP/2, HTTP/3 and WebSocket requests share one opts map,
one async envelope and one error taxonomy. Request and response maps,
header lists and version atoms are the `nhttp_lib` types:
`t:nhttp_lib:request/0`, `t:nhttp_lib:response/0`, `t:nhttp_lib:version/0`.

## Quick start

```erlang
%% One-shot GET
{ok, #{status := 200, body := Body}} =
    nhttpc:get(<<"https://api.example.com/users/42">>).

%% POST with headers
{ok, Resp} = nhttpc:post(
    <<"https://api.example.com/users">>,
    <<"{\"name\":\"alice\"}">>,
    #{headers => [{<<"content-type">>, <<"application/json">>}]}).

%% Build the canonical request manually
{ok, Req} = nhttpc:url(get, <<"https://api.example.com/users/42">>),
{ok, Resp} = nhttpc:request(Req#{headers => [{<<"accept">>, <<"application/json">>}]}).
```

Responses are plain maps:

```erlang
#{status := 200, headers := Headers, body := Body} = Resp.
```

## Request options

The options of `t:nhttpc:opts/0` that the sections below do not cover:

| Option | Default | Purpose |
|--------|---------|---------|
| `headers` | `[]` | Extra request headers |
| `compress` | `true` | Send `accept-encoding: gzip, deflate` unless the request sets it |
| `decompress` | `true` | Decode a gzip or deflate response body |
| `max_response_size` | 100 MiB | Larger bodies fail with `{stream_error, exceeded_max_response_size}` |
| `max_header_size` | 64 KiB | Bound on the response header block |
| `max_headers_count` | 100 | Bound on the number of response headers |
| `on_download_progress` | none | `fun(Received, Total) -> ok` called during a `body_to` download. `Total` is `unknown` without a `content-length` |
| `timeouts` | see Pool options | `#{connect, request, checkout, idle}` |

## Dispatch modes (via)

A single tagged value selects how the request gets to the wire. The
default is `oneshot`; pass `via => …` to change it.

```erlang
%% 1. one-shot (default): connect → send → recv → close
{ok, Resp} = nhttpc:get(Url).

%% 2. pooled: borrow a persistent connection
{ok, Resp} = nhttpc:get(Url, #{via => {pool, api_pool}}).

%% 3. on-connection: caller owns the multiplexed transport
{ok, Conn} = nhttpc:open(#{host => <<"api.example.com">>, port => 443},
                         #{versions => [http2]}),
{ok, Resp1} = nhttpc:get(<<"https://api.example.com/v1/a">>,
                         #{via => {conn, Conn}}),
{ok, Resp2} = nhttpc:get(<<"https://api.example.com/v1/b">>,
                         #{via => {conn, Conn}}),
ok = nhttpc:close(Conn).
```

Illegal combinations like "pool and conn at the same time" are
unrepresentable. Dialyzer enforces it.

## Connection pools

```erlang
%% Start (usually under your supervisor)
{ok, _} = nhttpc:start_link(api_pool, #{
    host       => <<"api.example.com">>,
    port       => 443,
    size       => 4,
    versions   => [http2, http1_1],
    tls        => #{verify => verify_peer},
    timeouts   => #{connect => 5_000, request => 30_000}
}).

%% Use the pool
{ok, Resp} = nhttpc:get(<<"https://api.example.com/users">>,
                        #{via => {pool, api_pool}}).

%% Stop
ok = nhttpc:stop(api_pool).
%% or with draining
ok = nhttpc:stop(api_pool, #{drain => true, drain_timeout => 5_000}).
```

The pool ref accepts the registered atom, the supervisor pid, or the
manager pid uniformly. One pool serves one `host:port` target; spin
up a pool per backend.

### Pool options

| Option | Default | Purpose |
|--------|---------|---------|
| `host` / `port` | required | Target |
| `size` | 4 | Persistent connections. Under `pack` this is an upper bound, not a target |
| `stream_selection` | `pack` | `pack` starts one multiplexed connection and adds more only when no connection has a free stream slot and the send credit for the body. `spread` opens `size` connections at start |
| `max_overflow` | 4 | Extra connections under load. See the sizing rule below |
| `versions` | `[http2, http1_1]` | Allowed protocols, advertised via ALPN |
| `tls` | `#{verify => verify_peer}` if scheme = https | TLS map (see below) |
| `timeouts.connect` | 5 000 ms | TCP / TLS handshake budget |
| `timeouts.checkout` | 5 000 ms | Wait for a free connection |
| `timeouts.request` | 30 000 ms | End-to-end request budget |
| `timeouts.idle` | 60 000 ms | Idle connection eviction |
| `idle_timeout` | `timeouts.idle` | Same as `timeouts.idle`, and takes precedence over it |
| `max_connection_age` | `infinity` | Age in ms after which a connection with no live stream is closed |
| `connection_sweep_interval` | 60 000 ms | How often the pool checks `max_connection_age` |
| `max_pending_requests` | 1000 | Checkouts that may wait at the manager. Past it a request gets `{error, pool_full}` |
| `reconnect_backoff_base` / `reconnect_backoff_max` | 100 / 30 000 ms | Bounds of the delay before reopening a connection that keeps failing |
| `max_streams_per_connection` | 2^29 | Streams an HTTP/2 connection opens before it retires. At most 2^30 - 1. See the rotation note below |
| `max_send_buffer` | 128 KiB | Octets of request bodies one HTTP/2 connection holds in its send queue, waiting for the peer's credit. See the large body note below |
| `max_queued_streams` | peer cap / 4 | Streams with octets in the send queue on one HTTP/2 connection |
| `proxy` | none | HTTP `CONNECT` proxy map |

A multiplexed connection takes a request only when it has a free stream
slot and the HTTP/2 send credit for the body. The pool grows on
whichever runs short first, up to `size + max_overflow`. An origin that
credits the connection window on processing bounds each connection to
`half_window / credit_latency` octets a second. Size the pool from
`offered_rate * body_size * credit_latency / half_window`, multiplied
by two for headroom, where `half_window` is the credit the peer returns
at a time and `credit_latency` is its response latency. The
`[nhttpc, connection, credit]` event reports both inputs: the peer's
credit rate and the interval between its WINDOW_UPDATE frames. APNs
returns 32830 octets at a time, 270 milliseconds after the body at the
median, so 10000 requests a second of 134-octet bodies need 11
connections at zero headroom: `size => 22, max_overflow => 11`. The
defaults fit an origin that credits on receipt.

A pool sized above that product keeps every checkout on the lock-free
path. A pool sized below it serves at the ceiling of its connections
and queues the rest at the manager, which then sees every checkout and
every checkin as a message.

An HTTP/2 connection has 2^30 stream identifiers. A connection that
opens `max_streams_per_connection` streams tells the pool it is
draining, serves what is in flight, and exits. The pool opens the
replacement when the drain starts, for a rotation and for a peer
GOAWAY alike, so the pool is never a connection short while the old one
finishes. The default of 2^29 gives 4.3 days for each connection at
1430 streams a second. Each rotation pays a TLS handshake and a cold
HPACK table. `size + max_overflow` bounds the connections that take
requests. The live process count is that bound plus the connections
draining at the moment, which the request timeout bounds. The
`[nhttpc, connection, rotated]` event fires when a drain starts.

## Send-side flow control

An HTTP/2 peer bounds what the client can send with two windows: one
for the connection and one for each stream, 65535 octets each at the
start. The peer grows them with WINDOW_UPDATE frames on its own
schedule. APNs credits the connection window on processing, 32830
octets at a time, about 270 milliseconds after the body, and never
credits a stream window.

The client admits a body only against credit it holds. Checkout
reserves the body size from the connection's credit slot with one
atomic operation, next to the stream slot. A connection whose credit is
short takes no body, so the connection never writes a frame the peer
cannot receive, never blocks on the socket, and keeps reading the peer
while its callers wait. The credit returns when the peer's WINDOW_UPDATE
arrives, and the connection publishes it to the pool at once.

What a caller sees at the bound:

- When no connection has the credit, the pool opens one, up to
  `size + max_overflow`. The `[nhttpc, pool, credit_starved]` event
  fires, rate-limited to one a second, when a checkout finds every
  connection short of credit. That event is the sizing signal.
- Past the bound the request waits for credit as a pool waiter and gets
  `{timeout, checkout}` at the checkout deadline. Nothing was sent, so
  the request is safe to retry.
  The `[nhttpc, pool, occupancy]` event reports the waiters and the
  connections as a level, once a second while a waiter is queued.
- A body that fits a fresh stream window, and that reaches a connection
  whose credit an earlier batch spent, is refused before a stream opens.
  The pool re-dispatches it up to two more times on another connection
  and then answers `{stream_error, {protocol, refused_stream}}`. The
  peer never saw the request. The
  `[nhttpc, connection, send_credit_refused]` event counts those
  refusals.
- A body above the initial window, 65535 octets, or above the stream
  window the peer advertises, opens its stream and sends what the
  windows take. The rest waits in the connection's send queue and
  leaves as the peer credits. `max_send_buffer` bounds the octets one
  connection holds this way, 128 KiB by default, and
  `max_queued_streams` the streams, a quarter of the peer's stream cap
  by default. A body that meets either bound is refused and
  re-dispatched like a body without credit. A request with octets still
  queued at its deadline gets the retryable refusal too: its END_STREAM
  never left, so the peer never processed it.
  While the queued octets leave, every grant from the peer pays for
  them, so the connection takes no other body until its queue is empty.
  A caller with a body waits at the manager or goes to another
  connection. A pool whose every connection holds a queued body against
  a peer that credits nothing answers each caller with
  `{timeout, checkout}` at its deadline and opens no connection.
  The `[nhttpc, connection, send_blocked]` and
  `[nhttpc, connection, send_resumed]` events mark the queue going
  non-empty and empty, and `[nhttpc, connection, send_queue]` samples
  its occupancy once a second while it holds octets.
- A GOAWAY refuses the streams above its last stream id with
  `{protocol_error, http2, {h2, Code, DebugData}}`. The peer never
  processed them. The streams at or below it complete on the draining
  connection.

Size the pool from the bandwidth-delay product of the peer's credit,
as described under Pool options.

## TLS

`tls => Map` is `t:tls_opts/0`, the client side of the `nhttp` server
TLS map. Verify-peer is the default; system CAs come from
`public_key:cacerts_get/0`.

```erlang
%% Custom CA file
#{tls => #{cacertfile => <<"/etc/ssl/internal-ca.crt">>}}

%% Pinned CA list
#{tls => #{cacerts => Derlist}}

%% Disabled verification (avoid in production)
#{tls => #{verify => verify_none}}

%% Versions, ALPN, SNI
#{tls => #{versions => ['tlsv1.2', 'tlsv1.3'],
           alpn_advertised_protocols => [<<"h2">>, <<"http/1.1">>],
           server_name_indication => "api.example.com"}}

%% Key exchange groups, in order of preference
#{tls => #{supported_groups => [x25519, secp256r1]}}
```

There is no `transport` option. The wire transport is derived from
URL scheme + presence of `tls` + chosen `versions`:

| URL scheme | versions | Wire transport |
|------------|----------|----------------|
| `http`     | h1 / h2 (h2c) | plaintext TCP |
| `https`    | h1 / h2  | TLS over TCP   |
| `https`    | h3       | QUIC / UDP     |

## HTTP/3

```erlang
%% Poolless
{ok, Resp} = nhttpc:get(<<"https://example.com/path">>,
                        #{versions => [http3]}).

%% Pooled
{ok, _} = nhttpc:start_link(h3_pool, #{
    host => <<"api.example.com">>,
    port => 443,
    versions => [http3]
}),
{ok, Resp} = nhttpc:get(<<"https://api.example.com/v1/users">>,
                        #{via => {pool, h3_pool}}).
```

`nquic` is in `nhttpc.app.src`'s `applications` list, so H3 is
available from a release boot.

## WebSocket

WebSocket folds into the unified async protocol:

```erlang
{ok, H} = nhttpc:ws_open(<<"wss://example.com/socket">>, #{}),
ok = nhttpc:ws_send(H, {text, <<"hello">>}),

receive
    {nhttpc, H, {ws_ready, _Info}}              -> ok;
    {nhttpc, H, {ws, {text, Msg}}}              -> handle_text(Msg);
    {nhttpc, H, {ws, {binary, Bin}}}            -> handle_bin(Bin);
    {nhttpc, H, {done, {remote_closed, Code}}}  -> server_closed(Code);
    {nhttpc, H, {done, normal}}                 -> ok
end,

ok = nhttpc:cancel(H).
```

WS-only options ride under `ws => …`:

```erlang
nhttpc:ws_open(Url, #{
    ws => #{subprotocols => [<<"chat">>],
            ping_interval => 30_000}
}).
```

## Async requests

```erlang
{ok, H} = nhttpc:send(Req, #{}),
{ok, Resp} = nhttpc:await(H, 5_000),
%% or cancel without waiting:
ok = nhttpc:cancel(H).
```

Cancellation drains the owner's mailbox of `{nhttpc, H, _}` before
returning, so no events leak.

## Streaming downloads

For sync calls, `body_to` discriminates the sink:

```erlang
%% Buffer to file
{ok, #{body := {file, Path, BytesWritten}}} =
    nhttpc:get(Url, #{body_to => {file, "/tmp/big.bin"}}).

%% Fold each chunk
{ok, #{body := {fold, Sum}}} =
    nhttpc:get(Url, #{body_to   => {fold, fun(Chunk, Acc) ->
                                        {continue, Acc + byte_size(Chunk)}
                                    end},
                      fold_init => 0}).
```

For event-driven streaming use `stream/2` and pace the producer with
`consume/2`:

```erlang
{ok, H} = nhttpc:stream(Req, #{consume_initial => 64 * 1024,
                                consume_low_water => 16 * 1024}),
receive {nhttpc, H, {headers, _S, _Hs}} -> ok end,
loop(H, 0).

loop(H, Total) ->
    receive
        {nhttpc, H, {data, Chunk, more}} ->
            handle_chunk(Chunk),
            ok = nhttpc:consume(H, byte_size(Chunk)),
            loop(H, Total + byte_size(Chunk));
        {nhttpc, H, {data, Chunk, fin}} ->
            handle_chunk(Chunk),
            {ok, Total + byte_size(Chunk)};
        {nhttpc, H, {pause, _Buffered}}  -> loop(H, Total);
        {nhttpc, H, {resume, _Buffered}} -> loop(H, Total);
        {nhttpc, H, {done, normal}}      -> {ok, Total};
        {nhttpc, H, {error, R}}          -> {error, R}
    end.
```

## Streaming uploads

```erlang
%% From a file
{ok, Resp} = nhttpc:post(Url, <<>>,
                         #{body_from => {file, "/path/to/big.bin"}}).

%% From a custom iterator
Iter = fun() ->
    case ... of
        eof -> fin;
        Chunk -> {ok, Chunk, Iter}
    end
end,
{ok, Resp} = nhttpc:post(Url, <<>>, #{body_from => {iter, Iter}}).

%% Progress
nhttpc:post(Url, <<>>,
            #{body_from => {file, Path},
              on_upload_progress => fun(Sent, _Total) ->
                  io:format("uploaded ~p~n", [Sent])
              end}).

%% Read size of a file source, 65536 octets by default
nhttpc:post(Url, {stream, {file, Path}, #{chunk_size => 262144}}, #{}).
```

A file source is read `chunk_size` octets at a time. On HTTP/2 the
codec splits every chunk at the peer's frame size and holds what the
peer's windows cannot take yet, at most 128 KiB, so `chunk_size` is a
read size and not a frame size. The uploader reads the socket while it
waits for credit, and `on_upload_progress` reports the octets written
to the socket. An upload that reaches its request timeout with octets
still held sends RST_STREAM and answers
`{stream_error, {protocol, refused_stream}}`.

## Cookie jar

```erlang
{ok, Jar} = nhttpc_cookie_jar:start_link(),
{ok, _} = nhttpc:get(<<"https://example.com/login">>,
                     #{cookie_jar => Jar}),
{ok, _} = nhttpc:get(<<"https://example.com/dashboard">>,
                     #{cookie_jar => Jar}),
ok = nhttpc_cookie_jar:stop(Jar).
```

`jar()` is an opaque `reference()`. The pid + ETS table are held under
`persistent_term` keyed on the ref; the cookie API is the only thing
the caller ever touches.

## Retries

```erlang
%% Up to 3 attempts (retries are off by default)
nhttpc:get(Url, #{retry => 3}).

%% Full control
nhttpc:get(Url, #{retry => #{
    max_attempts => 5,
    base_delay   => 100,
    max_delay    => 5_000,
    backoff      => exponential,   %% exponential | linear | constant
    jitter       => true,
    retry_on     => [timeout, connection_error, 503, 429]
}}).
```

Without `retry` no request is retried. Once enabled, the default
condition set depends on the method. An idempotent method retries on
`timeout`, `connection_error`, `connection_refused`, `connection_reset`,
and 502/503/504. A `POST` or a `PATCH` retries on `connection_refused`
alone, the one condition that proves the request never reached the
server.

`retry_on => [auto]` delegates the decision to the protocol layer,
subject to the same method gate. For an idempotent method `auto` retries
a closed connection, a GOAWAY, and a refused stream. For a non-idempotent
method `auto` shrinks to `connection_refused`. Status codes listed next to
`auto` are explicit and apply to every method. A caller that wants
at-least-once delivery on a `POST` must list the conditions or supply a
`should_retry` callback.

## Redirects

`follow_redirects => true` (default) follows 301/302/303/307/308. The
`max_redirects` cap defaults to 5. `redirect_policy => fun/4` lets you
inspect each hop and either continue, stop, or supply updated opts.

A 303 changes the method to `GET`. A 301 or a 302 changes every method
except `GET`, `HEAD` and `QUERY` to `GET`. A 307 or a 308 keeps the
method and the body.

Cross-origin hops strip `Authorization`, `Cookie`, and other
credential-bearing headers by default.

## Error taxonomy

```erlang
-type nhttpc:error() ::
      {invalid_request,   atom()}
    | {invalid_opts,      atom()}
    | {connect_error,     inet:posix() | term()}    %% term() only for QUIC
    | {tls_error,         tls_reason()}
    | {timeout,           connect | request | checkout | idle | body}
    | {protocol_error,    nhttp_lib:version(), protocol_detail()}
    | {http_error,        nhttp_lib:status(), nhttp_lib:headers()}
    | {redirect_error,    redirect_reason()}
    | {retries_exhausted, pos_integer(), nhttpc:error()}
    | {stream_error,      stream_reason()}
    | {body_error,        body_reason()}
    | {cookie_jar_error,  cookie_jar_reason()}
    | {file_error,        file:posix() | badarg | terminated | system_limit}
    | pool_full
    | pool_closed.
```

Every public spec returns `{error, nhttpc:error()}`; no upstream
`term()` leaks except the documented `connect_error` arm.

An HTTP/2 GOAWAY reaches the caller as `{protocol_error, http2, {h2,
Code}}`. When the frame carries debug data, the detail is `{h2, Code,
DebugData}` with the raw binary from the frame. APNs puts a JSON
`reason` there. The client does not parse it.

## Telemetry

Instrumentation is opt-in through the `otel` request / pool option and
is off by default. When it is unset the request path reads one map key
and takes an uninstrumented branch: no clock read, no metadata map, no
pool lookup, no `telemetry:execute/3`.

```erlang
otel => #{events => true, metrics => true, traces => true}
otel => true    %% shorthand for all three
otel => false   %% default
```

The map form enables only the flags it lists: `#{metrics => true}`
records instruments but publishes no `telemetry` events. `traces` is
accepted and reserved; no spans are emitted yet.

### Events

Published when `events` is enabled. Request events also carry
`method`, `url`, `scheme`, `host`, `port`, `path` and `pool`.

| Event | Measurements | Extra metadata |
|-------|--------------|----------------|
| `[nhttpc, request, start]`   | `monotonic_time`  | none |
| `[nhttpc, request, stop]`    | `duration` (µs), `req_body_length`, `resp_body_length` | `status` |
| `[nhttpc, request, error]`   | `duration` (µs), `req_body_length`, `resp_body_length` | `reason` |
| `[nhttpc, connect, start]`   | `monotonic_time`  | `host`, `port`, `transport` |
| `[nhttpc, connect, stop]`    | `duration` (µs)   | `host`, `port`, `protocol` |
| `[nhttpc, connect, error]`   | `duration` (µs)   | `host`, `port`, `reason` |
| `[nhttpc, pool, checkout]`   | `queue_time` (ms) | `pool` |
| `[nhttpc, pool, checkin]`    | none                 | `pool` |
| `[nhttpc, pool, timeout]`    | none                 | `pool` |
| `[nhttpc, pool, exhausted]`  | none                 | `pool` |
| `[nhttpc, connection, idle_timeout]` | none         | `host`, `port` |
| `[nhttpc, connection, send_credit_refused]` | `send_window` | `host`, `port`, `conn_pid`, `stream_id` |
| `[nhttpc, connection, credit]` | `count`, `octets`, `mean_interval` (µs), `max_interval` (µs) | `host`, `port`, `conn_pid` |
| `[nhttpc, connection, goaway]` | none      | `host`, `port`, `conn_pid`, `error_code`, `last_stream_id`, `debug_data` |
| `[nhttpc, connection, rotated]` | `streams` | `host`, `port`, `conn_pid` |
| `[nhttpc, pool, credit_starved]` | none    | `pool` |
| `[nhttpc, connection, send_blocked]` | `send_window`, `send_buffer_bytes`, `queued_streams`, `send_blocked_ms` (ms) | `host`, `port`, `conn_pid` |
| `[nhttpc, connection, send_resumed]` | the four send queue gauges, `blocked_ms` (ms) | `host`, `port`, `conn_pid` |
| `[nhttpc, connection, send_queue]` | the four send queue gauges | `host`, `port`, `conn_pid` |
| `[nhttpc, connection, max_streams_changed]` | `previous_max_streams`, `max_streams` | `host`, `port`, `conn_pid` |
| `[nhttpc, pool, occupancy]` | `waiter_count`, `connections` | `pool` |

The body length measurements are omitted when the size cannot be known
up front, as for a streaming source, rather than reported as zero.

The `queue_time` of the `[nhttpc, pool, checkout]` event is in
milliseconds. The `nhttpc.http.pool.queue_time` metric records the same
wait in microseconds.

The version atom in events is `t:nhttp_lib:version/0` (`http1_1 |
http2 | http3`).

The send queue gauges describe one HTTP/2 connection. `send_window` is
the connection send window of the codec, `send_buffer_bytes` the octets
the send queue holds, `queued_streams` the streams with an entry in it,
and `send_blocked_ms` the time the connection held a non-empty send
queue since it connected. `send_blocked_ms` accumulates. It counts the
period that is open when the event fires, and it never resets, so a
collector takes the rate from it.

`send_blocked` fires when the send queue goes from empty to non-empty
and `send_resumed` when it empties, with `blocked_ms` the length of the
period that ended. The pair marks transitions. It does not bracket every
starvation period: against a peer that credits every few hundred
milliseconds, one large upload keeps the queue non-empty for its whole
life, and the pair degenerates to one `send_blocked` event and a
growing `send_blocked_ms`. `send_queue` is the sample for that case. It
fires once a second at most while the queue is non-empty, and once with
every `credit` report. The events are about the codec send queue. The
`credit_starved` and `send_credit_refused` events are about pool
admission, which keeps a small body off the queue, so a unary workload
sees those two and not these.

`max_streams_changed` fires when a SETTINGS frame moves the concurrent
stream cap of a connection, with the cap before and after the frame. A
frame that repeats the cap fires nothing.

`occupancy` reports the level of a pool. `waiter_count` is the checkouts
parked at the manager for a connection with a free stream slot and the
send credit for the body. `connections` is the connections that take
requests, ready or connecting, which `size + max_overflow` bounds. A
draining connection is not counted. The manager samples the level once a
second at most while it holds a waiter, and once more when the last
waiter leaves, so the level returns to zero. A pool that never queues a
waiter emits nothing. A pool at the bound with a non-zero `waiter_count`
is saturated.

### Metrics

Recorded when `metrics` is enabled, against the `nhttpc` meter.
Attributes follow the OpenTelemetry HTTP client semantic conventions.

| Instrument | Kind | Unit |
|------------|------|------|
| `nhttpc.http.request.duration`   | histogram | µs |
| `nhttpc.http.connect.duration`   | histogram | µs |
| `nhttpc.http.pool.queue_time`    | histogram | µs |
| `nhttpc.http.request.body.size`  | histogram | bytes |
| `nhttpc.http.response.body.size` | histogram | bytes |
| `nhttpc.http.active_requests`    | updown counter | 1 |
| `nhttpc.http.pool.exhausted`     | counter | 1 |
| `nhttpc.http.pool.timeout`       | counter | 1 |

### Coverage

`request/2` and `send/2` are instrumented. `stream/2` is instrumented
on HTTP/3 only; the HTTP/1.1 and HTTP/2 streaming path goes through
`nhttpc_stream_recv` and does not emit request events yet.

## Production notes

- One pool per `host:port`. A pool with `host => <<"a">>` will not
  serve requests targeted at `<<"b">>` even if the URL says otherwise.
- Pools are long-lived. The `persistent_term` lookup pays a global
  scan cost once at start_link; rapid pool churn defeats the
  optimisation.
- A cookie jar holds a `persistent_term` entry too, and stopping it
  pays the same global cost. Start a jar per session or per user, not
  per request.
- `verify_none` disables certificate validation. Use it only for
  development; production should keep `verify_peer` and supply a
  pinned CA if the default trust store isn't sufficient.
- Idle connections are evicted by `timeouts.idle`. Servers that send
  `Connection: close` mid-pool cause one extra reconnect on the next
  checkout. Servers that send `GOAWAY` (H2) trigger graceful drain,
  and the pool opens the replacement connection when the drain starts.
- HTTP/3 is over connected UDP. Networks that drop UDP between
  reconnects (corporate proxies, some load balancers) require an H2
  fallback in `versions => [http3, http2, http1_1]`.

## Supervision

```erlang
init([]) ->
    Pool = nhttpc:child_spec(api_pool, #{
        host => <<"api.example.com">>,
        port => 443,
        size => 8,
        tls  => #{verify => verify_peer}
    }),
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [Pool]}}.
```
""".

-export([
    child_spec/2,
    start_link/2,

    stop/1,
    stop/2
]).

%% Synchronous requests
-export([
    request/1,
    request/2
]).

%% Asynchronous requests (high-level, URL-based)
-export([
    await/1,
    await/2,
    cancel/1,
    consume/2,
    flush/1,
    send/1,
    send/2,
    stream/1,
    stream/2
]).

%% Connection-level async dispatch (for direct connection access)
-export([
    await_response/3, send_request/2
]).

%% Convenience functions (poolless)
-export([
    delete/1, delete/2,
    get/1, get/2,

    head/1, head/2,
    options/1, options/2,
    patch/2, patch/3,
    post/2, post/3,
    put/2, put/3
]).

%% URL parsing / request builder
-export([
    parse_url/1,
    url/2
]).

%% Connection lifecycle
-export([
    close/1,
    conn_pid/1,
    open/2
]).

%% WebSocket
-export([
    ws_open/2,
    ws_send/2
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    backoff_strategy/0,
    body_reason/0,
    connection/0,
    cookie_jar_reason/0,
    done_reason/0,
    error/0,
    event/0,
    file_reason/0,
    handle/0,
    handle_kind/0,
    iterator/0,
    on_data_fun/0,
    opts/0,
    pool_opts/0,
    pool_ref/0,
    progress_fun/0,
    protocol_detail/0,
    proxy_opts/0,
    redirect_policy/0,
    redirect_reason/0,
    request/0,
    response/0,
    retry_condition/0,
    retry_opts/0,
    stop_opts/0,
    stream_reason/0,
    stream_selection/0,
    target/0,
    timeouts/0,
    tls_opts/0,
    tls_reason/0,
    url_info/0,
    via/0
]).

%%%-----------------------------------------------------------------------------
%% TYPES - REQUEST / RESPONSE
%%%-----------------------------------------------------------------------------
-type request() :: nhttp_lib:request() | map().

-type response() :: nhttp_lib:response().

-type url_info() :: nhttpc_url:url_info().

%%%-----------------------------------------------------------------------------
%% TYPES - PUBLIC VOCABULARY (1.0 PROPOSAL)
%%%-----------------------------------------------------------------------------
-type pool_ref() :: atom() | pid().

-type target() :: #{
    host := binary() | inet:ip_address(),
    port := inet:port_number()
}.

-type handle() :: nhttpc_handle:handle().

-type handle_kind() :: nhttpc_handle:kind().

-opaque connection() :: #{
    pid := pid(),
    version := nhttp_lib:version(),
    peer := nhttp_lib:peer()
}.

-type via() ::
    oneshot
    | {pool, pool_ref()}
    | {conn, connection()}.

-type event() ::
    {response, nhttp_lib:response()}
    | {headers, nhttp_lib:status(), nhttp_lib:headers()}
    | {data, binary(), nhttp_lib:fin()}
    | {trailers, nhttp_lib:headers()}
    | {pause, non_neg_integer()}
    | {resume, non_neg_integer()}
    | {ws, nhttp_lib:ws_frame()}
    | {ws_ready, map()}
    | {done, done_reason()}
    | {error, error()}.

-type done_reason() ::
    normal
    | cancelled
    | {remote_closed, nhttp_lib:error_code() | undefined}.

-type progress_fun() ::
    fun((Sent :: non_neg_integer(), Total :: non_neg_integer() | unknown) -> ok).

-type on_data_fun() ::
    fun((Chunk :: binary(), Acc :: term()) -> {continue, term()} | {stop, term(), term()}).

-type iterator() ::
    fun(() -> {ok, iodata(), iterator()} | fin | {error, body_reason()}).

-type stop_opts() :: #{
    drain => boolean(),
    drain_timeout => timeout(),
    force_kill_delay => non_neg_integer()
}.

-type timeouts() :: #{
    connect => timeout(),
    request => timeout(),
    checkout => timeout(),
    idle => timeout()
}.

-type tls_opts() :: #{
    cacertfile => file:filename(),
    cacerts => [public_key:der_encoded()],
    certfile => file:filename(),
    keyfile => file:filename(),
    certs_keys => [map()],
    verify => verify_none | verify_peer,
    server_name_indication => inet:hostname() | disable,
    wildcard_hostname => boolean(),
    versions => [ssl:protocol_version()],
    alpn_advertised_protocols => [binary()],
    supported_groups => [ssl:group()],
    extra => [ssl:tls_client_option()]
}.

%%%-----------------------------------------------------------------------------
%% TYPES - OPTS
%%%-----------------------------------------------------------------------------
-type opts() :: #{
    timeouts => timeouts(),
    tls => tls_opts(),
    versions => [nhttp_lib:version()],
    headers => nhttp_lib:headers(),
    compress => boolean(),
    decompress => boolean(),
    cookie_jar => nhttpc_cookie_jar:jar(),
    follow_redirects => boolean(),
    max_redirects => non_neg_integer(),
    redirect_policy => redirect_policy(),
    retry => false | pos_integer() | retry_opts(),
    on_upload_progress => progress_fun(),
    on_download_progress => progress_fun(),
    max_response_size => pos_integer() | infinity,
    max_header_size => pos_integer() | infinity,
    max_headers_count => pos_integer() | infinity,
    proxy => proxy_opts(),
    body_to => {file, file:filename()} | {fold, on_data_fun()},
    body_from => {file, file:filename()} | {iter, iterator()},
    fold_init => term(),
    consume_initial => pos_integer() | infinity,
    consume_low_water => pos_integer(),
    via => via(),
    ws => map(),
    otel => map()
}.

-doc """
Pool configuration.

`size` means one of two things, and `stream_selection` decides which. Under
`spread` the pool opens `size` connections at start and keeps that
many. Under `pack`, which is the default, `size` is a ceiling: the pool
opens one multiplexed connection and adds another only when no
connection has both a free stream slot and the HTTP/2 send credit for
the body, up to `size + max_overflow`. See `nhttpc_opts` for the sizing
rule against a peer that credits on processing. An HTTP/1.1 pool always fills to `size`, because
its checkout is exclusive.
""".
-type pool_opts() :: #{
    host := binary() | string() | inet:ip_address(),
    port := inet:port_number(),
    timeouts => timeouts(),
    tls => tls_opts(),
    versions => [nhttp_lib:version()],
    size => pos_integer(),
    max_overflow => non_neg_integer(),
    idle_timeout => timeout(),
    max_connection_age => timeout(),
    max_pending_requests => pos_integer(),
    max_streams_per_connection => pos_integer(),
    max_send_buffer => pos_integer() | infinity,
    max_queued_streams => pos_integer() | infinity,
    connection_sweep_interval => timeout(),
    stream_selection => stream_selection(),
    reconnect_backoff_base => pos_integer(),
    reconnect_backoff_max => pos_integer(),
    proxy => proxy_opts(),
    otel => map()
}.

-doc """
Connection selection policy for HTTP/2 and HTTP/3 pools.

`pack` walks the pool oldest connection first and takes the first one
with a free stream slot, so traffic concentrates on one warm
connection. The pool opens another connection only when every
connection is at its concurrent stream cap. RFC 9113 section 9.1 and
RFC 9114 section 3.3 both ask a client to do this.

`spread` distributes requests over the pool: round-robin on the ETS
fast path, least-loaded through the manager.

HTTP/1.1 checkout is exclusive and ignores this option.
""".
-type stream_selection() :: pack | spread.

-type proxy_opts() :: #{
    host := binary() | string() | inet:ip_address(),
    port := inet:port_number(),
    username => binary(),
    password => binary()
}.

-type retry_opts() :: #{
    max_attempts := pos_integer(),
    base_delay => non_neg_integer(),
    max_delay => non_neg_integer(),
    backoff => backoff_strategy(),
    jitter => boolean(),
    retry_on => [retry_condition()],
    should_retry => fun(
        (
            Result :: {ok, nhttp_lib:response()} | {error, error()},
            Attempt :: pos_integer(),
            Opts :: retry_opts()
        ) -> boolean()
    )
}.

-type backoff_strategy() :: exponential | linear | constant.

-type retry_condition() ::
    timeout
    | connection_error
    | connection_refused
    | connection_reset
    | auto
    | nhttp_lib:status().

-type redirect_policy() ::
    fun(
        (
            Status :: nhttp_lib:status(),
            Location :: binary(),
            Request :: nhttp_lib:request(),
            Opts :: opts()
        ) -> continue | stop | {continue, opts()}
    ).

%%%-----------------------------------------------------------------------------
%% TYPES - ERROR TAXONOMY (PLAN 10 WIRES THROUGH SPECS)
%%%-----------------------------------------------------------------------------
-type error() ::
    {invalid_request, atom()}
    | {invalid_opts, atom()}
    | {connect_error, inet:posix() | term()}
    | {tls_error, tls_reason()}
    | {timeout, connect | request | checkout | idle | body}
    | {protocol_error, nhttp_lib:version(), protocol_detail()}
    | {http_error, nhttp_lib:status(), nhttp_lib:headers()}
    | {redirect_error, redirect_reason()}
    | {retries_exhausted, pos_integer(), error()}
    | {stream_error, stream_reason()}
    | {body_error, body_reason()}
    | {cookie_jar_error, cookie_jar_reason()}
    | {file_error, file_reason()}
    | pool_full
    | pool_closed.

-type tls_reason() ::
    {tls_alert, ssl:tls_alert()}
    | {options, term()}
    | {handshake, term()}
    | {hostname_mismatch, binary()}.

-type protocol_detail() ::
    nhttp_h1:parse_error()
    | {h2, nhttp_lib:error_code()}
    | {h2, nhttp_lib:error_code(), binary()}
    | {h3, nhttp_lib:error_code()}
    | nhttp_lib:error().

-type redirect_reason() ::
    max_redirects_exceeded
    | {redirect_loop, binary()}
    | {invalid_redirect_location, binary()}
    | {cross_origin_blocked, binary()}.

-type stream_reason() ::
    cancelled
    | rst_stream
    | {flow_control, term()}
    | {protocol, nhttp_lib:error()}
    | exceeded_max_response_size.

-type body_reason() ::
    {iterator, term()}
    | {file_read, file_reason()}
    | {file_open, file_reason()}
    | size_mismatch.

-type cookie_jar_reason() ::
    stopped
    | persist_timeout
    | {file, file:posix()}.

-type file_reason() :: file:posix() | badarg | terminated | system_limit.

%%%-----------------------------------------------------------------------------
%% POOL MANAGEMENT
%%%-----------------------------------------------------------------------------
-doc "Return a child spec for embedding in supervision trees.".
-spec child_spec(term(), pool_opts()) -> supervisor:child_spec().
child_spec(Ref, Opts) ->
    case nhttpc_opts:normalize_pool(Opts) of
        {ok, NormalizedOpts} ->
            nhttpc_pool_sup:child_spec(Ref, NormalizedOpts);
        {error, Reason} ->
            error(Reason)
    end.

-doc "Start a client pool linked to the calling process.".
-spec start_link(term(), pool_opts()) -> {ok, pid()} | {error, error()}.
start_link(Ref, Opts) ->
    case nhttpc_opts:normalize_pool(Opts) of
        {ok, NormalizedOpts} ->
            nhttpc_error:wrap_result(nhttpc_pool_sup:start_link(Ref, NormalizedOpts));
        {error, _} = Error ->
            nhttpc_error:wrap(Error)
    end.

-doc "Stop a client pool immediately.".
-spec stop(pid() | term()) -> ok.
stop(PoolRef) ->
    nhttpc_pool_sup:stop(PoolRef).

-doc """
Stop a client pool with options.
Mirrors `nhttp:stop/2` (server side). Accepted options:
  * `drain => false` (default) - stop immediately, equivalent to `stop/1`.
  * `drain => true` - stop accepting new checkouts, wait for in-flight
    requests to complete, then shut down. The waiter queue is released
    immediately with `{error, pool_closed}`.
  * `drain_timeout => timeout()` - upper bound on drain (default 10000 ms).
    Only meaningful when `drain => true`.
  * `force_kill_delay => non_neg_integer()` - milliseconds to wait for the
    supervisor to terminate politely before sending `exit/2` with `kill`.
""".
-spec stop(pid() | term(), stop_opts()) ->
    ok | {error, error()}.
stop(PoolRef, Opts) ->
    case nhttpc_pool_sup:stop(PoolRef, Opts) of
        ok -> ok;
        {error, {invalid_stop_opts, Kind}} -> nhttpc_error:invalid_opts(Kind)
    end.

%%%-----------------------------------------------------------------------------
%% SYNCHRONOUS REQUESTS
%%%-----------------------------------------------------------------------------
-spec apply_body_from(nhttp_lib:request(), opts()) -> {nhttp_lib:request(), opts()}.
apply_body_from(Req, #{body_from := {file, Path}} = Opts) ->
    {Req#{body => {stream, {file, Path}}}, maps:remove(body_from, Opts)};
apply_body_from(Req, #{body_from := {iter, Iter}} = Opts) when is_function(Iter, 0) ->
    {Req#{body => {stream, Iter}}, maps:remove(body_from, Opts)};
apply_body_from(Req, Opts) ->
    {Req, Opts}.

-doc "Send a request and wait for the response.".
-spec request(request()) -> {ok, nhttp_lib:response()} | {error, error()}.
request(Request) ->
    request(Request, #{}).

-doc "Send a request with options.".
-spec request(request(), opts()) -> {ok, nhttp_lib:response()} | {error, error()}.
request(Request, Opts) ->
    Result =
        case nhttpc_opts:normalize(Opts) of
            {ok, NormalizedOpts} ->
                case nhttpc_url:to_request(Request) of
                    {ok, NormalizedReq, UrlInfo} ->
                        {Req, ReqOpts} = apply_body_from(NormalizedReq, NormalizedOpts),
                        nhttpc_request:do_request(Req, UrlInfo, ReqOpts);
                    {error, _} = Error ->
                        Error
                end;
            {error, _} = Error ->
                Error
        end,
    nhttpc_error:wrap_result(Result).

%%%-----------------------------------------------------------------------------
%% ASYNCHRONOUS REQUESTS
%%%-----------------------------------------------------------------------------
-doc "Wait for async response with default timeout.".
-spec await(handle()) -> {ok, nhttp_lib:response()} | {error, error()}.
await(Handle) ->
    await(Handle, 30000).

-doc "Wait for async response with timeout.".
-spec await(handle(), timeout()) ->
    {ok, nhttp_lib:response()} | {error, error()}.
await(Handle, Timeout) ->
    Result =
        receive
            {nhttpc, Handle, {response, Resp}} ->
                flush_done(Handle),
                {ok, Resp};
            {nhttpc, Handle, {error, Reason}} ->
                {error, Reason}
        after Timeout ->
            nhttpc_error:timeout(request)
        end,
    nhttpc_error:wrap_result(Result).

-doc """
Cancel an async request.
The call blocks until the worker has torn down any in-flight request
and acknowledged, then drains the caller's mailbox of
`{nhttpc, Handle, _}` so the no-leak contract holds. Returns `ok`.
""".
-spec cancel(handle()) -> ok.
cancel(Handle) ->
    case nhttpc_handle:kind(Handle) of
        ws -> cancel_ws(Handle);
        _ -> cancel_worker(Handle)
    end.

-spec cancel_worker(handle()) -> ok.
cancel_worker(Handle) ->
    Pid = nhttpc_handle:pid(Handle),
    Ref = nhttpc_handle:ref(Handle),
    MRef = monitor(process, Pid),
    Pid ! {'$nhttpc_cancel', Ref, self()},
    receive
        {'$nhttpc_cancel_ack', Ref} ->
            erlang:demonitor(MRef, [flush]);
        {'DOWN', MRef, process, _, _} ->
            ok
    after 5000 ->
        exit(Pid, kill),
        receive
            {'DOWN', MRef, process, _, _} -> ok
        after 1000 ->
            erlang:demonitor(MRef, [flush])
        end
    end,
    flush(Handle),
    ok.

-spec cancel_ws(handle()) -> ok.
cancel_ws(Handle) ->
    Pid = nhttpc_handle:pid(Handle),
    case is_process_alive(Pid) of
        false ->
            flush(Handle),
            ok;
        true ->
            MRef = monitor(process, Pid),
            Pid ! {ws_close, 1000, <<>>},
            cancel_ws_wait(Handle, Pid, MRef)
    end.

-spec cancel_ws_wait(handle(), pid(), reference()) -> ok.
cancel_ws_wait(Handle, Pid, MRef) ->
    receive
        {nhttpc, Handle, {done, _}} ->
            erlang:demonitor(MRef, [flush]),
            flush(Handle),
            ok;
        {nhttpc, Handle, {error, _}} ->
            erlang:demonitor(MRef, [flush]),
            flush(Handle),
            ok;
        {'DOWN', MRef, process, _, _} ->
            flush(Handle),
            ok
    after 5000 ->
        exit(Pid, kill),
        receive
            {'DOWN', MRef, process, _, _} -> ok
        after 1000 ->
            erlang:demonitor(MRef, [flush])
        end,
        flush(Handle),
        ok
    end.

-doc """
Grant `N` additional bytes of consumer credit to a streaming handle.
Streaming downloads use explicit consumer-credit backpressure: the
worker delivers `{nhttpc, Handle, {data, _, _}}` events only while the
caller has uncomsumed credit. When credit is exhausted, the worker
holds the next chunk and emits `{nhttpc, Handle, {pause, BytesBuffered}}`;
after `consume/2` drops the buffer below `consume_low_water`, a
`{resume, BytesBuffered}` event is emitted.
`consume/2` is a no-op for non-streaming handles (`send/2`, `ws_open/2`),
for already-finished streams, and for streams started with
`consume_initial => infinity`.
""".
-spec consume(handle(), pos_integer()) -> ok.
consume(Handle, N) when is_integer(N), N > 0 ->
    Pid = nhttpc_handle:pid(Handle),
    Ref = nhttpc_handle:ref(Handle),
    erlang:send(Pid, {'$nhttpc_consume', Ref, N}, [noconnect]),
    ok.

-doc """
Drain any pending `{nhttpc, Handle, _}` messages from the caller's mailbox.
Idempotent and never blocks. Useful after a unified-protocol request
already completed to discard the lingering `{done, _}` event.
""".
-spec flush(handle()) -> ok.
flush(Handle) ->
    receive
        {nhttpc, Handle, _} -> flush(Handle)
    after 0 ->
        ok
    end.

-spec flush_done(handle()) -> ok.
flush_done(Handle) ->
    receive
        {nhttpc, Handle, {done, _}} -> ok
    after 0 ->
        ok
    end.

-doc """
Send an async request returning a unified handle.
The handle delivers events as `{nhttpc, Handle, Event}` messages to the
caller. The terminal event is `{done, normal}` after `{response, _}` or
`{error, _}` on failure.
""".
-spec send(nhttp_lib:request()) -> {ok, handle()} | {error, error()}.
send(Request) ->
    send(Request, #{}).

-doc "Send an async request with options. Returns a unified handle.".
-spec send(nhttp_lib:request(), opts()) -> {ok, handle()} | {error, error()}.
send(Request, Opts) ->
    Result =
        case nhttpc_opts:normalize(Opts) of
            {ok, NormalizedOpts} ->
                case nhttpc_url:to_request(Request) of
                    {ok, NormalizedReq, UrlInfo} ->
                        {Req, ReqOpts} = apply_body_from(NormalizedReq, NormalizedOpts),
                        nhttpc_request_worker:start(self(), Req, UrlInfo, ReqOpts);
                    {error, _} = Error ->
                        Error
                end;
            {error, _} = Error ->
                Error
        end,
    nhttpc_error:wrap_result(Result).

-doc """
Open a streaming request, returning a unified `handle()`.
Per-chunk events are delivered as `{nhttpc, Handle, Event}` messages on
the caller's mailbox:
```
{nhttpc, Handle, {headers, Status, Headers}}
{nhttpc, Handle, {data, Chunk, more}}
{nhttpc, Handle, {trailers, Headers}}     %% HTTP/2 + HTTP/3 only
{nhttpc, Handle, {done, normal}}          %% terminal
{nhttpc, Handle, {error, Reason}}         %% terminal
```
Streaming is always one-shot for the duration of plan 07b — `via` other
than the implicit `oneshot` is rejected, and `body_to` is not accepted
because streaming downloads emit events instead of writing to a sink.
""".
-spec stream(nhttp_lib:request()) -> {ok, handle()} | {error, error()}.
stream(Request) ->
    stream(Request, #{}).

-doc "Open a streaming request with options. See `stream/1`.".
-spec stream(nhttp_lib:request(), opts()) -> {ok, handle()} | {error, error()}.
stream(Request, Opts) ->
    Result =
        case validate_stream_opts(Opts) of
            ok ->
                case nhttpc_opts:normalize(Opts) of
                    {ok, NormalizedOpts} ->
                        case nhttpc_url:to_request(Request) of
                            {ok, NormalizedReq, UrlInfo} ->
                                {Req, ReqOpts} = apply_body_from(NormalizedReq, NormalizedOpts),
                                nhttpc_stream_worker:start(self(), Req, UrlInfo, ReqOpts);
                            {error, _} = Error ->
                                Error
                        end;
                    {error, _} = Error ->
                        Error
                end;
            {error, _} = Error ->
                Error
        end,
    nhttpc_error:wrap_result(Result).

-spec validate_stream_opts(opts()) -> ok | {error, error()}.
validate_stream_opts(Opts) ->
    case maps:is_key(body_to, Opts) of
        true ->
            nhttpc_error:invalid_opts(body_to_on_stream);
        false ->
            case maps:get(via, Opts, oneshot) of
                oneshot -> ok;
                _ -> nhttpc_error:invalid_opts(stream_via_not_oneshot)
            end
    end.

%%%-----------------------------------------------------------------------------
%% CONNECTION-LEVEL ASYNC DISPATCH
%%%-----------------------------------------------------------------------------
-doc """
Await response from a specific connection with proper monitor.
Handles connection death gracefully. Use this with `send_request/2`.
""".
-spec await_response(pid(), reference(), timeout()) ->
    {ok, nhttp_lib:response()} | {error, error()}.
await_response(ConnPid, StreamRef, Timeout) ->
    MonRef = monitor(process, ConnPid),
    Result =
        try
            receive
                {nhttpc_response, ConnPid, StreamRef, Response} ->
                    Response;
                {nhttpc_error, ConnPid, StreamRef, Reason} ->
                    {error, Reason};
                {'DOWN', MonRef, process, ConnPid, Reason} ->
                    {error, {connection_down, Reason}}
            after Timeout ->
                nhttpc_error:timeout(request)
            end
        after
            demonitor(MonRef, [flush])
        end,
    nhttpc_error:wrap_result(Result).

-doc """
Send request asynchronously on a specific connection.
This is for users who manage connections directly (e.g., via pools).
Returns immediately with a StreamRef. Response delivered via message:
  `{nhttpc_response, ConnPid, StreamRef, {ok, Response}}`
  `{nhttpc_error, ConnPid, StreamRef, Reason}`
Use `await_response/3` for convenient selective receive.
```erlang
{ok, ConnPid} = nhttpc_conn:start_link(Opts),
{ok, StreamRef} = nhttpc:send_request(ConnPid, Req),
{ok, Response} = nhttpc:await_response(ConnPid, StreamRef, 5000).
```
""".
-spec send_request(pid(), nhttp_lib:request()) -> {ok, reference()}.
send_request(ConnPid, Req) ->
    nhttpc_conn:send_async(ConnPid, Req, self()).

%%%-----------------------------------------------------------------------------
%% CONVENIENCE FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec convenience(nhttp_lib:method(), binary(), iodata() | undefined, opts()) ->
    {ok, nhttp_lib:response()} | {error, error()}.
convenience(Method, Url, Body, Opts) ->
    case url(Method, Url) of
        {ok, Req0} ->
            Headers = maps:get(headers, Opts, []),
            ReqOpts = maps:remove(headers, Opts),
            Req1 = Req0#{headers => Headers},
            Req =
                case Body of
                    undefined -> Req1;
                    _ -> Req1#{body => Body}
                end,
            request(Req, ReqOpts);
        {error, _} = Error ->
            Error
    end.

-doc "DELETE request.".
-spec delete(binary()) -> {ok, nhttp_lib:response()} | {error, error()}.
delete(Url) ->
    delete(Url, #{}).

-doc "DELETE request with options.".
-spec delete(binary(), opts()) ->
    {ok, nhttp_lib:response()} | {error, error()}.
delete(Url, Opts) ->
    convenience(delete, Url, undefined, Opts).

-doc "Simple GET request.".
-spec get(binary()) -> {ok, nhttp_lib:response()} | {error, error()}.
get(Url) ->
    get(Url, #{}).

-doc "GET request with options.".
-spec get(binary(), opts()) -> {ok, nhttp_lib:response()} | {error, error()}.
get(Url, Opts) ->
    convenience(get, Url, undefined, Opts).

-doc "HEAD request.".
-spec head(binary()) -> {ok, nhttp_lib:response()} | {error, error()}.
head(Url) ->
    head(Url, #{}).

-doc "HEAD request with options.".
-spec head(binary(), opts()) -> {ok, nhttp_lib:response()} | {error, error()}.
head(Url, Opts) ->
    convenience(head, Url, undefined, Opts).

-doc "OPTIONS request.".
-spec options(binary()) -> {ok, nhttp_lib:response()} | {error, error()}.
options(Url) ->
    options(Url, #{}).

-doc "OPTIONS request with options.".
-spec options(binary(), opts()) ->
    {ok, nhttp_lib:response()} | {error, error()}.
options(Url, Opts) ->
    convenience(options, Url, undefined, Opts).

-doc "PATCH request with body.".
-spec patch(binary(), iodata()) -> {ok, nhttp_lib:response()} | {error, error()}.
patch(Url, Body) ->
    patch(Url, Body, #{}).

-doc "PATCH request with body and options.".
-spec patch(binary(), iodata(), opts()) ->
    {ok, nhttp_lib:response()} | {error, error()}.
patch(Url, Body, Opts) ->
    convenience(patch, Url, Body, Opts).

-doc "POST request with body.".
-spec post(binary(), iodata()) -> {ok, nhttp_lib:response()} | {error, error()}.
post(Url, Body) ->
    post(Url, Body, #{}).

-doc "POST request with body and options.".
-spec post(binary(), iodata(), opts()) ->
    {ok, nhttp_lib:response()} | {error, error()}.
post(Url, Body, Opts) ->
    convenience(post, Url, Body, Opts).

-doc "PUT request with body.".
-spec put(binary(), iodata()) -> {ok, nhttp_lib:response()} | {error, error()}.
put(Url, Body) ->
    put(Url, Body, #{}).

-doc "PUT request with body and options.".
-spec put(binary(), iodata(), opts()) ->
    {ok, nhttp_lib:response()} | {error, error()}.
put(Url, Body, Opts) ->
    convenience(put, Url, Body, Opts).

%%%-----------------------------------------------------------------------------
%% URL PARSING / REQUEST BUILDER
%%%-----------------------------------------------------------------------------
-doc """
Parse a URL into components.
Supports the `http`, `https`, `ws`, and `wss` schemes.
""".
-spec parse_url(binary()) -> {ok, url_info()} | {error, error()}.
parse_url(Url) ->
    case nhttpc_url:parse_url(Url) of
        {ok, _} = Ok -> Ok;
        {error, Reason} -> nhttpc_error:invalid_request(Reason)
    end.

-doc """
Build a canonical `nhttp_lib:request()` from a method and URL.
Parses the URL and fills `method`, `scheme`, `authority`, `path`, and
an empty `headers` list on the result. Callers merge in their own
headers and body before dispatching:
```erlang
{ok, Req0} = nhttpc:url(get, <<"https://api.example.com/users/42">>),
Req = Req0#{headers => [{<<"accept">>, <<"application/json">>}]},
nhttpc:request(Req).
```
`url/2` accepts `http`, `https`, `ws`, and `wss` URLs.
""".
-spec url(nhttp_lib:method(), binary()) ->
    {ok, nhttp_lib:request()} | {error, error()}.
url(Method, UrlBin) when is_binary(UrlBin) ->
    case nhttpc_url:parse_url(UrlBin) of
        {ok, #{scheme := Scheme, path := Path} = UrlInfo} ->
            Query = maps:get(query, UrlInfo, <<>>),
            FullPath =
                case Query of
                    <<>> -> Path;
                    _ -> <<Path/binary, "?", Query/binary>>
                end,
            Authority = url_info_to_authority(UrlInfo),
            {ok, #{
                method => nhttpc_url:normalize_method(Method),
                scheme => Scheme,
                authority => Authority,
                path => FullPath,
                headers => []
            }};
        {error, Reason} ->
            nhttpc_error:invalid_request(Reason)
    end.

-spec url_info_to_authority(url_info()) -> binary().
url_info_to_authority(#{host := Host, port := Port, scheme := Scheme}) ->
    DefaultPort =
        case Scheme of
            http -> 80;
            https -> 443;
            ws -> 80;
            wss -> 443
        end,
    case Port of
        DefaultPort -> Host;
        _ -> <<Host/binary, ":", (integer_to_binary(Port))/binary>>
    end.

%%%-----------------------------------------------------------------------------
%% CONNECTION LIFECYCLE
%%%-----------------------------------------------------------------------------
-doc """
Close a connection opened with `open/2`. Idempotent — calling close on
an already-closed connection is a no-op.
""".
-spec close(connection()) -> ok.
close(#{pid := Pid}) when is_pid(Pid) ->
    case is_process_alive(Pid) of
        false ->
            ok;
        true ->
            MRef = monitor(process, Pid),
            try
                nhttpc_conn:stop(Pid)
            catch
                _:_ -> ok
            end,
            receive
                {'DOWN', MRef, process, _, _} -> ok
            after 5000 ->
                exit(Pid, kill),
                receive
                    {'DOWN', MRef, process, _, _} -> ok
                after 1000 ->
                    erlang:demonitor(MRef, [flush]),
                    ok
                end
            end
    end.

-doc false.
-spec conn_pid(connection()) -> pid().
conn_pid(#{pid := Pid}) -> Pid.

-spec do_open(target(), opts()) -> {ok, connection()} | {error, nhttpc:error() | term()}.
do_open(#{host := Host, port := Port}, Opts) ->
    HostBin = host_to_binary(Host),
    Versions = nhttpc_opts:versions(Opts),
    Transport =
        case nhttpc_opts:tls(Opts) of
            undefined -> tcp;
            _ -> ssl
        end,
    ConnOpts0 = #{
        host => HostBin,
        port => Port,
        transport => Transport,
        connect_timeout => nhttpc_opts:timeout(connect, Opts),
        protocols => Versions
    },
    ConnOpts = maybe_add_tls(ConnOpts0, nhttpc_opts:tls(Opts)),
    OldFlag = process_flag(trap_exit, true),
    Result =
        case nhttpc_conn:start_link(ConnOpts) of
            {ok, Pid} ->
                case nhttpc_conn:info(Pid) of
                    {ok, #{version := Version, peer := Peer}} ->
                        unlink(Pid),
                        {ok, #{pid => Pid, version => Version, peer => Peer}};
                    {error, Reason} ->
                        unlink(Pid),
                        try
                            nhttpc_conn:stop(Pid)
                        catch
                            _:_ -> ok
                        end,
                        {error, {connect_error, Reason}}
                end;
            {error, Reason} ->
                drain_exit_signals(),
                {error, {connect_error, Reason}}
        end,
    case OldFlag of
        false -> process_flag(trap_exit, false);
        true -> ok
    end,
    Result.

-spec drain_exit_signals() -> ok.
drain_exit_signals() ->
    receive
        {'EXIT', _, _} -> drain_exit_signals()
    after 0 ->
        ok
    end.

-spec host_to_binary(binary() | string() | inet:ip_address()) -> binary().
host_to_binary(Host) when is_binary(Host) -> Host;
host_to_binary(Host) when is_list(Host) -> list_to_binary(Host);
host_to_binary(Host) when is_tuple(Host) -> list_to_binary(inet:ntoa(Host)).

-spec maybe_add_tls(map(), undefined | map()) -> map().
maybe_add_tls(ConnOpts, undefined) -> ConnOpts;
maybe_add_tls(ConnOpts, TlsMap) -> maps:merge(ConnOpts, TlsMap).

-doc """
Open a standalone connection to `Target`, returning an opaque
`connection()` the caller passes via `via => {conn, _}` on subsequent
requests.
Useful primarily for HTTP/2 and HTTP/3 multiplexing where one
transport carries many concurrent requests. The handshake (TCP + TLS,
or QUIC) completes synchronously; a failure surfaces as
`{error, {connect_error, _}}`.
""".
-spec open(target(), opts()) -> {ok, connection()} | {error, error()}.
open(Target, Opts) ->
    Result =
        case nhttpc_opts:normalize(Opts) of
            {ok, NormalizedOpts} ->
                do_open(Target, NormalizedOpts);
            {error, _} = Error ->
                Error
        end,
    nhttpc_error:wrap_result(Result).

%%%-----------------------------------------------------------------------------
%% WEBSOCKET
%%%-----------------------------------------------------------------------------
-spec build_ws_opts(opts(), reference()) -> nhttpc_ws:opts().
build_ws_opts(Opts, Ref) ->
    WsSub = maps:get(ws, Opts, #{}),
    Base = #{handle_ref => Ref},
    Base1 = ws_opt_connect_timeout(Base, Opts),
    Base2 = ws_opt_idle_timeout(Base1, Opts),
    Base3 = ws_opt_headers(Base2, Opts),
    Base4 = ws_opt_protocols(Base3, WsSub),
    Base5 = ws_opt_ping_interval(Base4, WsSub),
    ws_opt_tls(Base5, Opts).

-spec do_ws_open(binary(), opts()) -> {ok, handle()} | {error, nhttpc:error() | term()}.
do_ws_open(Url, Opts) ->
    Ref = make_ref(),
    WsOpts = build_ws_opts(Opts, Ref),
    case nhttpc_ws:start_link(Url, WsOpts) of
        {ok, Pid, _ConnInfo} ->
            unlink(Pid),
            {ok, nhttpc_handle:new(Pid, Ref, ws)};
        {error, Reason} ->
            {error, ws_connect_error(Reason)}
    end.

-spec tls_to_ssl_opts(map()) -> [ssl:tls_client_option()].
tls_to_ssl_opts(Tls) ->
    Pairs = [
        {verify, verify},
        {cacerts, cacerts},
        {cacertfile, cacertfile},
        {certfile, certfile},
        {keyfile, keyfile},
        {server_name_indication, server_name_indication},
        {versions, versions},
        {alpn_advertised_protocols, alpn_advertised_protocols},
        {supported_groups, supported_groups}
    ],
    Base = [{K, V} || {MapK, K} <- Pairs, {ok, V} <- [maps:find(MapK, Tls)]],
    Extra = maps:get(extra, Tls, []),
    Base ++ Extra.

-spec ws_connect_error(term()) -> error().
ws_connect_error({unexpected_status, Status}) ->
    {protocol_error, http1_1, {unexpected_status, Status}};
ws_connect_error(invalid_ws_uri) ->
    {invalid_request, invalid_ws_uri};
ws_connect_error(Reason) ->
    {connect_error, Reason}.

-doc """
Open a WebSocket connection, returning a `handle()`.
The owner receives events through the `{nhttpc, Handle, _}` envelope:
```
{nhttpc, Handle, {ws_ready, ConnInfo}}    %% subprotocol negotiation result
{nhttpc, Handle, {ws, Frame}}             %% per frame
{nhttpc, Handle, {done, normal}}          %% terminal: clean close
{nhttpc, Handle, {done, {remote_closed, Code}}}  %% terminal: server-initiated
{nhttpc, Handle, {error, Reason}}         %% terminal: timeout / protocol
```
WebSocket is always oneshot — the handshake consumes the transport so
neither `{conn, _}` nor `{pool, _}` dispatch is meaningful. Any
non-default `via` returns `{error, {invalid_opts, _}}`.
""".
-spec ws_open(binary(), opts()) ->
    {ok, handle()} | {error, error()}.
ws_open(Url, Opts) when is_binary(Url) ->
    Result =
        case maps:get(via, Opts, oneshot) of
            {conn, _} ->
                nhttpc_error:invalid_opts(ws_on_connection);
            {pool, _} ->
                nhttpc_error:invalid_opts(ws_via_not_oneshot);
            oneshot ->
                do_ws_open(Url, Opts)
        end,
    nhttpc_error:wrap_result(Result).

-spec ws_opt_connect_timeout(map(), opts()) -> map().
ws_opt_connect_timeout(Acc, #{timeouts := #{connect := T}}) -> Acc#{connect_timeout => T};
ws_opt_connect_timeout(Acc, _) -> Acc.

-spec ws_opt_headers(map(), opts()) -> map().
ws_opt_headers(Acc, #{headers := H}) when is_list(H) -> Acc#{headers => H};
ws_opt_headers(Acc, _) -> Acc.

-spec ws_opt_idle_timeout(map(), opts()) -> map().
ws_opt_idle_timeout(Acc, #{timeouts := #{idle := T}}) -> Acc#{idle_timeout => T};
ws_opt_idle_timeout(Acc, _) -> Acc.

-spec ws_opt_ping_interval(map(), map()) -> map().
ws_opt_ping_interval(Acc, #{ping_interval := I}) -> Acc#{ping_interval => I};
ws_opt_ping_interval(Acc, _) -> Acc.

-spec ws_opt_protocols(map(), map()) -> map().
ws_opt_protocols(Acc, #{subprotocols := P}) when is_list(P) -> Acc#{protocols => P};
ws_opt_protocols(Acc, _) -> Acc.

-spec ws_opt_tls(map(), opts()) -> map().
ws_opt_tls(Acc, #{tls := Tls}) when is_map(Tls) ->
    Acc#{ssl_opts => tls_to_ssl_opts(Tls)};
ws_opt_tls(Acc, _) ->
    Acc.

-doc """
Send a WebSocket frame on a handle returned by `ws_open/2`.
`Frame` is an `nhttp_lib:ws_frame()` — `{text, _}`, `{binary, _}`,
`{ping, _}`, or `{pong, _}`. Returns `{error, {invalid_request, _}}`
if the handle was not opened for WebSocket, and
`{error, {ws_closed, noproc}}` if the WS process has already exited.
""".
-spec ws_send(handle(), nhttp_lib:ws_frame()) -> ok | {error, error()}.
ws_send(Handle, Frame) ->
    case nhttpc_handle:is_handle(Handle) andalso nhttpc_handle:kind(Handle) =:= ws of
        true ->
            Pid = nhttpc_handle:pid(Handle),
            case is_process_alive(Pid) of
                true ->
                    Pid ! {ws_send, Frame},
                    ok;
                false ->
                    nhttpc_error:connection_closed()
            end;
        false ->
            nhttpc_error:invalid_request(not_ws_handle)
    end.
