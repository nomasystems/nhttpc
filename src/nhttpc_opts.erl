-module(nhttpc_opts).

-moduledoc """
Centralised validation and accessors for `nhttpc:opts/0` and
`nhttpc:pool_opts/0`.

Public entry points (`nhttpc:request/2`, every convenience verb,
`nhttpc:start_link/2`, …) call `normalize/1` or `normalize_pool/1`
exactly once. After normalisation, every internal consumer reads the
1.0 canonical vocabulary:

  * `timeouts => #{connect, request, checkout, idle}` (one mechanism).
  * `tls => #{...}` map mirroring `nhttp:tls/0` client-relevant keys.
  * `versions => [nhttp_lib:version()]`.
  * `body_to => {file, _} | {fold, F}`.
  * `body_from => {file, _} | {iter, F}`.

Unknown keys fail with `{invalid_opts, {unknown, Key}}`. The wire
transport is derived from URL scheme + `tls` presence + `versions`
(see `request_transport/2` / `pool_transport/1`).

## Sizing `size` and `max_overflow`

The pool grows to `size + max_overflow` connections. Under `pack` it
opens the next connection when no connection can take the request, for
lack of a stream slot or for lack of HTTP/2 send credit. The defaults,
`size => 4` and `max_overflow => 4`, fit an origin that credits on
receipt.

An origin that credits the connection window on processing bounds each
connection to `half_window / credit_latency` octets a second, where
`half_window` is the credit the peer returns at a time and
`credit_latency` is its response latency. The connections must cover
`offered_rate * body_size * credit_latency / half_window`, multiplied by
two for headroom. APNs returns 32830 octets at a time, 270 milliseconds
after the body at the median: 10000 requests a second of 134-octet
bodies need 11 connections at zero headroom, so `size => 22,
max_overflow => 11`. The `[nhttpc, connection, credit]` event reports
the peer's credit rate and inter-arrival, which are the inputs to the
rule.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS - NORMALISATION
%%%-----------------------------------------------------------------------------
-export([
    normalize/1,
    normalize_pool/1
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - ACCESSORS
%%%-----------------------------------------------------------------------------
-export([
    max_streams_per_connection/1,
    pool_transport/1,
    request_transport/2,
    send_queue_bounds/1,
    stream_selection/1,
    timeout/2,
    timeout/3,
    tls/1,
    versions/1,
    versions/2
]).

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(DEFAULT_REQUEST_TIMEOUT, 30_000).
-define(DEFAULT_CONNECT_TIMEOUT, 5_000).
-define(DEFAULT_CHECKOUT_TIMEOUT, 5_000).
-define(DEFAULT_IDLE_TIMEOUT, 60_000).
-define(DEFAULT_MAX_STREAMS_PER_CONNECTION, 1 bsl 29).
-define(MAX_STREAMS_PER_CONNECTION_BOUND, (1 bsl 30) - 1).

%%%-----------------------------------------------------------------------------
%% NORMALISATION
%%%-----------------------------------------------------------------------------
-doc """
Validate request options. Returns `{error, {invalid_opts, _}}` on
unknown keys or invalid cookie-jar values.
""".
-spec normalize(map()) -> {ok, nhttpc:opts()} | {error, {invalid_opts, term()}}.
normalize(Opts) when is_map(Opts) ->
    try
        ok = validate_keys(Opts, request_keys()),
        ok = validate_cookie_jar(Opts),
        {ok, Opts}
    catch
        throw:{invalid_opts, _} = Reason ->
            {error, Reason}
    end;
normalize(_Other) ->
    {error, {invalid_opts, not_a_map}}.

-doc """
Validate pool options. Same validation as `normalize/1`, with the
pool-specific whitelist (`host`, `port`, `size`, …).
""".
-spec normalize_pool(map()) ->
    {ok, nhttpc:pool_opts()} | {error, {invalid_opts, term()}}.
normalize_pool(Opts) when is_map(Opts) ->
    try
        ok = validate_keys(Opts, pool_keys()),
        ok = validate_stream_selection(Opts),
        ok = validate_max_streams_per_connection(Opts),
        ok = validate_send_queue_bounds(Opts),
        {ok, Opts}
    catch
        throw:{invalid_opts, _} = Reason ->
            {error, Reason}
    end;
normalize_pool(_Other) ->
    {error, {invalid_opts, not_a_map}}.

%%%-----------------------------------------------------------------------------
%% ACCESSORS
%%%-----------------------------------------------------------------------------
-doc """
The number of streams an HTTP/2 connection opens before it retires.
A client opens odd stream identifiers from 1 and RFC 9113 section 5.1.1
caps them at 2^31 - 1, so one connection holds 2^30 streams. When the
connection reaches this count it tells its pool, serves what is in
flight, and exits. The pool opens the replacement the moment the drain
starts. The default is 2^29 streams, half the identifier space: 4.3
days for each connection at 1430 streams a second, and 14.9 hours at
10000 streams a second on one connection. The option accepts a positive
integer at or below 2^30 - 1, so no accepted value can reach the codec
bound.
Every rotation pays a TLS handshake and a cold HPACK table. A
deployment that lowers the value for another reason, such as a
periodic TLS key refresh, pays that cost at the rate it chooses.
""".
-spec max_streams_per_connection(map()) -> pos_integer().
max_streams_per_connection(Opts) ->
    maps:get(max_streams_per_connection, Opts, ?DEFAULT_MAX_STREAMS_PER_CONNECTION).

-doc """
Wire transport for a pool, derived from `tls` presence + `versions`:
  * `versions` contains `http3` — `quic`.
  * `tls` set — `ssl`.
  * neither — `tcp`.
""".
-spec pool_transport(map()) -> tcp | ssl | quic.
pool_transport(Opts) ->
    Versions = versions(Opts, []),
    case lists:member(http3, Versions) of
        true ->
            quic;
        false ->
            case tls(Opts) of
                undefined -> tcp;
                _ -> ssl
            end
    end.

-doc """
Wire transport for a request, derived from URL scheme + `tls` presence
+ `versions`:
  * `https` URL with `http3` in `versions` — `quic`.
  * `https` URL — `ssl`.
  * `http`  URL — `tcp`.
A `tls` map is meaningful only on `https` URLs; on `http` it is
ignored.
""".
-spec request_transport(nhttpc_url:http_url_info(), map()) -> tcp | ssl | quic.
request_transport(#{scheme := https}, Opts) ->
    case lists:member(http3, versions(Opts, [])) of
        true -> quic;
        false -> ssl
    end;
request_transport(#{scheme := http}, _Opts) ->
    tcp.

-doc """
The bounds of the HTTP/2 codec send queue on each pooled connection,
as the `t:nhttp_h2:settings/0` keys `max_send_buffer` and
`max_queued_streams`. A key the pool options do not name is absent, and
the codec applies its own default: 128 KiB of queued octets and a
quarter of the peer's concurrent stream cap.
The queue holds the octets of a body above the initial connection
window that the peer's credit cannot take yet. A body that meets either
bound is refused with the retryable `refused_stream` reason and
re-dispatched, so the bounds cap how much of a large body one
connection holds open, not how large a body the pool accepts.
""".
-spec send_queue_bounds(map()) -> nhttp_h2:settings().
send_queue_bounds(Opts) ->
    maps:with([max_send_buffer, max_queued_streams], Opts).

-doc """
Connection selection policy for multiplexed protocols. `pack` walks
the connections oldest first and takes the first one with a free
stream slot. `spread` keeps a round-robin cursor on the fast path and
the least-loaded connection on the manager path. The default is
`pack`, which is what RFC 9113 section 9.1 and RFC 9114 section 3.3
ask a client to do. The policy has no effect on HTTP/1.1, whose
checkout is exclusive.
""".
-spec stream_selection(map()) -> nhttpc:stream_selection().
stream_selection(Opts) ->
    maps:get(stream_selection, Opts, pack).

-doc """
Read a `timeouts.<Key>` slot with a built-in default. Use `timeout/3`
to override.
Defaults:
  * `connect`  — 5_000 ms
  * `request`  — 30_000 ms
  * `checkout` — 5_000 ms
  * `idle`     — 60_000 ms
""".
-spec timeout(connect | request | checkout | idle, map()) -> timeout().
timeout(Key, Opts) ->
    timeout(Key, Opts, default_timeout(Key)).

-spec timeout(connect | request | checkout | idle, map(), timeout()) ->
    timeout().
timeout(Key, #{timeouts := Ts}, Default) when is_map(Ts) ->
    maps:get(Key, Ts, Default);
timeout(_Key, _Opts, Default) ->
    Default.

-doc """
The `tls` sub-map, or `undefined` if no TLS configuration is present.
""".
-spec tls(map()) -> map() | undefined.
tls(Opts) ->
    maps:get(tls, Opts, undefined).

-doc """
Versions list with default `[http2, http1_1]`. Use `versions/2` to
override the default (e.g. pool init that prefers a different order).
""".
-spec versions(map()) -> [nhttp_lib:version()].
versions(Opts) ->
    versions(Opts, [http2, http1_1]).

-spec versions(map(), [nhttp_lib:version()]) -> [nhttp_lib:version()].
versions(Opts, Default) ->
    maps:get(versions, Opts, Default).

%%%-----------------------------------------------------------------------------
%% INTERNAL — VALIDATION
%%%-----------------------------------------------------------------------------
-spec validate_cookie_jar(map()) -> ok.
validate_cookie_jar(#{cookie_jar := Jar}) when is_reference(Jar) ->
    ok;
validate_cookie_jar(#{cookie_jar := Other}) ->
    throw({invalid_opts, {bad_cookie_jar, Other}});
validate_cookie_jar(_Opts) ->
    ok.

-spec validate_keys(map(), [atom()]) -> ok.
validate_keys(Opts, Allowed) ->
    maps:foreach(
        fun(Key, _) ->
            case lists:member(Key, Allowed) of
                true -> ok;
                false -> throw({invalid_opts, {unknown, Key}})
            end
        end,
        Opts
    ).

-spec validate_max_streams_per_connection(map()) -> ok.
validate_max_streams_per_connection(#{max_streams_per_connection := N}) when
    is_integer(N), N > 0, N =< ?MAX_STREAMS_PER_CONNECTION_BOUND
->
    ok;
validate_max_streams_per_connection(#{max_streams_per_connection := Other}) ->
    throw({invalid_opts, {bad_max_streams_per_connection, Other}});
validate_max_streams_per_connection(_Opts) ->
    ok.

-spec validate_send_queue_bounds(map()) -> ok.
validate_send_queue_bounds(Opts) ->
    maps:foreach(
        fun
            (_Key, infinity) -> ok;
            (_Key, N) when is_integer(N), N > 0 -> ok;
            (Key, Other) -> throw({invalid_opts, {bad_send_queue_bound, Key, Other}})
        end,
        send_queue_bounds(Opts)
    ).

-spec validate_stream_selection(map()) -> ok.
validate_stream_selection(#{stream_selection := S}) when S =:= pack; S =:= spread ->
    ok;
validate_stream_selection(#{stream_selection := Other}) ->
    throw({invalid_opts, {bad_stream_selection, Other}});
validate_stream_selection(_Opts) ->
    ok.

%%%-----------------------------------------------------------------------------
%% INTERNAL — KEY WHITELISTS
%%%-----------------------------------------------------------------------------
-spec pool_keys() -> [atom()].
pool_keys() ->
    [
        connection_sweep_interval,
        host,
        idle_timeout,
        max_connection_age,
        max_overflow,
        max_pending_requests,
        max_queued_streams,
        max_send_buffer,
        max_streams_per_connection,
        otel,
        port,
        proxy,
        reconnect_backoff_base,
        reconnect_backoff_max,
        size,
        stream_selection,
        timeouts,
        tls,
        versions
    ].

-spec request_keys() -> [atom()].
request_keys() ->
    [
        body_from,
        body_to,
        compress,
        consume_initial,
        consume_low_water,
        cookie_jar,
        decompress,
        fold_init,
        follow_redirects,
        headers,
        max_header_size,
        max_headers_count,
        max_redirects,
        max_response_size,
        on_download_progress,
        on_upload_progress,
        otel,
        proxy,
        redirect_policy,
        retry,
        timeouts,
        tls,
        versions,
        via,
        ws
    ].

%%%-----------------------------------------------------------------------------
%% INTERNAL — DEFAULTS
%%%-----------------------------------------------------------------------------
-spec default_timeout(connect | request | checkout | idle) -> timeout().
default_timeout(connect) -> ?DEFAULT_CONNECT_TIMEOUT;
default_timeout(request) -> ?DEFAULT_REQUEST_TIMEOUT;
default_timeout(checkout) -> ?DEFAULT_CHECKOUT_TIMEOUT;
default_timeout(idle) -> ?DEFAULT_IDLE_TIMEOUT.
