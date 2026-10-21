-module(nhttpc_telemetry).

-moduledoc """
Telemetry integration for the nhttpc HTTP client.

Mirrors `nhttp_otel` on the server side: OpenTelemetry metrics for the
request, connection and pool lifecycle, plus `telemetry` events for
subscribers that prefer the Erlang telemetry library.

Configuration (the `otel` request / pool option):
- `otel => #{events => true, metrics => true, traces => true}`
- `otel => true`  - Shorthand for all three
- `otel => false` - Disabled (default)

The map form enables only the flags it lists; anything omitted stays
off. `#{metrics => true}` therefore records instruments but publishes
no `telemetry` events, and `#{events => true}` publishes events but
records nothing. Only the `true` shorthand turns everything on.

Disabled is the default and it costs nothing. Every entry point takes
the parsed config and returns before reading a clock, building an
attribute map or touching a counter, and callers use `enabled/1` to
skip assembling the metadata in the first place.

Metrics follow the server's naming (`nhttp.http.*` there,
`nhttpc.http.*` here) and the OpenTelemetry HTTP semantic conventions
for attributes:
- `nhttpc.http.request.duration`   - histogram, microseconds
- `nhttpc.http.active_requests`    - updown counter
- `nhttpc.http.request.body.size`  - histogram, bytes
- `nhttpc.http.response.body.size` - histogram, bytes
- `nhttpc.http.connect.duration`   - histogram, microseconds
- `nhttpc.http.pool.queue_time`    - histogram, microseconds
- `nhttpc.http.pool.exhausted`     - counter
- `nhttpc.http.pool.timeout`       - counter

Spans are not emitted yet; `traces` is accepted and reserved so the
option shape stays the same as the server's.

Instruments are registered with the meter on first use. Recording by
name only reaches a backend when the instrument exists, so skipping
that step would silently drop every measurement.

Coverage: `nhttpc:request/2` and `nhttpc:send/2` are instrumented.
`nhttpc:stream/2` is instrumented on HTTP/3 only, since the HTTP/1.1
and HTTP/2 streaming path runs through `nhttpc_stream_recv` rather
than `nhttpc_request:do_request/3`.

Events (published only when `events` is enabled):
  [nhttpc, request, start]  - Request started
  [nhttpc, request, stop]   - Request completed successfully
  [nhttpc, request, error]  - Request failed
  [nhttpc, connect, start]  - Connection attempt started
  [nhttpc, connect, stop]   - Connection established
  [nhttpc, connect, error]  - Connection failed
  [nhttpc, pool, checkout]  - Connection checked out from pool
  [nhttpc, pool, checkin]   - Connection returned to pool
  [nhttpc, pool, timeout]   - Pool checkout timeout
  [nhttpc, pool, exhausted] - Pool exhausted (max_pending_requests reached)
  [nhttpc, connection, idle_timeout] - Connection closed due to idle timeout
  [nhttpc, connection, send_credit_refused] - HTTP/2 request refused for lack of send credit
  [nhttpc, connection, credit] - HTTP/2 connection-level WINDOW_UPDATE rate, once a second at most
  [nhttpc, connection, goaway] - HTTP/2 GOAWAY received, with the peer's debug data
  [nhttpc, connection, rotated] - HTTP/2 connection retired at max_streams_per_connection
  [nhttpc, pool, credit_starved] - Every connection refused a credit reservation, once a second at most
  [nhttpc, connection, send_blocked] - HTTP/2 send queue went from empty to non-empty
  [nhttpc, connection, send_resumed] - HTTP/2 send queue emptied, with the blocked duration
  [nhttpc, connection, send_queue] - HTTP/2 send queue gauges, sampled once a second at most
  [nhttpc, connection, max_streams_changed] - The peer moved SETTINGS_MAX_CONCURRENT_STREAMS
  [nhttpc, pool, occupancy] - Waiter count and connection count, sampled once a second at most

Request events carry `method`, `url`, `scheme`, `host`, `port`, `path`
and `pool` as metadata, plus `status` on stop and `reason` on error.
`start` measures `monotonic_time`; `stop` and `error` measure
`duration`, `req_body_length` and `resp_body_length`, matching the
measurement names `erf` publishes on the server side. Note the unit:
`duration` is in microseconds here, the same unit the metrics use,
whereas `erf` reports native units.

`send_credit_refused` carries `host`, `port`, `conn_pid` and `stream_id`
as metadata and measures `send_window`, the credit the codec had left
for that stream when it refused the body.

`credit` carries `host`, `port` and `conn_pid` as metadata and measures
`count`, `octets`, `mean_interval` and `max_interval`: the WINDOW_UPDATE
frames the peer sent for the connection window since the previous
report, the credit they carried, and the mean and the longest gap
between two of them in microseconds. The credit round trip and the
credit rate that size a pool follow from these.

`goaway` carries `host`, `port`, `conn_pid`, `error_code`,
`last_stream_id` and `debug_data` as metadata. The debug data is the
raw binary from the frame. Nothing parses it.

`rotated` carries `host`, `port` and `conn_pid` as metadata and measures
`streams`, the number of streams the connection opened before it
started to drain. It fires once for each connection, at the moment the
drain starts.

`credit_starved` carries `pool` as metadata. It fires when a checkout
reaches the manager because every connection refused the credit
reservation while at least one had a free stream slot, at most once a
second for each pool, on the rise only.

`send_blocked`, `send_resumed` and `send_queue` carry `host`, `port` and
`conn_pid` as metadata and measure the send queue gauges: `send_window`,
the connection send window of the codec, `send_buffer_bytes`, the octets
the send queue holds, `queued_streams`, the streams with an entry in it,
and `send_blocked_ms`, the time the connection held a non-empty send
queue since it connected. `send_blocked_ms` accumulates. It counts the
period that is open when the event fires, and it never resets, so a
collector takes the rate. `send_resumed` also measures `blocked_ms`, the
length of the period that ended. `send_blocked` and `send_resumed` mark
the transitions of the send queue between empty and non-empty. They do
not bracket every starvation period: against a peer that credits every
few hundred milliseconds, one large upload keeps the queue non-empty
for its whole life, and the pair degenerates to one `send_blocked`
event and a growing `send_blocked_ms`. `send_queue` is the sample: it
fires once a second at most while the queue is non-empty, and with
every `credit` report.

`max_streams_changed` carries `host`, `port` and `conn_pid` as metadata
and measures `previous_max_streams` and `max_streams`, the concurrent
stream cap before and after a SETTINGS frame that moved it. The cap
before the first move is the one the connection reported ready with.

`occupancy` carries `pool` as metadata and measures `waiter_count`, the
checkouts parked at the manager for a connection with a free stream slot
and the send credit for the body, and `connections`, the connections
that take requests, ready or connecting, which `size + max_overflow`
bounds. A draining connection is not counted. The manager samples it
once a second at most while it holds a waiter, and once more when the
last waiter leaves, so the level returns to zero. A pool that never
queues a waiter emits nothing.
""".

%%%-----------------------------------------------------------------------------
%% CONFIGURATION API
%%%-----------------------------------------------------------------------------
-export([
    config/1,
    enabled/1,
    events_enabled/1,
    metrics_enabled/1,
    traces_enabled/1
]).

%%%-----------------------------------------------------------------------------
%% CLIENT REQUEST TELEMETRY
%%%-----------------------------------------------------------------------------
-export([
    client_request_error/3,
    client_request_error/4,
    client_request_start/2,
    client_request_stop/3,
    client_request_stop/4
]).

%%%-----------------------------------------------------------------------------
%% CLIENT CONNECTION TELEMETRY
%%%-----------------------------------------------------------------------------
-export([
    client_connect_error/3,
    client_connect_start/2,
    client_connect_stop/3,
    client_credit/3,
    client_goaway/2,
    client_idle_timeout/2,
    client_max_streams_changed/4,
    client_rotated/3,
    client_send_blocked/3,
    client_send_credit_refused/3,
    client_send_queue/3,
    client_send_resumed/4
]).

%%%-----------------------------------------------------------------------------
%% POOL TELEMETRY
%%%-----------------------------------------------------------------------------
-export([
    client_pool_checkin/2,
    client_pool_checkout/3,
    client_pool_credit_starved/2,
    client_pool_exhausted/2,
    client_pool_occupancy/3,
    client_pool_timeout/2
]).

%%%-----------------------------------------------------------------------------
%% ACTIVE REQUESTS COUNTER
%%%-----------------------------------------------------------------------------
-export([
    active_requests/0
]).

-export_type([
    client_conn_meta/0,
    client_connect_meta/0,
    client_connect_result/0,
    client_credit_meta/0,
    client_credit_report/0,

    client_goaway_meta/0,
    client_pool_level/0,
    client_pool_meta/0,
    client_request_meta/0,
    client_request_result/0,
    client_send_gauges/0,
    otel_config/0
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-type otel_config() ::
    #{
        events := boolean(),
        metrics := boolean(),
        traces := boolean()
    }
    | false.

-type client_request_meta() :: #{
    method := binary(),
    url := binary(),
    scheme := nhttp_lib:scheme(),
    host := binary(),
    port := inet:port_number(),
    path := binary(),
    pool => term()
}.

-type client_request_result() :: #{
    status => 100..599 | undefined,
    reason => term(),
    _ => _
}.

-type client_connect_meta() :: #{
    host := binary(),
    port := inet:port_number(),
    transport := tcp | ssl | udp
}.

-doc """
Connect metadata as in `t:client_connect_meta/0`, minus the transport,
plus the outcome: `protocol` on stop, `reason` on error. The authority is
repeated so the connect duration histogram can be broken down by upstream.
""".
-type client_connect_result() :: #{
    host := binary(),
    port := inet:port_number(),
    protocol => nhttp_lib:version(),
    reason => term()
}.

-type client_pool_meta() :: #{
    pool := term()
}.

-type client_credit_meta() :: #{
    host := binary(),
    port := inet:port_number(),
    conn_pid := pid(),
    stream_id := nhttp_lib:stream_id()
}.

-type client_conn_meta() :: #{
    host := binary(),
    port := inet:port_number(),
    conn_pid := pid()
}.

-type client_credit_report() :: #{
    count := pos_integer(),
    octets := pos_integer(),
    mean_interval := non_neg_integer(),
    max_interval := non_neg_integer()
}.

-type client_goaway_meta() :: #{
    host := binary(),
    port := inet:port_number(),
    conn_pid := pid(),
    error_code := nhttp_lib:error_code(),
    last_stream_id := nhttp_lib:stream_id(),
    debug_data := binary()
}.

-type client_pool_level() :: #{
    waiter_count := non_neg_integer(),
    connections := non_neg_integer()
}.

-type client_send_gauges() :: #{
    send_window := integer(),
    send_buffer_bytes := non_neg_integer(),
    queued_streams := non_neg_integer(),
    send_blocked_ms := non_neg_integer()
}.

%%%-----------------------------------------------------------------------------
%% LOCAL MACROS
%%%-----------------------------------------------------------------------------
-define(ACTIVE_REQUESTS_KEY, nhttpc_active_requests_counter).
-define(INSTRUMENTS_KEY, nhttpc_instruments_registered).
-define(METER_NAME, nhttpc).
-define(METRIC_ACTIVE_REQUESTS, 'nhttpc.http.active_requests').
-define(METRIC_CONNECT_DURATION, 'nhttpc.http.connect.duration').
-define(METRIC_POOL_EXHAUSTED, 'nhttpc.http.pool.exhausted').
-define(METRIC_POOL_QUEUE_TIME, 'nhttpc.http.pool.queue_time').
-define(METRIC_POOL_TIMEOUT, 'nhttpc.http.pool.timeout').
-define(METRIC_REQUEST_BODY_SIZE, 'nhttpc.http.request.body.size').
-define(METRIC_REQUEST_DURATION, 'nhttpc.http.request.duration').
-define(METRIC_RESPONSE_BODY_SIZE, 'nhttpc.http.response.body.size').

%%%-----------------------------------------------------------------------------
%% CONFIGURATION API
%%%-----------------------------------------------------------------------------
-doc "Parse otel configuration from client options.".
-spec config(map()) -> otel_config().
config(Opts) ->
    case maps:get(otel, Opts, false) of
        true ->
            #{events => true, metrics => true, traces => true};
        false ->
            false;
        #{} = Config ->
            #{
                events => maps:get(events, Config, false),
                metrics => maps:get(metrics, Config, false),
                traces => maps:get(traces, Config, false)
            }
    end.

-doc """
Whether anything at all is instrumented. Callers use this to skip
building request metadata on the hot path: with no `otel` option this
is a single map match and the instrumented path is never entered.
""".
-spec enabled(otel_config()) -> boolean().
enabled(false) -> false;
enabled(#{events := false, metrics := false, traces := false}) -> false;
enabled(_Config) -> true.

-doc "Check if telemetry events are enabled.".
-spec events_enabled(otel_config()) -> boolean().
events_enabled(#{events := true}) -> true;
events_enabled(_Config) -> false.

-doc "Check if metrics are enabled.".
-spec metrics_enabled(otel_config()) -> boolean().
metrics_enabled(#{metrics := true}) -> true;
metrics_enabled(_Config) -> false.

-doc "Check if traces are enabled.".
-spec traces_enabled(otel_config()) -> boolean().
traces_enabled(#{traces := true}) -> true;
traces_enabled(_Config) -> false.

%%%-----------------------------------------------------------------------------
%% CLIENT REQUEST TELEMETRY
%%%-----------------------------------------------------------------------------
-doc "Emit event when client request fails.".
-spec client_request_error(otel_config(), integer(), client_request_result()) -> ok.
client_request_error(Config, StartTime, Meta) ->
    client_request_error(Config, StartTime, Meta, #{}).

-doc """
As `client_request_error/3`, with extra measurements (byte counts)
merged alongside the computed `duration`.
""".
-spec client_request_error(otel_config(), integer(), client_request_result(), map()) -> ok.
client_request_error(Config, StartTime, Meta, ExtraMeasurements) ->
    case enabled(Config) of
        false ->
            ok;
        true ->
            request_end(Config, [nhttpc, request, error], StartTime, Meta, ExtraMeasurements)
    end.

-doc """
Emit event when client request starts. Returns the start time used to
compute the duration on stop or error.
""".
-spec client_request_start(otel_config(), client_request_meta()) -> integer().
client_request_start(Config, Meta) ->
    StartTime = erlang:monotonic_time(),
    case enabled(Config) of
        false ->
            StartTime;
        true ->
            incr_active_requests(Config, Meta),
            emit_event(
                Config, [nhttpc, request, start], #{monotonic_time => StartTime}, Meta
            ),
            StartTime
    end.

-doc "Emit event when client request completes successfully.".
-spec client_request_stop(otel_config(), integer(), client_request_result()) -> ok.
client_request_stop(Config, StartTime, Result) ->
    client_request_stop(Config, StartTime, Result, #{}).

-doc """
As `client_request_stop/3`, with extra measurements (byte counts)
merged alongside the computed `duration`.
""".
-spec client_request_stop(otel_config(), integer(), client_request_result(), map()) -> ok.
client_request_stop(Config, StartTime, Result, ExtraMeasurements) ->
    case enabled(Config) of
        false ->
            ok;
        true ->
            request_end(Config, [nhttpc, request, stop], StartTime, Result, ExtraMeasurements)
    end.

%%%-----------------------------------------------------------------------------
%% CLIENT CONNECTION TELEMETRY
%%%-----------------------------------------------------------------------------
-doc "Emit event when client connection fails.".
-spec client_connect_error(otel_config(), integer(), client_connect_result()) -> ok.
client_connect_error(Config, StartTime, Meta) ->
    case enabled(Config) of
        false ->
            ok;
        true ->
            Duration = duration_micros(StartTime),
            record_histogram(Config, ?METRIC_CONNECT_DURATION, Duration, connect_attributes(Meta)),
            emit_event(Config, [nhttpc, connect, error], #{duration => Duration}, Meta)
    end.

-doc """
Emit event when client connection attempt starts. Returns the start
time used to compute the duration on stop or error.
""".
-spec client_connect_start(otel_config(), client_connect_meta()) -> integer().
client_connect_start(Config, Meta) ->
    StartTime = erlang:monotonic_time(),
    case enabled(Config) of
        false ->
            StartTime;
        true ->
            emit_event(
                Config, [nhttpc, connect, start], #{monotonic_time => StartTime}, Meta
            ),
            StartTime
    end.

-doc "Emit event when client connection is established.".
-spec client_connect_stop(otel_config(), integer(), client_connect_result()) -> ok.
client_connect_stop(Config, StartTime, Result) ->
    case enabled(Config) of
        false ->
            ok;
        true ->
            Duration = duration_micros(StartTime),
            record_histogram(
                Config, ?METRIC_CONNECT_DURATION, Duration, connect_attributes(Result)
            ),
            emit_event(Config, [nhttpc, connect, stop], #{duration => Duration}, Result)
    end.

-doc """
Emit the connection-level WINDOW_UPDATE rate of one HTTP/2 connection.
The connection process accumulates the report and emits it once a second
at most, so a subscriber sees the peer's credit behaviour without a
message per frame.
""".
-spec client_credit(otel_config(), client_conn_meta(), client_credit_report()) -> ok.
client_credit(Config, Meta, Report) ->
    emit_event(Config, [nhttpc, connection, credit], Report, Meta).

-doc """
Emit event when an HTTP/2 connection receives GOAWAY. The debug data is
the raw binary the peer sent, unparsed.
""".
-spec client_goaway(otel_config(), client_goaway_meta()) -> ok.
client_goaway(Config, Meta) ->
    emit_event(Config, [nhttpc, connection, goaway], #{}, Meta).

-doc "Emit event when connection closes due to idle timeout.".
-spec client_idle_timeout(otel_config(), #{host := binary(), port := inet:port_number()}) -> ok.
client_idle_timeout(Config, Meta) ->
    emit_event(Config, [nhttpc, connection, idle_timeout], #{}, Meta).

-doc """
Emit event when a SETTINGS frame moves the concurrent stream cap of an
HTTP/2 connection. `Previous` is the cap the connection ran at before
the frame and `MaxStreams` the one it runs at after it.
""".
-spec client_max_streams_changed(
    otel_config(), client_conn_meta(), non_neg_integer(), non_neg_integer()
) -> ok.
client_max_streams_changed(Config, Meta, Previous, MaxStreams) ->
    emit_event(
        Config,
        [nhttpc, connection, max_streams_changed],
        #{previous_max_streams => Previous, max_streams => MaxStreams},
        Meta
    ).

-doc """
Emit event when an HTTP/2 connection starts to drain because it opened
`max_streams_per_connection` streams. `Streams` is that count.
""".
-spec client_rotated(otel_config(), client_conn_meta(), non_neg_integer()) -> ok.
client_rotated(Config, Meta, Streams) ->
    emit_event(Config, [nhttpc, connection, rotated], #{streams => Streams}, Meta).

-doc """
Emit event when the send queue of an HTTP/2 connection goes from empty
to non-empty. The gauges are read at the transition.
""".
-spec client_send_blocked(otel_config(), client_conn_meta(), client_send_gauges()) -> ok.
client_send_blocked(Config, Meta, Gauges) ->
    emit_event(Config, [nhttpc, connection, send_blocked], Gauges, Meta).

-doc """
Emit event when an HTTP/2 request is refused because the connection has
no send credit left for its body. `SendWindow` is the credit the codec
reported for that stream, which can be negative after a peer lowers its
initial window size.
""".
-spec client_send_credit_refused(otel_config(), client_credit_meta(), integer()) -> ok.
client_send_credit_refused(Config, Meta, SendWindow) ->
    emit_event(
        Config, [nhttpc, connection, send_credit_refused], #{send_window => SendWindow}, Meta
    ).

-doc """
Emit the send queue gauges of an HTTP/2 connection. The connection
process samples them once a second at most, so a subscriber sees the
level of a starved connection without a message per frame.
""".
-spec client_send_queue(otel_config(), client_conn_meta(), client_send_gauges()) -> ok.
client_send_queue(Config, Meta, Gauges) ->
    emit_event(Config, [nhttpc, connection, send_queue], Gauges, Meta).

-doc """
Emit event when the send queue of an HTTP/2 connection empties.
`BlockedMs` is the length of the period that ended, and the
`send_blocked_ms` gauge is the total over the life of the connection.
""".
-spec client_send_resumed(
    otel_config(), client_conn_meta(), client_send_gauges(), non_neg_integer()
) -> ok.
client_send_resumed(Config, Meta, Gauges, BlockedMs) ->
    emit_event(
        Config, [nhttpc, connection, send_resumed], Gauges#{blocked_ms => BlockedMs}, Meta
    ).

%%%-----------------------------------------------------------------------------
%% POOL TELEMETRY
%%%-----------------------------------------------------------------------------
-doc "Emit event when connection is returned to pool.".
-spec client_pool_checkin(otel_config(), client_pool_meta()) -> ok.
client_pool_checkin(Config, Meta) ->
    emit_event(Config, [nhttpc, pool, checkin], #{}, Meta).

-doc """
Emit event when connection is checked out from pool. `QueueTimeMs` is
milliseconds, as measured by the pool; the histogram converts it so
every duration instrument here is in microseconds.
""".
-spec client_pool_checkout(otel_config(), client_pool_meta(), non_neg_integer()) -> ok.
client_pool_checkout(Config, Meta, QueueTimeMs) ->
    case enabled(Config) of
        false ->
            ok;
        true ->
            record_histogram(
                Config, ?METRIC_POOL_QUEUE_TIME, QueueTimeMs * 1000, pool_attributes(Meta)
            ),
            emit_event(Config, [nhttpc, pool, checkout], #{queue_time => QueueTimeMs}, Meta)
    end.

-doc """
Emit event when every connection in a pool refused a credit reservation
while at least one had a free stream slot. The pool rate-limits it to
once a second on the rise.
""".
-spec client_pool_credit_starved(otel_config(), client_pool_meta()) -> ok.
client_pool_credit_starved(Config, Meta) ->
    emit_event(Config, [nhttpc, pool, credit_starved], #{}, Meta).

-doc "Emit event when pool is exhausted (max_pending_requests reached).".
-spec client_pool_exhausted(otel_config(), client_pool_meta()) -> ok.
client_pool_exhausted(Config, Meta) ->
    case enabled(Config) of
        false ->
            ok;
        true ->
            record_counter(Config, ?METRIC_POOL_EXHAUSTED, 1, pool_attributes(Meta)),
            emit_event(Config, [nhttpc, pool, exhausted], #{}, Meta)
    end.

-doc """
Emit the waiter count and the connection count of a pool. The manager
samples it once a second at most while it holds a waiter, and once more
when the last waiter leaves.
""".
-spec client_pool_occupancy(otel_config(), client_pool_meta(), client_pool_level()) -> ok.
client_pool_occupancy(Config, Meta, Level) ->
    emit_event(Config, [nhttpc, pool, occupancy], Level, Meta).

-doc "Emit event when pool checkout times out.".
-spec client_pool_timeout(otel_config(), client_pool_meta()) -> ok.
client_pool_timeout(Config, Meta) ->
    case enabled(Config) of
        false ->
            ok;
        true ->
            record_counter(Config, ?METRIC_POOL_TIMEOUT, 1, pool_attributes(Meta)),
            emit_event(Config, [nhttpc, pool, timeout], #{}, Meta)
    end.

%%%-----------------------------------------------------------------------------
%% ACTIVE REQUESTS COUNTER
%%%-----------------------------------------------------------------------------
-doc """
Get the current count of in-flight client requests. Only requests made
with `metrics` enabled are counted; everything else never touches the
counter.
Returns 0 if the counter hasn't been initialized.
""".
-spec active_requests() -> non_neg_integer().
active_requests() ->
    case persistent_term:get(?ACTIVE_REQUESTS_KEY, undefined) of
        undefined ->
            0;
        AtomicsRef ->
            max(0, atomics:get(AtomicsRef, 1))
    end.

-spec record_body_size(otel_config(), atom(), atom(), map(), map()) -> ok.
record_body_size(Config, Metric, Key, Measurements, Attributes) ->
    case maps:get(Key, Measurements, undefined) of
        undefined -> ok;
        Size -> record_histogram(Config, Metric, Size, Attributes)
    end.

-spec record_request_metrics(otel_config(), integer(), client_request_result(), map()) -> ok.
record_request_metrics(Config, Duration, Meta, Measurements) ->
    case metrics_enabled(Config) of
        false ->
            ok;
        true ->
            Attributes = request_attributes(Meta),
            record_histogram(Config, ?METRIC_REQUEST_DURATION, Duration, Attributes),
            record_body_size(
                Config, ?METRIC_REQUEST_BODY_SIZE, req_body_length, Measurements, Attributes
            ),
            record_body_size(
                Config, ?METRIC_RESPONSE_BODY_SIZE, resp_body_length, Measurements, Attributes
            )
    end.

-spec request_end(otel_config(), [atom()], integer(), client_request_result(), map()) -> ok.
request_end(Config, Event, StartTime, Meta, ExtraMeasurements) ->
    Duration = duration_micros(StartTime),
    Measurements = ExtraMeasurements#{duration => Duration},
    record_request_metrics(Config, Duration, Meta, Measurements),
    decr_active_requests(Config, Meta),
    emit_event(Config, Event, Measurements, Meta).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - ATTRIBUTES
%%%-----------------------------------------------------------------------------
-spec active_request_attributes(client_request_result()) -> map().
active_request_attributes(Meta) ->
    #{
        'http.request.method' => maps:get(method, Meta, <<"undefined">>),
        'url.scheme' => scheme_to_binary(maps:get(scheme, Meta, undefined)),
        'server.address' => maps:get(host, Meta, <<"undefined">>),
        'server.port' => maps:get(port, Meta, 0)
    }.

-spec connect_attributes(map()) -> map().
connect_attributes(Meta) ->
    BaseAttributes = #{
        'server.address' => maps:get(host, Meta, <<"undefined">>),
        'server.port' => maps:get(port, Meta, 0)
    },
    WithProtocol =
        case maps:get(protocol, Meta, undefined) of
            undefined ->
                BaseAttributes;
            Protocol ->
                BaseAttributes#{'network.protocol.version' => version_to_binary(Protocol)}
        end,
    case maps:get(reason, Meta, undefined) of
        undefined -> WithProtocol;
        Reason -> WithProtocol#{'error.type' => error_type(Reason)}
    end.

-spec error_type(term()) -> binary().
error_type(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
error_type({Reason, _Details}) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
error_type(_Reason) ->
    <<"_OTHER">>.

-spec pool_attributes(client_pool_meta()) -> map().
pool_attributes(#{pool := Pool}) ->
    #{'nhttpc.pool' => pool_to_binary(Pool)};
pool_attributes(_Meta) ->
    #{}.

-spec pool_to_binary(term()) -> binary().
pool_to_binary(Pool) when is_atom(Pool) -> atom_to_binary(Pool, utf8);
pool_to_binary(Pool) when is_binary(Pool) -> Pool;
pool_to_binary(Pool) -> iolist_to_binary(io_lib:format("~p", [Pool])).

-spec request_attributes(client_request_result()) -> map().
request_attributes(Meta) ->
    BaseAttributes = #{
        'http.request.method' => maps:get(method, Meta, <<"undefined">>),
        'url.scheme' => scheme_to_binary(maps:get(scheme, Meta, undefined)),
        'url.path' => maps:get(path, Meta, <<"/">>),
        'server.address' => maps:get(host, Meta, <<"undefined">>),
        'server.port' => maps:get(port, Meta, 0)
    },
    StatusAttributes =
        case maps:get(status, Meta, undefined) of
            undefined -> BaseAttributes;
            Status -> BaseAttributes#{'http.response.status_code' => Status}
        end,
    case maps:get(reason, Meta, undefined) of
        undefined -> StatusAttributes;
        Reason -> StatusAttributes#{'error.type' => error_type(Reason)}
    end.

-spec scheme_to_binary(nhttp_lib:scheme() | undefined) -> binary().
scheme_to_binary(http) -> <<"http">>;
scheme_to_binary(https) -> <<"https">>;
scheme_to_binary(ws) -> <<"ws">>;
scheme_to_binary(wss) -> <<"wss">>;
scheme_to_binary(Scheme) when is_binary(Scheme) -> Scheme;
scheme_to_binary(_Scheme) -> <<"undefined">>.

-spec version_to_binary(nhttp_lib:version()) -> binary().
version_to_binary(http1_0) -> <<"1.0">>;
version_to_binary(http1_1) -> <<"1.1">>;
version_to_binary(http2) -> <<"2">>;
version_to_binary(http3) -> <<"3">>;
version_to_binary(_Version) -> <<"undefined">>.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - ACTIVE REQUESTS COUNTER
%%%-----------------------------------------------------------------------------
-spec decr_active_requests(otel_config(), client_request_result()) -> ok.
decr_active_requests(Config, Meta) ->
    case metrics_enabled(Config) of
        false ->
            ok;
        true ->
            case persistent_term:get(?ACTIVE_REQUESTS_KEY, undefined) of
                undefined -> ok;
                AtomicsRef -> atomics:sub(AtomicsRef, 1, 1)
            end,
            record_updown_counter(?METRIC_ACTIVE_REQUESTS, -1, active_request_attributes(Meta))
    end.

-spec ensure_active_requests_counter() -> atomics:atomics_ref().
ensure_active_requests_counter() ->
    case persistent_term:get(?ACTIVE_REQUESTS_KEY, undefined) of
        undefined ->
            AtomicsRef = atomics:new(1, [{signed, true}]),
            persistent_term:put(?ACTIVE_REQUESTS_KEY, AtomicsRef),
            persistent_term:get(?ACTIVE_REQUESTS_KEY);
        AtomicsRef ->
            AtomicsRef
    end.

-spec incr_active_requests(otel_config(), client_request_meta()) -> ok.
incr_active_requests(Config, Meta) ->
    case metrics_enabled(Config) of
        false ->
            ok;
        true ->
            AtomicsRef = ensure_active_requests_counter(),
            atomics:add(AtomicsRef, 1, 1),
            record_updown_counter(?METRIC_ACTIVE_REQUESTS, 1, active_request_attributes(Meta))
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - RECORDING
%%%-----------------------------------------------------------------------------
-spec record_counter(otel_config(), atom(), number(), map()) -> ok.
record_counter(Config, Name, Value, Attributes) ->
    case metrics_enabled(Config) of
        false ->
            ok;
        true ->
            case meter() of
                undefined ->
                    ok;
                Meter ->
                    _ = otel_counter:add(
                        otel_ctx:get_current(), Meter, Name, Value, Attributes
                    ),
                    ok
            end
    end.

-spec record_histogram(otel_config(), atom(), number(), map()) -> ok.
record_histogram(Config, Name, Value, Attributes) ->
    case metrics_enabled(Config) of
        false ->
            ok;
        true ->
            case meter() of
                undefined ->
                    ok;
                Meter ->
                    _ = otel_histogram:record(
                        otel_ctx:get_current(), Meter, Name, Value, Attributes
                    ),
                    ok
            end
    end.

-spec record_updown_counter(atom(), number(), map()) -> ok.
record_updown_counter(Name, Value, Attributes) ->
    case meter() of
        undefined ->
            ok;
        Meter ->
            _ = otel_updown_counter:add(
                otel_ctx:get_current(), Meter, Name, Value, Attributes
            ),
            ok
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - INSTRUMENTS
%%%-----------------------------------------------------------------------------
-spec create_instrument(histogram | counter | updown_counter, otel_meter:t(), atom(), map()) ->
    ok.
create_instrument(histogram, Meter, Name, Opts) ->
    _ = otel_meter:create_histogram(Meter, Name, Opts),
    ok;
create_instrument(counter, Meter, Name, Opts) ->
    _ = otel_meter:create_counter(Meter, Name, Opts),
    ok;
create_instrument(updown_counter, Meter, Name, Opts) ->
    _ = otel_meter:create_updown_counter(Meter, Name, Opts),
    ok.

-spec create_instruments(otel_meter:t()) -> ok.
create_instruments(Meter) ->
    _ = ensure_active_requests_counter(),
    lists:foreach(
        fun({Kind, Name, Description, Unit}) ->
            create_instrument(Kind, Meter, Name, #{
                description => Description, unit => Unit
            })
        end,
        [
            {histogram, ?METRIC_REQUEST_DURATION, <<"Duration of HTTP client requests.">>, 'us'},
            {histogram, ?METRIC_CONNECT_DURATION,
                <<"Duration of HTTP client connection attempts.">>, 'us'},
            {histogram, ?METRIC_POOL_QUEUE_TIME,
                <<"Time a request waited for a pooled connection.">>, 'us'},
            {histogram, ?METRIC_REQUEST_BODY_SIZE,
                <<"The size of the request payload body in bytes.">>, 'By'},
            {histogram, ?METRIC_RESPONSE_BODY_SIZE,
                <<"The size of the response payload body in bytes.">>, 'By'},
            {counter, ?METRIC_POOL_EXHAUSTED, <<"Pool exhausted events.">>, '1'},
            {counter, ?METRIC_POOL_TIMEOUT, <<"Pool checkout timeouts.">>, '1'},
            {updown_counter, ?METRIC_ACTIVE_REQUESTS, <<"Number of active HTTP client requests.">>,
                '1'}
        ]
    ).

-spec duration_micros(integer()) -> integer().
duration_micros(StartTime) ->
    erlang:convert_time_unit(erlang:monotonic_time() - StartTime, native, microsecond).

-spec emit_event(otel_config(), [atom()], map(), map()) -> ok.
emit_event(Config, Event, Measurements, Metadata) ->
    case events_enabled(Config) of
        false -> ok;
        true -> telemetry:execute(Event, Measurements, Metadata)
    end.

-spec ensure_instruments(otel_meter:t()) -> otel_meter:t().
ensure_instruments(Meter) ->
    case persistent_term:get(?INSTRUMENTS_KEY, false) of
        true ->
            Meter;
        false ->
            ok = create_instruments(Meter),
            persistent_term:put(?INSTRUMENTS_KEY, true),
            Meter
    end.

-spec meter() -> otel_meter:t() | undefined.
meter() ->
    case opentelemetry_experimental:get_meter(?METER_NAME) of
        {otel_meter_noop, _Ignored} ->
            undefined;
        Meter ->
            ensure_instruments(Meter)
    end.
