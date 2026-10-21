-module(nhttpc_pool).

-moduledoc """
Pool manager for connection checkout/checkin.

Manages a pool of persistent connections to a single host:port target.
Handles connection lifecycle, checkout/checkin, and request routing.

## Connection Strategy

- Maintains up to size idle connections
- Allows up to max_overflow additional connections when busy
- HTTP/2 and HTTP/3 connections multiplex requests; HTTP/1.1 does not

## Draining Connections and the Live Bound

A connection leaves service before it exits in two cases: the peer sent
GOAWAY, or the connection opened `max_streams_per_connection` streams
and retires before its stream identifiers run out. Both reach the
manager as the `connection_draining` cast. The manager marks the
connection, drops it from the multiplexed tuple so no checkout reaches
it, and opens its replacement at once. The draining connection serves
every request already in flight and exits `normal` when the last one
completes. A `normal` exit moves no backoff counter, so rotation never
slows reconnection.

`size + max_overflow` bounds the connections that can take a request:
the ready ones that are not draining plus the ones in their handshake.
The bound on live connection processes is that number plus the
connections draining at the moment, and the request timeout bounds how
long a connection can drain. A pool under `drain/2` opens no
replacement.

## Concurrent Stream Cap

A multiplexed connection accepts a bounded number of live streams. The
bound is the `SETTINGS_MAX_CONCURRENT_STREAMS` value that the peer sends.
The connection process reports it once from the handshake, and again for
every later SETTINGS frame that carries it. A peer that names no value
leaves the client ceiling of 100 in place. RFC 9113 section 6.5.2 reads
the absence of the setting as "no limit", so that ceiling is a client
choice and not a value from the peer. An HTTP/1.1 connection has a bound
of one, which is what makes its checkout exclusive.

The connection process is the only writer of the bound, and
`nhttpc_stream_slots:reserve/1` is the only reader. The write belongs to
the connection because a cast to the manager puts the manager mailbox
between a peer that lowers its bound and the lock-free checkout path that
reads it. The manager creates the array with a bound of zero and hands it
to the connection, so no checkout can succeed before the connection
writes. The `update_max_streams` cast carries no write: a bound that goes
up can release a queued waiter, and only the manager holds that queue.

A peer can lower the bound below the live stream count. Open streams
survive, new reservations stop until the live count falls under the new
bound, and a request that the pool admitted against the old bound is
refused by `nhttp_h2:open_stream/1` before any frame leaves the client.
`request/4` re-dispatches such a request onto another connection.
The same re-dispatch serves a request refused for lack of send credit.
A re-dispatch never returns to the connection that refused: both
checkout paths skip that pid for the next attempt, so under `pack` a
refusal moves to another connection instead of hitting the same starved
one until the attempt budget runs out.

## Reconnection Brake

The pool owns reconnection. The connection child is `temporary`, so no
supervisor intensity applies, and the pool's `consecutive_failures`
counter is the only brake on a connection that keeps dying.
`reconnect_backoff_base` and `reconnect_backoff_max` bound the delay it
computes.

The counter moves on three events:

- A connection exits for a reason `exit_kind/1` calls a failure: a
  connect error, a crash, a socket that closed under an open request.
  The counter goes up by one.
- A connection exits cleanly: `shutdown`, `normal`, the idle timeout, a
  graceful close of an idle socket, a close after a cancel or a dead
  caller. The counter does not move.
- A connection delivers its first response. The counter resets to zero.

A completed handshake resets nothing. A connection that connects and dies
before it serves therefore walks the delay towards `reconnect_backoff_max`
instead of reconnecting at the base forever.

The counter is pool-wide. Every connection in a pool shares one origin
and one TLS configuration, so a connection that dies unserved says
something about the origin and not about that connection. One bad
connection brakes the reconnection of every connection in the pool, and
the first served response on any of them releases the brake for all.

## Connection Selection

A multiplexed pool picks its connection with the `stream_selection` policy.

`pack`, the default, walks the connections oldest first and takes the
first one that can take the request. RFC 9113 section 9.1 and RFC 9114
section 3.3 both ask a client to use one connection per origin and to
open another only when it cannot do otherwise. Packing keeps the HPACK
dynamic table and the congestion window on one connection instead of
splitting them across `size` cold ones. The pool starts one connection,
and `handle_checkout_internal/5` opens the next only when no connection
can take the request, bounded by `size + max_overflow`.

`spread` distributes requests: a round-robin cursor on the fast path,
and the least-loaded connection through the manager.

The policy does not reach HTTP/1.1, whose checkout is exclusive. A pool
that negotiates HTTP/1.1 under `pack` fills to `size`, because one
exclusive connection serves one caller at a time.

## Send Credit

A connection can take a request when it has a free stream slot and the
HTTP/2 connection send credit for the request body. Availability is
`stream slot reserved AND credit reserved`, under both policies. Credit
is not a policy: a connection that cannot carry a body is unavailable
whatever the policy says.

A checkout carries the credit claim and claims the credit the way it
claims the stream slot, with one compare-and-swap on the connection's
own array that holds only while nobody moved the value. N concurrent
checkouts cannot all pass on one reading of the value, so a request admitted to a
connection always finds the codec with credit for its body. The
connection process writes the peer's window at ready, adds every
connection-level WINDOW_UPDATE, refunds a request it refused before any
octet reached the socket, and rewrites the slot from the codec window
when it is idle. Encoding a request debits nothing, because the
reservation covered it. See `nhttpc_stream_slots`.

A body above the peer's initial stream window fits no fresh stream
whatever the connection credit. The connection refuses it before a stream
opens, and `request/4` re-dispatches it. That is the one way
`refused_stream` reaches a reserved request.

HTTP/3 carries its own flow control inside QUIC and publishes an
unbounded credit, so the reservation never binds it. HTTP/1.1 never
enters the multiplexed tuple.

Growth follows from either resource. A cold peer that advertises one
concurrent stream makes every connection slot-bound; a peer that credits
slowly makes every connection credit-bound; the pool opens the next
connection in both cases. `[nhttpc, pool, credit_starved]` fires when a
checkout reaches the manager because every connection refused the credit
while at least one had a free stream slot, at most once a second for
each pool.

Sizing a pool for a peer that credits on processing: the connections
must cover `offered_rate * body_size * credit_latency / half_window`,
multiplied by two for headroom, where `half_window` is the credit the
peer returns at a time and `credit_latency` is its response latency.
The `[nhttpc, connection, credit]` event reports both inputs
continuously. A pool whose `size + max_overflow` is below that product
cannot reach the offered rate.

## Architecture

The pool gen_server handles:
- Connection lifecycle (up/down, monitors, reconnection backoff)
- H1 exclusive checkout (idle map, waiter queue)
- Multiplexed gen_server fallback (when fast-path is exhausted)

The pool does NOT handle responses. Callers communicate directly with
connection processes. This halves gen_server message traffic.

Fast-path (no gen_server call):
- Multiplexed selection over the per-pool registry tuple, plus an atomic
  counter under `spread`
- H1 always goes through gen_server (exclusive access required)

The registry tuple is ordered oldest connection first, and the order is
stable across rebuilds, which is what makes `pack` a first fit. Every
element is a `{ConnPid, Slots}` pair, so a checkout reserves a stream slot
with one `atomics:compare_exchange/4` and reads no ETS at all. The connection ETS
table stays for the protocol column, which `multiplexed/2` reads on
checkin, and for the status column that the connection process writes.

Per-pool pid storage lives in `nhttpc_registry`, not in `persistent_term`.
The only persistent_term entry per pool is `{nhttpc_pool, Ref}` ->
`#pool_env{}`, which holds the per-pool registry tid, the ETS table, the
atomics reference, the telemetry configuration, and the selection policy.
Stable for the lifetime of the pool; no pid lives there.

`#pool_env{}` is what makes the fast path self-contained: a caller reads
it once and can reserve a stream slot and emit checkout telemetry without
touching the manager.
""".

-behaviour(gen_server).

%%%-----------------------------------------------------------------------------
%% COMPILE OPTIONS
%%%-----------------------------------------------------------------------------
-compile(
    {inline, [
        remaining_timeout/2
    ]}
).

-export([
    checkin/2,
    checkin_fast/2,
    checkout/2,
    connection_draining/2,
    connection_error/3,
    connection_ready/3,
    connection_ready/4,
    connection_served/2,
    drain/2,
    get_connection/1,
    get_connection/2,
    lookup/1,
    lookup_conn_sup/1,
    lookup_sup/1,
    manager/1,
    request/3,
    request/4,
    resolve/1,
    start_link/3,
    target/1,
    update_max_streams/3,
    wait_ready/2
]).

%% gen_server callbacks
-export([
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,

    terminate/2
]).

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(TIMEOUT_CHECK_INTERVAL, 100).
-define(DRAIN_CHECK_INTERVAL, 50).

-define(MAX_STREAM_REDISPATCH, 2).

-define(ATOMIC_RR, 1).
-define(ATOMIC_NOTIFY, 2).
-define(ATOMIC_CREDIT_STARVED_AT, 3).
-define(POOL_ATOMICS_ARITY, ?ATOMIC_CREDIT_STARVED_AT).
-define(CREDIT_STARVED_INTERVAL_MS, 1000).
-define(OCCUPANCY_REPORT_INTERVAL_MS, 1000).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-record(conn_info, {
    pid :: pid(),
    monitor_ref :: reference(),
    protocol :: nhttp_lib:version() | undefined,
    active_count :: non_neg_integer(),
    slots :: nhttpc_stream_slots:slots(),
    draining :: boolean(),
    created_at :: integer(),
    last_activity :: integer()
}).

-record(pool_env, {
    ref :: term(),
    tab :: nhttpc_registry:tab(),
    conn_ets :: ets:tid(),
    atomics :: atomics:atomics_ref(),
    telemetry :: nhttpc_telemetry:otel_config(),
    selection :: nhttpc:stream_selection(),
    conn_mod :: nhttpc_conn | nhttpc_h3_conn
}).

-record(waiter, {
    from :: gen_server:from(),
    deadline :: integer(),
    queued_at :: integer(),
    exclude :: pid() | undefined,
    body_size :: non_neg_integer()
}).

-record(ready_waiter, {
    from :: gen_server:from(),
    deadline :: integer()
}).

-record(state, {
    ref :: term(),
    tab :: nhttpc_registry:tab(),
    conn_sup :: pid() | undefined,

    size :: pos_integer(),
    max_overflow :: non_neg_integer(),
    checkout_timeout :: timeout(),
    stream_selection :: nhttpc:stream_selection(),
    conn_target :: pos_integer(),

    conn_opts :: nhttpc_conn:conn_opts(),

    connections :: #{pid() => #conn_info{}},
    pending_conns :: #{pid() => {reference(), nhttpc_stream_slots:slots()}},
    idle_h1_conns :: #{pid() => true},
    waiters :: queue:queue(#waiter{}),
    waiter_count :: non_neg_integer(),
    ready_waiters :: [#ready_waiter{}],

    conn_ets :: ets:tid() | undefined,
    conn_atomics :: atomics:atomics_ref() | undefined,

    total_created :: non_neg_integer(),

    reconnect_backoff_base :: pos_integer(),
    reconnect_backoff_max :: pos_integer(),
    consecutive_failures :: non_neg_integer(),
    last_failure_time :: integer() | undefined,

    max_pending_requests :: pos_integer(),
    connection_sweep_interval :: timeout(),
    max_connection_age :: timeout(),

    monitor_to_pid :: #{reference() => pid()},
    ready_count :: non_neg_integer(),

    waiter_check_timer_ref :: reference() | undefined,

    drain :: undefined | {gen_server:from(), integer() | infinity},

    telemetry_config :: nhttpc_telemetry:otel_config(),
    occupancy_reported_at = undefined :: integer() | undefined,
    occupancy_reported_waiters = 0 :: non_neg_integer()
}).

-doc """
The best connection found so far by a ranking fold: the connection, its
info and the rank that put it ahead. The fold starts from `none`.
""".
-type multiplexed_candidate() :: {pid(), #conn_info{}, integer()}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec checkin(pid() | atom(), pid()) -> ok.
checkin(MgrRef, ConnPid) ->
    gen_server:cast(MgrRef, {checkin, ConnPid}).

-doc """
Fast checkin. For HTTP/2 and HTTP/3 the connection process already
decremented the live stream count in ETS, so the manager only has to hear
about the checkin when it holds a queued waiter or runs a drain. Atomics
slot 2 says whether it does; when it does not, this sends no message.
HTTP/1.1 always notifies the manager, which owns the idle list and is the
only thing that makes the connection reusable.
""".
-spec checkin_fast(term(), pid()) -> ok.
checkin_fast(PoolRef, ConnPid) ->
    case pool_env(PoolRef) of
        {ok, Env} -> release(Env, ConnPid);
        error -> release_via_manager(PoolRef, ConnPid)
    end.

-doc """
Check a connection out through the manager, for a request that carries
no body. A caller that sends a body on the connection it gets here holds
no credit reservation for it, and the connection refuses the body when
the window cannot carry it.
""".
-spec checkout(pid() | atom(), timeout()) -> {ok, pid()} | {error, nhttpc_error:any_reason()}.
checkout(MgrRef, Timeout) ->
    checkout_excluding(MgrRef, Timeout, undefined, 0).

-spec connection_draining(pid(), pid()) -> ok.
connection_draining(MgrPid, ConnPid) ->
    gen_server:cast(MgrPid, {connection_draining, ConnPid}).

-spec connection_error(pid(), pid(), term()) -> ok.
connection_error(MgrPid, ConnPid, Reason) ->
    gen_server:cast(MgrPid, {connection_error, ConnPid, Reason}).

-doc """
Report a connection as ready with no cap from the peer. The pool applies
its own per-protocol ceiling.
""".
-spec connection_ready(pid(), pid(), nhttp_lib:version()) -> ok.
connection_ready(MgrPid, ConnPid, Protocol) ->
    connection_ready(MgrPid, ConnPid, Protocol, undefined).

-doc """
Report a connection as ready, with the concurrent stream cap the peer
advertised in its opening SETTINGS frame. `undefined` means the peer named
no cap, and the pool applies its own per-protocol ceiling.
The manager is the only writer of the cap. This cast and
`update_max_streams/3` both leave the connection process for the manager,
so their arrival order is their send order and a late cap cannot be
overwritten by an early default.
""".
-spec connection_ready(pid(), pid(), nhttp_lib:version(), non_neg_integer() | undefined) -> ok.
connection_ready(MgrPid, ConnPid, Protocol, MaxStreams) ->
    gen_server:cast(MgrPid, {connection_ready, ConnPid, Protocol, MaxStreams}).

-doc """
Report that a connection delivered its first response. This is the one
event that resets the reconnect backoff: a completed handshake proves
that the origin accepts connections, and only a served response proves
that it serves them.
""".
-spec connection_served(pid(), pid()) -> ok.
connection_served(MgrPid, ConnPid) ->
    gen_server:cast(MgrPid, {connection_served, ConnPid}).

-doc """
Stop accepting new checkouts and wait for in-flight requests to complete,
or until `Timeout` milliseconds elapse. Reply is always `ok` — callers
treat the deadline as advisory; force-shutdown follows separately.
Queued waiters and pending `wait_ready` callers are released with
`{error, pool_closed}` as soon as drain starts.
""".
-spec drain(pid(), timeout()) -> ok.
drain(MgrPid, Timeout) ->
    try gen_server:call(MgrPid, {drain, Timeout}, safe_call_timeout(Timeout)) of
        ok -> ok
    catch
        exit:noproc -> ok;
        exit:{noproc, _} -> ok;
        exit:{timeout, _} -> ok;
        exit:{shutdown, _} -> ok;
        exit:{normal, _} -> ok
    end.

-spec finalize_request(#pool_env{} | pid(), pid(), reference(), Result) -> Result when
    Result :: {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
finalize_request(Owner, ConnPid, _StreamRef, {ok, _} = Ok) ->
    release(Owner, ConnPid),
    Ok;
finalize_request(Owner, ConnPid, StreamRef, {error, _} = Err) ->
    ConnMod = conn_module(Owner, ConnPid),
    _ = ConnMod:cancel(ConnPid, StreamRef),
    flush_stream_messages(ConnPid, StreamRef),
    release(Owner, ConnPid),
    Err.

-spec flush_stream_messages(pid(), reference()) -> ok.
flush_stream_messages(ConnPid, StreamRef) ->
    receive
        {nhttpc_response, ConnPid, StreamRef, _} ->
            flush_stream_messages(ConnPid, StreamRef);
        {nhttpc_error, ConnPid, StreamRef, _} ->
            flush_stream_messages(ConnPid, StreamRef)
    after 0 ->
        ok
    end.

-doc """
Get a connection from the pool without a `gen_server` call where possible.
For HTTP/2 and HTTP/3: round-robin over the per-pool connection tuple, one
atomic ETS operation per candidate to reserve a stream slot.
For HTTP/1.1, and for a pool with no multiplexed connection ready yet,
falls back to the manager, which owns exclusive checkout and the waiter
queue.
Callers that release the connection afterwards must use `checkin_fast/2`,
which reads the same pool state and picks the matching release path.
""".
-spec get_connection(term()) -> {ok, pid()} | {error, nhttpc_error:any_reason()}.
get_connection(PoolRef) ->
    get_connection(PoolRef, 5000).

-spec get_connection(term(), timeout()) -> {ok, pid()} | {error, nhttpc_error:any_reason()}.
get_connection(PoolRef, Timeout) ->
    case acquire(PoolRef, Timeout, undefined, 0) of
        {ok, ConnPid, _Owner} -> {ok, ConnPid};
        {error, _} = Error -> Error
    end.

-doc """
Manager pid for the given pool reference. Accepts the user-supplied pool
ref, the manager pid, or the pool supervisor pid (all are normalised
through `resolve/1`). Throws `error:badarg` when the pool is not
registered. Used on the hot path where the absence of the pool is
unexpected.
""".
-spec lookup(pid() | term()) -> pid().
lookup(PidOrRef) ->
    case resolve(PidOrRef) of
        {ok, MgrPid, _SupPid} -> MgrPid;
        {error, pool_not_found} -> error(badarg)
    end.

-doc "Connection supervisor pid for the given reference, or `undefined`.".
-spec lookup_conn_sup(term()) -> pid() | undefined.
lookup_conn_sup(Ref) ->
    case pool_env(Ref) of
        {ok, #pool_env{tab = Tab}} -> nhttpc_registry:lookup_conn_sup(Tab);
        error -> undefined
    end.

-doc "Pool supervisor pid for the given reference, or `undefined`.".
-spec lookup_sup(term()) -> pid() | undefined.
lookup_sup(Ref) ->
    case pool_env(Ref) of
        {ok, #pool_env{tab = Tab}} -> nhttpc_registry:lookup_sup(Tab);
        error -> undefined
    end.

-doc "Pool manager pid for the given reference, or `undefined`.".
-spec manager(term()) -> pid() | undefined.
manager(Ref) ->
    case pool_env(Ref) of
        {ok, #pool_env{tab = Tab}} -> nhttpc_registry:lookup_mgr(Tab);
        error -> undefined
    end.

-doc false.
-spec request(pid() | term(), nhttp_lib:request(), timeout()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
request(PoolRef, Req, Timeout) ->
    request(PoolRef, Req, Timeout, #{}).

-doc false.
-spec request(pid() | term(), nhttp_lib:request(), timeout(), nhttpc_conn:send_opts()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
request(PoolRef, Req, Timeout, SendOpts) ->
    do_request(PoolRef, Req, Timeout, SendOpts, erlang:monotonic_time(millisecond), 0, undefined).

-doc """
Resolve any of `pool_ref()` / manager pid / supervisor pid to the canonical
`{ok, MgrPid, SupPid}` tuple. Returns `{error, pool_not_found}` if the input
does not identify a live pool.
Single source of truth for atom-to-pid resolution. Callers that previously
inspected atom suffixes or persistent_term keys directly must route through
this helper.
""".
-spec resolve(pid() | term()) -> {ok, pid(), pid()} | {error, pool_not_found}.
resolve(Pid) when is_pid(Pid) ->
    case is_process_alive(Pid) of
        false -> {error, pool_not_found};
        true -> resolve_pid(Pid)
    end;
resolve(Ref) ->
    case pool_env(Ref) of
        {ok, #pool_env{tab = Tab}} ->
            case {nhttpc_registry:lookup_mgr(Tab), nhttpc_registry:lookup_sup(Tab)} of
                {MgrPid, SupPid} when is_pid(MgrPid), is_pid(SupPid) ->
                    {ok, MgrPid, SupPid};
                _ ->
                    {error, pool_not_found}
            end;
        error ->
            {error, pool_not_found}
    end.

-spec start_link(term(), nhttpc_registry:tab(), nhttpc:pool_opts()) ->
    {ok, pid()} | {error, nhttpc_error:any_reason()}.
start_link(Ref, Tab, Opts) ->
    gen_server:start_link(?MODULE, {Ref, Tab, Opts}, []).

-doc """
Connection target (`scheme`, `host`, `port`) for the given pool
reference, or `undefined` if the pool is unknown. Requests dispatched
through a pool carry a path-only URL, so callers that need the real
authority (telemetry, logging) read it from here.
""".
-spec target(term()) -> nhttpc_registry:target() | undefined.
target(Ref) ->
    case pool_env(Ref) of
        {ok, #pool_env{tab = Tab}} -> nhttpc_registry:lookup_target(Tab);
        error -> undefined
    end.

-spec target_host(binary() | string() | inet:ip_address()) -> binary().
target_host(Host) when is_binary(Host) -> Host;
target_host(Host) when is_list(Host) -> list_to_binary(Host);
target_host(Host) when is_tuple(Host) -> list_to_binary(inet:ntoa(Host)).

-spec target_scheme(tcp | ssl | quic) -> nhttp_lib:scheme().
target_scheme(tcp) -> http;
target_scheme(_SslOrQuic) -> https.

-doc """
Report a mid-connection change of the peer's concurrent stream cap, from a
SETTINGS frame that arrived after the handshake.
""".
-spec update_max_streams(pid(), pid(), non_neg_integer()) -> ok.
update_max_streams(MgrPid, ConnPid, MaxStreams) ->
    gen_server:cast(MgrPid, {update_max_streams, ConnPid, MaxStreams}).

-doc """
Block until the pool reports at least one connection ready, or `Timeout`
milliseconds elapse. Accepts the pool ref, manager pid, or supervisor pid
(normalised through `resolve/1`).
""".
-spec wait_ready(pid() | term(), timeout()) ->
    ok | {error, timeout} | {error, pool_not_found} | {error, pool_closed}.
wait_ready(PidOrRef, Timeout) ->
    case resolve(PidOrRef) of
        {ok, MgrPid, _SupPid} ->
            gen_server:call(MgrPid, {wait_ready, Timeout}, safe_call_timeout(Timeout));
        {error, pool_not_found} = E ->
            E
    end.

%%%-----------------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%%-----------------------------------------------------------------------------
-spec handle_call(term(), gen_server:from(), #state{}) ->
    {reply, term(), #state{}} | {noreply, #state{}}.
handle_call({checkout, _Timeout, _Exclude, _BodySize}, _From, #state{drain = D} = State) when
    D =/= undefined
->
    {reply, {error, pool_closed}, State};
handle_call({checkout, Timeout, Exclude, BodySize}, From, State) ->
    handle_checkout(From, Timeout, Exclude, BodySize, State);
handle_call({wait_ready, _Timeout}, _From, #state{drain = D} = State) when D =/= undefined ->
    {reply, {error, pool_closed}, State};
handle_call({wait_ready, Timeout}, From, State) ->
    handle_wait_ready(From, Timeout, State);
handle_call({drain, Timeout}, From, State) ->
    handle_drain(From, Timeout, State);
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast({checkin, ConnPid}, State) ->
    {noreply, do_checkin(ConnPid, State)};
handle_cast({connection_ready, ConnPid, Protocol, _MaxStreams}, State) ->
    {noreply, handle_connection_ready(ConnPid, Protocol, State)};
handle_cast({connection_error, ConnPid, Reason}, State) ->
    ok = log_connect_failure(ConnPid, Reason, State),
    {noreply, handle_connection_down(ConnPid, Reason, State)};
handle_cast({connection_served, ConnPid}, State) ->
    {noreply, handle_connection_served(ConnPid, State)};
handle_cast({update_max_streams, ConnPid, _MaxStreams}, State) ->
    {noreply, handle_update_max_streams(ConnPid, State)};
handle_cast({connection_draining, ConnPid}, State) ->
    {noreply, handle_connection_draining(ConnPid, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(start_initial_connections, #state{conn_sup = undefined, tab = Tab} = State) ->
    case nhttpc_registry:lookup_conn_sup(Tab) of
        undefined ->
            erlang:send_after(100, self(), start_initial_connections),
            {noreply, State};
        ConnSup ->
            {noreply, top_up_connections(State#state{conn_sup = ConnSup})}
    end;
handle_info(start_initial_connections, State) ->
    {noreply, State};
handle_info(check_waiter_timeouts, State) ->
    {noreply, handle_check_waiter_timeouts(State)};
handle_info(drain_check, State) ->
    {noreply, handle_drain_check(State)};
handle_info({'DOWN', MonRef, process, _Pid, Reason}, State) ->
    case maps:get(MonRef, State#state.monitor_to_pid, undefined) of
        undefined ->
            {noreply, State};
        ConnPid ->
            {noreply, handle_connection_down(ConnPid, Reason, State)}
    end;
handle_info(delayed_reconnect, #state{conn_sup = undefined} = State) ->
    erlang:send_after(100, self(), delayed_reconnect),
    {noreply, State};
handle_info(delayed_reconnect, State) ->
    case serving_count(State) < State#state.conn_target of
        true ->
            case start_tracked_connection(State) of
                {ok, NewState} ->
                    {noreply, NewState};
                {error, _} ->
                    {noreply, schedule_reconnection(State)}
            end;
        false ->
            {noreply, State}
    end;
handle_info(connection_sweep, State) ->
    NewState = sweep_connections(State),
    schedule_connection_sweep(
        State#state.connection_sweep_interval,
        State#state.max_connection_age
    ),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

-spec init({term(), nhttpc_registry:tab(), nhttpc:pool_opts()}) -> {ok, #state{}}.
init({Ref, Tab, Opts}) ->
    process_flag(trap_exit, true),

    ok = nhttpc_registry:register_mgr(Tab, self()),

    Size = maps:get(size, Opts, 4),
    MaxOverflow = maps:get(max_overflow, Opts, 4),
    CheckoutTimeout = nhttpc_opts:timeout(checkout, Opts),
    Selection = nhttpc_opts:stream_selection(Opts),
    ConnTarget = initial_conn_target(Selection, Size),

    TelemetryConfig = nhttpc_telemetry:config(Opts),

    WireTransport = nhttpc_opts:pool_transport(Opts),
    ok = nhttpc_registry:register_target(Tab, #{
        scheme => target_scheme(WireTransport),
        host => target_host(maps:get(host, Opts)),
        port => maps:get(port, Opts)
    }),
    ConnTransport =
        case WireTransport of
            ssl -> ssl;
            _ -> tcp
        end,
    Versions = nhttpc_opts:versions(Opts),
    ConnOpts0 = maps:merge(
        #{
            pool_mgr => self(),
            host => maps:get(host, Opts),
            port => maps:get(port, Opts),
            transport => ConnTransport,
            protocols => Versions,
            connect_timeout => nhttpc_opts:timeout(connect, Opts),
            request_timeout => nhttpc_opts:timeout(request, Opts),
            idle_timeout => maps:get(idle_timeout, Opts, nhttpc_opts:timeout(idle, Opts)),
            max_streams_per_connection => nhttpc_opts:max_streams_per_connection(Opts)
        },
        nhttpc_opts:send_queue_bounds(Opts)
    ),
    ConnOpts1 =
        case nhttpc_opts:tls(Opts) of
            undefined -> ConnOpts0;
            TlsMap -> maps:merge(ConnOpts0, flatten_tls(TlsMap))
        end,
    ConnOptsWithExtras =
        case maps:find(otel, Opts) of
            {ok, OtelCfg} -> ConnOpts1#{otel => OtelCfg};
            error -> ConnOpts1
        end,

    ReconnectBackoffBase = maps:get(reconnect_backoff_base, Opts, 100),
    ReconnectBackoffMax = maps:get(reconnect_backoff_max, Opts, 30000),

    MaxPendingRequests = maps:get(max_pending_requests, Opts, 1000),
    ConnectionSweepInterval = maps:get(connection_sweep_interval, Opts, 60000),
    MaxConnectionAge = maps:get(max_connection_age, Opts, infinity),

    ConnEts = ets:new(conn_table, [
        public,
        set,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    ConnAtomics = atomics:new(?POOL_ATOMICS_ARITY, [{signed, false}]),
    persistent_term:put({nhttpc_pool, Ref}, #pool_env{
        ref = Ref,
        tab = Tab,
        conn_ets = ConnEts,
        atomics = ConnAtomics,
        telemetry = TelemetryConfig,
        selection = Selection,
        conn_mod = nhttpc_pool_conn_sup:conn_module(Opts)
    }),

    ok = nhttpc_registry:put_multiplexed_conns(Tab, {}),

    ConnOptsWithEts = ConnOptsWithExtras#{conn_ets => ConnEts},

    State = #state{
        ref = Ref,
        tab = Tab,
        conn_sup = undefined,
        size = Size,
        max_overflow = MaxOverflow,
        checkout_timeout = CheckoutTimeout,
        stream_selection = Selection,
        conn_target = ConnTarget,
        conn_opts = ConnOptsWithEts,
        connections = #{},
        pending_conns = #{},
        idle_h1_conns = #{},
        waiters = queue:new(),
        waiter_count = 0,
        ready_waiters = [],
        conn_ets = ConnEts,
        conn_atomics = ConnAtomics,
        total_created = 0,
        reconnect_backoff_base = ReconnectBackoffBase,
        reconnect_backoff_max = ReconnectBackoffMax,
        consecutive_failures = 0,
        last_failure_time = undefined,
        max_pending_requests = MaxPendingRequests,
        connection_sweep_interval = ConnectionSweepInterval,
        max_connection_age = MaxConnectionAge,
        monitor_to_pid = #{},
        ready_count = 0,
        waiter_check_timer_ref = undefined,
        drain = undefined,
        telemetry_config = TelemetryConfig
    },

    self() ! start_initial_connections,
    schedule_connection_sweep(ConnectionSweepInterval, MaxConnectionAge),

    {ok, State}.

-spec is_supervisor_shutdown(term()) -> boolean().
is_supervisor_shutdown(shutdown) -> true;
is_supervisor_shutdown({shutdown, _}) -> true;
is_supervisor_shutdown(normal) -> true;
is_supervisor_shutdown(_) -> false.

-spec terminate(term(), #state{}) -> ok.
terminate(Reason, #state{ref = Ref, connections = Conns, conn_ets = ConnEts}) ->
    case is_supervisor_shutdown(Reason) of
        true -> _ = persistent_term:erase({nhttpc_pool, Ref});
        false -> ok
    end,
    case ConnEts of
        undefined ->
            ok;
        _ ->
            try
                ets:delete(ConnEts)
            catch
                _:_ -> ok
            end
    end,
    maps:foreach(
        fun(_Pid, Info) ->
            try
                stop_connection(Info)
            catch
                _:_ -> ok
            end
        end,
        Conns
    ),
    ok.

%%%-----------------------------------------------------------------------------
%% INTERNAL: CHECKOUT/CHECKIN
%%%-----------------------------------------------------------------------------
-spec do_checkin(pid(), #state{}) -> #state{}.
do_checkin(ConnPid, #state{connections = Conns} = State) ->
    case maps:get(ConnPid, Conns, undefined) of
        undefined ->
            State;
        #conn_info{protocol = http1_1, draining = Draining} = Info ->
            emit_pool_checkin(State#state.telemetry_config, State#state.ref),
            Now = erlang:monotonic_time(millisecond),
            NewInfo = Info#conn_info{active_count = 0, last_activity = Now},
            NewIdleH1 =
                case Draining of
                    false -> (State#state.idle_h1_conns)#{ConnPid => true};
                    true -> State#state.idle_h1_conns
                end,
            NewState = State#state{
                connections = Conns#{ConnPid => NewInfo},
                idle_h1_conns = NewIdleH1
            },
            maybe_complete_drain(maybe_serve_waiters(NewState));
        #conn_info{} = Info ->
            emit_pool_checkin(State#state.telemetry_config, State#state.ref),
            Now = erlang:monotonic_time(millisecond),
            NewInfo = Info#conn_info{last_activity = Now},
            NewState = State#state{
                connections = Conns#{ConnPid => NewInfo}
            },
            maybe_complete_drain(maybe_serve_waiters(NewState))
    end.

-doc """
One pooled unary request, with a bounded re-dispatch for a stream that
never opened.
The pool grants a stream slot from a cap that a peer can lower at any
moment, so `nhttp_h2:open_stream/1` can refuse a request the pool
already admitted. No HEADERS frame leaves the client in that case, so the
request is safe to send again whatever its method, and it must not depend
on the caller opting in to retries: an idempotent-only retry policy is
about a request the peer saw.
The bound guards against a pathological loop only. The backpressure comes
from `acquire/2`: with every connection at a lowered cap the caller
queues as a waiter for the time it has left, which is what it would have
done had the cap been correct at checkout time.
A connection can also refuse a request for lack of send credit, with the
same retryable reason and the same guarantee that the peer saw nothing.
`Exclude` is the connection that refused the previous attempt. The next
acquire skips it, because a credit-starved connection has free stream
slots by definition and `pack` would otherwise select it again.
""".
-spec do_request(
    pid() | term(),
    nhttp_lib:request(),
    timeout(),
    nhttpc_conn:send_opts(),
    integer(),
    non_neg_integer(),
    pid() | undefined
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_request(PoolRef, Req, Timeout, SendOpts, Start, Attempt, Exclude) ->
    BodySize = nhttpc_h2:credit_claim(Req),
    case acquire(PoolRef, remaining_timeout(Timeout, Start), Exclude, BodySize) of
        {ok, ConnPid, Owner} ->
            ConnMod = conn_module(Owner, ConnPid),
            {ok, StreamRef} = ConnMod:send_async(ConnPid, Req, self(), SendOpts),
            Remaining = remaining_timeout(Timeout, Start),
            Result = nhttpc:await_response(ConnPid, StreamRef, Remaining),
            case finalize_request(Owner, ConnPid, StreamRef, Result) of
                {error, {stream_error, {protocol, refused_stream}}} = Refused ->
                    case redispatch_allowed(Timeout, Start, Attempt) of
                        true ->
                            do_request(
                                PoolRef, Req, Timeout, SendOpts, Start, Attempt + 1, ConnPid
                            );
                        false ->
                            Refused
                    end;
                Final ->
                    Final
            end;
        {error, _} = Error ->
            Error
    end.

-spec grow_or_queue(gen_server:from(), timeout(), pid() | undefined, non_neg_integer(), #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}}.
grow_or_queue(From, Timeout, Exclude, BodySize, State) ->
    MaxConns = State#state.size + State#state.max_overflow,
    CanGrow = serving_count(State) < MaxConns andalso State#state.conn_sup =/= undefined,
    case {CanGrow, has_alternative(Exclude, State)} of
        {true, _} ->
            case start_tracked_connection(State) of
                {ok, NewState} ->
                    queue_waiter(From, Timeout, Exclude, BodySize, NewState);
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        {false, true} ->
            queue_waiter(From, Timeout, Exclude, BodySize, State);
        {false, false} ->
            {reply, nhttpc_error:wrap(nhttp_error:stream_refused()), State}
    end.

-spec handle_checkout(gen_server:from(), timeout(), pid() | undefined, non_neg_integer(), #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}}.
handle_checkout(From, Timeout, Exclude, BodySize, State) ->
    case State#state.waiter_count >= State#state.max_pending_requests of
        true ->
            emit_pool_exhausted(State#state.telemetry_config, State#state.ref),
            {reply, nhttp_error:pool_exhausted(max_pending_requests), State};
        false ->
            handle_checkout_internal(From, Timeout, Exclude, BodySize, State)
    end.

-doc """
Serve a checkout from a connection that can take the request, or grow
the pool and queue the caller. A connection is unavailable when it is
short of either resource, stream slots or send credit, so the pool grows
on whichever runs short first, bounded by `size + max_overflow`. A
checkout that found free stream slots but no credit anywhere reports
`credit_starved`, which is the signal an operator sizes the pool from.
""".
-spec handle_checkout_internal(
    gen_server:from(), timeout(), pid() | undefined, non_neg_integer(), #state{}
) ->
    {noreply, #state{}} | {reply, term(), #state{}}.
handle_checkout_internal(From, Timeout, Exclude, BodySize, State) ->
    case find_available_connection(State, exclusion(Exclude), BodySize) of
        {ok, ConnPid, ConnInfo, NewState} ->
            emit_pool_checkout(NewState#state.telemetry_config, NewState#state.ref, 0),
            NewState2 = NewState#state{
                connections = (NewState#state.connections)#{ConnPid => ConnInfo}
            },
            {reply, {ok, ConnPid}, NewState2};
        Unavailable ->
            ok = note_unavailable(Unavailable, State),
            grow_or_queue(From, Timeout, Exclude, BodySize, State)
    end.

-doc """
Whether a checkout that excludes one connection has anywhere else to
wait for. A pool whose only connection is the one that refused, and that
cannot grow, has no capacity the waiter could ever be served from, so the
caller gets the refusal now instead of at its deadline. A connection that
is still connecting counts: the waiter is served on it once it is ready.
""".
-spec has_alternative(pid() | undefined, #state{}) -> boolean().
has_alternative(undefined, _State) ->
    true;
has_alternative(Exclude, #state{connections = Conns, pending_conns = Pending}) ->
    map_size(Pending) > 0 orelse
        lists:any(
            fun({Pid, #conn_info{draining = Draining}}) -> Pid =/= Exclude andalso not Draining end,
            maps:to_list(Conns)
        ).

-spec note_unavailable(none | credit_starved, #state{}) -> ok.
note_unavailable(none, _State) ->
    ok;
note_unavailable(credit_starved, #state{
    telemetry_config = Config, ref = Ref, conn_atomics = ConnAtomics
}) ->
    emit_pool_credit_starved(Config, Ref, ConnAtomics).

-doc """
Queue a checkout that found no free connection. The notify flag is
published before the re-check, so a stream slot that frees between the
failed search and the queue insert either reaches the waiter here or
provokes a checkin message. Without that order the waiter can sleep until
its deadline while capacity sits idle.
The waiter keeps the connection it must not return to. A re-dispatched
request that had to queue is served by another connection or not at all,
which is what the caller asked for when it named the exclusion.
""".
-spec queue_waiter(gen_server:from(), timeout(), pid() | undefined, non_neg_integer(), #state{}) ->
    {noreply, #state{}}.
queue_waiter(From, Timeout, Exclude, BodySize, State) ->
    Now = erlang:monotonic_time(millisecond),
    Deadline = Now + Timeout,
    QueuedAt = erlang:monotonic_time(),
    Waiter = #waiter{
        from = From,
        deadline = Deadline,
        queued_at = QueuedAt,
        exclude = Exclude,
        body_size = BodySize
    },
    NewState0 = State#state{
        waiters = queue:in(Waiter, State#state.waiters),
        waiter_count = State#state.waiter_count + 1
    },
    NewState = sync_checkin_flag(start_waiter_timeout_checker(NewState0)),
    {noreply, maybe_serve_waiters(NewState)}.

-doc """
Whether one more attempt is allowed for a stream that never opened. Both
the attempt count and the caller's own deadline have to hold: a loop that
re-acquires with the original timeout can outlive the deadline by a
multiple of it.
""".
-spec redispatch_allowed(timeout(), integer(), non_neg_integer()) -> boolean().
redispatch_allowed(_Timeout, _Start, Attempt) when Attempt >= ?MAX_STREAM_REDISPATCH ->
    false;
redispatch_allowed(infinity, _Start, _Attempt) ->
    true;
redispatch_allowed(Timeout, Start, _Attempt) ->
    erlang:monotonic_time(millisecond) - Start < Timeout.

%%%-----------------------------------------------------------------------------
%% INTERNAL: DRAIN
%%%-----------------------------------------------------------------------------
-spec all_connections_idle(#state{}) -> boolean().
all_connections_idle(#state{connections = Conns}) ->
    maps:fold(
        fun
            (_Pid, _Info, false) ->
                false;
            (_Pid, Info, true) ->
                live_stream_count(Info) =:= 0
        end,
        true,
        Conns
    ).

-spec drain_deadline(timeout()) -> integer() | infinity.
drain_deadline(infinity) ->
    infinity;
drain_deadline(Timeout) when is_integer(Timeout), Timeout >= 0 ->
    erlang:monotonic_time(millisecond) + Timeout.

-spec handle_drain(gen_server:from(), timeout(), #state{}) ->
    {reply, ok, #state{}} | {noreply, #state{}}.
handle_drain(From, _Timeout, #state{drain = D} = State) when D =/= undefined ->
    gen_server:reply(From, ok),
    {noreply, State};
handle_drain(From, Timeout, State) ->
    State1 = reject_pending_callers(State),
    Deadline = drain_deadline(Timeout),
    State2 = sync_checkin_flag(State1#state{drain = {From, Deadline}}),
    case all_connections_idle(State2) of
        true ->
            {reply, ok, sync_checkin_flag(State2#state{drain = undefined})};
        false ->
            schedule_drain_check(),
            {noreply, State2}
    end.

-spec handle_drain_check(#state{}) -> #state{}.
handle_drain_check(#state{drain = undefined} = State) ->
    State;
handle_drain_check(#state{drain = {From, Deadline}} = State) ->
    Now = erlang:monotonic_time(millisecond),
    case all_connections_idle(State) of
        true ->
            gen_server:reply(From, ok),
            sync_checkin_flag(State#state{drain = undefined});
        false when Deadline =/= infinity, Now >= Deadline ->
            gen_server:reply(From, ok),
            sync_checkin_flag(State#state{drain = undefined});
        false ->
            schedule_drain_check(),
            State
    end.

-spec maybe_complete_drain(#state{}) -> #state{}.
maybe_complete_drain(#state{drain = undefined} = State) ->
    State;
maybe_complete_drain(#state{drain = {From, _Deadline}} = State) ->
    case all_connections_idle(State) of
        true ->
            gen_server:reply(From, ok),
            sync_checkin_flag(State#state{drain = undefined});
        false ->
            State
    end.

-spec reject_pending_callers(#state{}) -> #state{}.
reject_pending_callers(#state{tab = Tab} = State) ->
    lists:foreach(
        fun(#waiter{from = W}) -> gen_server:reply(W, {error, pool_closed}) end,
        queue:to_list(State#state.waiters)
    ),
    lists:foreach(
        fun(#ready_waiter{from = RW}) -> gen_server:reply(RW, {error, pool_closed}) end,
        State#state.ready_waiters
    ),
    ok = nhttpc_registry:put_multiplexed_conns(Tab, {}),
    State#state{
        waiters = queue:new(),
        waiter_count = 0,
        ready_waiters = []
    }.

-spec schedule_drain_check() -> reference().
schedule_drain_check() ->
    erlang:send_after(?DRAIN_CHECK_INTERVAL, self(), drain_check).

%%%-----------------------------------------------------------------------------
%% INTERNAL: CONNECTION MANAGEMENT
%%%-----------------------------------------------------------------------------
-spec better_multiplexed_candidate(
    pid(), #conn_info{}, none | multiplexed_candidate(), integer()
) -> multiplexed_candidate().
better_multiplexed_candidate(Pid, Info, none, Rank) ->
    {Pid, Info, Rank};
better_multiplexed_candidate(Pid, Info, {_, _, BestRank}, Rank) when Rank < BestRank ->
    {Pid, Info, Rank};
better_multiplexed_candidate(_Pid, _Info, Best, _Rank) ->
    Best.

-spec compute_backoff_delay(non_neg_integer(), #state{}) -> non_neg_integer().
compute_backoff_delay(Failures, #state{
    reconnect_backoff_base = Base,
    reconnect_backoff_max = Max
}) ->
    case Failures of
        0 ->
            0;
        1 ->
            Base;
        N ->
            Delay = Base * (1 bsl (N - 1)),
            min(Delay, Max)
    end.

-doc """
Whether a connection exit says something about the origin.
A clean exit is one the client chose or the peer ended in order: the
supervisor's `shutdown`, the `normal` of a drain or a `stop`, the idle
timeout, a graceful close of an idle socket, and a close that follows
a cancel or a dead caller on HTTP/1.1. None of these proves that the
origin is unhealthy, so none of them moves the backoff counter.
Every other reason is a failure. The `DOWN` reason of a crash is an open
term, so the failure arm is the catch-all, and a new clean reason has to
be named here before the backoff stops counting it.
""".
-spec exit_kind(term()) -> clean | failure.
exit_kind(normal) -> clean;
exit_kind(shutdown) -> clean;
exit_kind({shutdown, _}) -> clean;
exit_kind(idle_timeout) -> clean;
exit_kind({connection_closed, graceful}) -> clean;
exit_kind({cancelled, _}) -> clean;
exit_kind({caller_down, _}) -> clean;
exit_kind(_Reason) -> failure.

-doc """
Pick a connection and claim one stream slot on it. The returned
`#conn_info{}` is already reserved, so a caller that drops it leaks the
slot. HTTP/1.1 leaves the idle map, which is what makes its checkout
exclusive; HTTP/2 and HTTP/3 reserve through `nhttpc_stream_slots:reserve/1`,
the same lock-free operation the ETS fast path uses. `Skip` holds the
multiplexed connections this search must not pick: a request whose
previous attempt a connection refused does not go back to it.
""".
-spec find_available_connection(#state{}, #{pid() => true}, non_neg_integer()) ->
    {ok, pid(), #conn_info{}, #state{}} | none | credit_starved.
find_available_connection(
    #state{idle_h1_conns = IdleH1, connections = Conns} = State, Skip, BodySize
) ->
    case maps:next(maps:iterator(IdleH1)) of
        {ConnPid, _, _} ->
            Rest = State#state{idle_h1_conns = maps:remove(ConnPid, IdleH1)},
            case maps:get(ConnPid, Conns, undefined) of
                #conn_info{protocol = http1_1, active_count = 0, draining = false} = Info ->
                    {ok, ConnPid, Info#conn_info{active_count = 1}, Rest};
                _ ->
                    find_available_connection(Rest, Skip, BodySize)
            end;
        none ->
            reserve_multiplexed(State, Skip, BodySize, false)
    end.

-spec fold_multiplexed_candidates(#state{}, #{pid() => true}, Pick) ->
    {pid(), #conn_info{}} | none
when
    Pick :: fun(
        (pid(), #conn_info{}, none | multiplexed_candidate()) -> none | multiplexed_candidate()
    ).
fold_multiplexed_candidates(#state{connections = Conns}, Skip, Pick) ->
    Result = maps:fold(
        fun
            (Pid, #conn_info{protocol = P, draining = false} = Info, Best) when
                P =:= http2; P =:= http3
            ->
                case maps:is_key(Pid, Skip) of
                    true -> Best;
                    false -> Pick(Pid, Info, Best)
                end;
            (_, _, Best) ->
                Best
        end,
        none,
        Conns
    ),
    case Result of
        none -> none;
        {Pid, Info, _Rank} -> {Pid, Info}
    end.

-doc """
Drop a connection that died before it reported a protocol. The monitor
already fired, so only the two indexes have to forget it. A pool below
its target schedules another attempt, with the backoff a failed connect
earns.
""".
-spec forget_pending(pid(), term(), #state{}) -> #state{}.
forget_pending(ConnPid, Reason, #state{pending_conns = Pending} = State) ->
    case maps:take(ConnPid, Pending) of
        error ->
            State;
        {{MonRef, _Slots}, Rest} ->
            erlang:demonitor(MonRef, [flush]),
            NewState = record_exit(Reason, State#state{
                pending_conns = Rest,
                monitor_to_pid = maps:remove(MonRef, State#state.monitor_to_pid)
            }),
            case State#state.drain of
                undefined when
                    NewState#state.conn_sup =/= undefined,
                    map_size(Rest) + map_size(NewState#state.connections) <
                        NewState#state.conn_target
                ->
                    schedule_reconnection(NewState);
                _ ->
                    NewState
            end
    end.

-doc """
Forget a connection that exited, whatever the path: the `connection_error`
cast carries the reason the connection chose, and a `DOWN` carries the
exit reason of a connection that sent none. `exit_kind/1` decides which
of the two the backoff counts.
""".
-spec handle_connection_down(pid(), term(), #state{}) -> #state{}.
handle_connection_down(
    ConnPid, Reason, #state{connections = Conns, conn_ets = ConnEts} = State
) ->
    case maps:get(ConnPid, Conns, undefined) of
        undefined ->
            forget_pending(ConnPid, Reason, State);
        #conn_info{monitor_ref = MonRef, protocol = Protocol} ->
            erlang:demonitor(MonRef, [flush]),
            NewIdleH1 = maps:remove(ConnPid, State#state.idle_h1_conns),
            ets:delete(ConnEts, ConnPid),
            NewReadyCount =
                case Protocol of
                    undefined -> State#state.ready_count;
                    _ -> max(0, State#state.ready_count - 1)
                end,
            NewState0 = State#state{
                connections = maps:remove(ConnPid, Conns),
                idle_h1_conns = NewIdleH1,
                monitor_to_pid = maps:remove(MonRef, State#state.monitor_to_pid),
                ready_count = NewReadyCount
            },
            rebuild_multiplexed_tuple(NewState0),
            NewState = record_exit(Reason, NewState0),
            case State#state.drain of
                undefined when
                    State#state.conn_sup =/= undefined,
                    map_size(NewState#state.connections) + map_size(NewState#state.pending_conns) <
                        NewState#state.conn_target
                ->
                    schedule_reconnection(NewState);
                undefined ->
                    NewState;
                _ ->
                    maybe_complete_drain(NewState)
            end
    end.

-doc """
Take a connection out of service and open its replacement now. The
connection sent this cast because the peer sent GOAWAY or because it
reached `max_streams_per_connection`, and in both cases it serves what
is in flight and exits when the last request completes. Waiting for
that exit before the top-up would leave the pool one connection short
for the whole drain. `serving_count/1` no longer counts the draining
connection, so the same top-up the `DOWN` path runs opens the
replacement here. `start_connections/2` refuses while `drain/2` runs,
so a pool that is shutting down opens nothing.
The `#conn_info{}` entry stays until the exit, so a checkin and a
`release/2` from a request still in flight find it.
""".
-spec handle_connection_draining(pid(), #state{}) -> #state{}.
handle_connection_draining(ConnPid, #state{connections = Conns} = State) ->
    case maps:get(ConnPid, Conns, undefined) of
        undefined ->
            State;
        Info ->
            NewInfo = Info#conn_info{draining = true},
            NewIdleH1 = maps:remove(ConnPid, State#state.idle_h1_conns),
            NewState = State#state{
                connections = Conns#{ConnPid => NewInfo},
                idle_h1_conns = NewIdleH1
            },
            rebuild_multiplexed_tuple(NewState),
            top_up_connections(NewState)
    end.

-doc """
Take a connection into the pool. The connection process wrote the
concurrent stream cap into its own array before it sent this cast, so
nothing here touches that slot.
One writer is still the rule, and the writer is the process that learns
the value first: a cast to the manager puts the manager mailbox between a
peer that lowers its cap and the lock-free checkout path that reads it.
The cast keeps the peer value in its payload. It is the one place an
observer sees what the peer advertised, and the manager reads none of it.
""".
-spec handle_connection_ready(pid(), nhttp_lib:version(), #state{}) -> #state{}.
handle_connection_ready(
    ConnPid,
    Protocol,
    #state{connections = Conns, conn_ets = ConnEts} = State
) ->
    Now = erlang:monotonic_time(millisecond),
    case maps:get(ConnPid, Conns, undefined) of
        undefined ->
            {MonRef, Slots, Pending} = take_pending(ConnPid, State#state.pending_conns),
            Info = #conn_info{
                pid = ConnPid,
                monitor_ref = MonRef,
                protocol = Protocol,
                active_count = 0,
                slots = Slots,
                draining = false,
                created_at = Now,
                last_activity = Now
            },
            NewIdleH1 =
                case Protocol of
                    http1_1 -> (State#state.idle_h1_conns)#{ConnPid => true};
                    _ -> State#state.idle_h1_conns
                end,
            EtsProtocol = protocol_to_ets(Protocol),
            ets:insert(ConnEts, {ConnPid, EtsProtocol, idle, Slots}),
            NewState = State#state{
                connections = Conns#{ConnPid => Info},
                pending_conns = Pending,
                idle_h1_conns = NewIdleH1,
                total_created = State#state.total_created + 1,
                monitor_to_pid = (State#state.monitor_to_pid)#{MonRef => ConnPid},
                ready_count = State#state.ready_count + 1
            },
            rebuild_multiplexed_tuple(NewState),
            NewState2 = notify_ready_waiters(maybe_promote_conn_target(Protocol, NewState)),
            maybe_serve_waiters(NewState2);
        Info ->
            NewInfo = Info#conn_info{
                protocol = Protocol,
                last_activity = Now
            },
            NewIdleH1 =
                case {Protocol, Info#conn_info.active_count} of
                    {http1_1, 0} -> (State#state.idle_h1_conns)#{ConnPid => true};
                    _ -> State#state.idle_h1_conns
                end,
            EtsProtocol = protocol_to_ets(Protocol),
            true = ets:update_element(ConnEts, ConnPid, {2, EtsProtocol}),
            NewReadyCount =
                case Info#conn_info.protocol of
                    undefined -> State#state.ready_count + 1;
                    _ -> State#state.ready_count
                end,
            NewState = State#state{
                connections = Conns#{ConnPid => NewInfo},
                idle_h1_conns = NewIdleH1,
                ready_count = NewReadyCount
            },
            rebuild_multiplexed_tuple(NewState),
            NewState2 = notify_ready_waiters(maybe_promote_conn_target(Protocol, NewState)),
            maybe_serve_waiters(NewState2)
    end.

-doc """
Reset the reconnect backoff on the first response a connection delivered.
Only a connection the pool holds counts: a stale cast from a pid the pool
no longer tracks says nothing about the origin it is reconnecting to.
""".
-spec handle_connection_served(pid(), #state{}) -> #state{}.
handle_connection_served(ConnPid, #state{connections = Conns} = State) ->
    case maps:is_key(ConnPid, Conns) of
        true -> reset_failures(State);
        false -> State
    end.

-doc """
Sweep the waiter queue after a mid-connection cap change. The connection
process already wrote the new cap into its own array, which is the only
copy the pool reads. A larger cap can release a queued waiter, and only
the manager holds that queue, so the cast exists for this pass alone.
""".
-spec handle_update_max_streams(pid(), #state{}) -> #state{}.
handle_update_max_streams(ConnPid, #state{connections = Conns} = State) ->
    case maps:is_key(ConnPid, Conns) of
        false -> State;
        true -> maybe_serve_waiters(State)
    end.

-doc """
How many connections the pool opens on its own, before any request.
Under `spread` that is `size`, which is what the pool did before the
policy existed. Under `pack` it is one: RFC 9113 section 9.1 asks a
client to use a single connection per origin and to open another only
when it cannot do otherwise, and `handle_checkout_internal/3` already
opens one on demand when every connection is at its cap. An HTTP/1.1
pool is promoted back to `size` by `maybe_promote_conn_target/2` once
the first connection reports its protocol, because exclusive checkout
needs one connection per concurrent caller.
""".
-spec initial_conn_target(nhttpc:stream_selection(), pos_integer()) -> pos_integer().
initial_conn_target(pack, _Size) -> 1;
initial_conn_target(spread, Size) -> Size.

-spec least_loaded_multiplexed_candidate(
    pid(), #conn_info{}, none | multiplexed_candidate(), non_neg_integer()
) -> none | multiplexed_candidate().
least_loaded_multiplexed_candidate(Pid, Info, Best, BodySize) ->
    rank_multiplexed_candidate(Pid, Info, Best, BodySize, fun(Live, _I) -> Live end).

-spec log_connect_failure(pid(), term(), #state{}) -> ok.
log_connect_failure(ConnPid, Reason, #state{pending_conns = Pending} = State) ->
    case maps:is_key(ConnPid, Pending) andalso exit_kind(Reason) =:= failure of
        true ->
            #state{
                ref = Ref,
                conn_opts = #{host := Host, port := Port},
                consecutive_failures = Failures
            } = State,
            logger:error(
                #{
                    event => connect_failed,
                    pool => Ref,
                    host => Host,
                    port => Port,
                    reason => Reason,
                    consecutive_failures => Failures + 1,
                    connection => ConnPid
                },
                #{domain => [nhttpc, pool]}
            );
        false ->
            ok
    end.

-doc """
Raise the connection target to `size` the first time an HTTP/1.1
connection reports ready under `pack`.
The pool cannot know before ALPN whether it will speak HTTP/1.1 or
HTTP/2, so it starts one connection either way. HTTP/1.1 checkout is
exclusive, so a pool that stays at one connection serves one caller at
a time until the checkout path grows it request by request. Promoting
here fills the pool once, and the guard on the current target makes it
happen once.
""".
-spec maybe_promote_conn_target(nhttp_lib:version(), #state{}) -> #state{}.
maybe_promote_conn_target(http1_1, #state{stream_selection = pack, size = Size} = State) when
    State#state.conn_target < Size
->
    top_up_connections(State#state{conn_target = Size});
maybe_promote_conn_target(_Protocol, State) ->
    State.

-spec oldest_multiplexed_candidate(
    pid(), #conn_info{}, none | multiplexed_candidate(), non_neg_integer()
) -> none | multiplexed_candidate().
oldest_multiplexed_candidate(Pid, Info, Best, BodySize) ->
    rank_multiplexed_candidate(Pid, Info, Best, BodySize, fun(_Live, I) ->
        I#conn_info.created_at
    end).

-doc """
Rank one connection that can take the request: a free stream slot and
credit for the body, read without reserving either. `reserve_multiplexed/4`
makes the exact claim afterwards, so this is a pre-filter that keeps a
connection short of either resource out of the ranking.
""".
-spec rank_multiplexed_candidate(
    pid(), #conn_info{}, none | multiplexed_candidate(), non_neg_integer(), Rank
) -> none | multiplexed_candidate() when
    Rank :: fun((integer(), #conn_info{}) -> integer()).
rank_multiplexed_candidate(Pid, #conn_info{slots = Slots} = Info, Best, BodySize, Rank) ->
    Live = nhttpc_stream_slots:live(Slots),
    HasSlot = Live < nhttpc_stream_slots:cap(Slots),
    case HasSlot andalso nhttpc_stream_slots:credit(Slots) >= BodySize of
        true -> better_multiplexed_candidate(Pid, Info, Best, Rank(Live, Info));
        false -> Best
    end.

-doc """
Count a connection exit against the backoff when `exit_kind/1` calls it a
failure. A clean exit leaves the counter where it is: it does not add to
it, and it does not reset it either, because only a served response is
proof that the origin serves.
""".
-spec record_exit(term(), #state{}) -> #state{}.
record_exit(Reason, State) ->
    case exit_kind(Reason) of
        failure -> record_failure(State);
        clean -> State
    end.

-spec record_failure(#state{}) -> #state{}.
record_failure(State) ->
    State#state{
        consecutive_failures = State#state.consecutive_failures + 1,
        last_failure_time = erlang:monotonic_time(millisecond)
    }.

-doc """
Multiplexed connection that accepts one more stream and the body, chosen
by the pool's policy. `Skip` holds the pids whose reservation lost the
race since this search started; without it a connection that fills up
between the fold and the reservation would be picked again on every
retry. `Starved` remembers that a connection with a free stream slot
refused the credit, which is what the answer `credit_starved` reports
when nothing else is left.
""".
-spec reserve_multiplexed(#state{}, #{pid() => true}, non_neg_integer(), boolean()) ->
    {ok, pid(), #conn_info{}, #state{}} | none | credit_starved.
reserve_multiplexed(State, Skip, BodySize, Starved) ->
    case select_multiplexed(State, Skip, BodySize) of
        none when Starved ->
            credit_starved;
        none ->
            none;
        {ConnPid, #conn_info{slots = Slots} = Info} ->
            case nhttpc_stream_slots:reserve(Slots, BodySize) of
                ok ->
                    {ok, ConnPid, Info, State};
                full ->
                    reserve_multiplexed(State, Skip#{ConnPid => true}, BodySize, Starved);
                starved ->
                    reserve_multiplexed(State, Skip#{ConnPid => true}, BodySize, true)
            end
    end.

-spec reset_failures(#state{}) -> #state{}.
reset_failures(State) ->
    State#state{
        consecutive_failures = 0,
        last_failure_time = undefined
    }.

-spec schedule_reconnection(#state{}) -> #state{}.
schedule_reconnection(#state{consecutive_failures = Failures} = State) ->
    Delay = compute_backoff_delay(Failures, State),
    erlang:send_after(Delay, self(), delayed_reconnect),
    State.

-doc """
Apply the selection policy. `pack` takes the oldest connection with a free
slot, which is the manager-side mirror of the fast path's first fit over
the oldest-first tuple. `spread` takes the connection carrying the fewest
live streams.
""".
-spec select_multiplexed(#state{}, #{pid() => true}, non_neg_integer()) ->
    {pid(), #conn_info{}} | none.
select_multiplexed(#state{stream_selection = pack} = State, Skip, BodySize) ->
    fold_multiplexed_candidates(State, Skip, fun(Pid, Info, Best) ->
        oldest_multiplexed_candidate(Pid, Info, Best, BodySize)
    end);
select_multiplexed(#state{stream_selection = spread} = State, Skip, BodySize) ->
    fold_multiplexed_candidates(State, Skip, fun(Pid, Info, Best) ->
        least_loaded_multiplexed_candidate(Pid, Info, Best, BodySize)
    end).

-doc """
Every connection the pool can still hand a request to, ready or in its
handshake. Growth decisions read this rather than the ready map: a burst
of checkouts that arrives while the first new connection is connecting
would otherwise start one connection per waiter and walk straight past
`size + max_overflow`.
A draining connection is left out. It takes no new stream, so it holds
no capacity, and counting it would leave the pool one connection short
for as long as its last request takes to complete.
""".
-spec serving_count(#state{}) -> non_neg_integer().
serving_count(#state{connections = Conns, pending_conns = Pending}) ->
    Serving = maps:fold(
        fun
            (_Pid, #conn_info{draining = false}, Acc) -> Acc + 1;
            (_Pid, #conn_info{draining = true}, Acc) -> Acc
        end,
        0,
        Conns
    ),
    Serving + maps:size(Pending).

-spec start_connection(#state{}, nhttpc_conn:conn_opts()) ->
    {ok, pid()} | {error, nhttpc_error:any_reason()}.
start_connection(#state{conn_sup = ConnSup}, ConnOpts) ->
    nhttpc_pool_conn_sup:start_connection(ConnSup, ConnOpts).

-spec start_connections(integer(), #state{}) -> #state{}.
start_connections(Count, State) when Count =< 0 ->
    State;
start_connections(_Count, #state{drain = D} = State) when D =/= undefined ->
    State;
start_connections(Count, State) ->
    lists:foldl(
        fun(_, Acc) ->
            case start_tracked_connection(Acc) of
                {ok, NewAcc} -> NewAcc;
                {error, _} -> Acc
            end
        end,
        State,
        lists:seq(1, Count)
    ).

-doc """
Start a connection and count it until it reports a protocol. The monitor
goes in `monitor_to_pid` next to the ready connections, so one `DOWN`
clause covers both, and it is handed to `#conn_info{}` on the way in
rather than replaced, which would leave a monitor with no owner.
The stream slots are created here and handed to that one child in its
connection options, so that the manager and the connection process share
one array from the first request onward.
""".
-spec start_tracked_connection(#state{}) -> {ok, #state{}} | {error, nhttpc_error:any_reason()}.
start_tracked_connection(#state{conn_opts = ConnOpts} = State) ->
    Slots = nhttpc_stream_slots:new(),
    case start_connection(State, ConnOpts#{stream_slots => Slots}) of
        {ok, ConnPid} ->
            MonRef = erlang:monitor(process, ConnPid),
            {ok, State#state{
                pending_conns = (State#state.pending_conns)#{ConnPid => {MonRef, Slots}},
                monitor_to_pid = (State#state.monitor_to_pid)#{MonRef => ConnPid}
            }};
        {error, _} = Error ->
            Error
    end.

-doc """
The monitor and the stream slots to record for a connection that just
reported ready.
A connection the pool started carries both. A connection that reports
ready with no pending entry has no array, and the manager cannot invent
one, because the connection process holds the array it releases into.
This match therefore fails rather than allocate a second array that
nothing else writes.
""".
-spec take_pending(pid(), Pending) ->
    {reference(), nhttpc_stream_slots:slots(), Pending}
when
    Pending :: #{pid() => {reference(), nhttpc_stream_slots:slots()}}.
take_pending(ConnPid, Pending) ->
    {{MonRef, Slots}, Rest} = maps:take(ConnPid, Pending),
    {MonRef, Slots, Rest}.

-doc """
Open enough connections to reach `conn_target`. Connections that are
mid-connect do not appear in `connections` yet, so a caller that runs
this while one is in flight over-starts; every caller here runs at a
point where none is.
""".
-spec top_up_connections(#state{}) -> #state{}.
top_up_connections(#state{conn_sup = undefined} = State) ->
    State;
top_up_connections(#state{conn_target = Target} = State) ->
    start_connections(Target - serving_count(State), State).

%%%-----------------------------------------------------------------------------
%% INTERNAL: WAITER HANDLING
%%%-----------------------------------------------------------------------------
-spec handle_wait_ready(gen_server:from(), timeout(), #state{}) ->
    {reply, ok, #state{}} | {noreply, #state{}}.
handle_wait_ready(From, Timeout, State) ->
    case State#state.ready_count > 0 of
        true ->
            {reply, ok, State};
        false ->
            Deadline = erlang:monotonic_time(millisecond) + Timeout,
            Waiter = #ready_waiter{from = From, deadline = Deadline},
            NewState0 = State#state{ready_waiters = [Waiter | State#state.ready_waiters]},
            NewState = start_waiter_timeout_checker(NewState0),
            {noreply, NewState}
    end.

-spec maybe_serve_waiters(#state{}) -> #state{}.
maybe_serve_waiters(#state{waiters = Waiters, waiter_count = WC} = State) when WC > 0 ->
    case queue:out(Waiters) of
        {{value, #waiter{exclude = Exclude, body_size = BodySize} = Waiter}, RestWaiters} ->
            case find_available_connection(State, exclusion(Exclude), BodySize) of
                {ok, ConnPid, ConnInfo, NewState} ->
                    serve_waiter(
                        Waiter,
                        ConnPid,
                        ConnInfo,
                        NewState#state{
                            waiters = RestWaiters,
                            waiter_count = WC - 1
                        }
                    );
                none ->
                    sample_occupancy(State);
                credit_starved ->
                    sample_occupancy(State)
            end;
        {empty, _} ->
            sample_occupancy(State)
    end;
maybe_serve_waiters(State) ->
    sample_occupancy(State).

-spec notify_ready_waiters(#state{}) -> #state{}.
notify_ready_waiters(#state{ready_waiters = []} = State) ->
    State;
notify_ready_waiters(#state{ready_waiters = Waiters} = State) ->
    lists:foreach(
        fun(#ready_waiter{from = From}) ->
            gen_server:reply(From, ok)
        end,
        Waiters
    ),
    State#state{ready_waiters = []}.

-spec occupancy_report_due(integer() | undefined, integer()) -> boolean().
occupancy_report_due(undefined, _Now) ->
    true;
occupancy_report_due(ReportedAt, Now) ->
    Now - ReportedAt >= ?OCCUPANCY_REPORT_INTERVAL_MS.

-spec report_occupancy(#state{}, integer()) -> #state{}.
report_occupancy(#state{waiter_count = WaiterCount} = State, Now) ->
    emit_pool_occupancy(State#state.telemetry_config, State#state.ref, #{
        waiter_count => WaiterCount, connections => serving_count(State)
    }),
    State#state{occupancy_reported_at = Now, occupancy_reported_waiters = WaiterCount}.

-doc """
Report the waiter count and the connection count as a level. Every path
that changes the waiter queue ends in `maybe_serve_waiters/1`, so this
runs once per manager step that touched the queue and never on the
lock-free path. While the queue holds a waiter the sample goes out once
a second at most. When the last waiter leaves it goes out once more, so
the level a subscriber holds returns to zero rather than sticking at the
last non-zero sample. A pool that never queues a waiter emits nothing.
""".
-spec sample_occupancy(#state{}) -> #state{}.
sample_occupancy(#state{telemetry_config = false} = State) ->
    State;
sample_occupancy(#state{waiter_count = 0, occupancy_reported_waiters = 0} = State) ->
    State;
sample_occupancy(#state{waiter_count = 0} = State) ->
    report_occupancy(State, erlang:monotonic_time(millisecond));
sample_occupancy(#state{occupancy_reported_at = ReportedAt} = State) ->
    Now = erlang:monotonic_time(millisecond),
    case occupancy_report_due(ReportedAt, Now) of
        true -> report_occupancy(State, Now);
        false -> State
    end.

-spec serve_waiter(#waiter{}, pid(), #conn_info{}, #state{}) -> #state{}.
serve_waiter(
    #waiter{from = From, queued_at = QueuedAt},
    ConnPid,
    ConnInfo,
    State
) ->
    QueueTimeMs = queue_time_ms(QueuedAt),
    emit_pool_checkout(State#state.telemetry_config, State#state.ref, QueueTimeMs),
    gen_server:reply(From, {ok, ConnPid}),
    NewState = sync_checkin_flag(State#state{
        connections = (State#state.connections)#{ConnPid => ConnInfo}
    }),
    maybe_serve_waiters(NewState).

%%%-----------------------------------------------------------------------------
%% INTERNAL: PERIODIC WAITER TIMEOUT CHECKER
%%%-----------------------------------------------------------------------------
-spec expire_front_waiters(integer(), queue:queue(#waiter{}), non_neg_integer(), #state{}) ->
    {queue:queue(#waiter{}), non_neg_integer()}.
expire_front_waiters(Now, Waiters, ExpiredCount, State) ->
    case queue:out(Waiters) of
        {{value, #waiter{deadline = D, from = From}}, Rest} when Now >= D ->
            emit_pool_timeout(State#state.telemetry_config, State#state.ref),
            gen_server:reply(From, nhttp_error:checkout_timeout()),
            expire_front_waiters(Now, Rest, ExpiredCount + 1, State);
        _ ->
            {Waiters, ExpiredCount}
    end.

-spec handle_check_waiter_timeouts(#state{}) -> #state{}.
handle_check_waiter_timeouts(#state{waiters = Waiters, ready_waiters = ReadyWaiters} = State) ->
    Now = erlang:monotonic_time(millisecond),
    {NewWaiters, Expired} = expire_front_waiters(Now, Waiters, 0, State),
    ActiveReadyWaiters = lists:filter(
        fun(#ready_waiter{from = From, deadline = Deadline}) ->
            case Now >= Deadline of
                true ->
                    gen_server:reply(From, {error, timeout}),
                    false;
                false ->
                    true
            end
        end,
        ReadyWaiters
    ),
    State1 = sync_checkin_flag(State#state{
        waiters = NewWaiters,
        waiter_count = State#state.waiter_count - Expired,
        ready_waiters = ActiveReadyWaiters
    }),
    State2 = maybe_serve_waiters(State1),
    HasWaiters = State2#state.waiter_count > 0 orelse State2#state.ready_waiters =/= [],
    case HasWaiters of
        false ->
            State2#state{waiter_check_timer_ref = undefined};
        true ->
            TimerRef = erlang:send_after(?TIMEOUT_CHECK_INTERVAL, self(), check_waiter_timeouts),
            State2#state{waiter_check_timer_ref = TimerRef}
    end.

-spec start_waiter_timeout_checker(#state{}) -> #state{}.
start_waiter_timeout_checker(#state{waiter_check_timer_ref = Ref} = State) when
    Ref =/= undefined
->
    State;
start_waiter_timeout_checker(State) ->
    TimerRef = erlang:send_after(?TIMEOUT_CHECK_INTERVAL, self(), check_waiter_timeouts),
    State#state{waiter_check_timer_ref = TimerRef}.

%%%-----------------------------------------------------------------------------
%% INTERNAL: ETS FAST PATH
%%%-----------------------------------------------------------------------------
-doc """
Reserve a stream slot and send credit for one request. `Exclude` is the
pid of the connection that refused the caller's previous attempt, or
`undefined`; both the ETS walk and the manager honour it, so a fallback
to the manager does not undo the exclusion. `BodySize` is the octets of
connection send credit the request needs, zero for a request without a
body.
Returns the connection and the owner of the reservation: `#pool_env{}`
when the fast path granted it directly, or the manager pid when the
checkout went through the `gen_server`. The owner decides how
`release/2` gives the slot back, so the two must be paired.
""".
-spec acquire(pid() | term(), timeout(), pid() | undefined, non_neg_integer()) ->
    {ok, pid(), #pool_env{} | pid()} | {error, nhttpc_error:any_reason()}.
acquire(PoolRef, Timeout, Exclude, BodySize) ->
    case pool_env(PoolRef) of
        {ok, Env} -> acquire_from_ets(Env, Timeout, Exclude, BodySize);
        error -> acquire_via_manager(PoolRef, Timeout, Exclude, BodySize)
    end.

-spec acquire_from_ets(#pool_env{}, timeout(), pid() | undefined, non_neg_integer()) ->
    {ok, pid(), #pool_env{} | pid()} | {error, nhttpc_error:any_reason()}.
acquire_from_ets(#pool_env{tab = Tab} = Env, Timeout, Exclude, BodySize) ->
    case multiplexed_conns(Tab) of
        {} ->
            checkout_from_tab(Tab, Timeout, Exclude, BodySize);
        ConnTuple ->
            Len = tuple_size(ConnTuple),
            try_multiplexed_conn(
                ConnTuple, Env, start_index(Env), Len, 0, Timeout, Exclude, BodySize, false
            )
    end.

-spec acquire_via_manager(pid() | term(), timeout(), pid() | undefined, non_neg_integer()) ->
    {ok, pid(), pid()} | {error, nhttpc_error:any_reason()}.
acquire_via_manager(PoolRef, Timeout, Exclude, BodySize) ->
    case resolve(PoolRef) of
        {ok, MgrPid, _SupPid} -> checkout_from_manager(MgrPid, Timeout, Exclude, BodySize);
        {error, pool_not_found} = Error -> Error
    end.

-spec cast_checkin(nhttpc_registry:tab(), pid()) -> ok.
cast_checkin(Tab, ConnPid) ->
    case registry_mgr(Tab) of
        undefined -> ok;
        MgrPid -> checkin(MgrPid, ConnPid)
    end.

-spec checkout_excluding(pid() | atom(), timeout(), pid() | undefined, non_neg_integer()) ->
    {ok, pid()} | {error, nhttpc_error:any_reason()}.
checkout_excluding(MgrRef, Timeout, Exclude, BodySize) ->
    gen_server:call(MgrRef, {checkout, Timeout, Exclude, BodySize}, safe_call_timeout(Timeout)).

-spec checkout_from_manager(pid(), timeout(), pid() | undefined, non_neg_integer()) ->
    {ok, pid(), pid()} | {error, nhttpc_error:any_reason()}.
checkout_from_manager(MgrPid, Timeout, Exclude, BodySize) ->
    case checkout_excluding(MgrPid, Timeout, Exclude, BodySize) of
        {ok, ConnPid} -> {ok, ConnPid, MgrPid};
        {error, _} = Error -> Error
    end.

-spec checkout_from_tab(nhttpc_registry:tab(), timeout(), pid() | undefined, non_neg_integer()) ->
    {ok, pid(), pid()} | {error, nhttpc_error:any_reason()}.
checkout_from_tab(Tab, Timeout, Exclude, BodySize) ->
    case registry_mgr(Tab) of
        undefined -> {error, pool_not_found};
        MgrPid -> checkout_from_manager(MgrPid, Timeout, Exclude, BodySize)
    end.

-doc """
Connection module that owns this connection pid.
The reservation owner answers it for nothing when it is a `#pool_env{}`,
because the pool runs one connection module for its whole life. A
manager pid owns the reservation only when the caller named the manager
instead of the pool reference, and that path is already a `gen_server`
call, so reading the connection's `$initial_call` costs nothing that
path did not already spend.
""".
-spec conn_module(#pool_env{} | pid(), pid()) -> nhttpc_conn | nhttpc_h3_conn.
conn_module(#pool_env{conn_mod = ConnMod}, _ConnPid) ->
    ConnMod;
conn_module(_MgrPid, ConnPid) ->
    case erlang:process_info(ConnPid, dictionary) of
        {dictionary, Dict} ->
            conn_module_of(proplists:get_value('$initial_call', Dict));
        undefined ->
            nhttpc_conn
    end.

-spec conn_module_of(term()) -> nhttpc_conn | nhttpc_h3_conn.
conn_module_of({nhttpc_h3_conn, init, _}) ->
    nhttpc_h3_conn;
conn_module_of(_Other) ->
    nhttpc_conn.

-spec exclusion(pid() | undefined) -> #{pid() => true}.
exclusion(undefined) ->
    #{};
exclusion(ConnPid) ->
    #{ConnPid => true}.

-doc """
Whether this connection keeps its live stream count in ETS, which is the
condition for skipping the checkin message. An unknown connection, or a
table that died with its pool, answers no and takes the message path,
where a cast to a dead manager is harmless.
""".
-spec multiplexed(#pool_env{}, pid()) -> boolean().
multiplexed(#pool_env{conn_ets = ConnEts}, ConnPid) ->
    try ets:lookup_element(ConnEts, ConnPid, 2, undefined) of
        h2 -> true;
        h3 -> true;
        _ -> false
    catch
        error:badarg -> false
    end.

-doc """
Round-robin tuple for this pool, or `{}` when there is none to read. The
registry table belongs to the pool supervisor and dies with it, so a
caller that outlives the pool it holds a `#pool_env{}` for reads a
table that is gone.
""".
-spec multiplexed_conns(nhttpc_registry:tab()) -> tuple().
multiplexed_conns(Tab) ->
    try
        nhttpc_registry:get_multiplexed_conns(Tab)
    catch
        error:badarg -> {}
    end.

-doc """
Pool state that a caller can reach without the manager, or `error` when
the reference names no live pool. A pool reference is any term, so the
lookup must not raise.
""".
-spec pool_env(pid() | term()) -> {ok, #pool_env{}} | error.
pool_env(PoolRef) ->
    case persistent_term:get({nhttpc_pool, PoolRef}, undefined) of
        #pool_env{} = Env -> {ok, Env};
        _ -> error
    end.

-spec protocol_to_ets(nhttp_lib:version() | undefined) -> h1 | h2 | h3 | undefined.
protocol_to_ets(http1_1) -> h1;
protocol_to_ets(http2) -> h2;
protocol_to_ets(http3) -> h3;
protocol_to_ets(undefined) -> undefined.

-doc """
Publish the multiplexed connection tuple in oldest-first order.
Every element is a `{ConnPid, Slots}` pair, so the fast path reserves a
stream slot without reading ETS at all.
The order is the search order of the `pack` policy, so it has to be
stable across rebuilds: a map fold alone gives an arbitrary order that
can change whenever a connection joins or leaves. `created_at` breaks
ties by pid, which two connections accepted in the same millisecond
need. The oldest connection holds the warmest HPACK table and the
largest congestion window, so it is the one to fill first.
A draining connection is left out, which is what stops the pool from
handing out a slot on a connection that already sent GOAWAY.
""".
-spec rebuild_multiplexed_tuple(#state{}) -> ok.
rebuild_multiplexed_tuple(#state{tab = Tab, connections = Conns}) ->
    Keyed = maps:fold(
        fun
            (Pid, #conn_info{protocol = P, draining = false, created_at = At, slots = S}, Acc) when
                P =:= http2; P =:= http3
            ->
                [{At, {Pid, S}} | Acc];
            (_, _, Acc) ->
                Acc
        end,
        [],
        Conns
    ),
    Ordered = [Pair || {_At, Pair} <- lists:sort(Keyed)],
    nhttpc_registry:put_multiplexed_conns(Tab, list_to_tuple(Ordered)).

-doc "Manager pid for this pool, or `undefined`. Mirror of `multiplexed_conns/1`.".
-spec registry_mgr(nhttpc_registry:tab()) -> pid() | undefined.
registry_mgr(Tab) ->
    try
        nhttpc_registry:lookup_mgr(Tab)
    catch
        error:badarg -> undefined
    end.

-doc """
Give one stream slot back. HTTP/2 and HTTP/3 already decremented the live
count in the connection process, so the manager only needs the message
when it holds a waiter or runs a drain. Everything else casts, because the
manager owns the state that makes the connection reusable.
""".
-spec release(#pool_env{} | pid(), pid()) -> ok.
release(#pool_env{atomics = ConnAtomics} = Env, ConnPid) ->
    case multiplexed(Env, ConnPid) andalso atomics:get(ConnAtomics, ?ATOMIC_NOTIFY) =:= 0 of
        true -> emit_pool_checkin(Env#pool_env.telemetry, Env#pool_env.ref);
        false -> cast_checkin(Env#pool_env.tab, ConnPid)
    end;
release(MgrPid, ConnPid) when is_pid(MgrPid) ->
    checkin(MgrPid, ConnPid).

-spec release_via_manager(pid() | term(), pid()) -> ok.
release_via_manager(PoolRef, ConnPid) ->
    case resolve(PoolRef) of
        {ok, MgrPid, _SupPid} -> checkin(MgrPid, ConnPid);
        {error, pool_not_found} -> ok
    end.

-doc """
Where the fast-path search starts in the connection tuple.
`pack` starts at the oldest connection every time, so the search is a
first fit over a stable order and the warm connection fills before a
cold one is touched. `spread` takes the next round-robin cursor, which
costs one atomic read-modify-write and moves every request to a
different connection.
""".
-spec start_index(#pool_env{}) -> non_neg_integer().
start_index(#pool_env{selection = pack}) ->
    0;
start_index(#pool_env{selection = spread, atomics = ConnAtomics}) ->
    atomics:add_get(ConnAtomics, ?ATOMIC_RR, 1).

-doc """
Walk the connection tuple from `Idx` until one connection reserves a
stream slot and the credit for the body. Every connection short of one
of the two means the caller has to queue, so the search hands over to
the manager, which owns the waiter queue and opens the next connection.
The walk is the same for both policies; only the start index differs.
Under `pack` it is 0, which makes this a first fit over the oldest-first
tuple. The connection `Exclude` names counts as tried and is never
reserved: it refused the caller's previous attempt. `Starved` remembers
a connection that had a free stream slot and refused the credit; a walk
that ends that way reports `credit_starved` before it hands over.
""".
-spec try_multiplexed_conn(
    tuple(),
    #pool_env{},
    non_neg_integer(),
    pos_integer(),
    non_neg_integer(),
    timeout(),
    pid() | undefined,
    non_neg_integer(),
    boolean()
) ->
    {ok, pid(), #pool_env{} | pid()} | {error, nhttpc_error:any_reason()}.
try_multiplexed_conn(_ConnTuple, Env, _Idx, Len, Tried, Timeout, Exclude, BodySize, Starved) when
    Tried >= Len
->
    #pool_env{tab = Tab, telemetry = Config, ref = Ref, atomics = ConnAtomics} = Env,
    case Starved of
        true -> emit_pool_credit_starved(Config, Ref, ConnAtomics);
        false -> ok
    end,
    checkout_from_tab(Tab, Timeout, Exclude, BodySize);
try_multiplexed_conn(ConnTuple, Env, Idx, Len, Tried, Timeout, Exclude, BodySize, Starved) ->
    case element((Idx rem Len) + 1, ConnTuple) of
        {Exclude, _Slots} ->
            try_multiplexed_conn(
                ConnTuple, Env, Idx + 1, Len, Tried + 1, Timeout, Exclude, BodySize, Starved
            );
        {ConnPid, Slots} ->
            case nhttpc_stream_slots:reserve(Slots, BodySize) of
                ok ->
                    emit_pool_checkout(Env#pool_env.telemetry, Env#pool_env.ref, 0),
                    {ok, ConnPid, Env};
                full ->
                    try_multiplexed_conn(
                        ConnTuple, Env, Idx + 1, Len, Tried + 1, Timeout, Exclude, BodySize, Starved
                    );
                starved ->
                    try_multiplexed_conn(
                        ConnTuple, Env, Idx + 1, Len, Tried + 1, Timeout, Exclude, BodySize, true
                    )
            end
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: CONNECTION SWEEP
%%%-----------------------------------------------------------------------------
-doc """
Drop every trace of a swept connection from the manager. The monitor goes
first, so no `DOWN` reaches `handle_connection_down/1` later; this is the
only other place that removes the ETS row.
""".
-spec forget_swept_connections([#conn_info{}], ets:tid() | undefined) -> ok.
forget_swept_connections(ToClose, ConnEts) ->
    lists:foreach(
        fun(#conn_info{pid = Pid, monitor_ref = MonRef}) ->
            erlang:demonitor(MonRef, [flush]),
            ets:delete(ConnEts, Pid)
        end,
        ToClose
    ).

-doc """
Bring the pool back up to `conn_target` after the sweep closed aged
connections. Under `pack` the target is 1, so a sweep that reaps three
idle connections restores none of them: the pool grows again only when a
request finds every connection at its stream cap.
""".
-spec replace_closed_connections(non_neg_integer(), #state{}) -> #state{}.
replace_closed_connections(0, State) ->
    State;
replace_closed_connections(_, #state{conn_sup = undefined} = State) ->
    State;
replace_closed_connections(Count, State) ->
    ToCreate = min(Count, State#state.conn_target - serving_count(State)),
    start_connections(ToCreate, State).

-spec schedule_connection_sweep(timeout(), timeout()) -> ok.
schedule_connection_sweep(_Interval, infinity) ->
    ok;
schedule_connection_sweep(infinity, _MaxAge) ->
    ok;
schedule_connection_sweep(Interval, _MaxAge) when is_integer(Interval), Interval > 0 ->
    _ = erlang:send_after(Interval, self(), connection_sweep),
    ok.

-spec stop_connection(#conn_info{}) -> ok.
stop_connection(#conn_info{pid = ConnPid, protocol = http3}) ->
    nhttpc_h3_conn:stop(ConnPid);
stop_connection(#conn_info{pid = ConnPid}) ->
    nhttpc_conn:stop(ConnPid).

-doc """
Shut the aged connections down off the manager. `nhttpc_conn:stop/1` waits
up to five seconds for each connection to die, and the manager is on the
request path.
""".
-spec stop_connections_async([#conn_info{}]) -> ok.
stop_connections_async([]) ->
    ok;
stop_connections_async(ToClose) ->
    Sweeper = spawn(fun() -> lists:foreach(fun stop_connection/1, ToClose) end),
    true = is_pid(Sweeper),
    ok.

-spec sweep_connections(#state{}) -> #state{}.
sweep_connections(#state{max_connection_age = infinity} = State) ->
    State;
sweep_connections(
    #state{connections = Conns, conn_ets = ConnEts, max_connection_age = MaxAge} = State
) ->
    Now = erlang:monotonic_time(millisecond),
    {ToClose, ToKeep} = maps:fold(
        fun(Pid, #conn_info{created_at = CreatedAt} = Info, {Close, Keep}) ->
            Age = Now - CreatedAt,
            case Age > MaxAge andalso live_stream_count(Info) =:= 0 of
                true ->
                    {[Info | Close], Keep};
                false ->
                    {Close, Keep#{Pid => Info}}
            end
        end,
        {[], #{}},
        Conns
    ),
    ok = stop_connections_async(ToClose),
    ok = forget_swept_connections(ToClose, ConnEts),
    NewState = State#state{
        connections = ToKeep,
        idle_h1_conns = maps:without(
            [Pid || #conn_info{pid = Pid} <- ToClose], State#state.idle_h1_conns
        ),
        monitor_to_pid = maps:without(
            [MonRef || #conn_info{monitor_ref = MonRef} <- ToClose], State#state.monitor_to_pid
        ),
        ready_count = max(0, State#state.ready_count - length(ToClose))
    },
    rebuild_multiplexed_tuple(NewState),
    replace_closed_connections(length(ToClose), NewState).

%%%-----------------------------------------------------------------------------
%% INTERNAL: HELPERS
%%%-----------------------------------------------------------------------------
-spec classify_pid(pid(), term(), list()) -> {ok, pid(), pid()} | {error, pool_not_found}.
classify_pid(Pid, {supervisor, nhttpc_pool_sup, _}, _Dict) ->
    mgr_from_sup(Pid);
classify_pid(Pid, {nhttpc_pool, init, _}, Dict) ->
    sup_from_mgr_dict(Pid, Dict);
classify_pid(_Pid, _Other, _Dict) ->
    {error, pool_not_found}.

-doc """
Live stream count for one connection. HTTP/2 and HTTP/3 keep it in the
connection's stream slots, which the connection process decrements on
stream close. Every other protocol keeps it in `#conn_info.active_count`,
because exclusive checkout reserves no stream slot.
""".
-spec live_stream_count(#conn_info{}) -> integer().
live_stream_count(#conn_info{protocol = P, slots = Slots}) when
    P =:= http2; P =:= http3
->
    nhttpc_stream_slots:live(Slots);
live_stream_count(#conn_info{active_count = ActiveCount}) ->
    ActiveCount.

-spec mgr_from_sup(pid()) -> {ok, pid(), pid()} | {error, pool_not_found}.
mgr_from_sup(SupPid) ->
    try supervisor:which_children(SupPid) of
        Children ->
            case lists:keyfind(nhttpc_pool, 1, Children) of
                {nhttpc_pool, MgrPid, _, _} when is_pid(MgrPid) ->
                    {ok, MgrPid, SupPid};
                _ ->
                    {error, pool_not_found}
            end
    catch
        exit:_ -> {error, pool_not_found}
    end.

-spec remaining_timeout(timeout(), integer()) -> timeout().
remaining_timeout(infinity, _Start) ->
    infinity;
remaining_timeout(Timeout, Start) ->
    max(1, Timeout - (erlang:monotonic_time(millisecond) - Start)).

-spec resolve_pid(pid()) -> {ok, pid(), pid()} | {error, pool_not_found}.
resolve_pid(Pid) ->
    case erlang:process_info(Pid, dictionary) of
        undefined ->
            {error, pool_not_found};
        {dictionary, Dict} ->
            classify_pid(Pid, proplists:get_value('$initial_call', Dict), Dict)
    end.

-spec safe_call_timeout(timeout()) -> timeout().
safe_call_timeout(infinity) -> infinity;
safe_call_timeout(Timeout) -> Timeout + 1000.

-spec sup_from_mgr_dict(pid(), list()) -> {ok, pid(), pid()} | {error, pool_not_found}.
sup_from_mgr_dict(MgrPid, Dict) ->
    case proplists:get_value('$ancestors', Dict) of
        [SupPid | _] when is_pid(SupPid) ->
            {ok, MgrPid, SupPid};
        _ ->
            {error, pool_not_found}
    end.

-doc """
Publish whether a fast-path checkin has to reach the manager. True while a
waiter sits in the queue or a drain runs; those are the only states in
which the manager acts on an HTTP/2 or HTTP/3 checkin. Call this after
every transition of either, and before the manager re-reads connection
capacity, so that a checkin racing the transition still sends its message.
""".
-spec sync_checkin_flag(#state{}) -> #state{}.
sync_checkin_flag(#state{conn_atomics = ConnAtomics, waiter_count = WC, drain = Drain} = State) ->
    Wanted =
        case WC > 0 orelse Drain =/= undefined of
            true -> 1;
            false -> 0
        end,
    ok = atomics:put(ConnAtomics, ?ATOMIC_NOTIFY, Wanted),
    State.

%%%-----------------------------------------------------------------------------
%% INTERNAL: TELEMETRY
%%%-----------------------------------------------------------------------------
-doc """
Pool telemetry takes the configuration and the pool reference rather than
`#state{}`, because both checkout paths emit it and only one of them runs
in the manager. The `false` clause keeps the metadata map off the hot path
of a pool with telemetry disabled, which is the default.
""".
-spec emit_pool_checkin(nhttpc_telemetry:otel_config(), term()) -> ok.
emit_pool_checkin(false, _Ref) ->
    ok;
emit_pool_checkin(Config, Ref) ->
    nhttpc_telemetry:client_pool_checkin(Config, #{pool => Ref}).

-spec emit_pool_checkout(nhttpc_telemetry:otel_config(), term(), non_neg_integer()) -> ok.
emit_pool_checkout(false, _Ref, _QueueTimeMs) ->
    ok;
emit_pool_checkout(Config, Ref, QueueTimeMs) ->
    nhttpc_telemetry:client_pool_checkout(Config, #{pool => Ref}, QueueTimeMs).

-doc """
Report that every connection refused a credit reservation while at least
one had a free stream slot, at most once a second for each pool and on
the rise only. Both checkout paths call this and only one of them runs
in the manager, so the timestamp of the last report lives in the pool
atomics, and a `compare_exchange` picks one emitter among concurrent
starved callers. Recovery emits nothing: it is visible as the event
stopping.
""".
-spec emit_pool_credit_starved(nhttpc_telemetry:otel_config(), term(), atomics:atomics_ref()) ->
    ok.
emit_pool_credit_starved(false, _Ref, _ConnAtomics) ->
    ok;
emit_pool_credit_starved(Config, Ref, ConnAtomics) ->
    Now = millis_since_vm_start(),
    Last = atomics:get(ConnAtomics, ?ATOMIC_CREDIT_STARVED_AT),
    case Last =:= 0 orelse Now - Last >= ?CREDIT_STARVED_INTERVAL_MS of
        true ->
            case atomics:compare_exchange(ConnAtomics, ?ATOMIC_CREDIT_STARVED_AT, Last, Now) of
                ok -> nhttpc_telemetry:client_pool_credit_starved(Config, #{pool => Ref});
                _Raced -> ok
            end;
        false ->
            ok
    end.

-spec emit_pool_exhausted(nhttpc_telemetry:otel_config(), term()) -> ok.
emit_pool_exhausted(false, _Ref) ->
    ok;
emit_pool_exhausted(Config, Ref) ->
    nhttpc_telemetry:client_pool_exhausted(Config, #{pool => Ref}).

-spec emit_pool_occupancy(
    nhttpc_telemetry:otel_config(), term(), nhttpc_telemetry:client_pool_level()
) -> ok.
emit_pool_occupancy(Config, Ref, Level) ->
    nhttpc_telemetry:client_pool_occupancy(Config, #{pool => Ref}, Level).

-spec emit_pool_timeout(nhttpc_telemetry:otel_config(), term()) -> ok.
emit_pool_timeout(false, _Ref) ->
    ok;
emit_pool_timeout(Config, Ref) ->
    nhttpc_telemetry:client_pool_timeout(Config, #{pool => Ref}).

-spec flatten_tls(nhttpc:tls_opts()) -> map().
flatten_tls(Tls) ->
    Extra = maps:get(extra, Tls, []),
    Base = maps:remove(extra, Tls),
    lists:foldl(
        fun
            ({K, V}, Acc) -> Acc#{K => V};
            (_, Acc) -> Acc
        end,
        Base,
        Extra
    ).

-doc """
A monotonic clock that fits an unsigned atomics slot. `monotonic_time/1`
can be negative, and the pool atomics are unsigned because the
round-robin cursor wraps rather than turns negative.
""".
-spec millis_since_vm_start() -> non_neg_integer().
millis_since_vm_start() ->
    Elapsed = erlang:monotonic_time() - erlang:system_info(start_time),
    erlang:convert_time_unit(Elapsed, native, millisecond).

-spec queue_time_ms(integer()) -> non_neg_integer().
queue_time_ms(QueuedAt) ->
    Duration = erlang:monotonic_time() - QueuedAt,
    erlang:convert_time_unit(Duration, native, millisecond).
