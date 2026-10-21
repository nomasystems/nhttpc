-module(nhttpc_conn).

-moduledoc """
Client connection process (proc_lib implementation).

Manages a single persistent connection to a server, handling the HTTP/1.1 or
HTTP/2 protocol state machine and request/response lifecycle.

This is an internal module used by nhttpc_pool. Do not use directly.

Performance optimizations (V3):
- proc_lib with explicit receive loops (eliminates gen_statem dispatch overhead)
- Opportunistic request batching via mailbox drain
- Single socket write per batch

## Stream-id rotation

An HTTP/2 connection counts the streams it opens. At
`max_streams_per_connection` it tells the pool it is draining, refuses
every request that still reaches it with the retryable `refused_stream`
reason, serves what is in flight, and exits `normal`. The count is of
streams opened and not of requests completed, because a stream costs
one identifier whatever the request does with it, and the identifier is
the resource that runs out. A GOAWAY from the peer takes the same path
with `goaway_received` in place of `rotating`.

A draining connection writes a cap of zero into its stream slots before
it tells the pool, so the lock-free checkout stops at once. It exits only
when no slot is live: a slot reserved before the write is a request on
its way, and the process waits for it, up to a grace period, to refuse
it with `refused_stream` rather than let the caller see the exit.

## Send queue and admission

The codec runs with its send queue on, and one rule, `body_admission/3`,
decides how a request body reaches it. A body that its credit claim
covers in full and that fits a fresh stream window is sent against the
credit the pool reserved for it. When the codec cannot send such a body
whole, the request is refused with the retryable `refused_stream`
reason and the connection state goes back to what the peer has seen, so
a small body never waits behind its HEADERS frame. A body above either
bound opens its stream, sends what the windows take, and leaves the rest
in the codec, which emits it as the peer's credit arrives. Such a
request is partly dispatched: the pool does not re-dispatch it, a cancel
or a timeout purges what is queued along with the RST_STREAM, and a
timeout before its END_STREAM left is retryable, because the peer never
saw a complete request.

The credit slot the pool reserves from tracks the codec's connection
send window. A queued body settles its claim when it is encoded and
debits the slot as the codec drains it, so the slot never counts credit
that queued octets are about to spend.
""".

-include("nhttpc.hrl").

-compile(
    {inline, [
        combine_buffer/2,
        has_pending_requests/1,
        find_request_by_stream/2,
        touch_activity/1,
        drain_sends/2
    ]}
).

%%%-----------------------------------------------------------------------------
%% EXPORTS - API
%%%-----------------------------------------------------------------------------
-export([
    cancel/2,
    info/1,
    ping/2,
    send_async/3,
    send_async/4,
    start_link/1,

    stop/1,
    wait_ready/2
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - PROC_LIB/SYS CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    format_status/1,
    init/1,

    system_code_change/4,
    system_continue/3,

    system_get_state/1,
    system_terminate/4
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([conn_opts/0, send_opts/0]).
-type send_opts() :: #{
    max_response_size => pos_integer() | infinity,
    max_header_size => pos_integer() | infinity,
    max_headers_count => pos_integer() | infinity
}.

-type h1_mode() :: ready | draining.

-type conn_opts() :: #{
    pool_mgr => pid(),
    host := binary(),
    port := inet:port_number(),
    transport => tcp | ssl,
    protocols => [nhttp_lib:version()],
    connect_timeout => timeout(),
    request_timeout => timeout(),
    idle_timeout => timeout(),
    certfile => file:filename(),
    keyfile => file:filename(),
    certs_keys => [map()],
    cacertfile => file:filename(),
    cacerts => [public_key:der_encoded()],
    verify => verify_none | verify_peer,
    server_name_indication => inet:hostname() | disable,
    wildcard_hostname => boolean(),
    supported_groups => [ssl:group()],
    otel => boolean() | #{traces => boolean(), metrics => boolean()},
    proxy => nhttpc_proxy:proxy_opts(),
    conn_ets => ets:tid(),
    stream_slots => nhttpc_stream_slots:slots(),
    max_streams_per_connection => pos_integer(),
    max_send_buffer => pos_integer() | infinity,
    max_queued_streams => pos_integer() | infinity
}.

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(H1_ACTIVE_N, 100).
-define(H2_ACTIVE_N, 64).
-define(H2_SETTINGS_TIMEOUT, 5000).
-define(MAX_H2_BATCH, 64).
-define(H2_LOCAL_INITIAL_WINDOW, 65535).
-define(CONN_MIN_BIN_VHEAP_SIZE, 1048576).
-define(CONN_FULLSWEEP_AFTER, 20).
-define(REQUEST_TIMEOUT_CHECK_INTERVAL, 100).
-define(DRAIN_GRACE_MS, 1000).
-define(CREDIT_REPORT_INTERVAL_US, 1_000_000).
-define(SEND_QUEUE_REPORT_INTERVAL_MS, 1000).
-define(CONNECTION_STREAM_ID, 0).

%%%-----------------------------------------------------------------------------
%% RECORDS
%%%-----------------------------------------------------------------------------
-record(credit_gauge, {
    count = 0 :: non_neg_integer(),
    octets = 0 :: non_neg_integer(),
    last_at :: integer() | undefined,
    gaps = 0 :: non_neg_integer(),
    gap_sum = 0 :: non_neg_integer(),
    gap_max = 0 :: non_neg_integer(),
    reported_at :: integer() | undefined
}).

-record(request_st, {
    stream_ref :: reference(),
    caller_pid :: pid(),
    caller_mref :: reference() | undefined,
    request :: nhttp_lib:request(),
    deadline :: integer() | undefined,
    stream_id :: nhttp_lib:stream_id() | undefined,
    response :: nhttp_lib:response() | undefined,
    body_stream :: nhttp_h1:body_stream() | undefined,
    body_acc = [] :: [binary()],
    body_size = 0 :: non_neg_integer(),
    body_pending = false :: boolean(),
    max_response_size = ?DEFAULT_MAX_RESPONSE_SIZE :: pos_integer() | infinity,
    parse_opts = #{
        max_header_size => ?DEFAULT_MAX_HEADER_SIZE,
        max_headers_count => ?DEFAULT_MAX_HEADERS_COUNT
    } :: nhttp_h1:opts()
}).

-record(conn_data, {
    parent :: pid(),
    pool_mgr :: pid() | undefined,
    self :: pid(),
    host :: binary(),
    port :: inet:port_number(),
    socket :: nhttp_sock:t() | undefined,
    transport :: tcp | ssl,
    protocol :: nhttp_lib:version() | undefined,
    h2_conn :: nhttp_h2:conn() | undefined,
    h2_scheme :: binary() | undefined,
    h2_authority :: binary() | undefined,
    h1_host_header :: binary() | undefined,
    h2_window = nhttpc_h2_window:new(?H2_LOCAL_INITIAL_WINDOW) :: nhttpc_h2_window:t(),
    h2_settings :: nhttp_h2:settings(),
    h2_max_streams = undefined :: non_neg_integer() | undefined,
    requests :: #{reference() => #request_st{}},
    stream_index :: #{nhttp_lib:stream_id() => reference()},
    caller_monitors = #{} :: #{reference() => reference()},
    goaway_received :: boolean(),
    served = false :: boolean(),
    current_request :: reference() | undefined,
    recv_buffer :: binary(),
    connect_timeout :: timeout(),
    request_timeout :: timeout(),
    idle_timeout :: timeout(),
    last_activity :: integer(),
    idle_check_timer_ref :: reference() | undefined,
    keep_alive_timeout :: timeout() | undefined,
    request_timeout_check_ref :: reference() | undefined,
    protocols :: [nhttp_lib:version()],
    pending_ping :: {pid(), reference(), binary()} | undefined,
    ready_waiters = [] :: [{{pid(), reference()}, integer()}],
    telemetry_config :: nhttpc_telemetry:otel_config(),
    telemetry_connect_start :: integer(),
    conn_ets :: ets:tid() | undefined,
    stream_slots :: nhttpc_stream_slots:slots() | undefined,
    credit_gauge = #credit_gauge{} :: #credit_gauge{},
    send_blocked_since = undefined :: integer() | undefined,
    send_blocked_ms = 0 :: non_neg_integer(),
    send_queue_reported_at = undefined :: integer() | undefined,
    connector_pid :: pid() | undefined,
    connector_mref :: reference() | undefined,
    streams_opened = 0 :: non_neg_integer(),
    max_streams_per_connection :: pos_integer(),
    rotating = false :: boolean(),
    drain_grace_ref = undefined :: reference() | undefined
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec cancel(pid(), reference()) -> ok | {error, not_found}.
cancel(ConnPid, Ref) ->
    call(ConnPid, {cancel, Ref}, 5000).

-spec info(pid()) ->
    {ok, #{version := nhttp_lib:version(), peer := nhttp_lib:peer()}}
    | {error, nhttpc_error:any_reason()}.
info(ConnPid) ->
    case wait_ready(ConnPid, 5000) of
        ok -> call(ConnPid, info, 5000);
        {error, _} = Error -> Error
    end.

-spec ping(pid(), timeout()) -> pong | {error, nhttpc_error:any_reason()}.
ping(ConnPid, Timeout) ->
    call(ConnPid, ping, Timeout).

-spec send_async(pid(), nhttp_lib:request(), pid()) -> {ok, reference()}.
send_async(ConnPid, Request, CallerPid) ->
    send_async(ConnPid, Request, CallerPid, #{}).

-spec send_async(pid(), nhttp_lib:request(), pid(), send_opts()) -> {ok, reference()}.
send_async(ConnPid, Request, CallerPid, Opts) ->
    StreamRef = make_ref(),
    ConnPid ! {send, StreamRef, Request, CallerPid, Opts},
    {ok, StreamRef}.

-spec start_link(conn_opts()) -> {ok, pid()} | {error, nhttpc_error:any_reason()}.
start_link(Opts) ->
    proc_lib:start_link(?MODULE, init, [{self(), Opts}]).

-spec stop(pid()) -> ok.
stop(ConnPid) ->
    MonRef = monitor(process, ConnPid),
    ConnPid ! {stop, self()},
    receive
        {'DOWN', MonRef, process, ConnPid, _Reason} -> ok
    after 5000 ->
        demonitor(MonRef, [flush]),
        exit(ConnPid, kill),
        ok
    end.

-spec wait_ready(pid(), timeout()) -> ok | {error, nhttpc_error:any_reason()}.
wait_ready(ConnPid, Timeout) ->
    call(ConnPid, wait_ready, Timeout).

%%%-----------------------------------------------------------------------------
%% CALL PROTOCOL
%%%-----------------------------------------------------------------------------
-spec call(pid(), term(), timeout()) -> term().
call(ConnPid, Request, Timeout) ->
    MonRef = monitor(process, ConnPid),
    Tag = MonRef,
    ConnPid ! {'$call', {self(), Tag}, Request},
    receive
        {Tag, Reply} ->
            demonitor(MonRef, [flush]),
            Reply;
        {'DOWN', MonRef, process, ConnPid, Reason} ->
            {error, {connection_died, Reason}}
    after Timeout ->
        demonitor(MonRef, [flush]),
        {error, timeout}
    end.

-spec reply({pid(), reference()}, term()) -> ok.
reply({Pid, Tag}, Reply) ->
    Pid ! {Tag, Reply},
    ok.

%%%-----------------------------------------------------------------------------
%% PROC_LIB INIT
%%%-----------------------------------------------------------------------------
-spec format_status(map()) -> map().
format_status(Status) ->
    case Status of
        #{state := {Phase, #conn_data{} = Data}} ->
            Status#{state := {Phase, sanitize_conn_data(Data)}};
        _ ->
            Status
    end.

-doc """
Start the connection process. Its heap holds the codec state, the
request map and the request bodies for the life of the connection. The
binary virtual heap is set to a megaword so that response bodies in
flight do not force a full sweep on every collection, and the full
sweep interval is set low so that a body's binaries are released soon
after its request completes. Both values come from a measurement of
the two workloads that matter, small bodies at a thousand streams and
large responses, recorded with the plan that introduced them.
""".
-spec init({pid(), conn_opts()}) -> no_return().
init({Parent, Opts}) ->
    process_flag(trap_exit, true),
    process_flag(min_bin_vheap_size, ?CONN_MIN_BIN_VHEAP_SIZE),
    process_flag(fullsweep_after, ?CONN_FULLSWEEP_AFTER),
    Self = self(),
    PoolMgr = maps:get(pool_mgr, Opts, undefined),
    Host = maps:get(host, Opts),
    Port = maps:get(port, Opts),
    Transport = maps:get(transport, Opts, tcp),
    Protocols = maps:get(protocols, Opts, [http2, http1_1]),
    ConnectTimeout = maps:get(connect_timeout, Opts, 5000),
    RequestTimeout = maps:get(request_timeout, Opts, 30000),
    IdleTimeout = maps:get(idle_timeout, Opts, 60000),

    ConnEts = maps:get(conn_ets, Opts, undefined),
    StreamSlots = maps:get(stream_slots, Opts, undefined),

    TelemetryConfig = nhttpc_telemetry:config(Opts),
    NormalizedHost = normalize_host(Host),
    TelemetryStartTime =
        case nhttpc_telemetry:enabled(TelemetryConfig) of
            false ->
                erlang:monotonic_time();
            true ->
                nhttpc_telemetry:client_connect_start(TelemetryConfig, #{
                    host => NormalizedHost,
                    port => Port,
                    transport => Transport
                })
        end,

    H2Scheme =
        case Transport of
            ssl -> <<"https">>;
            tcp -> <<"http">>
        end,
    DefaultPort =
        case Transport of
            ssl -> 443;
            tcp -> 80
        end,
    H2Authority =
        case Port of
            DefaultPort -> NormalizedHost;
            _ -> <<NormalizedHost/binary, ":", (integer_to_binary(Port))/binary>>
        end,
    H1HostHeader = H2Authority,

    ConnectOpts = build_connect_opts(Opts),
    ProxyOpts = maps:get(proxy, Opts, undefined),
    {ConnPid, ConnMRef} = spawn_monitor(fun() ->
        ConnectResult =
            case ProxyOpts of
                undefined ->
                    nhttp_sock:connect(NormalizedHost, Port, ConnectOpts, ConnectTimeout);
                _ ->
                    connect_via_proxy(
                        NormalizedHost, Port, Transport, ProxyOpts, ConnectOpts, ConnectTimeout
                    )
            end,
        case ConnectResult of
            {ok, Socket} ->
                case nhttp_sock:controlling_process(Socket, Self) of
                    ok ->
                        Self ! {connect_result, {ok, Socket}};
                    {error, Reason} ->
                        nhttp_sock:close(Socket),
                        Self ! {connect_result, {error, {controlling_process, Reason}}}
                end;
            {error, _} = Error ->
                Self ! {connect_result, Error}
        end
    end),

    Data = #conn_data{
        parent = Parent,
        pool_mgr = PoolMgr,
        self = Self,
        host = NormalizedHost,
        port = Port,
        transport = Transport,
        protocol = undefined,
        h2_conn = undefined,
        h2_scheme = H2Scheme,
        h2_authority = H2Authority,
        h1_host_header = H1HostHeader,
        h2_settings = h2_codec_settings(Opts),
        requests = #{},
        stream_index = #{},
        goaway_received = false,
        current_request = undefined,
        recv_buffer = <<>>,
        connect_timeout = ConnectTimeout,
        request_timeout = RequestTimeout,
        idle_timeout = IdleTimeout,
        last_activity = erlang:monotonic_time(millisecond),
        idle_check_timer_ref = undefined,
        keep_alive_timeout = undefined,
        request_timeout_check_ref = undefined,
        protocols = Protocols,
        pending_ping = undefined,
        ready_waiters = [],
        telemetry_config = TelemetryConfig,
        telemetry_connect_start = TelemetryStartTime,
        conn_ets = ConnEts,
        stream_slots = StreamSlots,
        connector_pid = ConnPid,
        connector_mref = ConnMRef,
        max_streams_per_connection = nhttpc_opts:max_streams_per_connection(Opts)
    },

    proc_lib:init_ack(Parent, {ok, Self}),
    connecting_loop(Data).

-spec system_code_change({atom(), #conn_data{}}, module(), term(), term()) ->
    {ok, {atom(), #conn_data{}}}.
system_code_change(State, _Module, _OldVsn, _Extra) ->
    {ok, State}.

-spec system_continue(pid(), [sys:debug_option()], {atom(), #conn_data{}}) -> no_return().
system_continue(_Parent, _Debug, {connecting, Data}) ->
    connecting_loop(Data);
system_continue(_Parent, _Debug, {ready, Data}) ->
    ready_loop(Data);
system_continue(_Parent, _Debug, {draining, Data}) ->
    draining_loop(Data).

-spec system_get_state({atom(), #conn_data{}}) -> {ok, {atom(), #conn_data{}}}.
system_get_state(State) ->
    {ok, State}.

-spec system_terminate(term(), pid(), [sys:debug_option()], {atom(), #conn_data{}}) -> no_return().
system_terminate(Reason, _Parent, _Debug, {_State, #conn_data{socket = Socket}}) ->
    case Socket of
        undefined -> ok;
        _ -> nhttp_sock:close(Socket)
    end,
    exit(Reason).

%%%-----------------------------------------------------------------------------
%% CONNECTING LOOP
%%%-----------------------------------------------------------------------------
-spec complete_connection(#conn_data{}) -> no_return().
complete_connection(#conn_data{protocol = http2} = Data) ->
    case init_h2_connection(Data) of
        {ok, Data1} ->
            emit_connect_stop(Data1),
            notify_pool_ready(Data1),
            case nhttp_sock:setopts(Data1#conn_data.socket, [{active, ?H2_ACTIVE_N}]) of
                ok ->
                    Data2 = start_idle_checker(touch_activity(Data1)),
                    reply_to_waiters(Data2#conn_data.ready_waiters, ok),
                    ready_loop(Data2#conn_data{ready_waiters = []});
                {error, Reason} ->
                    emit_connect_error(Data1, Reason),
                    notify_pool_error(Data1, {socket_error, Reason}),
                    exit({error, Reason})
            end;
        {error, Reason} ->
            emit_connect_error(Data, Reason),
            notify_pool_error(Data, Reason),
            exit({shutdown, Reason})
    end;
complete_connection(#conn_data{protocol = http1_1, socket = Socket} = Data) ->
    emit_connect_stop(Data),
    notify_pool_ready(Data),
    case nhttp_sock:setopts(Socket, [{active, ?H1_ACTIVE_N}]) of
        ok ->
            Data1 = start_idle_checker(touch_activity(Data)),
            reply_to_waiters(Data1#conn_data.ready_waiters, ok),
            ready_loop(Data1#conn_data{ready_waiters = []});
        {error, Reason} ->
            emit_connect_error(Data, Reason),
            notify_pool_error(Data, {socket_error, Reason}),
            exit({error, Reason})
    end.

-spec connecting_loop(#conn_data{}) -> no_return().
connecting_loop(
    #conn_data{
        parent = Parent,
        connect_timeout = ConnTimeout,
        connector_mref = ConnMRef
    } = Data
) ->
    receive
        {connect_result, {ok, Socket}} ->
            Data1 = clear_connector_mref(Data),
            Protocol = detect_protocol(Socket, Data1#conn_data.protocols),
            Data2 = Data1#conn_data{socket = Socket, protocol = Protocol},
            complete_connection(Data2);
        {connect_result, {error, Reason}} ->
            Data1 = clear_connector_mref(Data),
            emit_connect_error(Data1, Reason),
            notify_pool_error(Data1, Reason),
            exit({shutdown, Reason});
        {send, StreamRef, Request, CallerPid, _Opts} ->
            {error, Reason} = nhttp_error:connect_not_ready(),
            ok = reject_send(StreamRef, CallerPid, Reason, nhttpc_h2:credit_claim(Request), Data),
            connecting_loop(Data);
        {'$call', From, {cancel, _Ref}} ->
            reply(From, {error, not_found}),
            connecting_loop(Data);
        {'$nhttpc_cancel', Ref, From} ->
            ack_cancel(From, Ref),
            connecting_loop(Data);
        {'$call', From, wait_ready} ->
            Waiters = [{From, erlang:monotonic_time(millisecond)} | Data#conn_data.ready_waiters],
            connecting_loop(Data#conn_data{ready_waiters = Waiters});
        {'$call', From, ping} ->
            Waiters = [{From, erlang:monotonic_time(millisecond)} | Data#conn_data.ready_waiters],
            connecting_loop(Data#conn_data{ready_waiters = Waiters});
        {stop, _From} ->
            kill_connector(Data),
            exit(normal);
        {'EXIT', Parent, Reason} ->
            kill_connector(Data),
            emit_connect_error(Data, Reason),
            notify_pool_error(Data, Reason),
            exit(Reason);
        {'DOWN', ConnMRef, process, _, Reason} when ConnMRef =/= undefined ->
            emit_connect_error(Data, Reason),
            notify_pool_error(Data, Reason),
            exit({shutdown, {connect_worker_died, Reason}});
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [], {connecting, Data})
    after ConnTimeout ->
        kill_connector(Data),
        emit_connect_error(Data, connect_timeout),
        notify_pool_error(Data, connect_timeout),
        exit({shutdown, connect_timeout})
    end.

-spec reply_to_waiters([{{pid(), reference()}, integer()}], term()) -> ok.
reply_to_waiters([], _Reply) ->
    ok;
reply_to_waiters([{From, _Timestamp} | Rest], Reply) ->
    reply(From, Reply),
    reply_to_waiters(Rest, Reply).

%%%-----------------------------------------------------------------------------
%% READY LOOP
%%%-----------------------------------------------------------------------------
-spec ready_loop(#conn_data{}) -> no_return().
ready_loop(#conn_data{parent = Parent} = Data) ->
    receive
        {send, StreamRef, Request, CallerPid, Opts} when
            Data#conn_data.protocol == http2
        ->
            Batch = drain_sends([{StreamRef, Request, CallerPid, Opts}], ?MAX_H2_BATCH),
            Data1 = process_h2_batch(Batch, Data),
            case Data1#conn_data.rotating of
                true -> maybe_h2_state_transition_ready(Data1);
                false -> ready_loop(Data1)
            end;
        {send, StreamRef, Request, CallerPid, Opts} when
            Data#conn_data.protocol == http1_1,
            Data#conn_data.current_request == undefined
        ->
            Data1 = process_h1_single(StreamRef, Request, CallerPid, Opts, Data),
            ready_loop(Data1);
        {tcp, _Socket, Bytes} ->
            handle_socket_data_ready(Bytes, Data);
        {ssl, _Socket, Bytes} ->
            handle_socket_data_ready(Bytes, Data);
        {tcp_closed, _Socket} ->
            handle_socket_closed_ready(Data);
        {ssl_closed, _Socket} ->
            handle_socket_closed_ready(Data);
        {tcp_error, _Socket, Reason} ->
            fail_all_requests(Data, {socket_error, Reason}),
            notify_pool_error(Data, {socket_error, Reason}),
            exit({error, Reason});
        {ssl_error, _Socket, Reason} ->
            fail_all_requests(Data, {socket_error, Reason}),
            notify_pool_error(Data, {socket_error, Reason}),
            exit({error, Reason});
        {tcp_passive, _} ->
            ok = reactivate_socket(Data),
            ready_loop(Data);
        {ssl_passive, _} ->
            ok = reactivate_socket(Data),
            ready_loop(Data);
        check_request_timeouts ->
            case has_pending_requests(Data) of
                false ->
                    ready_loop(stop_request_timeout_checker(Data));
                true ->
                    ready_loop(handle_check_request_timeouts(Data))
            end;
        check_idle ->
            case has_pending_requests(Data) of
                true ->
                    ready_loop(Data#conn_data{idle_check_timer_ref = undefined});
                false ->
                    handle_check_idle_ready(Data)
            end;
        {'$call', From, {cancel, Ref}} ->
            case has_pending_requests(Data) of
                false ->
                    reply(From, {error, not_found}),
                    ready_loop(Data);
                true ->
                    Data1 = handle_cancel_request(Ref, From, Data),
                    ready_loop(Data1)
            end;
        {'$nhttpc_cancel', Ref, From} ->
            Data1 = handle_unified_cancel(Ref, From, Data),
            ready_loop(Data1);
        {'$call', From, wait_ready} ->
            reply(From, ok),
            ready_loop(Data);
        {'$call', From, info} ->
            reply(From, build_info(Data)),
            ready_loop(Data);
        {'$call', From, ping} ->
            Data1 = handle_ping(From, Data),
            ready_loop(Data1);
        {'DOWN', MRef, process, _Pid, _Reason} ->
            case maps:get(MRef, Data#conn_data.caller_monitors, undefined) of
                undefined ->
                    ready_loop(Data);
                StreamRef ->
                    ready_loop(handle_caller_down(StreamRef, Data))
            end;
        {stop, _From} ->
            nhttp_sock:close(Data#conn_data.socket),
            exit(normal);
        {'EXIT', Parent, Reason} ->
            fail_all_requests(Data, {connection_died, Reason}),
            nhttp_sock:close(Data#conn_data.socket),
            exit(Reason);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [], {ready, Data})
    end.

%%%-----------------------------------------------------------------------------
%% DRAINING LOOP
%%%-----------------------------------------------------------------------------
-spec draining_loop(#conn_data{}) -> no_return().
draining_loop(#conn_data{parent = Parent} = Data) ->
    receive
        {send, StreamRef, Request, CallerPid, _Opts} ->
            Reason = draining_refusal_reason(Data),
            ok = reject_send(StreamRef, CallerPid, Reason, nhttpc_h2:credit_claim(Request), Data),
            exit_when_drained(Data);
        {tcp, _Socket, Bytes} ->
            handle_socket_data_draining(Bytes, Data);
        {ssl, _Socket, Bytes} ->
            handle_socket_data_draining(Bytes, Data);
        {tcp_closed, _Socket} ->
            handle_socket_closed_draining(Data);
        {ssl_closed, _Socket} ->
            handle_socket_closed_draining(Data);
        {tcp_passive, _} ->
            ok = reactivate_socket(Data),
            draining_loop(Data);
        {ssl_passive, _} ->
            ok = reactivate_socket(Data),
            draining_loop(Data);
        check_request_timeouts ->
            exit_when_drained(handle_check_request_timeouts(Data));
        check_idle ->
            draining_loop(Data#conn_data{idle_check_timer_ref = undefined});
        {'$call', From, {cancel, Ref}} ->
            Data1 = handle_cancel_request(Ref, From, Data),
            draining_loop(Data1);
        {'$nhttpc_cancel', Ref, From} ->
            Data1 = handle_unified_cancel(Ref, From, Data),
            draining_loop(Data1);
        {'$call', From, wait_ready} ->
            reply(From, {error, draining}),
            draining_loop(Data);
        {'$call', From, ping} ->
            reply(From, {error, draining}),
            draining_loop(Data);
        {'DOWN', MRef, process, _Pid, _Reason} ->
            case maps:get(MRef, Data#conn_data.caller_monitors, undefined) of
                undefined ->
                    draining_loop(Data);
                StreamRef ->
                    exit_when_drained(handle_caller_down(StreamRef, Data))
            end;
        drain_grace ->
            exit(normal);
        {stop, _From} ->
            nhttp_sock:close(Data#conn_data.socket),
            exit(normal);
        {'EXIT', Parent, Reason} ->
            fail_all_requests(Data, {connection_died, Reason}),
            nhttp_sock:close(Data#conn_data.socket),
            exit(Reason);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [], {draining, Data})
    end.

-doc """
Leave the draining loop once the connection owes nothing. A request in
the map is served first. A live stream slot with no request behind it is
a reservation the lock-free checkout made before the cap dropped to
zero, so its `{send, ...}` is still on its way and the process stays to
refuse it. The grace timer bounds that wait, because a caller that died
between its reservation and its send never releases the slot.
""".
-spec exit_when_drained(#conn_data{}) -> no_return().
exit_when_drained(#conn_data{requests = Requests} = Data) when map_size(Requests) > 0 ->
    draining_loop(Data);
exit_when_drained(Data) ->
    case reserved_streams(Data) of
        0 -> exit(normal);
        _ -> draining_loop(start_drain_grace(Data))
    end.

-spec reserved_streams(#conn_data{}) -> integer().
reserved_streams(#conn_data{protocol = http2, stream_slots = Slots}) when Slots =/= undefined ->
    nhttpc_stream_slots:live(Slots);
reserved_streams(_Data) ->
    0.

-spec start_drain_grace(#conn_data{}) -> #conn_data{}.
start_drain_grace(#conn_data{drain_grace_ref = Ref} = Data) when Ref =/= undefined ->
    Data;
start_drain_grace(#conn_data{self = Self} = Data) ->
    Data#conn_data{drain_grace_ref = erlang:send_after(?DRAIN_GRACE_MS, Self, drain_grace)}.

%%%-----------------------------------------------------------------------------
%% OPPORTUNISTIC BATCHING (HTTP/2 ONLY)
%%%-----------------------------------------------------------------------------
-doc """
The admission rule for a request body, and the only place that decides
it.
`credit`: the credit claim covers the whole body and a fresh stream
window holds it, so the pool reserved every octet and the codec is to
send it in one call. A codec that queues any of it instead has less
credit than the pool believed, and `offer_h2_body/6` refuses the
request so that the pool re-dispatches it: a small body never waits
behind a HEADERS frame for credit.
`queue`: the body is above the claim cap or above the stream window, so
no fresh stream can carry it whole. The stream opens, the codec sends
what the windows take and holds the rest until the peer credits.
""".
-spec body_admission(nhttp_lib:request(), pos_integer(), nhttp_h2:conn()) -> credit | queue.
body_admission(Request, BodySize, H2Conn) ->
    ClaimCoversBody = nhttpc_h2:credit_claim(Request) =:= BodySize,
    case ClaimCoversBody andalso BodySize =< initial_stream_window(H2Conn) of
        true -> credit;
        false -> queue
    end.

-spec drain_sends([{reference(), nhttp_lib:request(), pid(), send_opts()}], non_neg_integer()) ->
    [{reference(), nhttp_lib:request(), pid(), send_opts()}].
drain_sends(Acc, 0) ->
    lists:reverse(Acc);
drain_sends(Acc, Remaining) ->
    receive
        {send, StreamRef, Request, CallerPid, Opts} ->
            drain_sends([{StreamRef, Request, CallerPid, Opts} | Acc], Remaining - 1)
    after 0 ->
        lists:reverse(Acc)
    end.

-spec encode_h2_batch(
    [{reference(), nhttp_lib:request(), pid(), send_opts()}],
    [iodata()],
    #conn_data{}
) -> {[iodata()], #conn_data{}}.
encode_h2_batch([], FramesAcc, Data) ->
    {FramesAcc, Data};
encode_h2_batch([{StreamRef, Request, CallerPid, Opts} | Rest], FramesAcc, Data) ->
    case encode_h2_request(StreamRef, Request, CallerPid, Opts, Data) of
        {ok, Frames, Data1} ->
            encode_h2_batch(Rest, [Frames | FramesAcc], Data1);
        {error, Reason, Data1} ->
            ok = reject_send(StreamRef, CallerPid, Reason, nhttpc_h2:credit_claim(Request), Data1),
            encode_h2_batch(Rest, FramesAcc, Data1)
    end.

-doc """
Open a stream and encode one request onto it. A stream the codec refuses
before it exists carries the reason that `open_stream_error/1` gives it.
An error return carries the `#conn_data{}` from before `open_stream/1`.
The batch discards every frame of a failed request and nothing of it
reaches the socket, so the revert has to be complete: the HPACK encoder
table, the stream map, `next_stream_id`, the send queue entry and the
caller monitor all go back to what the peer has seen. Keeping the
post-`open_stream/1` state on an error leaves an encoder entry the
peer's decoder never saw, and every later header block on the
connection decodes wrong.
""".
-spec encode_h2_request(reference(), nhttp_lib:request(), pid(), send_opts(), #conn_data{}) ->
    {ok, iodata(), #conn_data{}} | {error, term(), #conn_data{}}.
encode_h2_request(_StreamRef, _Request, _CallerPid, _Opts, #conn_data{rotating = true} = Data) ->
    {error, stream_refused_reason(), Data};
encode_h2_request(StreamRef, Request, CallerPid, Opts, #conn_data{h2_conn = H2Conn} = Data) ->
    case nhttp_h2:open_stream(H2Conn) of
        {ok, StreamId, H2Conn1} ->
            Opened = Data#conn_data{h2_conn = H2Conn1},
            case encode_h2_stream(StreamRef, Request, CallerPid, Opts, StreamId, Opened) of
                {ok, Frames, Data1} -> {ok, Frames, count_opened_stream(Data1)};
                {error, Reason} -> {error, Reason, Data}
            end;
        {error, Reason} ->
            {error, open_stream_error(Reason), Data}
    end.

-doc """
Encode one request onto a stream the peer accepted. Split from
`encode_h2_request/5` so that a refused request stays an error return: a
crash here would take every other in-flight request on the connection
with it.
An error carries no state on purpose. `encode_h2_request/5` reverts to
the state from before the stream opened, so no arm here can leak a
half-encoded request into the connection.
""".
-spec encode_h2_stream(
    reference(), nhttp_lib:request(), pid(), send_opts(), nhttp_lib:stream_id(), #conn_data{}
) ->
    {ok, iodata(), #conn_data{}} | {error, term()}.
encode_h2_stream(
    StreamRef,
    Request,
    CallerPid,
    Opts,
    StreamId,
    #conn_data{
        h2_conn = H2Conn1,
        h2_scheme = Scheme,
        h2_authority = Authority,
        request_timeout = RequestTimeout
    } = Data
) ->
    Headers = nhttpc_h2:request_to_h2_headers(Request, Scheme, Authority),
    Body = maps:get(body, Request, <<>>),
    BodySize = iolist_size(Body),
    EndStream =
        case BodySize of
            0 -> fin;
            _ -> nofin
        end,
    MRef = erlang:monitor(process, CallerPid),
    ReqSt = #request_st{
        stream_ref = StreamRef,
        caller_pid = CallerPid,
        caller_mref = MRef,
        request = Request,
        stream_id = StreamId,
        deadline = erlang:monotonic_time(millisecond) + RequestTimeout,
        max_response_size = max_response_size(Opts)
    },
    case nhttp_h2:send_headers(H2Conn1, StreamId, Headers, EndStream) of
        {ok, H2Conn2, HeaderFrame} when BodySize =:= 0 ->
            {ok, HeaderFrame, register_h2_request(ReqSt, H2Conn2, Data)};
        {ok, H2Conn2, HeaderFrame} ->
            case offer_h2_body(Request, Body, BodySize, StreamId, H2Conn2, Data) of
                {ok, H2Conn3, DataFrames, Buffered} ->
                    Pending = ReqSt#request_st{body_pending = Buffered > 0},
                    Data1 = register_h2_request(Pending, H2Conn3, Data),
                    ok = settle_credit_claim(Request, BodySize - Buffered, Data1),
                    {ok, [HeaderFrame, DataFrames], Data1};
                {error, Reason} ->
                    erlang:demonitor(MRef, [flush]),
                    {error, Reason}
            end;
        {error, Reason} ->
            erlang:demonitor(MRef, [flush]),
            {error, {http2_error, Reason}}
    end.

-spec h1_parse_opts(send_opts()) -> nhttp_h1:opts().
h1_parse_opts(Opts) ->
    Size = maps:get(max_header_size, Opts, ?DEFAULT_MAX_HEADER_SIZE),
    Count = maps:get(max_headers_count, Opts, ?DEFAULT_MAX_HEADERS_COUNT),
    maps:filter(
        fun(_, V) -> V =/= infinity end,
        #{max_header_size => Size, max_headers_count => Count}
    ).

-spec initial_stream_window(nhttp_h2:conn()) -> pos_integer().
initial_stream_window(H2Conn) ->
    maps:get(initial_window_size, nhttp_h2:peer_settings(H2Conn)).

-spec max_response_size(send_opts()) -> pos_integer() | infinity.
max_response_size(Opts) ->
    maps:get(max_response_size, Opts, ?DEFAULT_MAX_RESPONSE_SIZE).

-doc """
Offer one request body to the codec under `body_admission/3`. The
success arm carries the frames to write and the octets the codec holds.
A refusal, for credit the connection lacks or for a full send queue,
carries the retryable `refused_stream` reason: the frames are discarded
with the codec state, so the peer sees nothing of the request.
""".
-spec offer_h2_body(
    nhttp_lib:request(),
    iodata(),
    pos_integer(),
    nhttp_lib:stream_id(),
    nhttp_h2:conn(),
    #conn_data{}
) ->
    {ok, nhttp_h2:conn(), iodata(), non_neg_integer()} | {error, term()}.
offer_h2_body(Request, Body, BodySize, StreamId, H2Conn, Data) ->
    Admission = body_admission(Request, BodySize, H2Conn),
    case {Admission, nhttp_h2:send_data(H2Conn, StreamId, Body, fin)} of
        {_, {ok, H2Conn1, Frames}} ->
            {ok, H2Conn1, Frames, 0};
        {queue, {queued, H2Conn1, Frames, Buffered}} ->
            {ok, H2Conn1, Frames, Buffered};
        {credit, {queued, _H2Conn1, _Frames, _Buffered}} ->
            refuse_h2_body(StreamId, H2Conn, Data);
        {_, {error, send_buffer_full}} ->
            refuse_h2_body(StreamId, H2Conn, Data);
        {_, {error, Reason}} ->
            {error, {http2_error, Reason}}
    end.

-spec process_h1_single(reference(), nhttp_lib:request(), pid(), send_opts(), #conn_data{}) ->
    #conn_data{}.
process_h1_single(
    StreamRef, Request, CallerPid, Opts, #conn_data{requests = Requests} = Data
) ->
    MRef = erlang:monitor(process, CallerPid),
    ReqSt = #request_st{
        stream_ref = StreamRef,
        caller_pid = CallerPid,
        caller_mref = MRef,
        request = Request,
        max_response_size = max_response_size(Opts),
        parse_opts = h1_parse_opts(Opts)
    },
    ReqWithHost = add_host_header(Request, Data),
    ReqWithKeepalive = add_connection_keepalive(ReqWithHost),
    case nhttp_h1:encode_request(ReqWithKeepalive) of
        {ok, IOList} ->
            send_h1_single(IOList, MRef, StreamRef, ReqSt, Requests, Data);
        {error, EncodeError} ->
            erlang:demonitor(MRef, [flush]),
            send_response(
                ReqSt,
                nhttpc_error:invalid_request(element(1, EncodeError)),
                Data#conn_data.self
            ),
            Data
    end.

-spec process_h2_batch(
    [{reference(), nhttp_lib:request(), pid(), send_opts()}], #conn_data{}
) -> #conn_data{}.
process_h2_batch(Batch, Data) ->
    {Frames, Data1} = encode_h2_batch(Batch, [], Data),
    case Frames of
        [] ->
            Data1;
        _ ->
            case nhttp_sock:send(Data1#conn_data.socket, lists:reverse(Frames)) of
                ok ->
                    start_request_timeout_checker(track_send_buffer(Data1));
                {error, Reason} ->
                    lists:foreach(
                        fun({StreamRef, Request, CallerPid, _Opts}) ->
                            ReqSt = #request_st{
                                stream_ref = StreamRef, caller_pid = CallerPid, request = Request
                            },
                            send_response(
                                ReqSt, nhttp_error:send_error(Reason), Data1#conn_data.self
                            )
                        end,
                        Batch
                    ),
                    exit({error, Reason})
            end
    end.

-spec refuse_h2_body(nhttp_lib:stream_id(), nhttp_h2:conn(), #conn_data{}) -> {error, term()}.
refuse_h2_body(StreamId, H2Conn, Data) ->
    emit_send_credit_refused(Data, StreamId, nhttp_h2:connection_send_window(H2Conn)),
    {error, stream_refused_reason()}.

-spec register_h2_request(#request_st{}, nhttp_h2:conn(), #conn_data{}) -> #conn_data{}.
register_h2_request(
    #request_st{stream_ref = StreamRef, stream_id = StreamId, caller_mref = MRef} = ReqSt,
    H2Conn,
    #conn_data{requests = Requests, stream_index = StreamIndex, caller_monitors = Mons} = Data
) ->
    Data#conn_data{
        h2_conn = H2Conn,
        requests = Requests#{StreamRef => ReqSt},
        stream_index = StreamIndex#{StreamId => StreamRef},
        caller_monitors = Mons#{MRef => StreamRef}
    }.

-spec send_h1_single(
    iolist(), reference(), reference(), #request_st{}, #{reference() => #request_st{}}, #conn_data{}
) ->
    #conn_data{}.
send_h1_single(IOList, MRef, StreamRef, ReqSt, Requests, #conn_data{socket = Socket} = Data) ->
    case nhttp_sock:send(Socket, IOList) of
        ok ->
            Deadline = erlang:monotonic_time(millisecond) + Data#conn_data.request_timeout,
            NewReqSt = ReqSt#request_st{deadline = Deadline},
            CallerMons = (Data#conn_data.caller_monitors)#{MRef => StreamRef},
            Data1 = Data#conn_data{
                current_request = StreamRef,
                requests = Requests#{StreamRef => NewReqSt},
                caller_monitors = CallerMons
            },
            start_request_timeout_checker(Data1);
        {error, closed} ->
            erlang:demonitor(MRef, [flush]),
            send_response(ReqSt, nhttp_error:connection_closed(retryable), Data#conn_data.self),
            notify_pool_error(Data, {connection_closed, send_failed}),
            exit(normal);
        {error, Reason} ->
            erlang:demonitor(MRef, [flush]),
            send_response(ReqSt, nhttp_error:send_error(Reason), Data#conn_data.self),
            notify_pool_error(Data, {send_error, Reason}),
            exit({error, Reason})
    end.

-doc """
Settle the credit claim of an encoded body against what the codec sent.
The pool reserved `nhttpc_h2:credit_claim/1` octets at checkout. A body
admitted on credit sent exactly its claim and settles nothing. A queued
body sent what the windows allowed: the claim beyond that goes back to
the slot, because the queued octets debit the slot when they leave, and
octets sent past the claim, on a peer whose windows outgrow the cap, are
debited now.
""".
-spec settle_credit_claim(nhttp_lib:request(), non_neg_integer(), #conn_data{}) -> ok.
settle_credit_claim(Request, Sent, Data) ->
    case nhttpc_h2:credit_claim(Request) - Sent of
        0 -> ok;
        Refund when Refund > 0 -> refund_send_credit(Refund, Data);
        Overrun -> debit_send_credit(-Overrun, Data)
    end.

%%%-----------------------------------------------------------------------------
%% SOCKET DATA HANDLING
%%%-----------------------------------------------------------------------------
-spec advance_h1(h1_mode(), binary(), reference(), #request_st{}, #conn_data{}) -> no_return().
advance_h1(Mode, Combined, Ref, #request_st{response = undefined} = ReqSt, Data) ->
    case nhttp_h1:parse_response_head(Combined, ReqSt#request_st.parse_opts) of
        {ok, Status, Reason, Version, Headers, Rest} ->
            Method = h1_request_method(ReqSt),
            Stream = nhttp_h1:body_stream_from_response(Method, Status, Headers),
            Resp = #{
                status => Status,
                reason => Reason,
                version => Version,
                headers => Headers,
                body => <<>>
            },
            ReqSt1 = ReqSt#request_st{response = Resp, body_stream = Stream},
            drive_h1_body(Mode, Rest, Ref, ReqSt1, Data);
        {more, _} ->
            h1_loop(Mode, Data#conn_data{recv_buffer = Combined});
        {error, Reason} ->
            fail_h1(Mode, {http1_error, Reason}, {protocol_error, Reason}, ReqSt, Data)
    end;
advance_h1(Mode, Combined, Ref, ReqSt, Data) ->
    drive_h1_body(Mode, Combined, Ref, ReqSt, Data).

-spec apply_h1_chunks([nhttp_h1:body_chunk()], #request_st{}) ->
    {cont, #request_st{}}
    | {fin, #request_st{}, nhttp_lib:headers()}
    | {abort, nhttp_lib:error()}.
apply_h1_chunks([], ReqSt) ->
    {cont, ReqSt};
apply_h1_chunks([{data, Bin} | Rest], #request_st{body_acc = Acc, body_size = Size} = ReqSt) ->
    apply_h1_chunks(
        Rest, ReqSt#request_st{body_acc = [Bin | Acc], body_size = Size + byte_size(Bin)}
    );
apply_h1_chunks([{fin, Trailers} | _], ReqSt) ->
    {fin, ReqSt, Trailers};
apply_h1_chunks([{abort, Error} | _], _ReqSt) ->
    {abort, Error}.

-spec build_response_body(nhttp_lib:response(), binary(), nhttp_lib:headers()) ->
    nhttp_lib:response().
build_response_body(Resp, Body, []) ->
    Resp#{body => Body};
build_response_body(Resp, Body, Trailers) ->
    Resp#{body => Body, trailers => Trailers}.

-spec complete_h1(h1_mode(), nhttp_lib:response(), #conn_data{}) -> no_return().
complete_h1(ready, Resp, Data) ->
    complete_h1_request_ready(Resp, Data);
complete_h1(draining, Resp, Data) ->
    complete_h1_request_draining(Resp, Data).

-spec complete_h1_request_draining(nhttp_lib:response(), #conn_data{}) -> no_return().
complete_h1_request_draining(
    Resp,
    #conn_data{
        current_request = Ref,
        requests = Requests
    } = Data
) ->
    Data1 =
        case maps:get(Ref, Requests, undefined) of
            undefined -> Data;
            ReqSt -> serve_response(ReqSt, Resp, Data)
        end,
    _ = complete_request(Ref, undefined, Data1),
    exit(normal).

-spec complete_h1_request_ready(nhttp_lib:response(), #conn_data{}) -> no_return().
complete_h1_request_ready(
    Resp,
    #conn_data{
        current_request = Ref,
        requests = Requests
    } = Data
) ->
    Data0 =
        case maps:get(Ref, Requests, undefined) of
            undefined -> Data;
            ReqSt -> serve_response(ReqSt, Resp, Data)
        end,
    Data1 = complete_request(Ref, undefined, Data0),
    Data2 = touch_activity(Data1),
    KeepAliveTimeout = parse_keep_alive_timeout(maps:get(headers, Resp, [])),
    Data3 = Data2#conn_data{
        current_request = undefined,
        keep_alive_timeout = KeepAliveTimeout
    },
    maybe_next_request_ready(Data3).

-spec current_h1_request(#conn_data{}) -> {reference(), #request_st{}} | undefined.
current_h1_request(#conn_data{current_request = undefined}) ->
    undefined;
current_h1_request(#conn_data{current_request = Ref, requests = Requests}) ->
    case maps:get(Ref, Requests, undefined) of
        undefined -> undefined;
        ReqSt -> {Ref, ReqSt}
    end.

-spec deliver_h1(h1_mode(), #request_st{}, nhttp_lib:headers(), #conn_data{}) -> no_return().
deliver_h1(Mode, #request_st{response = Resp0, body_acc = Acc}, Trailers, Data) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    Resp = build_response_body(Resp0, Body, Trailers),
    complete_h1(Mode, Resp, Data).

-spec deliver_h1_eof(h1_mode(), #request_st{}, #conn_data{}) -> no_return().
deliver_h1_eof(Mode, #request_st{body_stream = Stream} = ReqSt, Data) ->
    case nhttp_h1:finalize_response_body(Stream) of
        {ok, Chunks} ->
            case apply_h1_chunks(Chunks, ReqSt) of
                {fin, #request_st{response = Resp0, body_acc = Acc} = ReqSt1, Trailers} ->
                    Body = iolist_to_binary(lists:reverse(Acc)),
                    Resp = build_response_body(Resp0, Body, Trailers),
                    Data1 = serve_response(ReqSt1, Resp, Data),
                    notify_pool_error(Data1, {connection_closed, graceful}),
                    exit(normal);
                _ ->
                    fail_h1(
                        Mode,
                        {http1_error, unexpected_eof},
                        {protocol_error, unexpected_eof},
                        ReqSt,
                        Data
                    )
            end;
        {error, Reason} ->
            fail_h1(Mode, {http1_error, Reason}, {protocol_error, Reason}, ReqSt, Data)
    end.

-spec drive_h1_body(h1_mode(), binary(), reference(), #request_st{}, #conn_data{}) -> no_return().
drive_h1_body(Mode, Bin, Ref, #request_st{body_stream = Stream} = ReqSt, Data) ->
    case nhttp_h1:parse_response_body(Bin, Stream) of
        {ok, Chunks, NewStream, Consumed} ->
            Rest = nhttp_h1:split_at(Bin, Consumed),
            ReqSt1 = ReqSt#request_st{body_stream = NewStream},
            handle_h1_chunks(Mode, Chunks, Rest, Ref, ReqSt1, Data);
        {more, _Min, NewStream} ->
            ReqSt1 = ReqSt#request_st{body_stream = NewStream},
            Data1 = update_request(Ref, ReqSt1, Data),
            h1_loop(Mode, Data1#conn_data{recv_buffer = Bin});
        {error, Reason} ->
            fail_h1(Mode, {http1_error, Reason}, {protocol_error, Reason}, ReqSt, Data)
    end.

-spec fail_h1(
    h1_mode(),
    nhttpc_error:any_reason(),
    {protocol_error, nhttp_h1:parse_error() | nhttp_lib:error()},
    #request_st{},
    #conn_data{}
) -> no_return().
fail_h1(_Mode, CallerReason, PoolReason, ReqSt, Data) ->
    send_response(ReqSt, {error, CallerReason}, Data#conn_data.self),
    notify_pool_error(Data, PoolReason),
    exit({error, PoolReason}).

-spec fail_h1_too_large(h1_mode(), #request_st{}, #conn_data{}) -> no_return().
fail_h1_too_large(_Mode, #request_st{body_size = Size, max_response_size = Max} = ReqSt, Data) ->
    send_response(ReqSt, nhttp_error:response_too_large(Size, Max), Data#conn_data.self),
    notify_pool_error(Data, {protocol_error, response_too_large}),
    exit(normal).

-spec h1_body_too_large(#request_st{}) -> boolean().
h1_body_too_large(#request_st{max_response_size = infinity}) ->
    false;
h1_body_too_large(#request_st{body_size = Size, max_response_size = Max}) ->
    Size > Max.

-spec h1_loop(h1_mode(), #conn_data{}) -> no_return().
h1_loop(ready, Data) ->
    ready_loop(Data);
h1_loop(draining, Data) ->
    draining_loop(Data).

-spec h1_request_method(#request_st{}) -> nhttp_lib:method().
h1_request_method(#request_st{request = Request}) ->
    maps:get(method, Request, get).

-spec h1_until_close_request(#conn_data{}) -> {reference(), #request_st{}} | none.
h1_until_close_request(#conn_data{protocol = http1_1} = Data) ->
    case current_h1_request(Data) of
        {Ref, #request_st{body_stream = until_close} = ReqSt} -> {Ref, ReqSt};
        _ -> none
    end;
h1_until_close_request(_Data) ->
    none.

-spec handle_h1_chunks(
    h1_mode(), [nhttp_h1:body_chunk()], binary(), reference(), #request_st{}, #conn_data{}
) -> no_return().
handle_h1_chunks(Mode, Chunks, Rest, Ref, ReqSt, Data) ->
    case apply_h1_chunks(Chunks, ReqSt) of
        {cont, ReqSt1} ->
            case h1_body_too_large(ReqSt1) of
                true ->
                    fail_h1_too_large(Mode, ReqSt1, Data);
                false ->
                    Data1 = update_request(Ref, ReqSt1, Data),
                    h1_loop(Mode, Data1#conn_data{recv_buffer = Rest})
            end;
        {fin, ReqSt1, Trailers} ->
            case h1_body_too_large(ReqSt1) of
                true ->
                    fail_h1_too_large(Mode, ReqSt1, Data);
                false ->
                    deliver_h1(Mode, ReqSt1, Trailers, Data#conn_data{recv_buffer = Rest})
            end;
        {abort, Error} ->
            fail_h1(Mode, {http1_error, Error}, {protocol_error, Error}, ReqSt, Data)
    end.

-spec handle_h1_data(h1_mode(), binary(), #conn_data{}) -> no_return().
handle_h1_data(Mode, Bytes, #conn_data{recv_buffer = Buffer} = Data) ->
    Combined = combine_buffer(Buffer, Bytes),
    case current_h1_request(Data) of
        undefined ->
            notify_pool_error(Data, {protocol_error, unsolicited_data}),
            exit({error, {protocol_error, unsolicited_data}});
        {Ref, ReqSt} ->
            advance_h1(Mode, Combined, Ref, ReqSt, Data)
    end.

-spec handle_h1_data_draining(binary(), #conn_data{}) -> no_return().
handle_h1_data_draining(Bytes, Data) ->
    handle_h1_data(draining, Bytes, Data).

-spec handle_h1_data_ready(binary(), #conn_data{}) -> no_return().
handle_h1_data_ready(Bytes, Data) ->
    handle_h1_data(ready, Bytes, Data).

-spec handle_h2_data_draining(binary(), #conn_data{}) -> no_return().
handle_h2_data_draining(Bytes, #conn_data{h2_conn = H2Conn, socket = Socket} = Data) ->
    case nhttp_h2:recv(H2Conn, Bytes) of
        {ok, Events, NewH2Conn} ->
            exit_when_drained(process_h2_events(Events, Data#conn_data{h2_conn = NewH2Conn}));
        {ok, Events, NewH2Conn, ToSend} ->
            case nhttp_sock:send(Socket, ToSend) of
                ok ->
                    exit_when_drained(
                        process_h2_events(Events, Data#conn_data{h2_conn = NewH2Conn})
                    );
                {error, Reason} ->
                    fail_all_requests(Data, {send_error, Reason}),
                    exit({error, Reason})
            end;
        {error, Reason} ->
            fail_all_requests(Data, {http2_error, Reason}),
            exit({error, Reason})
    end.

-spec handle_h2_data_ready(binary(), #conn_data{}) -> no_return().
handle_h2_data_ready(Bytes, #conn_data{h2_conn = H2Conn, socket = Socket} = Data) ->
    case nhttp_h2:recv(H2Conn, Bytes) of
        {ok, Events, NewH2Conn} ->
            Data1 = process_h2_events(Events, Data#conn_data{h2_conn = NewH2Conn}),
            maybe_h2_state_transition_ready(Data1);
        {ok, Events, NewH2Conn, ToSend} ->
            case nhttp_sock:send(Socket, ToSend) of
                ok ->
                    Data1 = process_h2_events(Events, Data#conn_data{h2_conn = NewH2Conn}),
                    maybe_h2_state_transition_ready(Data1);
                {error, Reason} ->
                    fail_all_requests(Data, {send_error, Reason}),
                    exit({error, Reason})
            end;
        {error, Reason} ->
            fail_all_requests(Data, {http2_error, Reason}),
            exit({error, Reason})
    end.

-spec handle_socket_data_draining(binary(), #conn_data{}) -> no_return().
handle_socket_data_draining(Bytes, #conn_data{protocol = http1_1} = Data) ->
    handle_h1_data_draining(Bytes, Data);
handle_socket_data_draining(Bytes, #conn_data{protocol = http2} = Data) ->
    handle_h2_data_draining(Bytes, Data).

-spec handle_socket_data_ready(binary(), #conn_data{}) -> no_return().
handle_socket_data_ready(Bytes, #conn_data{protocol = http1_1} = Data) ->
    handle_h1_data_ready(Bytes, Data);
handle_socket_data_ready(Bytes, #conn_data{protocol = http2} = Data) ->
    handle_h2_data_ready(Bytes, Data).

-spec maybe_h2_state_transition_ready(#conn_data{}) -> no_return().
maybe_h2_state_transition_ready(#conn_data{goaway_received = true} = Data) ->
    exit_when_drained(Data);
maybe_h2_state_transition_ready(#conn_data{rotating = true} = Data) ->
    exit_when_drained(Data);
maybe_h2_state_transition_ready(#conn_data{requests = Requests} = Data) ->
    case maps:size(Requests) of
        0 ->
            ok = resync_send_credit(Data),
            Data1 = start_idle_checker(touch_activity(Data)),
            ready_loop(Data1);
        _ ->
            ready_loop(Data)
    end.

-spec maybe_next_request_ready(#conn_data{}) -> no_return().
maybe_next_request_ready(Data) ->
    Data1 = start_idle_checker(Data),
    ready_loop(Data1).

-doc """
Apply one codec event. Every member of `t:nhttp_h2:event/0` has a
clause, so an event the codec adds fails here instead of vanishing.
The clauses that change nothing say why: a stream-level `window_update`
is credit the codec already spent inside `recv/2`, whose DATA frames
were written before the events were read. `settings_ack` and `ping` are
answered by the codec in the same iodata. `stream_closed` and
`stream_refused` describe streams the peer opened, which a client has
none of, as does `request`.
""".
-spec process_h2_event(nhttp_h2:event(), #conn_data{}) -> #conn_data{}.
process_h2_event({request, _StreamId, _Request, _EndStream}, Data) ->
    Data;
process_h2_event({response, StreamId, Response, EndStream}, Data) ->
    case find_request_by_stream(StreamId, Data) of
        {ok, Ref, ReqSt} ->
            case EndStream of
                fin ->
                    Data1 = serve_response(ReqSt, Response#{body => <<>>}, Data),
                    complete_request(Ref, StreamId, Data1);
                nofin ->
                    NewReqSt = ReqSt#request_st{response = Response},
                    update_request(Ref, NewReqSt, Data)
            end;
        error ->
            Data
    end;
process_h2_event({trailers, StreamId, Trailers}, Data) ->
    case find_request_by_stream(StreamId, Data) of
        {ok, Ref, ReqSt} ->
            Body = iolist_to_binary(lists:reverse(ReqSt#request_st.body_acc)),
            Response = ReqSt#request_st.response,
            Data1 = serve_response(ReqSt, Response#{body => Body, trailers => Trailers}, Data),
            complete_request(Ref, StreamId, Data1);
        error ->
            Data
    end;
process_h2_event({data, StreamId, BodyData, EndStream}, Data) ->
    DataLen = byte_size(BodyData),
    Data1 = credit_peer(StreamId, DataLen, EndStream, Data),
    case find_request_by_stream(StreamId, Data1) of
        {ok, Ref, ReqSt} ->
            NewBodySize = ReqSt#request_st.body_size + DataLen,
            #request_st{max_response_size = MaxSize} = ReqSt,
            case NewBodySize > MaxSize of
                true ->
                    abort_h2_stream_too_large(
                        Ref, StreamId, ReqSt, NewBodySize, MaxSize, Data1
                    );
                false ->
                    NewBodyAcc = [BodyData | ReqSt#request_st.body_acc],
                    case EndStream of
                        fin ->
                            Body = iolist_to_binary(lists:reverse(NewBodyAcc)),
                            Response = ReqSt#request_st.response,
                            Data2 = serve_response(ReqSt, Response#{body => Body}, Data1),
                            complete_request(Ref, StreamId, Data2);
                        nofin ->
                            NewReqSt = ReqSt#request_st{
                                body_acc = NewBodyAcc, body_size = NewBodySize
                            },
                            update_request(Ref, NewReqSt, Data1)
                    end
            end;
        error ->
            Data1
    end;
process_h2_event({stream_reset, StreamId, ErrorCode}, Data) ->
    case find_request_by_stream(StreamId, Data) of
        {ok, Ref, ReqSt} ->
            send_response(ReqSt, {error, stream_reset_to_error(ErrorCode)}, Data#conn_data.self),
            complete_request(Ref, StreamId, Data);
        error ->
            Data
    end;
process_h2_event({goaway, LastStreamId, ErrorCode, DebugData}, Data) ->
    notify_pool_draining(Data),
    emit_goaway(Data, LastStreamId, ErrorCode, DebugData),
    Self = Data#conn_data.self,
    Refused = maps:fold(
        fun(Ref, #request_st{stream_id = StreamId} = ReqSt, Acc) ->
            case StreamId =:= undefined orelse StreamId =< LastStreamId of
                true ->
                    Acc;
                false ->
                    Reason = nhttp_error:goaway(ErrorCode, DebugData, retryable),
                    send_response(ReqSt, Reason, Self),
                    [{Ref, StreamId} | Acc]
            end
        end,
        [],
        Data#conn_data.requests
    ),
    Data1 = lists:foldl(
        fun({Ref, StreamId}, Acc) -> complete_request(Ref, StreamId, Acc) end,
        Data,
        Refused
    ),
    ok = stop_admitting_streams(Data1),
    Data1#conn_data{goaway_received = true};
process_h2_event({settings, _Settings}, #conn_data{rotating = true} = Data) ->
    Data;
process_h2_event({settings, _Settings}, #conn_data{goaway_received = true} = Data) ->
    Data;
process_h2_event({settings, Settings}, Data) ->
    case peer_max_streams(Settings, undefined) of
        undefined ->
            Data;
        MaxStreams ->
            notify_pool_max_streams(MaxStreams, Data),
            note_max_streams_change(MaxStreams, Data)
    end;
process_h2_event({ping_ack, PingData}, #conn_data{pending_ping = {Pid, Tag, PingData}} = Data) ->
    Pid ! {Tag, pong},
    Data#conn_data{pending_ping = undefined};
process_h2_event({ping_ack, _OtherData}, Data) ->
    Data;
process_h2_event({window_update, ?CONNECTION_STREAM_ID, Increment}, Data) ->
    ok = add_send_credit(Increment, Data),
    record_credit_grant(Increment, Data);
process_h2_event({window_update, _StreamId, _Increment}, Data) ->
    Data;
process_h2_event({data_sent, StreamId, Bytes, EndStream}, Data) ->
    ok = debit_send_credit(Bytes, Data),
    case {EndStream, find_request_by_stream(StreamId, Data)} of
        {fin, {ok, Ref, ReqSt}} ->
            update_request(Ref, ReqSt#request_st{body_pending = false}, Data);
        {fin, error} ->
            Data;
        {nofin, _} ->
            Data
    end;
process_h2_event({stream_closed, _StreamId, _ErrorCode}, Data) ->
    Data;
process_h2_event({stream_refused, _StreamId}, Data) ->
    Data;
process_h2_event(settings_ack, Data) ->
    Data;
process_h2_event({ping, _OpaqueData}, Data) ->
    Data.

-spec process_h2_events([nhttp_h2:event()], #conn_data{}) -> #conn_data{}.
process_h2_events([], Data) ->
    track_send_buffer(Data);
process_h2_events([Event | Rest], Data) ->
    NewData = process_h2_event(Event, Data),
    process_h2_events(Rest, NewData).

%%%-----------------------------------------------------------------------------
%% SOCKET CLOSED HANDLING
%%%-----------------------------------------------------------------------------
-spec handle_socket_closed_draining(#conn_data{}) -> no_return().
handle_socket_closed_draining(Data) ->
    case h1_until_close_request(Data) of
        {_Ref, ReqSt} ->
            deliver_h1_eof(draining, ReqSt, Data);
        none ->
            nhttp_sock:close(Data#conn_data.socket),
            exit(normal)
    end.

-spec handle_socket_closed_ready(#conn_data{}) -> no_return().
handle_socket_closed_ready(Data) ->
    case h1_until_close_request(Data) of
        {_Ref, ReqSt} ->
            deliver_h1_eof(ready, ReqSt, Data);
        none ->
            case has_pending_requests(Data) of
                false ->
                    notify_pool_error(Data, {connection_closed, graceful}),
                    exit(normal);
                true ->
                    {error, Reason} = nhttp_error:connection_closed(retryable),
                    fail_all_requests(Data, Reason),
                    notify_pool_error(Data, {connection_closed, unexpected}),
                    exit(normal)
            end
    end.

%%%-----------------------------------------------------------------------------
%% IDLE TIMEOUT HANDLING
%%%-----------------------------------------------------------------------------
-spec handle_check_idle_ready(#conn_data{}) -> no_return().
handle_check_idle_ready(
    #conn_data{
        last_activity = LastActivity,
        idle_timeout = IdleTimeout,
        keep_alive_timeout = KeepAliveTimeout,
        self = Self
    } = Data
) ->
    ok = resync_send_credit(Data),
    Now = erlang:monotonic_time(millisecond),
    EffectiveTimeout = effective_idle_timeout(IdleTimeout, KeepAliveTimeout),
    Elapsed = Now - LastActivity,
    case Elapsed >= EffectiveTimeout of
        true ->
            emit_idle_timeout(Data),
            notify_pool_idle_close(Data),
            exit(normal);
        false ->
            CheckInterval = idle_check_interval(EffectiveTimeout),
            TimerRef = erlang:send_after(CheckInterval, Self, check_idle),
            ready_loop(Data#conn_data{idle_check_timer_ref = TimerRef})
    end.

%%%-----------------------------------------------------------------------------
%% CANCEL HANDLING
%%%-----------------------------------------------------------------------------
-spec ack_cancel(pid(), reference()) -> ok.
ack_cancel(From, Ref) ->
    From ! {'$nhttpc_cancel_ack', Ref},
    ok.

-spec do_cancel(reference(), fun((ok | {error, not_found}) -> any()), #conn_data{}) ->
    #conn_data{} | no_return().
do_cancel(Ref, AckFun, #conn_data{requests = Requests} = Data) ->
    case maps:get(Ref, Requests, undefined) of
        undefined ->
            AckFun({error, not_found}),
            Data;
        #request_st{stream_id = StreamId} ->
            Data1 = complete_request(Ref, StreamId, Data),
            case {Data#conn_data.protocol, StreamId} of
                {http2, SId} when SId =/= undefined ->
                    Data2 = reset_h2_stream(SId, Data1),
                    AckFun(ok),
                    Data2;
                {http1_1, _} ->
                    AckFun(ok),
                    nhttp_sock:close(Data1#conn_data.socket),
                    notify_pool_error(Data1, {cancelled, Ref}),
                    exit(normal);
                _ ->
                    AckFun(ok),
                    Data1
            end
    end.

-spec handle_caller_down(reference(), #conn_data{}) -> #conn_data{} | no_return().
handle_caller_down(StreamRef, #conn_data{requests = Requests, protocol = Protocol} = Data) ->
    case maps:get(StreamRef, Requests, undefined) of
        undefined ->
            Data;
        #request_st{stream_id = StreamId} ->
            Data1 = complete_request(StreamRef, StreamId, Data),
            case {Protocol, StreamId} of
                {http2, SId} when SId =/= undefined ->
                    reset_h2_stream(SId, Data1);
                {http1_1, _} ->
                    nhttp_sock:close(Data1#conn_data.socket),
                    notify_pool_error(Data1, {caller_down, StreamRef}),
                    exit(normal);
                _ ->
                    Data1
            end
    end.

-spec handle_cancel_request(reference(), {pid(), reference()}, #conn_data{}) ->
    #conn_data{} | no_return().
handle_cancel_request(Ref, From, Data) ->
    do_cancel(Ref, fun(Result) -> reply(From, Result) end, Data).

-spec handle_unified_cancel(reference(), pid(), #conn_data{}) -> #conn_data{} | no_return().
handle_unified_cancel(Ref, From, Data) ->
    Ack = fun(_Result) -> ack_cancel(From, Ref) end,
    do_cancel(Ref, Ack, Data).

%%%-----------------------------------------------------------------------------
%% PING HANDLING
%%%-----------------------------------------------------------------------------
-spec build_info(#conn_data{}) ->
    {ok, #{version := nhttp_lib:version(), peer := nhttp_lib:peer()}}
    | {error, nhttpc_error:any_reason()}.
build_info(#conn_data{protocol = Protocol, socket = Socket}) when Socket =/= undefined ->
    case nhttp_sock:peername(Socket) of
        {ok, Peer} -> {ok, #{version => Protocol, peer => Peer}};
        {error, _} = Error -> Error
    end;
build_info(#conn_data{}) ->
    {error, not_ready}.

-spec do_h2_ping({pid(), reference()}, #conn_data{}) -> #conn_data{}.
do_h2_ping({Pid, Tag}, #conn_data{h2_conn = H2Conn, socket = Socket} = Data) ->
    OpaqueData = crypto:strong_rand_bytes(8),
    {ok, NewH2Conn, Frame} = nhttp_h2:send_ping(H2Conn, OpaqueData),
    case nhttp_sock:send(Socket, Frame) of
        ok ->
            Data#conn_data{
                h2_conn = NewH2Conn,
                pending_ping = {Pid, Tag, OpaqueData}
            };
        {error, Reason} ->
            Pid ! {Tag, {error, {send_error, Reason}}},
            Data
    end.

-spec handle_ping({pid(), reference()}, #conn_data{}) -> #conn_data{}.
handle_ping(From, #conn_data{protocol = http1_1, socket = Socket} = Data) when
    Socket =/= undefined
->
    case nhttp_sock:peername(Socket) of
        {ok, _} ->
            reply(From, pong),
            Data;
        {error, Reason} ->
            reply(From, {error, Reason}),
            Data
    end;
handle_ping(
    {Pid, Tag} = _From,
    #conn_data{protocol = http2, pending_ping = {ExistingPid, ExistingTag, _}} = Data
) ->
    ExistingPid ! {ExistingTag, {error, ping_overwritten}},
    do_h2_ping({Pid, Tag}, Data#conn_data{pending_ping = undefined});
handle_ping(From, #conn_data{protocol = http2} = Data) ->
    do_h2_ping(From, Data).

%%%-----------------------------------------------------------------------------
%% HELPER FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec abort_h2_stream_too_large(
    reference(),
    nhttp_lib:stream_id(),
    #request_st{},
    non_neg_integer(),
    pos_integer() | infinity,
    #conn_data{}
) -> #conn_data{}.
abort_h2_stream_too_large(Ref, StreamId, ReqSt, BodySize, MaxSize, Data) ->
    Self = Data#conn_data.self,
    send_response(ReqSt, nhttp_error:response_too_large(BodySize, MaxSize), Self),
    complete_request(Ref, StreamId, reset_h2_stream(StreamId, Data)).

-doc """
Add the credit one connection-level WINDOW_UPDATE granted to the slot the
pool reserves from. The codec already applied the same increment to its
own window, so the two stay equal up to the bodies reserved and not yet
encoded.
""".
-spec add_send_credit(pos_integer(), #conn_data{}) -> ok.
add_send_credit(Increment, #conn_data{protocol = http2, stream_slots = Slots}) when
    Slots =/= undefined
->
    nhttpc_stream_slots:add_credit(Slots, Increment);
add_send_credit(_Increment, _Data) ->
    ok.

-spec combine_buffer(binary(), binary()) -> binary().
combine_buffer(<<>>, Bytes) ->
    Bytes;
combine_buffer(Buffer, Bytes) ->
    <<Buffer/binary, Bytes/binary>>.

-spec complete_request(reference(), nhttp_lib:stream_id() | undefined, #conn_data{}) ->
    #conn_data{}.
complete_request(Ref, undefined, #conn_data{requests = Requests} = Data) ->
    Data1 = demonitor_caller(Ref, Data),
    release_stream_slot(Data1),
    Data1#conn_data{requests = maps:remove(Ref, Requests)};
complete_request(
    Ref,
    StreamId,
    #conn_data{requests = Requests, stream_index = StreamIndex, h2_window = Window} = Data
) ->
    Data1 = demonitor_caller(Ref, Data),
    release_stream_slot(Data1),
    Data1#conn_data{
        requests = maps:remove(Ref, Requests),
        stream_index = maps:remove(StreamId, StreamIndex),
        h2_window = nhttpc_h2_window:forget(StreamId, Window)
    }.

-spec count_credit_grant(pos_integer(), integer(), #credit_gauge{}) -> #credit_gauge{}.
count_credit_grant(Increment, Now, #credit_gauge{last_at = undefined} = Gauge) ->
    Gauge#credit_gauge{
        count = 1, octets = Increment, last_at = Now, reported_at = report_epoch(Gauge, Now)
    };
count_credit_grant(Increment, Now, #credit_gauge{last_at = LastAt} = Gauge) ->
    Gap = Now - LastAt,
    Gauge#credit_gauge{
        count = Gauge#credit_gauge.count + 1,
        octets = Gauge#credit_gauge.octets + Increment,
        last_at = Now,
        gaps = Gauge#credit_gauge.gaps + 1,
        gap_sum = Gauge#credit_gauge.gap_sum + Gap,
        gap_max = max(Gauge#credit_gauge.gap_max, Gap)
    }.

-doc """
Take the octets the codec spent on a queued body, or on a body sent past
its claim, out of the credit slot. The peer's WINDOW_UPDATE went into
the slot in full, and no checkout reserved these octets.
""".
-spec debit_send_credit(non_neg_integer(), #conn_data{}) -> ok.
debit_send_credit(Octets, #conn_data{protocol = http2, stream_slots = Slots}) when
    Slots =/= undefined, Octets > 0
->
    nhttpc_stream_slots:debit_credit(Slots, Octets);
debit_send_credit(_Octets, _Data) ->
    ok.

-spec demonitor_caller(reference(), #conn_data{}) -> #conn_data{}.
demonitor_caller(Ref, #conn_data{requests = Requests, caller_monitors = Mons} = Data) ->
    case maps:get(Ref, Requests, undefined) of
        #request_st{caller_mref = MRef} when MRef =/= undefined ->
            erlang:demonitor(MRef, [flush]),
            Data#conn_data{caller_monitors = maps:remove(MRef, Mons)};
        _ ->
            Data
    end.

-spec fail_all_requests(#conn_data{}, term()) -> ok.
fail_all_requests(#conn_data{self = Self, requests = Requests}, Reason) ->
    maps:foreach(
        fun(_Ref, ReqSt) ->
            send_response(ReqSt, {error, Reason}, Self)
        end,
        Requests
    ),
    ok.

-spec find_request_by_stream(nhttp_lib:stream_id(), #conn_data{}) ->
    {ok, reference(), #request_st{}} | error.
find_request_by_stream(StreamId, #conn_data{stream_index = StreamIndex, requests = Requests}) ->
    case maps:get(StreamId, StreamIndex, undefined) of
        undefined ->
            error;
        Ref ->
            case maps:get(Ref, Requests, undefined) of
                undefined -> error;
                ReqSt -> {ok, Ref, ReqSt}
            end
    end.

-spec has_pending_requests(#conn_data{}) -> boolean().
has_pending_requests(#conn_data{protocol = http1_1, current_request = Current}) ->
    Current =/= undefined;
has_pending_requests(#conn_data{protocol = http2, requests = M}) ->
    map_size(M) > 0;
has_pending_requests(_) ->
    false.

-doc """
Sample the send queue gauges from the request timeout tick while the
send queue is non-empty, once a second at most. A connection whose peer
withholds credit sees no WINDOW_UPDATE and so no credit report, and the
tick is the one periodic point it still runs, because a queued body is
a request in flight.
""".
-spec maybe_report_send_queue(#conn_data{}) -> #conn_data{}.
maybe_report_send_queue(#conn_data{telemetry_config = false} = Data) ->
    Data;
maybe_report_send_queue(#conn_data{send_blocked_since = undefined} = Data) ->
    Data;
maybe_report_send_queue(#conn_data{send_queue_reported_at = ReportedAt} = Data) ->
    Now = erlang:monotonic_time(millisecond),
    case send_queue_report_due(ReportedAt, Now) of
        true -> report_send_queue(Data);
        false -> Data
    end.

-doc """
Record the concurrent stream cap a SETTINGS frame carried, and report
the move when it differs from the cap the connection ran at. The cap
before the first move is the one the connection reported ready with,
resolved the same way the pool resolved it.
""".
-spec note_max_streams_change(non_neg_integer(), #conn_data{}) -> #conn_data{}.
note_max_streams_change(
    MaxStreams, #conn_data{h2_max_streams = Known, protocol = Protocol} = Data
) ->
    case nhttpc_stream_slots:resolve_cap(Known, Protocol) of
        MaxStreams -> ok;
        Previous -> emit_max_streams_changed(Data, Previous, MaxStreams)
    end,
    Data#conn_data{h2_max_streams = MaxStreams}.

-spec reactivate_socket(#conn_data{}) -> ok | {error, nhttpc_error:any_reason()}.
reactivate_socket(#conn_data{socket = Socket, protocol = http2}) ->
    nhttp_sock:setopts(Socket, [{active, ?H2_ACTIVE_N}]);
reactivate_socket(#conn_data{socket = Socket}) ->
    nhttp_sock:setopts(Socket, [{active, ?H1_ACTIVE_N}]).

-doc """
Count one connection-level WINDOW_UPDATE towards the credit report, and
emit the report once a second at most. The peer's credit rate is the one
input to the sizing rule for a pool, so it is measured continuously
rather than once.
""".
-spec record_credit_grant(pos_integer(), #conn_data{}) -> #conn_data{}.
record_credit_grant(Increment, #conn_data{credit_gauge = Gauge} = Data) ->
    Now = erlang:monotonic_time(microsecond),
    Gauge1 = count_credit_grant(Increment, Now, Gauge),
    case Now - report_epoch(Gauge1, Now) >= ?CREDIT_REPORT_INTERVAL_US of
        true ->
            emit_credit_report(Data, Gauge1),
            report_send_queue(Data#conn_data{
                credit_gauge = #credit_gauge{last_at = Now, reported_at = Now}
            });
        false ->
            Data#conn_data{credit_gauge = Gauge1}
    end.

-doc """
Give the credit a refused request reserved back to the pool. The pool
subtracted the body size at checkout and the codec never debited it, so
without the refund every refusal leaks that many octets of send credit
for the life of the connection.
""".
-spec refund_send_credit(non_neg_integer(), #conn_data{}) -> ok.
refund_send_credit(BodySize, #conn_data{protocol = http2, stream_slots = Slots}) when
    Slots =/= undefined, BodySize > 0
->
    nhttpc_stream_slots:refund_credit(Slots, BodySize);
refund_send_credit(_BodySize, _Data) ->
    ok.

-doc """
Answer a send that never became a stream. The pool reserved a live stream
slot for it before the message left the caller, and `complete_request/3`
never runs for a request that was never registered, so the slot has to go
back here.
""".
-spec reject_send(reference(), pid(), nhttpc_error:any_reason(), non_neg_integer(), #conn_data{}) ->
    ok.
reject_send(StreamRef, CallerPid, Reason, BodySize, #conn_data{self = Self} = Data) ->
    CallerPid ! {nhttpc_error, Self, StreamRef, Reason},
    ok = refund_send_credit(BodySize, Data),
    release_stream_slot(Data).

-doc """
Give one live stream slot back to the pool. Only HTTP/2 reserves one:
HTTP/1.1 checkout is exclusive and counts in `#conn_info.active_count`,
so a release for it would drive the live count below zero.
""".
-spec release_stream_slot(#conn_data{}) -> ok.
release_stream_slot(#conn_data{protocol = http2, stream_slots = Slots}) when Slots =/= undefined ->
    nhttpc_stream_slots:release(Slots);
release_stream_slot(_Data) ->
    ok.

-spec report_epoch(#credit_gauge{}, integer()) -> integer().
report_epoch(#credit_gauge{reported_at = undefined}, Now) -> Now;
report_epoch(#credit_gauge{reported_at = ReportedAt}, _Now) -> ReportedAt.

-doc """
Emit the send queue gauges now and stamp the sample time. The credit
report calls it on its own once-a-second schedule and the timeout tick
calls it through `maybe_report_send_queue/1`.
""".
-spec report_send_queue(#conn_data{}) -> #conn_data{}.
report_send_queue(#conn_data{telemetry_config = false} = Data) ->
    Data;
report_send_queue(Data) ->
    Now = erlang:monotonic_time(millisecond),
    emit_send_queue(Data, send_gauges(Data, Now)),
    Data#conn_data{send_queue_reported_at = Now}.

-doc """
Reset one HTTP/2 stream with CANCEL. The codec purges the octets the
send queue held for the stream, so the queue can empty here, and the
send queue tracking runs on the result.
""".
-spec reset_h2_stream(nhttp_lib:stream_id(), #conn_data{}) -> #conn_data{}.
reset_h2_stream(StreamId, #conn_data{h2_conn = H2Conn, socket = Socket} = Data) ->
    {ok, NewH2Conn, RstFrame} = nhttp_h2:send_rst_stream(H2Conn, StreamId, cancel),
    ok = rst_written(nhttp_sock:send(Socket, RstFrame)),
    track_send_buffer(Data#conn_data{h2_conn = NewH2Conn}).

-doc """
Rewrite the credit slot from the codec window while the connection is
idle. With no request in flight, no live stream slot and no message
waiting, every body the pool reserved has been encoded or refunded, so
the two values are equal and the write corrects any drift a caller left
by dying between checkout and send. A live slot with no request behind
it is a caller between its reservation and its send, and a non-empty
mailbox can hold that send: in both cases the slot is short by a claim
the codec has not seen, and a write from the codec window would hand
that claim out a second time. The write waits for the next idle moment
instead.
""".
-spec resync_send_credit(#conn_data{}) -> ok.
resync_send_credit(#conn_data{protocol = http2, stream_slots = Slots, h2_conn = H2Conn} = Data) when
    Slots =/= undefined
->
    case {reserved_streams(Data), erlang:process_info(self(), message_queue_len)} of
        {0, {message_queue_len, 0}} ->
            nhttpc_stream_slots:set_credit(Slots, nhttp_h2:connection_send_window(H2Conn));
        {_Reserved, {message_queue_len, _Pending}} ->
            ok
    end;
resync_send_credit(_Data) ->
    ok.

-doc """
The result of an RST_STREAM write. A failed write is not acted on here:
the socket reports its fault to the loop as a close or error message,
and that path fails every request and ends the connection.
""".
-spec rst_written(ok | {error, nhttp_sock:socket_error()}) -> ok.
rst_written(ok) -> ok;
rst_written({error, _Reason}) -> ok.

-spec send_gauges(#conn_data{}, integer()) -> nhttpc_telemetry:client_send_gauges().
send_gauges(#conn_data{h2_conn = H2Conn} = Data, Now) ->
    #{
        send_window => nhttp_h2:connection_send_window(H2Conn),
        send_buffer_bytes => nhttp_h2:send_buffer_bytes(H2Conn),
        queued_streams => nhttp_h2:queued_streams(H2Conn),
        send_blocked_ms => total_blocked_ms(Data, Now)
    }.

-spec send_queue_report_due(integer() | undefined, integer()) -> boolean().
send_queue_report_due(undefined, _Now) ->
    true;
send_queue_report_due(ReportedAt, Now) ->
    Now - ReportedAt >= ?SEND_QUEUE_REPORT_INTERVAL_MS.

-spec send_response(
    #request_st{}, {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}, pid()
) -> ok.
send_response(
    #request_st{stream_ref = StreamRef, caller_pid = CallerPid}, {ok, _} = Result, Self
) ->
    CallerPid ! {nhttpc_response, Self, StreamRef, Result},
    ok;
send_response(#request_st{stream_ref = StreamRef, caller_pid = CallerPid}, {error, Reason}, Self) ->
    CallerPid ! {nhttpc_error, Self, StreamRef, Reason},
    ok.

-doc """
Deliver a complete response to its caller. The first one on this
connection also tells the pool, which resets its reconnect backoff on
proof of service rather than on a completed handshake. Every later
response costs one match on the `served` flag and nothing else.
""".
-spec serve_response(#request_st{}, nhttp_lib:response(), #conn_data{}) -> #conn_data{}.
serve_response(ReqSt, Resp, #conn_data{served = true, self = Self} = Data) ->
    ok = send_response(ReqSt, {ok, Resp}, Self),
    Data;
serve_response(ReqSt, Resp, #conn_data{served = false, self = Self} = Data) ->
    ok = send_response(ReqSt, {ok, Resp}, Self),
    ok = notify_pool_served(Data),
    Data#conn_data{served = true}.

-spec total_blocked_ms(#conn_data{}, integer()) -> non_neg_integer().
total_blocked_ms(#conn_data{send_blocked_ms = Total, send_blocked_since = undefined}, _Now) ->
    Total;
total_blocked_ms(#conn_data{send_blocked_ms = Total, send_blocked_since = Since}, Now) ->
    Total + (Now - Since).

-spec touch_activity(#conn_data{}) -> #conn_data{}.
touch_activity(Data) ->
    Data#conn_data{last_activity = erlang:monotonic_time(millisecond)}.

-doc """
Report the send queue transitions between empty and non-empty. The codec
reports no such transition: `send_data/4` answers `{queued, ...}` when it
holds octets and `recv/2` emits them as credit arrives, so the connection
reads the buffer occupancy once after every batch it encoded, every
socket read it processed and every RST_STREAM it sent, and compares it
with the period it has open. The read is one record field, and it runs
only with telemetry configured.
""".
-spec track_send_buffer(#conn_data{}) -> #conn_data{}.
track_send_buffer(#conn_data{telemetry_config = false} = Data) ->
    Data;
track_send_buffer(
    #conn_data{protocol = http2, h2_conn = H2Conn, send_blocked_since = Since} = Data
) ->
    case {nhttp_h2:send_buffer_bytes(H2Conn) > 0, Since =:= undefined} of
        {true, true} ->
            Now = erlang:monotonic_time(millisecond),
            Data1 = Data#conn_data{send_blocked_since = Now, send_queue_reported_at = Now},
            emit_send_blocked(Data1, send_gauges(Data1, Now)),
            Data1;
        {false, false} ->
            Now = erlang:monotonic_time(millisecond),
            BlockedMs = Now - Since,
            Data1 = Data#conn_data{
                send_blocked_since = undefined,
                send_blocked_ms = Data#conn_data.send_blocked_ms + BlockedMs,
                send_queue_reported_at = Now
            },
            emit_send_resumed(Data1, send_gauges(Data1, Now), BlockedMs),
            Data1;
        {true, false} ->
            Data;
        {false, true} ->
            Data
    end;
track_send_buffer(Data) ->
    Data.

-spec update_request(reference(), #request_st{}, #conn_data{}) -> #conn_data{}.
update_request(Ref, ReqSt, #conn_data{requests = Requests} = Data) ->
    Data#conn_data{requests = Requests#{Ref => ReqSt}}.

%%%-----------------------------------------------------------------------------
%% HTTP/1.1 HELPERS
%%%-----------------------------------------------------------------------------
-spec add_connection_keepalive(nhttp_lib:request()) -> nhttp_lib:request().
add_connection_keepalive(#{headers := Headers} = Req) ->
    case nhttp_headers:has(<<"connection">>, Headers) of
        true ->
            Req;
        false ->
            Req#{headers => [{<<"connection">>, <<"keep-alive">>} | Headers]}
    end.

-spec add_host_header(nhttp_lib:request(), #conn_data{}) -> nhttp_lib:request().
add_host_header(#{headers := Headers} = Req, #conn_data{h1_host_header = HostValue}) ->
    case nhttp_headers:has(<<"host">>, Headers) of
        true ->
            Req;
        false ->
            Req#{headers => [{<<"host">>, HostValue} | Headers]}
    end.

%%%-----------------------------------------------------------------------------
%% HTTP/2 INITIALIZATION
%%%-----------------------------------------------------------------------------
-doc """
The local settings of the HTTP/2 codec: the send queue on, with the
bounds the pool named. The client sends no SETTINGS_INITIAL_WINDOW_SIZE,
so every stream opens with the RFC 9113 default receive window that
`nhttpc_h2_window` batches credit against.
""".
-spec h2_codec_settings(conn_opts()) -> nhttp_h2:settings().
h2_codec_settings(Opts) ->
    Bounds = maps:with([max_send_buffer, max_queued_streams], Opts),
    Bounds#{send_queue => true}.

-spec init_h2_connection(#conn_data{}) -> {ok, #conn_data{}} | {error, nhttpc_error:any_reason()}.
init_h2_connection(#conn_data{socket = Socket, h2_settings = Settings} = Data) ->
    H2Conn0 = nhttp_h2:new(client, Settings),
    Preface = nhttp_h2:preface(H2Conn0),
    case nhttp_sock:send(Socket, Preface) of
        ok ->
            case recv_initial_settings(Socket, H2Conn0, ?H2_SETTINGS_TIMEOUT) of
                {ok, H2Conn1, MaxStreams} ->
                    {ok, Data#conn_data{h2_conn = H2Conn1, h2_max_streams = MaxStreams}};
                {error, _} = Error ->
                    Error
            end;
        {error, Reason} ->
            {error, {send_error, Reason}}
    end.

-doc """
`max_concurrent_streams` from one SETTINGS frame, or `Default` when the
frame omits it. RFC 9113 section 6.5.2 gives the absence the meaning "no
limit", and `infinity` carries the same meaning, so both keep the
client-side ceiling that the pool applies.
""".
-spec peer_max_streams(nhttp_h2:settings(), non_neg_integer() | undefined) ->
    non_neg_integer() | undefined.
peer_max_streams(Settings, Default) ->
    case maps:get(max_concurrent_streams, Settings, undefined) of
        MaxStreams when is_integer(MaxStreams), MaxStreams >= 0 -> MaxStreams;
        _Absent -> Default
    end.

-doc """
Read until the peer's opening SETTINGS frame arrives, and carry its
concurrent stream cap out with the connection. A peer that omits
`max_concurrent_streams` yields `undefined`, which leaves the pool on its
own default.
""".
-spec recv_initial_settings(nhttp_sock:t(), nhttp_h2:conn(), timeout()) ->
    {ok, nhttp_h2:conn(), non_neg_integer() | undefined}
    | {error, nhttpc_error:any_reason()}.
recv_initial_settings(Socket, H2Conn, Timeout) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, RecvData} ->
            case nhttp_h2:recv(H2Conn, RecvData) of
                {ok, Events, NewH2Conn} ->
                    settled_or_continue(Socket, NewH2Conn, Timeout, Events);
                {ok, Events, NewH2Conn, ToSend} ->
                    case nhttp_sock:send(Socket, ToSend) of
                        ok ->
                            settled_or_continue(Socket, NewH2Conn, Timeout, Events);
                        {error, Reason} ->
                            {error, {send_error, Reason}}
                    end;
                {error, Reason} ->
                    {error, {http2_error, Reason}}
            end;
        {error, Reason} ->
            {error, {recv_error, Reason}}
    end.

-doc """
Walk one batch of decoded events once, and report both whether the peer's
opening SETTINGS frame arrived and the concurrent stream cap it carried.
The cap is the last one in the batch: a peer is allowed to send more than
one SETTINGS frame in a single segment, and the later frame wins.
""".
-spec scan_settings([nhttp_h2:event()], boolean(), non_neg_integer() | undefined) ->
    {boolean(), non_neg_integer() | undefined}.
scan_settings([], Seen, MaxStreams) ->
    {Seen, MaxStreams};
scan_settings([{settings, Settings} | Rest], _Seen, MaxStreams) ->
    scan_settings(Rest, true, peer_max_streams(Settings, MaxStreams));
scan_settings([_Event | Rest], Seen, MaxStreams) ->
    scan_settings(Rest, Seen, MaxStreams).

-spec settled_or_continue(nhttp_sock:t(), nhttp_h2:conn(), timeout(), [nhttp_h2:event()]) ->
    {ok, nhttp_h2:conn(), non_neg_integer() | undefined}
    | {error, nhttpc_error:any_reason()}.
settled_or_continue(Socket, H2Conn, Timeout, Events) ->
    case scan_settings(Events, false, undefined) of
        {true, MaxStreams} -> {ok, H2Conn, MaxStreams};
        {false, _} -> recv_initial_settings(Socket, H2Conn, Timeout)
    end.

%%%-----------------------------------------------------------------------------
%% FLOW CONTROL
%%%-----------------------------------------------------------------------------
-doc """
Hand the peer the credit due after `Octets` of DATA on `StreamId`.
`nhttpc_h2_window` decides which WINDOW_UPDATE frames are due, and a
write that fails ends the connection: a peer whose credit is lost stalls
on every stream, which no request would survive.
""".
-spec credit_peer(nhttp_lib:stream_id(), non_neg_integer(), nhttp_h2:fin(), #conn_data{}) ->
    #conn_data{}.
credit_peer(StreamId, Octets, EndStream, #conn_data{h2_window = Window} = Data) ->
    case nhttpc_h2_window:consume(StreamId, Octets, EndStream, Window) of
        {[], Window1} ->
            Data#conn_data{h2_window = Window1};
        {Credits, Window1} ->
            send_window_updates(Credits, Data#conn_data{h2_window = Window1})
    end.

-spec send_window_updates([nhttpc_h2_window:credit()], #conn_data{}) -> #conn_data{}.
send_window_updates(Credits, #conn_data{h2_conn = H2Conn0, socket = Socket} = Data) ->
    {H2Conn, Frames} = lists:foldl(fun window_update_frame/2, {H2Conn0, []}, Credits),
    case nhttp_sock:send(Socket, lists:reverse(Frames)) of
        ok ->
            Data#conn_data{h2_conn = H2Conn};
        {error, Reason} ->
            fail_all_requests(Data, {send_error, Reason}),
            exit({error, Reason})
    end.

-doc """
Encode one WINDOW_UPDATE. A stream the codec no longer tracks yields no
frame, because the peer ignores credit on a closed stream. An increment
the codec refuses is credit for octets the peer never sent, which the
window policy rules out, so it ends the connection loudly.
""".
-spec window_update_frame(nhttpc_h2_window:credit(), {nhttp_h2:conn(), [iodata()]}) ->
    {nhttp_h2:conn(), [iodata()]}.
window_update_frame({Target, Increment}, {H2Conn, Frames}) ->
    case nhttp_h2:send_window_update(H2Conn, Target, Increment) of
        {ok, H2Conn1, []} -> {H2Conn1, Frames};
        {ok, H2Conn1, Frame} -> {H2Conn1, [Frame | Frames]};
        {error, Reason} -> exit({error, {window_update, Target, Reason}})
    end.

%%%-----------------------------------------------------------------------------
%% IDLE TIMER MANAGEMENT
%%%-----------------------------------------------------------------------------
-spec effective_idle_timeout(timeout(), timeout() | undefined) -> timeout().
effective_idle_timeout(IdleTimeout, undefined) ->
    IdleTimeout;
effective_idle_timeout(IdleTimeout, KeepAliveTimeout) when is_integer(KeepAliveTimeout) ->
    min(IdleTimeout, KeepAliveTimeout);
effective_idle_timeout(IdleTimeout, _) ->
    IdleTimeout.

-spec idle_check_interval(timeout()) -> pos_integer().
idle_check_interval(Timeout) when is_integer(Timeout) ->
    min(30000, max(5000, Timeout div 2));
idle_check_interval(_) ->
    30000.

-spec start_idle_checker(#conn_data{}) -> #conn_data{}.
start_idle_checker(#conn_data{idle_timeout = infinity} = Data) ->
    Data;
start_idle_checker(#conn_data{idle_check_timer_ref = Ref} = Data) when Ref =/= undefined ->
    Data;
start_idle_checker(
    #conn_data{self = Self, idle_timeout = IdleTimeout, keep_alive_timeout = KeepAliveTimeout} =
        Data
) ->
    EffectiveTimeout = effective_idle_timeout(IdleTimeout, KeepAliveTimeout),
    CheckInterval = idle_check_interval(EffectiveTimeout),
    TimerRef = erlang:send_after(CheckInterval, Self, check_idle),
    Data#conn_data{idle_check_timer_ref = TimerRef}.

%%%-----------------------------------------------------------------------------
%% REQUEST TIMEOUT CHECKER
%%%-----------------------------------------------------------------------------
-spec handle_check_request_timeouts(#conn_data{}) -> #conn_data{}.
handle_check_request_timeouts(#conn_data{requests = Requests} = Data) ->
    Now = erlang:monotonic_time(millisecond),
    {Expired, Active} = maps:fold(
        fun(Ref, #request_st{deadline = Deadline} = ReqSt, {ExpAcc, ActAcc}) ->
            case Deadline =/= undefined andalso Now >= Deadline of
                true ->
                    {[{Ref, ReqSt} | ExpAcc], ActAcc};
                false ->
                    {ExpAcc, ActAcc#{Ref => ReqSt}}
            end
        end,
        {[], #{}},
        Requests
    ),
    Data1 = lists:foldl(
        fun({_Ref, ReqSt}, AccData) ->
            #request_st{stream_id = StreamId} = ReqSt,
            send_response(ReqSt, timeout_reason(ReqSt, AccData), AccData#conn_data.self),
            case {AccData#conn_data.protocol, StreamId} of
                {http2, SId} when SId =/= undefined ->
                    AccData1 = reset_h2_stream(SId, AccData),
                    NewIndex = maps:remove(SId, AccData1#conn_data.stream_index),
                    AccData1#conn_data{stream_index = NewIndex};
                _ ->
                    AccData
            end
        end,
        Data#conn_data{requests = Active},
        Expired
    ),
    case maps:size(Active) of
        0 ->
            stop_request_timeout_checker(Data1);
        _ ->
            TimerRef = erlang:send_after(
                ?REQUEST_TIMEOUT_CHECK_INTERVAL, Data1#conn_data.self, check_request_timeouts
            ),
            maybe_report_send_queue(Data1#conn_data{request_timeout_check_ref = TimerRef})
    end.

-spec start_request_timeout_checker(#conn_data{}) -> #conn_data{}.
start_request_timeout_checker(#conn_data{request_timeout_check_ref = Ref} = Data) when
    Ref =/= undefined
->
    Data;
start_request_timeout_checker(Data) ->
    TimerRef = erlang:send_after(
        ?REQUEST_TIMEOUT_CHECK_INTERVAL, Data#conn_data.self, check_request_timeouts
    ),
    Data#conn_data{request_timeout_check_ref = TimerRef}.

-spec stop_request_timeout_checker(#conn_data{}) -> #conn_data{}.
stop_request_timeout_checker(#conn_data{request_timeout_check_ref = undefined} = Data) ->
    Data;
stop_request_timeout_checker(#conn_data{request_timeout_check_ref = Ref} = Data) ->
    _ = erlang:cancel_timer(Ref),
    Data#conn_data{request_timeout_check_ref = undefined}.

-doc """
The reason a request gets when its deadline passes. A request whose body
is still in the send queue never sent its END_STREAM, and the RST_STREAM
that follows tells the peer to discard the stream, so the peer never
processed it: that is the proof `refused_stream` rests on, and the
request carries that retryable reason. Any other request gets the plain
timeout, because the peer holds a complete request it may have acted on.
""".
-spec timeout_reason(#request_st{}, #conn_data{}) -> {error, nhttpc_error:any_reason()}.
timeout_reason(#request_st{body_pending = true}, _Data) ->
    {error, stream_refused_reason()};
timeout_reason(_ReqSt, #conn_data{request_timeout = RequestTimeout}) ->
    nhttp_error:request_timeout(RequestTimeout).

%%%-----------------------------------------------------------------------------
%% KEEP-ALIVE PARSING
%%%-----------------------------------------------------------------------------
-spec get_keep_alive_header(nhttp_lib:headers()) -> binary() | undefined.
get_keep_alive_header([]) ->
    undefined;
get_keep_alive_header([{<<"keep-alive">>, Value} | _]) ->
    Value;
get_keep_alive_header([_ | Rest]) ->
    get_keep_alive_header(Rest).

-spec parse_keep_alive_parts([binary()]) -> timeout() | undefined.
parse_keep_alive_parts([]) ->
    undefined;
parse_keep_alive_parts([Part | Rest]) ->
    Trimmed = string:trim(Part),
    case binary:split(Trimmed, <<"=">>) of
        [Key, Val] ->
            case nhttp_headers:to_lower(string:trim(Key)) of
                <<"timeout">> ->
                    try
                        Seconds = binary_to_integer(string:trim(Val)),
                        Seconds * 1000
                    catch
                        error:badarg -> parse_keep_alive_parts(Rest)
                    end;
                _ ->
                    parse_keep_alive_parts(Rest)
            end;
        _ ->
            parse_keep_alive_parts(Rest)
    end.

-spec parse_keep_alive_timeout(nhttp_lib:headers()) -> timeout() | undefined.
parse_keep_alive_timeout(Headers) ->
    case get_keep_alive_header(Headers) of
        undefined ->
            undefined;
        Value ->
            parse_keep_alive_value(Value)
    end.

-spec parse_keep_alive_value(binary()) -> timeout() | undefined.
parse_keep_alive_value(Value) ->
    Parts = binary:split(Value, <<",">>, [global]),
    parse_keep_alive_parts(Parts).

%%%-----------------------------------------------------------------------------
%% CONNECTION HELPERS
%%%-----------------------------------------------------------------------------
-spec apply_tls_defaults(tcp | ssl, nhttp_sock:connect_opts()) -> nhttp_sock:connect_opts().
apply_tls_defaults(tcp, Opts) ->
    Opts;
apply_tls_defaults(ssl, Opts) ->
    Opts1 =
        case maps:is_key(verify, Opts) of
            true -> Opts;
            false -> Opts#{verify => verify_peer}
        end,
    case maps:is_key(cacertfile, Opts1) orelse maps:is_key(cacerts, Opts1) of
        true -> Opts1;
        false -> Opts1#{cacerts => public_key:cacerts_get()}
    end.

-spec build_connect_opts(conn_opts()) -> nhttp_sock:connect_opts().
build_connect_opts(Opts) ->
    Transport = maps:get(transport, Opts, tcp),
    Protocols = maps:get(protocols, Opts, [http2, http1_1]),
    AlpnProtocols = protocols_to_alpn(Protocols),
    BaseOpts = #{
        transport => Transport,
        alpn_advertised_protocols => AlpnProtocols
    },
    OptsWithUser = lists:foldl(
        fun(Key, Acc) ->
            case maps:get(Key, Opts, undefined) of
                undefined -> Acc;
                Value -> Acc#{Key => Value}
            end
        end,
        BaseOpts,
        [
            certfile,
            keyfile,
            certs_keys,
            cacertfile,
            cacerts,
            verify,
            server_name_indication,
            wildcard_hostname,
            supported_groups
        ]
    ),
    apply_tls_defaults(Transport, OptsWithUser).

-spec detect_protocol(nhttp_sock:t(), [nhttp_lib:version()]) -> nhttp_lib:version().
detect_protocol(Socket, AllowedProtocols) ->
    case nhttp_sock:negotiated_protocol(Socket) of
        {ok, <<"h2">>} ->
            case lists:member(http2, AllowedProtocols) of
                true -> http2;
                false -> http1_1
            end;
        {ok, <<"http/1.1">>} ->
            http1_1;
        {ok, <<"http/1.0">>} ->
            http1_1;
        {error, no_alpn} ->
            case lists:member(http1_1, AllowedProtocols) of
                true -> http1_1;
                false -> http2
            end;
        _ ->
            http1_1
    end.

-spec normalize_host(binary() | string() | inet:ip_address()) -> binary().
normalize_host(Host) when is_binary(Host) -> Host;
normalize_host(Host) when is_list(Host) -> list_to_binary(Host);
normalize_host(Host) when is_tuple(Host) -> list_to_binary(inet:ntoa(Host)).

-spec protocols_to_alpn([nhttp_lib:version()]) -> [binary()].
protocols_to_alpn(Protocols) ->
    lists:filtermap(
        fun
            (http2) -> {true, <<"h2">>};
            (http1_1) -> {true, <<"http/1.1">>};
            (_) -> false
        end,
        Protocols
    ).

%%%-----------------------------------------------------------------------------
%% PROXY CONNECTION HELPERS
%%%-----------------------------------------------------------------------------
-spec build_proxy_ssl_opts(nhttp_sock:connect_opts()) -> map().
build_proxy_ssl_opts(ConnectOpts) ->
    Keys = [
        verify,
        cacerts,
        cacertfile,
        certfile,
        keyfile,
        server_name_indication,
        alpn_advertised_protocols
    ],
    maps:with(Keys, ConnectOpts).

-spec connect_via_proxy(
    binary(),
    inet:port_number(),
    tcp | ssl,
    nhttpc_proxy:proxy_opts(),
    nhttp_sock:connect_opts(),
    timeout()
) ->
    {ok, nhttp_sock:t()} | {error, nhttpc_error:any_reason()}.
connect_via_proxy(TargetHost, TargetPort, Transport, ProxyOpts, ConnectOpts, Timeout) ->
    SslOpts =
        case Transport of
            ssl -> build_proxy_ssl_opts(ConnectOpts);
            tcp -> #{}
        end,
    case nhttpc_proxy:connect(TargetHost, TargetPort, ProxyOpts, SslOpts, Timeout) of
        {ok, RawSocket} ->
            {ok, {Transport, RawSocket}};
        {error, _} = Error ->
            Error
    end.

%%%-----------------------------------------------------------------------------
%% POOL NOTIFICATIONS
%%%-----------------------------------------------------------------------------
-spec notify_pool_draining(#conn_data{}) -> ok.
notify_pool_draining(#conn_data{pool_mgr = undefined}) ->
    ok;
notify_pool_draining(#conn_data{self = Self, pool_mgr = PoolMgr, conn_ets = ConnEts}) ->
    ets_update_status(ConnEts, Self, draining),
    nhttpc_pool:connection_draining(PoolMgr, Self),
    ok.

-spec notify_pool_error(#conn_data{}, term()) -> ok.
notify_pool_error(#conn_data{pool_mgr = undefined}, _Reason) ->
    ok;
notify_pool_error(#conn_data{self = Self, pool_mgr = PoolMgr}, Reason) ->
    nhttpc_pool:connection_error(PoolMgr, Self, Reason),
    ok.

-spec notify_pool_idle_close(#conn_data{}) -> ok.
notify_pool_idle_close(#conn_data{pool_mgr = undefined}) ->
    ok;
notify_pool_idle_close(#conn_data{self = Self, pool_mgr = PoolMgr}) ->
    nhttpc_pool:connection_error(PoolMgr, Self, idle_timeout),
    ok.

-doc """
Report a mid-connection change of the peer's concurrent stream cap. The
connection owns the cap slot, so the write goes in first and the cast
follows. The cast exists for the waiter sweep: a cap that goes up can
release a queued caller, and only the manager holds that queue.
The write has to reach the lock-free checkout path without the manager
mailbox in between. A shrink that waits behind the manager queue is a
shrink the fast path keeps over-granting against.
""".
-spec notify_pool_max_streams(non_neg_integer(), #conn_data{}) -> ok.
notify_pool_max_streams(_MaxStreams, #conn_data{pool_mgr = undefined}) ->
    ok;
notify_pool_max_streams(MaxStreams, #conn_data{self = Self, pool_mgr = PoolMgr} = Data) ->
    ok = write_stream_cap(MaxStreams, Data),
    nhttpc_pool:update_max_streams(PoolMgr, Self, MaxStreams),
    ok.

-doc """
Report the connection as ready, with the send credit and the cap already
written, in that order. The manager creates the array with a cap of
zero, so no checkout can succeed before the cap lands, and a checkout
that passes the cap finds the peer's connection window in the credit
slot. Both writes happen in the process that learns the peer's values
first.
""".
-spec notify_pool_ready(#conn_data{}) -> ok.
notify_pool_ready(#conn_data{pool_mgr = undefined}) ->
    ok;
notify_pool_ready(
    #conn_data{
        self = Self, pool_mgr = PoolMgr, protocol = Protocol, h2_max_streams = MaxStreams
    } = Data
) ->
    ok = write_send_credit(Data),
    ok = write_stream_cap(nhttpc_stream_slots:resolve_cap(MaxStreams, Protocol), Data),
    nhttpc_pool:connection_ready(PoolMgr, Self, Protocol, MaxStreams),
    ok.

-doc """
Report the first response this connection delivered. The pool reads it
as proof that the origin serves, and resets its reconnect backoff there
and nowhere else.
""".
-spec notify_pool_served(#conn_data{}) -> ok.
notify_pool_served(#conn_data{pool_mgr = undefined}) ->
    ok;
notify_pool_served(#conn_data{self = Self, pool_mgr = PoolMgr}) ->
    nhttpc_pool:connection_served(PoolMgr, Self).

-doc """
Close the lock-free checkout path before the pool hears about the drain.
The cap slot at zero makes every later `nhttpc_stream_slots:reserve/2`
answer `full`, whatever the registry tuple still lists, so the window
between this write and the manager rebuild admits no new caller. A peer
SETTINGS frame that arrives while draining leaves the cap at zero.
""".
-spec stop_admitting_streams(#conn_data{}) -> ok.
stop_admitting_streams(Data) ->
    write_stream_cap(0, Data).

-doc """
Write the peer's connection send window into the credit slot at ready.
The codec window is the source, not the RFC 9113 default, so a
WINDOW_UPDATE that arrived with the opening SETTINGS frame is counted
once. HTTP/1.1 never enters the multiplexed tuple and reserves no credit.
""".
-spec write_send_credit(#conn_data{}) -> ok.
write_send_credit(#conn_data{protocol = http2, stream_slots = Slots, h2_conn = H2Conn}) when
    Slots =/= undefined
->
    nhttpc_stream_slots:set_credit(Slots, nhttp_h2:connection_send_window(H2Conn));
write_send_credit(_Data) ->
    ok.

-spec write_stream_cap(non_neg_integer(), #conn_data{}) -> ok.
write_stream_cap(_Cap, #conn_data{stream_slots = undefined}) ->
    ok;
write_stream_cap(Cap, #conn_data{stream_slots = Slots}) ->
    nhttpc_stream_slots:set_cap(Slots, Cap).

%%%-----------------------------------------------------------------------------
%% POOL SLOT AND STATUS HELPERS
%%%-----------------------------------------------------------------------------
-spec ets_update_status(ets:tid() | undefined, pid(), atom()) -> ok.
ets_update_status(undefined, _Self, _Status) ->
    ok;
ets_update_status(ConnEts, Self, Status) ->
    try ets:update_element(ConnEts, Self, {3, Status}) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

%%%-----------------------------------------------------------------------------
%% TELEMETRY
%%%-----------------------------------------------------------------------------
-spec conn_meta(#conn_data{}) -> nhttpc_telemetry:client_conn_meta().
conn_meta(#conn_data{self = Self, host = Host, port = Port}) ->
    #{host => Host, port => Port, conn_pid => Self}.

-spec emit_connect_error(#conn_data{}, term()) -> ok.
emit_connect_error(#conn_data{telemetry_config = false}, _Reason) ->
    ok;
emit_connect_error(
    #conn_data{
        telemetry_config = Config,
        telemetry_connect_start = StartTime,
        host = Host,
        port = Port
    },
    Reason
) ->
    nhttpc_telemetry:client_connect_error(Config, StartTime, #{
        host => Host, port => Port, reason => Reason
    }).

-spec emit_connect_stop(#conn_data{}) -> ok.
emit_connect_stop(#conn_data{telemetry_config = false}) ->
    ok;
emit_connect_stop(#conn_data{
    telemetry_config = Config,
    telemetry_connect_start = StartTime,
    protocol = Protocol,
    host = Host,
    port = Port
}) ->
    nhttpc_telemetry:client_connect_stop(Config, StartTime, #{
        host => Host, port => Port, protocol => Protocol
    }).

-spec emit_credit_report(#conn_data{}, #credit_gauge{}) -> ok.
emit_credit_report(#conn_data{telemetry_config = false}, _Gauge) ->
    ok;
emit_credit_report(
    #conn_data{telemetry_config = Config, self = Self, host = Host, port = Port},
    #credit_gauge{count = Count, octets = Octets, gaps = Gaps, gap_sum = GapSum, gap_max = GapMax}
) ->
    Intervals = max(1, Gaps),
    nhttpc_telemetry:client_credit(
        Config,
        #{host => Host, port => Port, conn_pid => Self},
        #{
            count => Count,
            octets => Octets,
            mean_interval => GapSum div Intervals,
            max_interval => GapMax
        }
    ).

-spec emit_goaway(#conn_data{}, nhttp_lib:stream_id(), nhttp_lib:error_code(), binary()) -> ok.
emit_goaway(#conn_data{telemetry_config = false}, _LastStreamId, _ErrorCode, _DebugData) ->
    ok;
emit_goaway(
    #conn_data{telemetry_config = Config, self = Self, host = Host, port = Port},
    LastStreamId,
    ErrorCode,
    DebugData
) ->
    nhttpc_telemetry:client_goaway(Config, #{
        host => Host,
        port => Port,
        conn_pid => Self,
        error_code => ErrorCode,
        last_stream_id => LastStreamId,
        debug_data => DebugData
    }).

-spec emit_idle_timeout(#conn_data{}) -> ok.
emit_idle_timeout(#conn_data{telemetry_config = false}) ->
    ok;
emit_idle_timeout(#conn_data{telemetry_config = Config, host = Host, port = Port}) ->
    nhttpc_telemetry:client_idle_timeout(Config, #{host => Host, port => Port}).

-spec emit_max_streams_changed(#conn_data{}, non_neg_integer(), non_neg_integer()) -> ok.
emit_max_streams_changed(#conn_data{telemetry_config = false}, _Previous, _MaxStreams) ->
    ok;
emit_max_streams_changed(
    #conn_data{telemetry_config = Config, self = Self, host = Host, port = Port},
    Previous,
    MaxStreams
) ->
    nhttpc_telemetry:client_max_streams_changed(
        Config, #{host => Host, port => Port, conn_pid => Self}, Previous, MaxStreams
    ).

-spec emit_rotated(#conn_data{}, non_neg_integer()) -> ok.
emit_rotated(#conn_data{telemetry_config = false}, _Streams) ->
    ok;
emit_rotated(#conn_data{telemetry_config = Config, self = Self, host = Host, port = Port}, Streams) ->
    nhttpc_telemetry:client_rotated(
        Config, #{host => Host, port => Port, conn_pid => Self}, Streams
    ).

-spec emit_send_blocked(#conn_data{}, nhttpc_telemetry:client_send_gauges()) -> ok.
emit_send_blocked(#conn_data{telemetry_config = Config} = Data, Gauges) ->
    nhttpc_telemetry:client_send_blocked(Config, conn_meta(Data), Gauges).

-spec emit_send_credit_refused(#conn_data{}, nhttp_lib:stream_id() | undefined, integer()) -> ok.
emit_send_credit_refused(#conn_data{telemetry_config = false}, _StreamId, _SendWindow) ->
    ok;
emit_send_credit_refused(
    #conn_data{telemetry_config = Config, self = Self, host = Host, port = Port},
    StreamId,
    SendWindow
) ->
    nhttpc_telemetry:client_send_credit_refused(
        Config,
        #{host => Host, port => Port, conn_pid => Self, stream_id => StreamId},
        SendWindow
    ).

-spec emit_send_queue(#conn_data{}, nhttpc_telemetry:client_send_gauges()) -> ok.
emit_send_queue(#conn_data{telemetry_config = Config} = Data, Gauges) ->
    nhttpc_telemetry:client_send_queue(Config, conn_meta(Data), Gauges).

-spec emit_send_resumed(#conn_data{}, nhttpc_telemetry:client_send_gauges(), non_neg_integer()) ->
    ok.
emit_send_resumed(#conn_data{telemetry_config = Config} = Data, Gauges, BlockedMs) ->
    nhttpc_telemetry:client_send_resumed(Config, conn_meta(Data), Gauges, BlockedMs).

%%%-----------------------------------------------------------------------------
%% HTTP/2 ERROR HELPERS
%%%-----------------------------------------------------------------------------
-doc """
Count one opened stream, and start the rotation when the count reaches
`max_streams_per_connection`. The count moves only on a stream the
codec opened and the request encoded in full: a refused request reverts
`next_stream_id`, so its identifier is never spent. The pool learns of
the drain before the batch reaches the socket, so a checkout that still
lands here is bounded by the manager's mailbox latency, and
`encode_h2_request/5` refuses it with the retryable reason.
""".
-spec count_opened_stream(#conn_data{}) -> #conn_data{}.
count_opened_stream(
    #conn_data{streams_opened = Opened, max_streams_per_connection = Max} = Data
) ->
    Data1 = Data#conn_data{streams_opened = Opened + 1},
    case Opened + 1 >= Max of
        true -> start_rotation(Data1);
        false -> Data1
    end.

-doc """
The reason a request gets when it reaches a connection that is draining.
An HTTP/2 request was never sent, so it carries the retryable
`refused_stream` shape and the pool re-dispatches it whatever its
method. An HTTP/1.1 connection drains on a `Connection: close`, which
the pool does not re-dispatch, so it keeps the `pool_draining` reason.
""".
-spec draining_refusal_reason(#conn_data{}) -> {nhttp_error:category(), nhttp_error:reason()}.
draining_refusal_reason(#conn_data{protocol = http2}) ->
    stream_refused_reason();
draining_refusal_reason(#conn_data{}) ->
    {error, Reason} = nhttp_error:pool_draining(),
    Reason.

-doc """
The reason a caller sees for a stream `nhttp_h2:open_stream/1` refused.
`max_streams_reached` is the pool granting a slot against a cap that the
peer had already lowered. `connection_closing` is a GOAWAY that arrived
first. Neither puts a HEADERS frame on the wire, so both carry the
retryable shape of a REFUSED_STREAM from the peer.
The clauses cover every reason that the codec returns. A new reason then
fails loudly here, instead of reaching a caller as a terminal error.
""".
-spec open_stream_error(max_streams_reached | connection_closing) ->
    {nhttp_error:category(), nhttp_error:reason()}.
open_stream_error(max_streams_reached) ->
    stream_refused_reason();
open_stream_error(connection_closing) ->
    stream_refused_reason().

-spec start_rotation(#conn_data{}) -> #conn_data{}.
start_rotation(#conn_data{streams_opened = Opened} = Data) ->
    ok = stop_admitting_streams(Data),
    notify_pool_draining(Data),
    emit_rotated(Data, Opened),
    Data#conn_data{rotating = true}.

-doc """
The retryable reason for a request the peer never saw: a stream the codec
refused to open, or a body the connection had no send credit for.
""".
-spec stream_refused_reason() -> {nhttp_error:category(), nhttp_error:reason()}.
stream_refused_reason() ->
    {error, Reason} = nhttp_error:stream_refused(),
    Reason.

-spec stream_reset_to_error(atom()) -> {nhttp_error:category(), nhttp_error:reason()}.
stream_reset_to_error(cancel) ->
    {error, Reason} = nhttp_error:stream_cancelled(),
    Reason;
stream_reset_to_error(refused_stream) ->
    {error, Reason} = nhttp_error:stream_refused(),
    Reason;
stream_reset_to_error(enhance_your_calm) ->
    {error, Reason} = nhttp_error:rate_limited(),
    Reason;
stream_reset_to_error(no_error) ->
    {error, Reason} = nhttp_error:stream_closed(graceful),
    Reason;
stream_reset_to_error(ErrorCode) ->
    {error, Reason} = nhttp_error:stream_reset(ErrorCode),
    Reason.

%%%-----------------------------------------------------------------------------
%% FORMAT STATUS HELPERS
%%%-----------------------------------------------------------------------------
-spec clear_connector_mref(#conn_data{}) -> #conn_data{}.
clear_connector_mref(#conn_data{connector_mref = undefined} = Data) ->
    Data;
clear_connector_mref(#conn_data{connector_mref = MRef} = Data) ->
    erlang:demonitor(MRef, [flush]),
    Data#conn_data{connector_pid = undefined, connector_mref = undefined}.

-spec is_set(undefined | term()) -> boolean().
is_set(undefined) -> false;
is_set(_) -> true.

-spec kill_connector(#conn_data{}) -> ok.
kill_connector(#conn_data{connector_pid = undefined}) ->
    ok;
kill_connector(#conn_data{connector_pid = Pid, connector_mref = MRef}) ->
    exit(Pid, kill),
    case MRef of
        undefined -> ok;
        _ -> erlang:demonitor(MRef, [flush])
    end,
    ok.

-spec sanitize_conn_data(#conn_data{}) -> map().
sanitize_conn_data(#conn_data{} = D) ->
    #{
        host => D#conn_data.host,
        port => D#conn_data.port,
        transport => D#conn_data.transport,
        protocol => D#conn_data.protocol,
        socket => is_set(D#conn_data.socket),
        h2_conn => is_set(D#conn_data.h2_conn),
        in_flight => map_size(D#conn_data.requests),
        recv_buffer_size => byte_size(D#conn_data.recv_buffer),
        goaway_received => D#conn_data.goaway_received,
        rotating => D#conn_data.rotating,
        streams_opened => D#conn_data.streams_opened,
        max_streams_per_connection => D#conn_data.max_streams_per_connection,
        last_activity => D#conn_data.last_activity,
        idle_timeout => D#conn_data.idle_timeout,
        request_timeout => D#conn_data.request_timeout,
        connect_timeout => D#conn_data.connect_timeout,
        protocols => D#conn_data.protocols,
        pending_ping => is_set(D#conn_data.pending_ping),
        ready_waiters => length(D#conn_data.ready_waiters),
        conn_ets => is_set(D#conn_data.conn_ets),
        stream_slots => is_set(D#conn_data.stream_slots),
        connector_pid => is_set(D#conn_data.connector_pid),
        connector_mref => is_set(D#conn_data.connector_mref)
    }.
