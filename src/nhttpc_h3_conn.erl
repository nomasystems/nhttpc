-module(nhttpc_h3_conn).

-moduledoc """
HTTP/3 client connection process (proc_lib, library mode).

Manages a single persistent QUIC connection using nquic library mode.
The connection process owns the QUIC context directly, eliminating
the gen_statem overhead of the default nquic API.

Performance characteristics:
- Zero gen_statem dispatch overhead (library mode)
- Connected UDP socket for kernel-routed recv
- Opportunistic request batching via mailbox drain
- Single timer-scheduling flush per batch
- Socket select-based async recv (completion I/O)
""".

-include("nhttpc.hrl").

-compile(
    {inline, [
        has_pending_requests/1,
        find_request_by_stream/2,
        touch_activity/1,
        drain_sends/2,
        bool_to_fin/1
    ]}
).

%%%-----------------------------------------------------------------------------
%% EXPORTS - API
%%%-----------------------------------------------------------------------------
-export([
    cancel/2,
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
-export_type([conn_opts/0]).
-type conn_opts() :: #{
    pool_mgr => pid(),
    host := binary() | string() | inet:ip_address(),
    port := inet:port_number(),
    protocols => [nhttp_lib:version()],
    connect_timeout => timeout(),
    request_timeout => timeout(),
    idle_timeout => timeout(),
    otel => boolean() | #{traces => boolean(), metrics => boolean()},
    conn_ets => ets:tid(),
    stream_slots => nhttpc_stream_slots:slots()
}.

-type send_item() :: {reference(), nhttp_lib:request(), pid(), nhttpc_conn:send_opts()}.

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(MAX_H3_BATCH, 64).
-define(REQUEST_TIMEOUT_CHECK_INTERVAL, 100).
-define(MAX_HANDSHAKE_DRAIN_PACKETS, 256).
-define(DEFAULT_SETTINGS_WAIT_MS, 5000).
-define(GRO_ACTIVATION_BYTES, 600).
-define(QPACK_MAX_TABLE_CAPACITY, 4096).
-define(QPACK_BLOCKED_STREAMS, 16).

-record(request_st, {
    stream_ref :: reference(),
    caller_pid :: pid(),
    request :: nhttp_lib:request(),
    deadline :: integer() | undefined,
    stream_id :: nhttp_lib:stream_id() | undefined,
    response :: nhttp_lib:response() | undefined,
    body_acc = [] :: [binary()],
    body_size = 0 :: non_neg_integer(),
    max_response_size = ?DEFAULT_MAX_RESPONSE_SIZE :: pos_integer() | infinity,
    max_header_size = ?DEFAULT_MAX_HEADER_SIZE :: pos_integer() | infinity,
    max_headers_count = ?DEFAULT_MAX_HEADERS_COUNT :: pos_integer() | infinity
}).

-record(h3_data, {
    parent :: pid(),
    pool_mgr :: pid() | undefined,
    self :: pid(),
    host :: binary(),
    port :: inet:port_number(),
    ctx :: nquic:ctx() | undefined,
    socket :: nquic_socket:t() | undefined,
    h3_conn :: nhttp_h3:conn() | undefined,
    h3_scheme :: binary(),
    h3_authority :: binary(),
    requests :: #{reference() => #request_st{}},
    stream_index :: #{nhttp_lib:stream_id() => reference()},
    goaway_received :: boolean(),
    connect_timeout :: timeout(),
    request_timeout :: timeout(),
    idle_timeout :: timeout(),
    last_activity :: integer(),
    idle_check_timer_ref :: reference() | undefined,
    request_timeout_check_ref :: reference() | undefined,
    ready_waiters = [] :: [{{pid(), reference()}, integer()}],
    telemetry_config :: nhttpc_telemetry:otel_config(),
    telemetry_connect_start :: integer(),
    conn_ets :: ets:tid() | undefined,
    stream_slots :: nhttpc_stream_slots:slots() | undefined,
    gro_capable = false :: boolean(),
    gro_enabled = false :: boolean(),
    settings_received = false :: boolean()
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec cancel(pid(), reference()) -> ok | {error, not_found}.
cancel(ConnPid, Ref) ->
    call(ConnPid, {cancel, Ref}, 5000).

-spec ping(pid(), timeout()) -> pong | {error, nhttpc_error:any_reason()}.
ping(ConnPid, Timeout) ->
    call(ConnPid, ping, Timeout).

-spec send_async(pid(), nhttp_lib:request(), pid()) -> {ok, reference()}.
send_async(ConnPid, Request, CallerPid) ->
    send_async(ConnPid, Request, CallerPid, #{}).

-spec send_async(pid(), nhttp_lib:request(), pid(), nhttpc_conn:send_opts()) ->
    {ok, reference()}.
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
        #{state := {Phase, #h3_data{} = Data}} ->
            Status#{state := {Phase, sanitize_h3_data(Data)}};
        _ ->
            Status
    end.

-spec init({pid(), conn_opts()}) -> no_return().
init({Parent, Opts}) ->
    process_flag(trap_exit, true),
    Self = self(),
    PoolMgr = maps:get(pool_mgr, Opts, undefined),
    Host = maps:get(host, Opts),
    Port = maps:get(port, Opts),
    ConnectTimeout = maps:get(connect_timeout, Opts, 5000),
    RequestTimeout = maps:get(request_timeout, Opts, 30000),
    IdleTimeout = maps:get(idle_timeout, Opts, 60000),

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
                    transport => udp
                })
        end,

    H3Authority =
        case Port of
            443 -> NormalizedHost;
            _ -> <<NormalizedHost/binary, ":", (integer_to_binary(Port))/binary>>
        end,

    Data = #h3_data{
        parent = Parent,
        pool_mgr = PoolMgr,
        self = Self,
        host = NormalizedHost,
        port = Port,
        ctx = undefined,
        socket = undefined,
        h3_conn = undefined,
        h3_scheme = <<"https">>,
        h3_authority = H3Authority,
        requests = #{},
        stream_index = #{},
        goaway_received = false,
        connect_timeout = ConnectTimeout,
        request_timeout = RequestTimeout,
        idle_timeout = IdleTimeout,
        last_activity = erlang:monotonic_time(millisecond),
        idle_check_timer_ref = undefined,
        request_timeout_check_ref = undefined,
        ready_waiters = [],
        telemetry_config = TelemetryConfig,
        telemetry_connect_start = TelemetryStartTime,
        conn_ets = maps:get(conn_ets, Opts, undefined),
        stream_slots = maps:get(stream_slots, Opts, undefined),
        gro_capable = maps:get(gro, nquic_socket:capabilities())
    },

    proc_lib:init_ack(Parent, {ok, Self}),

    QConnOpts = #{
        tls => #{alpn => [<<"h3">>], verify => verify_none},
        idle_timeout => quic_idle_timeout(IdleTimeout)
    },
    HostStr = binary_to_list(NormalizedHost),

    case nquic:connect(HostStr, Port, QConnOpts#{timeout => ConnectTimeout}) of
        {ok, Ctx0} ->
            HandshakeSocket = nquic_lib:ctx_socket(Ctx0),
            {Ctx1, OldSocket} =
                case nquic_lib:upgrade_to_connected(Ctx0) of
                    {ok, UpgradedCtx} ->
                        {UpgradedCtx, HandshakeSocket};
                    {error, _UpgradeReason} ->
                        {Ctx0, undefined}
                end,
            complete_h3_connection(Ctx1, OldSocket, Data);
        {error, Reason} ->
            emit_connect_error(Data, Reason),
            notify_pool_error(Data, Reason),
            exit({shutdown, Reason})
    end.

-spec system_code_change({atom(), #h3_data{}}, module(), term(), term()) ->
    {ok, {atom(), #h3_data{}}}.
system_code_change(State, _Module, _OldVsn, _Extra) ->
    {ok, State}.

-spec system_continue(pid(), [sys:debug_option()], {atom(), #h3_data{}}) -> no_return().
system_continue(_Parent, _Debug, {ready, Data}) ->
    ready_loop(Data);
system_continue(_Parent, _Debug, {draining, Data}) ->
    draining_loop(Data).

-spec system_get_state({atom(), #h3_data{}}) -> {ok, {atom(), #h3_data{}}}.
system_get_state(State) ->
    {ok, State}.

-spec system_terminate(term(), pid(), [sys:debug_option()], {atom(), #h3_data{}}) -> no_return().
system_terminate(Reason, _Parent, _Debug, {_State, #h3_data{ctx = Ctx}}) ->
    close_ctx(Ctx),
    exit(Reason).

%%%-----------------------------------------------------------------------------
%% H3 CONNECTION SETUP
%%%-----------------------------------------------------------------------------
-spec complete_h3_connection(nquic:ctx(), nquic_socket:t() | undefined, #h3_data{}) ->
    no_return().
complete_h3_connection(Ctx, OldSocket, Data) ->
    case init_h3_connection(Ctx) of
        {ok, H3Conn, Ctx1} ->
            Socket = nquic_lib:ctx_socket(Ctx1),
            Data1 = Data#h3_data{
                ctx = Ctx1,
                socket = Socket,
                h3_conn = H3Conn
            },
            Data1b = drain_handshake_socket(OldSocket, Data1),
            Data1c = drain_buffered_streams(Data1b),
            Data1d = await_peer_settings(Data1c, settings_wait_deadline(Data1c)),
            emit_connect_stop(Data1d),
            notify_pool_ready(Data1d),
            Data2 = start_idle_checker(touch_activity(Data1d)),
            reply_to_waiters(Data2#h3_data.ready_waiters, ok),
            enter_ready_loop(Data2#h3_data{ready_waiters = []});
        {error, Reason} ->
            close_ctx(Ctx),
            emit_connect_error(Data, Reason),
            notify_pool_error(Data, Reason),
            exit({shutdown, Reason})
    end.

-spec init_h3_connection(nquic:ctx()) ->
    {ok, nhttp_h3:conn(), nquic:ctx()} | {error, nhttpc_error:any_reason()}.
init_h3_connection(Ctx) ->
    H3Conn = nhttp_h3:new(client, #{
        qpack_max_table_capacity => ?QPACK_MAX_TABLE_CAPACITY,
        qpack_blocked_streams => ?QPACK_BLOCKED_STREAMS
    }),
    maybe
        {ok, CtrlId, Ctx1} ?= nquic_lib:open_stream(Ctx, #{type => uni}),
        {ok, EncId, Ctx2} ?= nquic_lib:open_stream(Ctx1, #{type => uni}),
        {ok, DecId, Ctx3} ?= nquic_lib:open_stream(Ctx2, #{type => uni}),
        {ok, H3Conn1, Actions} ?=
            nhttp_h3:init_local_streams(H3Conn, #{
                control => CtrlId,
                encoder => EncId,
                decoder => DecId
            }),
        {ok, Ctx4} ?= nhttpc_h3:execute_actions_flush(Ctx3, Actions),
        {ok, H3Conn1, Ctx4}
    end.

-spec reply_to_waiters([{{pid(), reference()}, integer()}], term()) -> ok.
reply_to_waiters([], _Reply) ->
    ok;
reply_to_waiters([{From, _Timestamp} | Rest], Reply) ->
    reply(From, Reply),
    reply_to_waiters(Rest, Reply).

%%%-----------------------------------------------------------------------------
%% ASYNC RECV MANAGEMENT
%%%-----------------------------------------------------------------------------
-doc """
Block until the peer's SETTINGS frame has been ingested or the deadline
elapses. The synchronous drains only catch the server's first 1-RTT
flight when it is already buffered by connect time; under load it can
arrive a moment later on the connected socket. Gating readiness on
SETTINGS lets the first request use the negotiated QPACK dynamic table
instead of falling back to static-only encoding. A non-conformant peer
that never sends SETTINGS still becomes ready (with default settings)
once the deadline passes rather than wedging the connection.
""".
-spec await_peer_settings(#h3_data{}, integer()) -> #h3_data{}.
await_peer_settings(#h3_data{settings_received = true} = Data, _Deadline) ->
    Data;
await_peer_settings(#h3_data{ctx = Ctx} = Data, Deadline) ->
    Remaining = Deadline - erlang:monotonic_time(millisecond),
    case Remaining > 0 of
        false ->
            Data;
        true ->
            case nquic_lib:recv_direct(Ctx, Remaining) of
                {ok, [], Ctx1} ->
                    await_peer_settings(Data#h3_data{ctx = Ctx1}, Deadline);
                {ok, QuicEvents, Ctx1} ->
                    Ctx2 = nquic_lib:schedule_timers(Ctx1),
                    {ok, Ctx3} = nquic_lib:flush(Ctx2),
                    Data1 = process_quic_events(QuicEvents, Data#h3_data{ctx = Ctx3}),
                    await_peer_settings(Data1, Deadline);
                {error, _Reason, Ctx1} ->
                    Data#h3_data{ctx = Ctx1}
            end
    end.

-doc """
Process server-initiated streams whose data was buffered during the
synchronous QUIC handshake. The server's first 1-RTT flight (its
control-stream SETTINGS and the QPACK encoder/decoder stream type
prefixes) is consumed by the connect-time gen_statem before the
library context is handed over, so the `{stream_data, _}` events those
STREAM frames produced are dropped rather than delivered to this
process. The bytes remain in the context's stream buffers; replay them
through the normal H3 pipeline so peer SETTINGS are applied and the
server's QPACK encoder stream is read.
""".
-spec drain_buffered_streams(#h3_data{}) -> #h3_data{}.
drain_buffered_streams(#h3_data{ctx = Ctx} = Data) ->
    StreamIds = nquic_protocol:pending_stream_ids(nquic_lib:ctx_state(Ctx)),
    Data1 = lists:foldl(
        fun(StreamId, Acc) -> process_quic_event({stream_data, StreamId}, Acc) end,
        Data,
        lists:sort(StreamIds)
    ),
    flush_ctx_after_batch(Data1).

-doc """
Process datagrams buffered on the abandoned handshake socket.
`nquic_lib:upgrade_to_connected/1` opens a fresh connected socket and
leaves the original handshake socket behind. The peer's post-handshake
flight (handshake ACKs, `HANDSHAKE_DONE`, the server's first 1-RTT
packets such as H3 SETTINGS) can already be sitting in the handshake
socket's kernel buffer by the time the upgrade runs. Those datagrams
would never be read once the connection selects only on the new
socket, wedging the connection (the local QUIC state never retires its
handshake packets and the peer's stream data is never delivered). Feed
them through the normal QUIC/H3 pipeline before discarding the old
socket. `nquic_lib:handle_packet/3` routes by connection ID, so the
source socket is irrelevant; ACKs are flushed over the new socket.
""".
-spec drain_handshake_socket(nquic_socket:t() | undefined, #h3_data{}) -> #h3_data{}.
drain_handshake_socket(undefined, Data) ->
    Data;
drain_handshake_socket(OldSocket, Data) ->
    Data1 = drain_handshake_socket_loop(OldSocket, Data, ?MAX_HANDSHAKE_DRAIN_PACKETS),
    _ = socket:close(OldSocket),
    Data1.

-spec drain_handshake_socket_loop(nquic_socket:t(), #h3_data{}, non_neg_integer()) ->
    #h3_data{}.
drain_handshake_socket_loop(_OldSocket, Data, 0) ->
    Data;
drain_handshake_socket_loop(OldSocket, #h3_data{ctx = Ctx} = Data, Remaining) ->
    case nquic_socket:recv_now(OldSocket) of
        {ok, {Source, PacketBin}} ->
            case nquic_lib:handle_packet(Ctx, Source, PacketBin) of
                {ok, QuicEvents, Ctx1} ->
                    {ok, Ctx2} = nquic_lib:flush(Ctx1),
                    Data1 = process_quic_events(QuicEvents, Data#h3_data{ctx = Ctx2}),
                    drain_handshake_socket_loop(OldSocket, Data1, Remaining - 1);
                {error, _Reason, Ctx1} ->
                    Data#h3_data{ctx = Ctx1}
            end;
        {select, _SelectInfo} ->
            Data;
        {error, _Reason} ->
            Data
    end.

-spec settings_wait_deadline(#h3_data{}) -> integer().
settings_wait_deadline(#h3_data{connect_timeout = ConnectTimeout}) ->
    Budget =
        case ConnectTimeout of
            infinity -> ?DEFAULT_SETTINGS_WAIT_MS;
            T when is_integer(T) -> T
        end,
    erlang:monotonic_time(millisecond) + Budget.

-spec start_recv(#h3_data{}) -> #h3_data{}.
start_recv(#h3_data{socket = Socket, gro_enabled = true} = Data) ->
    case nquic_socket:recv_msg_now(Socket) of
        {ok, {Source, PacketBin, Ctrl}} ->
            GsoSize = nquic_socket:get_gso_size_from_cmsg(Ctrl),
            self() ! {immediate_packet, Source, PacketBin, GsoSize},
            Data;
        {select, _SelectInfo} ->
            Data;
        {error, _Reason} ->
            Data
    end;
start_recv(#h3_data{socket = Socket} = Data) ->
    case nquic_socket:recv_now(Socket) of
        {ok, {Source, PacketBin}} ->
            self() ! {immediate_packet, Source, PacketBin, undefined},
            Data;
        {select, _SelectInfo} ->
            Data;
        {error, _Reason} ->
            Data
    end.

%%%-----------------------------------------------------------------------------
%% READY LOOP
%%%-----------------------------------------------------------------------------
-spec enter_ready_loop(#h3_data{}) -> no_return().
enter_ready_loop(Data) ->
    ready_loop(start_recv(Data)).

-spec ready_loop(#h3_data{}) -> no_return().
ready_loop(#h3_data{parent = Parent, socket = Socket} = Data) ->
    receive
        {send, StreamRef, Request, CallerPid, Opts} ->
            Batch = drain_sends([{StreamRef, Request, CallerPid, Opts}], ?MAX_H3_BATCH),
            Data1 = process_h3_batch(Batch, Data),
            ready_loop(Data1);
        {'$socket', Socket, select, _SelectInfo} ->
            handle_socket_ready(Data, ready);
        {'$socket', Socket, abort, _Info} ->
            fail_all_requests(Data, {quic_closed, socket_abort}),
            notify_pool_error(Data, {quic_closed, socket_abort}),
            exit(normal);
        {immediate_packet, Source, PacketBin, GsoSize} ->
            handle_incoming_packet(Source, PacketBin, GsoSize, Data, ready);
        {quic_timeout, Type} ->
            handle_quic_timeout(Type, Data, ready);
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
                    ready_loop(Data#h3_data{idle_check_timer_ref = undefined});
                false ->
                    handle_check_idle_ready(Data)
            end;
        {'$call', From, {cancel, Ref}} ->
            Data1 = handle_cancel_request(Ref, From, Data),
            ready_loop(Data1);
        {'$nhttpc_cancel', Ref, From} ->
            Data1 = handle_unified_cancel(Ref, From, Data),
            ready_loop(Data1);
        {'$call', From, wait_ready} ->
            reply(From, ok),
            ready_loop(Data);
        {'$call', From, ping} ->
            reply(From, pong),
            ready_loop(Data);
        {stop, _From} ->
            close_ctx(Data#h3_data.ctx),
            exit(normal);
        {'EXIT', Parent, Reason} ->
            fail_all_requests(Data, {connection_died, Reason}),
            close_ctx(Data#h3_data.ctx),
            exit(Reason);
        {'EXIT', _Pid, normal} ->
            ready_loop(Data);
        {'EXIT', _Pid, shutdown} ->
            ready_loop(Data);
        {'EXIT', _Pid, {shutdown, _}} ->
            ready_loop(Data);
        {'EXIT', _Pid, Reason} ->
            fail_all_requests(Data, {quic_closed, Reason}),
            notify_pool_error(Data, {quic_closed, Reason}),
            exit(Reason);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [], {ready, Data})
    end.

%%%-----------------------------------------------------------------------------
%% DRAINING LOOP
%%%-----------------------------------------------------------------------------
-spec draining_loop(#h3_data{}) -> no_return().
draining_loop(#h3_data{parent = Parent, socket = Socket} = Data) ->
    receive
        {send, StreamRef, _Request, CallerPid, _Opts} ->
            {error, Reason} = nhttp_error:pool_draining(),
            ok = reject_send(StreamRef, CallerPid, Reason, Data),
            draining_loop(Data);
        {'$socket', Socket, select, _SelectInfo} ->
            handle_socket_ready(Data, draining);
        {'$socket', Socket, abort, _Info} ->
            fail_all_requests(Data, {quic_closed, socket_abort}),
            exit(normal);
        {immediate_packet, Source, PacketBin, GsoSize} ->
            handle_incoming_packet(Source, PacketBin, GsoSize, Data, draining);
        {quic_timeout, Type} ->
            handle_quic_timeout(Type, Data, draining);
        check_request_timeouts ->
            Data1 = handle_check_request_timeouts(Data),
            case maps:size(Data1#h3_data.requests) of
                0 -> exit(normal);
                _ -> draining_loop(Data1)
            end;
        check_idle ->
            draining_loop(Data#h3_data{idle_check_timer_ref = undefined});
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
        {stop, _From} ->
            close_ctx(Data#h3_data.ctx),
            exit(normal);
        {'EXIT', Parent, Reason} ->
            fail_all_requests(Data, {connection_died, Reason}),
            close_ctx(Data#h3_data.ctx),
            exit(Reason);
        {'EXIT', _Pid, _Reason} ->
            draining_loop(Data);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [], {draining, Data})
    end.

%%%-----------------------------------------------------------------------------
%% OPPORTUNISTIC BATCHING
%%%-----------------------------------------------------------------------------
-spec drain_sends([send_item()], non_neg_integer()) -> [send_item()].
drain_sends(Acc, 0) ->
    lists:reverse(Acc);
drain_sends(Acc, Remaining) ->
    receive
        {send, StreamRef, Request, CallerPid, Opts} ->
            drain_sends([{StreamRef, Request, CallerPid, Opts} | Acc], Remaining - 1)
    after 0 ->
        lists:reverse(Acc)
    end.

-spec encode_h3_request_noflush(
    reference(), nhttp_lib:request(), pid(), nhttpc_conn:send_opts(), #h3_data{}
) ->
    {ok, #h3_data{}} | {error, term(), #h3_data{}}.
encode_h3_request_noflush(
    StreamRef,
    Request,
    CallerPid,
    Opts,
    #h3_data{
        ctx = Ctx,
        h3_conn = H3Conn,
        h3_scheme = Scheme,
        h3_authority = Authority,
        request_timeout = RequestTimeout,
        requests = Requests,
        stream_index = StreamIndex
    } = Data
) ->
    case nquic_lib:open_stream(Ctx, #{type => bidi}) of
        {ok, StreamId, Ctx1} ->
            Headers = nhttpc_h3:request_to_h3_headers(Request, Scheme, Authority),
            Body = maps:get(body, Request, <<>>),
            HasBody = iolist_size(Body) > 0,
            EndStream =
                case HasBody of
                    true -> nofin;
                    false -> fin
                end,
            case nhttp_h3:send_headers(H3Conn, StreamId, Headers, EndStream) of
                {ok, H3Conn1, HdrActions} ->
                    case nhttpc_h3:execute_actions_noflush(Ctx1, HdrActions) of
                        {ok, Ctx2} ->
                            Deadline = erlang:monotonic_time(millisecond) + RequestTimeout,
                            case HasBody of
                                false ->
                                    ReqSt = make_request_st(
                                        StreamRef, CallerPid, Request, StreamId, Deadline, Opts
                                    ),
                                    Data1 = Data#h3_data{
                                        ctx = Ctx2,
                                        h3_conn = H3Conn1,
                                        requests = Requests#{StreamRef => ReqSt},
                                        stream_index = StreamIndex#{StreamId => StreamRef}
                                    },
                                    {ok, start_request_timeout_checker(Data1)};
                                true ->
                                    send_h3_body_noflush(
                                        StreamRef,
                                        StreamId,
                                        Request,
                                        CallerPid,
                                        Opts,
                                        Body,
                                        H3Conn1,
                                        Ctx2,
                                        Deadline,
                                        Data
                                    )
                            end;
                        {error, Reason} ->
                            ok = reject_send(StreamRef, CallerPid, {send_error, Reason}, Data),
                            {error, Reason, Data#h3_data{ctx = Ctx1, h3_conn = H3Conn1}};
                        {close, Code} ->
                            fail_all_requests(Data, {h3_connection_error, Code}),
                            ok = nquic_lib:shutdown(Ctx1, Code, <<>>),
                            exit({error, {h3_connection_error, Code}})
                    end;
                {error, Reason} ->
                    ok = reject_send(StreamRef, CallerPid, {http3_error, Reason}, Data),
                    {error, Reason, Data#h3_data{ctx = Ctx1}}
            end;
        {error, Reason} ->
            ok = reject_send(StreamRef, CallerPid, {quic_error, Reason}, Data),
            {error, Reason, Data}
    end.

-spec flush_ctx_after_batch(#h3_data{}) -> #h3_data{}.
flush_ctx_after_batch(#h3_data{ctx = Ctx} = Data) ->
    {ok, Ctx1} = nquic_lib:flush(Ctx),
    Data#h3_data{ctx = Ctx1}.

-spec process_h3_batch([send_item(), ...], #h3_data{}) -> #h3_data{}.
process_h3_batch(Batch, Data) ->
    Data1 = lists:foldl(
        fun({StreamRef, Request, CallerPid, Opts}, AccData) ->
            case encode_h3_request_noflush(StreamRef, Request, CallerPid, Opts, AccData) of
                {ok, AccData1} -> AccData1;
                {error, _Reason, AccData1} -> AccData1
            end
        end,
        Data,
        Batch
    ),
    flush_ctx_after_batch(Data1).

-spec send_h3_body_noflush(
    reference(),
    nhttp_lib:stream_id(),
    nhttp_lib:request(),
    pid(),
    nhttpc_conn:send_opts(),
    iodata(),
    nhttp_h3:conn(),
    nquic:ctx(),
    integer(),
    #h3_data{}
) ->
    {ok, #h3_data{}} | {error, term(), #h3_data{}}.
send_h3_body_noflush(
    StreamRef,
    StreamId,
    Request,
    CallerPid,
    Opts,
    Body,
    H3Conn,
    Ctx,
    Deadline,
    #h3_data{requests = Requests, stream_index = StreamIndex} = Data
) ->
    case nhttp_h3:send_data(H3Conn, StreamId, Body, fin) of
        {ok, H3Conn2, BodyActions} ->
            case nhttpc_h3:execute_actions_noflush(Ctx, BodyActions) of
                {ok, Ctx1} ->
                    ReqSt = make_request_st(
                        StreamRef, CallerPid, Request, StreamId, Deadline, Opts
                    ),
                    Data1 = Data#h3_data{
                        ctx = Ctx1,
                        h3_conn = H3Conn2,
                        requests = Requests#{StreamRef => ReqSt},
                        stream_index = StreamIndex#{StreamId => StreamRef}
                    },
                    {ok, start_request_timeout_checker(Data1)};
                {error, Reason} ->
                    ok = reject_send(StreamRef, CallerPid, {send_error, Reason}, Data),
                    {error, Reason, Data#h3_data{ctx = Ctx, h3_conn = H3Conn2}};
                {close, Code} ->
                    fail_all_requests(Data, {h3_connection_error, Code}),
                    ok = nquic_lib:shutdown(Ctx, Code, <<>>),
                    exit({error, {h3_connection_error, Code}})
            end;
        {error, Reason} ->
            ok = reject_send(StreamRef, CallerPid, {http3_error, Reason}, Data),
            {error, Reason, Data#h3_data{ctx = Ctx, h3_conn = H3Conn}}
    end.

%%%-----------------------------------------------------------------------------
%% SOCKET RECV HANDLING
%%%-----------------------------------------------------------------------------
-spec drain_gro(#h3_data{}, ready | draining, [nquic_protocol:event()]) -> no_return().
drain_gro(#h3_data{socket = Socket} = Data, Loop, RevEvents) ->
    case nquic_socket:recv_msg_now(Socket) of
        {ok, {Source, PacketBin, Ctrl}} ->
            GsoSize = nquic_socket:get_gso_size_from_cmsg(Ctrl),
            drain_gro_packet(Source, PacketBin, GsoSize, Data, Loop, RevEvents);
        {select, _SelectInfo} ->
            finish_drain(Data, Loop, RevEvents);
        {error, _Reason} ->
            fail_all_requests(Data, {quic_closed, recv_error}),
            exit(normal)
    end.

-spec drain_gro_packet(
    nquic_socket:sockaddr(),
    binary(),
    undefined | pos_integer(),
    #h3_data{},
    ready | draining,
    [nquic_protocol:event()]
) -> no_return().
drain_gro_packet(Source, PacketBin, undefined, #h3_data{ctx = Ctx} = Data, Loop, RevEvents) ->
    case nquic_lib:handle_packet_notimers(Ctx, Source, PacketBin) of
        {ok, QuicEvents, Ctx1} ->
            drain_gro(Data#h3_data{ctx = Ctx1}, Loop, lists:reverse(QuicEvents, RevEvents));
        {error, Reason, Ctx1} ->
            fail_all_requests(Data#h3_data{ctx = Ctx1}, {quic_error, Reason}),
            exit({error, Reason})
    end;
drain_gro_packet(Source, Buf, GsoSize, #h3_data{ctx = Ctx} = Data, Loop, RevEvents) ->
    {ok, QuicEvents, Ctx1} = nquic_lib:handle_packet_batch_notimers(
        Ctx, Source, Buf, GsoSize, not_ect
    ),
    drain_gro(Data#h3_data{ctx = Ctx1}, Loop, lists:reverse(QuicEvents, RevEvents)).

-spec drain_plain(#h3_data{}, ready | draining, [nquic_protocol:event()]) -> no_return().
drain_plain(#h3_data{socket = Socket, ctx = Ctx} = Data, Loop, RevEvents) ->
    case nquic_socket:recv_now(Socket) of
        {ok, {Source, PacketBin}} ->
            case nquic_lib:handle_packet_notimers(Ctx, Source, PacketBin) of
                {ok, QuicEvents, Ctx1} ->
                    Data1 = Data#h3_data{ctx = Ctx1},
                    RevEvents1 = lists:reverse(QuicEvents, RevEvents),
                    case should_enable_gro(Data1, PacketBin) of
                        true ->
                            ok = nquic_socket:set_gro(Socket, true),
                            drain_gro(Data1#h3_data{gro_enabled = true}, Loop, RevEvents1);
                        false ->
                            drain_plain(Data1, Loop, RevEvents1)
                    end;
                {error, Reason, Ctx1} ->
                    fail_all_requests(Data#h3_data{ctx = Ctx1}, {quic_error, Reason}),
                    exit({error, Reason})
            end;
        {select, _SelectInfo} ->
            finish_drain(Data, Loop, RevEvents);
        {error, _Reason} ->
            fail_all_requests(Data, {quic_closed, recv_error}),
            exit(normal)
    end.

-spec drain_socket(#h3_data{}, ready | draining, [nquic_protocol:event()]) -> no_return().
drain_socket(#h3_data{gro_enabled = true} = Data, Loop, RevEvents) ->
    drain_gro(Data, Loop, RevEvents);
drain_socket(Data, Loop, RevEvents) ->
    drain_plain(Data, Loop, RevEvents).

-spec finish_drain(#h3_data{}, ready | draining, [nquic_protocol:event()]) -> no_return().
finish_drain(#h3_data{ctx = Ctx} = Data, Loop, RevEvents) ->
    Ctx1 = nquic_lib:schedule_timers(Ctx),
    {ok, Ctx2} = nquic_lib:flush(Ctx1),
    Data1 = Data#h3_data{ctx = Ctx2},
    Data2 = process_quic_events(lists:reverse(RevEvents), Data1),
    maybe_state_transition(Data2, Loop).

-spec handle_incoming_packet(
    nquic_socket:sockaddr(), binary(), undefined | pos_integer(), #h3_data{}, ready | draining
) -> no_return().
handle_incoming_packet(Source, PacketBin, undefined, #h3_data{ctx = Ctx} = Data, Loop) ->
    case nquic_lib:handle_packet_notimers(Ctx, Source, PacketBin) of
        {ok, QuicEvents, Ctx1} ->
            drain_socket(Data#h3_data{ctx = Ctx1}, Loop, lists:reverse(QuicEvents));
        {error, Reason, Ctx1} ->
            fail_all_requests(Data#h3_data{ctx = Ctx1}, {quic_error, Reason}),
            exit({error, Reason})
    end;
handle_incoming_packet(Source, Buf, GsoSize, #h3_data{ctx = Ctx} = Data, Loop) ->
    {ok, QuicEvents, Ctx1} = nquic_lib:handle_packet_batch_notimers(
        Ctx, Source, Buf, GsoSize, not_ect
    ),
    drain_socket(Data#h3_data{ctx = Ctx1}, Loop, lists:reverse(QuicEvents)).

-spec handle_quic_timeout(nquic_protocol:timer_type(), #h3_data{}, ready | draining) ->
    no_return().
handle_quic_timeout(Type, #h3_data{ctx = Ctx} = Data, Loop) ->
    case nquic_lib:timeout(Ctx, Type) of
        {ok, QuicEvents, Ctx1} ->
            Data1 = Data#h3_data{ctx = Ctx1},
            Data2 = process_quic_events(QuicEvents, Data1),
            maybe_state_transition(Data2, Loop);
        {error, _Reason, Ctx1} ->
            fail_all_requests(Data#h3_data{ctx = Ctx1}, {quic_error, timeout_error}),
            exit(normal)
    end.

-spec handle_socket_ready(#h3_data{}, ready | draining) -> no_return().
handle_socket_ready(Data, Loop) ->
    drain_socket(Data, Loop, []).

-spec should_enable_gro(#h3_data{}, binary()) -> boolean().
should_enable_gro(#h3_data{gro_capable = true}, PacketBin) ->
    byte_size(PacketBin) >= ?GRO_ACTIVATION_BYTES;
should_enable_gro(_Data, _PacketBin) ->
    false.

%%%-----------------------------------------------------------------------------
%% QUIC EVENT -> H3 EVENT PROCESSING
%%%-----------------------------------------------------------------------------
-spec process_quic_event(nquic_protocol:event(), #h3_data{}) -> #h3_data{}.
process_quic_event({stream_data, StreamId}, #h3_data{ctx = Ctx, h3_conn = H3Conn} = Data) ->
    case nquic_lib:recv(Ctx, StreamId) of
        {ok, Bin, IsFin, Ctx1} ->
            Fin = bool_to_fin(IsFin),
            case nhttp_h3:recv(H3Conn, StreamId, Bin, Fin) of
                {ok, H3Events, H3Conn1, Actions} ->
                    case nhttpc_h3:execute_actions_noflush(Ctx1, Actions) of
                        {ok, Ctx2} ->
                            process_h3_events(
                                H3Events, Data#h3_data{ctx = Ctx2, h3_conn = H3Conn1}
                            );
                        {error, Reason} ->
                            fail_all_requests(
                                Data#h3_data{ctx = Ctx1, h3_conn = H3Conn1},
                                {send_error, Reason}
                            ),
                            exit({error, Reason});
                        {close, Code} ->
                            fail_all_requests(
                                Data#h3_data{ctx = Ctx1, h3_conn = H3Conn1},
                                {h3_connection_error, Code}
                            ),
                            ok = nquic_lib:shutdown(Ctx1, Code, <<>>),
                            exit({error, {h3_connection_error, Code}})
                    end;
                {error, {connection_error, Code, _Msg}} ->
                    fail_all_requests(
                        Data#h3_data{ctx = Ctx1}, {http3_error, {connection_error, Code}}
                    ),
                    ok = nquic_lib:shutdown(Ctx1, nhttpc_h3:h3_error_to_code(Code), <<>>),
                    exit({error, {h3_connection_error, Code}});
                {error, {stream_error, SId, Code, _Msg}} ->
                    Data1 = reset_stream_with_code(SId, Code, Data#h3_data{ctx = Ctx1}),
                    case find_request_by_stream(SId, Data1) of
                        {ok, Ref, ReqSt} ->
                            send_response(
                                ReqSt,
                                {error, {http3_error, {stream_error, Code}}},
                                Data1#h3_data.self
                            ),
                            complete_request(Ref, SId, Data1);
                        error ->
                            Data1
                    end
            end;
        {error, _Reason} ->
            Data
    end;
process_quic_event({stream_opened, StreamId}, #h3_data{h3_conn = H3Conn} = Data) ->
    Type = stream_id_type(StreamId),
    case nhttp_h3:stream_opened(H3Conn, StreamId, Type) of
        {ok, H3Conn1} ->
            Data#h3_data{h3_conn = H3Conn1};
        {error, {stream_error, _SId, Code, _Msg}} ->
            reset_stream_with_code(StreamId, Code, Data)
    end;
process_quic_event({stream_reset, StreamId, ErrorCode}, #h3_data{h3_conn = H3Conn} = Data) ->
    {ok, H3Events, H3Conn1} = nhttp_h3:stream_reset(H3Conn, StreamId, ErrorCode),
    Data1 = Data#h3_data{h3_conn = H3Conn1},
    process_h3_events(H3Events, Data1);
process_quic_event(connection_closed, Data) ->
    fail_all_requests(Data, connection_closed),
    notify_pool_error(Data, connection_closed),
    exit(normal);
process_quic_event(_Event, Data) ->
    Data.

-spec process_quic_events([nquic_protocol:event()], #h3_data{}) -> #h3_data{}.
process_quic_events([], Data) ->
    Data;
process_quic_events([Event | Rest], Data) ->
    Data1 = process_quic_event(Event, Data),
    process_quic_events(Rest, Data1).

-spec stream_id_type(non_neg_integer()) -> bidi | uni.
stream_id_type(StreamId) ->
    case StreamId band 2 of
        0 -> bidi;
        _ -> uni
    end.

%%%-----------------------------------------------------------------------------
%% H3 EVENT PROCESSING
%%%-----------------------------------------------------------------------------
-spec process_h3_event(nhttp_h3:event(), #h3_data{}) -> #h3_data{}.
process_h3_event({response, StreamId, Response, EndStream}, Data) ->
    case find_request_by_stream(StreamId, Data) of
        {ok, Ref, ReqSt} ->
            case header_limit_breach(Response, ReqSt) of
                {breach, Detail} ->
                    abort_stream(
                        Ref,
                        StreamId,
                        ReqSt,
                        nhttpc_error:protocol_error(http3, Detail),
                        Data
                    );
                ok ->
                    deliver_response_headers(Ref, StreamId, ReqSt, Response, EndStream, Data)
            end;
        error ->
            Data
    end;
process_h3_event({data, StreamId, BodyData, EndStream}, Data) ->
    case find_request_by_stream(StreamId, Data) of
        {ok, Ref, ReqSt} ->
            NewBodySize = ReqSt#request_st.body_size + byte_size(BodyData),
            #request_st{max_response_size = MaxSize} = ReqSt,
            case NewBodySize > MaxSize of
                true ->
                    abort_stream(
                        Ref,
                        StreamId,
                        ReqSt,
                        nhttp_error:response_too_large(NewBodySize, MaxSize),
                        Data
                    );
                false ->
                    deliver_body_chunk(
                        Ref, StreamId, ReqSt, BodyData, NewBodySize, EndStream, Data
                    )
            end;
        error ->
            Data
    end;
process_h3_event({trailers, StreamId, Trailers}, Data) ->
    case find_request_by_stream(StreamId, Data) of
        {ok, Ref, ReqSt} ->
            Body = iolist_to_binary(lists:reverse(ReqSt#request_st.body_acc)),
            Response = ReqSt#request_st.response,
            send_response(
                ReqSt,
                {ok, Response#{body => Body, trailers => Trailers}},
                Data#h3_data.self
            ),
            complete_request(Ref, StreamId, Data);
        error ->
            Data
    end;
process_h3_event({stream_reset, StreamId, ErrorCode}, Data) ->
    case find_request_by_stream(StreamId, Data) of
        {ok, Ref, ReqSt} ->
            send_response(
                ReqSt,
                {error, {http3_error, {stream_reset, ErrorCode}}},
                Data#h3_data.self
            ),
            complete_request(Ref, StreamId, Data);
        error ->
            Data
    end;
process_h3_event({goaway, _LastStreamId, _ErrorCode, _Reason}, Data) ->
    notify_pool_draining(Data),
    Data#h3_data{goaway_received = true};
process_h3_event({settings, _Settings}, Data) ->
    Data#h3_data{settings_received = true};
process_h3_event(_Event, Data) ->
    Data.

-spec process_h3_events([nhttp_h3:event()], #h3_data{}) -> #h3_data{}.
process_h3_events([], Data) ->
    Data;
process_h3_events([Event | Rest], Data) ->
    NewData = process_h3_event(Event, Data),
    process_h3_events(Rest, NewData).

%%%-----------------------------------------------------------------------------
%% STATE TRANSITIONS
%%%-----------------------------------------------------------------------------
-spec maybe_state_transition(#h3_data{}, ready | draining) -> no_return().
maybe_state_transition(Data, ready) ->
    maybe_state_transition_ready(Data);
maybe_state_transition(Data, draining) ->
    maybe_state_transition_draining(Data).

-spec maybe_state_transition_draining(#h3_data{}) -> no_return().
maybe_state_transition_draining(#h3_data{requests = Requests} = Data) ->
    case maps:size(Requests) of
        0 -> exit(normal);
        _ -> draining_loop(Data)
    end.

-spec maybe_state_transition_ready(#h3_data{}) -> no_return().
maybe_state_transition_ready(
    #h3_data{requests = Requests, goaway_received = true} = Data
) ->
    case maps:size(Requests) of
        0 -> exit(normal);
        _ -> draining_loop(Data)
    end;
maybe_state_transition_ready(#h3_data{requests = Requests} = Data) ->
    case maps:size(Requests) of
        0 ->
            Data1 = start_idle_checker(touch_activity(Data)),
            ready_loop(Data1);
        _ ->
            ready_loop(Data)
    end.

%%%-----------------------------------------------------------------------------
%% HELPER FUNCTIONS
%%%-----------------------------------------------------------------------------
-doc """
End a stream that the client refuses to finish reading. The caller gets
the reason, the peer gets a `H3_REQUEST_CANCELLED` reset, and the request
gives its stream slot back.
""".
-spec abort_stream(
    reference(),
    nhttp_lib:stream_id(),
    #request_st{},
    {error, nhttpc_error:any_reason()},
    #h3_data{}
) -> #h3_data{}.
abort_stream(Ref, StreamId, ReqSt, Error, Data) ->
    send_response(ReqSt, Error, Data#h3_data.self),
    complete_request(Ref, StreamId, reset_request_stream(StreamId, Data)).

-spec bool_to_fin(boolean()) -> nhttp_h3:fin().
bool_to_fin(true) -> fin;
bool_to_fin(false) -> nofin.

-spec close_ctx(nquic:ctx() | undefined) -> ok.
close_ctx(undefined) ->
    ok;
close_ctx(Ctx) ->
    _ = nquic_lib:close(Ctx),
    ok.

-spec complete_request(reference(), nhttp_lib:stream_id() | undefined, #h3_data{}) ->
    #h3_data{}.
complete_request(Ref, undefined, #h3_data{requests = Requests} = Data) ->
    release_stream_slot(Data),
    Data#h3_data{requests = maps:remove(Ref, Requests)};
complete_request(
    Ref, StreamId, #h3_data{requests = Requests, stream_index = StreamIndex} = Data
) ->
    release_stream_slot(Data),
    Data#h3_data{
        requests = maps:remove(Ref, Requests),
        stream_index = maps:remove(StreamId, StreamIndex)
    }.

-spec deliver_body_chunk(
    reference(),
    nhttp_lib:stream_id(),
    #request_st{},
    binary(),
    non_neg_integer(),
    nhttp_h3:fin(),
    #h3_data{}
) -> #h3_data{}.
deliver_body_chunk(Ref, StreamId, ReqSt, BodyData, _BodySize, fin, Data) ->
    Body = iolist_to_binary(lists:reverse([BodyData | ReqSt#request_st.body_acc])),
    Response = ReqSt#request_st.response,
    send_response(ReqSt, {ok, Response#{body => Body}}, Data#h3_data.self),
    complete_request(Ref, StreamId, Data);
deliver_body_chunk(Ref, _StreamId, ReqSt, BodyData, BodySize, nofin, Data) ->
    NewReqSt = ReqSt#request_st{
        body_acc = [BodyData | ReqSt#request_st.body_acc],
        body_size = BodySize
    },
    update_request(Ref, NewReqSt, Data).

-spec deliver_response_headers(
    reference(),
    nhttp_lib:stream_id(),
    #request_st{},
    nhttp_lib:response(),
    nhttp_h3:fin(),
    #h3_data{}
) -> #h3_data{}.
deliver_response_headers(Ref, StreamId, ReqSt, Response, fin, Data) ->
    send_response(ReqSt, {ok, Response#{body => <<>>}}, Data#h3_data.self),
    complete_request(Ref, StreamId, Data);
deliver_response_headers(Ref, _StreamId, ReqSt, Response, nofin, Data) ->
    update_request(Ref, ReqSt#request_st{response = Response}, Data).

-spec fail_all_requests(#h3_data{}, term()) -> ok.
fail_all_requests(#h3_data{self = Self, requests = Requests}, Reason) ->
    maps:foreach(
        fun(_Ref, ReqSt) ->
            send_response(ReqSt, {error, Reason}, Self)
        end,
        Requests
    ),
    ok.

-spec find_request_by_stream(nhttp_lib:stream_id(), #h3_data{}) ->
    {ok, reference(), #request_st{}} | error.
find_request_by_stream(
    StreamId, #h3_data{stream_index = StreamIndex, requests = Requests}
) ->
    case maps:get(StreamId, StreamIndex, undefined) of
        undefined ->
            error;
        Ref ->
            case maps:get(Ref, Requests, undefined) of
                undefined -> error;
                ReqSt -> {ok, Ref, ReqSt}
            end
    end.

-spec has_pending_requests(#h3_data{}) -> boolean().
has_pending_requests(#h3_data{requests = M}) ->
    map_size(M) > 0.

-spec header_count_breach(nhttp_lib:headers(), pos_integer() | infinity) ->
    ok | {breach, too_many_headers}.
header_count_breach(_Headers, infinity) ->
    ok;
header_count_breach(Headers, MaxCount) when length(Headers) > MaxCount ->
    {breach, too_many_headers};
header_count_breach(_Headers, _MaxCount) ->
    ok.

-doc """
Whether the response header block is over either of the caller's header
limits. HTTP/1.1 reports the same two reasons for the same two limits, so
a caller cannot tell the protocols apart from the error alone.
""".
-spec header_limit_breach(nhttp_lib:response(), #request_st{}) ->
    ok | {breach, header_too_large | too_many_headers}.
header_limit_breach(Response, ReqSt) ->
    #request_st{max_header_size = MaxSize, max_headers_count = MaxCount} = ReqSt,
    Headers = maps:get(headers, Response, []),
    case header_count_breach(Headers, MaxCount) of
        ok -> header_size_breach(Headers, MaxSize);
        {breach, _} = Breach -> Breach
    end.

-spec header_size_breach(nhttp_lib:headers(), pos_integer() | infinity) ->
    ok | {breach, header_too_large}.
header_size_breach(_Headers, infinity) ->
    ok;
header_size_breach(Headers, MaxSize) ->
    Size = lists:foldl(
        fun({Name, Value}, Acc) -> Acc + byte_size(Name) + byte_size(Value) end,
        0,
        Headers
    ),
    case Size > MaxSize of
        true -> {breach, header_too_large};
        false -> ok
    end.

-spec make_request_st(
    reference(),
    pid(),
    nhttp_lib:request(),
    nhttp_lib:stream_id(),
    integer(),
    nhttpc_conn:send_opts()
) ->
    #request_st{}.
make_request_st(StreamRef, CallerPid, Request, StreamId, Deadline, Opts) ->
    #request_st{
        stream_ref = StreamRef,
        caller_pid = CallerPid,
        request = Request,
        stream_id = StreamId,
        deadline = Deadline,
        max_response_size = maps:get(max_response_size, Opts, ?DEFAULT_MAX_RESPONSE_SIZE),
        max_header_size = maps:get(max_header_size, Opts, ?DEFAULT_MAX_HEADER_SIZE),
        max_headers_count = maps:get(max_headers_count, Opts, ?DEFAULT_MAX_HEADERS_COUNT)
    }.

-doc """
Answer a send that never became a registered request. The pool reserved a
live stream slot for it before the message left the caller, and
`complete_request/3` never runs for a request that was never registered,
so the slot has to go back here.
""".
-spec reject_send(reference(), pid(), nhttpc_error:any_reason(), #h3_data{}) -> ok.
reject_send(StreamRef, CallerPid, Reason, #h3_data{self = Self} = Data) ->
    CallerPid ! {nhttpc_error, Self, StreamRef, Reason},
    release_stream_slot(Data).

-doc """
Reset one request stream and keep the context the reset produced.
`nquic_lib:reset_stream/3` flushes, so it advances packet numbers and
timers: dropping its context replays a packet number the peer already saw.
""".
-spec reset_request_stream(nhttp_lib:stream_id(), #h3_data{}) -> #h3_data{}.
reset_request_stream(StreamId, Data) ->
    reset_stream_with_code(StreamId, h3_request_cancelled, Data).

-spec reset_stream_with_code(nhttp_lib:stream_id(), nhttp_h3:h3_error_code(), #h3_data{}) ->
    #h3_data{}.
reset_stream_with_code(StreamId, Code, #h3_data{ctx = Ctx} = Data) ->
    case nquic_lib:reset_stream(Ctx, StreamId, nhttpc_h3:h3_error_to_code(Code)) of
        {ok, Ctx1} -> Data#h3_data{ctx = Ctx1};
        {error, _Reason} -> Data
    end.

-spec send_response(
    #request_st{}, {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}, pid()
) -> ok.
send_response(
    #request_st{stream_ref = StreamRef, caller_pid = CallerPid}, {ok, _} = Result, Self
) ->
    CallerPid ! {nhttpc_response, Self, StreamRef, Result},
    ok;
send_response(
    #request_st{stream_ref = StreamRef, caller_pid = CallerPid}, {error, Reason}, Self
) ->
    CallerPid ! {nhttpc_error, Self, StreamRef, Reason},
    ok.

-spec touch_activity(#h3_data{}) -> #h3_data{}.
touch_activity(Data) ->
    Data#h3_data{last_activity = erlang:monotonic_time(millisecond)}.

-spec update_request(reference(), #request_st{}, #h3_data{}) -> #h3_data{}.
update_request(Ref, ReqSt, #h3_data{requests = Requests} = Data) ->
    Data#h3_data{requests = Requests#{Ref => ReqSt}}.

%%%-----------------------------------------------------------------------------
%% CANCEL HANDLING
%%%-----------------------------------------------------------------------------
-spec ack_cancel(pid(), reference()) -> ok.
ack_cancel(From, Ref) ->
    From ! {'$nhttpc_cancel_ack', Ref},
    ok.

-spec do_cancel(reference(), fun((ok | {error, not_found}) -> any()), #h3_data{}) -> #h3_data{}.
do_cancel(Ref, AckFun, #h3_data{requests = Requests} = Data) ->
    case maps:get(Ref, Requests, undefined) of
        undefined ->
            AckFun({error, not_found}),
            Data;
        #request_st{stream_id = StreamId} ->
            Data1 = complete_request(Ref, StreamId, reset_cancelled_stream(StreamId, Data)),
            AckFun(ok),
            Data1
    end.

-spec handle_cancel_request(reference(), {pid(), reference()}, #h3_data{}) ->
    #h3_data{}.
handle_cancel_request(Ref, From, Data) ->
    do_cancel(Ref, fun(Result) -> reply(From, Result) end, Data).

-spec handle_unified_cancel(reference(), pid(), #h3_data{}) -> #h3_data{}.
handle_unified_cancel(Ref, From, Data) ->
    Ack = fun(_Result) -> ack_cancel(From, Ref) end,
    do_cancel(Ref, Ack, Data).

-spec reset_cancelled_stream(nhttp_lib:stream_id() | undefined, #h3_data{}) -> #h3_data{}.
reset_cancelled_stream(undefined, Data) ->
    Data;
reset_cancelled_stream(StreamId, Data) ->
    reset_request_stream(StreamId, Data).

%%%-----------------------------------------------------------------------------
%% IDLE TIMEOUT HANDLING
%%%-----------------------------------------------------------------------------
-spec handle_check_idle_ready(#h3_data{}) -> no_return().
handle_check_idle_ready(
    #h3_data{
        last_activity = LastActivity,
        idle_timeout = IdleTimeout,
        self = Self
    } = Data
) ->
    Now = erlang:monotonic_time(millisecond),
    Elapsed = Now - LastActivity,
    case Elapsed >= IdleTimeout of
        true ->
            emit_idle_timeout(Data),
            notify_pool_idle_close(Data),
            exit(normal);
        false ->
            CheckInterval = idle_check_interval(IdleTimeout),
            TimerRef = erlang:send_after(CheckInterval, Self, check_idle),
            ready_loop(Data#h3_data{idle_check_timer_ref = TimerRef})
    end.

%%%-----------------------------------------------------------------------------
%% IDLE TIMER MANAGEMENT
%%%-----------------------------------------------------------------------------
-spec idle_check_interval(timeout()) -> pos_integer().
idle_check_interval(Timeout) when is_integer(Timeout) ->
    min(30000, max(5000, Timeout div 2));
idle_check_interval(_) ->
    30000.

-spec start_idle_checker(#h3_data{}) -> #h3_data{}.
start_idle_checker(#h3_data{idle_timeout = infinity} = Data) ->
    Data;
start_idle_checker(#h3_data{idle_check_timer_ref = Ref} = Data) when Ref =/= undefined ->
    Data;
start_idle_checker(#h3_data{self = Self, idle_timeout = IdleTimeout} = Data) ->
    CheckInterval = idle_check_interval(IdleTimeout),
    TimerRef = erlang:send_after(CheckInterval, Self, check_idle),
    Data#h3_data{idle_check_timer_ref = TimerRef}.

%%%-----------------------------------------------------------------------------
%% REQUEST TIMEOUT CHECKER
%%%-----------------------------------------------------------------------------
-doc """
Answer one request that reached the connection's own deadline. The
request left `requests` before this runs, so `complete_request/3` never
sees it and the stream slot has to come back here.
""".
-spec expire_request({reference(), #request_st{}}, #h3_data{}) -> #h3_data{}.
expire_request({_Ref, #request_st{stream_id = StreamId} = ReqSt}, Data) ->
    send_response(
        ReqSt,
        nhttp_error:request_timeout(Data#h3_data.request_timeout),
        Data#h3_data.self
    ),
    ok = release_stream_slot(Data),
    Data1 = reset_cancelled_stream(StreamId, Data),
    Data1#h3_data{stream_index = maps:remove(StreamId, Data1#h3_data.stream_index)}.

-spec handle_check_request_timeouts(#h3_data{}) -> #h3_data{}.
handle_check_request_timeouts(#h3_data{requests = Requests} = Data) ->
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
    Data1 = lists:foldl(fun expire_request/2, Data#h3_data{requests = Active}, Expired),
    case maps:size(Active) of
        0 ->
            stop_request_timeout_checker(Data1);
        _ ->
            TimerRef = erlang:send_after(
                ?REQUEST_TIMEOUT_CHECK_INTERVAL, Data1#h3_data.self, check_request_timeouts
            ),
            Data1#h3_data{request_timeout_check_ref = TimerRef}
    end.

-spec start_request_timeout_checker(#h3_data{}) -> #h3_data{}.
start_request_timeout_checker(#h3_data{request_timeout_check_ref = Ref} = Data) when
    Ref =/= undefined
->
    Data;
start_request_timeout_checker(Data) ->
    TimerRef = erlang:send_after(
        ?REQUEST_TIMEOUT_CHECK_INTERVAL, Data#h3_data.self, check_request_timeouts
    ),
    Data#h3_data{request_timeout_check_ref = TimerRef}.

-spec stop_request_timeout_checker(#h3_data{}) -> #h3_data{}.
stop_request_timeout_checker(#h3_data{request_timeout_check_ref = undefined} = Data) ->
    Data;
stop_request_timeout_checker(#h3_data{request_timeout_check_ref = Ref} = Data) ->
    _ = erlang:cancel_timer(Ref),
    Data#h3_data{request_timeout_check_ref = undefined}.

%%%-----------------------------------------------------------------------------
%% CONNECTION HELPERS
%%%-----------------------------------------------------------------------------
-spec normalize_host(binary() | string() | inet:ip_address()) -> binary().
normalize_host(Host) when is_binary(Host) -> Host;
normalize_host(Host) when is_list(Host) -> list_to_binary(Host);
normalize_host(Host) when is_tuple(Host) -> list_to_binary(inet:ntoa(Host)).

-spec quic_idle_timeout(timeout()) -> timeout().
quic_idle_timeout(infinity) -> 60000;
quic_idle_timeout(Timeout) when is_integer(Timeout) -> Timeout.

%%%-----------------------------------------------------------------------------
%% POOL NOTIFICATIONS
%%%-----------------------------------------------------------------------------
-spec notify_pool_draining(#h3_data{}) -> ok.
notify_pool_draining(#h3_data{pool_mgr = undefined}) ->
    ok;
notify_pool_draining(#h3_data{self = Self, pool_mgr = PoolMgr, conn_ets = ConnEts}) ->
    h3_ets_update_status(ConnEts, Self, draining),
    nhttpc_pool:connection_draining(PoolMgr, Self),
    ok.

-spec notify_pool_error(#h3_data{}, term()) -> ok.
notify_pool_error(#h3_data{pool_mgr = undefined}, _Reason) ->
    ok;
notify_pool_error(#h3_data{self = Self, pool_mgr = PoolMgr}, Reason) ->
    nhttpc_pool:connection_error(PoolMgr, Self, Reason),
    ok.

-spec notify_pool_idle_close(#h3_data{}) -> ok.
notify_pool_idle_close(#h3_data{pool_mgr = undefined}) ->
    ok;
notify_pool_idle_close(#h3_data{self = Self, pool_mgr = PoolMgr}) ->
    nhttpc_pool:connection_error(PoolMgr, Self, idle_timeout),
    ok.

-doc """
Report the connection as ready, with the cap already written. The
manager creates the array with a cap of zero and never writes it, so a
checkout can only succeed after this write. An HTTP/3 connection reports
no peer cap, so the value is the client-side ceiling.
""".
-spec notify_pool_ready(#h3_data{}) -> ok.
notify_pool_ready(#h3_data{pool_mgr = undefined}) ->
    ok;
notify_pool_ready(#h3_data{self = Self, pool_mgr = PoolMgr, stream_slots = Slots}) ->
    ok = write_stream_cap(Slots),
    nhttpc_pool:connection_ready(PoolMgr, Self, http3),
    ok.

-doc """
Write the stream cap and the credit sentinel. QUIC carries its own flow
control inside `nquic`, so the pool has no connection send window to
reserve against here. The sentinel keeps the fast path free of a branch
on protocol: a credit reservation subtracts from it and never rolls
back.
""".
-spec write_stream_cap(nhttpc_stream_slots:slots() | undefined) -> ok.
write_stream_cap(undefined) ->
    ok;
write_stream_cap(Slots) ->
    ok = nhttpc_stream_slots:set_credit(Slots, nhttpc_stream_slots:unbounded_credit()),
    nhttpc_stream_slots:set_cap(Slots, nhttpc_stream_slots:default_cap(http3)).

%%%-----------------------------------------------------------------------------
%% POOL SLOT AND STATUS HELPERS
%%%-----------------------------------------------------------------------------
-spec h3_ets_update_status(ets:tid() | undefined, pid(), atom()) -> ok.
h3_ets_update_status(undefined, _Self, _Status) ->
    ok;
h3_ets_update_status(ConnEts, Self, Status) ->
    try ets:update_element(ConnEts, Self, {3, Status}) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

-doc "Give one live stream slot back to the pool when a stream ends.".
-spec release_stream_slot(#h3_data{}) -> ok.
release_stream_slot(#h3_data{stream_slots = undefined}) ->
    ok;
release_stream_slot(#h3_data{stream_slots = Slots}) ->
    nhttpc_stream_slots:release(Slots).

%%%-----------------------------------------------------------------------------
%% TELEMETRY
%%%-----------------------------------------------------------------------------
-spec emit_connect_error(#h3_data{}, term()) -> ok.
emit_connect_error(#h3_data{telemetry_config = false}, _Reason) ->
    ok;
emit_connect_error(
    #h3_data{
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

-spec emit_connect_stop(#h3_data{}) -> ok.
emit_connect_stop(#h3_data{telemetry_config = false}) ->
    ok;
emit_connect_stop(#h3_data{
    telemetry_config = Config,
    telemetry_connect_start = StartTime,
    host = Host,
    port = Port
}) ->
    nhttpc_telemetry:client_connect_stop(Config, StartTime, #{
        host => Host, port => Port, protocol => http3
    }).

-spec emit_idle_timeout(#h3_data{}) -> ok.
emit_idle_timeout(#h3_data{telemetry_config = false}) ->
    ok;
emit_idle_timeout(#h3_data{telemetry_config = Config, host = Host, port = Port}) ->
    nhttpc_telemetry:client_idle_timeout(Config, #{host => Host, port => Port}).

%%%-----------------------------------------------------------------------------
%% FORMAT STATUS HELPERS
%%%-----------------------------------------------------------------------------
-spec is_set(undefined | term()) -> boolean().
is_set(undefined) -> false;
is_set(_) -> true.

-spec sanitize_h3_data(#h3_data{}) -> map().
sanitize_h3_data(#h3_data{} = D) ->
    #{
        host => D#h3_data.host,
        port => D#h3_data.port,
        transport => udp,
        protocol => http3,
        ctx => is_set(D#h3_data.ctx),
        socket => is_set(D#h3_data.socket),
        h3_conn => is_set(D#h3_data.h3_conn),
        in_flight => map_size(D#h3_data.requests),
        goaway_received => D#h3_data.goaway_received,
        last_activity => D#h3_data.last_activity,
        idle_timeout => D#h3_data.idle_timeout,
        request_timeout => D#h3_data.request_timeout,
        connect_timeout => D#h3_data.connect_timeout,
        ready_waiters => length(D#h3_data.ready_waiters),
        conn_ets => is_set(D#h3_data.conn_ets),
        stream_slots => is_set(D#h3_data.stream_slots)
    }.
