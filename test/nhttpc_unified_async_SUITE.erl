%%%-----------------------------------------------------------------------------
%%% @doc Common Test suite covering the nhttpc 1.0 unified async protocol.
%%%
%%% Exercises the eight base cancel-no-leak cases from
%%% `nhttpc_api_proposal.md` plus the hardening cases (TLS handshake,
%%% pool race after conn death, H3 mid-drain, upload mid-file-read, and
%%% the send/flush race window).
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_unified_async_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    cancel_between_send_return_and_socket_flush/1,
    cancel_during_tls_handshake/1,
    cancel_h3_during_quic_drain/1,
    cancel_mixed_owners_under_load/1,
    cancel_one_stream_does_not_kill_others_on_h2/1,
    cancel_one_stream_does_not_kill_others_on_h3/1,
    cancel_pooled_handle_after_conn_died/1,
    cancel_request_leaves_empty_mailbox/1,
    cancel_stream_leaves_empty_mailbox/1,
    cancel_upload_mid_file_read/1,
    cancel_ws_leaves_empty_mailbox/1,
    connection_reusable_after_cancel/1,
    flush_is_idempotent/1,
    ws_cancel_initiates_normal_close/1,
    ws_on_dead_handle_returns_typed_error/1,
    ws_open_returns_handle_and_emits_envelope_events/1,
    ws_remote_close_emits_done_remote_closed/1,
    ws_send_via_handle_works/1
]).

%% WebSocket handler exports for the inline test server.
-export([init/1, handle_request/2, handle_ws_frame/3]).

%%%-----------------------------------------------------------------------------
%%% CT BOILERPLATE
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, http1},
        {group, http2},
        {group, http3},
        {group, ws},
        {group, hardening}
    ].

groups() ->
    [
        {http1, [], [
            cancel_request_leaves_empty_mailbox,
            cancel_stream_leaves_empty_mailbox,
            cancel_ws_leaves_empty_mailbox,
            cancel_mixed_owners_under_load,
            flush_is_idempotent
        ]},
        {http2, [], [
            cancel_one_stream_does_not_kill_others_on_h2,
            connection_reusable_after_cancel,
            cancel_between_send_return_and_socket_flush
        ]},
        {http3, [], [
            cancel_one_stream_does_not_kill_others_on_h3,
            cancel_h3_during_quic_drain
        ]},
        {ws, [], [
            ws_open_returns_handle_and_emits_envelope_events,
            ws_send_via_handle_works,
            ws_remote_close_emits_done_remote_closed,
            ws_cancel_initiates_normal_close,
            ws_on_dead_handle_returns_typed_error
        ]},
        {hardening, [], [
            cancel_during_tls_handshake,
            cancel_pooled_handle_after_conn_died,
            cancel_upload_mid_file_read
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    PrivDir = proplists:get_value(priv_dir, Config, ""),
    ProjectRoot = find_project_root(PrivDir),
    ConfDir = filename:join([ProjectRoot, "test", "conf"]),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_file(CertFile) andalso filelib:is_file(KeyFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> Config
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(http1, Config) ->
    case start_h1_server() of
        {ok, ServerPid, Port} ->
            [{server_pid, ServerPid}, {server_port, Port} | Config];
        {error, Reason} ->
            ct:fail("Failed to start H1 server: ~p", [Reason])
    end;
init_per_group(http2, Config) ->
    case has_tls(Config) of
        true ->
            CertFile = ?config(certfile, Config),
            KeyFile = ?config(keyfile, Config),
            case start_h2_server(CertFile, KeyFile) of
                {ok, ServerPid, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to start H2 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(ws, Config) ->
    case start_ws_server() of
        {ok, ServerPid, Port} ->
            [{server_pid, ServerPid}, {server_port, Port} | Config];
        {error, Reason} ->
            ct:fail("Failed to start WS server: ~p", [Reason])
    end;
init_per_group(http3, Config) ->
    case has_tls(Config) of
        true ->
            CertFile = ?config(certfile, Config),
            KeyFile = ?config(keyfile, Config),
            case start_h3_server(CertFile, KeyFile) of
                {ok, ServerPid, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    {skip, {h3_server_failed, Reason}}
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(hardening, Config) ->
    case start_h1_server() of
        {ok, ServerPid, Port} ->
            [{server_pid, ServerPid}, {server_port, Port} | Config];
        {error, Reason} ->
            ct:fail("Failed to start H1 server: ~p", [Reason])
    end.

end_per_group(_Group, Config) ->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok.

%%%-----------------------------------------------------------------------------
%%% CASES
%%%-----------------------------------------------------------------------------

cancel_request_leaves_empty_mailbox(Config) ->
    Port = ?config(server_port, Config),
    {ok, Handle} = nhttpc:send(slow_request(Port), #{}),
    ok = nhttpc:cancel(Handle),
    ?assertEqual(0, count_handle_events(Handle)),
    ok.

cancel_stream_leaves_empty_mailbox(Config) ->
    %% Smoke: full streaming events arrive in plan 07. Here we exercise
    %% the same cancel contract on a send/2 handle pointed at the large
    %% download endpoint.
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/large">>)),
    {ok, Handle} = nhttpc:send(Req, #{timeouts => #{request => 10000}}),
    ok = nhttpc:cancel(Handle),
    ?assertEqual(0, count_handle_events(Handle)),
    ok.

cancel_ws_leaves_empty_mailbox(_Config) ->
    {ServerPid, Port} = start_ws_handler_server(cancel_ws_leak_ref),
    URI = ws_uri(Port, <<"/ws">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = drain_ws_ready(Handle, 5000),
    ok = nhttpc:cancel(Handle),
    ?assertEqual(0, count_handle_events(Handle)),
    nhttpc_test_server:stop(ServerPid),
    ok.

cancel_mixed_owners_under_load(Config) ->
    Port = ?config(server_port, Config),
    Parent = self(),
    NumOwners = 4,
    Owners = [
        spawn_link(fun() -> mixed_owner(Parent, Port, N) end)
     || N <- lists:seq(1, NumOwners)
    ],
    [
        receive
            {owner_done, Pid, Count} ->
                ?assertEqual(0, Count, {owner_leak, Pid})
        after 15000 ->
            ct:fail("owner did not finish in time")
        end
     || _ <- Owners
    ],
    ok.

cancel_one_stream_does_not_kill_others_on_h2(Config) ->
    Port = ?config(server_port, Config),
    ConnOpts = h2_conn_opts(Port),
    {ok, ConnPid} = nhttpc_conn:start_link(ConnOpts),
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    {Req, _Url} = h2_slow_req(),
    {ok, CancelStream} = nhttpc:send_request(ConnPid, Req),
    {ok, KeepStream1} = nhttpc:send_request(ConnPid, h2_fast_req()),
    {ok, KeepStream2} = nhttpc:send_request(ConnPid, h2_fast_req()),
    CancelHandle = nhttpc_handle:new(ConnPid, CancelStream, request),
    ok = nhttpc:cancel(CancelHandle),
    ok = await_conn_ok(ConnPid, KeepStream1, 10000),
    ok = await_conn_ok(ConnPid, KeepStream2, 10000),
    ?assertEqual(0, count_handle_events(CancelHandle)),
    ok = nhttpc_conn:stop(ConnPid),
    ok.

cancel_one_stream_does_not_kill_others_on_h3(Config) ->
    Port = ?config(server_port, Config),
    ConnOpts = h3_conn_opts(Port),
    {ok, ConnPid} = nhttpc_h3_conn:start_link(ConnOpts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    SlowReq = h3_slow_req(),
    {ok, CancelStream} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    {ok, KeepStream1} = nhttpc_h3_conn:send_async(ConnPid, h3_fast_req(), self()),
    {ok, KeepStream2} = nhttpc_h3_conn:send_async(ConnPid, h3_fast_req(), self()),
    CancelHandle = nhttpc_handle:new(ConnPid, CancelStream, request),
    ok = nhttpc:cancel(CancelHandle),
    ok = await_conn_ok(ConnPid, KeepStream1, 10000),
    ok = await_conn_ok(ConnPid, KeepStream2, 10000),
    ?assertEqual(0, count_handle_events(CancelHandle)),
    ok = nhttpc_h3_conn:stop(ConnPid),
    ok.

flush_is_idempotent(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    {ok, Handle} = nhttpc:send(Req, #{}),
    {ok, _Resp} = nhttpc:await(Handle, 5000),
    %% A {done, normal} may linger after await; flush should pick it up,
    %% and a second call must be a no-op rather than blocking.
    ok = nhttpc:flush(Handle),
    ok = nhttpc:flush(Handle),
    ok = nhttpc:flush(Handle),
    ?assertEqual(0, count_handle_events(Handle)),
    ok.

ws_open_returns_handle_and_emits_envelope_events(Config) ->
    Port = ?config(server_port, Config),
    URI = ws_uri(Port, <<"/ws">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ?assert(nhttpc_handle:is_handle(Handle)),
    ?assertEqual(ws, nhttpc_handle:kind(Handle)),
    receive
        {nhttpc, Handle, {ws_ready, ConnInfo}} ->
            ?assertMatch(#{}, ConnInfo)
    after 5000 ->
        ct:fail(no_ws_ready)
    end,
    ok = nhttpc:cancel(Handle),
    ?assertEqual(0, count_handle_events(Handle)),
    ok.

ws_send_via_handle_works(Config) ->
    Port = ?config(server_port, Config),
    URI = ws_uri(Port, <<"/ws">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = drain_ws_ready(Handle, 5000),
    ok = nhttpc:ws_send(Handle, {text, <<"unified-hi">>}),
    receive
        {nhttpc, Handle, {ws, {text, <<"unified-hi">>}}} -> ok
    after 5000 ->
        ct:fail(no_echo_frame)
    end,
    ok = nhttpc:cancel(Handle),
    ?assertEqual(0, count_handle_events(Handle)),
    ok.

ws_remote_close_emits_done_remote_closed(Config) ->
    Port = ?config(server_port, Config),
    URI = ws_uri(Port, <<"/ws/send-close-code">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = drain_ws_ready(Handle, 5000),
    ok = nhttpc:ws_send(Handle, {text, <<"trigger">>}),
    Result = wait_for_terminal(Handle, 5000),
    ?assertMatch({done, {remote_closed, 1001}}, Result),
    nhttpc:flush(Handle),
    ?assertEqual(0, count_handle_events(Handle)),
    ok.

ws_cancel_initiates_normal_close(Config) ->
    Port = ?config(server_port, Config),
    URI = ws_uri(Port, <<"/ws">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = drain_ws_ready(Handle, 5000),
    Pid = nhttpc_handle:pid(Handle),
    MRef = monitor(process, Pid),
    ok = nhttpc:cancel(Handle),
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 5000 ->
        ct:fail(ws_did_not_exit)
    end,
    ?assertEqual(0, count_handle_events(Handle)),
    ok.

ws_on_dead_handle_returns_typed_error(Config) ->
    Port = ?config(server_port, Config),
    URI = ws_uri(Port, <<"/ws">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = drain_ws_ready(Handle, 5000),
    Pid = nhttpc_handle:pid(Handle),
    MRef = monitor(process, Pid),
    ok = nhttpc:cancel(Handle),
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 5000 ->
        ct:fail(ws_did_not_exit)
    end,
    ?assertEqual(
        {error, {connect_error, closed}},
        nhttpc:ws_send(Handle, {text, <<"dead">>})
    ),
    ok.

connection_reusable_after_cancel(Config) ->
    Port = ?config(server_port, Config),
    ConnOpts = h2_conn_opts(Port),
    {ok, ConnPid} = nhttpc_conn:start_link(ConnOpts),
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    {SlowReq, _Url} = h2_slow_req(),
    {ok, CancelStream} = nhttpc:send_request(ConnPid, SlowReq),
    CancelHandle = nhttpc_handle:new(ConnPid, CancelStream, request),
    ok = nhttpc:cancel(CancelHandle),
    {ok, OkStream} = nhttpc:send_request(ConnPid, h2_fast_req()),
    ok = await_conn_ok(ConnPid, OkStream, 5000),
    ok = nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HARDENING CASES (plan 11)
%%%-----------------------------------------------------------------------------

cancel_during_tls_handshake(_Config) ->
    application:ensure_all_started(ssl),
    {ok, ListenSocket} = gen_tcp:listen(0, [
        binary, {active, false}, {reuseaddr, true}, {ip, {127, 0, 0, 1}}
    ]),
    {ok, Port} = inet:port(ListenSocket),
    AcceptPid = spawn(fun() -> hang_accept_loop(ListenSocket) end),
    BaseSslProcs = ssl_handshake_proc_count(),
    PortBin = integer_to_binary(Port),
    URL = <<"https://127.0.0.1:", PortBin/binary, "/path">>,
    {ok, Req} = nhttpc:url(get, URL),
    {ok, Handle} = nhttpc:send(Req, #{
        tls => #{verify => verify_none},
        timeouts => #{request => 30_000, connect => 30_000}
    }),
    timer:sleep(200),
    WorkerPid = nhttpc_handle:pid(Handle),
    MRef = monitor(process, WorkerPid),
    ok = nhttpc:cancel(Handle),
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 5000 ->
        ct:fail(worker_did_not_exit_after_cancel)
    end,
    timer:sleep(500),
    AfterSslProcs = ssl_handshake_proc_count(),
    ?assert(
        AfterSslProcs =< BaseSslProcs + 1,
        {orphan_ssl_processes, BaseSslProcs, AfterSslProcs}
    ),
    ?assertEqual(0, count_handle_events(Handle)),
    exit(AcceptPid, kill),
    try
        gen_tcp:close(ListenSocket)
    catch
        _:_ -> ok
    end,
    ok.

cancel_pooled_handle_after_conn_died(Config) ->
    Port = ?config(server_port, Config),
    PoolRef = hardening_pool_dead,
    {ok, SupPid} = nhttpc:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0,
        versions => [http1_1]
    }),
    unlink(SupPid),
    ok = nhttpc_pool:wait_ready(PoolRef, 5000),
    {ok, ConnPid} = nhttpc_pool:get_connection(PoolRef, 1000),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/2000">>)),
    {ok, Handle} = nhttpc:send(Req, #{
        via => {pool, PoolRef},
        timeouts => #{request => 10_000, checkout => 5_000}
    }),
    timer:sleep(150),
    ConnMRef = monitor(process, ConnPid),
    exit(ConnPid, kill),
    receive
        {'DOWN', ConnMRef, process, _, _} -> ok
    after 5000 ->
        ct:fail(conn_did_not_die)
    end,
    ok = nhttpc:cancel(Handle),
    ?assertEqual(0, count_handle_events(Handle)),
    try
        nhttpc:stop(SupPid)
    catch
        _:_ -> ok
    end,
    ok.

cancel_h3_during_quic_drain(Config) ->
    Port = ?config(server_port, Config),
    ConnOpts = h3_conn_opts(Port),
    {ok, ConnPid} = nhttpc_h3_conn:start_link(ConnOpts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10_000),
    {ok, SlowStream} = nhttpc_h3_conn:send_async(ConnPid, h3_slow_req(), self()),
    SlowHandle = nhttpc_handle:new(ConnPid, SlowStream, request),
    timer:sleep(100),
    ok = nhttpc:cancel(SlowHandle),
    {ok, FastStream} = nhttpc_h3_conn:send_async(ConnPid, h3_fast_req(), self()),
    ok = await_conn_ok(ConnPid, FastStream, 10_000),
    ?assertEqual(0, count_handle_events(SlowHandle)),
    ok = nhttpc_h3_conn:stop(ConnPid),
    ok.

cancel_upload_mid_file_read(Config) ->
    Port = ?config(server_port, Config),
    PrivDir = ?config(priv_dir, Config),
    UploadPath = filename:join(PrivDir, "cancel_upload.bin"),
    ChunkBin = binary:copy(<<"abcdef0123456789">>, 4096),
    Bytes = 10 * 1024 * 1024,
    ok = write_upload_payload(UploadPath, ChunkBin, Bytes),
    {ok, FdsBefore} = list_open_fds(),
    URL = url(Port, <<"/upload/large">>),
    {ok, Req0} = nhttpc:url(post, URL),
    Req = Req0#{
        headers => [
            {<<"content-type">>, <<"application/octet-stream">>},
            {<<"content-length">>, integer_to_binary(Bytes)}
        ]
    },
    {ok, Handle} = nhttpc:send(Req, #{
        body_from => {file, UploadPath},
        timeouts => #{request => 30_000}
    }),
    timer:sleep(50),
    WorkerPid = nhttpc_handle:pid(Handle),
    MRef = monitor(process, WorkerPid),
    ok = nhttpc:cancel(Handle),
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 5000 ->
        ct:fail(upload_worker_did_not_exit)
    end,
    timer:sleep(200),
    ?assertEqual(0, count_handle_events(Handle)),
    {ok, FdsAfter} = list_open_fds(),
    ?assert(
        FdsAfter =< FdsBefore + 1,
        {file_fd_leak, FdsBefore, FdsAfter}
    ),
    ok = file:delete(UploadPath),
    ok.

cancel_between_send_return_and_socket_flush(Config) ->
    Port = ?config(server_port, Config),
    ConnOpts = h2_conn_opts(Port),
    {ok, ConnPid} = nhttpc_conn:start_link(ConnOpts),
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    {SlowReq, _Url} = h2_slow_req(),
    sys:suspend(ConnPid),
    {ok, CancelStream} = nhttpc:send_request(ConnPid, SlowReq),
    CancelHandle = nhttpc_handle:new(ConnPid, CancelStream, request),
    Parent = self(),
    CancelPid = spawn(fun() ->
        ok = nhttpc:cancel(CancelHandle),
        Parent ! {cancel_done, self()}
    end),
    timer:sleep(50),
    sys:resume(ConnPid),
    receive
        {cancel_done, CancelPid} -> ok
    after 5000 ->
        ct:fail(cancel_did_not_return)
    end,
    {ok, OkStream} = nhttpc:send_request(ConnPid, h2_fast_req()),
    ok = await_conn_ok(ConnPid, OkStream, 5000),
    ?assertEqual(0, count_handle_events(CancelHandle)),
    ok = nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS — REQUESTS
%%%-----------------------------------------------------------------------------

slow_request(Port) ->
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/2000">>)),
    Req.

h2_slow_req() ->
    Req = #{
        method => get,
        path => <<"/slow/2000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {Req, <<"/slow/2000">>}.

h2_fast_req() ->
    #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    }.

h3_slow_req() ->
    #{
        method => get,
        path => <<"/slow">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    }.

h3_fast_req() ->
    #{
        method => get,
        path => <<"/hello">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    }.

url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"http://127.0.0.1:", PortBin/binary, Path/binary>>.

%%%-----------------------------------------------------------------------------
%%% HELPERS — SERVERS
%%%-----------------------------------------------------------------------------

start_ws_server() ->
    application:ensure_all_started(nhttp),
    case
        nhttp:start_link(unified_async_ws, #{
            port => 0,
            handler => ?MODULE,
            protocols => [http1_1],
            telemetry => false
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, Reason} ->
            {error, Reason}
    end.

start_ws_handler_server(Ref) ->
    application:ensure_all_started(nhttp),
    {ok, ServerPid} = nhttp:start_link(Ref, #{
        port => 0,
        handler => ?MODULE,
        protocols => [http1_1],
        telemetry => false
    }),
    unlink(ServerPid),
    {ok, Port} = nhttp:get_port(ServerPid),
    {ServerPid, Port}.

ws_uri(Port, Path) ->
    <<"ws://127.0.0.1:", (integer_to_binary(Port))/binary, Path/binary>>.

drain_ws_ready(Handle, Timeout) ->
    receive
        {nhttpc, Handle, {ws_ready, _}} -> ok
    after Timeout ->
        {error, timeout}
    end.

wait_for_terminal(Handle, Timeout) ->
    receive
        {nhttpc, Handle, {done, _} = Event} -> Event;
        {nhttpc, Handle, {error, _} = Event} -> Event;
        {nhttpc, Handle, _Other} -> wait_for_terminal(Handle, Timeout)
    after Timeout ->
        {error, terminal_timeout}
    end.

start_h1_server() ->
    case
        nhttp:start_link(unified_async_h1, #{
            port => 0,
            handler => nhttp_test_handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, Reason} ->
            {error, Reason}
    end.

start_h2_server(CertFile, KeyFile) ->
    case
        nhttp:start_link(unified_async_h2, #{
            port => 0,
            handler => nhttp_test_handler,
            versions => [http2, http1_1],
            tls => #{certfile => CertFile, keyfile => KeyFile}
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, Reason} ->
            {error, Reason}
    end.

start_h3_server(CertFile, KeyFile) ->
    case
        nhttp:start_link(unified_async_h3, #{
            port => 0,
            handler => nhttpc_h3_test_handler,
            versions => [http3],
            tls => #{certfile => CertFile, keyfile => KeyFile}
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, Reason} ->
            {error, Reason}
    end.

h2_conn_opts(Port) ->
    #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        verify => verify_none,
        protocols => [http2],
        connect_timeout => 5000,
        request_timeout => 30000,
        idle_timeout => 60000
    }.

h3_conn_opts(Port) ->
    #{
        host => <<"127.0.0.1">>,
        port => Port,
        connect_timeout => 5000,
        request_timeout => 30000,
        idle_timeout => 60000
    }.

%%%-----------------------------------------------------------------------------
%%% HELPERS — OWNERS, AWAIT, MAILBOX INSPECTION
%%%-----------------------------------------------------------------------------

mixed_owner(Parent, Port, _N) ->
    {ok, H1} = nhttpc:send(slow_request(Port), #{}),
    {ok, H2} = nhttpc:send(slow_request(Port), #{}),
    %% Cancel both — the worker drains its mailbox.
    ok = nhttpc:cancel(H1),
    ok = nhttpc:cancel(H2),
    Count = count_handle_events(H1) + count_handle_events(H2),
    Parent ! {owner_done, self(), Count}.

count_handle_events(Handle) ->
    count_handle_events(Handle, 0).

count_handle_events(Handle, Acc) ->
    receive
        {nhttpc, Handle, _} -> count_handle_events(Handle, Acc + 1)
    after 50 ->
        Acc
    end.

await_conn_ok(ConnPid, StreamRef, Timeout) ->
    receive
        {nhttpc_response, ConnPid, StreamRef, {ok, _Resp}} ->
            ok;
        {nhttpc_error, ConnPid, StreamRef, Reason} ->
            {error, Reason}
    after Timeout ->
        {error, timeout}
    end.

has_tls(Config) ->
    proplists:get_value(certfile, Config) =/= undefined.

hang_accept_loop(ListenSocket) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, _Sock} -> hang_accept_loop(ListenSocket);
        {error, _} -> ok
    end.

ssl_handshake_proc_count() ->
    length([
        P
     || P <- erlang:processes(),
        is_ssl_handshake_proc(P)
    ]).

is_ssl_handshake_proc(P) ->
    case process_info(P, initial_call) of
        {initial_call, {ssl_gen_statem, init, _}} -> true;
        {initial_call, {tls_gen_connection, init, _}} -> true;
        {initial_call, {tls_connection_sup, _, _}} -> false;
        _ -> false
    end.

write_upload_payload(Path, Chunk, TotalBytes) ->
    {ok, Fd} = file:open(Path, [write, binary, raw]),
    try
        write_upload_loop(Fd, Chunk, TotalBytes)
    after
        file:close(Fd)
    end.

write_upload_loop(_Fd, _Chunk, Remaining) when Remaining =< 0 ->
    ok;
write_upload_loop(Fd, Chunk, Remaining) ->
    Size = byte_size(Chunk),
    case Remaining >= Size of
        true ->
            ok = file:write(Fd, Chunk),
            write_upload_loop(Fd, Chunk, Remaining - Size);
        false ->
            Slice = binary:part(Chunk, 0, Remaining),
            ok = file:write(Fd, Slice),
            ok
    end.

list_open_fds() ->
    case file:list_dir("/proc/self/fd") of
        {ok, Entries} -> {ok, length(Entries)};
        {error, _} -> {ok, 0}
    end.

find_project_root(Dir) ->
    RebarConfig = filename:join(Dir, "rebar.config"),
    case filelib:is_file(RebarConfig) of
        true ->
            Dir;
        false ->
            Parent = filename:dirname(Dir),
            case Parent =:= Dir of
                true -> ".";
                false -> find_project_root(Parent)
            end
    end.

%%%-----------------------------------------------------------------------------
%%% WEBSOCKET TEST HANDLER
%%%-----------------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_request(#{path := <<"/ws">>}, State) ->
    {upgrade, websocket, State};
handle_request(#{path := <<"/ws/send-close-code">>}, State) ->
    {upgrade, websocket, State#{action => send_close_code}};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

handle_ws_frame({text, _Data}, _Session, #{action := send_close_code} = State) ->
    {close, 1001, <<"Going away">>, State};
handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, Data}, State};
handle_ws_frame({binary, Data}, _Session, State) ->
    {reply, {binary, Data}, State}.
