%%%-----------------------------------------------------------------------------
%%% @doc HTTP/3 client connection process test suite.
%%%
%%% Integration tests for nhttpc_h3_conn using a real QUIC server
%%% (nquic listener + nhttp_h3_conn). Exercises connection lifecycle,
%%% request/response handling, multiplexing, cancellation, timeouts,
%%% draining, and sys callbacks.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_h3_conn_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(H3_SLOT_CAP, 100).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - CT CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    all/0,
    suite/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - LIFECYCLE TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_start_link/1,
    h3_stop/1,
    h3_connect_timeout/1,
    h3_connect_error/1,
    h3_ingests_server_settings/1,
    h3_pool_integration/1,
    h3_connection_error_paths/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - REQUEST TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_get_request/1,
    h3_post_with_body/1,
    h3_empty_body/1,
    h3_multiple_sequential/1,
    h3_large_response/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - MULTIPLEXING TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_concurrent_requests/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - CANCEL TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_cancel_unknown/1,
    h3_cancel_in_flight/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - TIMEOUT TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_request_timeout/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - PING TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_ping_ready/1,
    h3_ping_after_stop/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - DRAINING TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_draining_rejects_wait_ready/1,
    h3_draining_send_rejected/1,
    h3_draining_stop/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - SYS CALLBACK TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_sys_get_state/1,
    h3_sys_callbacks/1,
    h3_stop_kill/1,
    h3_call_timeout/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - WIRE PATH TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_stream_multi_frame/1,
    h3_stream_trailers/1,
    h3_stream_reset_abort/1,
    h3_handler_crash/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - FAULT LOOP TESTS
%%%-----------------------------------------------------------------------------
-export([
    h3_goaway_drains/1,
    h3_idle_close/1,
    h3_idle_reschedule/1,
    h3_idle_infinity/1,
    h3_idle_with_pending/1,
    h3_ready_exit_parent/1,
    h3_ready_exit_signals/1,
    h3_draining_message_matrix/1,
    h3_draining_request_timeout/1,
    h3_host_variants/1,
    h3_port_443_authority/1,
    h3_ets_tracking/1,
    h3_ets_badarg/1,
    h3_ready_unified_cancel/1,
    h3_idle_close_no_pool/1,
    h3_draining_exit_parent/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 3}}].

all() ->
    [
        {group, lifecycle},
        {group, requests},
        {group, multiplexing},
        {group, cancel},
        {group, timeout},
        {group, ping},
        {group, draining},
        {group, sys_callbacks},
        {group, wire_paths},
        {group, faults}
    ].

groups() ->
    [
        {lifecycle, [sequence], [
            h3_start_link,
            h3_stop,
            h3_ingests_server_settings,
            h3_connect_timeout,
            h3_connect_error,
            h3_pool_integration,
            h3_connection_error_paths
        ]},
        {requests, [sequence], [
            h3_get_request,
            h3_post_with_body,
            h3_empty_body,
            h3_multiple_sequential,
            h3_large_response
        ]},
        {multiplexing, [sequence], [
            h3_concurrent_requests
        ]},
        {cancel, [sequence], [
            h3_cancel_unknown,
            h3_cancel_in_flight
        ]},
        {timeout, [sequence], [
            h3_request_timeout
        ]},
        {ping, [sequence], [
            h3_ping_ready,
            h3_ping_after_stop
        ]},
        {draining, [sequence], [
            h3_draining_rejects_wait_ready,
            h3_draining_send_rejected,
            h3_draining_stop
        ]},
        {sys_callbacks, [sequence], [
            h3_sys_get_state,
            h3_sys_callbacks,
            h3_stop_kill,
            h3_call_timeout
        ]},
        {wire_paths, [sequence], [
            h3_stream_multi_frame,
            h3_stream_trailers,
            h3_stream_reset_abort,
            h3_handler_crash
        ]},
        {faults, [], [
            h3_goaway_drains,
            h3_idle_close,
            h3_idle_reschedule,
            h3_idle_infinity,
            h3_idle_with_pending,
            h3_ready_exit_parent,
            h3_ready_exit_signals,
            h3_draining_message_matrix,
            h3_draining_request_timeout,
            h3_host_variants,
            h3_port_443_authority,
            h3_ets_tracking,
            h3_ets_badarg,
            h3_ready_unified_cancel,
            h3_idle_close_no_pool,
            h3_draining_exit_parent
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    application:ensure_all_started(crypto),
    PrivDir = proplists:get_value(priv_dir, Config, ""),
    ProjectRoot = find_project_root(PrivDir),
    ConfDir = filename:join([ProjectRoot, "test", "conf"]),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_file(CertFile) andalso filelib:is_file(KeyFile) of
        true ->
            [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false ->
            ct:log("Warning: TLS certificates not found at ~s", [ConfDir]),
            Config
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) when
    Group =:= lifecycle;
    Group =:= requests;
    Group =:= multiplexing;
    Group =:= cancel;
    Group =:= timeout;
    Group =:= ping;
    Group =:= draining;
    Group =:= sys_callbacks;
    Group =:= wire_paths;
    Group =:= faults
->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            case start_h3_server(CertFile, KeyFile) of
                {ok, ServerPid, Port} ->
                    Config1 = [{h3_server, ServerPid}, {server_port, Port} | Config],
                    maybe_start_shared_conn(Group, Port, Config1);
                {error, Reason} ->
                    ct:fail("Failed to start H3 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when
    Group =:= lifecycle;
    Group =:= requests;
    Group =:= multiplexing;
    Group =:= cancel;
    Group =:= timeout;
    Group =:= ping;
    Group =:= draining;
    Group =:= sys_callbacks;
    Group =:= wire_paths;
    Group =:= faults
->
    case proplists:get_value(shared_conn, Config) of
        undefined ->
            ok;
        ConnPid ->
            try
                nhttpc_h3_conn:stop(ConnPid)
            catch
                _:_ -> ok
            end
    end,
    nhttpc_test_server:stop(proplists:get_value(h3_server, Config)),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% LIFECYCLE TESTS
%%%-----------------------------------------------------------------------------

h3_start_link(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = conn_opts(Port),
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ?assert(is_pid(ConnPid)),
    ?assert(is_process_alive(ConnPid)),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 5000),
    nhttpc_h3_conn:stop(ConnPid),
    ok.

h3_stop(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = conn_opts(Port),
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 5000),
    ?assert(is_process_alive(ConnPid)),
    ok = nhttpc_h3_conn:stop(ConnPid),
    timer:sleep(100),
    ?assertNot(is_process_alive(ConnPid)),
    ok.

h3_connect_timeout(_Config) ->
    process_flag(trap_exit, true),
    Opts = #{
        host => <<"10.255.255.1">>,
        port => 44333,
        connect_timeout => 500,
        request_timeout => 5000,
        idle_timeout => 60000
    },
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 5000 ->
        ct:fail("Expected connect timeout EXIT")
    end,
    ?assertNot(is_process_alive(ConnPid)),
    process_flag(trap_exit, false),
    ok.

h3_connect_error(_Config) ->
    process_flag(trap_exit, true),
    Opts = #{
        host => <<"thishostdoesnotexist.invalid">>,
        port => 44333,
        connect_timeout => 2000,
        request_timeout => 5000,
        idle_timeout => 60000
    },
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 5000 ->
        ct:fail("Expected connect error EXIT")
    end,
    ?assertNot(is_process_alive(ConnPid)),
    process_flag(trap_exit, false),
    ok.

%% Start connection with pool_mgr to exercise notify_pool_ready path.
%% The mock pool manager receives gen_server:cast messages from the conn.
h3_pool_integration(Config) ->
    Port = proplists:get_value(server_port, Config),
    TestPid = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(TestPid) end),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        pool_mgr => MockMgr,
        connect_timeout => 5000,
        request_timeout => 30000,
        idle_timeout => 60000
    },
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    %% Verify that notify_pool_ready was called
    receive
        %% HTTP/3 reports no peer cap yet, so the pool applies its own ceiling.
        {mock_pool_mgr, {connection_ready, ConnPid, http3, undefined}} -> ok
    after 5000 ->
        ct:fail("Expected connection_ready notification from pool_mgr")
    end,
    nhttpc_h3_conn:stop(ConnPid),
    MockMgr ! stop,
    ok.

%% Connection to a non-existent QUIC port exercises error notification
%% via notify_pool_error when the connection attempt fails.
h3_connection_error_paths(_Config) ->
    process_flag(trap_exit, true),
    TestPid = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(TestPid) end),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => 19999,
        pool_mgr => MockMgr,
        connect_timeout => 1000,
        request_timeout => 5000,
        idle_timeout => 60000
    },
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    %% Wait for the connection process to die
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 5000 ->
        ct:fail("Expected connect error EXIT")
    end,
    %% Verify that notify_pool_error was called
    receive
        {mock_pool_mgr, {connection_error, ConnPid, _Reason}} -> ok
    after 5000 ->
        ct:fail("Expected connection_error notification from pool_mgr")
    end,
    MockMgr ! stop,
    process_flag(trap_exit, false),
    ok.

%%%-----------------------------------------------------------------------------
%%% REQUEST TESTS
%%%-----------------------------------------------------------------------------

h3_get_request(Config) ->
    {ok, ConnPid} = get_conn(Config),
    Req = #{
        method => get,
        path => <<"/hello">>,
        headers => [],
        body => <<>>
    },
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    ?assert(is_reference(Ref)),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(<<"Hello!">>, maps:get(body, Resp, <<>>))
    after 10000 ->
        ct:fail("No response received for GET /hello")
    end,
    ok.

h3_post_with_body(Config) ->
    {ok, ConnPid} = get_conn(Config),
    Body = <<"test body content">>,
    Req = #{
        method => post,
        path => <<"/echo">>,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => Body
    },
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(Body, maps:get(body, Resp, <<>>))
    after 10000 ->
        ct:fail("No response received for POST /echo")
    end,
    ok.

h3_empty_body(Config) ->
    {ok, ConnPid} = get_conn(Config),
    Req = #{
        method => get,
        path => <<"/empty">>,
        headers => [],
        body => <<>>
    },
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(<<>>, maps:get(body, Resp, <<>>))
    after 10000 ->
        ct:fail("No response received for GET /empty")
    end,
    ok.

h3_multiple_sequential(Config) ->
    {ok, ConnPid} = get_conn(Config),
    lists:foreach(
        fun(N) ->
            Path = <<"/hello">>,
            Req = #{
                method => get,
                path => Path,
                headers => [],
                body => <<>>
            },
            {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
            receive
                {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
                    ?assertEqual(200, maps:get(status, Resp))
            after 10000 ->
                ct:fail("No response for sequential request ~p", [N])
            end
        end,
        lists:seq(1, 5)
    ),
    ok.

%% The 1300-byte position-dependent /large body spans multiple QUIC
%% reads, guarding the new {ok, Frame, Rest} | {more, N} decode/1
%% carry-over: any byte loss/dup/reorder fails the exact-body assert.
h3_large_response(Config) ->
    {ok, ConnPid} = get_conn(Config),
    Req = #{
        method => get,
        path => <<"/large">>,
        headers => [],
        body => <<>>
    },
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    ExpectedBody = <<<<(I rem 256)>> || I <- lists:seq(1, 1300)>>,
    ExpectedSize = byte_size(ExpectedBody),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(ExpectedSize, byte_size(maps:get(body, Resp, <<>>))),
            ?assertEqual(ExpectedBody, maps:get(body, Resp, <<>>))
    after 15000 ->
        ct:fail("No response received for GET /large")
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% MULTIPLEXING TESTS
%%%-----------------------------------------------------------------------------

h3_concurrent_requests(Config) ->
    {ok, ConnPid} = get_conn(Config),
    Refs = lists:map(
        fun(_N) ->
            Req = #{
                method => get,
                path => <<"/hello">>,
                headers => [],
                body => <<>>
            },
            {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
            Ref
        end,
        lists:seq(1, 5)
    ),
    Responses = collect_responses(ConnPid, Refs, 15000),
    ?assertEqual(5, length(Responses)),
    lists:foreach(
        fun({_Ref, Resp}) ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(<<"Hello!">>, maps:get(body, Resp, <<>>))
        end,
        Responses
    ),
    ok.

%%%-----------------------------------------------------------------------------
%%% CANCEL TESTS
%%%-----------------------------------------------------------------------------

h3_cancel_unknown(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    FakeRef = make_ref(),
    ?assertEqual({error, not_found}, nhttpc_h3_conn:cancel(ConnPid, FakeRef)),
    nhttpc_h3_conn:stop(ConnPid),
    ok.

h3_cancel_in_flight(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    Req = #{
        method => get,
        path => <<"/slow">>,
        headers => [],
        body => <<>>
    },
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    timer:sleep(100),
    ?assertEqual(ok, nhttpc_h3_conn:cancel(ConnPid, Ref)),
    timer:sleep(500),
    nhttpc_h3_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% TIMEOUT TESTS
%%%-----------------------------------------------------------------------------

h3_request_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        connect_timeout => 5000,
        request_timeout => 500,
        idle_timeout => 60000
    },
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 5000),
    Req = #{
        method => get,
        path => <<"/slow">>,
        headers => [],
        body => <<>>
    },
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ?assertMatch({request, #{type := request_timeout}}, Reason)
    after 5000 ->
        ct:fail("Expected request timeout error")
    end,
    nhttpc_h3_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% PING TESTS
%%%-----------------------------------------------------------------------------

h3_ping_ready(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    ?assertEqual(pong, nhttpc_h3_conn:ping(ConnPid, 5000)),
    nhttpc_h3_conn:stop(ConnPid),
    ok.

h3_ping_after_stop(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    ok = nhttpc_h3_conn:stop(ConnPid),
    timer:sleep(100),
    Result = nhttpc_h3_conn:ping(ConnPid, 1000),
    ?assertMatch({error, _}, Result),
    ok.

%%%-----------------------------------------------------------------------------
%%% DRAINING TESTS
%%%-----------------------------------------------------------------------------

h3_draining_rejects_wait_ready(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    ConnPid ! {goaway_test, self()},
    timer:sleep(200),
    nhttpc_h3_conn:stop(ConnPid),
    ok.

%% Put connection into draining state via sys:replace_state, then verify
%% that new send_async requests are rejected with a pool_draining error.
%% We first send a slow request to keep the draining loop alive (it exits
%% when there are no pending requests), then flip to draining and verify
%% that a second send is rejected.
h3_draining_send_rejected(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    %% Send a slow request so there is a pending in-flight request.
    %% This prevents the draining_loop from exiting immediately.
    SlowReq = #{
        method => get,
        path => <<"/slow">>,
        headers => [],
        body => <<>>
    },
    {ok, _SlowRef} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    %% Wait a moment for the request to be dispatched on a stream
    timer:sleep(200),
    %% Suspend, flip to draining state (goaway_received = true)
    sys:suspend(ConnPid),
    sys:replace_state(ConnPid, fun({ready, Data}) ->
        {draining, setelement(14, Data, true)}
    end),
    %% Enqueue a new send while the process is still suspended
    Req = #{
        method => get,
        path => <<"/hello">>,
        headers => [],
        body => <<>>
    },
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    %% Resume: system_continue dispatches to enter_draining_loop
    sys:resume(ConnPid),
    receive
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ?assertMatch({pool, _}, Reason)
    after 5000 ->
        ct:fail("Expected pool_draining error for send in draining state")
    end,
    try
        nhttpc_h3_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    process_flag(trap_exit, false),
    ok.

%% Stop a connection that is in draining state.
%% Exercises the {stop, _From} clause in draining_loop.
h3_draining_stop(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    %% Send a slow request so there is a pending in-flight request.
    %% This keeps the draining loop alive long enough to receive stop.
    SlowReq = #{
        method => get,
        path => <<"/slow">>,
        headers => [],
        body => <<>>
    },
    {ok, _SlowRef} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    timer:sleep(200),
    %% Suspend, set goaway, enqueue stop, then resume into draining
    sys:suspend(ConnPid),
    sys:replace_state(ConnPid, fun({ready, Data}) ->
        {draining, setelement(14, Data, true)}
    end),
    ConnPid ! {stop, self()},
    sys:resume(ConnPid),
    %% Wait for the process to terminate
    receive
        {'EXIT', ConnPid, normal} -> ok
    after 5000 ->
        ct:fail("Expected draining connection to exit on stop")
    end,
    ?assertNot(is_process_alive(ConnPid)),
    process_flag(trap_exit, false),
    ok.

%%%-----------------------------------------------------------------------------
%%% SYS CALLBACK TESTS
%%%-----------------------------------------------------------------------------

h3_sys_get_state(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    {status, ConnPid, _, _} = sys:get_status(ConnPid),
    nhttpc_h3_conn:stop(ConnPid),
    ok.

%% Direct calls into the exported sys callbacks plus driving them through
%% sys. Covers system_get_state/1, system_code_change/4, format_status/1
%% (with sanitize_h3_data/1 + is_set/1 redaction) and system_terminate/4
%% for both a live ctx and an undefined ctx.
h3_sys_callbacks(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    {ready, RData} = State = sys:get_state(ConnPid),

    ?assertEqual({ok, State}, nhttpc_h3_conn:system_get_state(State)),
    ?assertEqual(
        {ok, State}, nhttpc_h3_conn:system_code_change(State, nhttpc_h3_conn, "0", [])
    ),

    Out = nhttpc_h3_conn:format_status(#{state => {ready, RData}}),
    #{state := {ready, San}} = Out,
    ?assert(is_map(San)),
    ?assertEqual(true, maps:get(ctx, San)),
    ?assertEqual(true, maps:get(socket, San)),
    ?assertEqual(http3, maps:get(protocol, San)),
    %% the raw opaque ctx must never leak into status output
    RawCtx = element(7, RData),
    ?assertNot(lists:member(RawCtx, maps:values(San))),

    Passthrough = #{other => value, log => []},
    ?assertEqual(Passthrough, nhttpc_h3_conn:format_status(Passthrough)),

    {Runner1, M1} = spawn_monitor(fun() ->
        nhttpc_h3_conn:system_terminate(shutdown, self(), [], {ready, RData})
    end),
    receive
        {'DOWN', M1, process, Runner1, shutdown} -> ok
    after 5000 -> ct:fail("system_terminate (live ctx) did not exit shutdown")
    end,

    {Runner2, M2} = spawn_monitor(fun() ->
        nhttpc_h3_conn:system_terminate(
            shutdown, self(), [], {ready, setelement(7, RData, undefined)}
        )
    end),
    receive
        {'DOWN', M2, process, Runner2, shutdown} -> ok
    after 5000 -> ct:fail("system_terminate (undefined ctx) did not exit shutdown")
    end,

    %% Drive system_code_change through sys as well.
    ok = sys:suspend(ConnPid),
    ok = sys:change_code(ConnPid, nhttpc_h3_conn, "0", []),
    ok = sys:resume(ConnPid),
    try
        nhttpc_h3_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    ok.

%% stop/1 on a process that never answers {stop, _} falls through to the
%% monitor timeout, demonitor and exit(Pid, kill).
h3_stop_kill(_Config) ->
    Pid = spawn(fun() ->
        receive
            never -> ok
        end
    end),
    MRef = monitor(process, Pid),
    ok = nhttpc_h3_conn:stop(Pid),
    receive
        {'DOWN', MRef, process, Pid, killed} -> ok
    after 7000 -> ct:fail("stop/1 did not kill an unresponsive process")
    end,
    ?assertNot(is_process_alive(Pid)),
    ok.

%% call/3 returns {error, timeout} when the connection cannot reply
%% within the deadline (suspended via sys).
h3_call_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    ok = sys:suspend(ConnPid),
    ?assertEqual({error, timeout}, nhttpc_h3_conn:ping(ConnPid, 50)),
    ok = sys:resume(ConnPid),
    nhttpc_h3_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% WIRE PATH TESTS
%%%-----------------------------------------------------------------------------

%% Multi-frame streamed body: intermediate DATA frames arrive nofin,
%% the last one fin.
h3_stream_multi_frame(Config) ->
    {ok, ConnPid} = get_conn(Config),
    Req = #{method => get, path => <<"/stream-multi">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(<<"part-one part-two part-three">>, maps:get(body, Resp, <<>>))
    after 10000 -> ct:fail("no /stream-multi response")
    end,
    ok.

%% Streamed body followed by trailers exercises the {trailers, _} event.
h3_stream_trailers(Config) ->
    {ok, ConnPid} = get_conn(Config),
    Req = #{method => get, path => <<"/stream-trailers">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(<<"with-trailers">>, maps:get(body, Resp, <<>>)),
            Trailers = maps:get(trailers, Resp, []),
            ?assert(lists:keymember(<<"x-checksum">>, 1, Trailers))
    after 10000 -> ct:fail("no /stream-trailers response")
    end,
    ok.

%% Server aborts the request -> RST_STREAM -> stream reset delivered to
%% the caller as an error.
h3_stream_reset_abort(Config) ->
    {ok, ConnPid} = get_conn(Config),
    Req = #{method => get, path => <<"/abort">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_error, ConnPid, Ref, _Reason} -> ok;
        {nhttpc_response, ConnPid, Ref, {error, _}} -> ok
    after 10000 -> ct:fail("no error for /abort")
    end,
    ok.

%% Handler raises -> server resets the stream -> caller gets an error.
h3_handler_crash(Config) ->
    {ok, ConnPid} = get_conn(Config),
    Req = #{method => get, path => <<"/crash">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_error, ConnPid, Ref, _Reason} -> ok;
        {nhttpc_response, ConnPid, Ref, {error, _}} -> ok
    after 10000 -> ct:fail("no error for /crash")
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% FAULT LOOP TESTS
%%%-----------------------------------------------------------------------------

%% A server-initiated GOAWAY (via nhttp:drain) on an idle connection
%% drives the goaway event: notify the pool, update the ETS status, then
%% exit normal (no in-flight requests -> no close_all/RST coalescing race
%% with the subsequent CONNECTION_CLOSE datagram).
h3_goaway_drains(Config) ->
    process_flag(trap_exit, true),
    {Server, Port} = start_extra_server(h3_goaway_srv, Config),
    TestPid = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(TestPid) end),
    Tab = ets:new(h3_goaway_ets, [public, set]),
    Opts = (conn_opts(Port))#{pool_mgr => MockMgr, conn_ets => Tab},
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    true = ets:insert(Tab, {ConnPid, undefined, ready, 1}),
    %% A request round-trip settles the connection so the subsequent
    %% GOAWAY datagram is processed deterministically (draining a brand
    %% new connection races handshake completion).
    HReq = #{method => get, path => <<"/hello">>, headers => [], body => <<>>},
    {ok, HRef} = nhttpc_h3_conn:send_async(ConnPid, HReq, self()),
    receive
        {nhttpc_response, ConnPid, HRef, {ok, _}} -> ok
    after 10000 -> ct:fail("no /hello response before drain")
    end,
    CMRef = monitor(process, ConnPid),
    ok = nhttp:drain(Server),
    receive
        {mock_pool_mgr, {connection_draining, ConnPid}} -> ok
    after 6000 -> ct:fail("expected connection_draining from GOAWAY")
    end,
    receive
        {'DOWN', CMRef, process, ConnPid, normal} -> ok
    after 8000 -> ct:fail("conn did not exit normal after GOAWAY")
    end,
    ?assertEqual(draining, ets:lookup_element(Tab, ConnPid, 3)),
    MockMgr ! stop,
    nhttpc_test_server:stop(Server),
    process_flag(trap_exit, false),
    ok.

%% Idle check fires while idle and past the deadline -> emit telemetry,
%% notify pool, exit normal.
h3_idle_close(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    TestPid = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(TestPid) end),
    Opts = (conn_opts(Port))#{pool_mgr => MockMgr},
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    CMRef = monitor(process, ConnPid),
    ok = sys:suspend(ConnPid),
    %% idle_timeout = 0 makes the deadline fire on the next check_idle
    %% regardless of a concurrent socket packet touching last_activity.
    sys:replace_state(ConnPid, fun({ready, Data}) ->
        Old = erlang:monotonic_time(millisecond) - 1000000,
        D1 = setelement(18, Data, Old),
        D2 = setelement(17, D1, 0),
        {ready, D2}
    end),
    ok = sys:resume(ConnPid),
    ConnPid ! check_idle,
    receive
        {mock_pool_mgr, {connection_error, ConnPid, idle_timeout}} -> ok
    after 5000 -> ct:fail("expected idle_timeout pool error")
    end,
    receive
        {'DOWN', CMRef, process, ConnPid, normal} -> ok
    after 5000 -> ct:fail("conn did not exit normal on idle")
    end,
    MockMgr ! stop,
    process_flag(trap_exit, false),
    ok.

%% Idle check fires before the deadline -> reschedule, connection stays
%% alive and keeps serving requests.
h3_idle_reschedule(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    ConnPid ! check_idle,
    Req = #{method => get, path => <<"/hello">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, R}} ->
            ?assertEqual(200, maps:get(status, R))
    after 10000 -> ct:fail("no response after idle reschedule")
    end,
    nhttpc_h3_conn:stop(ConnPid),
    ok.

%% idle_timeout => infinity exercises start_idle_checker/quic_idle_timeout
%% infinity clauses and idle_check_interval's catch-all clause.
h3_idle_infinity(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = (conn_opts(Port))#{idle_timeout => infinity},
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    ConnPid ! check_idle,
    Req = #{method => get, path => <<"/hello">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, R}} ->
            ?assertEqual(200, maps:get(status, R))
    after 10000 -> ct:fail("no response with idle_timeout=infinity")
    end,
    nhttpc_h3_conn:stop(ConnPid),
    ok.

%% Idle check fires while a request is in flight -> the pending arm just
%% clears the timer ref and keeps looping.
h3_idle_with_pending(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    SlowReq = #{method => get, path => <<"/slow">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    {_, D} = sys:get_state(ConnPid),
    ?assert(map_size(element(12, D)) >= 1),
    ConnPid ! check_idle,
    receive
        {nhttpc_response, ConnPid, Ref, {ok, R}} ->
            ?assertEqual(200, maps:get(status, R))
    after 10000 -> ct:fail("no slow response after idle-with-pending")
    end,
    nhttpc_h3_conn:stop(ConnPid),
    ok.

%% Parent (proc_lib parent) dies while a request is in flight -> the conn
%% fails all requests, closes the ctx and exits with the parent's reason.
h3_ready_exit_parent(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    TestPid = self(),
    Owner = spawn(fun() ->
        {ok, CP} = nhttpc_h3_conn:start_link(conn_opts(Port)),
        ok = nhttpc_h3_conn:wait_ready(CP, 10000),
        TestPid ! {conn, CP},
        receive
            die -> exit(kaboom)
        end
    end),
    OMRef = monitor(process, Owner),
    ConnPid =
        receive
            {conn, CP} -> CP
        after 12000 -> ct:fail("owner did not start conn")
        end,
    CMRef = monitor(process, ConnPid),
    SlowReq = #{method => get, path => <<"/slow">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    {_, D} = sys:get_state(ConnPid),
    ?assert(map_size(element(12, D)) >= 1),
    Owner ! die,
    receive
        {nhttpc_error, ConnPid, Ref, {connection_died, kaboom}} -> ok
    after 5000 -> ct:fail("expected connection_died for in-flight request")
    end,
    receive
        {'DOWN', CMRef, process, ConnPid, kaboom} -> ok
    after 5000 -> ct:fail("conn did not exit with parent reason")
    end,
    receive
        {'DOWN', OMRef, process, Owner, kaboom} -> ok
    after 2000 -> ok
    end,
    process_flag(trap_exit, false),
    ok.

%% Benign 'EXIT' signals keep the ready loop alive; a non-benign one
%% fails all requests, notifies the pool and exits with that reason.
h3_ready_exit_signals(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    TestPid = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(TestPid) end),
    Opts = (conn_opts(Port))#{pool_mgr => MockMgr},
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    CMRef = monitor(process, ConnPid),
    SlowReq = #{method => get, path => <<"/slow">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    {_, D} = sys:get_state(ConnPid),
    ?assert(map_size(element(12, D)) >= 1),
    Fake = spawn(fun() -> ok end),
    ConnPid ! {'EXIT', Fake, normal},
    ConnPid ! {'EXIT', Fake, shutdown},
    ConnPid ! {'EXIT', Fake, {shutdown, bye}},
    %% barrier: the three benign signals were processed, conn still alive
    {ready, _} = sys:get_state(ConnPid),
    ConnPid ! {'EXIT', Fake, boom},
    receive
        {nhttpc_error, ConnPid, Ref, {quic_closed, boom}} -> ok
    after 5000 -> ct:fail("expected quic_closed for in-flight request")
    end,
    receive
        {mock_pool_mgr, {connection_error, ConnPid, {quic_closed, boom}}} -> ok
    after 5000 -> ct:fail("expected pool connection_error")
    end,
    receive
        {'DOWN', CMRef, process, ConnPid, boom} -> ok
    after 5000 -> ct:fail("conn did not exit boom")
    end,
    MockMgr ! stop,
    process_flag(trap_exit, false),
    ok.

%% Drives the draining-loop message matrix: unified cancel, $call cancel,
%% wait_ready, ping, check_idle, a stray EXIT and a rejected send, then
%% lets the in-flight request complete so the loop exits normal.
h3_draining_message_matrix(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    TestPid = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(TestPid) end),
    Opts = (conn_opts(Port))#{pool_mgr => MockMgr},
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    CMRef = monitor(process, ConnPid),
    SlowReq = #{method => get, path => <<"/slow">>, headers => [], body => <<>>},
    {ok, SlowRef} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    {_, D} = sys:get_state(ConnPid),
    ?assert(map_size(element(12, D)) >= 1),
    ok = sys:suspend(ConnPid),
    sys:replace_state(ConnPid, fun({ready, Data}) ->
        {draining, setelement(14, Data, true)}
    end),
    UCRef = make_ref(),
    ConnPid ! {'$nhttpc_cancel', UCRef, self()},
    _CancelP = spawn(fun() ->
        TestPid ! {cancel_result, nhttpc_h3_conn:cancel(ConnPid, make_ref())}
    end),
    _WaitP = spawn(fun() ->
        TestPid ! {wr_result, nhttpc_h3_conn:wait_ready(ConnPid, 4000)}
    end),
    _PingP = spawn(fun() ->
        TestPid ! {ping_result, nhttpc_h3_conn:ping(ConnPid, 4000)}
    end),
    ConnPid ! check_idle,
    Fake = spawn(fun() -> ok end),
    ConnPid ! {'EXIT', Fake, somenoise},
    {ok, RejRef} = nhttpc_h3_conn:send_async(
        ConnPid, #{method => get, path => <<"/hello">>, headers => [], body => <<>>}, self()
    ),
    ok = sys:resume(ConnPid),
    %% a system message in the draining loop hits its sys clause
    {draining, _} = sys:get_state(ConnPid),
    receive
        {'$nhttpc_cancel_ack', UCRef} -> ok
    after 5000 -> ct:fail("no unified cancel ack in draining")
    end,
    receive
        {cancel_result, CR} -> ?assertEqual({error, not_found}, CR)
    after 5000 -> ct:fail("no $call cancel result in draining")
    end,
    receive
        {wr_result, WR} -> ?assertEqual({error, draining}, WR)
    after 5000 -> ct:fail("no wait_ready result in draining")
    end,
    receive
        {ping_result, PR} -> ?assertEqual({error, draining}, PR)
    after 5000 -> ct:fail("no ping result in draining")
    end,
    receive
        {nhttpc_error, ConnPid, RejRef, _} -> ok
    after 5000 -> ct:fail("send not rejected in draining")
    end,
    receive
        {'DOWN', CMRef, process, ConnPid, normal} -> ok
    after 8000 -> ct:fail("draining conn did not exit normal")
    end,
    receive
        {nhttpc_error, ConnPid, SlowRef, _} -> ok;
        {nhttpc_response, ConnPid, SlowRef, _} -> ok
    after 1000 -> ok
    end,
    MockMgr ! stop,
    process_flag(trap_exit, false),
    ok.

%% A request that times out while the connection is draining drives the
%% draining check_request_timeouts arm and the exit-when-empty path.
h3_draining_request_timeout(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    Opts = (conn_opts(Port))#{request_timeout => 300},
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    CMRef = monitor(process, ConnPid),
    SlowReq = #{method => get, path => <<"/slow">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    ok = sys:suspend(ConnPid),
    sys:replace_state(ConnPid, fun({ready, Data}) ->
        {draining, setelement(14, Data, true)}
    end),
    ok = sys:resume(ConnPid),
    receive
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ?assertMatch({request, #{type := request_timeout}}, Reason)
    after 5000 -> ct:fail("expected request timeout while draining")
    end,
    receive
        {'DOWN', CMRef, process, ConnPid, normal} -> ok
    after 5000 -> ct:fail("draining conn did not exit after timeout")
    end,
    process_flag(trap_exit, false),
    ok.

%% normalize_host/1 list and IP-tuple clauses.
h3_host_variants(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, C1} = nhttpc_h3_conn:start_link((conn_opts(Port))#{host => "127.0.0.1"}),
    ok = nhttpc_h3_conn:wait_ready(C1, 10000),
    nhttpc_h3_conn:stop(C1),
    {ok, C2} = nhttpc_h3_conn:start_link((conn_opts(Port))#{host => {127, 0, 0, 1}}),
    ok = nhttpc_h3_conn:wait_ready(C2, 10000),
    nhttpc_h3_conn:stop(C2),
    ok.

%% port 443 takes the bare-authority init branch (no :port suffix).
h3_port_443_authority(_Config) ->
    process_flag(trap_exit, true),
    Opts = #{
        host => <<"10.255.255.1">>,
        port => 443,
        connect_timeout => 400,
        request_timeout => 5000,
        idle_timeout => 60000
    },
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 5000 -> ct:fail("expected connect failure on :443")
    end,
    ?assertNot(is_process_alive(ConnPid)),
    process_flag(trap_exit, false),
    ok.

%% Stream slots and conn_ets present: completion releases one live stream
%% slot and the GOAWAY draining path updates the status element.
h3_ets_tracking(Config) ->
    process_flag(trap_exit, true),
    {Server, Port} = start_extra_server(h3_ets_srv, Config),
    TestPid = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(TestPid) end),
    Tab = ets:new(h3_ets_track, [public, set]),
    Slots = nhttpc_stream_slots:new(),
    ok = nhttpc_stream_slots:set_cap(Slots, ?H3_SLOT_CAP),
    Opts = (conn_opts(Port))#{pool_mgr => MockMgr, conn_ets => Tab, stream_slots => Slots},
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    true = ets:insert(Tab, {ConnPid, undefined, ready, Slots}),
    ok = nhttpc_stream_slots:reserve(Slots),
    ok = nhttpc_stream_slots:reserve(Slots),
    ok = nhttpc_stream_slots:reserve(Slots),
    Req = #{method => get, path => <<"/hello">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, R}} ->
            ?assertEqual(200, maps:get(status, R))
    after 10000 -> ct:fail("no /hello response")
    end,
    {ready, _} = sys:get_state(ConnPid),
    ?assertEqual(2, nhttpc_stream_slots:live(Slots)),
    CMRef = monitor(process, ConnPid),
    ok = nhttp:drain(Server),
    receive
        {mock_pool_mgr, {connection_draining, ConnPid}} -> ok
    after 6000 -> ct:fail("no connection_draining for ets test")
    end,
    receive
        {'DOWN', CMRef, process, ConnPid, normal} -> ok
    after 8000 -> ct:fail("ets conn did not exit normal")
    end,
    ?assertEqual(draining, ets:lookup_element(Tab, ConnPid, 3)),
    MockMgr ! stop,
    nhttpc_test_server:stop(Server),
    process_flag(trap_exit, false),
    ok.

%% conn_ets present but the row is missing / wrong table type: the ETS
%% helpers swallow the badarg and keep going.
h3_ets_badarg(Config) ->
    process_flag(trap_exit, true),
    {Server, Port} = start_extra_server(h3_ets_bad_srv, Config),
    TestPid = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(TestPid) end),
    Tab = ets:new(h3_ets_bad, [public, bag]),
    Opts = (conn_opts(Port))#{pool_mgr => MockMgr, conn_ets => Tab},
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    Req = #{method => get, path => <<"/hello">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, R}} ->
            ?assertEqual(200, maps:get(status, R))
    after 10000 -> ct:fail("no /hello response (ets badarg)")
    end,
    CMRef = monitor(process, ConnPid),
    ok = nhttp:drain(Server),
    receive
        {'DOWN', CMRef, process, ConnPid, normal} -> ok
    after 8000 -> ct:fail("ets-badarg conn did not exit normal")
    end,
    MockMgr ! stop,
    nhttpc_test_server:stop(Server),
    process_flag(trap_exit, false),
    ok.

%% Unified cancel ('$nhttpc_cancel') of an in-flight request in the
%% ready loop.
h3_ready_unified_cancel(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    SlowReq = #{method => get, path => <<"/slow">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    {_, D} = sys:get_state(ConnPid),
    ?assert(map_size(element(12, D)) >= 1),
    ConnPid ! {'$nhttpc_cancel', Ref, self()},
    receive
        {'$nhttpc_cancel_ack', Ref} -> ok
    after 5000 -> ct:fail("no unified cancel ack in ready")
    end,
    ?assert(is_process_alive(ConnPid)),
    nhttpc_h3_conn:stop(ConnPid),
    ok.

%% Idle close on a pool-less connection takes the no-op
%% notify_pool_idle_close clause.
h3_idle_close_no_pool(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    CMRef = monitor(process, ConnPid),
    ok = sys:suspend(ConnPid),
    sys:replace_state(ConnPid, fun({ready, Data}) ->
        Old = erlang:monotonic_time(millisecond) - 1000000,
        {ready, setelement(17, setelement(18, Data, Old), 0)}
    end),
    ok = sys:resume(ConnPid),
    ConnPid ! check_idle,
    receive
        {'DOWN', CMRef, process, ConnPid, normal} -> ok
    after 5000 -> ct:fail("pool-less conn did not idle-close")
    end,
    process_flag(trap_exit, false),
    ok.

%% Parent dies while the connection is in the draining loop.
h3_draining_exit_parent(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    TestPid = self(),
    Owner = spawn(fun() ->
        {ok, CP} = nhttpc_h3_conn:start_link(conn_opts(Port)),
        ok = nhttpc_h3_conn:wait_ready(CP, 10000),
        TestPid ! {conn, CP},
        receive
            die -> exit(parent_gone)
        end
    end),
    OMRef = monitor(process, Owner),
    ConnPid =
        receive
            {conn, CP} -> CP
        after 12000 -> ct:fail("owner did not start conn")
        end,
    CMRef = monitor(process, ConnPid),
    SlowReq = #{method => get, path => <<"/slow">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, SlowReq, self()),
    {_, D} = sys:get_state(ConnPid),
    ?assert(map_size(element(12, D)) >= 1),
    ok = sys:suspend(ConnPid),
    sys:replace_state(ConnPid, fun({ready, Data}) ->
        {draining, setelement(14, Data, true)}
    end),
    ok = sys:resume(ConnPid),
    {draining, _} = sys:get_state(ConnPid),
    Owner ! die,
    receive
        {nhttpc_error, ConnPid, Ref, {connection_died, parent_gone}} -> ok
    after 5000 -> ct:fail("expected connection_died in draining")
    end,
    receive
        {'DOWN', CMRef, process, ConnPid, parent_gone} -> ok
    after 5000 -> ct:fail("draining conn did not exit with parent reason")
    end,
    receive
        {'DOWN', OMRef, process, Owner, parent_gone} -> ok
    after 2000 -> ok
    end,
    process_flag(trap_exit, false),
    ok.

%%%-----------------------------------------------------------------------------
%%% INTERNAL HELPERS
%%%-----------------------------------------------------------------------------

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

maybe_start_shared_conn(Group, Port, Config) when
    Group =:= requests;
    Group =:= multiplexing;
    Group =:= wire_paths
->
    {ok, ConnPid} = start_ready_conn(Port),
    unlink(ConnPid),
    [{shared_conn, ConnPid} | Config];
maybe_start_shared_conn(_Group, _Port, Config) ->
    Config.

get_conn(Config) ->
    case proplists:get_value(shared_conn, Config) of
        undefined ->
            Port = proplists:get_value(server_port, Config),
            start_ready_conn(Port);
        ConnPid ->
            {ok, ConnPid}
    end.

conn_opts(Port) ->
    #{
        host => <<"127.0.0.1">>,
        port => Port,
        connect_timeout => 5000,
        request_timeout => 30000,
        idle_timeout => 60000
    }.

start_ready_conn(Port) ->
    Opts = conn_opts(Port),
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    {ok, ConnPid}.

%% Regression: the client must read the server's control + QPACK
%% unidirectional streams that the connect-time handshake buffered, so
%% peer SETTINGS are applied. Field offsets track #h3_data (h3_conn at
%% 9) and the nhttp_h3 conn record (settings_received at 6,
%% peer_control_stream at 8).
h3_ingests_server_settings(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, ConnPid} = start_ready_conn(Port),
    {ready, Data} = sys:get_state(ConnPid),
    H3Conn = element(9, Data),
    ?assertEqual(true, element(6, H3Conn)),
    ?assertNotEqual(undefined, element(8, H3Conn)),
    nhttpc_h3_conn:stop(ConnPid),
    ok.

start_h3_server(CertFile, KeyFile) ->
    ServerOpts = #{
        port => 0,
        versions => [http3],
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => nhttpc_h3_test_handler
    },
    case nhttp:start_link(h3_conn_test, ServerOpts) of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, Reason} ->
            {error, Reason}
    end.

%% A dedicated server (own registered name) for tests that drain it,
%% so the shared group server keeps accepting new connections.
start_extra_server(Name, Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    ServerOpts = #{
        port => 0,
        versions => [http3],
        tls => #{certfile => CertFile, keyfile => KeyFile},
        handler => nhttpc_h3_test_handler
    },
    {ok, ServerPid} = nhttp:start_link(Name, ServerOpts),
    unlink(ServerPid),
    {ok, Port} = nhttp:get_port(ServerPid),
    {ServerPid, Port}.

collect_responses(_ConnPid, [], _Timeout) ->
    [];
collect_responses(ConnPid, Refs, Timeout) ->
    collect_responses(ConnPid, Refs, Timeout, []).

collect_responses(_ConnPid, [], _Timeout, Acc) ->
    lists:reverse(Acc);
collect_responses(ConnPid, Refs, Timeout, Acc) ->
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            case lists:member(Ref, Refs) of
                true ->
                    RemainingRefs = lists:delete(Ref, Refs),
                    collect_responses(ConnPid, RemainingRefs, Timeout, [{Ref, Resp} | Acc]);
                false ->
                    collect_responses(ConnPid, Refs, Timeout, Acc)
            end
    after Timeout ->
        ct:fail("Timed out waiting for ~p responses", [length(Refs)])
    end.

%% Mock pool manager process. Receives gen_server:cast messages (which arrive
%% as {'$gen_cast', Msg}) and forwards them to the test process for assertions.
mock_pool_mgr_loop(TestPid) ->
    receive
        {'$gen_cast', Msg} ->
            TestPid ! {mock_pool_mgr, Msg},
            mock_pool_mgr_loop(TestPid);
        stop ->
            ok;
        _Other ->
            mock_pool_mgr_loop(TestPid)
    end.
