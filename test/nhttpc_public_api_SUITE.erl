%%%-----------------------------------------------------------------------------
%%% @doc Coverage of the public API surface in `nhttpc.erl` that the
%%% topical suites do not exercise: default-opts arities, the
%%% `host_to_binary/1` clauses, the WS option folds + tls_to_ssl_opts,
%%% the `ws_connect_error` mapping, and the kill-after-timeout fall
%%% back branches in `cancel/1` / `close/1`.
%%%
%%% The kill-after-timeout cases construct fake handles / connection
%%% maps that point at a process which deliberately ignores the cancel
%%% protocol. They take ~5 s of wall-clock each (5 s wait + 1 s drain),
%%% but exercise the only paths in `nhttpc:cancel/1` / `nhttpc:close/1`
%%% that fire after the cooperative ack times out.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_public_api_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    suite/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    request_default_opts_arity/1,
    convenience_put_default_opts/1,
    convenience_url_error_propagates/1,
    open_normalize_error_propagates/1,
    open_host_string_input/1,
    open_host_ip_tuple_input/1,
    open_to_unreachable_drains_exit_signals/1,
    open_from_trapping_caller_preserves_flag/1,
    request_invalid_opts_returns_error/1,
    request_invalid_url_in_request_returns_error/1,
    send_invalid_opts_returns_error/1,
    send_invalid_url_in_request_returns_error/1,
    stream_invalid_opts_returns_error/1,
    stream_invalid_url_in_request_returns_error/1,
    stream_via_not_oneshot_returns_error/1,
    stream_with_body_to_returns_error/1,
    await_returns_request_timeout/1,
    await_returns_worker_error/1,
    await_skips_done_drain_when_absent/1,
    await_response_returns_request_timeout/1,
    await_response_returns_connection_down/1,
    child_spec_normalizes_pool_opts/1,
    child_spec_invalid_opts_raises/1,
    start_link_invalid_opts_returns_error/1,
    stop_with_invalid_opts_returns_error/1,
    ws_send_to_request_handle_returns_invalid/1,
    ws_send_to_dead_ws_returns_closed/1,
    ws_open_invalid_uri_maps_to_invalid_request/1,
    ws_open_unexpected_status_maps_to_protocol_error/1,
    ws_open_connect_error_maps_to_connect_error/1,
    ws_open_runs_full_option_fold/1,
    cancel_unresponsive_worker_kills_after_timeout/1,
    cancel_unresponsive_ws_kills_after_timeout/1,
    cancel_dead_ws_handle_is_noop/1,
    close_unresponsive_connection_kills_after_timeout/1
]).

%%%-----------------------------------------------------------------------------
%%% CT BOILERPLATE
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {seconds, 60}}].

all() ->
    [
        {group, public_api},
        {group, slow_kill}
    ].

groups() ->
    [
        {public_api, [sequence], [
            request_default_opts_arity,
            convenience_put_default_opts,
            convenience_url_error_propagates,
            open_normalize_error_propagates,
            open_host_string_input,
            open_host_ip_tuple_input,
            open_to_unreachable_drains_exit_signals,
            open_from_trapping_caller_preserves_flag,
            request_invalid_opts_returns_error,
            request_invalid_url_in_request_returns_error,
            send_invalid_opts_returns_error,
            send_invalid_url_in_request_returns_error,
            stream_invalid_opts_returns_error,
            stream_invalid_url_in_request_returns_error,
            stream_via_not_oneshot_returns_error,
            stream_with_body_to_returns_error,
            await_returns_request_timeout,
            await_returns_worker_error,
            await_skips_done_drain_when_absent,
            await_response_returns_request_timeout,
            await_response_returns_connection_down,
            child_spec_normalizes_pool_opts,
            child_spec_invalid_opts_raises,
            start_link_invalid_opts_returns_error,
            stop_with_invalid_opts_returns_error,
            ws_send_to_request_handle_returns_invalid,
            ws_send_to_dead_ws_returns_closed,
            ws_open_invalid_uri_maps_to_invalid_request,
            ws_open_unexpected_status_maps_to_protocol_error,
            ws_open_connect_error_maps_to_connect_error,
            ws_open_runs_full_option_fold,
            cancel_dead_ws_handle_is_noop
        ]},
        {slow_kill, [sequence], [
            cancel_unresponsive_worker_kills_after_timeout,
            cancel_unresponsive_ws_kills_after_timeout,
            close_unresponsive_connection_kills_after_timeout
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(public_api, Config) ->
    case
        nhttp:start_link(public_api_h1, #{
            port => 0,
            handler => nhttp_test_handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            ResponderPort = start_unexpected_status_responder(),
            [
                {server_pid, ServerPid},
                {server_port, Port},
                {responder_port, ResponderPort}
                | Config
            ];
        {error, Reason} ->
            ct:fail("Failed to start H1 server: ~p", [Reason])
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(public_api, Config) ->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok;
end_per_group(_Group, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% PUBLIC API TESTS
%%%-----------------------------------------------------------------------------

request_default_opts_arity(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    {ok, #{status := 200}} = nhttpc:request(Req),
    ok.

convenience_put_default_opts(Config) ->
    Port = ?config(server_port, Config),
    {ok, #{status := 200, body := Body}} =
        nhttpc:put(url(Port, <<"/upload/echo">>), <<"hello">>),
    ?assertMatch(<<"Received: 5 bytes", _/binary>>, Body),
    ok.

convenience_url_error_propagates(_Config) ->
    Result = nhttpc:get(<<"not-a-url">>),
    ?assertMatch({error, {invalid_request, _}}, Result),
    ok.

open_normalize_error_propagates(_Config) ->
    Target = #{host => <<"127.0.0.1">>, port => 1},
    Result = nhttpc:open(Target, #{unknown_opt => true}),
    ?assertMatch({error, {invalid_opts, _}}, Result),
    ok.

open_host_string_input(Config) ->
    Port = ?config(server_port, Config),
    Target = #{host => "127.0.0.1", port => Port},
    {ok, Conn} = nhttpc:open(Target, #{versions => [http1_1]}),
    ?assertMatch(#{pid := _, version := http1_1}, Conn),
    ok = nhttpc:close(Conn),
    ok.

open_host_ip_tuple_input(Config) ->
    Port = ?config(server_port, Config),
    Target = #{host => {127, 0, 0, 1}, port => Port},
    {ok, Conn} = nhttpc:open(Target, #{versions => [http1_1]}),
    ?assertMatch(#{pid := _}, Conn),
    ok = nhttpc:close(Conn),
    ok.

open_to_unreachable_drains_exit_signals(_Config) ->
    Target = #{host => <<"127.0.0.1">>, port => unused_port()},
    %% trap_exit is set inside do_open and restored on exit; our trap_exit
    %% state should not change.
    PrevTrap = process_flag(trap_exit, false),
    Result = nhttpc:open(Target, #{
        versions => [http1_1],
        timeouts => #{connect => 200}
    }),
    ?assertMatch({error, {connect_error, _}}, Result),
    process_flag(trap_exit, PrevTrap),
    ok.

open_from_trapping_caller_preserves_flag(_Config) ->
    PrevTrap = process_flag(trap_exit, true),
    Target = #{host => <<"127.0.0.1">>, port => unused_port()},
    Result = nhttpc:open(Target, #{
        versions => [http1_1],
        timeouts => #{connect => 200}
    }),
    ?assertMatch({error, {connect_error, _}}, Result),
    ?assertEqual(true, process_flag(trap_exit, PrevTrap)),
    ok.

request_invalid_opts_returns_error(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    Result = nhttpc:request(Req, #{nope => true}),
    ?assertMatch({error, {invalid_opts, _}}, Result),
    ok.

request_invalid_url_in_request_returns_error(_Config) ->
    BadReq = #{method => get, url => <<"not-a-url">>},
    Result = nhttpc:request(BadReq, #{}),
    ?assertMatch({error, {invalid_request, _}}, Result),
    ok.

send_invalid_opts_returns_error(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    Result = nhttpc:send(Req, #{nope => true}),
    ?assertMatch({error, {invalid_opts, _}}, Result),
    ok.

send_invalid_url_in_request_returns_error(_Config) ->
    BadReq = #{method => get, url => <<"not-a-url">>},
    Result = nhttpc:send(BadReq, #{}),
    ?assertMatch({error, {invalid_request, _}}, Result),
    ok.

stream_invalid_opts_returns_error(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    Result = nhttpc:stream(Req, #{nope => true}),
    ?assertMatch({error, {invalid_opts, _}}, Result),
    ok.

stream_invalid_url_in_request_returns_error(_Config) ->
    BadReq = #{method => get, url => <<"not-a-url">>},
    Result = nhttpc:stream(BadReq, #{}),
    ?assertMatch({error, {invalid_request, _}}, Result),
    ok.

stream_via_not_oneshot_returns_error(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    Result = nhttpc:stream(Req, #{via => {pool, fake}}),
    ?assertEqual({error, {invalid_opts, stream_via_not_oneshot}}, Result),
    ok.

stream_with_body_to_returns_error(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    Result = nhttpc:stream(Req, #{body_to => {file, "/tmp/x"}}),
    ?assertEqual({error, {invalid_opts, body_to_on_stream}}, Result),
    ok.

await_returns_request_timeout(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/2000">>)),
    {ok, Handle} = nhttpc:send(Req),
    Result = nhttpc:await(Handle, 50),
    ?assertMatch({error, {timeout, request}}, Result),
    ok = nhttpc:cancel(Handle),
    ok.

await_returns_worker_error(_Config) ->
    {ok, Req} = nhttpc:url(get, <<"http://127.0.0.1:1/echo">>),
    {ok, Handle} = nhttpc:send(Req, #{timeouts => #{connect => 200}}),
    Result = nhttpc:await(Handle, 5000),
    ?assertMatch({error, _}, Result),
    ok.

await_skips_done_drain_when_absent(_Config) ->
    Self = self(),
    Handle = nhttpc_handle:new(Self, make_ref(), request),
    Self ! {nhttpc, Handle, {response, #{status => 200, headers => [], body => <<>>}}},
    {ok, _} = nhttpc:await(Handle, 100),
    receive
        {nhttpc, Handle, _} = M -> ct:fail({unexpected, M})
    after 0 ->
        ok
    end.

await_response_returns_request_timeout(_Config) ->
    Pid = spawn(fun ignore_loop/0),
    Result = nhttpc:await_response(Pid, make_ref(), 100),
    ?assertEqual({error, {timeout, request}}, Result),
    exit(Pid, kill),
    ok.

await_response_returns_connection_down(_Config) ->
    Pid = spawn(fun ignore_loop/0),
    Self = self(),
    Watcher = spawn(fun() ->
        Result = nhttpc:await_response(Pid, make_ref(), 5000),
        Self ! {result, Result}
    end),
    timer:sleep(50),
    exit(Pid, killed_by_test),
    receive
        {result, Result} ->
            ?assertMatch({error, _}, Result)
    after 5000 ->
        exit(Watcher, kill),
        ct:fail("await_response did not return")
    end,
    ok.

child_spec_normalizes_pool_opts(Config) ->
    Port = ?config(server_port, Config),
    Spec = nhttpc:child_spec(
        public_api_pool_for_child_spec,
        #{host => <<"127.0.0.1">>, port => Port, size => 1}
    ),
    ?assertMatch(#{id := _, start := {_, _, _}}, Spec),
    ok.

child_spec_invalid_opts_raises(_Config) ->
    ?assertError(
        {invalid_opts, _},
        nhttpc:child_spec(public_api_bad_pool, #{nope => true})
    ),
    ok.

start_link_invalid_opts_returns_error(_Config) ->
    Result = nhttpc:start_link(public_api_bad_start_pool, #{nope => true}),
    ?assertMatch({error, {invalid_opts, _}}, Result),
    ok.

stop_with_invalid_opts_returns_error(Config) ->
    Port = ?config(server_port, Config),
    PoolRef = public_api_stop_invalid,
    {ok, Sup} = nhttpc:start_link(PoolRef, #{
        host => <<"127.0.0.1">>, port => Port, size => 1, versions => [http1_1]
    }),
    unlink(Sup),
    Result = nhttpc:stop(PoolRef, #{drain_timeout => 1000}),
    ?assertEqual({error, {invalid_opts, drain_timeout_without_drain}}, Result),
    ok = nhttpc:stop(PoolRef),
    ok.

ws_send_to_request_handle_returns_invalid(_Config) ->
    FakeRequest = nhttpc_handle:new(self(), make_ref(), request),
    Result = nhttpc:ws_send(FakeRequest, {text, <<"x">>}),
    ?assertMatch({error, {invalid_request, _}}, Result),
    ok.

ws_send_to_dead_ws_returns_closed(_Config) ->
    Dead = spawn(fun() -> ok end),
    wait_for_exit(Dead, 1000),
    Handle = nhttpc_handle:new(Dead, make_ref(), ws),
    Result = nhttpc:ws_send(Handle, {text, <<"x">>}),
    ?assertMatch({error, _}, Result),
    ok.

ws_open_invalid_uri_maps_to_invalid_request(_Config) ->
    Result = nhttpc:ws_open(<<"not-a-uri">>, #{}),
    ?assertEqual({error, {invalid_request, invalid_ws_uri}}, Result),
    ok.

ws_open_unexpected_status_maps_to_protocol_error(Config) ->
    Port = ?config(responder_port, Config),
    Url = ws_url(Port, <<"/">>),
    Result = nhttpc:ws_open(Url, #{
        timeouts => #{connect => 1000, idle => 5000}
    }),
    ?assertMatch({error, {protocol_error, http1_1, {unexpected_status, _}}}, Result),
    ok.

ws_open_connect_error_maps_to_connect_error(_Config) ->
    Url = ws_url(unused_port(), <<"/">>),
    Result = nhttpc:ws_open(Url, #{
        timeouts => #{connect => 200}
    }),
    ?assertMatch({error, {connect_error, _}}, Result),
    ok.

ws_open_runs_full_option_fold(_Config) ->
    %% All option folders execute even if connect ultimately fails. The
    %% connect failure is not the assertion — running the fold is.
    Url = ws_url(unused_port(), <<"/">>),
    Result = nhttpc:ws_open(Url, #{
        timeouts => #{connect => 200, idle => 5000},
        headers => [{<<"x-test">>, <<"1">>}],
        ws => #{
            subprotocols => [<<"chat">>],
            ping_interval => 30000
        },
        tls => #{
            verify => verify_none,
            cacerts => [],
            cacertfile => "/dev/null",
            certfile => "/dev/null",
            keyfile => "/dev/null",
            server_name_indication => disable,
            versions => ['tlsv1.3'],
            alpn_advertised_protocols => [<<"http/1.1">>],
            extra => [{depth, 5}]
        }
    }),
    ?assertMatch({error, _}, Result),
    ok.

cancel_dead_ws_handle_is_noop(_Config) ->
    Dead = spawn(fun() -> ok end),
    wait_for_exit(Dead, 1000),
    Handle = nhttpc_handle:new(Dead, make_ref(), ws),
    ok = nhttpc:cancel(Handle),
    ok.

%%%-----------------------------------------------------------------------------
%%% SLOW KILL TESTS
%%%-----------------------------------------------------------------------------

cancel_unresponsive_worker_kills_after_timeout(_Config) ->
    Pid = spawn(fun ignore_loop/0),
    Handle = nhttpc_handle:new(Pid, make_ref(), request),
    T0 = erlang:monotonic_time(millisecond),
    ok = nhttpc:cancel(Handle),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed >= 4500, {elapsed, Elapsed}),
    ?assertNot(is_process_alive(Pid)),
    ok.

cancel_unresponsive_ws_kills_after_timeout(_Config) ->
    Pid = spawn(fun ignore_loop/0),
    Handle = nhttpc_handle:new(Pid, make_ref(), ws),
    T0 = erlang:monotonic_time(millisecond),
    ok = nhttpc:cancel(Handle),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed >= 4500, {elapsed, Elapsed}),
    ?assertNot(is_process_alive(Pid)),
    ok.

close_unresponsive_connection_kills_after_timeout(_Config) ->
    Pid = spawn(fun ignore_loop/0),
    Conn = #{pid => Pid, version => http1_1, peer => {{127, 0, 0, 1}, 1}},
    T0 = erlang:monotonic_time(millisecond),
    ok = nhttpc:close(Conn),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed >= 4500, {elapsed, Elapsed}),
    ?assertNot(is_process_alive(Pid)),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"http://127.0.0.1:", PortBin/binary, Path/binary>>.

ws_url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"ws://127.0.0.1:", PortBin/binary, Path/binary>>.

unused_port() ->
    {ok, Sock} = gen_tcp:listen(0, [{reuseaddr, true}]),
    {ok, Port} = inet:port(Sock),
    gen_tcp:close(Sock),
    Port.

ignore_loop() ->
    receive
        _Anything -> ignore_loop()
    end.

wait_for_exit(Pid, Timeout) ->
    MRef = monitor(process, Pid),
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after Timeout ->
        erlang:demonitor(MRef, [flush]),
        error(process_did_not_exit)
    end.

start_unexpected_status_responder() ->
    Self = self(),
    Pid = spawn(fun() ->
        {ok, LSock} = gen_tcp:listen(0, [
            binary, {reuseaddr, true}, {active, false}, {packet, raw}
        ]),
        {ok, Port} = inet:port(LSock),
        Self ! {responder_port, self(), Port},
        responder_loop(LSock)
    end),
    receive
        {responder_port, Pid, Port} -> Port
    after 5000 ->
        error(responder_did_not_start)
    end.

responder_loop(LSock) ->
    case gen_tcp:accept(LSock, infinity) of
        {ok, Sock} ->
            _ = inet:setopts(Sock, [{active, false}]),
            _ =
                (try
                    gen_tcp:recv(Sock, 0, 200)
                catch
                    _:_ -> ok
                end),
            Body = <<"not a websocket">>,
            BodySize = integer_to_binary(byte_size(Body)),
            Resp = [
                <<"HTTP/1.1 200 OK\r\n">>,
                <<"Content-Type: text/plain\r\n">>,
                <<"Content-Length: ">>,
                BodySize,
                <<"\r\n\r\n">>,
                Body
            ],
            _ = gen_tcp:send(Sock, Resp),
            _ = gen_tcp:close(Sock),
            responder_loop(LSock);
        {error, _} ->
            ok
    end.
