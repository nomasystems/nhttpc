%%%-----------------------------------------------------------------------------
%%% @doc WebSocket client integration tests (plan 22).
%%%
%%% Clean paths run against a real `nhttp' WS server using
%%% `nhttpc_ws_test_handler'. Handshake-failure, timeout, multi-frame
%%% and protocol-error paths run against a raw `gen_tcp' responder that
%%% lets each test script the exact bytes on the wire.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_ws_integration_SUITE).

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
    open_then_close/1,
    send_and_recv_text/1,
    send_and_recv_binary/1,
    ping_pong/1,
    ping_interval_keepalive/1,
    server_initiated_close/1,
    subprotocol_negotiation/1,
    owner_down_closes_session/1,
    sys_status_redacts_state/1,
    sys_lifecycle_callbacks/1
]).

-export([
    idle_timeout/1,
    ping_timeout/1,
    unexpected_status/1,
    missing_accept_header/1,
    invalid_upgrade_header/1,
    invalid_connection_header/1,
    ws_connect_error/1,
    ws_connect_error_leaves_no_exit/1,
    invalid_ws_uri_rejected/1,
    list_uri_via_start_link/1,
    request_path_variants/1,
    handshake_invalid_accept/1,
    handshake_connection_multitoken/1,
    connect_timeout_infinity/1,
    tcp_server_abrupt_close/1,
    server_ping_frame/1,
    closing_loop_drains/1,
    closing_loop_protocol_error/1,
    closing_loop_timeout/1,
    handshake_recv_error/1,
    handshake_parse_error/1,
    multi_frame_pipeline/1,
    split_handshake_and_partial_frame/1,
    protocol_error_frame/1,
    no_handle_owner_path/1
]).

-export([
    wss_open_send_text/1,
    wss_server_close/1,
    wss_server_abrupt_close/1,
    wss_verify_peer_default/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, clean},
        {group, faults},
        {group, wss}
    ].

groups() ->
    [
        {clean, [sequence], [
            open_then_close,
            send_and_recv_text,
            send_and_recv_binary,
            ping_pong,
            ping_interval_keepalive,
            server_initiated_close,
            subprotocol_negotiation,
            owner_down_closes_session,
            sys_status_redacts_state,
            sys_lifecycle_callbacks
        ]},
        {faults, [sequence], [
            idle_timeout,
            ping_timeout,
            unexpected_status,
            missing_accept_header,
            invalid_upgrade_header,
            invalid_connection_header,
            ws_connect_error,
            ws_connect_error_leaves_no_exit,
            invalid_ws_uri_rejected,
            list_uri_via_start_link,
            request_path_variants,
            handshake_invalid_accept,
            handshake_connection_multitoken,
            connect_timeout_infinity,
            tcp_server_abrupt_close,
            server_ping_frame,
            closing_loop_drains,
            closing_loop_protocol_error,
            closing_loop_timeout,
            handshake_recv_error,
            handshake_parse_error,
            multi_frame_pipeline,
            split_handshake_and_partial_frame,
            protocol_error_frame,
            no_handle_owner_path
        ]},
        {wss, [sequence], [
            wss_open_send_text,
            wss_server_close,
            wss_verify_peer_default,
            wss_server_abrupt_close
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(nhttp),
    {ok, _} = application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(clean, Config) ->
    {ok, ServerPid} = nhttp:start_link(nhttpc_ws_it_server, #{
        port => 0,
        handler => nhttpc_ws_test_handler,
        protocols => [http1_1],
        telemetry => false
    }),
    unlink(ServerPid),
    {ok, Port} = nhttp:get_port(ServerPid),
    [{server, ServerPid}, {port, Port} | Config];
init_per_group(wss, Config) ->
    ConfDir = filename:join([project_root(), "test", "conf"]),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    true = filelib:is_file(CertFile) andalso filelib:is_file(KeyFile),
    {ok, ServerPid} = nhttp:start_link(nhttpc_wss_it_server, #{
        port => 0,
        handler => nhttpc_ws_test_handler,
        protocols => [http1_1],
        telemetry => false,
        tls => #{certfile => CertFile, keyfile => KeyFile}
    }),
    unlink(ServerPid),
    {ok, Port} = nhttp:get_port(ServerPid),
    [{server, ServerPid}, {port, Port} | Config];
init_per_group(faults, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= clean; Group =:= wss ->
    nhttpc_test_server:stop(?config(server, Config)),
    ok;
end_per_group(faults, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% CLEAN PATHS (real nhttp server)
%%%-----------------------------------------------------------------------------

open_then_close(Config) ->
    URI = ws_uri(Config, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    Pid = nhttpc_handle:pid(Handle),
    MRef = monitor(process, Pid),
    ok = nhttpc:cancel(Handle),
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 5000 ->
        ct:fail(ws_did_not_exit)
    end,
    ?assertEqual(0, drain_count(Handle)),
    ok.

send_and_recv_text(Config) ->
    URI = ws_uri(Config, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"hello-text">>}),
    ?assertEqual({ws, {text, <<"hello-text">>}}, wait_event(Handle)),
    ok = nhttpc:cancel(Handle),
    ok.

send_and_recv_binary(Config) ->
    URI = ws_uri(Config, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {binary, <<1, 2, 3, 4>>}),
    ?assertEqual({ws, {binary, <<1, 2, 3, 4>>}}, wait_event(Handle)),
    ok = nhttpc:cancel(Handle),
    ok.

ping_pong(Config) ->
    URI = ws_uri(Config, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {ping, <<"pingdata">>}),
    ?assertEqual({ws, {pong, <<"pingdata">>}}, wait_event(Handle)),
    ok = nhttpc:cancel(Handle),
    ok.

ping_interval_keepalive(Config) ->
    URI = ws_uri(Config, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{ws => #{ping_interval => 100}}),
    ok = wait_ready(Handle),
    ?assertEqual({ws, {pong, <<>>}}, wait_event(Handle, 5000)),
    ok = nhttpc:cancel(Handle),
    ok.

server_initiated_close(Config) ->
    URI = ws_uri(Config, <<"/ws/server-close">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"trigger">>}),
    ?assertMatch({done, {remote_closed, 1011}}, wait_terminal(Handle)),
    nhttpc:flush(Handle),
    ok.

subprotocol_negotiation(Config) ->
    URI = ws_uri(Config, <<"/ws/sub">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{ws => #{subprotocols => [<<"v1">>, <<"v2">>]}}),
    receive
        {nhttpc, Handle, {ws_ready, ConnInfo}} ->
            ?assertEqual(<<"v2">>, maps:get(protocol, ConnInfo))
    after 5000 ->
        ct:fail(no_ws_ready)
    end,
    ok = nhttpc:cancel(Handle),
    ok.

owner_down_closes_session(Config) ->
    URI = ws_uri(Config, <<"/ws/echo">>),
    Parent = self(),
    Owner = spawn(fun() ->
        {ok, Handle} = nhttpc:ws_open(URI, #{}),
        ok = wait_ready(Handle),
        Parent ! {handle, Handle, nhttpc_handle:pid(Handle)},
        receive
            stop -> ok
        end
    end),
    receive
        {handle, _Handle, WsPid} ->
            MRef = monitor(process, WsPid),
            exit(Owner, kill),
            receive
                {'DOWN', MRef, process, _, _} -> ok
            after 5000 ->
                ct:fail(ws_did_not_exit_on_owner_down)
            end
    after 5000 ->
        ct:fail(owner_never_started)
    end,
    ok.

sys_status_redacts_state(Config) ->
    URI = ws_uri(Config, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    Pid = nhttpc_handle:pid(Handle),
    St = sys:get_state(Pid),
    #{state := San} = nhttpc_ws:format_status(#{state => St}),
    ?assert(is_map(San)),
    ?assertEqual(true, maps:get(socket, San)),
    ?assertEqual(false, maps:get(frame_pending, San)),
    ?assertEqual(true, maps:get(decoder, San)),
    ?assertEqual(false, maps:get(ping_timer, San)),
    {status, Pid, {module, nhttpc_ws}, _} = sys:get_status(Pid),
    Passthrough = #{other => value, log => []},
    ?assertEqual(Passthrough, nhttpc_ws:format_status(Passthrough)),
    ok = nhttpc:cancel(Handle),
    ok.

sys_lifecycle_callbacks(Config) ->
    URI = ws_uri(Config, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    Pid = nhttpc_handle:pid(Handle),
    ok = sys:suspend(Pid),
    ok = sys:change_code(Pid, nhttpc_ws, undefined, []),
    ok = sys:resume(Pid),
    MRef = monitor(process, Pid),
    ok = sys:terminate(Pid, normal),
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 5000 ->
        ct:fail(ws_did_not_terminate)
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% FAULT PATHS (raw gen_tcp responder)
%%%-----------------------------------------------------------------------------

idle_timeout(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        _ = gen_tcp:recv(Sock, 0, 10000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{timeouts => #{idle => 150}}),
    ok = wait_ready(Handle),
    ?assertEqual({error, {timeout, idle}}, wait_terminal(Handle)),
    stop_raw(Srv),
    ok.

ping_timeout(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{ws => #{ping_interval => 100}}),
    ok = wait_ready(Handle),
    ?assertEqual({error, {timeout, idle}}, wait_terminal(Handle, 10000)),
    stop_raw(Srv),
    ok.

unexpected_status(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, _Key) ->
        ok = gen_tcp:send(
            Sock, <<"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n">>
        ),
        _ = gen_tcp:recv(Sock, 0, 5000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    ?assertEqual(
        {error, {protocol_error, http1_1, {unexpected_status, 200}}},
        nhttpc:ws_open(URI, #{})
    ),
    stop_raw(Srv),
    ok.

missing_accept_header(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, _Key) ->
        ok = gen_tcp:send(
            Sock,
            <<"HTTP/1.1 101 Switching Protocols\r\n",
                "Upgrade: websocket\r\nConnection: Upgrade\r\n\r\n">>
        ),
        _ = gen_tcp:recv(Sock, 0, 5000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    ?assertEqual(
        {error, {connect_error, missing_accept}},
        nhttpc:ws_open(URI, #{})
    ),
    stop_raw(Srv),
    ok.

invalid_upgrade_header(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, _Key) ->
        ok = gen_tcp:send(
            Sock,
            <<"HTTP/1.1 101 Switching Protocols\r\n",
                "Upgrade: h2c\r\nConnection: Upgrade\r\n\r\n">>
        ),
        _ = gen_tcp:recv(Sock, 0, 5000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    ?assertEqual(
        {error, {connect_error, invalid_upgrade}},
        nhttpc:ws_open(URI, #{})
    ),
    stop_raw(Srv),
    ok.

invalid_connection_header(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, _Key) ->
        ok = gen_tcp:send(
            Sock,
            <<"HTTP/1.1 101 Switching Protocols\r\n",
                "Upgrade: websocket\r\nConnection: keep-alive\r\n\r\n">>
        ),
        _ = gen_tcp:recv(Sock, 0, 5000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    ?assertEqual(
        {error, {connect_error, invalid_connection}},
        nhttpc:ws_open(URI, #{})
    ),
    stop_raw(Srv),
    ok.

ws_connect_error(_Config) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(LSock),
    ok = gen_tcp:close(LSock),
    URI = raw_uri(Port, <<"/ws/echo">>),
    ?assertMatch({error, {connect_error, _}}, nhttpc:ws_open(URI, #{})),
    ok.

ws_connect_error_leaves_no_exit(_Config) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(LSock),
    ok = gen_tcp:close(LSock),
    URI = raw_uri(Port, <<"/ws/echo">>),
    false = process_flag(trap_exit, true),
    ?assertMatch({error, {connect_error, _}}, nhttpc:ws_open(URI, #{})),
    receive
        {'EXIT', _, _} = Exit -> ct:fail({stray_exit, Exit})
    after 200 -> ok
    end,
    ok.

multi_frame_pipeline(_Config) ->
    Batch = [
        nhttp_ws:encode_text(<<"a">>),
        nhttp_ws:encode_binary(<<"b">>),
        nhttp_ws:encode_ping(<<"p">>),
        nhttp_ws:encode_pong(<<"q">>),
        nhttp_ws:encode_close(1000, <<>>)
    ],
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, iolist_to_binary(Batch)),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"go">>}),
    ?assertEqual({ws, {text, <<"a">>}}, wait_event(Handle)),
    ?assertEqual({ws, {binary, <<"b">>}}, wait_event(Handle)),
    ?assertEqual({ws, {ping, <<"p">>}}, wait_event(Handle)),
    ?assertEqual({ws, {pong, <<"q">>}}, wait_event(Handle)),
    ?assertMatch({done, {remote_closed, 1000}}, wait_terminal(Handle)),
    nhttpc:flush(Handle),
    stop_raw(Srv),
    ok.

split_handshake_and_partial_frame(_Config) ->
    Frame = iolist_to_binary(nhttp_ws:encode_text(<<"split-frame">>)),
    <<First:1/binary, Rest/binary>> = Frame,
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        Resp = iolist_to_binary(nhttp_ws:handshake_response(Key)),
        <<RA:10/binary, RB/binary>> = Resp,
        ok = gen_tcp:send(Sock, RA),
        ok = gen_tcp:send(Sock, RB),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, First),
        ok = gen_tcp:send(Sock, Rest),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"go">>}),
    ?assertEqual({ws, {text, <<"split-frame">>}}, wait_event(Handle)),
    nhttpc:cancel(Handle),
    stop_raw(Srv),
    ok.

protocol_error_frame(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, <<131, 0>>),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"go">>}),
    ?assertMatch({error, {stream_error, {protocol, _}}}, wait_terminal(Handle)),
    nhttpc:flush(Handle),
    stop_raw(Srv),
    ok.

no_handle_owner_path(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, iolist_to_binary(nhttp_ws:encode_text(<<"x">>))),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Pid, ConnInfo} = nhttpc_ws:start_link(URI, #{}),
    ?assertMatch(#{}, ConnInfo),
    unlink(Pid),
    Pid ! {ws_send, {text, <<"go">>}},
    MRef = monitor(process, Pid),
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 5000 ->
        exit(Pid, kill),
        ct:fail(no_handle_ws_did_not_exit)
    end,
    stop_raw(Srv),
    ok.

invalid_ws_uri_rejected(_Config) ->
    ?assertEqual(
        {error, {invalid_request, invalid_ws_uri}},
        nhttpc:ws_open(<<"http://127.0.0.1:1/x">>, #{})
    ),
    ?assertEqual(
        {error, {invalid_request, invalid_ws_uri}},
        nhttpc:ws_open(<<"not-a-uri">>, #{})
    ),
    ok.

list_uri_via_start_link(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = "ws://127.0.0.1:" ++ integer_to_list(Port) ++ "/ws/echo",
    {ok, Pid, ConnInfo} = nhttpc_ws:start_link(URI, #{}),
    ?assertMatch(#{transport := tcp}, ConnInfo),
    unlink(Pid),
    exit(Pid, kill),
    stop_raw(Srv),
    ok.

request_path_variants(_Config) ->
    Builders = [
        fun(P) -> raw_uri(P, <<"/ws/echo">>) end,
        fun(P) -> raw_uri(P, <<"/ws/echo?a=b">>) end,
        fun(P) -> <<"ws://127.0.0.1:", (integer_to_binary(P))/binary>> end,
        fun(P) -> <<"ws://127.0.0.1:", (integer_to_binary(P))/binary, "?q=1">> end
    ],
    lists:foreach(
        fun(Build) ->
            {Srv, Port} = start_raw(fun(Sock, Key) ->
                ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
                raw_drain(Sock),
                gen_tcp:close(Sock)
            end),
            {ok, Handle} = nhttpc:ws_open(Build(Port), #{}),
            ok = wait_ready(Handle),
            ok = nhttpc:cancel(Handle),
            stop_raw(Srv)
        end,
        Builders
    ),
    ok.

handshake_invalid_accept(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, _Key) ->
        ok = gen_tcp:send(
            Sock,
            <<"HTTP/1.1 101 Switching Protocols\r\n",
                "Upgrade: websocket\r\nConnection: Upgrade\r\n",
                "Sec-WebSocket-Accept: bogus-accept-value\r\n\r\n">>
        ),
        _ = gen_tcp:recv(Sock, 0, 5000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    ?assertEqual(
        {error, {connect_error, invalid_accept}},
        nhttpc:ws_open(URI, #{})
    ),
    stop_raw(Srv),
    ok.

handshake_connection_multitoken(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        Accept = nhttp_ws:accept_key(Key),
        ok = gen_tcp:send(Sock, [
            <<"HTTP/1.1 101 Switching Protocols\r\n">>,
            <<"Upgrade: websocket\r\n">>,
            <<"Connection: keep-alive, Upgrade\r\n">>,
            <<"Sec-WebSocket-Protocol: x\r\n">>,
            <<"Sec-WebSocket-Accept: ">>,
            Accept,
            <<"\r\n\r\n">>
        ]),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    receive
        {nhttpc, Handle, {ws_ready, ConnInfo}} ->
            ?assertEqual(undefined, maps:get(protocol, ConnInfo))
    after 5000 ->
        ct:fail(no_ws_ready)
    end,
    ok = nhttpc:cancel(Handle),
    stop_raw(Srv),
    ok.

connect_timeout_infinity(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{timeouts => #{connect => infinity}}),
    ok = wait_ready(Handle),
    ok = nhttpc:cancel(Handle),
    stop_raw(Srv),
    ok.

tcp_server_abrupt_close(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"go">>}),
    ?assertMatch({done, {remote_closed, undefined}}, wait_terminal(Handle)),
    nhttpc:flush(Handle),
    stop_raw(Srv),
    ok.

closing_loop_drains(_Config) ->
    Text = iolist_to_binary(nhttp_ws:encode_text(<<"mid">>)),
    <<TFirst:1/binary, TRest/binary>> = Text,
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, iolist_to_binary(nhttp_ws:encode_close(1000, <<>>))),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, iolist_to_binary(nhttp_ws:encode_text(<<"a">>))),
        ok = gen_tcp:send(Sock, TFirst),
        ok = gen_tcp:send(Sock, TRest),
        ok = gen_tcp:send(Sock, iolist_to_binary(nhttp_ws:encode_close(1000, <<>>))),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"go">>}),
    ?assertMatch({done, {remote_closed, 1000}}, wait_terminal(Handle)),
    nhttpc:flush(Handle),
    stop_raw(Srv),
    ok.

server_ping_frame(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, iolist_to_binary(nhttp_ws:encode_ping(<<"px">>))),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"go">>}),
    ?assertEqual({ws, {ping, <<"px">>}}, wait_event(Handle)),
    ?assertMatch({done, {remote_closed, undefined}}, wait_terminal(Handle)),
    nhttpc:flush(Handle),
    stop_raw(Srv),
    ok.

closing_loop_protocol_error(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, iolist_to_binary(nhttp_ws:encode_close(1000, <<>>))),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, <<131, 0>>),
        raw_drain(Sock),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"go">>}),
    ?assertMatch({error, {stream_error, {protocol, _}}}, wait_terminal(Handle)),
    nhttpc:flush(Handle),
    stop_raw(Srv),
    ok.

%% CLOSE not last in buffer -> handle_frame_then_continue/4 enters closing_loop
%% without re-arming the socket, so a silent peer trips ?CLOSE_TIMEOUT.
closing_loop_timeout(_Config) ->
    Coalesced = iolist_to_binary([
        nhttp_ws:encode_close(1000, <<>>),
        nhttp_ws:encode_text(<<"after-close">>)
    ]),
    {Srv, Port} = start_raw(fun(Sock, Key) ->
        ok = gen_tcp:send(Sock, nhttp_ws:handshake_response(Key)),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        ok = gen_tcp:send(Sock, Coalesced),
        timer:sleep(8000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"go">>}),
    ?assertEqual({error, {timeout, idle}}, wait_terminal(Handle, 8000)),
    nhttpc:flush(Handle),
    stop_raw(Srv),
    ok.

handshake_recv_error(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, _Key) ->
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    ?assertMatch({error, {connect_error, _}}, nhttpc:ws_open(URI, #{})),
    stop_raw(Srv),
    ok.

handshake_parse_error(_Config) ->
    {Srv, Port} = start_raw(fun(Sock, _Key) ->
        ok = gen_tcp:send(Sock, <<"NOT-HTTP garbage line\r\n\r\n">>),
        _ = gen_tcp:recv(Sock, 0, 5000),
        gen_tcp:close(Sock)
    end),
    URI = raw_uri(Port, <<"/ws/echo">>),
    ?assertMatch({error, {connect_error, _}}, nhttpc:ws_open(URI, #{})),
    stop_raw(Srv),
    ok.

%%%-----------------------------------------------------------------------------
%%% WSS PATHS (TLS nhttp server)
%%%-----------------------------------------------------------------------------

wss_open_send_text(Config) ->
    URI = wss_uri(Config, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{tls => #{verify => verify_none}}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"tls-hi">>}),
    ?assertEqual({ws, {text, <<"tls-hi">>}}, wait_event(Handle)),
    ok = nhttpc:cancel(Handle),
    ok.

wss_server_close(Config) ->
    URI = wss_uri(Config, <<"/ws/server-close">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{tls => #{verify => verify_none}}),
    ok = wait_ready(Handle),
    ok = nhttpc:ws_send(Handle, {text, <<"trigger">>}),
    ?assertMatch({done, {remote_closed, 1011}}, wait_terminal(Handle)),
    nhttpc:flush(Handle),
    ok.

%% No tls opts -> build_connect_opts/2 keeps the verify_peer base, which
%% the self-signed server cert fails. Exercises the ssl branch with the
%% empty-user-opts path.
wss_verify_peer_default(Config) ->
    URI = wss_uri(Config, <<"/ws/echo">>),
    ?assertMatch({error, {connect_error, _}}, nhttpc:ws_open(URI, #{})),
    ok.

wss_server_abrupt_close(Config) ->
    URI = wss_uri(Config, <<"/ws/echo">>),
    {ok, Handle} = nhttpc:ws_open(URI, #{tls => #{verify => verify_none}}),
    ok = wait_ready(Handle),
    nhttpc_test_server:stop(?config(server, Config)),
    case wait_terminal(Handle, 5000) of
        {done, _} -> ok;
        {error, _} -> ok;
        Other -> ct:fail({unexpected_terminal, Other})
    end,
    nhttpc:flush(Handle),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

ws_uri(Config, Path) ->
    Port = ?config(port, Config),
    raw_uri(Port, Path).

raw_uri(Port, Path) ->
    <<"ws://127.0.0.1:", (integer_to_binary(Port))/binary, Path/binary>>.

wss_uri(Config, Path) ->
    Port = ?config(port, Config),
    <<"wss://127.0.0.1:", (integer_to_binary(Port))/binary, Path/binary>>.

project_root() ->
    find_root(filename:dirname(code:which(?MODULE))).

find_root(Dir) ->
    case filelib:is_file(filename:join(Dir, "rebar.config")) of
        true ->
            Dir;
        false ->
            Parent = filename:dirname(Dir),
            case Parent =:= Dir of
                true -> ".";
                false -> find_root(Parent)
            end
    end.

wait_ready(Handle) ->
    wait_ready(Handle, 5000).

wait_ready(Handle, Timeout) ->
    receive
        {nhttpc, Handle, {ws_ready, _ConnInfo}} -> ok
    after Timeout ->
        {error, no_ws_ready}
    end.

wait_event(Handle) ->
    wait_event(Handle, 5000).

wait_event(Handle, Timeout) ->
    receive
        {nhttpc, Handle, {ws, Frame}} -> {ws, Frame}
    after Timeout ->
        {error, no_event}
    end.

wait_terminal(Handle) ->
    wait_terminal(Handle, 5000).

wait_terminal(Handle, Timeout) ->
    receive
        {nhttpc, Handle, {done, _} = E} -> E;
        {nhttpc, Handle, {error, _} = E} -> E;
        {nhttpc, Handle, {ws, _}} -> wait_terminal(Handle, Timeout)
    after Timeout ->
        {error, no_terminal}
    end.

drain_count(Handle) ->
    receive
        {nhttpc, Handle, _} -> 1 + drain_count(Handle)
    after 100 ->
        0
    end.

start_raw(Fun) ->
    {ok, LSock} = gen_tcp:listen(0, [
        binary, {active, false}, {reuseaddr, true}, {packet, raw}
    ]),
    {ok, Port} = inet:port(LSock),
    Pid = spawn(fun() ->
        {ok, Sock} = gen_tcp:accept(LSock, 10000),
        Key = read_handshake_key(Sock, <<>>),
        Fun(Sock, Key),
        try
            gen_tcp:close(LSock)
        catch
            _:_ -> ok
        end
    end),
    {{Pid, LSock}, Port}.

stop_raw({Pid, LSock}) ->
    try
        gen_tcp:close(LSock)
    catch
        _:_ -> ok
    end,
    exit(Pid, kill),
    ok.

read_handshake_key(Sock, Acc) ->
    case binary:match(Acc, <<"\r\n\r\n">>) of
        nomatch ->
            {ok, Data} = gen_tcp:recv(Sock, 0, 5000),
            read_handshake_key(Sock, <<Acc/binary, Data/binary>>);
        _ ->
            extract_key(Acc)
    end.

extract_key(Req) ->
    case binary:split(Req, <<"Sec-WebSocket-Key: ">>) of
        [_, AfterKey] ->
            [Key | _] = binary:split(AfterKey, <<"\r\n">>),
            Key;
        _ ->
            <<>>
    end.

raw_drain(Sock) ->
    case gen_tcp:recv(Sock, 0, 500) of
        {ok, _} -> raw_drain(Sock);
        {error, _} -> ok
    end.
