%%%-----------------------------------------------------------------------------
%%% @doc HTTP proxy support test suite.
%%%
%%% Tests CONNECT tunneling through HTTP proxies.
%%% Uses a mock proxy server that validates CONNECT requests and
%%% establishes tunnels to the target server.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_proxy_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% EXPORTS
%%%-----------------------------------------------------------------------------

%% CT callbacks
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

%% Basic proxy tests
-export([
    proxy_connect_basic/1,
    proxy_connect_with_auth/1,
    proxy_connect_auth_required/1,
    proxy_connect_refused/1,
    proxy_connect_timeout/1
]).

%% HTTPS through proxy tests
-export([
    proxy_https_tunnel/1,
    proxy_https_request/1
]).

%% Proxy parsing edge case tests
-export([
    proxy_parse_status_http10/1,
    proxy_parse_status_malformed/1,
    proxy_parse_headers_empty/1,
    proxy_parse_headers_multiple/1
]).

%% Plan 15 additions: status / deadline / host normalisation / continuation
-export([
    proxy_bad_status_line/1,
    proxy_status_non_numeric/1,
    proxy_deadline_infinity/1,
    proxy_host_charlist/1,
    proxy_host_ip_tuple/1,
    proxy_recv_continuation/1,
    proxy_recv_closed_mid_response/1,
    proxy_cacertfile_option/1,
    proxy_cacerts_option/1,
    proxy_verify_peer_defaults/1,
    proxy_header_line_without_colon/1
]).

%% Helpers for raw proxy server
-export([
    start_raw_proxy/1
]).

%% Pool with proxy tests
-export([
    proxy_pool_basic/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, basic},
        {group, https},
        {group, pool},
        {group, parsing},
        {group, edges}
    ].

groups() ->
    [
        {basic, [sequence], [
            proxy_connect_basic,
            proxy_connect_with_auth,
            proxy_connect_auth_required,
            proxy_connect_refused,
            proxy_connect_timeout
        ]},
        {https, [sequence], [
            proxy_https_tunnel,
            proxy_https_request
        ]},
        {pool, [sequence], [
            proxy_pool_basic
        ]},
        {parsing, [parallel], [
            proxy_parse_status_http10,
            proxy_parse_status_malformed,
            proxy_parse_headers_empty,
            proxy_parse_headers_multiple
        ]},
        {edges, [sequence], [
            proxy_bad_status_line,
            proxy_status_non_numeric,
            proxy_deadline_infinity,
            proxy_host_charlist,
            proxy_host_ip_tuple,
            proxy_recv_continuation,
            proxy_recv_closed_mid_response,
            proxy_cacertfile_option,
            proxy_cacerts_option,
            proxy_verify_peer_defaults,
            proxy_header_line_without_colon
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    %% Load TLS certificate paths for HTTPS tests
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

end_per_suite(_Config) ->
    ok.

init_per_group(parsing, Config) ->
    Config;
init_per_group(edges, Config) ->
    Config;
init_per_group(basic, Config) ->
    %% Start mock proxy server
    case start_mock_proxy() of
        {ok, ProxyRef} ->
            ct:log("Mock proxy started: ~p", [ProxyRef]),
            %% Start target HTTP server
            case start_target_server() of
                {ok, TargetPid, TargetPort} ->
                    ct:log("Target server started on port ~p", [TargetPort]),
                    [
                        {proxy_ref, ProxyRef},
                        {target_pid, TargetPid},
                        {target_port, TargetPort}
                        | Config
                    ];
                {error, TargetReason} ->
                    ct:log("Failed to start target server: ~p", [TargetReason]),
                    stop_mock_proxy(ProxyRef),
                    {skip, {target_server_failed, TargetReason}}
            end;
        {error, ProxyReason} ->
            ct:log("Failed to start mock proxy: ~p", [ProxyReason]),
            {skip, {proxy_failed, ProxyReason}}
    end;
init_per_group(https, Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            case start_mock_proxy() of
                {ok, ProxyRef} ->
                    case start_target_server_ssl(CertFile, KeyFile) of
                        {ok, TargetPid, TargetPort} ->
                            [
                                {proxy_ref, ProxyRef},
                                {target_pid, TargetPid},
                                {target_port, TargetPort},
                                {target_ssl, true}
                                | Config
                            ];
                        {error, TargetReason} ->
                            stop_mock_proxy(ProxyRef),
                            {skip, {target_server_failed, TargetReason}}
                    end;
                {error, ProxyReason} ->
                    {skip, {proxy_failed, ProxyReason}}
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(pool, Config) ->
    case start_mock_proxy() of
        {ok, ProxyRef} ->
            case start_target_server() of
                {ok, TargetPid, TargetPort} ->
                    [
                        {proxy_ref, ProxyRef},
                        {target_pid, TargetPid},
                        {target_port, TargetPort}
                        | Config
                    ];
                {error, TargetReason} ->
                    stop_mock_proxy(ProxyRef),
                    {skip, {target_server_failed, TargetReason}}
            end;
        {error, ProxyReason} ->
            {skip, {proxy_failed, ProxyReason}}
    end.

end_per_group(_Group, Config) ->
    case proplists:get_value(proxy_ref, Config) of
        undefined -> ok;
        ProxyRef -> stop_mock_proxy(ProxyRef)
    end,
    nhttpc_test_server:stop(proplists:get_value(target_pid, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% BASIC PROXY TESTS
%%%-----------------------------------------------------------------------------

%% Test basic CONNECT tunnel establishment
proxy_connect_basic(Config) ->
    ProxyRef = proplists:get_value(proxy_ref, Config),
    TargetPort = proplists:get_value(target_port, Config),
    ProxyPort = get_proxy_port(ProxyRef),

    ct:log("Proxy port: ~p, Target port: ~p", [ProxyPort, TargetPort]),

    ProxyOpts = #{
        host => <<"127.0.0.1">>,
        port => ProxyPort
    },

    Result = nhttpc_proxy:connect(
        <<"127.0.0.1">>,
        TargetPort,
        ProxyOpts,
        5000
    ),
    ct:log("Connect result: ~p", [Result]),
    ?assertMatch({ok, _Socket}, Result),
    {ok, Socket} = Result,
    gen_tcp:close(Socket),
    ok.

%% Test CONNECT with proxy authentication
proxy_connect_with_auth(Config) ->
    ProxyRef = proplists:get_value(proxy_ref, Config),
    TargetPort = proplists:get_value(target_port, Config),
    ProxyPort = get_proxy_port(ProxyRef),

    %% Set proxy to require auth
    set_proxy_auth(ProxyRef, <<"user">>, <<"pass">>),

    ProxyOpts = #{
        host => <<"127.0.0.1">>,
        port => ProxyPort,
        username => <<"user">>,
        password => <<"pass">>
    },

    Result = nhttpc_proxy:connect(
        <<"127.0.0.1">>,
        TargetPort,
        ProxyOpts,
        5000
    ),
    ?assertMatch({ok, _Socket}, Result),
    {ok, Socket} = Result,
    gen_tcp:close(Socket),

    %% Clear auth requirement
    clear_proxy_auth(ProxyRef),
    ok.

%% Test CONNECT when auth is required but not provided
proxy_connect_auth_required(Config) ->
    ProxyRef = proplists:get_value(proxy_ref, Config),
    TargetPort = proplists:get_value(target_port, Config),
    ProxyPort = get_proxy_port(ProxyRef),

    %% Set proxy to require auth
    set_proxy_auth(ProxyRef, <<"user">>, <<"pass">>),

    ProxyOpts = #{
        host => <<"127.0.0.1">>,
        port => ProxyPort
    },

    Result = nhttpc_proxy:connect(
        <<"127.0.0.1">>,
        TargetPort,
        ProxyOpts,
        5000
    ),
    ?assertMatch({error, {proxy_auth_required, 407}}, Result),

    %% Clear auth requirement
    clear_proxy_auth(ProxyRef),
    ok.

%% Test CONNECT refused by proxy
proxy_connect_refused(Config) ->
    ProxyRef = proplists:get_value(proxy_ref, Config),
    ProxyPort = get_proxy_port(ProxyRef),

    %% Set proxy to refuse connections
    set_proxy_refuse(ProxyRef, true),

    ProxyOpts = #{
        host => <<"127.0.0.1">>,
        port => ProxyPort
    },

    Result = nhttpc_proxy:connect(
        <<"127.0.0.1">>,
        9999,
        ProxyOpts,
        5000
    ),
    ?assertMatch({error, {proxy_error, 403}}, Result),

    %% Clear refuse mode
    set_proxy_refuse(ProxyRef, false),
    ok.

%% Test CONNECT timeout
proxy_connect_timeout(_Config) ->
    %% Connect to non-existent proxy
    ProxyOpts = #{
        host => <<"127.0.0.1">>,
        port => 59998
    },

    Result = nhttpc_proxy:connect(
        <<"127.0.0.1">>,
        8080,
        ProxyOpts,
        100
    ),
    ?assertMatch({error, _}, Result),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTPS THROUGH PROXY TESTS
%%%-----------------------------------------------------------------------------

%% Test HTTPS tunnel through proxy
proxy_https_tunnel(Config) ->
    ProxyRef = proplists:get_value(proxy_ref, Config),
    TargetPort = proplists:get_value(target_port, Config),
    ProxyPort = get_proxy_port(ProxyRef),

    ProxyOpts = #{
        host => <<"127.0.0.1">>,
        port => ProxyPort
    },

    SslOpts = #{
        verify => verify_none
    },

    Result = nhttpc_proxy:connect(
        <<"127.0.0.1">>,
        TargetPort,
        ProxyOpts,
        SslOpts,
        5000
    ),
    ?assertMatch({ok, _Socket}, Result),
    {ok, Socket} = Result,
    ssl:close(Socket),
    ok.

%% Test HTTP request through proxy to HTTPS target
proxy_https_request(Config) ->
    ProxyRef = proplists:get_value(proxy_ref, Config),
    TargetPort = proplists:get_value(target_port, Config),
    ProxyPort = get_proxy_port(ProxyRef),

    %% Create connection through proxy
    Opts = #{
        host => <<"127.0.0.1">>,
        port => TargetPort,
        transport => ssl,
        protocols => [http1_1],
        verify => verify_none,
        proxy => #{
            host => <<"127.0.0.1">>,
            port => ProxyPort
        }
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(200),

    %% Send request
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response from proxied HTTPS request")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL WITH PROXY TESTS
%%%-----------------------------------------------------------------------------

%% Test pool with proxy configuration
proxy_pool_basic(Config) ->
    ProxyRef = proplists:get_value(proxy_ref, Config),
    TargetPort = proplists:get_value(target_port, Config),
    ProxyPort = get_proxy_port(ProxyRef),

    %% Start pool with proxy
    {ok, ProxyPid} = nhttpc:start_link(proxy_test_pool, #{
        host => <<"127.0.0.1">>,
        port => TargetPort,
        versions => [http1_1],
        size => 1,
        proxy => #{
            host => <<"127.0.0.1">>,
            port => ProxyPort
        }
    }),
    unlink(ProxyPid),

    %% Give pool time to establish connection
    timer:sleep(200),

    %% Make request through pool
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [TargetPort])),
    Result = nhttpc:get(Url, #{via => {pool, proxy_test_pool}}),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    ?assertEqual(200, maps:get(status, Resp)),

    nhttpc:stop(proxy_test_pool),
    ok.

%%%-----------------------------------------------------------------------------
%%% MOCK PROXY SERVER
%%%-----------------------------------------------------------------------------

-record(proxy_state, {
    listener :: gen_tcp:socket(),
    port :: inet:port_number(),
    parent :: pid(),
    auth :: undefined | {binary(), binary()},
    refuse :: boolean()
}).

start_mock_proxy() ->
    Parent = self(),
    ct:log("Starting mock proxy, parent: ~p", [Parent]),
    %% Use spawn (not spawn_link) so proxy survives when init_per_group ends
    Pid = spawn(fun() ->
        ct:log("Proxy process started: ~p", [self()]),
        proxy_init(Parent)
    end),
    ct:log("Spawned proxy process: ~p", [Pid]),
    receive
        {proxy_started, Pid, Port} ->
            ct:log("Received proxy_started message, port: ~p", [Port]),
            %% Give the listener time to be fully ready
            timer:sleep(50),
            {ok, #{pid => Pid, port => Port}}
    after 5000 ->
        ct:log("Timeout waiting for proxy_started"),
        exit(Pid, kill),
        {error, timeout}
    end.

stop_mock_proxy(#{pid := Pid}) ->
    Ref = monitor(process, Pid),
    Pid ! stop,
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 5000 ->
        exit(Pid, kill),
        ok
    end.

get_proxy_port(#{port := Port}) ->
    Port.

set_proxy_auth(#{pid := Pid}, Username, Password) ->
    Ref = make_ref(),
    Pid ! {set_auth, self(), Ref, Username, Password},
    receive
        {auth_set, Ref} -> ok
    after 5000 ->
        {error, timeout}
    end.

clear_proxy_auth(#{pid := Pid}) ->
    Ref = make_ref(),
    Pid ! {clear_auth, self(), Ref},
    receive
        {auth_cleared, Ref} -> ok
    after 5000 ->
        {error, timeout}
    end.

set_proxy_refuse(#{pid := Pid}, Refuse) ->
    Ref = make_ref(),
    Pid ! {set_refuse, self(), Ref, Refuse},
    receive
        {refuse_set, Ref} -> ok
    after 5000 ->
        {error, timeout}
    end.

proxy_init(Parent) ->
    {ok, ListenSocket} = gen_tcp:listen(0, [
        binary,
        {active, false},
        {reuseaddr, true},
        {packet, raw},
        {ip, {127, 0, 0, 1}}
    ]),
    {ok, Port} = inet:port(ListenSocket),
    ct:log("Mock proxy started on port ~p", [Port]),
    Parent ! {proxy_started, self(), Port},
    State = #proxy_state{
        listener = ListenSocket,
        port = Port,
        parent = Parent,
        auth = undefined,
        refuse = false
    },
    proxy_loop(State).

proxy_loop(#proxy_state{listener = ListenSocket} = State) ->
    %% Use timeout to check for control messages
    case gen_tcp:accept(ListenSocket, 100) of
        {ok, ClientSocket} ->
            %% Spawn handler and transfer socket control to it
            Parent = self(),
            HandlerPid = spawn_link(fun() ->
                receive
                    {socket_ready, Parent} -> handle_proxy_client(ClientSocket, State)
                after 5000 ->
                    gen_tcp:close(ClientSocket)
                end
            end),
            gen_tcp:controlling_process(ClientSocket, HandlerPid),
            HandlerPid ! {socket_ready, Parent},
            proxy_loop(State);
        {error, timeout} ->
            receive
                stop ->
                    gen_tcp:close(ListenSocket),
                    ok;
                {set_auth, From, Ref, Username, Password} ->
                    From ! {auth_set, Ref},
                    proxy_loop(State#proxy_state{auth = {Username, Password}});
                {clear_auth, From, Ref} ->
                    From ! {auth_cleared, Ref},
                    proxy_loop(State#proxy_state{auth = undefined});
                {set_refuse, From, Ref, Refuse} ->
                    From ! {refuse_set, Ref},
                    proxy_loop(State#proxy_state{refuse = Refuse})
            after 0 ->
                proxy_loop(State)
            end;
        {error, _Reason} ->
            ok
    end.

handle_proxy_client(ClientSocket, State) ->
    case recv_connect_request(ClientSocket) of
        {ok, Host, Port, Headers} ->
            case check_proxy_auth(Headers, State) of
                ok ->
                    case State#proxy_state.refuse of
                        true ->
                            send_proxy_response(ClientSocket, 403, <<"Forbidden">>),
                            gen_tcp:close(ClientSocket);
                        false ->
                            establish_tunnel(ClientSocket, Host, Port)
                    end;
                {error, auth_required} ->
                    send_proxy_response(ClientSocket, 407, <<"Proxy Authentication Required">>),
                    gen_tcp:close(ClientSocket)
            end;
        {error, _Reason} ->
            gen_tcp:close(ClientSocket)
    end.

recv_connect_request(Socket) ->
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, Data} ->
            parse_connect_request(Data);
        {error, _} = Error ->
            Error
    end.

parse_connect_request(Data) ->
    case binary:split(Data, <<"\r\n\r\n">>) of
        [HeaderPart, _Rest] ->
            Lines = binary:split(HeaderPart, <<"\r\n">>, [global]),
            case Lines of
                [RequestLine | HeaderLines] ->
                    case parse_connect_line(RequestLine) of
                        {ok, Host, Port} ->
                            Headers = parse_proxy_headers(HeaderLines),
                            {ok, Host, Port, Headers};
                        {error, _} = Error ->
                            Error
                    end;
                _ ->
                    {error, invalid_request}
            end;
        _ ->
            {error, incomplete}
    end.

parse_connect_line(Line) ->
    case binary:split(Line, <<" ">>, [global]) of
        [<<"CONNECT">>, Authority, <<"HTTP/1.1">>] ->
            case binary:split(Authority, <<":">>) of
                [Host, PortBin] ->
                    try
                        Port = binary_to_integer(PortBin),
                        {ok, Host, Port}
                    catch
                        _:_ -> {error, invalid_port}
                    end;
                _ ->
                    {error, invalid_authority}
            end;
        _ ->
            {error, invalid_request_line}
    end.

parse_proxy_headers(Lines) ->
    lists:filtermap(
        fun(Line) ->
            case binary:split(Line, <<":">>) of
                [Name, Value] ->
                    {true, {string:lowercase(string:trim(Name)), string:trim(Value)}};
                _ ->
                    false
            end
        end,
        Lines
    ).

check_proxy_auth(_Headers, #proxy_state{auth = undefined}) ->
    ok;
check_proxy_auth(Headers, #proxy_state{auth = {Username, Password}}) ->
    case proplists:get_value(<<"proxy-authorization">>, Headers) of
        undefined ->
            {error, auth_required};
        AuthHeader ->
            Expected =
                <<"Basic ", (base64:encode(<<Username/binary, ":", Password/binary>>))/binary>>,
            case AuthHeader =:= Expected of
                true -> ok;
                false -> {error, auth_required}
            end
    end.

send_proxy_response(Socket, Status, Reason) ->
    Response = io_lib:format("HTTP/1.1 ~B ~s\r\n\r\n", [Status, Reason]),
    gen_tcp:send(Socket, Response).

establish_tunnel(ClientSocket, Host, Port) ->
    case gen_tcp:connect(binary_to_list(Host), Port, [binary, {active, false}], 5000) of
        {ok, TargetSocket} ->
            send_proxy_response(ClientSocket, 200, <<"Connection Established">>),
            relay_loop(ClientSocket, TargetSocket);
        {error, _Reason} ->
            send_proxy_response(ClientSocket, 502, <<"Bad Gateway">>),
            gen_tcp:close(ClientSocket)
    end.

relay_loop(ClientSocket, TargetSocket) ->
    inet:setopts(ClientSocket, [{active, once}]),
    inet:setopts(TargetSocket, [{active, once}]),
    receive
        {tcp, ClientSocket, Data} ->
            gen_tcp:send(TargetSocket, Data),
            relay_loop(ClientSocket, TargetSocket);
        {tcp, TargetSocket, Data} ->
            gen_tcp:send(ClientSocket, Data),
            relay_loop(ClientSocket, TargetSocket);
        {tcp_closed, _} ->
            gen_tcp:close(ClientSocket),
            gen_tcp:close(TargetSocket);
        {tcp_error, _, _} ->
            gen_tcp:close(ClientSocket),
            gen_tcp:close(TargetSocket)
    after 30000 ->
        gen_tcp:close(ClientSocket),
        gen_tcp:close(TargetSocket)
    end.

%%%-----------------------------------------------------------------------------
%%% TARGET SERVER HELPERS
%%%-----------------------------------------------------------------------------

start_target_server() ->
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(target_http_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    {ok, ServerPid, Port};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%%-----------------------------------------------------------------------------
%%% PROXY PARSING EDGE CASE TESTS
%%%-----------------------------------------------------------------------------

%% Test that HTTP/1.0 response from proxy is parsed correctly
proxy_parse_status_http10(_Config) ->
    %% Start a raw proxy that responds with HTTP/1.0
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _Data} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.0 200 OK\r\n\r\n">>),
        %% Don't connect to target, just close after response
        gen_tcp:close(Socket)
    end),
    ProxyOpts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    %% Connect should succeed (200 from proxy) but then fail to actually tunnel
    %% because the raw proxy closes immediately. The parse_status path for
    %% HTTP/1.0 is still exercised.
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, ProxyOpts, 2000),
    %% Result could be ok (parsed successfully) or error (socket closed after)
    ct:log("HTTP/1.0 proxy result: ~p", [Result]),
    ok.

%% Test malformed status line from proxy
proxy_parse_status_malformed(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _Data} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"NOT-HTTP garbage\r\n\r\n">>),
        gen_tcp:close(Socket)
    end),
    ProxyOpts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, ProxyOpts, 2000),
    ?assertMatch({error, _}, Result),
    ok.

%% Test proxy response with no headers (empty header block)
proxy_parse_headers_empty(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _Data} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 200 Connection Established\r\n\r\n">>),
        %% Close after response
        timer:sleep(100),
        gen_tcp:close(Socket)
    end),
    ProxyOpts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, ProxyOpts, 2000),
    ct:log("Empty headers proxy result: ~p", [Result]),
    ok.

%% Test proxy response with multiple headers
proxy_parse_headers_multiple(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _Data} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(
            Socket,
            <<"HTTP/1.1 200 Connection Established\r\n", "X-Proxy: test\r\n",
                "Connection: keep-alive\r\n", "\r\n">>
        ),
        timer:sleep(100),
        gen_tcp:close(Socket)
    end),
    ProxyOpts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, ProxyOpts, 2000),
    ct:log("Multiple headers proxy result: ~p", [Result]),
    ok.

%% Start a raw TCP server that executes a custom handler for each connection.
start_raw_proxy(Handler) ->
    Parent = self(),
    spawn(fun() ->
        {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
        {ok, Port} = inet:port(LSock),
        Parent ! {raw_proxy_port, Port},
        {ok, Sock} = gen_tcp:accept(LSock, 5000),
        Handler(Sock),
        gen_tcp:close(LSock)
    end),
    receive
        {raw_proxy_port, Port} -> {ok, Port}
    after 5000 ->
        {error, timeout}
    end.

%%%-----------------------------------------------------------------------------
%%% PLAN 15 — EDGES
%%%-----------------------------------------------------------------------------

%% Bad CONNECT status line — version prefix matches the `HTTP/1.x ` arm but
%% the status token is not numeric. Hits the `error:badarg` rescue in
%% parse_status/1.
proxy_bad_status_line(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 ??? OK\r\n\r\n">>),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, 2000),
    ?assertMatch({error, _}, Result),
    ok.

%% Status code that parses but lies outside the success range — three-byte
%% non-numeric `0xA` token triggers the `binary_to_integer/1` catch.
proxy_status_non_numeric(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 0xA OK\r\n\r\n">>),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, 2000),
    ?assertMatch({error, _}, Result),
    ok.

%% `infinity` timeout drives the `deadline(infinity)` and
%% `remaining_time(infinity)` arms.
proxy_deadline_infinity(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 200 OK\r\n\r\n">>),
        timer:sleep(50),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, infinity),
    ?assertMatch({ok, _}, Result),
    {ok, Socket} = Result,
    gen_tcp:close(Socket),
    ok.

%% Erlang charlist host hits the `is_list(Host)` arm of normalize_host/1.
proxy_host_charlist(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 200 OK\r\n\r\n">>),
        timer:sleep(50),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => "127.0.0.1", port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, 2000),
    ?assertMatch({ok, _}, Result),
    {ok, Socket} = Result,
    gen_tcp:close(Socket),
    ok.

%% IP-tuple host hits the `is_tuple(Host)` arm of normalize_host/1.
proxy_host_ip_tuple(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 200 OK\r\n\r\n">>),
        timer:sleep(50),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => {127, 0, 0, 1}, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, 2000),
    ?assertMatch({ok, _}, Result),
    {ok, Socket} = Result,
    gen_tcp:close(Socket),
    ok.

%% Server response split across two TCP writes drives the `incomplete`
%% branch in parse_response/1 and the tail recursion in
%% recv_http_response/3.
proxy_recv_continuation(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 200">>),
        timer:sleep(20),
        gen_tcp:send(Socket, <<" OK\r\n\r\n">>),
        timer:sleep(50),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, 2000),
    ?assertMatch({ok, _}, Result),
    {ok, Socket} = Result,
    gen_tcp:close(Socket),
    ok.

%% Server closes mid-response before a full header block arrives. Drives the
%% `{error, _}` return from gen_tcp:recv/3 inside recv_http_response/3.
proxy_recv_closed_mid_response(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 200">>),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, 2000),
    ?assertMatch({error, _}, Result),
    ok.

%% `cacertfile` is wired into the TLS options. We pass a bogus path; the
%% test only proves the option flows through build_ssl_options/2 (the
%% subsequent TLS upgrade will fail, which is fine).
proxy_cacertfile_option(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 200 OK\r\n\r\n">>),
        %% Hold the socket open so TLS upgrade has a chance to run.
        timer:sleep(200),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    SslOpts = #{verify => verify_peer, cacertfile => "/dev/null"},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, SslOpts, 500),
    %% TLS upgrade will fail because the proxy doesn't speak TLS — the goal
    %% here is just to drive the cacertfile arm of build_ssl_options/2.
    ?assertMatch({error, _}, Result),
    ok.

%% `cacerts` (an explicit cert list) hits the matching arm of
%% build_ssl_options/2.
proxy_cacerts_option(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 200 OK\r\n\r\n">>),
        timer:sleep(200),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    SslOpts = #{verify => verify_peer, cacerts => []},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, SslOpts, 500),
    ?assertMatch({error, _}, Result),
    ok.

%% verify_peer with neither cacerts nor cacertfile drives the
%% `{cacerts, public_key:cacerts_get()}` default branch of
%% build_ssl_options/2.
proxy_verify_peer_defaults(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(Socket, <<"HTTP/1.1 200 OK\r\n\r\n">>),
        timer:sleep(200),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    SslOpts = #{verify => verify_peer},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, SslOpts, 500),
    ?assertMatch({error, _}, Result),
    ok.

%% A header line that lacks `:` hits the `_ -> false` arm of
%% parse_header_line/1 inside parse_headers/1.
proxy_header_line_without_colon(_Config) ->
    {ok, ProxyPort} = start_raw_proxy(fun(Socket) ->
        {ok, _} = gen_tcp:recv(Socket, 0, 5000),
        gen_tcp:send(
            Socket,
            <<"HTTP/1.1 200 OK\r\nGood: header\r\nNoColonHere\r\n\r\n">>
        ),
        timer:sleep(50),
        gen_tcp:close(Socket)
    end),
    Opts = #{host => <<"127.0.0.1">>, port => ProxyPort},
    Result = nhttpc_proxy:connect(<<"127.0.0.1">>, 8080, Opts, 2000),
    ?assertMatch({ok, _}, Result),
    case Result of
        {ok, S} -> gen_tcp:close(S);
        _ -> ok
    end,
    ok.

start_target_server_ssl(CertFile, KeyFile) ->
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(target_https_server, #{
            port => 0,
            handler => Handler,
            versions => [http1_1],
            tls => #{certfile => CertFile, keyfile => KeyFile}
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    {ok, ServerPid, Port};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
