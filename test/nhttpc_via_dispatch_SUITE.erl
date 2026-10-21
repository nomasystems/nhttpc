%%%-----------------------------------------------------------------------------
%%% @doc `via` dispatch suite — `nhttpc:open/2`, `nhttpc:close/1`.
%%%
%%% Pins down the three dispatch modes (`oneshot`, `{pool, _}`,
%%% `{conn, _}`) and verifies that the flat `pool => Ref` opt is not
%%% accepted. Covers the open / close lifecycle and the ws_open guards
%%% on non-default `via`.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_via_dispatch_SUITE).

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
    oneshot_default_works_for_get/1,
    oneshot_default_works_for_post/1,
    via_pool_routes_through_pool/1,
    via_pool_works_for_post/1,
    via_pool_sends_accept_encoding/1,
    via_pool_decompresses_response/1,
    via_conn_decompresses_response/1,
    pool_opt_no_longer_accepted/1,
    via_conn_routes_through_caller_owned_conn/1,
    via_conn_h2_multiplexes_two_concurrent_requests/1,
    ws_open_with_non_default_via_returns_invalid_opts/1,
    open_close_lifecycle/1,
    close_idempotent/1,
    open_to_unreachable_returns_connect_error/1
]).

%%%-----------------------------------------------------------------------------
%%% CT BOILERPLATE
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {seconds, 60}}].

all() ->
    [
        {group, h1_dispatch},
        {group, h2_dispatch},
        {group, lifecycle}
    ].

groups() ->
    [
        {h1_dispatch, [sequence], [
            oneshot_default_works_for_get,
            oneshot_default_works_for_post,
            via_pool_routes_through_pool,
            via_pool_works_for_post,
            via_pool_sends_accept_encoding,
            via_pool_decompresses_response,
            via_conn_decompresses_response,
            pool_opt_no_longer_accepted,
            via_conn_routes_through_caller_owned_conn
        ]},
        {h2_dispatch, [sequence], [
            via_conn_h2_multiplexes_two_concurrent_requests
        ]},
        {lifecycle, [sequence], [
            ws_open_with_non_default_via_returns_invalid_opts,
            open_close_lifecycle,
            close_idempotent,
            open_to_unreachable_returns_connect_error
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

init_per_group(h2_dispatch, Config) ->
    case proplists:get_value(certfile, Config) of
        undefined ->
            {skip, "TLS certificates not available"};
        CertFile ->
            KeyFile = proplists:get_value(keyfile, Config),
            case start_h2_server(CertFile, KeyFile) of
                {ok, ServerPid, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to start H2 server: ~p", [Reason])
            end
    end;
init_per_group(_Group, Config) ->
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
%%% ONESHOT MODE
%%%-----------------------------------------------------------------------------

oneshot_default_works_for_get(Config) ->
    Port = ?config(server_port, Config),
    {ok, #{status := 200, body := Body}} = nhttpc:get(url(Port, <<"/echo">>), #{}),
    ?assertMatch(<<"GET request to /echo", _/binary>>, Body),
    ok.

oneshot_default_works_for_post(Config) ->
    Port = ?config(server_port, Config),
    {ok, #{status := 200, body := Body}} = nhttpc:post(
        url(Port, <<"/upload/echo">>), <<"payload">>, #{}
    ),
    ?assertMatch(<<"Received: 7 bytes", _/binary>>, Body),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL MODE
%%%-----------------------------------------------------------------------------

via_pool_routes_through_pool(Config) ->
    Port = ?config(server_port, Config),
    PoolRef = via_pool_routes_through_pool,
    {ok, PoolSup} = nhttpc:start_link(PoolRef, #{
        host => <<"127.0.0.1">>, port => Port, size => 2, versions => [http1_1]
    }),
    unlink(PoolSup),
    ok = nhttpc_pool:wait_ready(PoolRef, 5000),
    try
        {ok, #{status := 200, body := Body}} =
            nhttpc:get(url(Port, <<"/echo">>), #{via => {pool, PoolRef}}),
        ?assertMatch(<<"GET request to /echo", _/binary>>, Body)
    after
        nhttpc:stop(PoolRef)
    end,
    ok.

via_pool_works_for_post(Config) ->
    Port = ?config(server_port, Config),
    PoolRef = via_pool_post,
    {ok, PoolSup} = nhttpc:start_link(PoolRef, #{
        host => <<"127.0.0.1">>, port => Port, size => 2, versions => [http1_1]
    }),
    unlink(PoolSup),
    ok = nhttpc_pool:wait_ready(PoolRef, 5000),
    try
        {ok, #{status := 200, body := Body}} =
            nhttpc:post(
                url(Port, <<"/upload/echo">>), <<"hello">>, #{via => {pool, PoolRef}}
            ),
        ?assertMatch(<<"Received: 5 bytes", _/binary>>, Body)
    after
        nhttpc:stop(PoolRef)
    end,
    ok.

pool_opt_no_longer_accepted(_Config) ->
    %% Flat `pool => Ref` opt is not accepted; `via => {pool, Ref}` is the
    %% only way to dispatch through a pool.
    ?assertEqual(
        {error, {invalid_opts, {unknown, pool}}},
        nhttpc_opts:normalize(#{pool => some_pool})
    ),
    ok.

%%%-----------------------------------------------------------------------------
%%% ON-CONNECTION MODE
%%%-----------------------------------------------------------------------------
via_pool_sends_accept_encoding(Config) ->
    Port = ?config(server_port, Config),
    PoolRef = via_pool_sends_accept_encoding,
    {ok, PoolSup} = nhttpc:start_link(PoolRef, #{
        host => <<"127.0.0.1">>, port => Port, size => 1, versions => [http1_1]
    }),
    unlink(PoolSup),
    ok = nhttpc_pool:wait_ready(PoolRef, 5000),
    try
        {ok, #{status := 200, body := Body}} =
            nhttpc:get(url(Port, <<"/echo">>), #{via => {pool, PoolRef}}),
        ?assertNotEqual(nomatch, binary:match(Body, <<"Accept-Encoding: gzip, deflate">>))
    after
        nhttpc:stop(PoolRef)
    end,
    ok.

via_pool_decompresses_response(Config) ->
    Port = ?config(server_port, Config),
    PoolRef = via_pool_decompresses_response,
    {ok, PoolSup} = nhttpc:start_link(PoolRef, #{
        host => <<"127.0.0.1">>, port => Port, size => 1, versions => [http1_1]
    }),
    unlink(PoolSup),
    ok = nhttpc_pool:wait_ready(PoolRef, 5000),
    try
        {ok, #{status := 200, body := Body, headers := Headers}} =
            nhttpc:get(url(Port, <<"/gzip">>), #{via => {pool, PoolRef}}),
        ?assertEqual(<<"Hello, compressed world! This is a gzip response.">>, Body),
        ?assertEqual(undefined, proplists:get_value(<<"content-encoding">>, Headers)),
        {ok, #{body := RawBody, headers := RawHeaders}} =
            nhttpc:get(url(Port, <<"/gzip">>), #{
                via => {pool, PoolRef}, decompress => false
            }),
        ?assertNotEqual(<<"Hello, compressed world! This is a gzip response.">>, RawBody),
        ?assertEqual(<<"gzip">>, proplists:get_value(<<"content-encoding">>, RawHeaders))
    after
        nhttpc:stop(PoolRef)
    end,
    ok.

via_conn_decompresses_response(Config) ->
    Port = ?config(server_port, Config),
    Target = #{host => <<"127.0.0.1">>, port => Port},
    {ok, Conn} = nhttpc:open(Target, #{versions => [http1_1]}),
    try
        ConnReq = #{
            method => get,
            path => <<"/gzip">>,
            headers => [{<<"host">>, <<"127.0.0.1">>}],
            body => <<>>
        },
        {ok, #{status := 200, body := Body, headers := Headers}} =
            nhttpc:request(ConnReq, #{via => {conn, Conn}}),
        ?assertEqual(<<"Hello, compressed world! This is a gzip response.">>, Body),
        ?assertEqual(undefined, proplists:get_value(<<"content-encoding">>, Headers))
    after
        ok = nhttpc:close(Conn)
    end,
    ok.

via_conn_routes_through_caller_owned_conn(Config) ->
    Port = ?config(server_port, Config),
    Target = #{host => <<"127.0.0.1">>, port => Port},
    {ok, Conn} = nhttpc:open(Target, #{versions => [http1_1]}),
    try
        ?assertMatch(#{pid := _, version := http1_1, peer := {_, _}}, Conn),
        ConnReq = #{
            method => get,
            path => <<"/echo">>,
            headers => [{<<"host">>, <<"127.0.0.1">>}],
            body => <<>>
        },
        {ok, #{status := 200, body := Body}} =
            nhttpc:request(ConnReq, #{via => {conn, Conn}}),
        ?assertMatch(<<"GET request to /echo", _/binary>>, Body)
    after
        ok = nhttpc:close(Conn)
    end,
    ok.

via_conn_h2_multiplexes_two_concurrent_requests(Config) ->
    Port = ?config(server_port, Config),
    Target = #{host => <<"127.0.0.1">>, port => Port},
    Opts = #{
        versions => [http2],
        tls => #{verify => verify_none}
    },
    {ok, Conn} = nhttpc:open(Target, Opts),
    try
        ?assertMatch(#{version := http2}, Conn),
        Self = self(),
        F = fun(Tag, Path) ->
            spawn_link(fun() ->
                Req = #{
                    method => get,
                    path => Path,
                    headers => [{<<"host">>, <<"127.0.0.1">>}],
                    body => <<>>
                },
                Self ! {Tag, nhttpc:request(Req, #{via => {conn, Conn}})}
            end)
        end,
        _ = F(req_a, <<"/echo">>),
        _ = F(req_b, <<"/final">>),
        receive
            {req_a, {ok, #{status := 200}}} -> ok
        after 10000 ->
            ct:fail("req_a timeout")
        end,
        receive
            {req_b, {ok, #{status := 200}}} -> ok
        after 10000 ->
            ct:fail("req_b timeout")
        end
    after
        ok = nhttpc:close(Conn)
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% LIFECYCLE / GUARDS
%%%-----------------------------------------------------------------------------

ws_open_with_non_default_via_returns_invalid_opts(_Config) ->
    Conn = #{pid => self(), version => http1_1, peer => {{127, 0, 0, 1}, 1}},
    ?assertEqual(
        {error, {invalid_opts, ws_on_connection}},
        nhttpc:ws_open(<<"ws://127.0.0.1:1/ws">>, #{via => {conn, Conn}})
    ),
    ?assertEqual(
        {error, {invalid_opts, ws_via_not_oneshot}},
        nhttpc:ws_open(<<"ws://127.0.0.1:1/ws">>, #{via => {pool, fake}})
    ),
    ok.

open_close_lifecycle(Config) ->
    Port = ?config(server_port, Config),
    Target = #{host => <<"127.0.0.1">>, port => Port},
    {ok, Conn} = nhttpc:open(Target, #{versions => [http1_1]}),
    #{pid := Pid} = Conn,
    ?assert(is_process_alive(Pid)),
    ok = nhttpc:close(Conn),
    %% Give the linked process a moment to wind down.
    timer:sleep(50),
    ?assertNot(is_process_alive(Pid)),
    ok.

close_idempotent(Config) ->
    Port = ?config(server_port, Config),
    Target = #{host => <<"127.0.0.1">>, port => Port},
    {ok, Conn} = nhttpc:open(Target, #{versions => [http1_1]}),
    ok = nhttpc:close(Conn),
    ok = nhttpc:close(Conn),
    ok.

open_to_unreachable_returns_connect_error(_Config) ->
    Target = #{host => <<"127.0.0.1">>, port => unused_port()},
    Result = nhttpc:open(Target, #{
        versions => [http1_1],
        timeouts => #{connect => 500}
    }),
    case Result of
        {error, {connect_error, _}} -> ok;
        Other -> ct:fail({unexpected, Other})
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"http://127.0.0.1:", PortBin/binary, Path/binary>>.

start_h1_server() ->
    case
        nhttp:start_link(via_dispatch_h1, #{
            port => 0,
            handler => nhttp_test_handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, _} = Error ->
            Error
    end.

start_h2_server(CertFile, KeyFile) ->
    case
        nhttp:start_link(via_dispatch_h2, #{
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
        {error, _} = Error ->
            Error
    end.

unused_port() ->
    {ok, Sock} = gen_tcp:listen(0, [{reuseaddr, true}]),
    {ok, Port} = inet:port(Sock),
    gen_tcp:close(Sock),
    Port.

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
