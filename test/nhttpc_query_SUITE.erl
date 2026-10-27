-module(nhttpc_query_SUITE).

-moduledoc """
Common Test suite for the QUERY method (RFC 10008) on HTTP/1.1, HTTP/2 and
HTTP/3. Each group sends a QUERY request with a body through the oneshot
path and through a pool. The test server echoes the method and the body.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(POOL, nhttpc_query_pool).
-define(FORM, <<"application/x-www-form-urlencoded">>).

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
    query_oneshot/1,
    query_pooled/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
        {group, h1},
        {group, h2},
        {group, h3}
    ].

groups() ->
    Cases = [query_oneshot, query_pooled],
    [
        {h1, [sequence], Cases},
        {h2, [sequence], Cases},
        {h3, [sequence], Cases}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ssl),
    PrivDir = proplists:get_value(priv_dir, Config, ""),
    ConfDir = filename:join([find_project_root(PrivDir), "test", "conf"]),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_file(CertFile) andalso filelib:is_file(KeyFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> Config
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(h1, Config) ->
    start_server(Config, query_h1, #{protocols => [http1_1]}, http, #{versions => [http1_1]});
init_per_group(h2, Config) ->
    start_tls_server(Config, query_h2, [http2, http1_1], [http2]);
init_per_group(h3, Config) ->
    start_tls_server(Config, query_h3, [http3], [http3]).

end_per_group(_Group, Config) ->
    nhttpc_test_server:stop(?config(server_pid, Config)).

%%%-----------------------------------------------------------------------------
%%% CASES
%%%-----------------------------------------------------------------------------

query_oneshot(Config) ->
    Body = <<"q=oneshot">>,
    {ok, Resp} = nhttpc:request(query_request(Config, Body), ?config(client_opts, Config)),
    assert_echoed(Body, Resp).

query_pooled(Config) ->
    Body = <<"q=pooled">>,
    PoolOpts = (?config(client_opts, Config))#{
        host => <<"127.0.0.1">>,
        port => ?config(server_port, Config),
        size => 1
    },
    {ok, PoolPid} = nhttpc:start_link(?POOL, PoolOpts),
    true = unlink(PoolPid),
    Result = nhttpc:request(query_request(Config, Body), #{via => {pool, ?POOL}}),
    ok = nhttpc:stop(?POOL),
    {ok, Resp} = Result,
    assert_echoed(Body, Resp).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

-spec start_tls_server([{atom(), term()}], atom(), [atom()], [atom()]) ->
    [{atom(), term()}] | {skip, string()}.
start_tls_server(Config, Ref, ServerVersions, ClientVersions) ->
    case ?config(certfile, Config) of
        undefined ->
            {skip, "TLS certificates not available"};
        CertFile ->
            ServerOpts = #{
                versions => ServerVersions,
                tls => #{certfile => CertFile, keyfile => ?config(keyfile, Config)}
            },
            ClientOpts = #{versions => ClientVersions, tls => #{verify => verify_none}},
            start_server(Config, Ref, ServerOpts, https, ClientOpts)
    end.

-spec start_server([{atom(), term()}], atom(), map(), http | https, nhttpc:opts()) ->
    [{atom(), term()}].
start_server(Config, Ref, ServerOpts, Scheme, ClientOpts) ->
    {ok, ServerPid} = nhttp:start_link(Ref, ServerOpts#{
        port => 0, handler => nhttp_test_handler
    }),
    true = unlink(ServerPid),
    {ok, Port} = nhttp:get_port(ServerPid),
    [
        {server_pid, ServerPid},
        {server_port, Port},
        {scheme, Scheme},
        {client_opts, ClientOpts}
        | Config
    ].

-spec query_request([{atom(), term()}], binary()) -> nhttp_lib:request().
query_request(Config, Body) ->
    Url = iolist_to_binary([
        atom_to_binary(?config(scheme, Config)),
        <<"://127.0.0.1:">>,
        integer_to_binary(?config(server_port, Config)),
        <<"/check-method">>
    ]),
    {ok, Req} = nhttpc:url(query, Url),
    Req#{headers => [{<<"content-type">>, ?FORM}], body => Body}.

-spec assert_echoed(binary(), nhttp_lib:response()) -> ok.
assert_echoed(Body, Resp) ->
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Method: QUERY\nBody: ", Body/binary>>, maps:get(body, Resp, <<>>)).

-spec find_project_root(file:filename()) -> file:filename().
find_project_root(Dir) ->
    case filelib:is_file(filename:join(Dir, "rebar.config")) of
        true ->
            Dir;
        false ->
            case filename:dirname(Dir) of
                Dir -> ".";
                Parent -> find_project_root(Parent)
            end
    end.
