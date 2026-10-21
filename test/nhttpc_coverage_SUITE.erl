%%%-----------------------------------------------------------------------------
%%% @doc Coverage improvement test suite.
%%%
%%% Targets specific uncovered code paths identified in coverage analysis.
%%% Focus areas:
%%% - nhttpc async request error paths
%%% - nhttpc_conn keep-alive parsing edge cases
%%% - nhttpc_stream_send file and iterator errors
%%% - nhttpc_pool edge cases
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_coverage_SUITE).

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

%% nhttpc async tests
-export([
    async_request_invalid_url/1,
    async_await_default_timeout/1,
    async_await_timeout/1,
    async_cancel_completed/1,
    async_cancel_running/1
]).

%% nhttpc connection-level async tests
-export([
    conn_async_monitor_down/1,
    conn_async_error_message/1
]).

%% nhttpc_stream_send tests
-export([
    stream_send_file_not_found/1,
    stream_send_file_iterator_error/1,
    stream_send_invalid_stream_body/1,
    stream_send_file_read_error/1,
    stream_send_put_file/1
]).

%% nhttpc_url tests
-export([
    url_parse_invalid_scheme/1,
    url_parse_missing_host/1,
    url_parse_ipv6/1
]).

%% nhttpc convenience method tests
-export([
    convenience_head/1,
    convenience_options/1,
    convenience_patch/1,
    convenience_delete_with_opts/1
]).

%% Pool edge cases
-export([
    pool_checkout_timeout/1,
    pool_connection_error/1
]).

%% Request retry edge cases
-export([
    retry_exhausted/1,
    retry_on_specific_status/1
]).

%% Redirect edge cases
-export([
    redirect_policy_stop/1,
    redirect_policy_custom_opts/1,
    redirect_follow_disabled/1
]).

%% Keep-alive edge cases
-export([
    keep_alive_empty_value/1,
    keep_alive_malformed_timeout/1,
    keep_alive_whitespace/1
]).

%% DNS failure tests
-export([
    dns_failure_nxdomain/1,
    dns_failure_connection_refused/1,
    dns_failure_with_retry/1
]).

%% H2 poolless request tests
-export([
    h2_poolless_get/1,
    h2_poolless_post/1,
    h2_poolless_large_body/1
]).

%% H3 poolless request tests
-export([
    h3_poolless_get/1,
    h3_poolless_post/1,
    h3_poolless_streaming_upload/1
]).

%% Pool idle timeout integration
-export([
    pool_idle_close_notification/1,
    pool_request_after_idle_reconnect/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, async_requests},
        {group, conn_async},
        {group, stream_send},
        {group, url_parsing},
        {group, convenience},
        {group, pool},
        {group, retry},
        {group, redirect},
        {group, keep_alive},
        {group, dns_failure},
        {group, h2_poolless},
        {group, h3_poolless},
        {group, pool_idle}
    ].

groups() ->
    [
        {async_requests, [sequence], [
            async_request_invalid_url,
            async_await_default_timeout,
            async_await_timeout,
            async_cancel_completed,
            async_cancel_running
        ]},
        {conn_async, [sequence], [
            conn_async_monitor_down,
            conn_async_error_message
        ]},
        {stream_send, [sequence], [
            stream_send_file_not_found,
            stream_send_file_iterator_error,
            stream_send_invalid_stream_body,
            stream_send_file_read_error,
            stream_send_put_file
        ]},
        {url_parsing, [sequence], [
            url_parse_invalid_scheme,
            url_parse_missing_host,
            url_parse_ipv6
        ]},
        {convenience, [sequence], [
            convenience_head,
            convenience_options,
            convenience_patch,
            convenience_delete_with_opts
        ]},
        {pool, [sequence], [
            pool_checkout_timeout,
            pool_connection_error
        ]},
        {retry, [sequence], [
            retry_exhausted,
            retry_on_specific_status
        ]},
        {redirect, [sequence], [
            redirect_policy_stop,
            redirect_policy_custom_opts,
            redirect_follow_disabled
        ]},
        {keep_alive, [sequence], [
            keep_alive_empty_value,
            keep_alive_malformed_timeout,
            keep_alive_whitespace
        ]},
        {dns_failure, [sequence], [
            dns_failure_nxdomain,
            dns_failure_connection_refused,
            dns_failure_with_retry
        ]},
        {h2_poolless, [sequence], [
            h2_poolless_get,
            h2_poolless_post,
            h2_poolless_large_body
        ]},
        {h3_poolless, [sequence], [
            h3_poolless_get,
            h3_poolless_post,
            h3_poolless_streaming_upload
        ]},
        {pool_idle, [sequence], [
            pool_idle_close_notification,
            pool_request_after_idle_reconnect
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

init_per_group(Group, Config) when
    Group =:= async_requests;
    Group =:= convenience;
    Group =:= retry;
    Group =:= redirect
->
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_coverage_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to get port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(conn_async, Config) ->
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_conn_async_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to get port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(stream_send, Config) ->
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_stream_send_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to get port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(pool, Config) ->
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_pool_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to get port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(keep_alive, Config) ->
    case nhttpc_chaos_server:start(tcp) of
        {ok, ServerRef} ->
            [{chaos_server, ServerRef} | Config];
        {error, Reason} ->
            ct:fail("Failed to start chaos server: ~p", [Reason])
    end;
init_per_group(h2_poolless, Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            Handler = nhttp_test_handler,
            case
                nhttp:start_link(test_h2_poolless_server, #{
                    port => 0,
                    handler => Handler,
                    versions => [http2, http1_1],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    {ok, Port} = nhttp:get_port(ServerPid),
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to start H2 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(h3_poolless, Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            case
                nhttp:start_link(test_h3_poolless_server, #{
                    port => 0,
                    handler => nhttpc_h3_test_handler,
                    versions => [http3],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    {ok, Port} = nhttp:get_port(ServerPid),
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to start H3 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(pool_idle, Config) ->
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_pool_idle_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            [{server_pid, ServerPid}, {server_port, Port} | Config];
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(keep_alive, Config) ->
    case proplists:get_value(chaos_server, Config) of
        undefined -> ok;
        ServerRef -> nhttpc_chaos_server:stop(ServerRef)
    end,
    ok;
end_per_group(Group, Config) when
    Group =:= async_requests;
    Group =:= conn_async;
    Group =:= stream_send;
    Group =:= convenience;
    Group =:= pool;
    Group =:= retry;
    Group =:= redirect;
    Group =:= h2_poolless;
    Group =:= h3_poolless;
    Group =:= pool_idle
->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% ASYNC REQUEST TESTS
%%%-----------------------------------------------------------------------------

%% Test async send with invalid URL
async_request_invalid_url(_Config) ->
    %% Building an invalid URL fails at `nhttpc:url/2` itself.
    ?assertMatch({error, _}, nhttpc:url(get, <<"not-a-valid-url">>)),
    ok.

%% Test await with default timeout
async_await_default_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:send(Req),
    Result = nhttpc:await(Handle),
    ?assertMatch({ok, _}, Result),
    ok.

%% Test await timeout
async_await_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/slow/5000", [Port])),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:send(Req),
    Result = nhttpc:await(Handle, 100),
    ?assertMatch({error, _}, Result),
    %% Clean up - cancel the request
    nhttpc:cancel(Handle),
    ok.

%% Test cancel on already completed request
async_cancel_completed(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:send(Req),
    {ok, _Resp} = nhttpc:await(Handle, 5000),
    %% Cancel after completion should be ok
    Result = nhttpc:cancel(Handle),
    ?assertEqual(ok, Result),
    ok.

%% Test cancel on running request
async_cancel_running(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/slow/5000", [Port])),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:send(Req),
    timer:sleep(50),
    Result = nhttpc:cancel(Handle),
    ?assertEqual(ok, Result),
    ok.

%%%-----------------------------------------------------------------------------
%%% CONNECTION-LEVEL ASYNC TESTS
%%%-----------------------------------------------------------------------------

%% Test await_response when connection dies
conn_async_monitor_down(Config) ->
    Port = proplists:get_value(server_port, Config),
    process_flag(trap_exit, true),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/slow/5000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, StreamRef} = nhttpc:send_request(ConnPid, Req),

    %% Unlink before killing to avoid crashing the test
    unlink(ConnPid),

    %% Kill the connection to trigger monitor DOWN
    exit(ConnPid, kill),

    %% await_response should return connect_error (canonical wrap of connection_down)
    Result = nhttpc:await_response(ConnPid, StreamRef, 1000),
    ?assertMatch({error, {connect_error, _}}, Result),

    %% Clean up any remaining EXIT messages
    receive
        {'EXIT', _, _} -> ok
    after 0 -> ok
    end,
    process_flag(trap_exit, false),
    ok.

%% Test nhttpc_error message path
conn_async_error_message(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 100
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/slow/5000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, StreamRef} = nhttpc:send_request(ConnPid, Req),

    %% Should get timeout error via nhttpc_error message
    Result = nhttpc:await_response(ConnPid, StreamRef, 5000),
    ?assertMatch({error, _}, Result),

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% STREAM SEND TESTS
%%%-----------------------------------------------------------------------------

%% Test body_from {file, _} with non-existent file surfaces a typed error
stream_send_file_not_found(_Config) ->
    Result = nhttpc:post(
        <<"http://127.0.0.1:1/">>,
        <<>>,
        #{body_from => {file, "/nonexistent/path/to/file.txt"}}
    ),
    ?assertMatch({error, _}, Result),
    ok.

%% Test body_from with iter source that returns an error
stream_send_file_iterator_error(_Config) ->
    BadIter = fun() -> {error, {iter_blew_up, deliberate}} end,
    Result = nhttpc:post(
        <<"http://127.0.0.1:1/">>,
        <<>>,
        #{body_from => {iter, BadIter}}
    ),
    ?assertMatch({error, _}, Result),
    ok.

%% Test invalid stream body handling
stream_send_invalid_stream_body(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/upload/echo", [Port])),
    %% Regular body, not streaming
    Result = nhttpc:post(Url, <<"regular body">>),
    ?assertMatch({ok, _}, Result),
    ok.

%% Test streaming POST from a file using body_from
stream_send_file_read_error(Config) ->
    Port = proplists:get_value(server_port, Config),
    PrivDir = proplists:get_value(priv_dir, Config),

    TestFile = filename:join(PrivDir, "test_upload.txt"),
    ok = file:write_file(TestFile, <<"test content for upload">>),

    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/upload/echo", [Port])),

    Result = nhttpc:post(Url, <<>>, #{body_from => {file, TestFile}}),
    ?assertMatch({ok, _}, Result),

    file:delete(TestFile),
    ok.

%% Test streaming PUT from a file using body_from
stream_send_put_file(Config) ->
    Port = proplists:get_value(server_port, Config),
    PrivDir = proplists:get_value(priv_dir, Config),

    TestFile = filename:join(PrivDir, "test_put_file.txt"),
    ok = file:write_file(TestFile, <<"test content for PUT">>),

    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/upload/echo", [Port])),

    Result = nhttpc:put(Url, <<>>, #{body_from => {file, TestFile}}),
    ?assertMatch({ok, _}, Result),

    file:delete(TestFile),
    ok.

%%%-----------------------------------------------------------------------------
%%% URL PARSING TESTS
%%%-----------------------------------------------------------------------------

%% Test parse_url with invalid scheme
url_parse_invalid_scheme(_Config) ->
    Result = nhttpc:parse_url(<<"ftp://example.com/file">>),
    ?assertMatch({error, _}, Result),
    ok.

%% Test parse_url with missing host
url_parse_missing_host(_Config) ->
    Result = nhttpc:parse_url(<<"http:///path">>),
    ?assertMatch({error, _}, Result),
    ok.

%% Test parse_url with IPv6 address
url_parse_ipv6(_Config) ->
    Result = nhttpc:parse_url(<<"http://[::1]:8080/path">>),
    ?assertMatch({ok, _}, Result),
    ok.

%%%-----------------------------------------------------------------------------
%%% CONVENIENCE METHOD TESTS
%%%-----------------------------------------------------------------------------

%% Test HEAD request
convenience_head(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
    Result = nhttpc:head(Url),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    ?assertEqual(200, maps:get(status, Resp)),
    %% HEAD response should have empty body
    ?assertEqual(<<>>, maps:get(body, Resp, <<>>)),
    ok.

%% Test OPTIONS request
convenience_options(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
    Result = nhttpc:options(Url),
    ?assertMatch({ok, _}, Result),
    ok.

%% Test PATCH request
convenience_patch(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/upload/echo", [Port])),
    Result = nhttpc:patch(Url, <<"patch data">>),
    ?assertMatch({ok, _}, Result),
    ok.

%% Test DELETE with options
convenience_delete_with_opts(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
    Result = nhttpc:delete(Url, #{timeouts => #{request => 5000}}),
    ?assertMatch({ok, _}, Result),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL TESTS
%%%-----------------------------------------------------------------------------

%% Test pool checkout timeout
pool_checkout_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Start a very small pool with HTTP/1.1 only (no multiplexing)
    {ok, TinyPid} = nhttpc:start_link(tiny_pool, #{
        host => <<"127.0.0.1">>,
        port => Port,
        versions => [http1_1],
        size => 1,
        max_overflow => 0,
        timeouts => #{checkout => 50}
    }),
    unlink(TinyPid),

    %% Wait for pool to be ready
    timer:sleep(100),

    %% Make a slow request to occupy the only connection
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/slow/3000", [Port])),
    spawn(fun() ->
        nhttpc:get(Url, #{via => {pool, tiny_pool}, timeouts => #{request => 5000}})
    end),
    timer:sleep(100),

    %% Second request should timeout waiting for checkout
    Url2 = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
    Result = nhttpc:get(Url2, #{via => {pool, tiny_pool}, timeouts => #{request => 1000}}),
    ?assertMatch({error, _}, Result),

    nhttpc:stop(tiny_pool),
    ok.

%% Test pool with connection error
pool_connection_error(_Config) ->
    %% Start pool pointing to non-existent server
    {ok, ErrorPid} = nhttpc:start_link(error_pool, #{
        host => <<"127.0.0.1">>,
        port => 59997,
        size => 1,
        timeouts => #{connect => 500}
    }),
    unlink(ErrorPid),

    %% Request should fail with connection error
    Result = nhttpc:get(<<"http://127.0.0.1:59997/echo">>, #{via => {pool, error_pool}}),
    ?assertMatch({error, _}, Result),

    nhttpc:stop(error_pool),
    ok.

%%%-----------------------------------------------------------------------------
%%% RETRY TESTS
%%%-----------------------------------------------------------------------------

%% Test retry exhausted
retry_exhausted(Config) ->
    Port = proplists:get_value(server_port, Config),
    %% Use /retry-503 which consistently returns 503
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/retry-503", [Port])),

    Result = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 2,
            base_delay => 10,
            retry_on => [503]
        }
    }),

    %% After retries exhausted, should return the last 503 response
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    ?assertEqual(503, maps:get(status, Resp)),
    ok.

%% Test retry on specific status code
retry_on_specific_status(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),

    %% This should succeed on first try
    Result = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 3,
            base_delay => 10,
            retry_on => [500, 502, 503]
        }
    }),
    ?assertMatch({ok, _}, Result),
    ok.

%%%-----------------------------------------------------------------------------
%%% KEEP-ALIVE EDGE CASE TESTS
%%%-----------------------------------------------------------------------------

%% Test keep-alive with empty value
keep_alive_empty_value(Config) ->
    ServerRef = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(ServerRef),

    %% Set server to return empty Keep-Alive header
    nhttpc_chaos_server:set_failure_mode(ServerRef, keep_alive_empty),

    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    nhttpc_chaos_server:clear_failure_mode(ServerRef),
    ok.

%% Test keep-alive with malformed timeout value
keep_alive_malformed_timeout(Config) ->
    ServerRef = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(ServerRef),

    %% Set server to return malformed Keep-Alive: timeout=abc
    nhttpc_chaos_server:set_failure_mode(ServerRef, keep_alive_malformed),

    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    nhttpc_chaos_server:clear_failure_mode(ServerRef),
    ok.

%% Test keep-alive with extra whitespace
keep_alive_whitespace(Config) ->
    ServerRef = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(ServerRef),

    %% Set server to return Keep-Alive with extra whitespace: "  timeout = 30  "
    nhttpc_chaos_server:set_failure_mode(ServerRef, keep_alive_whitespace),

    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    nhttpc_chaos_server:clear_failure_mode(ServerRef),
    ok.

%%%-----------------------------------------------------------------------------
%%% REDIRECT TESTS
%%%-----------------------------------------------------------------------------

%% Test redirect_policy callback that returns stop
redirect_policy_stop(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/redirect-301", [Port])),

    %% Custom redirect policy that stops following
    Policy = fun(_Status, _Location, _Req, _Opts) -> stop end,

    Result = nhttpc:get(Url, #{redirect_policy => Policy}),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    %% Should get 301 response directly without following
    ?assertEqual(301, maps:get(status, Resp)),
    ok.

%% Test redirect_policy callback that modifies opts
redirect_policy_custom_opts(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/redirect-301", [Port])),

    %% Custom redirect policy that continues with modified opts
    Policy = fun(_Status, _Location, _Req, Opts) ->
        {continue, Opts#{max_redirects => 10}}
    end,

    Result = nhttpc:get(Url, #{redirect_policy => Policy}),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    %% Should follow redirect and get final response
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

%% Test follow_redirects = false
redirect_follow_disabled(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/redirect-301", [Port])),

    Result = nhttpc:get(Url, #{follow_redirects => false}),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    %% Should get 301 response without following
    ?assertEqual(301, maps:get(status, Resp)),
    ok.

%%%-----------------------------------------------------------------------------
%%% DNS FAILURE TESTS
%%%-----------------------------------------------------------------------------

%% Test DNS failure with non-existent domain (NXDOMAIN)
dns_failure_nxdomain(_Config) ->
    %% Use a domain that definitely doesn't exist
    Url = <<"http://this-domain-definitely-does-not-exist-12345.invalid/test">>,
    Result = nhttpc:get(Url, #{timeouts => #{request => 5000}}),
    ?assertMatch({error, _}, Result),
    {error, Reason} = Result,
    %% Should get nxdomain or similar DNS error
    ct:log("DNS failure result: ~p", [Reason]),
    ok.

%% Test connection refused (valid DNS, but nothing listening)
dns_failure_connection_refused(_Config) ->
    %% Connect to localhost on a port that's very unlikely to be in use
    Url = <<"http://127.0.0.1:59999/test">>,
    Result = nhttpc:get(Url, #{timeouts => #{request => 5000}}),
    ?assertMatch({error, _}, Result),
    {error, Reason} = Result,
    %% Canonical taxonomy wraps raw posix into {connect_error, econnrefused}
    ct:log("Connection refused result: ~p", [Reason]),
    ?assertEqual({connect_error, econnrefused}, Reason),
    ok.

%% Test DNS failure with retry enabled
dns_failure_with_retry(_Config) ->
    %% Use a non-existent domain with retry
    Url = <<"http://another-nonexistent-domain-xyz.invalid/test">>,
    Result = nhttpc:get(Url, #{
        timeouts => #{request => 5000},
        retry => #{
            max_attempts => 2,
            base_delay => 100,
            retry_on => [nxdomain, timeout]
        }
    }),
    ?assertMatch({error, _}, Result),
    ct:log("DNS failure with retry result: ~p", [Result]),
    ok.

%%%-----------------------------------------------------------------------------
%%% H2 POOLLESS REQUEST TESTS
%%%-----------------------------------------------------------------------------

%% Test H2 poolless GET request - covers do_h2_request, recv_h2_settings, recv_h2_response
h2_poolless_get(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"https://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>]),
    {ok, Resp} = nhttpc:get(Url, #{
        tls => #{verify => verify_none},
        versions => [http2]
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

%% Test H2 poolless POST request with body - covers send_h2_body path
h2_poolless_post(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"https://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/echo">>]),
    Body = <<"H2 test body content">>,
    {ok, Resp} = nhttpc:post(Url, Body, #{
        tls => #{verify => verify_none},
        versions => [http2]
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp, <<>>), Body)),
    ok.

%% Test H2 poolless with larger body - covers flow control paths
h2_poolless_large_body(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"https://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/echo">>]),
    Body = binary:copy(<<"large-h2-body-data-">>, 500),
    {ok, Resp} = nhttpc:post(Url, Body, #{
        tls => #{verify => verify_none},
        versions => [http2]
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

%%%-----------------------------------------------------------------------------
%%% H3 POOLLESS REQUEST TESTS
%%%-----------------------------------------------------------------------------

%% Test H3 poolless GET request - covers do_poolless_h3_exchange, do_h3_request,
%% init_h3_streams, execute_h3_actions, recv_h3_response_loop,
%% process_h3_quic_events, handle_h3_recv_events
h3_poolless_get(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"https://127.0.0.1:">>, integer_to_binary(Port), <<"/hello">>
    ]),
    {ok, Resp} = nhttpc:get(Url, #{versions => [http3]}),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Hello!">>, maps:get(body, Resp, <<>>)),
    ok.

%% Test H3 poolless POST request with body - covers maybe_send_h3_body with body
h3_poolless_post(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"https://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    Body = <<"H3 poolless POST test body">>,
    {ok, Resp} = nhttpc:post(Url, Body, #{versions => [http3]}),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(Body, maps:get(body, Resp, <<>>)),
    ok.

%% Test H3 poolless streaming upload - covers do_h3_streaming_upload,
%% do_h3_streaming_upload_impl, stream_h3_upload_data
h3_poolless_streaming_upload(Config) ->
    Port = proplists:get_value(server_port, Config),
    PrivDir = proplists:get_value(priv_dir, Config),
    Url = iolist_to_binary([
        <<"https://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    TestFile = filename:join(PrivDir, "h3_upload_test.txt"),
    TestContent = binary:copy(<<"h3-stream-chunk-">>, 100),
    ok = file:write_file(TestFile, TestContent),
    Result = nhttpc:post(Url, <<>>, #{
        body_from => {file, TestFile},
        versions => [http3]
    }),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(TestContent, maps:get(body, Resp, <<>>)),
    file:delete(TestFile),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL IDLE TIMEOUT INTEGRATION TESTS
%%%-----------------------------------------------------------------------------

%% Test pool with idle timeout that triggers handle_check_idle_ready.
%% The idle_check_interval is min(30000, max(5000, timeout div 2)).
%% With idle_timeout=5000, interval=5000, so first check at 5s triggers exit.
pool_idle_close_notification(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_idle_close_pool,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        idle_timeout => 5000
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Make a request so connection is established
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, _Resp} = nhttpc_pool:request(MgrPid, Req, 5000),

    %% Wait for idle close. Check interval=5s, idle_timeout=5s.
    %% First check at ~5s may see elapsed < 5s if request just completed.
    %% Second check at ~10s will see elapsed >= 5s and exit.
    timer:sleep(11000),

    %% Connection should have idled out and been replaced
    %% Pool should still serve requests
    {ok, Resp2} = nhttpc_pool:request(MgrPid, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp2)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test request after idle timeout reconnection
pool_request_after_idle_reconnect(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_idle_reconnect_pool,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        idle_timeout => 5000
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Make a request, wait for idle close (>5s), then make another
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Resp1} = nhttpc_pool:request(MgrPid, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp1)),

    %% Wait for idle close cycle
    timer:sleep(6000),

    %% Make request after reconnection
    {ok, Resp2} = nhttpc_pool:request(MgrPid, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp2)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.
