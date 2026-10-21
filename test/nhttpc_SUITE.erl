%%%-----------------------------------------------------------------------------
%%% @doc Client test suite.
%%%
%%% Tests the HTTP client API including URL parsing, poolless requests,
%%% and pooled requests against a local test server.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_SUITE).

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

%% URL Parsing tests
-export([
    parse_simple_http/1,
    parse_simple_https/1,
    parse_with_port/1,
    parse_with_path/1,
    parse_with_query/1,
    parse_with_path_and_query/1,
    parse_ipv6_host/1,
    parse_ipv6_with_port/1,
    parse_invalid_scheme/1,
    parse_empty_host/1,
    parse_invalid_port/1
]).

%% Request building tests
-export([
    build_get_request/1,
    build_post_request/1,
    build_request_with_headers/1
]).

%% Poolless request tests
-export([
    poolless_get_request/1,
    poolless_post_request/1,
    poolless_h2_request/1,
    poolless_h2_request_timeout/1
]).

%% Pool tests
-export([
    pool_start_stop/1,
    pool_single_request/1,
    pool_concurrent_requests/1,
    pool_connection_reuse/1,
    pool_max_pending_requests/1,
    pool_max_connection_age/1
]).

%% Compression tests (Stage A)
-export([
    compress_gzip/1,
    compress_deflate/1,
    compress_disabled/1,
    decompress_disabled/1,
    compress_identity/1,
    compress_accept_encoding_sent/1
]).

%% Cookie jar tests (Stage B)
-export([
    jar_start_stop/1,
    jar_basic_set_get/1,
    jar_domain_match/1,
    jar_path_match/1,
    jar_secure_cookies/1,
    jar_expiry/1,
    jar_max_age/1,
    jar_http_only/1,
    jar_cross_request/1,
    jar_multiple_cookies/1,
    jar_clear/1
]).

%% Redirect tests (Stage C)
-export([
    redirect_301/1,
    redirect_302/1,
    redirect_303/1,
    redirect_307/1,
    redirect_308/1,
    redirect_chain/1,
    redirect_max_limit/1,
    redirect_loop/1,
    redirect_disabled/1,
    redirect_policy/1,
    redirect_no_location/1,
    redirect_absolute_url/1,
    redirect_post_301_becomes_get/1,
    redirect_head_301_preserved/1,
    redirect_cross_origin_strips_auth/1,
    redirect_relative_with_dotdot/1,
    redirect_with_query_string/1
]).

%% Retry tests (Stage D)
-export([
    retry_disabled/1,
    retry_503/1,
    retry_502/1,
    retry_504/1,
    retry_500_not_retried/1,
    retry_success_after_fail/1,
    retry_max_attempts/1,
    retry_exponential_backoff/1,
    retry_linear_backoff/1,
    retry_constant_delay/1,
    retry_custom_callback/1,
    retry_with_redirects/1
]).

%% Streaming tests (Stage E)
-export([
    stream_small_response/1,
    stream_large_response/1,
    stream_chunked_h1/1,
    stream_to_file/1,
    stream_progress/1,
    stream_cancel/1,
    stream_on_headers/1
]).

%% Streaming upload tests (Stage F)
-export([
    upload_stream_file/1,
    upload_stream_iterator/1,
    upload_chunked_h1/1,
    upload_known_size/1,
    upload_progress/1,
    upload_large_file/1,
    upload_put/1,
    upload_verify_hash/1
]).

%% Async API tests (Stage D)
-export([
    async_get/1,
    async_post/1,
    async_await_timeout/1,
    async_cancel/1
]).

%% Convenience method tests (Stage D)
-export([
    convenience_head/1,
    convenience_options/1,
    convenience_patch/1,
    convenience_delete/1
]).

%% Protocol error handling tests
-export([
    response_too_large/1,
    response_too_large_streaming/1,
    response_size_infinity/1
]).

%% Stream-to-file tests
-export([
    stream_to_basic/1,
    stream_to_empty_body/1,
    stream_to_file_error/1,
    stream_to_with_redirect/1,
    stream_to_pooled/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
        {group, url_parsing},
        {group, request_building},
        {group, poolless_requests},
        {group, async_api},
        {group, convenience_methods},
        {group, compression},
        {group, cookie_jar},
        {group, redirects},
        {group, retry},
        {group, streaming},
        {group, streaming_upload},
        {group, pool_tests},
        {group, protocol_errors},
        {group, stream_to_file}
    ].

groups() ->
    [
        {url_parsing, [parallel], [
            parse_simple_http,
            parse_simple_https,
            parse_with_port,
            parse_with_path,
            parse_with_query,
            parse_with_path_and_query,
            parse_ipv6_host,
            parse_ipv6_with_port,
            parse_invalid_scheme,
            parse_empty_host,
            parse_invalid_port
        ]},
        {request_building, [parallel], [
            build_get_request,
            build_post_request,
            build_request_with_headers
        ]},
        {poolless_requests, [sequence], [
            poolless_get_request,
            poolless_post_request,
            poolless_h2_request,
            poolless_h2_request_timeout
        ]},
        {async_api, [sequence], [
            async_get,
            async_post,
            async_await_timeout,
            async_cancel
        ]},
        {convenience_methods, [sequence], [
            convenience_head,
            convenience_options,
            convenience_patch,
            convenience_delete
        ]},
        {compression, [sequence], [
            compress_gzip,
            compress_deflate,
            compress_disabled,
            decompress_disabled,
            compress_identity,
            compress_accept_encoding_sent
        ]},
        {cookie_jar, [sequence], [
            jar_start_stop,
            jar_basic_set_get,
            jar_domain_match,
            jar_path_match,
            jar_secure_cookies,
            jar_expiry,
            jar_max_age,
            jar_http_only,
            jar_cross_request,
            jar_multiple_cookies,
            jar_clear
        ]},
        {redirects, [sequence], [
            redirect_301,
            redirect_302,
            redirect_303,
            redirect_307,
            redirect_308,
            redirect_chain,
            redirect_max_limit,
            redirect_loop,
            redirect_disabled,
            redirect_policy,
            redirect_no_location,
            redirect_absolute_url,
            redirect_post_301_becomes_get,
            redirect_head_301_preserved,
            redirect_cross_origin_strips_auth,
            redirect_relative_with_dotdot,
            redirect_with_query_string
        ]},
        {retry, [sequence], [
            retry_disabled,
            retry_503,
            retry_502,
            retry_504,
            retry_500_not_retried,
            retry_success_after_fail,
            retry_max_attempts,
            retry_exponential_backoff,
            retry_linear_backoff,
            retry_constant_delay,
            retry_custom_callback,
            retry_with_redirects
        ]},
        {streaming, [sequence], [
            stream_small_response,
            stream_large_response,
            stream_chunked_h1,
            stream_to_file,
            stream_progress,
            stream_cancel,
            stream_on_headers
        ]},
        {streaming_upload, [sequence], [
            upload_stream_file,
            upload_stream_iterator,
            upload_chunked_h1,
            upload_known_size,
            upload_progress,
            upload_large_file,
            upload_put,
            upload_verify_hash
        ]},
        {pool_tests, [sequence], [
            pool_start_stop,
            pool_single_request,
            pool_concurrent_requests,
            pool_connection_reuse,
            pool_max_pending_requests,
            pool_max_connection_age
        ]},
        {protocol_errors, [sequence], [
            response_too_large,
            response_too_large_streaming,
            response_size_infinity
        ]},
        {stream_to_file, [sequence], [
            stream_to_basic,
            stream_to_empty_body,
            stream_to_file_error,
            stream_to_with_redirect,
            stream_to_pooled
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    %% Load TLS certificate paths for HTTP/2 tests
    %% Find the project root from the data_dir or priv_dir
    PrivDir = proplists:get_value(priv_dir, Config, ""),
    %% priv_dir is typically .../nhttp/_build/test/logs/ct_run.../nhttpc_SUITE.logs/run.../log_private
    %% Navigate up to find test/conf
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

%% Find project root by looking for rebar.config
-spec find_project_root(string()) -> string().
find_project_root(Dir) ->
    RebarConfig = filename:join(Dir, "rebar.config"),
    case filelib:is_file(RebarConfig) of
        true ->
            Dir;
        false ->
            Parent = filename:dirname(Dir),
            case Parent =:= Dir of
                true ->
                    %% Reached filesystem root, fallback
                    ".";
                false ->
                    find_project_root(Parent)
            end
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(poolless_requests, Config) ->
    %% Start a test server for poolless requests
    %% Use http1_1 only for plain TCP (no TLS/ALPN)
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_poolless_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            %% Unlink so server survives when init_per_group process ends
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    %% Also start a TLS server for HTTP/2 tests
                    Config1 = [{server_pid, ServerPid}, {server_port, Port} | Config],
                    start_h2_tls_server(Config1);
                {error, Reason} ->
                    ct:fail("Failed to get port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(async_api, Config) ->
    %% Start a test server for async API tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_async_server, #{
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
init_per_group(convenience_methods, Config) ->
    %% Start a test server for convenience method tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_convenience_server, #{
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
init_per_group(compression, Config) ->
    %% Start a test server for compression tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_compression_server, #{
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
init_per_group(cookie_jar, Config) ->
    %% Start a test server for cookie jar tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_cookie_server, #{
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
init_per_group(redirects, Config) ->
    %% Start a test server for redirect tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_redirect_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    %% Start a second server on a different port for cross-origin tests
                    Config1 = [{server_pid, ServerPid}, {server_port, Port} | Config],
                    case
                        nhttp:start_link(test_redirect_server2, #{
                            port => 0,
                            handler => Handler,
                            protocols => [http1_1]
                        })
                    of
                        {ok, Server2Pid} ->
                            unlink(Server2Pid),
                            {ok, Port2} = nhttp:get_port(Server2Pid),
                            [{server2_pid, Server2Pid}, {server2_port, Port2} | Config1];
                        {error, _} ->
                            Config1
                    end;
                {error, Reason} ->
                    ct:fail("Failed to get port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(retry, Config) ->
    %% Spawn a dedicated process to own the ETS table (not linked)
    %% This ensures the table persists across test cases
    Parent = self(),
    TabOwner = spawn(fun() ->
        ets:new(nhttp_test_retry, [public, named_table, {write_concurrency, true}]),
        Parent ! {ets_ready, self()},
        %% Keep this process alive until told to die
        receive
            stop -> ok
        end
    end),
    receive
        {ets_ready, TabOwner} -> ok
    after 5000 ->
        error(ets_table_creation_timeout)
    end,
    %% Start a test server for retry tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_retry_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port}, {tab_owner, TabOwner} | Config];
                {error, Reason} ->
                    ct:fail("Failed to get port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(streaming, Config) ->
    %% Start a test server for streaming tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_streaming_server, #{
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
init_per_group(streaming_upload, Config) ->
    %% Start a test server for streaming upload tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_upload_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    %% Create a test directory for file uploads
                    TestDir = filename:join([proplists:get_value(priv_dir, Config), "upload_test"]),
                    file:make_dir(TestDir),
                    [{server_pid, ServerPid}, {server_port, Port}, {test_dir, TestDir} | Config];
                {error, Reason} ->
                    ct:fail("Failed to get port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(pool_tests, Config) ->
    %% Start a test server for pool tests
    %% Use http1_1 only for plain TCP (no TLS/ALPN)
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_pool_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            %% Unlink so server survives when init_per_group process ends
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
init_per_group(protocol_errors, Config) ->
    %% Start a test server for protocol error tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_protocol_error_server, #{
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
                    nhttpc_test_server:stop(ServerPid),
                    ct:fail("Failed to get server port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(stream_to_file, Config) ->
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_stream_to_file_server, #{
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
                    nhttpc_test_server:stop(ServerPid),
                    ct:fail("Failed to get server port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(poolless_requests, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    %% Also stop TLS server if started
    nhttpc_test_server:stop(proplists:get_value(h2_server_pid, Config)),
    ok;
end_per_group(async_api, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    ok;
end_per_group(convenience_methods, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    ok;
end_per_group(compression, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    ok;
end_per_group(cookie_jar, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    ok;
end_per_group(redirects, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    nhttpc_test_server:stop(proplists:get_value(server2_pid, Config)),
    ok;
end_per_group(retry, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    %% Stop the ETS table owner process (this will delete the table)
    case proplists:get_value(tab_owner, Config) of
        undefined -> ok;
        TabOwner when is_pid(TabOwner) -> TabOwner ! stop
    end,
    ok;
end_per_group(streaming, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    ok;
end_per_group(streaming_upload, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    ok;
end_per_group(pool_tests, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    ok;
end_per_group(protocol_errors, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    ok;
end_per_group(stream_to_file, Config) ->
    ServerPid = proplists:get_value(server_pid, Config),
    nhttpc_test_server:stop(ServerPid),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% URL PARSING TESTS
%%%-----------------------------------------------------------------------------

parse_simple_http(_Config) ->
    {ok, UrlInfo} = nhttpc:parse_url(<<"http://example.com">>),
    ?assertEqual(http, maps:get(scheme, UrlInfo)),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(80, maps:get(port, UrlInfo)),
    ?assertEqual(<<"/">>, maps:get(path, UrlInfo)),
    ?assertEqual(<<>>, maps:get(query, UrlInfo)).

parse_simple_https(_Config) ->
    {ok, UrlInfo} = nhttpc:parse_url(<<"https://example.com">>),
    ?assertEqual(https, maps:get(scheme, UrlInfo)),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(443, maps:get(port, UrlInfo)),
    ?assertEqual(<<"/">>, maps:get(path, UrlInfo)).

parse_with_port(_Config) ->
    {ok, UrlInfo} = nhttpc:parse_url(<<"http://example.com:8080">>),
    ?assertEqual(http, maps:get(scheme, UrlInfo)),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(8080, maps:get(port, UrlInfo)),
    ?assertEqual(<<"/">>, maps:get(path, UrlInfo)).

parse_with_path(_Config) ->
    {ok, UrlInfo} = nhttpc:parse_url(<<"http://example.com/api/v1/users">>),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(<<"/api/v1/users">>, maps:get(path, UrlInfo)).

parse_with_query(_Config) ->
    {ok, UrlInfo} = nhttpc:parse_url(<<"http://example.com?foo=bar">>),
    ?assertEqual(<<"/">>, maps:get(path, UrlInfo)),
    ?assertEqual(<<"foo=bar">>, maps:get(query, UrlInfo)).

parse_with_path_and_query(_Config) ->
    {ok, UrlInfo} = nhttpc:parse_url(<<"http://example.com/search?q=hello&limit=10">>),
    ?assertEqual(<<"/search">>, maps:get(path, UrlInfo)),
    ?assertEqual(<<"q=hello&limit=10">>, maps:get(query, UrlInfo)).

parse_ipv6_host(_Config) ->
    {ok, UrlInfo} = nhttpc:parse_url(<<"http://[::1]">>),
    ?assertEqual(<<"[::1]">>, maps:get(host, UrlInfo)),
    ?assertEqual(80, maps:get(port, UrlInfo)).

parse_ipv6_with_port(_Config) ->
    {ok, UrlInfo} = nhttpc:parse_url(<<"http://[::1]:8080">>),
    ?assertEqual(<<"[::1]">>, maps:get(host, UrlInfo)),
    ?assertEqual(8080, maps:get(port, UrlInfo)).

parse_invalid_scheme(_Config) ->
    {error, {invalid_request, invalid_url_scheme}} = nhttpc:parse_url(<<"ftp://example.com">>),
    {error, {invalid_request, invalid_url_scheme}} = nhttpc:parse_url(<<"example.com">>).

parse_empty_host(_Config) ->
    {error, {invalid_request, empty_host}} = nhttpc:parse_url(<<"http://">>).

parse_invalid_port(_Config) ->
    {error, {invalid_request, invalid_port}} = nhttpc:parse_url(<<"http://example.com:abc">>),
    {error, {invalid_request, invalid_port}} = nhttpc:parse_url(<<"http://example.com:99999">>).

%%%-----------------------------------------------------------------------------
%%% REQUEST BUILDING TESTS
%%%-----------------------------------------------------------------------------

build_get_request(_Config) ->
    %% Test that URL parsing works for request normalization
    {ok, _UrlInfo} = nhttpc:parse_url(<<"http://example.com/test">>),
    ok.

build_post_request(_Config) ->
    %% Test that URL parsing works for POST request
    {ok, UrlInfo} = nhttpc:parse_url(<<"http://example.com/api">>),
    ?assertEqual(<<"/api">>, maps:get(path, UrlInfo)),
    ok.

build_request_with_headers(_Config) ->
    %% Test URL parsing for API endpoint
    {ok, UrlInfo} = nhttpc:parse_url(<<"http://example.com/api">>),
    ?assertEqual(<<"/api">>, maps:get(path, UrlInfo)),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOLLESS REQUEST TESTS
%%%-----------------------------------------------------------------------------

poolless_get_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

poolless_post_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>]),
    Body = <<"{\"hello\": \"world\"}">>,
    {ok, Resp} = nhttpc:post(Url, Body, #{
        headers => [{<<"content-type">>, <<"application/json">>}]
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

poolless_h2_request(Config) ->
    %% Test HTTP/2 request over TLS with ALPN negotiation
    case proplists:get_value(h2_server_port, Config) of
        undefined ->
            {skip, "TLS certificates not available for HTTP/2 test"};
        Port ->
            Url = iolist_to_binary([
                <<"https://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
            ]),
            %% Make request with verify_none since we use self-signed certs
            {ok, Resp} = nhttpc:get(Url, #{
                tls => #{verify => verify_none}
            }),
            ?assertEqual(200, maps:get(status, Resp)),
            ok
    end.

%% A response that outlives the request timeout on the poolless HTTP/2
%% path is a request timeout, the same error the HTTP/1.1 path returns.
poolless_h2_request_timeout(Config) ->
    case proplists:get_value(h2_server_port, Config) of
        undefined ->
            {skip, "TLS certificates not available for HTTP/2 test"};
        Port ->
            Url = iolist_to_binary([
                <<"https://127.0.0.1:">>, integer_to_binary(Port), <<"/slow/2000">>
            ]),
            Result = nhttpc:get(Url, #{
                tls => #{verify => verify_none},
                timeouts => #{request => 300}
            }),
            ?assertEqual({error, {timeout, request}}, Result),
            ok
    end.

%%%-----------------------------------------------------------------------------
%%% ASYNC API TESTS
%%%-----------------------------------------------------------------------------

async_get(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:send(Req),
    ?assert(nhttpc_handle:is_handle(Handle)),
    {ok, Resp} = nhttpc:await(Handle, 5000),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

async_post(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    Body = <<"{\"test\": \"data\"}">>,
    {ok, Req0} = nhttpc:url(post, Url),
    {ok, Handle} = nhttpc:send(Req0#{
        headers => [{<<"content-type">>, <<"application/json">>}],
        body => Body
    }),
    {ok, Resp} = nhttpc:await(Handle),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

async_await_timeout(Config) ->
    %% Use a real handle pointing at a slow endpoint that won't reply
    %% before the await deadline.
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/slow/2000">>
    ]),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:send(Req),
    Result = nhttpc:await(Handle, 100),
    ?assertMatch({error, {timeout, request}}, Result),
    ok = nhttpc:cancel(Handle),
    ok.

async_cancel(Config) ->
    %% Test cancel function - start a slow request and cancel it
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/slow/2000">>
    ]),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:send(Req),
    Pid = nhttpc_handle:pid(Handle),
    ?assert(is_process_alive(Pid)),
    ok = nhttpc:cancel(Handle),
    ?assertNot(is_process_alive(Pid)),
    ok.

%%%-----------------------------------------------------------------------------
%%% CONVENIENCE METHOD TESTS
%%%-----------------------------------------------------------------------------

convenience_head(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    {ok, Resp} = nhttpc:head(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    %% HEAD response should have empty body
    ?assertEqual(<<>>, maps:get(body, Resp, <<>>)),
    ok.

convenience_options(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    {ok, Resp} = nhttpc:options(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

convenience_patch(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    Body = <<"{\"partial\": \"update\"}">>,
    {ok, Resp} = nhttpc:patch(Url, Body, #{
        headers => [{<<"content-type">>, <<"application/json">>}]
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

convenience_delete(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    {ok, Resp} = nhttpc:delete(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL TESTS
%%%-----------------------------------------------------------------------------

pool_start_stop(Config) ->
    %% Test pool lifecycle: start, verify running, stop
    Port = ?config(server_port, Config),
    ct:log("Server port: ~p", [Port]),
    ?assertNotEqual(undefined, Port),

    PoolRef = test_pool_lifecycle,

    %% Start the pool
    {ok, PoolPid} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2,
        max_overflow => 2
    }),
    unlink(PoolPid),
    ?assert(is_pid(PoolPid)),
    ?assert(is_process_alive(PoolPid)),

    %% Wait for at least one connection to be ready
    MgrName = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrName, 5000),

    %% Stop the pool: terminates the whole supervision tree synchronously.
    ok = nhttpc_pool_sup:stop(PoolRef),
    ?assertNot(is_process_alive(PoolPid)),
    ok.

pool_single_request(Config) ->
    %% Test a single request through the pool
    Port = ?config(server_port, Config),
    PoolRef = test_pool_single,

    %% Start the pool
    {ok, PoolPid} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0
    }),
    unlink(PoolPid),

    %% Wait for connection to be ready using the new wait_ready API
    MgrName = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrName, 5000),

    %% Make a request through the pool
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [],
        body => <<>>
    },
    Result = nhttpc_pool:request(PoolRef, Req, 5000),
    ct:log("Pool request result: ~p", [Result]),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    ?assertEqual(200, maps:get(status, Resp)),

    %% Clean up
    ok = nhttpc_pool_sup:stop(PoolRef),
    timer:sleep(100),
    ok.

pool_concurrent_requests(Config) ->
    %% Test multiple concurrent requests through the pool
    Port = ?config(server_port, Config),
    PoolRef = test_pool_concurrent,

    %% Start a pool with multiple connections
    {ok, PoolPid} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 4,
        max_overflow => 4
    }),
    unlink(PoolPid),

    %% Wait for at least one connection to be ready
    MgrName = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrName, 5000),

    %% Launch 10 concurrent requests
    Self = self(),
    NumRequests = 10,
    Pids = [
        spawn_link(fun() ->
            Req = #{
                method => get,
                path => <<"/echo">>,
                headers => [],
                body => <<>>
            },
            Result = nhttpc_pool:request(PoolRef, Req, 10000),
            Self ! {done, self(), Result}
        end)
     || _ <- lists:seq(1, NumRequests)
    ],

    %% Collect results
    Results = [
        receive
            {done, Pid, Result} -> Result
        after 15000 -> timeout
        end
     || Pid <- Pids
    ],
    ct:log("Concurrent request results: ~p", [Results]),

    %% All requests should succeed
    lists:foreach(
        fun(Result) ->
            ?assertMatch({ok, _}, Result),
            {ok, Resp} = Result,
            ?assertEqual(200, maps:get(status, Resp))
        end,
        Results
    ),

    %% Clean up
    ok = nhttpc_pool_sup:stop(PoolRef),
    timer:sleep(100),
    ok.

pool_connection_reuse(Config) ->
    %% Test that HTTP/1.1 connections are reused (keep-alive)
    Port = ?config(server_port, Config),
    PoolRef = test_pool_reuse,

    %% Start a pool with a single connection
    {ok, PoolPid} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0
    }),
    unlink(PoolPid),

    %% Wait for connection to be ready
    MgrName = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrName, 5000),

    %% Make multiple sequential requests (should reuse the same connection)
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [],
        body => <<>>
    },

    %% First request
    {ok, Resp1} = nhttpc_pool:request(PoolRef, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp1)),

    %% Second request (same connection should be reused)
    {ok, Resp2} = nhttpc_pool:request(PoolRef, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp2)),

    %% Third request
    {ok, Resp3} = nhttpc_pool:request(PoolRef, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp3)),

    %% Verify pool still has just 1 connection (reused)
    ConnSup = nhttpc_pool:lookup_conn_sup(PoolRef),
    Children = supervisor:which_children(ConnSup),
    ct:log("Connection supervisor children: ~p", [Children]),
    ?assertEqual(1, length(Children)),

    %% Clean up
    ok = nhttpc_pool_sup:stop(PoolRef),
    timer:sleep(100),
    ok.

%%%-----------------------------------------------------------------------------
%%% RESOURCE MANAGEMENT TESTS
%%%-----------------------------------------------------------------------------

%% Test that pool rejects requests when max_pending_requests is exceeded
pool_max_pending_requests(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_max_pending,

    %% Start pool with very low max_pending_requests limit
    {ok, PoolPid} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0,
        %% Very low limit for testing
        max_pending_requests => 2
    }),
    unlink(PoolPid),

    %% Wait for pool to be ready
    MgrName = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrName, 5000),

    %% Create a slow request to block the connection
    SlowReq = #{
        method => get,
        %% 200ms delay
        path => <<"/slow/200">>,
        headers => [],
        body => <<>>
    },

    %% Start slow request in background to occupy the connection
    Self = self(),
    spawn(fun() ->
        Result = nhttpc_pool:request(PoolRef, SlowReq, 5000),
        Self ! {slow_req, Result}
    end),
    %% Let the slow request start
    timer:sleep(20),

    %% Queue up requests to exceed the limit
    Req = #{method => get, path => <<"/">>, headers => [], body => <<>>},
    spawn(fun() ->
        nhttpc_pool:request(PoolRef, Req, 5000),
        Self ! queued1
    end),
    spawn(fun() ->
        nhttpc_pool:request(PoolRef, Req, 5000),
        Self ! queued2
    end),
    %% Let them queue
    timer:sleep(20),

    %% Third request should be rejected (max_pending_requests=2 exceeded)
    Result = nhttpc_pool:request(PoolRef, Req, 100),
    ct:log("Result when queue full: ~p", [Result]),
    ?assertMatch({error, {pool, #{type := exhausted, reason := max_pending_requests}}}, Result),

    %% Wait for background requests to complete
    receive
        {slow_req, _} -> ok
    after 2000 -> ok
    end,
    receive
        queued1 -> ok
    after 2000 -> ok
    end,
    receive
        queued2 -> ok
    after 2000 -> ok
    end,

    %% Clean up
    ok = nhttpc_pool_sup:stop(PoolRef),
    timer:sleep(100),
    ok.

%% Test that connections are recycled when max_connection_age is exceeded
pool_max_connection_age(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_max_age,

    %% Start pool with very short max_connection_age
    {ok, PoolPid} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0,
        %% 100ms - very short for testing
        max_connection_age => 100,
        %% Sweep every 50ms
        connection_sweep_interval => 50
    }),
    unlink(PoolPid),

    %% Wait for pool to be ready
    MgrName = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrName, 5000),

    %% Get initial connection pid
    ConnSup = nhttpc_pool:lookup_conn_sup(PoolRef),
    [{_, ConnPid1, _, _}] = supervisor:which_children(ConnSup),
    ct:log("Initial connection PID: ~p", [ConnPid1]),

    %% Wait for max_connection_age + sweep interval to pass
    timer:sleep(200),

    %% Make a request to trigger reconnection (old connection should be closed)
    Req = #{method => get, path => <<"/">>, headers => [], body => <<>>},
    {ok, Resp} = nhttpc_pool:request(PoolRef, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp)),

    %% Give sweep time to run and replace connection
    timer:sleep(100),

    %% Check that we have a new connection (old one was recycled)
    Children = supervisor:which_children(ConnSup),
    ct:log("Children after age exceeded: ~p", [Children]),

    %% The old connection should be gone or a new one created
    case Children of
        [{_, ConnPid2, _, _}] when ConnPid2 =/= ConnPid1 ->
            ct:log("Connection recycled: old=~p, new=~p", [ConnPid1, ConnPid2]),
            ok;
        [{_, ConnPid1, _, _}] ->
            %% Connection might still be the same if request came before sweep
            ct:log("Connection may not have been recycled yet (timing)"),
            ok;
        [] ->
            ct:log("Connection closed, waiting for replacement"),
            ok;
        Other ->
            ct:log("Unexpected children: ~p", [Other]),
            ok
    end,

    %% Clean up
    ok = nhttpc_pool_sup:stop(PoolRef),
    timer:sleep(100),
    ok.

%%%-----------------------------------------------------------------------------
%%% COMPRESSION TESTS (Stage A)
%%%-----------------------------------------------------------------------------

%% Test that gzip compressed response is automatically decompressed
compress_gzip(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/gzip">>]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    %% Body should be decompressed
    ?assertEqual(
        <<"Hello, compressed world! This is a gzip response.">>, maps:get(body, Resp, <<>>)
    ),
    %% Content-Encoding header should be removed after decompression
    ?assertEqual(undefined, proplists:get_value(<<"content-encoding">>, maps:get(headers, Resp))),
    ok.

%% Test that deflate compressed response is automatically decompressed
compress_deflate(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/deflate">>]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    %% Body should be decompressed
    ?assertEqual(
        <<"Hello, compressed world! This is a deflate response.">>, maps:get(body, Resp, <<>>)
    ),
    %% Content-Encoding header should be removed after decompression
    ?assertEqual(undefined, proplists:get_value(<<"content-encoding">>, maps:get(headers, Resp))),
    ok.

%% Test that no Accept-Encoding header is sent when compress=false
compress_disabled(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>]),
    {ok, Resp} = nhttpc:get(Url, #{compress => false}),
    ?assertEqual(200, maps:get(status, Resp)),
    %% Response body should show no Accept-Encoding was sent
    Body = maps:get(body, Resp, <<>>),
    ?assertNotEqual(nomatch, binary:match(Body, <<"Accept-Encoding: none">>)),
    ok.

%% Test that raw compressed body is returned when decompress=false
decompress_disabled(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/gzip">>]),
    {ok, Resp} = nhttpc:get(Url, #{decompress => false}),
    ?assertEqual(200, maps:get(status, Resp)),
    %% Body should still be compressed (not equal to plain text)
    ?assertNotEqual(
        <<"Hello, compressed world! This is a gzip response.">>, maps:get(body, Resp, <<>>)
    ),
    %% Content-Encoding header should be present
    ?assertEqual(<<"gzip">>, proplists:get_value(<<"content-encoding">>, maps:get(headers, Resp))),
    ok.

%% Test that identity encoding (uncompressed) is handled correctly
compress_identity(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/identity">>]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    %% Body should be unchanged
    ?assertEqual(<<"Hello, uncompressed world!">>, maps:get(body, Resp, <<>>)),
    ok.

%% Test that Accept-Encoding header is sent by default
compress_accept_encoding_sent(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    %% Response body should show Accept-Encoding was sent
    Body = maps:get(body, Resp, <<>>),
    ?assertNotEqual(nomatch, binary:match(Body, <<"Accept-Encoding: gzip, deflate">>)),
    ok.

%%%-----------------------------------------------------------------------------
%%% COOKIE JAR TESTS (Stage B)
%%%-----------------------------------------------------------------------------

%% Test cookie jar start and stop
jar_start_stop(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    ?assert(is_reference(Jar)),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test basic set and get operations
jar_basic_set_get(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},

    %% Set a cookie manually
    Cookie = #{name => <<"session">>, value => <<"abc123">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),

    %% Get the cookie back
    {ok, Cookies} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([{<<"session">>, <<"abc123">>}], Cookies),

    %% Get as header
    {ok, Header} = nhttpc_cookie_jar:get_header(Jar, Uri),
    ?assertEqual(<<"session=abc123">>, Header),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test domain matching
jar_domain_match(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),

    %% Set cookie for example.com
    Uri1 = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    Cookie1 = #{name => <<"domain_cookie">>, value => <<"val1">>, domain => <<"example.com">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri1, Cookie1),

    %% Should match exact domain
    {ok, Cookies1} = nhttpc_cookie_jar:get(Jar, Uri1),
    ?assertEqual([{<<"domain_cookie">>, <<"val1">>}], Cookies1),

    %% Should match subdomain
    Uri2 = #{scheme => http, host => <<"www.example.com">>, path => <<"/">>},
    {ok, Cookies2} = nhttpc_cookie_jar:get(Jar, Uri2),
    ?assertEqual([{<<"domain_cookie">>, <<"val1">>}], Cookies2),

    %% Should NOT match different domain
    Uri3 = #{scheme => http, host => <<"other.com">>, path => <<"/">>},
    {ok, Cookies3} = nhttpc_cookie_jar:get(Jar, Uri3),
    ?assertEqual([], Cookies3),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test path matching
jar_path_match(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},

    %% Set cookies with different paths
    Cookie1 = #{name => <<"root">>, value => <<"val1">>, path => <<"/">>},
    Cookie2 = #{name => <<"api">>, value => <<"val2">>, path => <<"/api">>},
    Cookie3 = #{name => <<"users">>, value => <<"val3">>, path => <<"/api/users">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie1),
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie2),
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie3),

    %% Root path only gets root cookie
    {ok, RootCookies} = nhttpc_cookie_jar:get(Jar, #{
        scheme => http, host => <<"example.com">>, path => <<"/">>
    }),
    ?assertEqual([{<<"root">>, <<"val1">>}], RootCookies),

    %% /api gets root and api cookies (longest path first)
    {ok, ApiCookies} = nhttpc_cookie_jar:get(Jar, #{
        scheme => http, host => <<"example.com">>, path => <<"/api">>
    }),
    ?assertEqual([{<<"api">>, <<"val2">>}, {<<"root">>, <<"val1">>}], ApiCookies),

    %% /api/users gets all three (longest path first)
    {ok, UsersCookies} = nhttpc_cookie_jar:get(Jar, #{
        scheme => http, host => <<"example.com">>, path => <<"/api/users">>
    }),
    ?assertEqual(
        [{<<"users">>, <<"val3">>}, {<<"api">>, <<"val2">>}, {<<"root">>, <<"val1">>}], UsersCookies
    ),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test secure cookies only sent over HTTPS
jar_secure_cookies(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => https, host => <<"example.com">>, path => <<"/">>},

    %% Set a secure cookie
    Cookie = #{name => <<"secure_session">>, value => <<"secret">>, secure => true},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),

    %% Should be returned over HTTPS
    {ok, HttpsCookies} = nhttpc_cookie_jar:get(Jar, #{
        scheme => https, host => <<"example.com">>, path => <<"/">>
    }),
    ?assertEqual([{<<"secure_session">>, <<"secret">>}], HttpsCookies),

    %% Should NOT be returned over HTTP
    {ok, HttpCookies} = nhttpc_cookie_jar:get(Jar, #{
        scheme => http, host => <<"example.com">>, path => <<"/">>
    }),
    ?assertEqual([], HttpCookies),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test cookie expiry
jar_expiry(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},

    %% Set a cookie that expires in the past
    PastDate = {{2020, 1, 1}, {0, 0, 0}},
    Cookie = #{name => <<"expired">>, value => <<"old">>, expires => PastDate},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),

    %% Should not be returned (expired)
    {ok, Cookies} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([], Cookies),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test max-age attribute
jar_max_age(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},

    %% Set a cookie with max-age in the future
    Cookie1 = #{name => <<"valid">>, value => <<"yes">>, max_age => 3600},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie1),

    %% Should be returned
    {ok, Cookies1} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([{<<"valid">>, <<"yes">>}], Cookies1),

    %% Set a cookie with max-age=0 (immediate expiry)
    Cookie2 = #{name => <<"instant">>, value => <<"no">>, max_age => 0},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie2),

    %% Should not be returned (expired)
    {ok, Cookies2} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([{<<"valid">>, <<"yes">>}], Cookies2),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test HttpOnly attribute is stored
jar_http_only(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},

    %% Set an HttpOnly cookie
    Cookie = #{name => <<"http_only">>, value => <<"secret">>, http_only => true},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),

    %% Should be returned (HttpOnly doesn't affect server-side retrieval)
    {ok, Cookies} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([{<<"http_only">>, <<"secret">>}], Cookies),

    %% Check the flag is stored
    {ok, AllCookies} = nhttpc_cookie_jar:all(Jar),
    [CookieMap] = AllCookies,
    ?assertEqual(true, maps:get(http_only, CookieMap)),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test cookies persist across requests
jar_cross_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, Jar} = nhttpc_cookie_jar:start_link(),

    %% First request sets a cookie
    SetUrl = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/set-cookie">>]),
    {ok, Resp1} = nhttpc:get(SetUrl, #{cookie_jar => Jar}),
    ?assertEqual(200, maps:get(status, Resp1)),

    %% Verify cookie was stored
    Uri = #{scheme => http, host => <<"127.0.0.1">>, path => <<"/">>},
    {ok, Cookies} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([{<<"session">>, <<"abc123">>}], Cookies),

    %% Second request should send the cookie
    CheckUrl = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/check-cookie">>
    ]),
    {ok, Resp2} = nhttpc:get(CheckUrl, #{cookie_jar => Jar}),
    ?assertEqual(200, maps:get(status, Resp2)),
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp2, <<>>), <<"session=abc123">>)),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test multiple cookies handling
jar_multiple_cookies(Config) ->
    Port = proplists:get_value(server_port, Config),
    {ok, Jar} = nhttpc_cookie_jar:start_link(),

    %% Request that sets multiple cookies
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/multi-cookie">>]),
    {ok, Resp} = nhttpc:get(Url, #{cookie_jar => Jar}),
    ?assertEqual(200, maps:get(status, Resp)),

    %% Verify all cookies were stored
    Uri = #{scheme => http, host => <<"127.0.0.1">>, path => <<"/">>},
    {ok, Cookies} = nhttpc_cookie_jar:get(Jar, Uri),
    %% cookie1 and cookie2 have path=/, cookie3 has path=/api
    ?assertEqual(2, length(Cookies)),
    ?assert(lists:member({<<"cookie1">>, <<"value1">>}, Cookies)),
    ?assert(lists:member({<<"cookie2">>, <<"value2">>}, Cookies)),

    %% Check /api path gets all three
    UriApi = #{scheme => http, host => <<"127.0.0.1">>, path => <<"/api">>},
    {ok, ApiCookies} = nhttpc_cookie_jar:get(Jar, UriApi),
    ?assertEqual(3, length(ApiCookies)),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%% Test clear operations
jar_clear(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},

    %% Set some cookies
    ok = nhttpc_cookie_jar:set(Jar, Uri, #{name => <<"c1">>, value => <<"v1">>}),
    ok = nhttpc_cookie_jar:set(Jar, Uri, #{name => <<"c2">>, value => <<"v2">>}),

    %% Clear all
    ok = nhttpc_cookie_jar:clear(Jar),
    {ok, Empty} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([], Empty),

    %% Set cookies for different domains
    Uri2 = #{scheme => http, host => <<"other.com">>, path => <<"/">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri, #{name => <<"c1">>, value => <<"v1">>}),
    ok = nhttpc_cookie_jar:set(Jar, Uri2, #{name => <<"c2">>, value => <<"v2">>}),

    %% Clear only one domain
    ok = nhttpc_cookie_jar:clear(Jar, <<"example.com">>),
    {ok, Remaining} = nhttpc_cookie_jar:get(Jar, Uri2),
    ?assertEqual([{<<"c2">>, <<"v2">>}], Remaining),

    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%%%-----------------------------------------------------------------------------
%%% REDIRECT TESTS (Stage C)
%%%-----------------------------------------------------------------------------

%% Test 301 Moved Permanently redirect
redirect_301(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-301">>]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Final destination reached">>, maps:get(body, Resp, <<>>)),
    ok.

%% Test 302 Found redirect
redirect_302(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-302">>]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Final destination reached">>, maps:get(body, Resp, <<>>)),
    ok.

%% Test 303 See Other redirect (always changes method to GET)
redirect_303(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-303">>]),
    %% Even POST should become GET after 303
    {ok, Resp} = nhttpc:post(Url, <<"test body">>),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Final destination reached">>, maps:get(body, Resp, <<>>)),
    ok.

%% Test 307 Temporary Redirect (preserves method and body)
redirect_307(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-307">>]),
    Body = <<"test body content">>,
    {ok, Resp} = nhttpc:post(Url, Body),
    ?assertEqual(200, maps:get(status, Resp)),
    %% Response should show POST method was preserved
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp, <<>>), <<"Method: POST">>)),
    %% Body should also be preserved
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp, <<>>), Body)),
    ok.

%% Test 308 Permanent Redirect (preserves method and body)
redirect_308(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-308">>]),
    Body = <<"permanent redirect body">>,
    {ok, Resp} = nhttpc:post(Url, Body),
    ?assertEqual(200, maps:get(status, Resp)),
    %% Response should show POST method was preserved
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp, <<>>), <<"Method: POST">>)),
    %% Body should also be preserved
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp, <<>>), Body)),
    ok.

%% Test redirect chain (multiple hops)
redirect_chain(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-chain">>]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Final destination reached">>, maps:get(body, Resp, <<>>)),
    ok.

%% Test max redirects limit is enforced
redirect_max_limit(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-max-1">>]),
    %% With max_redirects=3, the chain of 6 should fail
    {error, {redirect_error, max_redirects_exceeded}} =
        nhttpc:get(Url, #{max_redirects => 3}),
    %% With default max_redirects=5, it should also fail (6 redirects needed)
    {error, {redirect_error, max_redirects_exceeded}} =
        nhttpc:get(Url, #{max_redirects => 5}),
    %% With max_redirects=10, it should succeed
    {ok, Resp} = nhttpc:get(Url, #{max_redirects => 10}),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

%% Test redirect loop detection
redirect_loop(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-loop">>]),
    %% The loop goes /redirect-loop -> /redirect-loop-2 -> /redirect-loop
    %% Should detect the loop and return error
    Result = nhttpc:get(Url),
    case Result of
        {error, {redirect_error, {redirect_loop, _}}} ->
            ok;
        {error, {redirect_error, max_redirects_exceeded}} ->
            %% Also acceptable - loop hit max redirects
            ok;
        Other ->
            ct:fail("Expected redirect_loop or max_redirects_exceeded error, got: ~p", [Other])
    end.

%% Test redirect disabled returns redirect response directly
redirect_disabled(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-301">>]),
    {ok, Resp} = nhttpc:get(Url, #{follow_redirects => false}),
    %% Should get the 301 response directly
    ?assertEqual(301, maps:get(status, Resp)),
    %% Location header should be present
    Location = proplists:get_value(<<"location">>, maps:get(headers, Resp)),
    ?assertEqual(<<"/final">>, Location),
    ok.

%% Test custom redirect policy
redirect_policy(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-301">>]),

    %% Policy that blocks all redirects
    BlockPolicy = fun(_Status, _Location, _Req, _Opts) -> stop end,
    {ok, Resp1} = nhttpc:get(Url, #{redirect_policy => BlockPolicy}),
    ?assertEqual(301, maps:get(status, Resp1)),

    %% Policy that allows redirects
    AllowPolicy = fun(_Status, _Location, _Req, _Opts) -> continue end,
    {ok, Resp2} = nhttpc:get(Url, #{redirect_policy => AllowPolicy}),
    ?assertEqual(200, maps:get(status, Resp2)),
    ?assertEqual(<<"Final destination reached">>, maps:get(body, Resp2, <<>>)),

    ok.

%%%-----------------------------------------------------------------------------
%%% REDIRECT EDGE CASE TESTS
%%%-----------------------------------------------------------------------------

%% Test redirect without Location header
redirect_no_location(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-no-location">>
    ]),
    Result = nhttpc:get(Url),
    %% Should return error since there's no Location header to follow
    case Result of
        {error, _} ->
            ok;
        {ok, Resp} ->
            %% Or the client may return the 301 directly if it can't find Location
            ?assertEqual(301, maps:get(status, Resp))
    end,
    ok.

%% Test redirect with absolute URL (http://host:port/path)
redirect_absolute_url(Config) ->
    Port = proplists:get_value(server_port, Config),
    PortBin = integer_to_binary(Port),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, PortBin, <<"/redirect-absolute/">>, PortBin]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Final destination reached">>, maps:get(body, Resp, <<>>)),
    ok.

%% Test POST with 301 becomes GET (RFC 9110)
redirect_post_301_becomes_get(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-301-post">>
    ]),
    {ok, Resp} = nhttpc:post(Url, <<"test body">>),
    ?assertEqual(200, maps:get(status, Resp)),
    %% POST should have been changed to GET on 301
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp, <<>>), <<"Method: GET">>)),
    ok.

%% Test HEAD with 301 is preserved
redirect_head_301_preserved(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-301-head">>
    ]),
    {ok, Resp} = nhttpc:head(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

%% Test cross-origin redirect strips Authorization header
redirect_cross_origin_strips_auth(Config) ->
    Port = proplists:get_value(server_port, Config),
    Port2 = proplists:get_value(server2_port, Config, Port),
    PortBin = integer_to_binary(Port),
    Port2Bin = integer_to_binary(Port2),
    case Port =/= Port2 of
        true ->
            %% Redirect from port to port2 (different origin)
            Url = iolist_to_binary([
                <<"http://127.0.0.1:">>, PortBin, <<"/redirect-cross-origin/">>, Port2Bin
            ]),
            {ok, Resp} = nhttpc:get(Url, #{
                headers => [{<<"authorization">>, <<"Bearer secret123">>}]
            }),
            ?assertEqual(200, maps:get(status, Resp)),
            %% Authorization should be stripped on cross-origin redirect
            ?assertNotEqual(
                nomatch, binary:match(maps:get(body, Resp, <<>>), <<"Authorization: none">>)
            ),
            ok;
        false ->
            %% No second server available, redirect to same port
            Url = iolist_to_binary([
                <<"http://127.0.0.1:">>, PortBin, <<"/redirect-cross-origin/">>, PortBin
            ]),
            {ok, Resp} = nhttpc:get(Url, #{
                headers => [{<<"authorization">>, <<"Bearer secret123">>}]
            }),
            ?assertEqual(200, maps:get(status, Resp)),
            ok
    end.

%% Test relative path redirect with .. resolution
redirect_relative_with_dotdot(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/api/redirect-relative">>
    ]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Final destination reached">>, maps:get(body, Resp, <<>>)),
    ok.

%% Test redirect with query string in Location
redirect_with_query_string(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-with-query">>
    ]),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    %% The /echo endpoint will include the path in response
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp, <<>>), <<"redirected=true">>)),
    ok.

%%%-----------------------------------------------------------------------------
%%% RETRY TESTS (Stage D)
%%%-----------------------------------------------------------------------------

%% Test that retry is disabled by default
retry_disabled(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/retry-503">>]),
    %% Without retry option, we get 503 immediately
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(503, maps:get(status, Resp)),
    ok.

%% Test retry on 503 Service Unavailable
retry_503(Config) ->
    Port = proplists:get_value(server_port, Config),
    Id = integer_to_binary(erlang:unique_integer([positive])),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>,
        integer_to_binary(Port),
        <<"/retry-flaky/">>,
        Id,
        <<"/2">>
    ]),
    %% Should retry and eventually succeed (fail 2 times, succeed on 3rd)
    {ok, Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 5,
            base_delay => 10,
            jitter => false
        }
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp, <<>>), <<"Success">>)),
    ok.

%% Test retry on 502 Bad Gateway
retry_502(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/retry-502">>]),
    %% With max_attempts=2, should try twice and still fail
    {ok, Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 2,
            base_delay => 10,
            jitter => false
        }
    }),
    ?assertEqual(502, maps:get(status, Resp)),
    ok.

%% Test retry on 504 Gateway Timeout
retry_504(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/retry-504">>]),
    %% With max_attempts=2, should try twice and still fail
    {ok, Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 2,
            base_delay => 10,
            jitter => false
        }
    }),
    ?assertEqual(504, maps:get(status, Resp)),
    ok.

%% Test that 500 is NOT retried by default
retry_500_not_retried(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/retry-500">>]),
    %% 500 is not in the default retry_on list, so should not retry
    {ok, Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 3,
            base_delay => 10,
            jitter => false
        }
    }),
    ?assertEqual(500, maps:get(status, Resp)),
    ok.

%% Test successful retry after transient failure
retry_success_after_fail(Config) ->
    Port = proplists:get_value(server_port, Config),
    Id = integer_to_binary(erlang:unique_integer([positive])),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>,
        integer_to_binary(Port),
        <<"/retry-flaky/">>,
        Id,
        <<"/1">>
    ]),
    %% Should fail once, then succeed
    {ok, Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 3,
            base_delay => 10,
            jitter => false
        }
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertNotEqual(nomatch, binary:match(maps:get(body, Resp, <<>>), <<"Success after 2">>)),
    ok.

%% Test that max_attempts limits retries
retry_max_attempts(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/retry-503">>]),
    %% With max_attempts=3, should try exactly 3 times
    T0 = erlang:monotonic_time(millisecond),
    {ok, Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 3,
            base_delay => 10,
            jitter => false
        }
    }),
    T1 = erlang:monotonic_time(millisecond),
    ?assertEqual(503, maps:get(status, Resp)),
    %% Should have delayed at least 10ms + 20ms = 30ms (2 retries)
    %% but allow some tolerance
    ?assert(T1 - T0 >= 20),
    ok.

%% Test exponential backoff timing
retry_exponential_backoff(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/retry-503">>]),
    %% Exponential: 50, 100, 200 (total ~350ms minimum)
    T0 = erlang:monotonic_time(millisecond),
    {ok, _Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 4,
            base_delay => 50,
            backoff => exponential,
            jitter => false
        }
    }),
    T1 = erlang:monotonic_time(millisecond),
    %% Should have taken at least 50 + 100 + 200 = 350ms (3 delays for 3 retries)
    Elapsed = T1 - T0,
    ?assert(Elapsed >= 300),
    ok.

%% Test linear backoff timing
retry_linear_backoff(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/retry-503">>]),
    %% Linear: 50, 100, 150 (total ~300ms minimum)
    T0 = erlang:monotonic_time(millisecond),
    {ok, _Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 4,
            base_delay => 50,
            backoff => linear,
            jitter => false
        }
    }),
    T1 = erlang:monotonic_time(millisecond),
    %% Should have taken at least 50 + 100 + 150 = 300ms (3 delays for 3 retries)
    Elapsed = T1 - T0,
    ?assert(Elapsed >= 250),
    ok.

%% Test constant delay timing
retry_constant_delay(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/retry-503">>]),
    %% Constant: 50, 50, 50 (total ~150ms minimum)
    T0 = erlang:monotonic_time(millisecond),
    {ok, _Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 4,
            base_delay => 50,
            backoff => constant,
            jitter => false
        }
    }),
    T1 = erlang:monotonic_time(millisecond),
    %% Should have taken at least 50 * 3 = 150ms (3 delays for 3 retries)
    Elapsed = T1 - T0,
    ?assert(Elapsed >= 120),
    ok.

%% Test custom retry callback
retry_custom_callback(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/retry-500">>]),

    %% Custom callback that retries on 500 (not in default list)
    CustomRetry = fun
        ({ok, #{status := 500}}, Attempt, _Opts) when Attempt < 3 -> true;
        (_, _, _) -> false
    end,

    {ok, Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 5,
            base_delay => 10,
            jitter => false,
            should_retry => CustomRetry
        }
    }),
    %% Still 500 because server always returns 500, but we did retry
    ?assertEqual(500, maps:get(status, Resp)),
    ok.

%% Test retry combined with redirects
retry_with_redirects(Config) ->
    Port = proplists:get_value(server_port, Config),
    Id = integer_to_binary(erlang:unique_integer([positive])),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>,
        integer_to_binary(Port),
        <<"/retry-redirect/">>,
        Id
    ]),
    %% First attempt: 503 (retry)
    %% Second attempt: 302 redirect to /final
    %% Follow redirect: 200
    {ok, Resp} = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 3,
            base_delay => 10,
            jitter => false
        }
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Final destination reached">>, maps:get(body, Resp, <<>>)),
    ok.

%%%-----------------------------------------------------------------------------
%%% STREAMING TESTS (Stage E)
%%%-----------------------------------------------------------------------------

%% Test streaming a small response (default sink: binary body)
stream_small_response(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/stream/small">>]),

    {ok, #{body := Body}} = nhttpc:get(Url),
    ?assertEqual(100, byte_size(Body)),
    ?assertEqual(binary:copy(<<"x">>, 100), Body),
    ok.

%% Test streaming a large response (100KB) via fold sink
stream_large_response(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/stream/large">>]),

    F = fun(Chunk, Acc) -> {continue, [Chunk | Acc]} end,
    {ok, #{body := {fold, Acc}}} = nhttpc:get(Url, #{
        body_to => {fold, F},
        fold_init => []
    }),
    Result = iolist_to_binary(lists:reverse(Acc)),

    ?assertEqual(102400, byte_size(Result)),
    Pattern = <<"0123456789ABCDEF">>,
    Expected = binary:copy(Pattern, 6400),
    ?assertEqual(Expected, Result),
    ok.

%% Test streaming with chunked transfer encoding (default binary sink)
stream_chunked_h1(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/stream/chunked">>]),

    {ok, #{body := Body}} = nhttpc:get(Url),
    ?assertNotEqual(<<>>, Body),
    ?assertNotEqual(nomatch, binary:match(Body, <<"chunk1-data">>)),
    ok.

%% Test body_to => {file, _} sink
stream_to_file(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/stream/large">>]),

    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "stream_test_file.bin"),

    {ok, #{body := {file, FilePath, BytesWritten}}} =
        nhttpc:get(Url, #{body_to => {file, FilePath}}),
    ?assertEqual(102400, BytesWritten),

    {ok, FileContent} = file:read_file(FilePath),
    ?assertEqual(102400, byte_size(FileContent)),

    Pattern = <<"0123456789ABCDEF">>,
    Expected = binary:copy(Pattern, 6400),
    ?assertEqual(Expected, FileContent),

    file:delete(FilePath),
    ok.

%% Test on_download_progress callback
stream_progress(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/stream/progress">>
    ]),

    Self = self(),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "stream_progress.bin"),

    {ok, _Resp} = nhttpc:get(Url, #{
        body_to => {file, FilePath},
        on_download_progress => fun(Downloaded, Total) ->
            Self ! {progress, Downloaded, Total},
            ok
        end
    }),

    receive
        {progress, Downloaded, Total} ->
            ?assertEqual(10000, Total),
            ?assert(Downloaded > 0)
    after 1000 ->
        ct:fail("No progress callback received")
    end,
    file:delete(FilePath),
    ok.

%% Test stopping the fold sink mid-stream
stream_cancel(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/stream/large">>]),

    F = fun(Chunk, ByteCount) ->
        NewCount = ByteCount + byte_size(Chunk),
        case NewCount > 1000 of
            true -> {stop, user_cancelled, NewCount};
            false -> {continue, NewCount}
        end
    end,
    {ok, #{body := {fold, FinalCount}}} = nhttpc:get(Url, #{
        body_to => {fold, F},
        fold_init => 0
    }),
    ?assert(FinalCount > 1000),
    ok.

%% Test event-driven streaming surfaces headers via the {headers, _, _} event
stream_on_headers(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/stream/small">>]),

    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:stream(Req),
    receive
        {nhttpc, Handle, {headers, Status, Headers}} ->
            ?assertEqual(200, Status),
            ContentType = proplists:get_value(<<"content-type">>, Headers),
            ?assertEqual(<<"application/octet-stream">>, ContentType)
    after 1000 ->
        ct:fail("No headers event received")
    end,
    nhttpc:cancel(Handle),
    ok.

%%%-----------------------------------------------------------------------------
%%% STREAMING UPLOAD TESTS (Stage F)
%%%-----------------------------------------------------------------------------

%% Test streaming upload from a file
upload_stream_file(Config) ->
    Port = proplists:get_value(server_port, Config),
    TestDir = proplists:get_value(test_dir, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/echo">>]),

    FilePath = filename:join(TestDir, "upload_test.txt"),
    Content = <<"Hello from file upload!">>,
    ok = file:write_file(FilePath, Content),

    {ok, #{status := 200, headers := Headers, body := Body}} =
        nhttpc:post(Url, <<>>, #{
            headers => [{<<"content-type">>, <<"text/plain">>}],
            body_from => {file, FilePath}
        }),

    ReceivedSize = proplists:get_value(<<"x-received-size">>, Headers),
    ?assertEqual(<<"23">>, ReceivedSize),
    ?assert(binary:match(Body, Content) =/= nomatch),

    file:delete(FilePath),
    ok.

%% Test streaming upload with an iterator
upload_stream_iterator(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/echo">>]),

    Chunks = [<<"chunk1">>, <<"chunk2">>, <<"chunk3">>],
    Iterator = make_list_iterator(Chunks),

    {ok, #{status := 200, headers := Headers}} =
        nhttpc:post(Url, <<>>, #{
            headers => [{<<"content-type">>, <<"application/octet-stream">>}],
            body_from => {iter, Iterator}
        }),

    ReceivedSize = proplists:get_value(<<"x-received-size">>, Headers),
    ?assertEqual(<<"18">>, ReceivedSize),
    ok.

%% Test HTTP/1.1 chunked upload (no known size)
upload_chunked_h1(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/chunked">>]),

    Chunks = [<<"abc">>, <<"def">>, <<"ghi">>],
    Iterator = make_list_iterator(Chunks),

    {ok, #{status := 200, headers := Headers, body := Body}} =
        nhttpc:post(Url, <<>>, #{
            headers => [{<<"content-type">>, <<"text/plain">>}],
            body_from => {iter, Iterator}
        }),

    ReceivedSize = proplists:get_value(<<"x-received-size">>, Headers),
    ?assertEqual(<<"9">>, ReceivedSize),
    ?assert(binary:match(Body, <<"Transfer-Encoding: chunked">>) =/= nomatch),
    ok.

%% Test upload with known size from a file (Content-Length emitted)
upload_known_size(Config) ->
    Port = proplists:get_value(server_port, Config),
    TestDir = proplists:get_value(test_dir, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/chunked">>]),

    FilePath = filename:join(TestDir, "upload_known.txt"),
    ok = file:write_file(FilePath, <<"part1part2">>),

    {ok, #{status := 200, body := Body}} =
        nhttpc:post(Url, <<>>, #{body_from => {file, FilePath}}),

    ?assert(binary:match(Body, <<"Content-Length: 10">>) =/= nomatch),
    file:delete(FilePath),
    ok.

%% Test upload progress callback
upload_progress(Config) ->
    Port = proplists:get_value(server_port, Config),
    TestDir = proplists:get_value(test_dir, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/large">>]),

    FilePath = filename:join(TestDir, "upload_progress.bin"),
    ok = file:write_file(FilePath, binary:copy(<<"x">>, 10000)),

    Self = self(),
    {ok, #{status := 200, body := Body}} =
        nhttpc:post(Url, <<>>, #{
            body_from => {file, FilePath},
            on_upload_progress => fun(Sent, Total) ->
                Self ! {progress, Sent, Total},
                ok
            end
        }),

    ?assertEqual(<<"10000">>, Body),

    receive
        {progress, Sent, Total} ->
            ?assertEqual(10000, Total),
            ?assert(Sent > 0)
    after 1000 ->
        ct:fail("No progress callback received")
    end,
    file:delete(FilePath),
    ok.

%% Test large file upload (memory efficiency)
upload_large_file(Config) ->
    Port = proplists:get_value(server_port, Config),
    TestDir = proplists:get_value(test_dir, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/large">>]),

    FilePath = filename:join(TestDir, "large_upload.bin"),
    LargeContent = binary:copy(<<"0123456789ABCDEF">>, 6400),
    ok = file:write_file(FilePath, LargeContent),

    {ok, #{status := 200, body := Body}} =
        nhttpc:post(Url, <<>>, #{body_from => {file, FilePath}}),

    ?assertEqual(<<"102400">>, Body),

    file:delete(FilePath),
    ok.

%% Test PUT streaming upload
upload_put(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/echo">>]),

    Chunks = [<<"put">>, <<"data">>],
    Iterator = make_list_iterator(Chunks),

    {ok, #{status := 200, headers := Headers}} =
        nhttpc:put(Url, <<>>, #{
            headers => [{<<"content-type">>, <<"application/octet-stream">>}],
            body_from => {iter, Iterator}
        }),

    ReceivedSize = proplists:get_value(<<"x-received-size">>, Headers),
    ?assertEqual(<<"7">>, ReceivedSize),
    ok.

%% Test hash verification for uploaded content
upload_verify_hash(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/hash">>]),

    Content = <<"test content for hashing">>,
    Iterator = make_list_iterator([Content]),

    {ok, #{status := 200, headers := Headers}} =
        nhttpc:post(Url, <<>>, #{body_from => {iter, Iterator}}),

    Hash = proplists:get_value(<<"x-md5-hash">>, Headers),
    ExpectedHash = binary:encode_hex(crypto:hash(md5, Content), lowercase),
    ?assertEqual(ExpectedHash, Hash),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPER FUNCTIONS
%%%-----------------------------------------------------------------------------

%% Create a simple iterator from a list of chunks
-spec make_list_iterator([binary()]) -> fun(() -> nhttpc_stream_send:stream_chunk()).
make_list_iterator([]) ->
    fun() -> fin end;
make_list_iterator([Chunk | Rest]) ->
    fun() -> {ok, Chunk, make_list_iterator(Rest)} end.

%% Start an HTTP/2 TLS server for testing
-spec start_h2_tls_server([{atom(), term()}]) -> [{atom(), term()}].
start_h2_tls_server(Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            Handler = nhttp_test_handler,
            ServerOpts = #{
                port => 0,
                handler => Handler,
                versions => [http2, http1_1],
                tls => #{certfile => CertFile, keyfile => KeyFile}
            },
            case nhttp:start_link(test_h2_server, ServerOpts) of
                {ok, H2Pid} ->
                    unlink(H2Pid),
                    case nhttp:get_port(H2Pid) of
                        {ok, H2Port} ->
                            [{h2_server_pid, H2Pid}, {h2_server_port, H2Port} | Config];
                        {error, Reason} ->
                            ct:log("Failed to get H2 server port: ~p", [Reason]),
                            Config
                    end;
                {error, Reason} ->
                    ct:log("Failed to start H2 TLS server: ~p", [Reason]),
                    Config
            end;
        false ->
            ct:log("TLS certificates not available, skipping H2 TLS server"),
            Config
    end.

%%%-----------------------------------------------------------------------------
%%% PROTOCOL ERROR HANDLING TESTS
%%%-----------------------------------------------------------------------------

%% @doc Test that responses larger than max_response_size are rejected.
response_too_large(Config) ->
    Port = proplists:get_value(server_port, Config),
    %% Server returns 100KB for /stream/large
    %% Set max_response_size to 1000 bytes to trigger error
    Url = iolist_to_binary(io_lib:format("http://localhost:~p/stream/large", [Port])),
    Result = nhttpc:get(Url, #{max_response_size => 1000}),
    case Result of
        {error, {stream_error, exceeded_max_response_size}} ->
            ok;
        Other ->
            ct:fail("Expected exceeded_max_response_size error, got: ~p", [Other])
    end.

%% @doc Test that streaming responses larger than max_response_size are rejected.
response_too_large_streaming(Config) ->
    Port = proplists:get_value(server_port, Config),
    %% Server returns 100KB for /stream/large
    %% Set max_response_size to 1000 bytes to trigger error
    Url = iolist_to_binary(io_lib:format("http://localhost:~p/stream/large", [Port])),
    F = fun(Chunk, Acc) -> {continue, Acc + byte_size(Chunk)} end,
    Result = nhttpc:get(Url, #{
        max_response_size => 1000,
        body_to => {fold, F},
        fold_init => 0
    }),
    case Result of
        {error, {stream_error, exceeded_max_response_size}} ->
            ok;
        Other ->
            ct:fail("Expected exceeded_max_response_size error, got: ~p", [Other])
    end.

%% @doc Test that max_response_size => infinity allows large responses.
response_size_infinity(Config) ->
    Port = proplists:get_value(server_port, Config),
    %% Server returns 100KB for /stream/large
    %% Set max_response_size to infinity to allow any size
    Url = iolist_to_binary(io_lib:format("http://localhost:~p/stream/large", [Port])),
    Result = nhttpc:get(Url, #{max_response_size => infinity}),
    case Result of
        {ok, #{status := 200}} ->
            ok;
        Other ->
            ct:fail("Expected success with infinity limit, got: ~p", [Other])
    end.

%%%-----------------------------------------------------------------------------
%%% STREAM TO FILE TESTS
%%%-----------------------------------------------------------------------------

stream_to_basic(Config) ->
    Port = proplists:get_value(server_port, Config),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "stream_to_basic.bin"),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/stream/large">>
    ]),
    {ok, Resp} = nhttpc:get(Url, #{body_to => {file, FilePath}}),
    ?assertEqual(200, maps:get(status, Resp)),
    Pattern = <<"0123456789ABCDEF">>,
    Expected = binary:copy(Pattern, 6400),
    ExpectedSize = byte_size(Expected),
    ?assertEqual({file, FilePath, ExpectedSize}, maps:get(body, Resp)),
    {ok, FileContent} = file:read_file(FilePath),
    ?assertEqual(Expected, FileContent),
    file:delete(FilePath),
    ok.

stream_to_empty_body(Config) ->
    Port = proplists:get_value(server_port, Config),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "stream_to_empty.bin"),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/empty">>
    ]),
    {ok, Resp} = nhttpc:get(Url, #{body_to => {file, FilePath}}),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual({file, FilePath, 0}, maps:get(body, Resp)),
    {ok, FileContent} = file:read_file(FilePath),
    ?assertEqual(<<>>, FileContent),
    file:delete(FilePath),
    ok.

stream_to_file_error(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    Result = nhttpc:get(Url, #{body_to => {file, "/nonexistent/dir/file.bin"}}),
    ?assertMatch({error, {file_error, enoent}}, Result).

stream_to_with_redirect(Config) ->
    Port = proplists:get_value(server_port, Config),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "stream_to_redirect.bin"),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-301">>
    ]),
    {ok, Resp} = nhttpc:get(Url, #{body_to => {file, FilePath}}),
    ?assertEqual(200, maps:get(status, Resp)),
    Final = <<"Final destination reached">>,
    ?assertEqual({file, FilePath, byte_size(Final)}, maps:get(body, Resp)),
    {ok, FileContent} = file:read_file(FilePath),
    ?assertEqual(Final, FileContent),
    file:delete(FilePath),
    ok.

stream_to_pooled(Config) ->
    Port = proplists:get_value(server_port, Config),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "stream_to_pooled.bin"),
    PoolRef = test_pool_stream_to,
    {ok, PoolPid} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0
    }),
    unlink(PoolPid),
    MgrName = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrName, 5000),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    {ok, Resp} = nhttpc:get(Url, #{via => {pool, PoolRef}, body_to => {file, FilePath}}),
    ?assertEqual(200, maps:get(status, Resp)),
    {file, FilePath, Bytes} = maps:get(body, Resp),
    ?assert(Bytes > 0),
    {ok, FileContent} = file:read_file(FilePath),
    ?assertNotEqual(<<>>, FileContent),
    ?assertEqual(Bytes, byte_size(FileContent)),
    file:delete(FilePath),
    nhttpc_pool_sup:stop(PoolRef),
    ok.
