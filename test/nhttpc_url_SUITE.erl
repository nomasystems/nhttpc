%%%-----------------------------------------------------------------------------
%%% @doc URL parsing and request canonicalisation tests.
%%%
%%% Covers `nhttpc_url:parse_url/1`, `nhttpc_url:to_request/1`,
%%% `nhttpc_url:normalize_method/1`, and the `nhttpc:url/2` request
%%% builder added in plan 06.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_url_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% EXPORTS
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

-export([
    url_parse_http/1,
    url_parse_https/1,
    url_parse_ws/1,
    url_parse_wss/1,
    url_parse_with_port/1,
    url_parse_with_path/1,
    url_parse_with_query/1,
    url_parse_with_fragment/1,
    url_parse_invalid_scheme/1,
    url_parse_no_host/1,
    url_parse_ipv6_default_port/1,
    url_parse_ipv6_explicit_port/1,
    url_parse_ipv6_invalid_port/1,
    url_parse_ipv6_missing_bracket/1,
    url_parse_invalid_authority/1,
    url_parse_query_before_slash/1,
    url_parse_ws_default_port/1,
    url_parse_wss_default_port/1
]).

-export([
    to_request_canonical/1,
    to_request_partial_path_only/1,
    to_request_invalid/1,
    to_request_ipv6_authority/1,
    to_request_ipv6_authority_with_port/1,
    to_request_ipv6_invalid_port/1,
    to_request_ipv6_unterminated/1,
    to_request_ws_scheme/1,
    to_request_wss_scheme/1,
    to_request_unknown_scheme/1,
    to_request_authority_extra_colons/1,
    to_request_authority_bad_port/1,
    to_request_query_only_path/1
]).

-export([
    normalize_method_atoms/1,
    normalize_method_binaries/1,
    normalize_method_connect/1,
    normalize_method_options/1,
    normalize_method_trace/1,
    normalize_method_patch/1,
    normalize_method_query/1,
    normalize_method_custom/1
]).

-export([
    url_info_scheme/1,
    url_info_host/1,
    url_info_port/1,
    url_info_path/1
]).

-export([
    url_2_parses_https_correctly/1,
    url_2_parses_http_correctly/1,
    url_2_parses_ws_correctly/1,
    url_2_parses_wss_correctly/1,
    url_2_includes_query_in_path/1,
    url_2_keeps_non_default_port_in_authority/1,
    url_2_rejects_invalid_url/1,
    canonical_request_passes_through/1,
    convenience_get_builds_canonical_request/1,
    request_missing_scheme_returns_invalid_request/1,
    request_ws_scheme_returns_invalid_request/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
        {group, url_parsing},
        {group, to_request},
        {group, method_normalization},
        {group, url_info},
        {group, plan_06}
    ].

groups() ->
    [
        {url_parsing, [sequence], [
            url_parse_http,
            url_parse_https,
            url_parse_ws,
            url_parse_wss,
            url_parse_with_port,
            url_parse_with_path,
            url_parse_with_query,
            url_parse_with_fragment,
            url_parse_invalid_scheme,
            url_parse_no_host,
            url_parse_ipv6_default_port,
            url_parse_ipv6_explicit_port,
            url_parse_ipv6_invalid_port,
            url_parse_ipv6_missing_bracket,
            url_parse_invalid_authority,
            url_parse_query_before_slash,
            url_parse_ws_default_port,
            url_parse_wss_default_port
        ]},
        {to_request, [sequence], [
            to_request_canonical,
            to_request_partial_path_only,
            to_request_invalid,
            to_request_ipv6_authority,
            to_request_ipv6_authority_with_port,
            to_request_ipv6_invalid_port,
            to_request_ipv6_unterminated,
            to_request_ws_scheme,
            to_request_wss_scheme,
            to_request_unknown_scheme,
            to_request_authority_extra_colons,
            to_request_authority_bad_port,
            to_request_query_only_path
        ]},
        {method_normalization, [sequence], [
            normalize_method_atoms,
            normalize_method_binaries,
            normalize_method_connect,
            normalize_method_options,
            normalize_method_trace,
            normalize_method_patch,
            normalize_method_query,
            normalize_method_custom
        ]},
        {url_info, [sequence], [
            url_info_scheme,
            url_info_host,
            url_info_port,
            url_info_path
        ]},
        {plan_06, [sequence], [
            url_2_parses_https_correctly,
            url_2_parses_http_correctly,
            url_2_parses_ws_correctly,
            url_2_parses_wss_correctly,
            url_2_includes_query_in_path,
            url_2_keeps_non_default_port_in_authority,
            url_2_rejects_invalid_url,
            canonical_request_passes_through,
            convenience_get_builds_canonical_request,
            request_missing_scheme_returns_invalid_request,
            request_ws_scheme_returns_invalid_request
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% URL PARSING TESTS
%%%-----------------------------------------------------------------------------

url_parse_http(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"http://example.com">>),
    ?assertEqual(http, maps:get(scheme, UrlInfo)),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(80, maps:get(port, UrlInfo)),
    ok.

url_parse_https(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"https://example.com">>),
    ?assertEqual(https, maps:get(scheme, UrlInfo)),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(443, maps:get(port, UrlInfo)),
    ok.

url_parse_ws(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"ws://example.com/socket">>),
    ?assertEqual(ws, maps:get(scheme, UrlInfo)),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(80, maps:get(port, UrlInfo)),
    ?assertEqual(<<"/socket">>, maps:get(path, UrlInfo)),
    ok.

url_parse_wss(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"wss://example.com/socket">>),
    ?assertEqual(wss, maps:get(scheme, UrlInfo)),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(443, maps:get(port, UrlInfo)),
    ?assertEqual(<<"/socket">>, maps:get(path, UrlInfo)),
    ok.

url_parse_with_port(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"http://example.com:8080">>),
    ?assertEqual(8080, maps:get(port, UrlInfo)),
    ok.

url_parse_with_path(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"http://example.com/api/v1/users">>),
    ?assertEqual(<<"/api/v1/users">>, maps:get(path, UrlInfo)),
    ok.

url_parse_with_query(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"http://example.com/search?q=test&page=1">>),
    ?assertEqual(<<"/search">>, maps:get(path, UrlInfo)),
    ?assertEqual(<<"q=test&page=1">>, maps:get(query, UrlInfo)),
    ok.

url_parse_with_fragment(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"http://example.com/page#section">>),
    ?assertEqual(<<"/page#section">>, maps:get(path, UrlInfo)),
    ok.

url_parse_invalid_scheme(_Config) ->
    {error, _} = nhttpc_url:parse_url(<<"ftp://example.com">>),
    ok.

url_parse_no_host(_Config) ->
    {error, _} = nhttpc_url:parse_url(<<"http://">>),
    ok.

url_parse_ipv6_default_port(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"http://[::1]/">>),
    ?assertEqual(<<"[::1]">>, maps:get(host, UrlInfo)),
    ?assertEqual(80, maps:get(port, UrlInfo)),
    ?assertEqual(<<"/">>, maps:get(path, UrlInfo)),
    {ok, UrlInfo2} = nhttpc_url:parse_url(<<"https://[2001:db8::1]/api">>),
    ?assertEqual(<<"[2001:db8::1]">>, maps:get(host, UrlInfo2)),
    ?assertEqual(443, maps:get(port, UrlInfo2)),
    ok.

url_parse_ipv6_explicit_port(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"http://[::1]:8443/path">>),
    ?assertEqual(<<"[::1]">>, maps:get(host, UrlInfo)),
    ?assertEqual(8443, maps:get(port, UrlInfo)),
    ?assertEqual(<<"/path">>, maps:get(path, UrlInfo)),
    ok.

url_parse_ipv6_invalid_port(_Config) ->
    ?assertEqual({error, invalid_port}, nhttpc_url:parse_url(<<"http://[::1]:abc/">>)),
    ok.

url_parse_ipv6_missing_bracket(_Config) ->
    ?assertEqual(
        {error, invalid_ipv6_address}, nhttpc_url:parse_url(<<"http://[bad-ipv6/path">>)
    ),
    ok.

url_parse_invalid_authority(_Config) ->
    %% Three or more colons in the authority is not a valid host:port pair.
    ?assertEqual(
        {error, invalid_authority},
        nhttpc_url:parse_url(<<"http://host:1:2/path">>)
    ),
    ok.

url_parse_query_before_slash(_Config) ->
    %% A query marker appearing before any slash: authority terminates at `?`.
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"http://example.com?q=1">>),
    ?assertEqual(<<"example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(<<"/">>, maps:get(path, UrlInfo)),
    ?assertEqual(<<"q=1">>, maps:get(query, UrlInfo)),
    ok.

url_parse_ws_default_port(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"ws://example.com/socket">>),
    ?assertEqual(80, maps:get(port, UrlInfo)),
    ok.

url_parse_wss_default_port(_Config) ->
    {ok, UrlInfo} = nhttpc_url:parse_url(<<"wss://example.com/socket">>),
    ?assertEqual(443, maps:get(port, UrlInfo)),
    ok.

%%%-----------------------------------------------------------------------------
%%% to_request/1 TESTS
%%%-----------------------------------------------------------------------------

to_request_canonical(_Config) ->
    Canonical = #{
        method => get,
        scheme => https,
        authority => <<"api.example.com">>,
        path => <<"/v1/users">>,
        headers => [],
        body => <<>>
    },
    {ok, Req, UrlInfo} = nhttpc_url:to_request(Canonical),
    ?assertEqual(get, maps:get(method, Req)),
    ?assertEqual(https, maps:get(scheme, Req)),
    ?assertEqual(<<"api.example.com">>, maps:get(authority, Req)),
    ?assertEqual(<<"/v1/users">>, maps:get(path, Req)),
    ?assertEqual(<<"api.example.com">>, maps:get(host, UrlInfo)),
    ?assertEqual(443, maps:get(port, UrlInfo)),
    ok.

to_request_partial_path_only(_Config) ->
    %% Partial requests (path-only) are valid for pool / on-connection
    %% dispatch — the connection carries the authority.
    Partial = #{method => get, path => <<"/api/users">>, headers => [], body => <<>>},
    {ok, Req, UrlInfo} = nhttpc_url:to_request(Partial),
    ?assertEqual(get, maps:get(method, Req)),
    ?assertEqual(<<"/api/users">>, maps:get(path, Req)),
    %% No scheme / authority on the partial request.
    ?assertNot(maps:is_key(scheme, Req)),
    ?assertNot(maps:is_key(authority, Req)),
    %% UrlInfo is the dummy used to keep the transport layer happy.
    ?assertEqual(<<>>, maps:get(host, UrlInfo)),
    ok.

to_request_invalid(_Config) ->
    {error, {invalid_request, bad_shape}} = nhttpc_url:to_request(#{not_a => request}),
    {error, {invalid_request, bad_shape}} = nhttpc_url:to_request(#{}),
    %% The `#{method, url, ...}` shape is not a valid request map.
    {error, {invalid_request, bad_shape}} = nhttpc_url:to_request(
        #{method => get, url => <<"http://example.com/">>}
    ),
    ok.

to_request_ipv6_authority(_Config) ->
    %% A canonical request with an IPv6 authority and no explicit port: the
    %% IPv6 host is returned with the scheme's default port. This hits the
    %% `[IPv6, <<>>]` arm of split_authority_host_port/2.
    Canonical = #{
        method => get,
        scheme => https,
        authority => <<"[::1]">>,
        path => <<"/foo">>
    },
    {ok, _Req, UrlInfo} = nhttpc_url:to_request(Canonical),
    ?assertEqual(<<"[::1]">>, maps:get(host, UrlInfo)),
    ?assertEqual(443, maps:get(port, UrlInfo)),
    ok.

to_request_ipv6_authority_with_port(_Config) ->
    %% Canonical IPv6 authority with explicit port hits the `{ok, Port}`
    %% branch of parse_port/1 in split_authority_host_port/2.
    Canonical = #{
        method => get,
        scheme => https,
        authority => <<"[::1]:8443">>,
        path => <<"/foo">>
    },
    {ok, _Req, UrlInfo} = nhttpc_url:to_request(Canonical),
    ?assertEqual(<<"[::1]">>, maps:get(host, UrlInfo)),
    ?assertEqual(8443, maps:get(port, UrlInfo)),
    ok.

to_request_ipv6_invalid_port(_Config) ->
    %% Canonical IPv6 authority with a non-numeric port falls through to the
    %% DefaultPort arm in split_authority_host_port/2.
    Canonical = #{
        method => get,
        scheme => http,
        authority => <<"[::1]:abc">>,
        path => <<"/foo">>
    },
    {ok, _Req, UrlInfo} = nhttpc_url:to_request(Canonical),
    ?assertEqual(<<"[::1]">>, maps:get(host, UrlInfo)),
    ?assertEqual(80, maps:get(port, UrlInfo)),
    ok.

to_request_ipv6_unterminated(_Config) ->
    %% Unterminated IPv6 bracket in the canonical authority falls through to
    %% the catch-all in split_authority_host_port/2 (Rest + DefaultPort).
    Canonical = #{
        method => get,
        scheme => https,
        authority => <<"[bad-ipv6">>,
        path => <<"/foo">>
    },
    {ok, _Req, _UrlInfo} = nhttpc_url:to_request(Canonical),
    ok.

to_request_ws_scheme(_Config) ->
    Canonical = #{
        method => get,
        scheme => ws,
        authority => <<"chat.example.com">>,
        path => <<"/socket">>
    },
    ?assertEqual(
        {error, {invalid_request, invalid_url_scheme}}, nhttpc_url:to_request(Canonical)
    ).

to_request_wss_scheme(_Config) ->
    Canonical = #{
        method => get,
        scheme => wss,
        authority => <<"chat.example.com">>,
        path => <<"/socket">>
    },
    ?assertEqual(
        {error, {invalid_request, invalid_url_scheme}}, nhttpc_url:to_request(Canonical)
    ).

to_request_unknown_scheme(_Config) ->
    Canonical = #{
        method => get,
        scheme => ftp,
        authority => <<"files.example.com">>,
        path => <<"/pub">>
    },
    ?assertEqual(
        {error, {invalid_request, invalid_url_scheme}}, nhttpc_url:to_request(Canonical)
    ).

to_request_authority_extra_colons(_Config) ->
    %% Authority with three or more colons: split with [global] yields >2
    %% segments and hits the catch-all DefaultPort arm in
    %% split_authority_host_port/2.
    Canonical = #{
        method => get,
        scheme => http,
        authority => <<"host:1:2">>,
        path => <<"/foo">>
    },
    {ok, _Req, UrlInfo} = nhttpc_url:to_request(Canonical),
    ?assertEqual(80, maps:get(port, UrlInfo)),
    ok.

to_request_authority_bad_port(_Config) ->
    %% Non-numeric port in the regular authority arm hits the `error ->
    %% DefaultPort` branch of split_authority_host_port/2.
    Canonical = #{
        method => get,
        scheme => http,
        authority => <<"host:abc">>,
        path => <<"/foo">>
    },
    {ok, _Req, UrlInfo} = nhttpc_url:to_request(Canonical),
    ?assertEqual(<<"host">>, maps:get(host, UrlInfo)),
    ?assertEqual(80, maps:get(port, UrlInfo)),
    ok.

to_request_query_only_path(_Config) ->
    %% Canonical path containing only a query string: empty path normalises
    %% to `/` in url_info_from_canonical/3.
    Canonical = #{
        method => get,
        scheme => http,
        authority => <<"host">>,
        path => <<"?q=1">>
    },
    {ok, _Req, UrlInfo} = nhttpc_url:to_request(Canonical),
    ?assertEqual(<<"/">>, maps:get(path, UrlInfo)),
    ?assertEqual(<<"q=1">>, maps:get(query, UrlInfo)),
    ok.

%%%-----------------------------------------------------------------------------
%%% METHOD NORMALIZATION TESTS
%%%-----------------------------------------------------------------------------

normalize_method_atoms(_Config) ->
    [
        ?assertEqual(M, nhttpc_url:normalize_method(M))
     || M <- [get, post, put, delete, head]
    ],
    ok.

normalize_method_binaries(_Config) ->
    Pairs = [
        {<<"GET">>, get},
        {<<"POST">>, post},
        {<<"PUT">>, put},
        {<<"DELETE">>, delete},
        {<<"HEAD">>, head}
    ],
    [?assertEqual(Atom, nhttpc_url:normalize_method(Bin)) || {Bin, Atom} <- Pairs],
    ok.

normalize_method_connect(_Config) ->
    ?assertEqual(connect, nhttpc_url:normalize_method(connect)),
    ?assertEqual(connect, nhttpc_url:normalize_method(<<"CONNECT">>)),
    ok.

normalize_method_options(_Config) ->
    ?assertEqual(options, nhttpc_url:normalize_method(options)),
    ?assertEqual(options, nhttpc_url:normalize_method(<<"OPTIONS">>)),
    ok.

normalize_method_trace(_Config) ->
    ?assertEqual(trace, nhttpc_url:normalize_method(trace)),
    ?assertEqual(trace, nhttpc_url:normalize_method(<<"TRACE">>)),
    ok.

normalize_method_patch(_Config) ->
    ?assertEqual(patch, nhttpc_url:normalize_method(patch)),
    ?assertEqual(patch, nhttpc_url:normalize_method(<<"PATCH">>)),
    ok.

normalize_method_query(_Config) ->
    ?assertEqual(query, nhttpc_url:normalize_method(query)),
    ?assertEqual(query, nhttpc_url:normalize_method(<<"QUERY">>)),
    ok.

normalize_method_custom(_Config) ->
    ?assertEqual(<<"CUSTOM">>, nhttpc_url:normalize_method(<<"CUSTOM">>)),
    ok.

%%%-----------------------------------------------------------------------------
%%% URL INFO EXTRACTION TESTS
%%%-----------------------------------------------------------------------------

url_info_scheme(_Config) ->
    {ok, Info1} = nhttpc_url:parse_url(<<"http://example.com">>),
    ?assertEqual(http, maps:get(scheme, Info1)),
    {ok, Info2} = nhttpc_url:parse_url(<<"https://example.com">>),
    ?assertEqual(https, maps:get(scheme, Info2)),
    ok.

url_info_host(_Config) ->
    {ok, Info1} = nhttpc_url:parse_url(<<"http://localhost">>),
    ?assertEqual(<<"localhost">>, maps:get(host, Info1)),
    {ok, Info2} = nhttpc_url:parse_url(<<"http://api.example.com">>),
    ?assertEqual(<<"api.example.com">>, maps:get(host, Info2)),
    ok.

url_info_port(_Config) ->
    {ok, Info1} = nhttpc_url:parse_url(<<"http://example.com">>),
    ?assertEqual(80, maps:get(port, Info1)),
    {ok, Info2} = nhttpc_url:parse_url(<<"https://example.com">>),
    ?assertEqual(443, maps:get(port, Info2)),
    {ok, Info3} = nhttpc_url:parse_url(<<"http://example.com:3000">>),
    ?assertEqual(3000, maps:get(port, Info3)),
    ok.

url_info_path(_Config) ->
    {ok, Info1} = nhttpc_url:parse_url(<<"http://example.com">>),
    ?assertEqual(<<"/">>, maps:get(path, Info1)),
    {ok, Info2} = nhttpc_url:parse_url(<<"http://example.com/api/v1">>),
    ?assertEqual(<<"/api/v1">>, maps:get(path, Info2)),
    ok.

%%%-----------------------------------------------------------------------------
%%% PLAN 06 — nhttpc:url/2 AND CANONICAL SHAPE
%%%-----------------------------------------------------------------------------

url_2_parses_https_correctly(_Config) ->
    {ok, Req} = nhttpc:url(get, <<"https://api/foo">>),
    ?assertEqual(get, maps:get(method, Req)),
    ?assertEqual(https, maps:get(scheme, Req)),
    ?assertEqual(<<"api">>, maps:get(authority, Req)),
    ?assertEqual(<<"/foo">>, maps:get(path, Req)),
    ?assertEqual([], maps:get(headers, Req)),
    ?assertNot(maps:is_key(url, Req)),
    ok.

url_2_parses_http_correctly(_Config) ->
    {ok, Req} = nhttpc:url(post, <<"http://example.com/api/v1">>),
    ?assertEqual(post, maps:get(method, Req)),
    ?assertEqual(http, maps:get(scheme, Req)),
    ?assertEqual(<<"example.com">>, maps:get(authority, Req)),
    ?assertEqual(<<"/api/v1">>, maps:get(path, Req)),
    ok.

url_2_parses_ws_correctly(_Config) ->
    {ok, Req} = nhttpc:url(get, <<"ws://example.com:9000/socket">>),
    ?assertEqual(ws, maps:get(scheme, Req)),
    ?assertEqual(<<"example.com:9000">>, maps:get(authority, Req)),
    ?assertEqual(<<"/socket">>, maps:get(path, Req)),
    ok.

url_2_parses_wss_correctly(_Config) ->
    {ok, Req} = nhttpc:url(get, <<"wss://example.com/socket">>),
    ?assertEqual(wss, maps:get(scheme, Req)),
    ?assertEqual(<<"example.com">>, maps:get(authority, Req)),
    ?assertEqual(<<"/socket">>, maps:get(path, Req)),
    ok.

url_2_includes_query_in_path(_Config) ->
    {ok, Req} = nhttpc:url(get, <<"https://api/search?q=x&n=10">>),
    ?assertEqual(<<"/search?q=x&n=10">>, maps:get(path, Req)),
    ok.

url_2_keeps_non_default_port_in_authority(_Config) ->
    {ok, Req1} = nhttpc:url(get, <<"http://h:8080/">>),
    ?assertEqual(<<"h:8080">>, maps:get(authority, Req1)),
    {ok, Req2} = nhttpc:url(get, <<"https://h:9443/">>),
    ?assertEqual(<<"h:9443">>, maps:get(authority, Req2)),
    {ok, Req3} = nhttpc:url(get, <<"https://h/">>),
    ?assertEqual(<<"h">>, maps:get(authority, Req3)),
    ok.

url_2_rejects_invalid_url(_Config) ->
    {error, {invalid_request, _}} = nhttpc:url(get, <<"not-a-url">>),
    {error, {invalid_request, _}} = nhttpc:url(get, <<"ftp://example.com/">>),
    ok.

canonical_request_passes_through(_Config) ->
    Canonical = #{
        method => get,
        scheme => https,
        authority => <<"api.example.com">>,
        path => <<"/v1/x">>,
        headers => [{<<"accept">>, <<"application/json">>}]
    },
    {ok, Req, UrlInfo} = nhttpc_url:to_request(Canonical),
    ?assertEqual(get, maps:get(method, Req)),
    ?assertEqual(https, maps:get(scheme, Req)),
    ?assertEqual(<<"api.example.com">>, maps:get(authority, Req)),
    ?assertEqual(<<"/v1/x">>, maps:get(path, Req)),
    ?assertEqual([{<<"accept">>, <<"application/json">>}], maps:get(headers, Req)),
    ?assertEqual(<<"api.example.com">>, maps:get(host, UrlInfo)),
    ok.

request_missing_scheme_returns_invalid_request(_Config) ->
    %% Path-only is acceptable at to_request/1 (preserved for pool / on-connection
    %% dispatch) but oneshot dispatch must reject it as `invalid_request`.
    PartialReq = #{method => get, path => <<"/foo">>, headers => [], body => <<>>},
    {error, {invalid_request, missing_scheme_or_authority}} = nhttpc:request(PartialReq, #{}),
    ok.

request_ws_scheme_returns_invalid_request(_Config) ->
    {ok, Req} = nhttpc:url(get, <<"ws://127.0.0.1:1/socket">>),
    ?assertEqual({error, {invalid_request, invalid_url_scheme}}, nhttpc:request(Req, #{})).

convenience_get_builds_canonical_request(_Config) ->
    %% End-to-end check: `nhttpc:get/2` must build a canonical request and
    %% reach the listener with the right path. The body echoed back by the
    %% test handler contains the path, so a successful round-trip proves
    %% the convenience verb wired URL → canonical map → wire correctly.
    {ok, ServerPid} = nhttp:start_link(test_convenience_canonical_server, #{
        port => 0, handler => nhttp_test_handler, protocols => [http1_1]
    }),
    unlink(ServerPid),
    try
        {ok, Port} = nhttp:get_port(ServerPid),
        Url = iolist_to_binary(
            io_lib:format("http://127.0.0.1:~B/echo?x=1", [Port])
        ),
        {ok, #{status := 200, body := Body}} = nhttpc:get(Url, #{}),
        ?assertNotEqual(nomatch, binary:match(Body, <<"GET request to /echo?x=1">>))
    after
        nhttpc_test_server:stop(ServerPid)
    end,
    ok.
