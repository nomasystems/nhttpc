%%%-----------------------------------------------------------------------------
%%% @doc Unit tests for nhttpc_request module.
%%%
%%% Tests internal helper functions and edge cases not covered by
%%% integration tests in nhttpc_SUITE.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_request_SUITE).

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
    end_per_group/2
]).

%% URL/path helper tests
-export([
    url_info_to_url_http_default_port/1,
    url_info_to_url_https_default_port/1,
    url_info_to_url_custom_port/1,
    url_info_to_url_with_query/1,
    url_info_to_url_empty_query/1
]).

%% Header helper tests
-export([
    has_header_found/1,
    has_header_missing/1,
    get_header_value_found/1,
    get_header_value_missing/1,
    get_header_value_lowercase/1
]).

%% Compression helper tests
-export([
    get_content_encoding_gzip/1,
    get_content_encoding_x_gzip/1,
    get_content_encoding_deflate/1,
    get_content_encoding_identity/1,
    get_content_encoding_unknown/1,
    get_content_encoding_missing/1,
    maybe_add_accept_encoding_default/1,
    maybe_add_accept_encoding_disabled/1,
    maybe_add_accept_encoding_already_set/1
]).

%% Method conversion tests
-export([
    method_to_binary_get/1,
    method_to_binary_head/1,
    method_to_binary_post/1,
    method_to_binary_put/1,
    method_to_binary_delete/1,
    method_to_binary_options/1,
    method_to_binary_patch/1,
    method_to_binary_connect/1,
    method_to_binary_trace/1,
    method_to_binary_custom/1
]).

%% HTTP/2 header conversion tests
-export([
    request_to_h2_headers_basic/1,
    request_to_h2_headers_with_host/1,
    request_to_h2_headers_filters_host/1
]).

%% Connection options tests
-export([
    build_connect_opts_tcp/1,
    build_connect_opts_ssl_verify/1,
    build_connect_opts_ssl_cacerts/1,
    build_connect_opts_ssl_cacertfile/1,
    build_connect_opts_ssl_certfile/1,
    build_connect_opts_ssl_keyfile/1,
    build_connect_opts_ssl_sni/1,
    build_connect_opts_ssl_versions/1,
    build_connect_opts_ssl_alpn/1,
    build_connect_opts_ssl_wildcard_hostname/1,
    build_connect_opts_unknown_ssl_opt/1
]).

%% Host header tests
-export([
    add_host_header_ssl_default_port/1,
    add_host_header_ssl_custom_port/1,
    add_host_header_tcp_default_port/1,
    add_host_header_tcp_custom_port/1,
    add_host_header_already_present/1
]).

%% Header limit tests
-export([
    h1_parse_opts_defaults/1,
    h1_parse_opts_override_size/1,
    h1_parse_opts_override_count/1,
    h1_parse_opts_infinity_drops_bound/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [
        {group, url_helpers},
        {group, header_helpers},
        {group, compression_helpers},
        {group, method_conversion},
        {group, h2_headers},
        {group, connect_opts},
        {group, host_header},
        {group, header_limits}
    ].

groups() ->
    [
        {url_helpers, [parallel], [
            url_info_to_url_http_default_port,
            url_info_to_url_https_default_port,
            url_info_to_url_custom_port,
            url_info_to_url_with_query,
            url_info_to_url_empty_query
        ]},
        {header_helpers, [parallel], [
            has_header_found,
            has_header_missing,
            get_header_value_found,
            get_header_value_missing,
            get_header_value_lowercase
        ]},
        {compression_helpers, [parallel], [
            get_content_encoding_gzip,
            get_content_encoding_x_gzip,
            get_content_encoding_deflate,
            get_content_encoding_identity,
            get_content_encoding_unknown,
            get_content_encoding_missing,
            maybe_add_accept_encoding_default,
            maybe_add_accept_encoding_disabled,
            maybe_add_accept_encoding_already_set
        ]},
        {method_conversion, [parallel], [
            method_to_binary_get,
            method_to_binary_head,
            method_to_binary_post,
            method_to_binary_put,
            method_to_binary_delete,
            method_to_binary_options,
            method_to_binary_patch,
            method_to_binary_connect,
            method_to_binary_trace,
            method_to_binary_custom
        ]},
        {h2_headers, [parallel], [
            request_to_h2_headers_basic,
            request_to_h2_headers_with_host,
            request_to_h2_headers_filters_host
        ]},
        {connect_opts, [parallel], [
            build_connect_opts_tcp,
            build_connect_opts_ssl_verify,
            build_connect_opts_ssl_cacerts,
            build_connect_opts_ssl_cacertfile,
            build_connect_opts_ssl_certfile,
            build_connect_opts_ssl_keyfile,
            build_connect_opts_ssl_sni,
            build_connect_opts_ssl_versions,
            build_connect_opts_ssl_alpn,
            build_connect_opts_ssl_wildcard_hostname,
            build_connect_opts_unknown_ssl_opt
        ]},
        {host_header, [parallel], [
            add_host_header_ssl_default_port,
            add_host_header_ssl_custom_port,
            add_host_header_tcp_default_port,
            add_host_header_tcp_custom_port,
            add_host_header_already_present
        ]},
        {header_limits, [parallel], [
            h1_parse_opts_defaults,
            h1_parse_opts_override_size,
            h1_parse_opts_override_count,
            h1_parse_opts_infinity_drops_bound
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

%%%-----------------------------------------------------------------------------
%%% URL HELPER TESTS
%%%-----------------------------------------------------------------------------

url_info_to_url_http_default_port(_Config) ->
    UrlInfo = #{scheme => http, host => <<"example.com">>, port => 80, path => <<"/">>},
    Result = nhttpc_request:url_info_to_url(UrlInfo),
    ?assertEqual(<<"http://example.com/">>, Result).

url_info_to_url_https_default_port(_Config) ->
    UrlInfo = #{scheme => https, host => <<"example.com">>, port => 443, path => <<"/api">>},
    Result = nhttpc_request:url_info_to_url(UrlInfo),
    ?assertEqual(<<"https://example.com/api">>, Result).

url_info_to_url_custom_port(_Config) ->
    UrlInfo = #{scheme => http, host => <<"example.com">>, port => 8080, path => <<"/">>},
    Result = nhttpc_request:url_info_to_url(UrlInfo),
    ?assertEqual(<<"http://example.com:8080/">>, Result).

url_info_to_url_with_query(_Config) ->
    UrlInfo = #{
        scheme => https,
        host => <<"example.com">>,
        port => 443,
        path => <<"/search">>,
        query => <<"q=test&limit=10">>
    },
    Result = nhttpc_request:url_info_to_url(UrlInfo),
    ?assertEqual(<<"https://example.com/search?q=test&limit=10">>, Result).

url_info_to_url_empty_query(_Config) ->
    UrlInfo = #{
        scheme => http,
        host => <<"example.com">>,
        port => 80,
        path => <<"/path">>,
        query => <<>>
    },
    Result = nhttpc_request:url_info_to_url(UrlInfo),
    ?assertEqual(<<"http://example.com/path">>, Result).

%%%-----------------------------------------------------------------------------
%%% HEADER HELPER TESTS
%%%-----------------------------------------------------------------------------

has_header_found(_Config) ->
    %% Headers are lowercase (normalized at parse time)
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    ?assert(nhttp_headers:has(<<"content-type">>, Headers)).

has_header_missing(_Config) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    ?assertNot(nhttp_headers:has(<<"accept">>, Headers)).

get_header_value_found(_Config) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    ?assertEqual(
        <<"text/plain">>, nhttpc_request:get_header_value(<<"content-type">>, Headers)
    ).

get_header_value_missing(_Config) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    ?assertEqual(undefined, nhttpc_request:get_header_value(<<"accept">>, Headers)).

get_header_value_lowercase(_Config) ->
    %% Headers are lowercase (normalized at parse time)
    Headers = [{<<"content-type">>, <<"application/json">>}],
    ?assertEqual(
        <<"application/json">>, nhttpc_request:get_header_value(<<"content-type">>, Headers)
    ).

%%%-----------------------------------------------------------------------------
%%% COMPRESSION HELPER TESTS
%%%-----------------------------------------------------------------------------

get_content_encoding_gzip(_Config) ->
    Headers = [{<<"content-encoding">>, <<"gzip">>}],
    ?assertEqual(gzip, nhttpc_request:get_content_encoding(Headers)).

get_content_encoding_x_gzip(_Config) ->
    Headers = [{<<"content-encoding">>, <<"x-gzip">>}],
    ?assertEqual(gzip, nhttpc_request:get_content_encoding(Headers)).

get_content_encoding_deflate(_Config) ->
    Headers = [{<<"content-encoding">>, <<"deflate">>}],
    ?assertEqual(deflate, nhttpc_request:get_content_encoding(Headers)).

get_content_encoding_identity(_Config) ->
    Headers = [{<<"content-encoding">>, <<"identity">>}],
    ?assertEqual(identity, nhttpc_request:get_content_encoding(Headers)).

get_content_encoding_unknown(_Config) ->
    Headers = [{<<"content-encoding">>, <<"br">>}],
    ?assertEqual({unknown, <<"br">>}, nhttpc_request:get_content_encoding(Headers)).

get_content_encoding_missing(_Config) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    ?assertEqual(identity, nhttpc_request:get_content_encoding(Headers)).

maybe_add_accept_encoding_default(_Config) ->
    Req = #{method => get, path => <<"/">>, headers => [], body => <<>>},
    Opts = #{},
    Result = nhttpc_request:maybe_add_accept_encoding(Req, Opts),
    ?assert(nhttp_headers:has(<<"accept-encoding">>, maps:get(headers, Result))).

maybe_add_accept_encoding_disabled(_Config) ->
    Req = #{method => get, path => <<"/">>, headers => [], body => <<>>},
    Opts = #{compress => false},
    Result = nhttpc_request:maybe_add_accept_encoding(Req, Opts),
    ?assertNot(nhttp_headers:has(<<"accept-encoding">>, maps:get(headers, Result))).

maybe_add_accept_encoding_already_set(_Config) ->
    Req = #{
        method => get,
        path => <<"/">>,
        headers => [{<<"accept-encoding">>, <<"br">>}],
        body => <<>>
    },
    Opts = #{},
    Result = nhttpc_request:maybe_add_accept_encoding(Req, Opts),
    %% Should not add another accept-encoding
    AcceptEncodingHeaders = [
        V
     || {K, V} <- maps:get(headers, Result), string:lowercase(K) =:= <<"accept-encoding">>
    ],
    ?assertEqual(1, length(AcceptEncodingHeaders)),
    ?assertEqual([<<"br">>], AcceptEncodingHeaders).

%%%-----------------------------------------------------------------------------
%%% METHOD CONVERSION TESTS
%%%-----------------------------------------------------------------------------

method_to_binary_get(_Config) ->
    ?assertEqual(<<"GET">>, nhttp_lib:encode_method(get)).

method_to_binary_head(_Config) ->
    ?assertEqual(<<"HEAD">>, nhttp_lib:encode_method(head)).

method_to_binary_post(_Config) ->
    ?assertEqual(<<"POST">>, nhttp_lib:encode_method(post)).

method_to_binary_put(_Config) ->
    ?assertEqual(<<"PUT">>, nhttp_lib:encode_method(put)).

method_to_binary_delete(_Config) ->
    ?assertEqual(<<"DELETE">>, nhttp_lib:encode_method(delete)).

method_to_binary_options(_Config) ->
    ?assertEqual(<<"OPTIONS">>, nhttp_lib:encode_method(options)).

method_to_binary_patch(_Config) ->
    ?assertEqual(<<"PATCH">>, nhttp_lib:encode_method(patch)).

method_to_binary_connect(_Config) ->
    ?assertEqual(<<"CONNECT">>, nhttp_lib:encode_method(connect)).

method_to_binary_trace(_Config) ->
    ?assertEqual(<<"TRACE">>, nhttp_lib:encode_method(trace)).

method_to_binary_custom(_Config) ->
    ?assertEqual(<<"CUSTOM">>, nhttp_lib:encode_method(<<"CUSTOM">>)).

%%%-----------------------------------------------------------------------------
%%% HTTP/2 HEADER CONVERSION TESTS
%%%-----------------------------------------------------------------------------

request_to_h2_headers_basic(_Config) ->
    Req = #{
        method => get,
        path => <<"/api/test">>,
        headers => [{<<"host">>, <<"example.com">>}],
        body => <<>>
    },
    Headers = nhttpc_h2:request_to_h2_headers(Req),
    %% Check pseudo-headers
    ?assertEqual(<<"GET">>, proplists:get_value(<<":method">>, Headers)),
    ?assertEqual(<<"/api/test">>, proplists:get_value(<<":path">>, Headers)),
    ?assertEqual(<<"https">>, proplists:get_value(<<":scheme">>, Headers)),
    ?assertEqual(<<"example.com">>, proplists:get_value(<<":authority">>, Headers)),
    %% Host header should be filtered out
    ?assertEqual(undefined, proplists:get_value(<<"host">>, Headers)).

request_to_h2_headers_with_host(_Config) ->
    Req = #{
        method => post,
        path => <<"/">>,
        headers => [
            {<<"host">>, <<"api.example.com">>}, {<<"content-type">>, <<"application/json">>}
        ],
        body => <<>>
    },
    Headers = nhttpc_h2:request_to_h2_headers(Req),
    ?assertEqual(<<"api.example.com">>, proplists:get_value(<<":authority">>, Headers)),
    ?assertEqual(<<"application/json">>, proplists:get_value(<<"content-type">>, Headers)).

request_to_h2_headers_filters_host(_Config) ->
    %% Headers should be lowercase (normalized at parse time)
    Req = #{
        method => get,
        path => <<"/">>,
        headers => [
            {<<"host">>, <<"example.com">>},
            {<<"accept">>, <<"*/*">>}
        ],
        body => <<>>
    },
    Headers = nhttpc_h2:request_to_h2_headers(Req),
    %% host should be filtered (replaced by :authority)
    ?assertEqual(undefined, proplists:get_value(<<"host">>, Headers)),
    %% accept should be preserved
    ?assertEqual(<<"*/*">>, proplists:get_value(<<"accept">>, Headers)).

%%%-----------------------------------------------------------------------------
%%% CONNECTION OPTIONS TESTS
%%%-----------------------------------------------------------------------------

build_connect_opts_tcp(_Config) ->
    Result = nhttpc_request:build_connect_opts(tcp, #{}),
    ?assertEqual(#{transport => tcp}, Result).

build_connect_opts_ssl_verify(_Config) ->
    Opts = #{tls => #{verify => verify_peer}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual(verify_peer, maps:get(verify, Result)).

build_connect_opts_ssl_cacerts(_Config) ->
    CaCerts = [<<"cert1">>, <<"cert2">>],
    Opts = #{tls => #{cacerts => CaCerts}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual(CaCerts, maps:get(cacerts, Result)).

build_connect_opts_ssl_cacertfile(_Config) ->
    Opts = #{tls => #{cacertfile => "/path/to/ca.crt"}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual("/path/to/ca.crt", maps:get(cacertfile, Result)).

build_connect_opts_ssl_certfile(_Config) ->
    Opts = #{tls => #{certfile => "/path/to/cert.crt"}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual("/path/to/cert.crt", maps:get(certfile, Result)).

build_connect_opts_ssl_keyfile(_Config) ->
    Opts = #{tls => #{keyfile => "/path/to/key.pem"}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual("/path/to/key.pem", maps:get(keyfile, Result)).

build_connect_opts_ssl_sni(_Config) ->
    Opts = #{tls => #{server_name_indication => "example.com"}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual("example.com", maps:get(server_name_indication, Result)).

build_connect_opts_ssl_versions(_Config) ->
    Versions = ['tlsv1.2', 'tlsv1.3'],
    Opts = #{tls => #{versions => Versions}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual(Versions, maps:get(versions, Result)).

build_connect_opts_ssl_alpn(_Config) ->
    Alpn = [<<"h2">>, <<"http/1.1">>],
    Opts = #{tls => #{alpn_advertised_protocols => Alpn}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual(Alpn, maps:get(alpn_advertised_protocols, Result)).

build_connect_opts_ssl_wildcard_hostname(_Config) ->
    Opts = #{tls => #{wildcard_hostname => true}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual(true, maps:get(wildcard_hostname, Result)).

build_connect_opts_unknown_ssl_opt(_Config) ->
    %% `extra` provides the escape hatch for keys we don't know about.
    Opts = #{tls => #{extra => [{unknown_option, some_value}]}},
    Result = nhttpc_request:build_connect_opts(ssl, Opts),
    ?assertEqual(some_value, maps:get(unknown_option, Result)).

%%%-----------------------------------------------------------------------------
%%% HOST HEADER TESTS
%%%-----------------------------------------------------------------------------

add_host_header_ssl_default_port(_Config) ->
    Req = #{method => get, path => <<"/">>, headers => [], body => <<>>},
    Result = nhttpc_request:add_host_header(Req, <<"example.com">>, 443, ssl),
    ?assertEqual(<<"example.com">>, proplists:get_value(<<"host">>, maps:get(headers, Result))).

add_host_header_ssl_custom_port(_Config) ->
    Req = #{method => get, path => <<"/">>, headers => [], body => <<>>},
    Result = nhttpc_request:add_host_header(Req, <<"example.com">>, 8443, ssl),
    ?assertEqual(
        <<"example.com:8443">>, proplists:get_value(<<"host">>, maps:get(headers, Result))
    ).

add_host_header_tcp_default_port(_Config) ->
    Req = #{method => get, path => <<"/">>, headers => [], body => <<>>},
    Result = nhttpc_request:add_host_header(Req, <<"example.com">>, 80, tcp),
    ?assertEqual(<<"example.com">>, proplists:get_value(<<"host">>, maps:get(headers, Result))).

add_host_header_tcp_custom_port(_Config) ->
    Req = #{method => get, path => <<"/">>, headers => [], body => <<>>},
    Result = nhttpc_request:add_host_header(Req, <<"example.com">>, 8080, tcp),
    ?assertEqual(
        <<"example.com:8080">>, proplists:get_value(<<"host">>, maps:get(headers, Result))
    ).

add_host_header_already_present(_Config) ->
    Req = #{
        method => get,
        path => <<"/">>,
        headers => [{<<"host">>, <<"original.com">>}],
        body => <<>>
    },
    Result = nhttpc_request:add_host_header(Req, <<"new.com">>, 80, tcp),
    %% Should not add new host header
    ?assertEqual(1, length(maps:get(headers, Result))),
    ?assertEqual(<<"original.com">>, proplists:get_value(<<"host">>, maps:get(headers, Result))).

%%%-----------------------------------------------------------------------------
%%% HEADER LIMIT TESTS
%%%-----------------------------------------------------------------------------

h1_parse_opts_defaults(_Config) ->
    %% Empty opts must surface the safe defaults: 64 KiB / 100 headers.
    ?assertEqual(
        #{max_header_size => 65536, max_headers_count => 100},
        nhttpc_request:h1_parse_opts(#{})
    ).

h1_parse_opts_override_size(_Config) ->
    ?assertEqual(
        #{max_header_size => 8192, max_headers_count => 100},
        nhttpc_request:h1_parse_opts(#{max_header_size => 8192})
    ).

h1_parse_opts_override_count(_Config) ->
    ?assertEqual(
        #{max_header_size => 65536, max_headers_count => 50},
        nhttpc_request:h1_parse_opts(#{max_headers_count => 50})
    ).

h1_parse_opts_infinity_drops_bound(_Config) ->
    %% `infinity` opts out of a bound by being absent from the parser opts map.
    ?assertEqual(
        #{max_headers_count => 100},
        nhttpc_request:h1_parse_opts(#{max_header_size => infinity})
    ),
    ?assertEqual(
        #{max_header_size => 65536},
        nhttpc_request:h1_parse_opts(#{max_headers_count => infinity})
    ),
    ?assertEqual(
        #{},
        nhttpc_request:h1_parse_opts(#{
            max_header_size => infinity, max_headers_count => infinity
        })
    ).
