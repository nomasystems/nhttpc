%%%-----------------------------------------------------------------------------
%%% @doc Unit tests for `nhttpc_opts:normalize/1`, transport derivation,
%%% accessors, and unknown-key rejection. The 1.0 surface accepts only
%%% the canonical vocabulary; the pre-1.0 key shapes are not recognised.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_opts_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    suite/0,
    groups/0
]).

-export([
    transport_opt_derived_from_scheme/1,
    pool_transport_derived_from_tls/1,
    pool_transport_quic_when_h3_in_versions/1,
    unknown_opt_returns_invalid_opts/1,
    unknown_pool_opt_returns_invalid_opts/1,
    pre_1_0_timeout_key_is_unknown/1,
    pre_1_0_protocols_key_is_unknown/1,
    pre_1_0_ssl_options_key_is_unknown/1,
    pre_1_0_transport_key_is_unknown/1,
    pre_1_0_stream_to_key_is_unknown/1,
    conn_h2_codec_bound_is_unreachable_by_default/1,
    timeout_accessor_uses_default/1,
    timeout_accessor_reads_canonical/1,
    versions_accessor_default/1
]).

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [
        {group, transport_derivation},
        {group, validation},
        {group, accessors}
    ].

groups() ->
    [
        {transport_derivation, [parallel], [
            transport_opt_derived_from_scheme,
            pool_transport_derived_from_tls,
            pool_transport_quic_when_h3_in_versions
        ]},
        {validation, [parallel], [
            unknown_opt_returns_invalid_opts,
            unknown_pool_opt_returns_invalid_opts,
            pre_1_0_timeout_key_is_unknown,
            pre_1_0_protocols_key_is_unknown,
            pre_1_0_ssl_options_key_is_unknown,
            pre_1_0_transport_key_is_unknown,
            pre_1_0_stream_to_key_is_unknown,
            conn_h2_codec_bound_is_unreachable_by_default
        ]},
        {accessors, [parallel], [
            timeout_accessor_uses_default,
            timeout_accessor_reads_canonical,
            versions_accessor_default
        ]}
    ].

%%%-----------------------------------------------------------------------------
%%% TRANSPORT DERIVATION TESTS
%%%-----------------------------------------------------------------------------

transport_opt_derived_from_scheme(_Config) ->
    ?assertEqual(
        ssl,
        nhttpc_opts:request_transport(#{scheme => https, host => <<"x">>, port => 443}, #{})
    ),
    ?assertEqual(
        tcp,
        nhttpc_opts:request_transport(#{scheme => http, host => <<"x">>, port => 80}, #{})
    ),
    ?assertEqual(
        quic,
        nhttpc_opts:request_transport(
            #{scheme => https, host => <<"x">>, port => 443},
            #{versions => [http3]}
        )
    ).

pool_transport_derived_from_tls(_Config) ->
    ?assertEqual(tcp, nhttpc_opts:pool_transport(#{})),
    ?assertEqual(ssl, nhttpc_opts:pool_transport(#{tls => #{verify => verify_peer}})).

pool_transport_quic_when_h3_in_versions(_Config) ->
    ?assertEqual(
        quic,
        nhttpc_opts:pool_transport(#{versions => [http3]})
    ).

%%%-----------------------------------------------------------------------------
%%% VALIDATION TESTS
%%%-----------------------------------------------------------------------------

unknown_opt_returns_invalid_opts(_Config) ->
    Result = nhttpc_opts:normalize(#{not_a_real_opt => 1}),
    ?assertMatch({error, {invalid_opts, {unknown, not_a_real_opt}}}, Result).

unknown_pool_opt_returns_invalid_opts(_Config) ->
    Result = nhttpc_opts:normalize_pool(
        #{host => <<"x">>, port => 443, mystery => yes}
    ),
    ?assertMatch({error, {invalid_opts, {unknown, mystery}}}, Result).

%% `nhttp_h2:open_stream/1` refuses an id above 2^31 - 1, which is the
%% 2^30th stream of a connection. The default threshold and the option
%% bound both sit below that, so `stream_ids_exhausted` cannot fire under
%% any accepted configuration.
conn_h2_codec_bound_is_unreachable_by_default(_Config) ->
    CodecBound = 1 bsl 30,
    Default = nhttpc_opts:max_streams_per_connection(#{}),
    ?assert(Default > 0),
    ?assert(Default < CodecBound),
    Base = #{host => <<"x">>, port => 443},
    ?assertMatch(
        {ok, _},
        nhttpc_opts:normalize_pool(Base#{max_streams_per_connection => CodecBound - 1})
    ),
    ?assertMatch({ok, _}, nhttpc_opts:normalize_pool(Base#{max_streams_per_connection => 1})),
    ?assertEqual(
        CodecBound - 1,
        nhttpc_opts:max_streams_per_connection(#{max_streams_per_connection => CodecBound - 1})
    ),
    lists:foreach(
        fun(Bad) ->
            ?assertEqual(
                {error, {invalid_opts, {bad_max_streams_per_connection, Bad}}},
                nhttpc_opts:normalize_pool(Base#{max_streams_per_connection => Bad})
            )
        end,
        [CodecBound, CodecBound + 1, 0, -1, infinity, <<"1">>]
    ).

pre_1_0_timeout_key_is_unknown(_Config) ->
    ?assertMatch(
        {error, {invalid_opts, {unknown, timeout}}},
        nhttpc_opts:normalize(#{timeout => 5000})
    ),
    ?assertMatch(
        {error, {invalid_opts, {unknown, recv_timeout}}},
        nhttpc_opts:normalize(#{recv_timeout => 5000})
    ),
    ?assertMatch(
        {error, {invalid_opts, {unknown, connect_timeout}}},
        nhttpc_opts:normalize(#{connect_timeout => 5000})
    ).

pre_1_0_protocols_key_is_unknown(_Config) ->
    ?assertMatch(
        {error, {invalid_opts, {unknown, protocols}}},
        nhttpc_opts:normalize(#{protocols => [http2, http1_1]})
    ).

pre_1_0_ssl_options_key_is_unknown(_Config) ->
    ?assertMatch(
        {error, {invalid_opts, {unknown, ssl_options}}},
        nhttpc_opts:normalize(#{ssl_options => [{verify, verify_none}]})
    ).

pre_1_0_transport_key_is_unknown(_Config) ->
    ?assertMatch(
        {error, {invalid_opts, {unknown, transport}}},
        nhttpc_opts:normalize(#{transport => ssl})
    ).

pre_1_0_stream_to_key_is_unknown(_Config) ->
    ?assertMatch(
        {error, {invalid_opts, {unknown, stream_to}}},
        nhttpc_opts:normalize(#{stream_to => "/tmp/out.bin"})
    ).

%%%-----------------------------------------------------------------------------
%%% ACCESSOR TESTS
%%%-----------------------------------------------------------------------------

timeout_accessor_uses_default(_Config) ->
    ?assertEqual(30000, nhttpc_opts:timeout(request, #{})),
    ?assertEqual(5000, nhttpc_opts:timeout(connect, #{})),
    ?assertEqual(5000, nhttpc_opts:timeout(checkout, #{})),
    ?assertEqual(60000, nhttpc_opts:timeout(idle, #{})).

timeout_accessor_reads_canonical(_Config) ->
    Opts = #{timeouts => #{request => 1234, connect => 999}},
    ?assertEqual(1234, nhttpc_opts:timeout(request, Opts)),
    ?assertEqual(999, nhttpc_opts:timeout(connect, Opts)),
    %% Missing key falls back to default.
    ?assertEqual(5000, nhttpc_opts:timeout(checkout, Opts)).

versions_accessor_default(_Config) ->
    ?assertEqual([http2, http1_1], nhttpc_opts:versions(#{})),
    ?assertEqual([http3], nhttpc_opts:versions(#{versions => [http3]})).
