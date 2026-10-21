%%%-----------------------------------------------------------------------------
%%% @doc Unit tests for nhttpc_h3 module.
%%%
%%% Tests HTTP/3 protocol utilities including:
%%% - Request header conversion (request_to_h3_headers)
%%% - H3 response event processing
%%% - Error code mapping (RFC 9114)
%%% - Error wrapping helpers
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_h3_SUITE).

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

%% request_to_h3_headers tests
-export([
    request_to_h3_basic/1,
    request_to_h3_with_scheme_authority/1,
    request_to_h3_filters_host/1,
    request_to_h3_preserves_other_headers/1,
    request_to_h3_post_method/1
]).

%% h3_error_to_code tests
-export([
    h3_error_no_error/1,
    h3_error_general_protocol/1,
    h3_error_internal/1,
    h3_error_request_cancelled/1,
    h3_error_request_rejected/1,
    h3_error_qpack_decompression/1,
    h3_error_code_table/1
]).

%% execute_actions tests
-export([
    execute_actions_flush_empty/1,
    execute_actions_flush_close_connection/1,
    execute_actions_noflush_empty/1,
    execute_actions_noflush_close_connection/1,
    execute_actions_flush_close_short_circuits/1,
    execute_actions_noflush_close_short_circuits/1
]).

%% process_h3_response_events tests
-export([
    h3_events_empty/1,
    h3_events_response_fin/1,
    h3_events_response_nofin_data_fin/1,
    h3_events_multiple_data_chunks/1,
    h3_events_trailers/1,
    h3_events_stream_reset_cancelled/1,
    h3_events_stream_reset_refused/1,
    h3_events_stream_reset_other/1,
    h3_events_goaway/1,
    h3_events_skips_other_events/1
]).

%% Error wrapping helper tests
-export([
    wrap_h3_send_result_ok/1,
    wrap_h3_send_result_error/1
]).

%% Response accumulator tests
-export([
    new_response_acc/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [
        {group, request_to_h3_headers},
        {group, h3_error_codes},
        {group, process_h3_response_events},
        {group, error_wrapping},
        {group, response_accumulator},
        {group, execute_actions}
    ].

groups() ->
    [
        {request_to_h3_headers, [parallel], [
            request_to_h3_basic,
            request_to_h3_with_scheme_authority,
            request_to_h3_filters_host,
            request_to_h3_preserves_other_headers,
            request_to_h3_post_method
        ]},
        {h3_error_codes, [parallel], [
            h3_error_no_error,
            h3_error_general_protocol,
            h3_error_internal,
            h3_error_request_cancelled,
            h3_error_request_rejected,
            h3_error_qpack_decompression,
            h3_error_code_table
        ]},
        {execute_actions, [parallel], [
            execute_actions_flush_empty,
            execute_actions_flush_close_connection,
            execute_actions_flush_close_short_circuits,
            execute_actions_noflush_empty,
            execute_actions_noflush_close_connection,
            execute_actions_noflush_close_short_circuits
        ]},
        {process_h3_response_events, [parallel], [
            h3_events_empty,
            h3_events_response_fin,
            h3_events_response_nofin_data_fin,
            h3_events_multiple_data_chunks,
            h3_events_trailers,
            h3_events_stream_reset_cancelled,
            h3_events_stream_reset_refused,
            h3_events_stream_reset_other,
            h3_events_goaway,
            h3_events_skips_other_events
        ]},
        {error_wrapping, [parallel], [
            wrap_h3_send_result_ok,
            wrap_h3_send_result_error
        ]},
        {response_accumulator, [parallel], [
            new_response_acc
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
%%% request_to_h3_headers TESTS
%%%-----------------------------------------------------------------------------

request_to_h3_basic(_Config) ->
    Req = #{
        method => get,
        path => <<"/api/test">>,
        headers => [{<<"host">>, <<"example.com">>}],
        body => <<>>
    },
    Headers = nhttpc_h3:request_to_h3_headers(Req),
    ?assertEqual(<<"GET">>, proplists:get_value(<<":method">>, Headers)),
    ?assertEqual(<<"/api/test">>, proplists:get_value(<<":path">>, Headers)),
    ?assertEqual(<<"https">>, proplists:get_value(<<":scheme">>, Headers)),
    ?assertEqual(<<"example.com">>, proplists:get_value(<<":authority">>, Headers)).

request_to_h3_with_scheme_authority(_Config) ->
    Req = #{
        method => post,
        path => <<"/upload">>,
        headers => [{<<"content-type">>, <<"application/json">>}],
        body => <<"{}">>
    },
    Headers = nhttpc_h3:request_to_h3_headers(Req, <<"https">>, <<"api.example.com:8443">>),
    ?assertEqual(<<"POST">>, proplists:get_value(<<":method">>, Headers)),
    ?assertEqual(<<"/upload">>, proplists:get_value(<<":path">>, Headers)),
    ?assertEqual(<<"https">>, proplists:get_value(<<":scheme">>, Headers)),
    ?assertEqual(<<"api.example.com:8443">>, proplists:get_value(<<":authority">>, Headers)).

request_to_h3_filters_host(_Config) ->
    Req = #{
        method => get,
        path => <<"/">>,
        headers => [
            {<<"host">>, <<"example.com">>},
            {<<"accept">>, <<"*/*">>}
        ],
        body => <<>>
    },
    Headers = nhttpc_h3:request_to_h3_headers(Req),
    ?assertEqual(undefined, proplists:get_value(<<"host">>, Headers)),
    ?assertEqual(<<"*/*">>, proplists:get_value(<<"accept">>, Headers)).

request_to_h3_preserves_other_headers(_Config) ->
    Req = #{
        method => get,
        path => <<"/">>,
        headers => [
            {<<"host">>, <<"example.com">>},
            {<<"accept">>, <<"application/json">>},
            {<<"authorization">>, <<"Bearer token">>},
            {<<"x-custom">>, <<"value">>}
        ],
        body => <<>>
    },
    Headers = nhttpc_h3:request_to_h3_headers(Req),
    ?assertEqual(<<"application/json">>, proplists:get_value(<<"accept">>, Headers)),
    ?assertEqual(<<"Bearer token">>, proplists:get_value(<<"authorization">>, Headers)),
    ?assertEqual(<<"value">>, proplists:get_value(<<"x-custom">>, Headers)).

request_to_h3_post_method(_Config) ->
    Req = #{
        method => post,
        path => <<"/data">>,
        headers => [
            {<<"host">>, <<"example.com">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"payload">>
    },
    Headers = nhttpc_h3:request_to_h3_headers(Req),
    ?assertEqual(<<"POST">>, proplists:get_value(<<":method">>, Headers)),
    ?assertEqual(<<"example.com">>, proplists:get_value(<<":authority">>, Headers)),
    ?assertEqual(<<"text/plain">>, proplists:get_value(<<"content-type">>, Headers)).

%%%-----------------------------------------------------------------------------
%%% h3_error_to_code TESTS
%%%-----------------------------------------------------------------------------

h3_error_no_error(_Config) ->
    ?assertEqual(16#0100, nhttpc_h3:h3_error_to_code(h3_no_error)).

h3_error_general_protocol(_Config) ->
    ?assertEqual(16#0101, nhttpc_h3:h3_error_to_code(h3_general_protocol_error)).

h3_error_internal(_Config) ->
    ?assertEqual(16#0102, nhttpc_h3:h3_error_to_code(h3_internal_error)).

h3_error_request_cancelled(_Config) ->
    ?assertEqual(16#010c, nhttpc_h3:h3_error_to_code(h3_request_cancelled)).

h3_error_request_rejected(_Config) ->
    ?assertEqual(16#010b, nhttpc_h3:h3_error_to_code(h3_request_rejected)).

h3_error_qpack_decompression(_Config) ->
    ?assertEqual(16#0200, nhttpc_h3:h3_error_to_code(qpack_decompression_failed)).

h3_error_code_table(_Config) ->
    %% Walk every RFC 9114 §8.1 error code (and the QPACK error codes from
    %% RFC 9204 §6) to ensure the dispatch table stays in sync with the spec.
    Cases = [
        {h3_no_error, 16#0100},
        {h3_general_protocol_error, 16#0101},
        {h3_internal_error, 16#0102},
        {h3_stream_creation_error, 16#0103},
        {h3_closed_critical_stream, 16#0104},
        {h3_frame_unexpected, 16#0105},
        {h3_frame_error, 16#0106},
        {h3_excessive_load, 16#0107},
        {h3_id_error, 16#0108},
        {h3_settings_error, 16#0109},
        {h3_missing_settings, 16#010a},
        {h3_request_rejected, 16#010b},
        {h3_request_cancelled, 16#010c},
        {h3_request_incomplete, 16#010d},
        {h3_message_error, 16#010e},
        {h3_connect_error, 16#010f},
        {h3_version_fallback, 16#0110},
        {qpack_decompression_failed, 16#0200},
        {qpack_encoder_stream_error, 16#0201},
        {qpack_decoder_stream_error, 16#0202}
    ],
    [?assertEqual(Code, nhttpc_h3:h3_error_to_code(Name)) || {Name, Code} <- Cases],
    ok.

%%%-----------------------------------------------------------------------------
%%% process_h3_response_events TESTS
%%%-----------------------------------------------------------------------------

h3_events_empty(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Result = nhttpc_h3:process_h3_response_events([], 1, Acc),
    ?assertMatch({continue, _}, Result).

h3_events_response_fin(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Response = #{status => 204, headers => []},
    Events = [{response, 1, Response, fin}],
    Result = nhttpc_h3:process_h3_response_events(Events, 1, Acc),
    ?assertMatch({done, #{status := 204, body := <<>>}}, Result).

h3_events_response_nofin_data_fin(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Response = #{status => 200, headers => []},
    Events = [
        {response, 1, Response, nofin},
        {data, 1, <<"Hello">>, fin}
    ],
    Result = nhttpc_h3:process_h3_response_events(Events, 1, Acc),
    ?assertMatch({done, #{status := 200, body := <<"Hello">>}}, Result).

h3_events_multiple_data_chunks(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Response = #{status => 200, headers => []},
    Events = [
        {response, 1, Response, nofin},
        {data, 1, <<"Hello, ">>, nofin},
        {data, 1, <<"World!">>, fin}
    ],
    Result = nhttpc_h3:process_h3_response_events(Events, 1, Acc),
    ?assertMatch({done, #{status := 200, body := <<"Hello, World!">>}}, Result).

h3_events_trailers(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Response = #{status => 200, headers => []},
    Trailers = [{<<"grpc-status">>, <<"0">>}],
    Events = [
        {response, 1, Response, nofin},
        {data, 1, <<"body">>, nofin},
        {trailers, 1, Trailers}
    ],
    Result = nhttpc_h3:process_h3_response_events(Events, 1, Acc),
    ?assertMatch(
        {done, #{status := 200, body := <<"body">>, trailers := [{<<"grpc-status">>, <<"0">>}]}},
        Result
    ).

h3_events_stream_reset_cancelled(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Events = [{stream_reset, 1, 16#010c}],
    Result = nhttpc_h3:process_h3_response_events(Events, 1, Acc),
    ?assertEqual({error, cancelled}, Result).

h3_events_stream_reset_refused(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Events = [{stream_reset, 1, 16#010b}],
    Result = nhttpc_h3:process_h3_response_events(Events, 1, Acc),
    ?assertEqual({error, {refused, retryable}}, Result).

h3_events_stream_reset_other(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Events = [{stream_reset, 1, 16#0102}],
    Result = nhttpc_h3:process_h3_response_events(Events, 1, Acc),
    ?assertEqual({error, {http3_error, {stream_reset, 16#0102}}}, Result).

h3_events_goaway(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Events = [{goaway, 4, h3_no_error, <<"shutdown">>}],
    Result = nhttpc_h3:process_h3_response_events(Events, 1, Acc),
    ?assertEqual({error, {http3_error, {goaway, retryable}}}, Result).

h3_events_skips_other_events(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    Response = #{status => 200, headers => []},
    Events = [
        {settings, #{max_field_section_size => 8192}},
        {response, 1, Response, fin}
    ],
    Result = nhttpc_h3:process_h3_response_events(Events, 1, Acc),
    ?assertMatch({done, #{status := 200}}, Result).

%%%-----------------------------------------------------------------------------
%%% ERROR WRAPPING HELPER TESTS
%%%-----------------------------------------------------------------------------

wrap_h3_send_result_ok(_Config) ->
    Result = nhttpc_h3:wrap_h3_send_result({ok, mock_conn, []}),
    ?assertEqual({ok, mock_conn, []}, Result).

wrap_h3_send_result_error(_Config) ->
    Result = nhttpc_h3:wrap_h3_send_result({error, some_reason}),
    ?assertEqual({error, {http3_error, some_reason}}, Result).

%%%-----------------------------------------------------------------------------
%%% RESPONSE ACCUMULATOR TESTS
%%%-----------------------------------------------------------------------------

new_response_acc(_Config) ->
    Acc = nhttpc_h3:new_response_acc(),
    ?assertEqual(undefined, maps:get(response, Acc)),
    ?assertEqual([], maps:get(body_acc, Acc)),
    ?assertEqual(0, maps:get(acc_size, Acc)).

%%%-----------------------------------------------------------------------------
%%% EXECUTE_ACTIONS TESTS
%%%-----------------------------------------------------------------------------
%%%
%%% Cover the pure dispatch arms: empty action list and `{close_connection,
%%% Code, _Reason}`. The `{send, _}` / `{send_fin, _}` happy paths and their
%%% `{error, _}` / `{error, _, _}` returns are exercised end-to-end by the
%%% H3 connection process (`nhttpc_h3_conn_SUITE`) and the H3 chaos paths
%%% scheduled in plan 19 — driving them here would require either spinning
%%% up a real `nquic:ctx()` (heavy for a unit suite) or mocking the
%%% `nquic_lib` boundary, neither of which earns its keep in this module.

execute_actions_flush_empty(_Config) ->
    %% Empty action list returns `{ok, Ctx}` verbatim — Ctx may be any term
    %% because we never inspect it for the empty arm.
    Ctx = ctx_placeholder,
    ?assertEqual({ok, Ctx}, nhttpc_h3:execute_actions_flush(Ctx, [])).

execute_actions_noflush_empty(_Config) ->
    Ctx = ctx_placeholder,
    ?assertEqual({ok, Ctx}, nhttpc_h3:execute_actions_noflush(Ctx, [])).

execute_actions_flush_close_connection(_Config) ->
    %% The `{close_connection, Code, _}` action short-circuits the loop and
    %% returns `{close, Code}` without inspecting Ctx.
    Actions = [{close_connection, 16#0100, <<"shutdown">>}],
    ?assertEqual(
        {close, 16#0100},
        nhttpc_h3:execute_actions_flush(ctx_placeholder, Actions)
    ).

execute_actions_noflush_close_connection(_Config) ->
    Actions = [{close_connection, 16#0102, <<"internal">>}],
    ?assertEqual(
        {close, 16#0102},
        nhttpc_h3:execute_actions_noflush(ctx_placeholder, Actions)
    ).

execute_actions_flush_close_short_circuits(_Config) ->
    %% Anything after the close action is unreachable; we don't even touch
    %% the placeholder ctx for follow-on sends. The action list intentionally
    %% includes a bogus `{send, _, _}` that would crash if reached.
    Actions = [
        {close_connection, 16#0101, <<"protocol">>},
        {send, 0, <<"never reached">>}
    ],
    ?assertEqual(
        {close, 16#0101},
        nhttpc_h3:execute_actions_flush(ctx_placeholder, Actions)
    ).

execute_actions_noflush_close_short_circuits(_Config) ->
    Actions = [
        {close_connection, 16#010c, <<"cancelled">>},
        {send_fin, 0, <<"never reached">>}
    ],
    ?assertEqual(
        {close, 16#010c},
        nhttpc_h3:execute_actions_noflush(ctx_placeholder, Actions)
    ).
