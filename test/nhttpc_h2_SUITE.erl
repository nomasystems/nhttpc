%%%-----------------------------------------------------------------------------
%%% @doc Unit tests for nhttpc_h2 module.
%%%
%%% Tests HTTP/2 protocol utilities including:
%%% - Request header conversion (request_to_h2_headers)
%%% - H2 response event processing
%%% - Error code conversion
%%% - Response size checking
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_h2_SUITE).

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

%% request_to_h2_headers tests
-export([
    request_to_h2_basic/1,
    request_to_h2_with_scheme_authority/1,
    request_to_h2_filters_host/1,
    request_to_h2_preserves_other_headers/1
]).

%% stream_reset via events tests (stream_reset_to_error is internal)
-export([
    h2_events_reset_cancel/1,
    h2_events_reset_refused_stream/1,
    h2_events_reset_protocol_error/1,
    h2_events_reset_enhance_your_calm/1
]).

%% process_h2_response_events tests
-export([
    h2_events_empty/1,
    h2_events_response_fin/1,
    h2_events_response_nofin_data_fin/1,
    h2_events_multiple_data_chunks/1,
    h2_events_trailers/1,
    h2_events_stream_reset/1,
    h2_events_goaway/1,
    h2_events_skips_other_events/1,
    h2_events_size_exceeded/1,
    h2_events_size_infinity/1
]).

%% Error wrapping helper tests
-export([
    send_with_error_ok/1,
    send_with_error_failure/1,
    wrap_h2_send_result_ok/1,
    wrap_h2_send_result_error/1,
    send_h2_body_no_body/1
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
        {group, request_to_h2_headers},
        {group, stream_reset_via_events},
        {group, process_h2_response_events},
        {group, error_wrapping},
        {group, response_accumulator}
    ].

groups() ->
    [
        {request_to_h2_headers, [parallel], [
            request_to_h2_basic,
            request_to_h2_with_scheme_authority,
            request_to_h2_filters_host,
            request_to_h2_preserves_other_headers
        ]},
        {stream_reset_via_events, [parallel], [
            h2_events_reset_cancel,
            h2_events_reset_refused_stream,
            h2_events_reset_protocol_error,
            h2_events_reset_enhance_your_calm
        ]},
        {process_h2_response_events, [parallel], [
            h2_events_empty,
            h2_events_response_fin,
            h2_events_response_nofin_data_fin,
            h2_events_multiple_data_chunks,
            h2_events_trailers,
            h2_events_stream_reset,
            h2_events_goaway,
            h2_events_skips_other_events,
            h2_events_size_exceeded,
            h2_events_size_infinity
        ]},
        {error_wrapping, [parallel], [
            send_with_error_ok,
            send_with_error_failure,
            wrap_h2_send_result_ok,
            wrap_h2_send_result_error,
            send_h2_body_no_body
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
%%% request_to_h2_headers TESTS
%%%-----------------------------------------------------------------------------

request_to_h2_basic(_Config) ->
    Req = #{
        method => get,
        path => <<"/api/test">>,
        headers => [{<<"host">>, <<"example.com">>}],
        body => <<>>
    },
    Headers = nhttpc_h2:request_to_h2_headers(Req),
    ?assertEqual(<<"GET">>, proplists:get_value(<<":method">>, Headers)),
    ?assertEqual(<<"/api/test">>, proplists:get_value(<<":path">>, Headers)),
    ?assertEqual(<<"https">>, proplists:get_value(<<":scheme">>, Headers)),
    ?assertEqual(<<"example.com">>, proplists:get_value(<<":authority">>, Headers)).

request_to_h2_with_scheme_authority(_Config) ->
    Req = #{
        method => post,
        path => <<"/upload">>,
        headers => [{<<"content-type">>, <<"application/json">>}],
        body => <<"{}">>
    },
    Headers = nhttpc_h2:request_to_h2_headers(Req, <<"http">>, <<"api.example.com:8080">>),
    ?assertEqual(<<"POST">>, proplists:get_value(<<":method">>, Headers)),
    ?assertEqual(<<"/upload">>, proplists:get_value(<<":path">>, Headers)),
    ?assertEqual(<<"http">>, proplists:get_value(<<":scheme">>, Headers)),
    ?assertEqual(<<"api.example.com:8080">>, proplists:get_value(<<":authority">>, Headers)).

request_to_h2_filters_host(_Config) ->
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

request_to_h2_preserves_other_headers(_Config) ->
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
    Headers = nhttpc_h2:request_to_h2_headers(Req),
    ?assertEqual(<<"application/json">>, proplists:get_value(<<"accept">>, Headers)),
    ?assertEqual(<<"Bearer token">>, proplists:get_value(<<"authorization">>, Headers)),
    ?assertEqual(<<"value">>, proplists:get_value(<<"x-custom">>, Headers)).

%%%-----------------------------------------------------------------------------
%%% STREAM RESET VIA EVENTS TESTS
%%% Tests stream_reset_to_error indirectly through process_h2_response_events
%%%-----------------------------------------------------------------------------

h2_events_reset_cancel(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    Events = [{stream_reset, 1, cancel}],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertEqual({error, cancelled}, Result).

h2_events_reset_refused_stream(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    Events = [{stream_reset, 1, refused_stream}],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertEqual({error, {refused, retryable}}, Result).

h2_events_reset_protocol_error(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    Events = [{stream_reset, 1, protocol_error}],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertEqual({error, {http2_error, {stream_reset, protocol_error}}}, Result).

h2_events_reset_enhance_your_calm(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    Events = [{stream_reset, 1, enhance_your_calm}],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertMatch({error, {http2_error, {stream_reset, enhance_your_calm}}}, Result).

%%%-----------------------------------------------------------------------------
%%% process_h2_response_events TESTS
%%%-----------------------------------------------------------------------------

h2_events_empty(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    Result = nhttpc_h2:process_h2_response_events([], 1, Acc, infinity),
    ?assertMatch({continue, _}, Result).

h2_events_response_fin(_Config) ->
    %% Response with END_STREAM (no body, e.g., 204 No Content)
    Acc = nhttpc_h2:new_response_acc(),
    Response = #{status => 204, headers => []},
    Events = [{response, 1, Response, fin}],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertMatch({done, #{status := 204, body := <<>>}}, Result).

h2_events_response_nofin_data_fin(_Config) ->
    %% Response without END_STREAM, then data with END_STREAM
    Acc = nhttpc_h2:new_response_acc(),
    Response = #{status => 200, headers => []},
    Events = [
        {response, 1, Response, nofin},
        {data, 1, <<"Hello">>, fin}
    ],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertMatch({done, #{status := 200, body := <<"Hello">>}}, Result).

h2_events_multiple_data_chunks(_Config) ->
    %% Multiple data events before final
    Acc = nhttpc_h2:new_response_acc(),
    Response = #{status => 200, headers => []},
    Events = [
        {response, 1, Response, nofin},
        {data, 1, <<"Hello, ">>, nofin},
        {data, 1, <<"World!">>, fin}
    ],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertMatch({done, #{status := 200, body := <<"Hello, World!">>}}, Result).

h2_events_trailers(_Config) ->
    %% Response, data, then trailers (terminal)
    Acc = nhttpc_h2:new_response_acc(),
    Response = #{status => 200, headers => []},
    Trailers = [{<<"x-trailer">>, <<"value">>}],
    Events = [
        {response, 1, Response, nofin},
        {data, 1, <<"body">>, nofin},
        {trailers, 1, Trailers}
    ],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertMatch(
        {done, #{status := 200, body := <<"body">>, trailers := [{<<"x-trailer">>, <<"value">>}]}},
        Result
    ).

h2_events_stream_reset(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    Events = [{stream_reset, 1, cancel}],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertEqual({error, cancelled}, Result).

h2_events_goaway(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    Events = [{goaway, 0, protocol_error, <<>>}],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertEqual({error, {http2_error, {goaway, protocol_error, retryable}}}, Result).

h2_events_skips_other_events(_Config) ->
    %% Settings, window_update, ping etc. should be skipped
    Acc = nhttpc_h2:new_response_acc(),
    Response = #{status => 200, headers => []},
    Events = [
        {settings, #{max_concurrent_streams => 100}},
        {window_update, 0, 65535},
        {ping_ack, <<1, 2, 3, 4, 5, 6, 7, 8>>},
        {response, 1, Response, fin}
    ],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc, infinity),
    ?assertMatch({done, #{status := 200}}, Result).

h2_events_size_exceeded(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    %% Set response first
    Acc1 = Acc#{response => #{status => 200, headers => []}},
    %% Large data chunk exceeds 100 byte limit
    Events = [{data, 1, binary:copy(<<0>>, 200), fin}],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc1, 100),
    ?assertMatch({error, {request, #{type := response_too_large}}}, Result).

h2_events_size_infinity(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    Acc1 = Acc#{response => #{status => 200, headers => []}},
    %% Large data with infinity limit should succeed
    LargeData = binary:copy(<<0>>, 10000),
    Events = [{data, 1, LargeData, fin}],
    Result = nhttpc_h2:process_h2_response_events(Events, 1, Acc1, infinity),
    ?assertMatch({done, #{status := 200}}, Result).

%%%-----------------------------------------------------------------------------
%%% ERROR WRAPPING HELPER TESTS
%%%-----------------------------------------------------------------------------

send_with_error_ok(_Config) ->
    Result = nhttpc_h2:wrap_h2_send_result({ok, mock_conn, <<"frame">>}),
    ?assertEqual({ok, mock_conn, <<"frame">>}, Result).

send_with_error_failure(_Config) ->
    Result = nhttpc_h2:wrap_h2_send_result({error, some_reason}),
    ?assertEqual({error, {http2_error, some_reason}}, Result).

wrap_h2_send_result_ok(_Config) ->
    Result = nhttpc_h2:wrap_h2_send_result({ok, conn, <<"data">>}),
    ?assertEqual({ok, conn, <<"data">>}, Result).

wrap_h2_send_result_error(_Config) ->
    Result = nhttpc_h2:wrap_h2_send_result({error, flow_control}),
    ?assertEqual({error, {http2_error, flow_control}}, Result).

send_h2_body_no_body(_Config) ->
    Result = nhttpc_stream_send:send_h2_body(undefined, mock_conn, 1, <<>>, 5000),
    ?assertEqual({ok, mock_conn, []}, Result).

%%%-----------------------------------------------------------------------------
%%% RESPONSE ACCUMULATOR TESTS
%%%-----------------------------------------------------------------------------

new_response_acc(_Config) ->
    Acc = nhttpc_h2:new_response_acc(),
    ?assertEqual(undefined, maps:get(response, Acc)),
    ?assertEqual([], maps:get(body_acc, Acc)),
    ?assertEqual(0, maps:get(acc_size, Acc)).
