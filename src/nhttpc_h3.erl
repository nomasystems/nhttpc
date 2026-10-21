-module(nhttpc_h3).

-moduledoc """
HTTP/3 protocol utilities for the nhttpc client.

This module provides HTTP/3-specific utilities for request/response handling:
- Converting `t:nhttp_lib:request/0` maps to HTTP/3 pseudo-headers
- Error wrapping helpers for maybe expressions
- Response event accumulation and processing
- H3 error code mapping (RFC 9114)

Response and trailers come pre-decoded from `nhttp_h3:recv/4` as
`t:nhttp_lib:response/0` maps and `t:nhttp_lib:headers/0` lists. No local
header-to-response conversion is needed.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS - REQUEST CONVERSION
%%%-----------------------------------------------------------------------------
-export([
    request_to_h3_headers/1,
    request_to_h3_headers/3
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - ERROR HELPERS
%%%-----------------------------------------------------------------------------
-export([
    h3_error_to_code/1,
    wrap_h3_send_result/1
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - H3 RESPONSE EVENT PROCESSING
%%%-----------------------------------------------------------------------------
-export([
    new_response_acc/0,
    process_h3_response_events/3
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - ACTION EXECUTION (LIBRARY MODE)
%%%-----------------------------------------------------------------------------
-export([
    execute_actions_flush/2,
    execute_actions_noflush/2
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([h3_response_acc/0]).

-type h3_response_acc() :: #{
    response := nhttp_lib:response() | undefined,
    body_acc := [binary()],
    acc_size := non_neg_integer()
}.

%%%-----------------------------------------------------------------------------
%% REQUEST CONVERSION
%%%-----------------------------------------------------------------------------
-doc """
Convert request map to HTTP/3 request headers.

Uses "https" as the default scheme. Extracts host header for :authority.
""".
-spec request_to_h3_headers(nhttp_lib:request()) -> nhttp_lib:headers().
request_to_h3_headers(#{method := Method, path := Path, headers := Headers}) ->
    Authority = proplists:get_value(<<"host">>, Headers, <<>>),
    request_to_h3_headers_impl(Method, Path, <<"https">>, Authority, Headers).

-doc """
Convert request map to HTTP/3 request headers with explicit scheme and authority.
Used when scheme and authority are already known from connection state.
""".
-spec request_to_h3_headers(nhttp_lib:request(), binary(), binary()) -> nhttp_lib:headers().
request_to_h3_headers(
    #{method := Method, path := Path, headers := Headers}, Scheme, Authority
) ->
    request_to_h3_headers_impl(Method, Path, Scheme, Authority, Headers).

-spec request_to_h3_headers_impl(
    nhttp_lib:method(), binary(), binary(), binary(), nhttp_lib:headers()
) ->
    nhttp_lib:headers().
request_to_h3_headers_impl(Method, Path, Scheme, Authority, Headers) ->
    PseudoHeaders = [
        {<<":method">>, nhttp_lib:encode_method(Method)},
        {<<":path">>, Path},
        {<<":scheme">>, Scheme},
        {<<":authority">>, Authority}
    ],
    RegularHeaders = nhttp_headers:delete(<<"host">>, Headers),
    PseudoHeaders ++ RegularHeaders.

%%%-----------------------------------------------------------------------------
%% ERROR HELPERS
%%%-----------------------------------------------------------------------------
-doc """
Map H3 error code atom to numeric code (RFC 9114 Section 8.1).
""".
-dialyzer({no_match, h3_error_to_code/1}).
-spec h3_error_to_code(nhttp_h3:h3_error_code()) -> non_neg_integer().
h3_error_to_code(h3_no_error) -> 16#0100;
h3_error_to_code(h3_general_protocol_error) -> 16#0101;
h3_error_to_code(h3_internal_error) -> 16#0102;
h3_error_to_code(h3_stream_creation_error) -> 16#0103;
h3_error_to_code(h3_closed_critical_stream) -> 16#0104;
h3_error_to_code(h3_frame_unexpected) -> 16#0105;
h3_error_to_code(h3_frame_error) -> 16#0106;
h3_error_to_code(h3_excessive_load) -> 16#0107;
h3_error_to_code(h3_id_error) -> 16#0108;
h3_error_to_code(h3_settings_error) -> 16#0109;
h3_error_to_code(h3_missing_settings) -> 16#010a;
h3_error_to_code(h3_request_rejected) -> 16#010b;
h3_error_to_code(h3_request_cancelled) -> 16#010c;
h3_error_to_code(h3_request_incomplete) -> 16#010d;
h3_error_to_code(h3_message_error) -> 16#010e;
h3_error_to_code(h3_connect_error) -> 16#010f;
h3_error_to_code(h3_version_fallback) -> 16#0110;
h3_error_to_code(qpack_decompression_failed) -> 16#0200;
h3_error_to_code(qpack_encoder_stream_error) -> 16#0201;
h3_error_to_code(qpack_decoder_stream_error) -> 16#0202.

-doc """
Wrap H3 send result with http3_error tag on failure.
Used with maybe expressions to transform H3 protocol errors.
""".
-spec wrap_h3_send_result(
    {ok, nhttp_h3:conn(), [nhttp_h3:action()]} | {error, nhttpc_error:any_reason()}
) ->
    {ok, nhttp_h3:conn(), [nhttp_h3:action()]} | {error, nhttpc_error:any_reason()}.
wrap_h3_send_result({ok, _, _} = Ok) -> Ok;
wrap_h3_send_result({error, Reason}) -> {error, {http3_error, Reason}}.

%%%-----------------------------------------------------------------------------
%% H3 RESPONSE EVENT PROCESSING
%%%-----------------------------------------------------------------------------
-doc "Create a new empty response accumulator.".
-spec new_response_acc() -> h3_response_acc().
new_response_acc() ->
    #{response => undefined, body_acc => [], acc_size => 0}.

-doc """
Process H3 response events for a single stream.
Returns:
- `{continue, NewAcc}` - need more events
- `{done, nhttp_lib:response()}` - complete response received
- `{error, nhttpc_error:any_reason()}` - error occurred (stream reset, goaway, size exceeded)
""".
-spec process_h3_response_events(
    [nhttp_h3:event()],
    nhttp_lib:stream_id(),
    h3_response_acc()
) ->
    {continue, h3_response_acc()}
    | {done, nhttp_lib:response()}
    | {error, nhttpc_error:any_reason()}.
process_h3_response_events([], _StreamId, Acc) ->
    {continue, Acc};
process_h3_response_events([Event | Rest], StreamId, Acc) ->
    case Event of
        {response, StreamId, Response, fin} ->
            {done, Response#{body => <<>>}};
        {response, StreamId, Response, nofin} ->
            NewAcc = Acc#{response => Response},
            process_h3_response_events(Rest, StreamId, NewAcc);
        {data, StreamId, Data, fin} ->
            #{response := Response, body_acc := BodyAcc} = Acc,
            Body = iolist_to_binary(lists:reverse([Data | BodyAcc])),
            {done, Response#{body => Body}};
        {data, StreamId, Data, nofin} ->
            #{body_acc := BodyAcc, acc_size := AccSize} = Acc,
            DataSize = byte_size(Data),
            NewAcc = Acc#{
                body_acc => [Data | BodyAcc],
                acc_size => AccSize + DataSize
            },
            process_h3_response_events(Rest, StreamId, NewAcc);
        {trailers, StreamId, Trailers} ->
            #{response := Response, body_acc := BodyAcc} = Acc,
            Body = iolist_to_binary(lists:reverse(BodyAcc)),
            {done, Response#{body => Body, trailers => Trailers}};
        {stream_reset, StreamId, ErrorCode} ->
            stream_reset_to_error(ErrorCode);
        {goaway, _LastStreamId, _ErrorCode, _Reason} ->
            {error, {http3_error, {goaway, retryable}}};
        _ ->
            process_h3_response_events(Rest, StreamId, Acc)
    end.

-spec stream_reset_to_error(non_neg_integer()) -> {error, nhttpc_error:any_reason()}.
stream_reset_to_error(16#010c) ->
    {error, cancelled};
stream_reset_to_error(16#010b) ->
    {error, {refused, retryable}};
stream_reset_to_error(ErrorCode) ->
    {error, {http3_error, {stream_reset, ErrorCode}}}.

%%%-----------------------------------------------------------------------------
%% ACTION EXECUTION (LIBRARY MODE)
%%%-----------------------------------------------------------------------------
-doc """
Execute H3 actions with flush after each send (library mode).
Used for one-time setup (H3 connection init) where batching is not needed.
Each send operation flushes to the network immediately.
""".
-spec execute_actions_flush(nquic:ctx(), [nhttp_h3:action()]) ->
    {ok, nquic:ctx()} | {error, nhttpc_error:any_reason()} | {close, non_neg_integer()}.
execute_actions_flush(Ctx, []) ->
    {ok, Ctx};
execute_actions_flush(Ctx, [{send, StreamId, Data} | Rest]) ->
    case nquic_lib:send(Ctx, StreamId, Data) of
        {ok, Ctx1} -> execute_actions_flush(Ctx1, Rest);
        {error, _} = Error -> Error;
        {error, Reason, _Ctx1} -> {error, Reason}
    end;
execute_actions_flush(Ctx, [{send_fin, StreamId, Data} | Rest]) ->
    case nquic_lib:send_fin(Ctx, StreamId, Data) of
        {ok, Ctx1} -> execute_actions_flush(Ctx1, Rest);
        {error, _} = Error -> Error;
        {error, Reason, _Ctx1} -> {error, Reason}
    end;
execute_actions_flush(_Ctx, [{close_connection, Code, _Reason} | _Rest]) ->
    {close, Code}.

-doc """
Execute H3 actions without flushing FIN sends (library mode).
Used for request batching. Non-FIN sends (control/encoder/decoder streams)
are sent immediately via `lib_send/3`. FIN sends use `lib_send_fin_noflush/3`
so the caller can batch multiple requests before a single
`nquic_lib:flush_notimers/1`.
""".
-spec execute_actions_noflush(nquic:ctx(), [nhttp_h3:action()]) ->
    {ok, nquic:ctx()} | {error, nhttpc_error:any_reason()} | {close, non_neg_integer()}.
execute_actions_noflush(Ctx, []) ->
    {ok, Ctx};
execute_actions_noflush(Ctx, [{send, StreamId, Data} | Rest]) ->
    case nquic_lib:send(Ctx, StreamId, Data) of
        {ok, Ctx1} -> execute_actions_noflush(Ctx1, Rest);
        {error, _} = Error -> Error;
        {error, Reason, _Ctx1} -> {error, Reason}
    end;
execute_actions_noflush(Ctx, [{send_fin, StreamId, Data} | Rest]) ->
    case nquic_lib:send_fin_noflush(Ctx, StreamId, Data) of
        {ok, Ctx1} -> execute_actions_noflush(Ctx1, Rest);
        {error, _} = Error -> Error;
        {error, Reason, _Ctx1} -> {error, Reason}
    end;
execute_actions_noflush(_Ctx, [{close_connection, Code, _Reason} | _Rest]) ->
    {close, Code}.
