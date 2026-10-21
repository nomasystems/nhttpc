-module(nhttpc_h2).

-moduledoc """
HTTP/2 protocol utilities for the nhttpc client.

This module provides HTTP/2-specific utilities for request/response handling:
- Converting `t:nhttp_lib:request/0` maps to HTTP/2 pseudo-headers
- Error wrapping helpers for maybe expressions
- Response event accumulation and processing

Response and trailers come pre-decoded from `nhttp_h2:recv/2` as
`t:nhttp_lib:response/0` maps and `t:nhttp_lib:headers/0` lists. No local
header-to-response conversion is needed.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    body_size/1,
    codec_settings/0,
    credit_claim/1,
    max_send_buffer/0,
    new_response_acc/0,
    process_h2_response_events/4,

    request_to_h2_headers/1,
    request_to_h2_headers/3,
    send_with_error/2,
    window_update_frames/2,

    wrap_h2_send_result/1
]).

-export_type([h2_response_acc/0]).

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(CREDIT_CLAIM_CAP, 65535).
-define(MAX_SEND_BUFFER, 131072).

%%%-----------------------------------------------------------------------------
%% REQUEST CONVERSION
%%%-----------------------------------------------------------------------------
-doc """
The octets of a request body on an HTTP/2 connection. A request without
a body, or one whose body streams from a source the pool cannot measure,
has a size of zero here.
""".
-spec body_size(nhttp_lib:request()) -> non_neg_integer().
body_size(#{body := Body}) when is_binary(Body); is_list(Body) ->
    iolist_size(Body);
body_size(_Request) ->
    0.

-doc """
The local settings of the HTTP/2 codec on a poolless connection: the
send queue on, bounded at `MAX_SEND_BUFFER` octets. A body that outruns
the peer's credit waits in the codec, and the sender reads the socket
for the WINDOW_UPDATE that drains it (RFC 9113 Section 5.2.2), so no
body size is refused. The bound caps the octets held for the peer, not
the body: a sender slices its offers at `max_send_buffer/0` and blocks
until the queue takes the next slice.
""".
-spec codec_settings() -> nhttp_h2:settings().
codec_settings() ->
    #{send_queue => true, max_send_buffer => ?MAX_SEND_BUFFER}.

-doc """
The octets of send credit a request claims from a connection at
checkout: its body size, capped at the initial connection window of
RFC 9113 Section 6.9.2. A body inside the cap is sent in full against
the credit the pool reserved for it. A body above the cap claims what a
fresh connection can carry, opens its stream, and the connection
process settles the rest of its octets against the credit slot as the
codec send queue drains them.
""".
-spec credit_claim(nhttp_lib:request()) -> non_neg_integer().
credit_claim(Request) ->
    min(body_size(Request), ?CREDIT_CLAIM_CAP).

-doc "The octets the poolless codec send queue holds at most, the largest offer it accepts.".
-spec max_send_buffer() -> pos_integer().
max_send_buffer() ->
    ?MAX_SEND_BUFFER.

-doc "Convert request map to HTTP/2 request headers. Uses \"https\" as the default scheme. Extracts host header for :authority.".
-spec request_to_h2_headers(nhttp_lib:request()) -> nhttp_lib:headers().
request_to_h2_headers(#{method := Method, path := Path, headers := Headers}) ->
    Authority = proplists:get_value(<<"host">>, Headers, <<>>),
    request_to_h2_headers_impl(Method, Path, <<"https">>, Authority, Headers).

-doc "Convert request map to HTTP/2 request headers with explicit scheme and authority. Used when scheme and authority are already known (e.g., from connection state).".
-spec request_to_h2_headers(nhttp_lib:request(), binary(), binary()) -> nhttp_lib:headers().
request_to_h2_headers(
    #{method := Method, path := Path, headers := Headers}, Scheme, Authority
) ->
    request_to_h2_headers_impl(Method, Path, Scheme, Authority, Headers).

-doc "Build HTTP/2 headers with pseudo-headers.".
-spec request_to_h2_headers_impl(
    nhttp_lib:method(), binary(), binary(), binary(), nhttp_lib:headers()
) ->
    nhttp_lib:headers().
request_to_h2_headers_impl(Method, Path, Scheme, Authority, Headers) ->
    PseudoHeaders = [
        {<<":method">>, nhttp_lib:encode_method(Method)},
        {<<":path">>, Path},
        {<<":scheme">>, Scheme},
        {<<":authority">>, Authority}
    ],
    RegularHeaders = nhttp_headers:delete(<<"host">>, Headers),
    PseudoHeaders ++ RegularHeaders.

%%%-----------------------------------------------------------------------------
%% ERROR WRAPPING HELPERS (FOR MAYBE EXPRESSIONS)
%%%-----------------------------------------------------------------------------
-doc "Wrap socket send result with send_error tag on failure. Used with maybe expressions to transform socket errors.".
-spec send_with_error(nhttp_sock:t(), iodata()) -> ok | {error, nhttpc_error:any_reason()}.
send_with_error(Socket, Data) ->
    case nhttp_sock:send(Socket, Data) of
        ok -> ok;
        {error, Reason} -> {error, {send_error, Reason}}
    end.

-doc """
Wrap the result of `nhttp_h2:send_headers/4` with the `http2_error` tag on
failure, for `maybe` expressions. The input is the narrow result of a
HEADERS send: a DATA send on a queued codec also answers `{queued, ...}`,
and `nhttpc_stream_send:send_h2_body/5` is the path for those.
""".
-spec wrap_h2_send_result({ok, nhttp_h2:conn(), iodata()} | {error, nhttp_h2:send_error()}) ->
    {ok, nhttp_h2:conn(), iodata()} | {error, nhttpc_error:any_reason()}.
wrap_h2_send_result({ok, _, _} = Ok) -> Ok;
wrap_h2_send_result({error, Reason}) -> {error, {http2_error, Reason}}.

%%%-----------------------------------------------------------------------------
%% H2 RESPONSE EVENT PROCESSING
%%%-----------------------------------------------------------------------------
-type h2_response_acc() :: #{
    response := nhttp_lib:response() | undefined,
    body_acc := [binary()],
    acc_size := non_neg_integer()
}.
-spec check_response_size(non_neg_integer(), pos_integer() | infinity) ->
    ok | nhttp_error:t().
check_response_size(_AccSize, infinity) ->
    ok;
check_response_size(AccSize, MaxSize) when AccSize > MaxSize ->
    nhttp_error:response_too_large(AccSize, MaxSize);
check_response_size(_AccSize, _MaxSize) ->
    ok.

-doc "Create a new empty response accumulator.".
-spec new_response_acc() -> h2_response_acc().
new_response_acc() ->
    #{response => undefined, body_acc => [], acc_size => 0}.

-doc """
Process H2 response events for a single stream.
Returns:
- `{continue, NewAcc}` - need more events, call recv and process again
- `{done, nhttp_lib:response()}` - complete response received
- `{error, nhttpc_error:any_reason()}` - error occurred (stream reset, goaway, size exceeded)
""".
-spec process_h2_response_events(
    [nhttp_h2:event()],
    nhttp_lib:stream_id(),
    h2_response_acc(),
    pos_integer() | infinity
) ->
    {continue, h2_response_acc()}
    | {done, nhttp_lib:response()}
    | {error, nhttpc_error:any_reason()}.
process_h2_response_events([], _StreamId, Acc, _MaxResponseSize) ->
    {continue, Acc};
process_h2_response_events([Event | Rest], StreamId, Acc, MaxResponseSize) ->
    case Event of
        {response, StreamId, Response, fin} ->
            {done, Response#{body => <<>>}};
        {response, StreamId, Response, nofin} ->
            NewAcc = Acc#{response => Response},
            process_h2_response_events(Rest, StreamId, NewAcc, MaxResponseSize);
        {data, StreamId, Data, fin} ->
            DataSize = byte_size(Data),
            #{response := Response, body_acc := BodyAcc, acc_size := AccSize} = Acc,
            NewAccSize = AccSize + DataSize,
            case check_response_size(NewAccSize, MaxResponseSize) of
                ok ->
                    Body = iolist_to_binary(lists:reverse([Data | BodyAcc])),
                    {done, Response#{body => Body}};
                {error, _} = Error ->
                    Error
            end;
        {data, StreamId, Data, nofin} ->
            DataSize = byte_size(Data),
            #{body_acc := BodyAcc, acc_size := AccSize} = Acc,
            NewAccSize = AccSize + DataSize,
            case check_response_size(NewAccSize, MaxResponseSize) of
                ok ->
                    NewAcc = Acc#{body_acc => [Data | BodyAcc], acc_size => NewAccSize},
                    process_h2_response_events(Rest, StreamId, NewAcc, MaxResponseSize);
                {error, _} = Error ->
                    Error
            end;
        {trailers, StreamId, Trailers} ->
            #{response := Response, body_acc := BodyAcc} = Acc,
            Body = iolist_to_binary(lists:reverse(BodyAcc)),
            {done, Response#{body => Body, trailers => Trailers}};
        {stream_reset, StreamId, ErrorCode} ->
            stream_reset_to_error(ErrorCode);
        {goaway, _LastStreamId, ErrorCode, _DebugData} ->
            {error, {http2_error, {goaway, ErrorCode, retryable}}};
        _ ->
            process_h2_response_events(Rest, StreamId, Acc, MaxResponseSize)
    end.

-doc "Convert stream reset error code to appropriate error tuple.".
-spec stream_reset_to_error(atom()) -> {error, nhttpc_error:any_reason()}.
stream_reset_to_error(cancel) ->
    {error, cancelled};
stream_reset_to_error(refused_stream) ->
    {error, {refused, retryable}};
stream_reset_to_error(ErrorCode) ->
    {error, {http2_error, {stream_reset, ErrorCode}}}.

%%%-----------------------------------------------------------------------------
%% RECEIVE-SIDE CREDIT
%%%-----------------------------------------------------------------------------
-doc """
Encode the WINDOW_UPDATE frames of the credits that
`nhttpc_h2_window:consume/4` found due, in order. A stream the codec no
longer tracks yields no frame, because the peer ignores credit on a
closed stream.
""".
-spec window_update_frames([nhttpc_h2_window:credit()], nhttp_h2:conn()) ->
    {ok, nhttp_h2:conn(), [iodata()]} | {error, nhttpc_error:any_reason()}.
window_update_frames(Credits, H2Conn) ->
    window_update_frames(Credits, H2Conn, []).

-spec window_update_frames([nhttpc_h2_window:credit()], nhttp_h2:conn(), [iodata()]) ->
    {ok, nhttp_h2:conn(), [iodata()]} | {error, nhttpc_error:any_reason()}.
window_update_frames([], H2Conn, Frames) ->
    {ok, H2Conn, lists:reverse(Frames)};
window_update_frames([{Target, Increment} | Rest], H2Conn, Frames) ->
    case nhttp_h2:send_window_update(H2Conn, Target, Increment) of
        {ok, H2Conn1, []} -> window_update_frames(Rest, H2Conn1, Frames);
        {ok, H2Conn1, Frame} -> window_update_frames(Rest, H2Conn1, [Frame | Frames]);
        {error, Reason} -> {error, {http2_error, Reason}}
    end.
