-module(nhttpc_stream_recv).

-moduledoc """
Response streaming (download) for nhttpc client.

Extracted from nhttpc.erl to improve module cohesion.
This module handles:
- Streaming GET requests
- Streaming to file
- HTTP/1.1 chunked/content-length streaming
- HTTP/2 DATA frame streaming
""".

-include("nhttpc.hrl").

-export([
    do_stream_on_connection/5,
    do_stream_request/3
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    on_complete_callback/0,
    on_data_callback/0,
    on_headers_callback/0,

    on_progress_callback/0,
    on_trailers_callback/0,
    stream_data_result/0,
    stream_opts/0
]).

-type stream_opts() :: #{
    on_headers => on_headers_callback(),
    on_data => on_data_callback(),
    on_complete => on_complete_callback(),
    on_progress => on_progress_callback(),
    on_trailers => on_trailers_callback(),
    initial_acc => term(),
    timeout => timeout(),
    body_timeout => timeout(),
    max_response_size => pos_integer() | infinity,
    max_header_size => pos_integer() | infinity,
    max_headers_count => pos_integer() | infinity
}.

-type on_headers_callback() ::
    fun(
        (Headers :: nhttp_lib:headers(), Status :: nhttp_lib:status()) -> continue | {stop, term()}
    ).

-type on_data_callback() ::
    fun((Chunk :: binary(), Acc :: term()) -> {continue, term()} | {stop, term(), term()}).

-type on_complete_callback() ::
    fun((Acc :: term()) -> {ok, term()}).

-type on_progress_callback() ::
    fun((Downloaded :: non_neg_integer(), Total :: non_neg_integer() | unknown) -> ok).

-type on_trailers_callback() ::
    fun((Trailers :: nhttp_lib:headers()) -> ok).

-type stream_data_result() ::
    {ok, binary(), more}
    | {ok, binary(), fin}
    | {error, nhttpc_error:any_reason()}.

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(H2_LOCAL_INITIAL_WINDOW, 65535).

%%%-----------------------------------------------------------------------------
%% RECORDS
%%%-----------------------------------------------------------------------------
-record(h2_stream, {
    socket :: nhttp_sock:t(),
    h2_conn :: nhttp_h2:conn(),
    stream_id :: nhttp_lib:stream_id(),
    opts :: stream_opts(),
    timeout :: timeout(),
    window :: nhttpc_h2_window:t(),
    downloaded = 0 :: non_neg_integer(),
    total = unknown :: non_neg_integer() | unknown,
    acc :: term()
}).

%%%-----------------------------------------------------------------------------
%% INTERNAL: STREAMING IMPLEMENTATION
%%%-----------------------------------------------------------------------------
-spec do_stream_on_connection(
    nhttp_sock:t(),
    nhttp_lib:version(),
    nhttp_lib:request(),
    stream_opts(),
    timeout()
) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
do_stream_on_connection(Socket, http1_1, Req, StreamOpts, Timeout) ->
    BodyTimeout = maps:get(body_timeout, StreamOpts, Timeout),
    do_h1_stream_request(Socket, Req, StreamOpts, Timeout, BodyTimeout);
do_stream_on_connection(Socket, http2, Req, StreamOpts, Timeout) ->
    BodyTimeout = maps:get(body_timeout, StreamOpts, Timeout),
    do_h2_stream_request(Socket, Req, StreamOpts, Timeout, BodyTimeout).

-spec do_stream_request(nhttp_lib:request(), nhttpc_url:http_url_info(), stream_opts()) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
do_stream_request(Req, UrlInfo, StreamOpts) ->
    #{scheme := Scheme, host := Host, port := Port} = UrlInfo,
    Transport =
        case Scheme of
            https -> ssl;
            http -> tcp
        end,
    Timeout = maps:get(timeout, StreamOpts, 30000),
    ConnectTimeout = min(5000, Timeout),
    ConnectOpts = nhttpc_request:build_connect_opts(Transport, StreamOpts),
    case nhttp_sock:connect(Host, Port, ConnectOpts, ConnectTimeout) of
        {ok, Socket} ->
            Protocol = nhttpc_request:detect_protocol(Socket),
            ReqWithHost = nhttpc_request:add_host_header(Req, Host, Port, Transport),
            ReqWithEncoding = nhttpc_request:maybe_add_accept_encoding(ReqWithHost, #{}),
            Result = do_stream_on_connection(
                Socket, Protocol, ReqWithEncoding, StreamOpts, Timeout
            ),
            nhttp_sock:close(Socket),
            Result;
        {error, _} = Error ->
            Error
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/1.1 STREAMING
%%%-----------------------------------------------------------------------------
-spec do_h1_stream_request(
    nhttp_sock:t(), nhttp_lib:request(), stream_opts(), timeout(), timeout()
) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
do_h1_stream_request(Socket, Req, StreamOpts, Timeout, BodyTimeout) ->
    case nhttp_h1:encode_request(Req) of
        {ok, IOList} ->
            case nhttp_sock:send(Socket, IOList) of
                ok ->
                    recv_h1_stream_response(Socket, <<>>, StreamOpts, Timeout, BodyTimeout);
                {error, Reason} ->
                    nhttp_error:send_error(Reason)
            end;
        {error, EncodeError} ->
            nhttpc_error:invalid_request(element(1, EncodeError))
    end.

-spec parse_chunk_size(binary()) -> {ok, non_neg_integer(), binary()} | need_more.
parse_chunk_size(Buffer) ->
    case binary:split(Buffer, <<"\r\n">>) of
        [_] ->
            need_more;
        [SizeLine, Rest] ->
            SizePart =
                case binary:split(SizeLine, <<";">>) of
                    [S, _Extensions] -> S;
                    [S] -> S
                end,
            try
                Size = binary_to_integer(SizePart, 16),
                {ok, Size, Rest}
            catch
                error:badarg ->
                    need_more
            end
    end.

-spec parse_h1_stream_headers(
    nhttp_sock:t(),
    binary(),
    stream_opts(),
    timeout(),
    timeout()
) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
parse_h1_stream_headers(Socket, Buffer, StreamOpts, Timeout, BodyTimeout) ->
    case nhttp_h1:parse_response_headers(Buffer, nhttpc_request:h1_parse_opts(StreamOpts)) of
        {ok, Status, Headers, Rest} ->
            case invoke_on_headers(Status, Headers, StreamOpts) of
                continue ->
                    ContentLength = get_content_length(Headers),
                    IsChunked = is_chunked_transfer(Headers),
                    ContentEncoding = nhttpc_request:get_content_encoding(Headers),
                    Total =
                        case ContentLength of
                            undefined -> unknown;
                            N -> N
                        end,
                    MaxResponseSize = get_max_response_size(StreamOpts),
                    case check_size_limit(Total, MaxResponseSize) of
                        ok ->
                            InitialAcc = maps:get(initial_acc, StreamOpts, undefined),
                            stream_h1_body(
                                Socket,
                                Rest,
                                StreamOpts,
                                BodyTimeout,
                                ContentLength,
                                IsChunked,
                                ContentEncoding,
                                0,
                                Total,
                                InitialAcc
                            );
                        {error, _} = Error ->
                            Error
                    end;
                {stop, Reason} ->
                    nhttp_error:stream_stopped(Reason)
            end;
        {more, _} ->
            recv_h1_stream_response(Socket, Buffer, StreamOpts, Timeout, BodyTimeout);
        {error, Reason} ->
            nhttp_error:malformed_response(Reason, Buffer)
    end.

-spec recv_h1_chunk_data(
    nhttp_sock:t(),
    binary(),
    non_neg_integer(),
    stream_opts(),
    timeout(),
    gzip | deflate | identity | {unknown, binary()},
    non_neg_integer(),
    non_neg_integer() | unknown,
    term()
) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
recv_h1_chunk_data(Socket, Buffer, ChunkSize, StreamOpts, Timeout, Enc, Downloaded, Total, Acc) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case byte_size(NewBuffer) >= ChunkSize + 2 of
                true ->
                    <<Chunk:ChunkSize/binary, "\r\n", Rest/binary>> = NewBuffer,
                    DecompressedChunk = maybe_decompress_chunk(Chunk, Enc),
                    NewDownloaded = Downloaded + byte_size(DecompressedChunk),
                    case invoke_on_data(DecompressedChunk, Acc, NewDownloaded, Total, StreamOpts) of
                        {continue, NewAcc} ->
                            stream_h1_chunked(
                                Socket, Rest, StreamOpts, Timeout, Enc, NewDownloaded, Total, NewAcc
                            );
                        {stop, Reason, FinalAcc} ->
                            nhttp_error:stream_stopped(Reason, FinalAcc)
                    end;
                false ->
                    recv_h1_chunk_data(
                        Socket,
                        NewBuffer,
                        ChunkSize,
                        StreamOpts,
                        Timeout,
                        Enc,
                        Downloaded,
                        Total,
                        Acc
                    )
            end;
        {error, timeout} ->
            nhttp_error:body_timeout(Timeout);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

-spec recv_h1_content_length_body(
    nhttp_sock:t(),
    timeout(),
    non_neg_integer(),
    gzip | deflate | identity | {unknown, binary()},
    non_neg_integer(),
    non_neg_integer() | unknown,
    term(),
    stream_opts()
) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
recv_h1_content_length_body(_Socket, _Timeout, 0, _Enc, _Downloaded, _Total, Acc, StreamOpts) ->
    invoke_on_complete(Acc, StreamOpts);
recv_h1_content_length_body(Socket, Timeout, Remaining, Enc, Downloaded, Total, Acc, StreamOpts) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            DataSize = byte_size(Data),
            DecompressedChunk = maybe_decompress_chunk(Data, Enc),
            NewDownloaded = Downloaded + byte_size(DecompressedChunk),
            case invoke_on_data(DecompressedChunk, Acc, NewDownloaded, Total, StreamOpts) of
                {continue, NewAcc} ->
                    NewRemaining = Remaining - DataSize,
                    case NewRemaining =< 0 of
                        true ->
                            invoke_on_complete(NewAcc, StreamOpts);
                        false ->
                            recv_h1_content_length_body(
                                Socket,
                                Timeout,
                                NewRemaining,
                                Enc,
                                NewDownloaded,
                                Total,
                                NewAcc,
                                StreamOpts
                            )
                    end;
                {stop, Reason, FinalAcc} ->
                    nhttp_error:stream_stopped(Reason, FinalAcc)
            end;
        {error, timeout} ->
            nhttp_error:body_timeout(Timeout);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

-spec recv_h1_stream_response(
    nhttp_sock:t(),
    binary(),
    stream_opts(),
    timeout(),
    timeout()
) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
recv_h1_stream_response(Socket, Buffer, StreamOpts, Timeout, BodyTimeout) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            Combined = <<Buffer/binary, Data/binary>>,
            parse_h1_stream_headers(Socket, Combined, StreamOpts, Timeout, BodyTimeout);
        {error, timeout} ->
            nhttp_error:request_timeout(Timeout);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

-spec stream_h1_body(
    nhttp_sock:t(),
    binary(),
    stream_opts(),
    timeout(),
    non_neg_integer() | undefined,
    boolean(),
    gzip | deflate | identity | {unknown, binary()},
    non_neg_integer(),
    non_neg_integer() | unknown,
    term()
) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
stream_h1_body(_Socket, _Buffer, StreamOpts, _Timeout, 0, false, _Enc, _Downloaded, _Total, Acc) ->
    invoke_on_complete(Acc, StreamOpts);
stream_h1_body(
    _Socket, Buffer, StreamOpts, _Timeout, ContentLength, false, Enc, Downloaded, Total, Acc
) when
    is_integer(ContentLength), byte_size(Buffer) >= ContentLength
->
    <<Body:ContentLength/binary, _Rest/binary>> = Buffer,
    DecompressedBody = maybe_decompress_chunk(Body, Enc),
    NewDownloaded = Downloaded + byte_size(DecompressedBody),
    case invoke_on_data(DecompressedBody, Acc, NewDownloaded, Total, StreamOpts) of
        {continue, NewAcc} ->
            invoke_on_complete(NewAcc, StreamOpts);
        {stop, Reason, FinalAcc} ->
            nhttp_error:stream_stopped(Reason, FinalAcc)
    end;
stream_h1_body(
    Socket, Buffer, StreamOpts, Timeout, ContentLength, false, Enc, Downloaded, Total, Acc
) when
    is_integer(ContentLength)
->
    Remaining = ContentLength - byte_size(Buffer),
    case Buffer of
        <<>> ->
            recv_h1_content_length_body(
                Socket, Timeout, Remaining, Enc, Downloaded, Total, Acc, StreamOpts
            );
        _ ->
            DecompressedChunk = maybe_decompress_chunk(Buffer, Enc),
            NewDownloaded = Downloaded + byte_size(DecompressedChunk),
            case invoke_on_data(DecompressedChunk, Acc, NewDownloaded, Total, StreamOpts) of
                {continue, NewAcc} ->
                    recv_h1_content_length_body(
                        Socket,
                        Timeout,
                        Remaining,
                        Enc,
                        NewDownloaded,
                        Total,
                        NewAcc,
                        StreamOpts
                    );
                {stop, Reason, FinalAcc} ->
                    nhttp_error:stream_stopped(Reason, FinalAcc)
            end
    end;
stream_h1_body(Socket, Buffer, StreamOpts, Timeout, undefined, true, Enc, Downloaded, Total, Acc) ->
    stream_h1_chunked(Socket, Buffer, StreamOpts, Timeout, Enc, Downloaded, Total, Acc);
stream_h1_body(
    _Socket, Buffer, StreamOpts, _Timeout, undefined, false, Enc, Downloaded, Total, Acc
) ->
    DecompressedBody = maybe_decompress_chunk(Buffer, Enc),
    NewDownloaded = Downloaded + byte_size(DecompressedBody),
    case invoke_on_data(DecompressedBody, Acc, NewDownloaded, Total, StreamOpts) of
        {continue, NewAcc} ->
            invoke_on_complete(NewAcc, StreamOpts);
        {stop, Reason, FinalAcc} ->
            nhttp_error:stream_stopped(Reason, FinalAcc)
    end.

-spec stream_h1_chunked(
    nhttp_sock:t(),
    binary(),
    stream_opts(),
    timeout(),
    gzip | deflate | identity | {unknown, binary()},
    non_neg_integer(),
    non_neg_integer() | unknown,
    term()
) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
stream_h1_chunked(Socket, Buffer, StreamOpts, Timeout, Enc, Downloaded, Total, Acc) ->
    case parse_chunk_size(Buffer) of
        {ok, 0, _Rest} ->
            invoke_on_complete(Acc, StreamOpts);
        {ok, ChunkSize, Rest} when byte_size(Rest) >= ChunkSize + 2 ->
            <<Chunk:ChunkSize/binary, "\r\n", NewBuffer/binary>> = Rest,
            DecompressedChunk = maybe_decompress_chunk(Chunk, Enc),
            NewDownloaded = Downloaded + byte_size(DecompressedChunk),
            case invoke_on_data(DecompressedChunk, Acc, NewDownloaded, Total, StreamOpts) of
                {continue, NewAcc} ->
                    stream_h1_chunked(
                        Socket, NewBuffer, StreamOpts, Timeout, Enc, NewDownloaded, Total, NewAcc
                    );
                {stop, Reason, FinalAcc} ->
                    nhttp_error:stream_stopped(Reason, FinalAcc)
            end;
        {ok, ChunkSize, Rest} ->
            recv_h1_chunk_data(
                Socket, Rest, ChunkSize, StreamOpts, Timeout, Enc, Downloaded, Total, Acc
            );
        need_more ->
            case nhttp_sock:recv(Socket, 0, Timeout) of
                {ok, Data} ->
                    stream_h1_chunked(
                        Socket,
                        <<Buffer/binary, Data/binary>>,
                        StreamOpts,
                        Timeout,
                        Enc,
                        Downloaded,
                        Total,
                        Acc
                    );
                {error, timeout} ->
                    nhttp_error:body_timeout(Timeout);
                {error, Reason} ->
                    nhttp_error:recv_error(Reason)
            end
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/2 STREAMING
%%%-----------------------------------------------------------------------------
-spec continue_h2_stream(nhttp_h2:fin(), [nhttp_h2:event()], #h2_stream{}) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
continue_h2_stream(fin, _Rest, #h2_stream{acc = Acc, opts = StreamOpts}) ->
    invoke_on_complete(Acc, StreamOpts);
continue_h2_stream(nofin, Rest, Stream) ->
    process_h2_stream_events(Rest, Stream).

-spec do_h2_stream_request(
    nhttp_sock:t(), nhttp_lib:request(), stream_opts(), timeout(), timeout()
) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
do_h2_stream_request(Socket, Req, StreamOpts, Timeout, BodyTimeout) ->
    H2Conn0 = nhttp_h2:new(client, nhttpc_h2:codec_settings()),
    Preface = nhttp_h2:preface(H2Conn0),
    Headers = nhttpc_h2:request_to_h2_headers(Req),
    Body = maps:get(body, Req, <<>>),
    EndStream =
        case iolist_size(Body) of
            0 -> fin;
            _Size -> nofin
        end,
    maybe
        ok ?= nhttpc_h2:send_with_error(Socket, Preface),
        {ok, H2Conn1} ?= nhttpc_request:recv_h2_settings(Socket, H2Conn0, Timeout),
        {ok, StreamId, H2Conn2} = nhttp_h2:open_stream(H2Conn1),
        {ok, H2Conn3, HeaderFrame} ?=
            nhttpc_h2:wrap_h2_send_result(
                nhttp_h2:send_headers(H2Conn2, StreamId, Headers, EndStream)
            ),
        ok ?= nhttpc_h2:send_with_error(Socket, HeaderFrame),
        {ok, H2Conn4, Events} ?=
            nhttpc_stream_send:send_h2_body(Socket, H2Conn3, StreamId, Body, Timeout),
        Stream = #h2_stream{
            socket = Socket,
            h2_conn = H2Conn4,
            stream_id = StreamId,
            opts = StreamOpts,
            timeout = BodyTimeout,
            window = nhttpc_h2_window:new(?H2_LOCAL_INITIAL_WINDOW),
            acc = maps:get(initial_acc, StreamOpts, undefined)
        },
        process_h2_stream_events(Events, Stream)
    end.

-spec process_h2_stream_events([nhttp_h2:event()], #h2_stream{}) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
process_h2_stream_events([], Stream) ->
    recv_h2_stream_loop(Stream);
process_h2_stream_events(
    [Event | Rest], #h2_stream{stream_id = StreamId, opts = StreamOpts, acc = Acc} = Stream
) ->
    case Event of
        {response, StreamId, #{status := Status, headers := Headers}, fin} ->
            case invoke_on_headers(Status, Headers, StreamOpts) of
                continue ->
                    invoke_on_complete(Acc, StreamOpts);
                {stop, Reason} ->
                    nhttp_error:stream_stopped(Reason)
            end;
        {response, StreamId, #{status := Status, headers := Headers}, nofin} ->
            Total =
                case get_content_length(Headers) of
                    undefined -> unknown;
                    N -> N
                end,
            case invoke_on_headers(Status, Headers, StreamOpts) of
                continue ->
                    process_h2_stream_events(Rest, Stream#h2_stream{total = Total});
                {stop, Reason} ->
                    nhttp_error:stream_stopped(Reason)
            end;
        {trailers, StreamId, Trailers} ->
            invoke_on_trailers(Trailers, StreamOpts),
            invoke_on_complete(Acc, StreamOpts);
        {data, StreamId, BodyData, Fin} ->
            Downloaded = Stream#h2_stream.downloaded + byte_size(BodyData),
            Total = Stream#h2_stream.total,
            case invoke_on_data(BodyData, Acc, Downloaded, Total, StreamOpts) of
                {continue, NewAcc} ->
                    Stream1 = Stream#h2_stream{downloaded = Downloaded, acc = NewAcc},
                    maybe
                        {ok, Stream2} ?= credit_peer(byte_size(BodyData), Fin, Stream1),
                        continue_h2_stream(Fin, Rest, Stream2)
                    end;
                {stop, Reason, FinalAcc} ->
                    nhttp_error:stream_stopped(Reason, FinalAcc)
            end;
        {stream_reset, StreamId, ErrorCode} ->
            nhttp_error:stream_reset(ErrorCode);
        {goaway, _LastStreamId, ErrorCode, _DebugData} ->
            nhttp_error:goaway(ErrorCode);
        _ ->
            process_h2_stream_events(Rest, Stream)
    end.

-spec recv_h2_stream_loop(#h2_stream{}) -> {ok, term()} | {error, nhttpc_error:any_reason()}.
recv_h2_stream_loop(#h2_stream{socket = Socket, h2_conn = H2Conn, timeout = Timeout} = Stream) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            case nhttp_h2:recv(H2Conn, Data) of
                {ok, Events, NewH2Conn} ->
                    process_h2_stream_events(Events, Stream#h2_stream{h2_conn = NewH2Conn});
                {ok, Events, NewH2Conn, ToSend} ->
                    case nhttp_sock:send(Socket, ToSend) of
                        ok ->
                            process_h2_stream_events(
                                Events, Stream#h2_stream{h2_conn = NewH2Conn}
                            );
                        {error, Reason} ->
                            nhttp_error:send_error(Reason)
                    end;
                {error, Reason} ->
                    {error, {http2_error, Reason}}
            end;
        {error, timeout} ->
            nhttp_error:body_timeout(Timeout);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/2 FLOW CONTROL HELPERS
%%%-----------------------------------------------------------------------------
-doc """
Hand the peer the credit due after `Octets` of DATA payload.
`nhttpc_h2_window` decides which WINDOW_UPDATE frames are due, the same
policy the pooled connection applies, so one peer sees one client.
""".
-spec credit_peer(non_neg_integer(), nhttp_h2:fin(), #h2_stream{}) ->
    {ok, #h2_stream{}} | {error, nhttpc_error:any_reason()}.
credit_peer(Octets, Fin, #h2_stream{stream_id = StreamId, window = Window} = Stream) ->
    {Credits, Window1} = nhttpc_h2_window:consume(StreamId, Octets, Fin, Window),
    maybe
        {ok, H2Conn, Frames} ?= nhttpc_h2:window_update_frames(Credits, Stream#h2_stream.h2_conn),
        ok ?= send_window_updates(Stream#h2_stream.socket, Frames),
        {ok, Stream#h2_stream{window = Window1, h2_conn = H2Conn}}
    end.

-spec send_window_updates(nhttp_sock:t(), [iodata()]) -> ok | {error, nhttpc_error:any_reason()}.
send_window_updates(_Socket, []) ->
    ok;
send_window_updates(Socket, Frames) ->
    case nhttp_sock:send(Socket, Frames) of
        ok -> ok;
        {error, Reason} -> nhttp_error:send_error(Reason)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: STREAMING HELPERS
%%%-----------------------------------------------------------------------------
-spec get_content_length(nhttp_lib:headers()) -> non_neg_integer() | undefined.
get_content_length(Headers) ->
    case nhttpc_request:get_header_value(<<"content-length">>, Headers) of
        undefined ->
            undefined;
        Value ->
            try
                binary_to_integer(Value)
            catch
                error:badarg -> undefined
            end
    end.

-spec invoke_on_complete(term(), stream_opts()) ->
    {ok, term()} | {error, nhttpc_error:any_reason()}.
invoke_on_complete(Acc, StreamOpts) ->
    case maps:get(on_complete, StreamOpts, undefined) of
        undefined ->
            {ok, Acc};
        Callback when is_function(Callback, 1) ->
            Callback(Acc)
    end.

-spec invoke_on_data(
    binary(), term(), non_neg_integer(), non_neg_integer() | unknown, stream_opts()
) ->
    {continue, term()} | {stop, term(), term()}.
invoke_on_data(Chunk, Acc, Downloaded, Total, StreamOpts) ->
    MaxResponseSize = get_max_response_size(StreamOpts),
    case check_size_limit(Downloaded, MaxResponseSize) of
        {error, {request, #{type := response_too_large, size := Size, max_size := MaxSize}}} ->
            {stop, {response_too_large, Size, MaxSize}, Acc};
        ok ->
            case maps:get(on_progress, StreamOpts, undefined) of
                undefined ->
                    ok;
                ProgressCb when is_function(ProgressCb, 2) ->
                    ProgressCb(Downloaded, Total)
            end,
            case maps:get(on_data, StreamOpts, undefined) of
                undefined ->
                    NewAcc =
                        case Acc of
                            undefined -> Chunk;
                            Bin when is_binary(Bin) -> <<Bin/binary, Chunk/binary>>;
                            _ -> Chunk
                        end,
                    {continue, NewAcc};
                Callback when is_function(Callback, 2) ->
                    Callback(Chunk, Acc)
            end
    end.

-spec invoke_on_headers(nhttp_lib:status(), nhttp_lib:headers(), stream_opts()) ->
    continue | {stop, term()}.
invoke_on_headers(Status, Headers, StreamOpts) ->
    case maps:get(on_headers, StreamOpts, undefined) of
        undefined ->
            continue;
        Callback when is_function(Callback, 2) ->
            Callback(Headers, Status)
    end.

-spec invoke_on_trailers(nhttp_lib:headers(), stream_opts()) -> ok.
invoke_on_trailers(Trailers, StreamOpts) ->
    case maps:get(on_trailers, StreamOpts, undefined) of
        undefined ->
            ok;
        Callback when is_function(Callback, 1) ->
            _ = Callback(Trailers),
            ok
    end.

-spec is_chunked_transfer(nhttp_lib:headers()) -> boolean().
is_chunked_transfer(Headers) ->
    case nhttpc_request:get_header_value(<<"transfer-encoding">>, Headers) of
        undefined -> false;
        Value -> nhttp_headers:to_lower(Value) =:= <<"chunked">>
    end.

-spec maybe_decompress_chunk(binary(), gzip | deflate | identity | {unknown, binary()}) -> binary().
maybe_decompress_chunk(Chunk, identity) ->
    Chunk;
maybe_decompress_chunk(Chunk, {unknown, _}) ->
    Chunk;
maybe_decompress_chunk(Chunk, gzip) ->
    case nhttp_compress:decompress(Chunk, gzip) of
        {ok, Decompressed} -> Decompressed;
        {error, _} -> Chunk
    end;
maybe_decompress_chunk(Chunk, deflate) ->
    case nhttp_compress:decompress(Chunk, deflate) of
        {ok, Decompressed} -> Decompressed;
        {error, _} -> Chunk
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: SIZE LIMIT HELPERS
%%%-----------------------------------------------------------------------------
-doc """
Check if size exceeds the limit.
Returns ok if within limit, or error if exceeded.
""".
-spec check_size_limit(non_neg_integer() | unknown, pos_integer() | infinity) ->
    ok | nhttp_error:t().
check_size_limit(_, infinity) ->
    ok;
check_size_limit(unknown, _MaxSize) ->
    ok;
check_size_limit(Size, MaxSize) when Size > MaxSize ->
    nhttp_error:response_too_large(Size, MaxSize);
check_size_limit(_Size, _MaxSize) ->
    ok.

-doc "Get max_response_size from stream options.".
-spec get_max_response_size(stream_opts()) -> pos_integer() | infinity.
get_max_response_size(StreamOpts) ->
    maps:get(max_response_size, StreamOpts, ?DEFAULT_MAX_RESPONSE_SIZE).
