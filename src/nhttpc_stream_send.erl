-module(nhttpc_stream_send).

-moduledoc """
Request streaming (upload) for nhttpc client.

Extracted from nhttpc.erl to improve module cohesion.
This module handles:
- Streaming POST/PUT requests with large bodies
- File upload streaming
- HTTP/1.1 chunked upload
- HTTP/2 DATA frame upload streaming through the codec send queue

On HTTP/2 the body goes through the send queue of `nhttp_h2`. A chunk
that outruns the peer's credit waits in the codec, bounded by
`nhttpc_h2:max_send_buffer/0`, and the uploader reads the socket while
it waits, so that the WINDOW_UPDATE frames that free the queue are
seen. Progress reports the octets written to the socket.
""".

-include("nhttpc.hrl").

-export([
    do_h3_streaming_upload/4,
    do_streaming_upload/5,
    send_h2_body/5
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    body/0,

    on_upload_progress_callback/0,
    stream_body_opts/0,
    stream_chunk/0,
    stream_iterator/0,
    stream_source/0
]).

-type body() ::
    iodata()
    | {stream, stream_source()}
    | {stream, stream_source(), stream_body_opts()}.

-type stream_source() ::
    {file, file:filename()}
    | stream_iterator().

-type stream_iterator() ::
    fun(() -> stream_chunk()).

-type stream_chunk() ::
    {ok, iodata(), stream_iterator()}
    | fin
    | {error, nhttpc_error:any_reason()}.

-doc """
Options of a streaming body. `size` is the body length for the
`content-length` header. `chunk_size` is the read size of a file
source, 65536 octets by default. It is not a frame size: the HTTP/2
codec splits every chunk at the peer's SETTINGS_MAX_FRAME_SIZE and
holds what the send windows cannot take yet.
""".
-type stream_body_opts() :: #{
    size => pos_integer(),
    chunk_size => pos_integer()
}.

-type on_upload_progress_callback() ::
    fun((Sent :: non_neg_integer(), Total :: non_neg_integer() | unknown) -> ok).

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(DEFAULT_CHUNK_SIZE, 65536).

%%%-----------------------------------------------------------------------------
%% RECORDS
%%%-----------------------------------------------------------------------------
-record(h2_upload, {
    socket :: nhttp_sock:t(),
    h2_conn :: nhttp_h2:conn(),
    stream_id :: nhttp_lib:stream_id(),
    timeout :: timeout(),
    slice :: pos_integer(),
    progress_cb :: on_upload_progress_callback() | undefined,
    sent = 0 :: non_neg_integer(),
    total :: non_neg_integer() | unknown,
    events = [] :: [nhttp_h2:event()],
    open = true :: boolean()
}).

%%%-----------------------------------------------------------------------------
%% INTERNAL: STREAMING UPLOAD EXECUTION
%%%-----------------------------------------------------------------------------
-doc "Execute a streaming upload request.".
-spec do_streaming_upload(
    nhttp_sock:t(), http1_1 | http2, nhttp_lib:request(), nhttpc:opts(), timeout()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_streaming_upload(Socket, Protocol, #{body := StreamBody} = Req, Opts, Timeout) ->
    case get_stream_source(StreamBody) of
        {ok, Source, StreamOpts} ->
            case prepare_stream_source(Source, StreamOpts) of
                {ok, Iterator, Size} ->
                    case Protocol of
                        http1_1 ->
                            do_h1_streaming_upload(Socket, Req, Iterator, Size, Opts, Timeout);
                        http2 ->
                            do_h2_streaming_upload(Socket, Req, Iterator, Size, Opts, Timeout)
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, not_streaming} ->
            nhttp_error:upload_error(invalid_stream_body)
    end.

-spec file_iterator(file:filename(), pos_integer()) ->
    {ok, stream_iterator()} | {error, nhttpc_error:any_reason()}.
file_iterator(FilePath, ChunkSize) ->
    case file:open(FilePath, [read, binary, raw]) of
        {ok, Fd} ->
            {ok, make_file_iterator(Fd, ChunkSize)};
        {error, Reason} ->
            nhttp_error:file_error(open, FilePath, Reason)
    end.

-doc "Get streaming source and options from body.".
-spec get_stream_source(body()) ->
    {ok, stream_source(), stream_body_opts()} | {error, not_streaming}.
get_stream_source({stream, Source}) ->
    {ok, Source, #{}};
get_stream_source({stream, Source, Opts}) ->
    {ok, Source, Opts};
get_stream_source(_) ->
    {error, not_streaming}.

-spec make_file_iterator(file:io_device(), pos_integer()) -> stream_iterator().
make_file_iterator(Fd, ChunkSize) ->
    fun() ->
        case file:read(Fd, ChunkSize) of
            {ok, Data} ->
                {ok, Data, make_file_iterator(Fd, ChunkSize)};
            eof ->
                file:close(Fd),
                fin;
            {error, Reason} ->
                file:close(Fd),
                nhttp_error:file_error(read, Reason)
        end
    end.

-doc "Convert file source to iterator if needed.".
-spec prepare_stream_source(stream_source(), stream_body_opts()) ->
    {ok, stream_iterator(), non_neg_integer() | unknown} | {error, nhttpc_error:any_reason()}.
prepare_stream_source({file, FilePath}, Opts) ->
    ChunkSize = maps:get(chunk_size, Opts, ?DEFAULT_CHUNK_SIZE),
    case maps:get(size, Opts, undefined) of
        undefined ->
            case file:read_file_info(FilePath) of
                {ok, FileInfo} ->
                    Size = element(2, FileInfo),
                    case file_iterator(FilePath, ChunkSize) of
                        {ok, Iterator} -> {ok, Iterator, Size};
                        {error, _} = Error -> Error
                    end;
                {error, Reason} ->
                    nhttp_error:file_error(read_file_info, Reason)
            end;
        Size when is_integer(Size) ->
            case file_iterator(FilePath, ChunkSize) of
                {ok, Iterator} -> {ok, Iterator, Size};
                {error, _} = Error -> Error
            end
    end;
prepare_stream_source(Iterator, Opts) when is_function(Iterator, 0) ->
    Size = maps:get(size, Opts, unknown),
    {ok, Iterator, Size}.

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/1.1 STREAMING UPLOAD
%%%-----------------------------------------------------------------------------
-doc """
Execute HTTP/1.1 streaming upload.
Uses Content-Length if size is known, otherwise uses chunked transfer encoding.
""".
-spec do_h1_streaming_upload(
    nhttp_sock:t(),
    nhttp_lib:request(),
    stream_iterator(),
    non_neg_integer() | unknown,
    nhttpc:opts(),
    timeout()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_h1_streaming_upload(Socket, Req, Iterator, Size, Opts, Timeout) ->
    #{method := Method, path := Path, headers := Headers} = Req,
    {FinalHeaders, UseChunked} =
        case Size of
            unknown ->
                ChunkedHeaders = add_header_if_missing(
                    <<"transfer-encoding">>, <<"chunked">>, Headers
                ),
                {ChunkedHeaders, true};
            N when is_integer(N) ->
                CLHeaders = add_header_if_missing(
                    <<"content-length">>, integer_to_binary(N), Headers
                ),
                {CLHeaders, false}
        end,
    RequestLine = [
        nhttp_lib:encode_method(Method),
        <<" ">>,
        Path,
        <<" HTTP/1.1\r\n">>,
        encode_headers_iolist(FinalHeaders),
        <<"\r\n">>
    ],
    case nhttp_sock:send(Socket, RequestLine) of
        ok ->
            ProgressCb = maps:get(on_upload_progress, Opts, undefined),
            case stream_h1_upload_body(Socket, Iterator, UseChunked, ProgressCb, 0, Size) of
                ok ->
                    recv_h1_response(
                        Socket, <<>>, Timeout, nhttpc_request:h1_parse_opts(Opts)
                    );
                {error, _} = Error ->
                    Error
            end;
        {error, Reason} ->
            nhttp_error:send_error(Reason)
    end.

-spec recv_h1_response(nhttp_sock:t(), binary(), timeout(), nhttp_h1:opts()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
recv_h1_response(Socket, Buffer, Timeout, ParseOpts) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            Combined = <<Buffer/binary, Data/binary>>,
            case nhttp_h1:parse_response(Combined, ParseOpts) of
                {ok, Resp, _Consumed} ->
                    {ok, Resp};
                {more, _} ->
                    recv_h1_response(Socket, Combined, Timeout, ParseOpts);
                {error, Reason} ->
                    nhttp_error:malformed_response(Reason, <<>>)
            end;
        {error, timeout} ->
            nhttp_error:request_timeout(Timeout);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

-doc "Stream body chunks for HTTP/1.1.".
-spec stream_h1_upload_body(
    nhttp_sock:t(),
    stream_iterator(),
    boolean(),
    on_upload_progress_callback() | undefined,
    non_neg_integer(),
    non_neg_integer() | unknown
) ->
    ok | {error, nhttpc_error:any_reason()}.
stream_h1_upload_body(Socket, Iterator, UseChunked, ProgressCb, Sent, Total) ->
    case Iterator() of
        {ok, Chunk, NextIterator} ->
            ChunkData =
                case UseChunked of
                    true ->
                        ChunkSize = iolist_size(Chunk),
                        [integer_to_binary(ChunkSize, 16), <<"\r\n">>, Chunk, <<"\r\n">>];
                    false ->
                        Chunk
                end,
            case nhttp_sock:send(Socket, ChunkData) of
                ok ->
                    ChunkBytes = iolist_size(Chunk),
                    NewSent = Sent + ChunkBytes,
                    maybe_call_progress(ProgressCb, NewSent, Total),
                    stream_h1_upload_body(
                        Socket, NextIterator, UseChunked, ProgressCb, NewSent, Total
                    );
                {error, Reason} ->
                    nhttp_error:send_error(Reason)
            end;
        fin ->
            case UseChunked of
                true ->
                    nhttp_sock:send(Socket, <<"0\r\n\r\n">>);
                false ->
                    ok
            end;
        {error, Reason} ->
            nhttp_error:upload_error(Reason)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/2 STREAMING UPLOAD
%%%-----------------------------------------------------------------------------
-doc """
Execute an HTTP/2 streaming upload: the HEADERS frame, then the body
through the codec send queue, then the response.
""".
-spec do_h2_streaming_upload(
    nhttp_sock:t(),
    nhttp_lib:request(),
    stream_iterator(),
    non_neg_integer() | unknown,
    nhttpc:opts(),
    timeout()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_h2_streaming_upload(Socket, Req, Iterator, Size, Opts, Timeout) ->
    H2Conn0 = nhttp_h2:new(client, nhttpc_h2:codec_settings()),
    Preface = nhttp_h2:preface(H2Conn0),
    Headers = request_to_h2_headers_with_size(Req, Size),
    ProgressCb = maps:get(on_upload_progress, Opts, undefined),
    maybe
        ok ?= nhttpc_h2:send_with_error(Socket, Preface),
        {ok, H2Conn1} ?= nhttpc_request:recv_h2_settings(Socket, H2Conn0, Timeout),
        {ok, StreamId, H2Conn2} = nhttp_h2:open_stream(H2Conn1),
        {ok, H2Conn3, HeaderFrame} ?=
            nhttpc_h2:wrap_h2_send_result(
                nhttp_h2:send_headers(H2Conn2, StreamId, Headers, nofin)
            ),
        ok ?= nhttpc_h2:send_with_error(Socket, HeaderFrame),
        Upload = new_h2_upload(Socket, H2Conn3, StreamId, Timeout, ProgressCb, Size),
        {ok, FinalH2Conn, Events} ?= stream_h2_upload_data(Iterator, Upload),
        recv_h2_response(Socket, FinalH2Conn, StreamId, Timeout, Events)
    end.

-spec handle_h2_response_events(
    nhttp_sock:t(),
    nhttp_h2:conn(),
    nhttp_lib:stream_id(),
    timeout(),
    nhttpc_h2:h2_response_acc(),
    [nhttp_h2:event()]
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
handle_h2_response_events(Socket, H2Conn, StreamId, Timeout, Acc, Events) ->
    case nhttpc_h2:process_h2_response_events(Events, StreamId, Acc, infinity) of
        {continue, NewAcc} ->
            recv_h2_response_loop(Socket, H2Conn, StreamId, Timeout, NewAcc);
        {done, Resp} ->
            {ok, Resp};
        {error, _} = Error ->
            Error
    end.

-doc """
Receive the HTTP/2 response after the upload. `Events` are the response
events that reached the codec while the body was going out.
""".
-spec recv_h2_response(
    nhttp_sock:t(), nhttp_h2:conn(), nhttp_lib:stream_id(), timeout(), [nhttp_h2:event()]
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
recv_h2_response(Socket, H2Conn, StreamId, Timeout, Events) ->
    Acc = nhttpc_h2:new_response_acc(),
    handle_h2_response_events(Socket, H2Conn, StreamId, Timeout, Acc, Events).

-spec recv_h2_response_loop(
    nhttp_sock:t(),
    nhttp_h2:conn(),
    nhttp_lib:stream_id(),
    timeout(),
    nhttpc_h2:h2_response_acc()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
recv_h2_response_loop(Socket, H2Conn, StreamId, Timeout, Acc) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            case nhttp_h2:recv(H2Conn, Data) of
                {ok, Events, NewH2Conn} ->
                    handle_h2_response_events(Socket, NewH2Conn, StreamId, Timeout, Acc, Events);
                {ok, Events, NewH2Conn, ToSend} ->
                    case nhttp_sock:send(Socket, ToSend) of
                        ok ->
                            handle_h2_response_events(
                                Socket, NewH2Conn, StreamId, Timeout, Acc, Events
                            );
                        {error, Reason} ->
                            nhttp_error:send_error(Reason)
                    end;
                {error, Reason} ->
                    {error, {http2_error, Reason}}
            end;
        {error, timeout} ->
            nhttp_error:request_timeout(Timeout);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

-doc "Build HTTP/2 headers with optional content-length.".
-spec request_to_h2_headers_with_size(nhttp_lib:request(), non_neg_integer() | unknown) ->
    nhttp_lib:headers().
request_to_h2_headers_with_size(Req, Size) ->
    BaseHeaders = nhttpc_h2:request_to_h2_headers(Req),
    case Size of
        unknown ->
            BaseHeaders;
        N when is_integer(N) ->
            case nhttp_headers:has(<<"content-length">>, BaseHeaders) of
                true ->
                    BaseHeaders;
                false ->
                    BaseHeaders ++ [{<<"content-length">>, integer_to_binary(N)}]
            end
    end.

-doc """
Stream the DATA frames of an iterator body. Each chunk is offered to the
codec as it is pulled, and the last frame is an empty one with
END_STREAM. The iterator is left alone once the peer ends the stream.
""".
-spec stream_h2_upload_data(stream_iterator(), #h2_upload{}) ->
    {ok, nhttp_h2:conn(), [nhttp_h2:event()]} | {error, nhttpc_error:any_reason()}.
stream_h2_upload_data(_Iterator, #h2_upload{open = false} = Upload) ->
    drain_h2_queue(Upload);
stream_h2_upload_data(Iterator, Upload) ->
    case Iterator() of
        {ok, Chunk, NextIterator} ->
            maybe
                {ok, Upload1} ?= offer_h2_data(Chunk, iolist_size(Chunk), nofin, Upload),
                stream_h2_upload_data(NextIterator, Upload1)
            end;
        fin ->
            maybe
                {ok, Upload1} ?= offer_h2_data(<<>>, 0, fin, Upload),
                drain_h2_queue(Upload1)
            end;
        {error, Reason} ->
            nhttp_error:upload_error(Reason)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/2 SEND QUEUE
%%%-----------------------------------------------------------------------------
-spec abort_h2_upload(#h2_upload{}) -> {error, nhttpc_error:any_reason()}.
abort_h2_upload(#h2_upload{socket = Socket, h2_conn = H2Conn, stream_id = StreamId}) ->
    {ok, _H2Conn1, RstFrame} = nhttp_h2:send_rst_stream(H2Conn, StreamId, cancel),
    case nhttp_sock:send(Socket, RstFrame) of
        ok -> nhttp_error:stream_refused();
        {error, _Reason} -> nhttp_error:stream_refused()
    end.

-doc """
Block on the socket for the peer's next frames, hand them to the codec
and write what it drained. A sender that stops reading while it waits
for credit never sees the WINDOW_UPDATE it waits for (RFC 9113 Section
5.2.2). A silent peer is a timeout with octets still queued: the stream
is reset, so the peer discards a request it never processed, and the
caller gets the retryable refusal. A write error on the RST_STREAM
changes nothing: the caller closes the socket after the error.
""".
-spec await_h2_credit(#h2_upload{}) -> {ok, #h2_upload{}} | {error, nhttpc_error:any_reason()}.
await_h2_credit(#h2_upload{socket = Socket, h2_conn = H2Conn, timeout = Timeout} = Upload) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            case nhttp_h2:recv(H2Conn, Data) of
                {ok, Events, H2Conn1} ->
                    {ok, note_h2_upload_events(Events, Upload#h2_upload{h2_conn = H2Conn1})};
                {ok, Events, H2Conn1, ToSend} ->
                    case nhttp_sock:send(Socket, ToSend) of
                        ok ->
                            {ok,
                                note_h2_upload_events(Events, Upload#h2_upload{h2_conn = H2Conn1})};
                        {error, Reason} ->
                            nhttp_error:send_error(Reason)
                    end;
                {error, Reason} ->
                    {error, {http2_error, Reason}}
            end;
        {error, timeout} ->
            abort_h2_upload(Upload);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

-doc """
Read the socket until the codec holds no octets of the stream. The
peer's credit drains the queue, and END_STREAM rides the last frame.
""".
-spec drain_h2_queue(#h2_upload{}) ->
    {ok, nhttp_h2:conn(), [nhttp_h2:event()]} | {error, nhttpc_error:any_reason()}.
drain_h2_queue(#h2_upload{open = false} = Upload) ->
    finish_h2_upload(Upload);
drain_h2_queue(#h2_upload{h2_conn = H2Conn, stream_id = StreamId} = Upload) ->
    case nhttp_h2:send_buffer_bytes(H2Conn, StreamId) of
        0 ->
            finish_h2_upload(Upload);
        _Held ->
            maybe
                {ok, Upload1} ?= await_h2_credit(Upload),
                drain_h2_queue(Upload1)
            end
    end.

-spec finish_h2_upload(#h2_upload{}) -> {ok, nhttp_h2:conn(), [nhttp_h2:event()]}.
finish_h2_upload(#h2_upload{h2_conn = H2Conn, events = Events}) ->
    {ok, H2Conn, lists:reverse(Events)}.

-spec keep_h2_event(nhttp_h2:event(), nhttp_h2:fin(), #h2_upload{}) -> #h2_upload{}.
keep_h2_event(Event, nofin, #h2_upload{events = Events} = Upload) ->
    Upload#h2_upload{events = [Event | Events]};
keep_h2_event(Event, fin, #h2_upload{events = Events} = Upload) ->
    Upload#h2_upload{events = [Event | Events], open = false}.

-spec new_h2_upload(
    nhttp_sock:t(),
    nhttp_h2:conn(),
    nhttp_lib:stream_id(),
    timeout(),
    on_upload_progress_callback() | undefined,
    non_neg_integer() | unknown
) -> #h2_upload{}.
new_h2_upload(Socket, H2Conn, StreamId, Timeout, ProgressCb, Total) ->
    #h2_upload{
        socket = Socket,
        h2_conn = H2Conn,
        stream_id = StreamId,
        timeout = Timeout,
        slice = nhttpc_h2:max_send_buffer(),
        progress_cb = ProgressCb,
        total = Total
    }.

-spec note_h2_sent(non_neg_integer(), #h2_upload{}) -> #h2_upload{}.
note_h2_sent(0, Upload) ->
    Upload;
note_h2_sent(Bytes, #h2_upload{sent = Sent, total = Total, progress_cb = ProgressCb} = Upload) ->
    NewSent = Sent + Bytes,
    maybe_call_progress(ProgressCb, NewSent, Total),
    Upload#h2_upload{sent = NewSent}.

-doc """
Account for one event that arrived while the body was going out. The
octets the queue emitted count as sent. Flow-control and connection
events are the codec's. The events of the response are kept for the
response loop, and one that ends the stream ends the upload. An event
of another stream is kept too, for the response loop to skip, because
the poolless codec carries one stream and the loop names the id.
""".
-spec note_h2_upload_event(nhttp_h2:event(), #h2_upload{}) -> #h2_upload{}.
note_h2_upload_event({data_sent, StreamId, Bytes, _Fin}, #h2_upload{stream_id = StreamId} = Up) ->
    note_h2_sent(Bytes, Up);
note_h2_upload_event({data_sent, _OtherStream, _Bytes, _Fin}, Upload) ->
    Upload;
note_h2_upload_event({window_update, _StreamId, _Increment}, Upload) ->
    Upload;
note_h2_upload_event({settings, _Settings}, Upload) ->
    Upload;
note_h2_upload_event(settings_ack, Upload) ->
    Upload;
note_h2_upload_event({ping, _Data}, Upload) ->
    Upload;
note_h2_upload_event({ping_ack, _Data}, Upload) ->
    Upload;
note_h2_upload_event({goaway, _LastStreamId, _Code, _Debug} = Event, Upload) ->
    keep_h2_event(Event, fin, Upload);
note_h2_upload_event(
    {response, StreamId, _Resp, Fin} = Event, #h2_upload{stream_id = StreamId} = Up
) ->
    keep_h2_event(Event, Fin, Up);
note_h2_upload_event({data, StreamId, _Data, Fin} = Event, #h2_upload{stream_id = StreamId} = Up) ->
    keep_h2_event(Event, Fin, Up);
note_h2_upload_event(
    {trailers, StreamId, _Trailers} = Event, #h2_upload{stream_id = StreamId} = Up
) ->
    keep_h2_event(Event, fin, Up);
note_h2_upload_event(
    {stream_reset, StreamId, _Code} = Event, #h2_upload{stream_id = StreamId} = Up
) ->
    keep_h2_event(Event, fin, Up);
note_h2_upload_event(
    {stream_closed, StreamId, _Code} = Event, #h2_upload{stream_id = StreamId} = Up
) ->
    keep_h2_event(Event, fin, Up);
note_h2_upload_event({stream_refused, StreamId} = Event, #h2_upload{stream_id = StreamId} = Up) ->
    keep_h2_event(Event, fin, Up);
note_h2_upload_event({request, _StreamId, _Request, _Fin} = Event, Upload) ->
    keep_h2_event(Event, nofin, Upload);
note_h2_upload_event({response, _OtherStream, _Resp, _Fin} = Event, Upload) ->
    keep_h2_event(Event, nofin, Upload);
note_h2_upload_event({data, _OtherStream, _Data, _Fin} = Event, Upload) ->
    keep_h2_event(Event, nofin, Upload);
note_h2_upload_event({trailers, _OtherStream, _Trailers} = Event, Upload) ->
    keep_h2_event(Event, nofin, Upload);
note_h2_upload_event({stream_reset, _OtherStream, _Code} = Event, Upload) ->
    keep_h2_event(Event, nofin, Upload);
note_h2_upload_event({stream_closed, _OtherStream, _Code} = Event, Upload) ->
    keep_h2_event(Event, nofin, Upload);
note_h2_upload_event({stream_refused, _OtherStream} = Event, Upload) ->
    keep_h2_event(Event, nofin, Upload).

-spec note_h2_upload_events([nhttp_h2:event()], #h2_upload{}) -> #h2_upload{}.
note_h2_upload_events(Events, Upload) ->
    lists:foldl(fun note_h2_upload_event/2, Upload, Events).

-doc """
Offer `Size` octets of `Data` to the codec, in slices of at most the
send queue bound. A slice above the bound can never be accepted, and a
slice at the bound is accepted once the queue has drained enough.
""".
-spec offer_h2_data(iodata(), non_neg_integer(), nhttp_h2:fin(), #h2_upload{}) ->
    {ok, #h2_upload{}} | {error, nhttpc_error:any_reason()}.
offer_h2_data(_Data, _Size, _Fin, #h2_upload{open = false} = Upload) ->
    {ok, Upload};
offer_h2_data(Data, Size, Fin, #h2_upload{slice = Slice} = Upload) when Size =< Slice ->
    offer_h2_slice(Data, Size, Fin, Upload);
offer_h2_data(Data, Size, Fin, #h2_upload{slice = Slice} = Upload) ->
    <<Head:Slice/binary, Rest/binary>> = iolist_to_binary(Data),
    maybe
        {ok, Upload1} ?= offer_h2_slice(Head, Slice, nofin, Upload),
        offer_h2_data(Rest, Size - Slice, Fin, Upload1)
    end.

-doc """
Offer one slice. The frames the credit pays for go out at once and the
rest waits in the codec. A full queue blocks on the socket until the
peer's credit drains it, then offers the slice again.
""".
-spec offer_h2_slice(iodata(), non_neg_integer(), nhttp_h2:fin(), #h2_upload{}) ->
    {ok, #h2_upload{}} | {error, nhttpc_error:any_reason()}.
offer_h2_slice(Data, Size, Fin, #h2_upload{h2_conn = H2Conn, stream_id = StreamId} = Upload) ->
    Held = nhttp_h2:send_buffer_bytes(H2Conn, StreamId),
    case nhttp_h2:send_data(H2Conn, StreamId, Data, Fin) of
        {ok, H2Conn1, Frames} ->
            write_h2_frames(Frames, Size, Upload#h2_upload{h2_conn = H2Conn1});
        {queued, H2Conn1, Frames, Buffered} ->
            Emitted = Size - (Buffered - Held),
            write_h2_frames(Frames, Emitted, Upload#h2_upload{h2_conn = H2Conn1});
        {error, send_buffer_full} ->
            maybe
                {ok, Upload1} ?= await_h2_credit(Upload),
                offer_h2_data(Data, Size, Fin, Upload1)
            end;
        {error, Reason} ->
            {error, {http2_error, Reason}}
    end.

-doc """
Send a body the caller holds in memory on an open stream, and read the
socket until every octet is on the wire. An empty body sends nothing,
because the HEADERS frame of such a request carries END_STREAM. The
result carries the response events the peer sent meanwhile, for the
response loop to continue with.
""".
-spec send_h2_body(nhttp_sock:t(), nhttp_h2:conn(), nhttp_lib:stream_id(), iodata(), timeout()) ->
    {ok, nhttp_h2:conn(), [nhttp_h2:event()]} | {error, nhttpc_error:any_reason()}.
send_h2_body(Socket, H2Conn, StreamId, Body, Timeout) ->
    case iolist_size(Body) of
        0 ->
            {ok, H2Conn, []};
        Size ->
            Upload = new_h2_upload(Socket, H2Conn, StreamId, Timeout, undefined, Size),
            maybe
                {ok, Upload1} ?= offer_h2_data(Body, Size, fin, Upload),
                drain_h2_queue(Upload1)
            end
    end.

-spec write_h2_frames(iodata(), non_neg_integer(), #h2_upload{}) ->
    {ok, #h2_upload{}} | {error, nhttpc_error:any_reason()}.
write_h2_frames([], 0, Upload) ->
    {ok, Upload};
write_h2_frames(Frames, Emitted, #h2_upload{socket = Socket} = Upload) ->
    case nhttp_sock:send(Socket, Frames) of
        ok ->
            {ok, note_h2_sent(Emitted, Upload)};
        {error, Reason} ->
            nhttp_error:send_error(Reason)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/3 STREAMING UPLOAD
%%%-----------------------------------------------------------------------------
-spec do_h3_streaming_upload(nquic:ctx(), nhttp_lib:request(), nhttpc:opts(), timeout()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_h3_streaming_upload(Ctx, #{body := StreamBody} = Req, Opts, Timeout) ->
    case get_stream_source(StreamBody) of
        {ok, Source, StreamBodyOpts} ->
            case prepare_stream_source(Source, StreamBodyOpts) of
                {ok, Iterator, Size} ->
                    do_h3_streaming_upload_impl(Ctx, Req, Iterator, Size, Opts, Timeout);
                {error, _} = Error ->
                    Error
            end;
        {error, not_streaming} ->
            nhttp_error:upload_error(invalid_stream_body)
    end.

-spec do_h3_streaming_upload_impl(
    nquic:ctx(),
    nhttp_lib:request(),
    stream_iterator(),
    non_neg_integer() | unknown,
    nhttpc:opts(),
    timeout()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_h3_streaming_upload_impl(Ctx0, Req, Iterator, Size, Opts, Timeout) ->
    ProgressCb = maps:get(on_upload_progress, Opts, undefined),
    MaxResponseSize = maps:get(max_response_size, Opts, ?DEFAULT_MAX_RESPONSE_SIZE),
    maybe
        {ok, H3Conn0, Ctx1, InitActions} ?= nhttpc_request:init_h3_streams(Ctx0),
        {ok, Ctx2} ?= nhttpc_request:execute_h3_actions(Ctx1, InitActions),
        {ok, StreamId, Ctx3} ?= nquic_lib:open_stream(Ctx2, #{type => bidi}),
        Headers = request_to_h3_headers_with_size(Req, Size),
        {ok, H3Conn1, HeaderActions} ?=
            nhttpc_h3:wrap_h3_send_result(
                nhttp_h3:send_headers(H3Conn0, StreamId, Headers, nofin)
            ),
        {ok, Ctx4} ?= nhttpc_request:execute_h3_actions(Ctx3, HeaderActions),
        {ok, Ctx5} = nquic_lib:flush_notimers(Ctx4),
        {ok, H3ConnFinal, Ctx6} ?=
            stream_h3_upload_data(
                Ctx5, H3Conn1, StreamId, Iterator, ProgressCb, 0, Size
            ),
        nhttpc_request:recv_h3_response(
            Ctx6, H3ConnFinal, StreamId, Timeout, MaxResponseSize
        )
    end.

-spec request_to_h3_headers_with_size(nhttp_lib:request(), non_neg_integer() | unknown) ->
    nhttp_lib:headers().
request_to_h3_headers_with_size(Req, Size) ->
    BaseHeaders = nhttpc_h3:request_to_h3_headers(Req),
    case Size of
        unknown ->
            BaseHeaders;
        N when is_integer(N) ->
            case nhttp_headers:has(<<"content-length">>, BaseHeaders) of
                true ->
                    BaseHeaders;
                false ->
                    BaseHeaders ++ [{<<"content-length">>, integer_to_binary(N)}]
            end
    end.

-spec stream_h3_upload_data(
    nquic:ctx(),
    nhttp_h3:conn(),
    nhttp_lib:stream_id(),
    stream_iterator(),
    on_upload_progress_callback() | undefined,
    non_neg_integer(),
    non_neg_integer() | unknown
) ->
    {ok, nhttp_h3:conn(), nquic:ctx()} | {error, nhttpc_error:any_reason()}.
stream_h3_upload_data(Ctx, H3Conn, _StreamId, Iterator, _ProgressCb, _Sent, _Total) when
    Iterator =:= undefined
->
    {ok, H3Conn, Ctx};
stream_h3_upload_data(Ctx, H3Conn, StreamId, Iterator, ProgressCb, Sent, Total) ->
    case Iterator() of
        {ok, Chunk, NextIterator} ->
            case
                nhttpc_h3:wrap_h3_send_result(
                    nhttp_h3:send_data(H3Conn, StreamId, Chunk, nofin)
                )
            of
                {ok, NewH3Conn, Actions} ->
                    case nhttpc_request:execute_h3_actions(Ctx, Actions) of
                        {ok, Ctx1} ->
                            {ok, Ctx2} = nquic_lib:flush_notimers(Ctx1),
                            ChunkBytes = iolist_size(Chunk),
                            NewSent = Sent + ChunkBytes,
                            maybe_call_progress(ProgressCb, NewSent, Total),
                            stream_h3_upload_data(
                                Ctx2,
                                NewH3Conn,
                                StreamId,
                                NextIterator,
                                ProgressCb,
                                NewSent,
                                Total
                            );
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        fin ->
            case
                nhttpc_h3:wrap_h3_send_result(
                    nhttp_h3:send_data(H3Conn, StreamId, <<>>, fin)
                )
            of
                {ok, NewH3Conn, Actions} ->
                    case nhttpc_request:execute_h3_actions(Ctx, Actions) of
                        {ok, Ctx1} ->
                            {ok, Ctx2} = nquic_lib:flush_notimers(Ctx1),
                            {ok, NewH3Conn, Ctx2};
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, Reason} ->
            nhttp_error:upload_error(Reason)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: HELPERS
%%%-----------------------------------------------------------------------------
-doc "Add header if not already present.".
-spec add_header_if_missing(binary(), binary(), nhttp_lib:headers()) -> nhttp_lib:headers().
add_header_if_missing(Name, Value, Headers) ->
    case nhttp_headers:has(Name, Headers) of
        true -> Headers;
        false -> [{Name, Value} | Headers]
    end.

-doc "Encode headers to iolist.".
-spec encode_headers_iolist(nhttp_lib:headers()) -> iolist().
encode_headers_iolist(Headers) ->
    [[Name, <<": ">>, Value, <<"\r\n">>] || {Name, Value} <- Headers].

-doc "Call upload progress callback if provided.".
-spec maybe_call_progress(
    on_upload_progress_callback() | undefined,
    non_neg_integer(),
    non_neg_integer() | unknown
) ->
    ok.
maybe_call_progress(undefined, _Sent, _Total) ->
    ok;
maybe_call_progress(Callback, Sent, Total) when is_function(Callback, 2) ->
    Callback(Sent, Total).
