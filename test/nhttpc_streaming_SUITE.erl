%%%-----------------------------------------------------------------------------
%%% @doc Streaming sync sinks / sources suite.
%%%
%%% Covers `body_to => {file, _} | {fold, F}`, `body_from => {file, _}
%%% | {iter, F}`, and the `on_upload_progress` / `on_download_progress`
%%% callbacks.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_streaming_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    suite/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    download_with_body_to_file/1,
    download_with_body_to_file_via_pool/1,
    download_with_body_to_fold_callback/1,
    download_with_body_to_fold_callback_via_pool/1,
    download_with_body_to_fold_stops_early/1,
    download_with_body_to_default_returns_binary_body/1,
    upload_with_body_from_file/1,
    upload_with_body_from_iter/1,
    on_upload_progress_fires_during_upload/1,
    on_download_progress_fires_during_download/1
]).

-export([
    chunked_multi_chunk_happy/1,
    chunked_with_extensions/1,
    chunked_bad_hex_size/1,
    chunked_split_size_header/1,
    chunked_data_split/1,
    chunked_body_timeout_data/1,
    chunked_body_timeout_need_more/1,
    chunked_recv_error_data/1,
    chunked_recv_error_need_more/1,
    cl_full_body_in_packet/1,
    cl_body_separate/1,
    cl_partial_in_packet/1,
    cl_premature_close/1,
    cl_body_timeout/1,
    cl_bad_content_length/1,
    cl_zero_length/1,
    cl_exceeds_max_at_headers/1,
    no_cl_not_chunked/1,
    headers_split_across_packets/1,
    malformed_status_line/1,
    request_timeout_before_headers/1,
    recv_error_before_headers/1,
    gzip_content_length/1,
    gzip_chunked/1,
    deflate_content_length/1,
    gzip_garbage_passthrough/1,
    deflate_garbage_passthrough/1,
    unknown_encoding_passthrough/1,
    stop_chunked_complete_chunk/1,
    stop_chunked_recv_chunk_data/1,
    stop_no_cl_until_close/1,
    download_progress_across_chunks/1,
    max_response_size_infinity/1,
    stream_connect_refused/1,
    h2_stream_large_body/1,
    h2_stream_trailers/1,
    h2_stream_too_large/1,
    h2_stream_empty_body/1,
    h2_post_body_streamed/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
        {group, sync_sinks},
        {group, sync_sources},
        {group, progress},
        {group, h1_chunked},
        {group, h1_content_length},
        {group, h1_header_paths},
        {group, h1_decompression},
        {group, h1_stream_stopped},
        {group, recv_progress},
        {group, h2_streaming}
    ].

groups() ->
    [
        {sync_sinks, [sequence], [
            download_with_body_to_file,
            download_with_body_to_file_via_pool,
            download_with_body_to_fold_callback,
            download_with_body_to_fold_callback_via_pool,
            download_with_body_to_fold_stops_early,
            download_with_body_to_default_returns_binary_body
        ]},
        {sync_sources, [sequence], [
            upload_with_body_from_file,
            upload_with_body_from_iter
        ]},
        {progress, [sequence], [
            on_upload_progress_fires_during_upload,
            on_download_progress_fires_during_download
        ]},
        {h1_chunked, [], [
            chunked_multi_chunk_happy,
            chunked_with_extensions,
            chunked_bad_hex_size,
            chunked_split_size_header,
            chunked_data_split,
            chunked_body_timeout_data,
            chunked_body_timeout_need_more,
            chunked_recv_error_data,
            chunked_recv_error_need_more
        ]},
        {h1_content_length, [], [
            cl_full_body_in_packet,
            cl_body_separate,
            cl_partial_in_packet,
            cl_premature_close,
            cl_body_timeout,
            cl_bad_content_length,
            cl_zero_length,
            cl_exceeds_max_at_headers,
            no_cl_not_chunked
        ]},
        {h1_header_paths, [], [
            headers_split_across_packets,
            malformed_status_line,
            request_timeout_before_headers,
            recv_error_before_headers
        ]},
        {h1_decompression, [], [
            gzip_content_length,
            gzip_chunked,
            deflate_content_length,
            gzip_garbage_passthrough,
            deflate_garbage_passthrough,
            unknown_encoding_passthrough
        ]},
        {h1_stream_stopped, [], [
            stop_chunked_complete_chunk,
            stop_chunked_recv_chunk_data,
            stop_no_cl_until_close
        ]},
        {recv_progress, [], [
            download_progress_across_chunks,
            max_response_size_infinity,
            stream_connect_refused
        ]},
        {h2_streaming, [], [
            h2_stream_large_body,
            h2_stream_trailers,
            h2_stream_too_large,
            h2_stream_empty_body,
            h2_post_body_streamed
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    PrivDir = proplists:get_value(priv_dir, Config, ""),
    ConfDir = filename:join([find_project_root(PrivDir), "test", "conf"]),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_file(CertFile) andalso filelib:is_file(KeyFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> Config
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) when
    Group =:= sync_sinks; Group =:= sync_sources; Group =:= progress
->
    Handler = nhttp_test_handler,
    {ok, ServerPid} = nhttp:start_link(test_streaming_server, #{
        port => 0,
        handler => Handler,
        protocols => [http1_1]
    }),
    unlink(ServerPid),
    {ok, Port} = nhttp:get_port(ServerPid),
    PoolRef = test_streaming_pool,
    {ok, PoolPid} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2,
        max_overflow => 0,
        versions => [http1_1]
    }),
    unlink(PoolPid),
    MgrName = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrName, 5000),
    [{server_pid, ServerPid}, {server_port, Port}, {pool_ref, PoolRef} | Config];
init_per_group(h2_streaming, Config) ->
    case proplists:get_value(certfile, Config) of
        undefined ->
            {skip, "TLS certificates not available"};
        CertFile ->
            KeyFile = proplists:get_value(keyfile, Config),
            {ok, ServerPid} = nhttp:start_link(test_streaming_h2_server, #{
                port => 0,
                handler => nhttp_test_handler,
                versions => [http2, http1_1],
                tls => #{certfile => CertFile, keyfile => KeyFile}
            }),
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            [{server_pid, ServerPid}, {server_port, Port} | Config]
    end;
init_per_group(_RawGroup, Config) ->
    Config.

end_per_group(Group, _Config) when
    Group =:= h1_chunked;
    Group =:= h1_content_length;
    Group =:= h1_header_paths;
    Group =:= h1_decompression;
    Group =:= h1_stream_stopped;
    Group =:= recv_progress
->
    ok;
end_per_group(_Group, Config) ->
    case proplists:get_value(pool_ref, Config) of
        undefined ->
            ok;
        PoolRef ->
            try
                nhttpc_pool_sup:stop(PoolRef)
            catch
                _:_ -> ok
            end
    end,
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok.

%%%-----------------------------------------------------------------------------
%%% SYNC SINKS
%%%-----------------------------------------------------------------------------

download_with_body_to_file(Config) ->
    Port = proplists:get_value(server_port, Config),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "body_to_file.bin"),
    Url = url(Port, "/stream/large"),
    {ok, Resp} = nhttpc:get(Url, #{body_to => {file, FilePath}}),
    ?assertEqual(200, maps:get(status, Resp)),
    {file, FilePath, Bytes} = maps:get(body, Resp),
    Pattern = <<"0123456789ABCDEF">>,
    Expected = binary:copy(Pattern, 6400),
    ?assertEqual(byte_size(Expected), Bytes),
    {ok, Content} = file:read_file(FilePath),
    ?assertEqual(Expected, Content),
    file:delete(FilePath),
    ok.

download_with_body_to_file_via_pool(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = proplists:get_value(pool_ref, Config),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "body_to_file_pool.bin"),
    Url = url(Port, "/stream/large"),
    {ok, Resp} = nhttpc:get(Url, #{
        via => {pool, PoolRef},
        body_to => {file, FilePath}
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    {file, FilePath, Bytes} = maps:get(body, Resp),
    ?assert(Bytes > 0),
    {ok, Content} = file:read_file(FilePath),
    ?assertEqual(Bytes, byte_size(Content)),
    file:delete(FilePath),
    ok.

download_with_body_to_fold_callback(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = url(Port, "/stream/large"),
    F = fun(Chunk, Acc) -> {continue, Acc + byte_size(Chunk)} end,
    {ok, Resp} = nhttpc:get(Url, #{body_to => {fold, F}, fold_init => 0}),
    ?assertEqual(200, maps:get(status, Resp)),
    Pattern = <<"0123456789ABCDEF">>,
    Expected = binary:copy(Pattern, 6400),
    {fold, Total} = maps:get(body, Resp),
    ?assertEqual(byte_size(Expected), Total),
    ok.

download_with_body_to_fold_callback_via_pool(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = proplists:get_value(pool_ref, Config),
    Url = url(Port, "/stream/large"),
    F = fun(Chunk, Acc) -> {continue, [Chunk | Acc]} end,
    {ok, Resp} = nhttpc:get(Url, #{
        via => {pool, PoolRef},
        body_to => {fold, F},
        fold_init => []
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    {fold, Acc} = maps:get(body, Resp),
    Reassembled = iolist_to_binary(lists:reverse(Acc)),
    Pattern = <<"0123456789ABCDEF">>,
    ?assertEqual(binary:copy(Pattern, 6400), Reassembled),
    ok.

download_with_body_to_fold_stops_early(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = url(Port, "/stream/large"),
    F = fun(Chunk, Acc) ->
        NewAcc = Acc + byte_size(Chunk),
        case NewAcc >= 1024 of
            true -> {stop, NewAcc, NewAcc};
            false -> {continue, NewAcc}
        end
    end,
    {ok, Resp} = nhttpc:get(Url, #{body_to => {fold, F}, fold_init => 0}),
    ?assertEqual(200, maps:get(status, Resp)),
    {fold, FinalSize} = maps:get(body, Resp),
    ?assert(FinalSize >= 1024),
    ok.

download_with_body_to_default_returns_binary_body(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = url(Port, "/echo"),
    {ok, Resp} = nhttpc:get(Url),
    ?assertEqual(200, maps:get(status, Resp)),
    Body = maps:get(body, Resp),
    ?assert(is_binary(Body)),
    ok.

%%%-----------------------------------------------------------------------------
%%% SYNC SOURCES
%%%-----------------------------------------------------------------------------

upload_with_body_from_file(Config) ->
    Port = proplists:get_value(server_port, Config),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "body_from_file.txt"),
    Payload = binary:copy(<<"abcd">>, 1024),
    ok = file:write_file(FilePath, Payload),
    Url = url(Port, "/upload/echo"),
    {ok, Resp} = nhttpc:post(Url, <<>>, #{body_from => {file, FilePath}}),
    ?assertEqual(200, maps:get(status, Resp)),
    Body = maps:get(body, Resp),
    %% Echo route prepends "Received: N bytes\nBody: ..."
    Marker = <<"Received: ", (integer_to_binary(byte_size(Payload)))/binary, " bytes">>,
    case binary:match(Body, Marker) of
        nomatch -> ct:fail("Echo body did not contain expected size marker: ~p", [Body]);
        _ -> ok
    end,
    case binary:match(Body, Payload) of
        nomatch -> ct:fail("Echo body did not contain uploaded payload");
        _ -> ok
    end,
    file:delete(FilePath),
    ok.

upload_with_body_from_iter(Config) ->
    Port = proplists:get_value(server_port, Config),
    Chunks = [<<"alpha">>, <<"beta">>, <<"gamma">>],
    Iter = make_chunk_iter(Chunks),
    Url = url(Port, "/upload/echo"),
    {ok, Resp} = nhttpc:post(Url, <<>>, #{body_from => {iter, Iter}}),
    ?assertEqual(200, maps:get(status, Resp)),
    Body = maps:get(body, Resp),
    Full = iolist_to_binary(Chunks),
    case binary:match(Body, Full) of
        nomatch -> ct:fail("Echo body did not contain concatenated chunks: ~p", [Body]);
        _ -> ok
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% PROGRESS CALLBACKS
%%%-----------------------------------------------------------------------------

on_upload_progress_fires_during_upload(Config) ->
    Port = proplists:get_value(server_port, Config),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "upload_progress.bin"),
    Payload = binary:copy(<<"x">>, 65536 * 3),
    ok = file:write_file(FilePath, Payload),
    Parent = self(),
    Cb = fun(Sent, Total) ->
        Parent ! {upload_progress, Sent, Total},
        ok
    end,
    Url = url(Port, "/upload/echo"),
    {ok, _Resp} = nhttpc:post(Url, <<>>, #{
        body_from => {file, FilePath},
        on_upload_progress => Cb
    }),
    Events = drain_progress(upload_progress, []),
    ?assert(length(Events) >= 1),
    {LastSent, LastTotal} = lists:last(Events),
    ?assertEqual(byte_size(Payload), LastTotal),
    ?assertEqual(byte_size(Payload), LastSent),
    file:delete(FilePath),
    ok.

on_download_progress_fires_during_download(Config) ->
    Port = proplists:get_value(server_port, Config),
    TempDir = proplists:get_value(priv_dir, Config),
    FilePath = filename:join(TempDir, "download_progress.bin"),
    Parent = self(),
    Cb = fun(Downloaded, _Total) ->
        Parent ! {download_progress, Downloaded},
        ok
    end,
    Url = url(Port, "/stream/large"),
    {ok, _Resp} = nhttpc:get(Url, #{
        body_to => {file, FilePath},
        on_download_progress => Cb
    }),
    Events = drain_download_progress([]),
    ?assert(length(Events) >= 1),
    Last = lists:last(Events),
    ?assertEqual(byte_size(binary:copy(<<"0123456789ABCDEF">>, 6400)), Last),
    file:delete(FilePath),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 CHUNKED DECODE
%%%-----------------------------------------------------------------------------

chunked_multi_chunk_happy(_Config) ->
    Resp = chunked_response([<<"abc">>, <<"def">>, <<"ghi">>]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(<<"abcdefghi">>, Body),
    ok.

chunked_with_extensions(_Config) ->
    Resp = [
        resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
        <<"5;name=value\r\nHELLO\r\n0\r\n\r\n">>
    ],
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(<<"HELLO">>, Body),
    ok.

chunked_bad_hex_size(_Config) ->
    Resp = [
        resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
        <<"ZZ\r\nABCD">>
    ],
    ?assertEqual(
        {error, {connect_error, closed}}, raw_get([{send, Resp}, close], #{})
    ),
    ok.

chunked_split_size_header(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
    Steps = [
        {send, [Hdr, <<"5">>]},
        {hold, 120},
        {send, <<"\r\nWORLD\r\n0\r\n\r\n">>},
        close
    ],
    {ok, #{status := 200, body := {fold, Body}}} = raw_get(Steps, #{}),
    ?assertEqual(<<"WORLD">>, Body),
    ok.

chunked_data_split(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
    Steps = [
        {send, [Hdr, <<"8\r\nAB">>]},
        {hold, 100},
        {send, <<"CD">>},
        {hold, 100},
        {send, <<"EFGH\r\n0\r\n\r\n">>},
        close
    ],
    {ok, #{status := 200, body := {fold, Body}}} = raw_get(Steps, #{}),
    ?assertEqual(<<"ABCDEFGH">>, Body),
    ok.

chunked_body_timeout_data(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
    Steps = [{send, [Hdr, <<"8\r\nAB">>]}, {hold, 1500}, close],
    ?assertEqual(
        {error, {timeout, body}},
        raw_get(Steps, #{timeouts => #{request => 400}})
    ),
    ok.

chunked_body_timeout_need_more(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
    Steps = [{send, Hdr}, {hold, 1500}, close],
    ?assertEqual(
        {error, {timeout, body}},
        raw_get(Steps, #{timeouts => #{request => 400}})
    ),
    ok.

chunked_recv_error_data(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
    ?assertEqual(
        {error, {connect_error, closed}},
        raw_get([{send, [Hdr, <<"8\r\nAB">>]}, close], #{})
    ),
    ok.

chunked_recv_error_need_more(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
    ?assertEqual(
        {error, {connect_error, closed}}, raw_get([{send, Hdr}, close], #{})
    ),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 CONTENT-LENGTH DECODE
%%%-----------------------------------------------------------------------------

cl_full_body_in_packet(_Config) ->
    Resp = cl_response(<<"hello world">>, []),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(<<"hello world">>, Body),
    ok.

cl_body_separate(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"content-length", "10"}]),
    Steps = [
        {send, Hdr},
        {hold, 80},
        {send, <<"ABCDE">>},
        {hold, 80},
        {send, <<"FGHIJ">>},
        close
    ],
    {ok, #{status := 200, body := {fold, Body}}} = raw_get(Steps, #{}),
    ?assertEqual(<<"ABCDEFGHIJ">>, Body),
    ok.

cl_partial_in_packet(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"content-length", "10"}]),
    Steps = [
        {send, [Hdr, <<"ABC">>]},
        {hold, 80},
        {send, <<"DEFGHIJ">>},
        close
    ],
    {ok, #{status := 200, body := {fold, Body}}} = raw_get(Steps, #{}),
    ?assertEqual(<<"ABCDEFGHIJ">>, Body),
    ok.

cl_premature_close(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"content-length", "100"}]),
    ?assertEqual(
        {error, {connect_error, closed}},
        raw_get([{send, [Hdr, <<"ABCDE">>]}, close], #{})
    ),
    ok.

cl_body_timeout(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"content-length", "100"}]),
    Steps = [{send, [Hdr, <<"ABCDE">>]}, {hold, 1500}, close],
    ?assertEqual(
        {error, {timeout, body}},
        raw_get(Steps, #{timeouts => #{request => 400}})
    ),
    ok.

cl_bad_content_length(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"content-length", "notanumber"}]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, [Hdr, <<"PAYLOAD">>]}, close], #{}),
    ?assertEqual(<<"PAYLOAD">>, Body),
    ok.

cl_zero_length(_Config) ->
    Resp = resp_headers("HTTP/1.1 200 OK", [{"content-length", "0"}]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(<<>>, Body),
    ok.

cl_exceeds_max_at_headers(_Config) ->
    Resp = cl_response(binary:copy(<<"y">>, 4096), []),
    ?assertEqual(
        {error, {stream_error, exceeded_max_response_size}},
        raw_get([{send, Resp}, close], #{max_response_size => 1024})
    ),
    ok.

no_cl_not_chunked(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"content-type", "text/plain"}]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, [Hdr, <<"NOCLBODY">>]}, close], #{}),
    ?assertEqual(<<"NOCLBODY">>, Body),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 HEADER PATHS
%%%-----------------------------------------------------------------------------

headers_split_across_packets(_Config) ->
    Steps = [
        {send, <<"HTTP/1.1 200 OK\r\n">>},
        {hold, 100},
        {send, <<"content-length: 3\r\n\r\nabc">>},
        close
    ],
    {ok, #{status := 200, body := {fold, Body}}} = raw_get(Steps, #{}),
    ?assertEqual(<<"abc">>, Body),
    ok.

malformed_status_line(_Config) ->
    {error, {protocol_error, http1_1, _ParseError}} =
        raw_get([{send, <<"GARBAGE STATUS LINE!\r\nX: y\r\n\r\n">>}, close], #{}),
    ok.

request_timeout_before_headers(_Config) ->
    ?assertEqual(
        {error, {timeout, request}},
        raw_get([{hold, 1500}, close], #{timeouts => #{request => 400}})
    ),
    ok.

recv_error_before_headers(_Config) ->
    ?assertEqual({error, {connect_error, closed}}, raw_get([close], #{})),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 DECOMPRESSION
%%%-----------------------------------------------------------------------------

gzip_content_length(_Config) ->
    Plain = binary:copy(<<"compress-me-">>, 64),
    {ok, Gz} = nhttp_compress:compress(Plain, gzip, 6),
    Resp = cl_response(Gz, [{"content-encoding", "gzip"}]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(Plain, Body),
    ok.

gzip_chunked(_Config) ->
    Plain = binary:copy(<<"chunked-gzip-">>, 50),
    {ok, Gz} = nhttp_compress:compress(Plain, gzip, 6),
    Resp = [
        resp_headers("HTTP/1.1 200 OK", [
            {"content-encoding", "gzip"}, {"transfer-encoding", "chunked"}
        ]),
        encode_chunk(Gz),
        <<"0\r\n\r\n">>
    ],
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(Plain, Body),
    ok.

deflate_content_length(_Config) ->
    Plain = binary:copy(<<"deflate-me-">>, 64),
    {ok, Df} = nhttp_compress:compress(Plain, deflate, 6),
    Resp = cl_response(Df, [{"content-encoding", "deflate"}]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(Plain, Body),
    ok.

gzip_garbage_passthrough(_Config) ->
    Raw = <<"this is definitely not a gzip stream">>,
    Resp = cl_response(Raw, [{"content-encoding", "gzip"}]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(Raw, Body),
    ok.

deflate_garbage_passthrough(_Config) ->
    Raw = <<"this is definitely not a deflate stream">>,
    Resp = cl_response(Raw, [{"content-encoding", "deflate"}]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(Raw, Body),
    ok.

unknown_encoding_passthrough(_Config) ->
    Raw = <<"brotli-not-supported-passthrough">>,
    Resp = cl_response(Raw, [{"content-encoding", "br"}]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{}),
    ?assertEqual(Raw, Body),
    ok.

%%%-----------------------------------------------------------------------------
%%% STREAM STOPPED (response_too_large carry-through)
%%%-----------------------------------------------------------------------------

%% The internal `nhttp_error:stream_stopped(Reason, FinalAcc)` tuple
%% carries the accumulator (covered for line coverage of the
%% stream_stopped clauses), but `nhttpc_error:wrap_request_map/1`
%% intentionally flattens it to the canonical taxonomy before it
%% reaches the public API, so the observable result is
%% `{stream_error, exceeded_max_response_size}` with no acc. See plan
%% 21 notes.
stop_chunked_complete_chunk(_Config) ->
    C1 = binary:copy(<<"a">>, 600),
    C2 = binary:copy(<<"b">>, 600),
    Resp = chunked_response([C1, C2]),
    ?assertEqual(
        {error, {stream_error, exceeded_max_response_size}},
        raw_get([{send, Resp}, close], #{max_response_size => 1024})
    ),
    ok.

stop_chunked_recv_chunk_data(_Config) ->
    C1 = binary:copy(<<"a">>, 100),
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
    Big = binary:copy(<<"x">>, 1000),
    Steps = [
        {send, [Hdr, encode_chunk(C1), <<"3E8\r\n">>, binary:part(Big, 0, 500)]},
        {hold, 120},
        {send, [binary:part(Big, 500, 500), <<"\r\n0\r\n\r\n">>]},
        close
    ],
    ?assertEqual(
        {error, {stream_error, exceeded_max_response_size}},
        raw_get(Steps, #{max_response_size => 512})
    ),
    ok.

stop_no_cl_until_close(_Config) ->
    Hdr = resp_headers("HTTP/1.1 200 OK", [{"content-type", "text/plain"}]),
    Big = binary:copy(<<"z">>, 4096),
    ?assertEqual(
        {error, {stream_error, exceeded_max_response_size}},
        raw_get([{send, [Hdr, Big]}, close], #{max_response_size => 1024})
    ),
    ok.

%%%-----------------------------------------------------------------------------
%%% PROGRESS AND SIZE-LIMIT EDGE
%%%-----------------------------------------------------------------------------

download_progress_across_chunks(_Config) ->
    Parent = self(),
    Cb = fun(Downloaded, _Total) ->
        Parent ! {dl_progress, Downloaded},
        ok
    end,
    Resp = chunked_response([
        binary:copy(<<"p">>, 100),
        binary:copy(<<"q">>, 100),
        binary:copy(<<"r">>, 100)
    ]),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{on_download_progress => Cb}),
    ?assertEqual(300, byte_size(Body)),
    Events = drain_dl_progress([]),
    ?assert(length(Events) >= 2),
    ?assertEqual(300, lists:last(Events)),
    ok.

max_response_size_infinity(_Config) ->
    Payload = binary:copy(<<"big-payload-">>, 256),
    Resp = cl_response(Payload, []),
    {ok, #{status := 200, body := {fold, Body}}} =
        raw_get([{send, Resp}, close], #{max_response_size => infinity}),
    ?assertEqual(Payload, Body),
    ok.

stream_connect_refused(_Config) ->
    {ok, LSock} = gen_tcp:listen(0, [binary]),
    {ok, Port} = inet:port(LSock),
    gen_tcp:close(LSock),
    {ok, Req} = nhttpc:url(get, raw_url(Port, "/x")),
    Opts = #{timeouts => #{connect => 500, request => 1000}},
    case nhttpc:stream(Req, Opts) of
        {ok, Handle} ->
            receive
                {nhttpc, Handle, {error, _}} -> ok
            after 5000 ->
                ct:fail("no error event for refused connection")
            end;
        {error, _} ->
            ok
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 STREAMING
%%%-----------------------------------------------------------------------------

h2_stream_large_body(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = h2_url(Port, "/stream/large"),
    {ok, #{status := 200, body := {fold, Body}}} = nhttpc:get(Url, #{
        body_to => {fold, fun collect_fold/2},
        fold_init => <<>>,
        tls => #{verify => verify_none}
    }),
    Expected = binary:copy(<<"0123456789ABCDEF">>, 6400),
    ?assertEqual(byte_size(Expected), byte_size(Body)),
    ?assertEqual(Expected, Body),
    ok.

h2_stream_trailers(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = h2_url(Port, "/trailers"),
    {ok, #{status := 200, body := {fold, Body}}} = nhttpc:get(Url, #{
        body_to => {fold, fun collect_fold/2},
        fold_init => <<>>,
        tls => #{verify => verify_none}
    }),
    ?assertEqual(<<"grpc-response-body">>, Body),
    ok.

h2_stream_too_large(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = h2_url(Port, "/stream/large"),
    ?assertEqual(
        {error, {stream_error, exceeded_max_response_size}},
        nhttpc:get(Url, #{
            body_to => {fold, fun collect_fold/2},
            fold_init => <<>>,
            max_response_size => 1024,
            tls => #{verify => verify_none}
        })
    ),
    ok.

h2_stream_empty_body(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = h2_url(Port, "/trailers-only"),
    {ok, #{status := 200, body := {fold, Body}}} = nhttpc:get(Url, #{
        body_to => {fold, fun collect_fold/2},
        fold_init => <<>>,
        tls => #{verify => verify_none}
    }),
    ?assertEqual(<<>>, Body),
    ok.

h2_post_body_streamed(Config) ->
    Port = proplists:get_value(server_port, Config),
    Url = h2_url(Port, "/echo"),
    Payload = binary:copy(<<"h2-upload-chunk-">>, 32),
    {ok, #{status := 200, body := {fold, Body}}} = nhttpc:post(Url, Payload, #{
        body_to => {fold, fun collect_fold/2},
        fold_init => <<>>,
        tls => #{verify => verify_none}
    }),
    ?assertEqual(Payload, Body),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

-spec url(inet:port_number(), string()) -> binary().
url(Port, Path) ->
    iolist_to_binary([
        <<"http://127.0.0.1:">>,
        integer_to_binary(Port),
        list_to_binary(Path)
    ]).

-spec make_chunk_iter([binary()]) -> fun(() -> term()).
make_chunk_iter([]) ->
    fun() -> fin end;
make_chunk_iter([Chunk | Rest]) ->
    fun() -> {ok, Chunk, make_chunk_iter(Rest)} end.

-spec drain_progress(atom(), [{non_neg_integer(), non_neg_integer()}]) ->
    [{non_neg_integer(), non_neg_integer()}].
drain_progress(Tag, Acc) ->
    receive
        {Tag, Sent, Total} -> drain_progress(Tag, [{Sent, Total} | Acc])
    after 200 ->
        lists:reverse(Acc)
    end.

-spec drain_download_progress([non_neg_integer()]) -> [non_neg_integer()].
drain_download_progress(Acc) ->
    receive
        {download_progress, N} -> drain_download_progress([N | Acc])
    after 200 ->
        lists:reverse(Acc)
    end.

%%%-----------------------------------------------------------------------------
%%% RAW HTTP/1.1 RESPONDER + STREAM HELPERS
%%%-----------------------------------------------------------------------------

-type raw_step() :: {send, iodata()} | {hold, pos_integer()} | close.

-spec collect_fold(binary(), binary()) -> {continue, binary()}.
collect_fold(Chunk, Acc) ->
    {continue, <<Acc/binary, Chunk/binary>>}.

-spec raw_get([raw_step()], map()) ->
    {ok, nhttp_lib:response()} | {error, term()}.
raw_get(Steps, Opts0) ->
    {Port, Pid} = raw_server(Steps),
    Url = raw_url(Port, "/probe"),
    Opts = maps:merge(
        #{body_to => {fold, fun collect_fold/2}, fold_init => <<>>}, Opts0
    ),
    Result = nhttpc:get(Url, Opts),
    try
        exit(Pid, kill)
    catch
        _:_ -> ok
    end,
    Result.

-spec raw_server([raw_step()]) -> {inet:port_number(), pid()}.
raw_server(Steps) ->
    {ok, LSock} = gen_tcp:listen(0, [
        binary, {active, false}, {reuseaddr, true}, {nodelay, true}
    ]),
    {ok, Port} = inet:port(LSock),
    Pid = spawn(fun() ->
        case gen_tcp:accept(LSock, 5000) of
            {ok, Sock} ->
                gen_tcp:close(LSock),
                drain_request(Sock),
                run_steps(Sock, Steps);
            {error, _} ->
                gen_tcp:close(LSock)
        end
    end),
    {Port, Pid}.

-spec drain_request(gen_tcp:socket()) -> ok.
drain_request(Sock) ->
    case gen_tcp:recv(Sock, 0, 2000) of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

-spec run_steps(gen_tcp:socket(), [raw_step()]) -> ok.
run_steps(_Sock, []) ->
    ok;
run_steps(Sock, [{send, Data} | Rest]) ->
    case gen_tcp:send(Sock, Data) of
        ok -> run_steps(Sock, Rest);
        {error, _} -> ok
    end;
run_steps(Sock, [{hold, Ms} | Rest]) ->
    timer:sleep(Ms),
    run_steps(Sock, Rest);
run_steps(Sock, [close | _Rest]) ->
    gen_tcp:close(Sock).

-spec raw_url(inet:port_number(), string()) -> binary().
raw_url(Port, Path) ->
    iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), list_to_binary(Path)
    ]).

-spec h2_url(inet:port_number(), string()) -> binary().
h2_url(Port, Path) ->
    iolist_to_binary([
        <<"https://127.0.0.1:">>, integer_to_binary(Port), list_to_binary(Path)
    ]).

-spec resp_headers(string(), [{string(), string()}]) -> iolist().
resp_headers(StatusLine, Headers) ->
    [
        StatusLine,
        "\r\n",
        [[K, ": ", V, "\r\n"] || {K, V} <- Headers],
        "\r\n"
    ].

-spec cl_response(binary(), [{string(), string()}]) -> iolist().
cl_response(Body, ExtraHeaders) ->
    Hdrs = [{"content-length", integer_to_list(byte_size(Body))} | ExtraHeaders],
    [resp_headers("HTTP/1.1 200 OK", Hdrs), Body].

-spec chunked_response([binary()]) -> iolist().
chunked_response(Chunks) ->
    [
        resp_headers("HTTP/1.1 200 OK", [{"transfer-encoding", "chunked"}]),
        [encode_chunk(C) || C <- Chunks],
        <<"0\r\n\r\n">>
    ].

-spec encode_chunk(binary()) -> iolist().
encode_chunk(Data) ->
    [integer_to_binary(byte_size(Data), 16), <<"\r\n">>, Data, <<"\r\n">>].

-spec drain_dl_progress([non_neg_integer()]) -> [non_neg_integer()].
drain_dl_progress(Acc) ->
    receive
        {dl_progress, N} -> drain_dl_progress([N | Acc])
    after 300 ->
        lists:reverse(Acc)
    end.

-spec find_project_root(string()) -> string().
find_project_root(Dir) ->
    case filelib:is_file(filename:join(Dir, "rebar.config")) of
        true ->
            Dir;
        false ->
            Parent = filename:dirname(Dir),
            case Parent =:= Dir of
                true -> ".";
                false -> find_project_root(Parent)
            end
    end.
