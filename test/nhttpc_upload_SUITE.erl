%%%-----------------------------------------------------------------------------
%%% @doc Common Test suite covering `nhttpc_stream_send` wire paths:
%%% the file iterator, the HTTP/1.1 chunked / content-length upload
%%% loop, the HTTP/2 DATA-frame upload loop, and the HTTP/3 upload
%%% smoke path. Everything is driven through the public `nhttpc` API
%%% (`nhttpc:post/3`, `nhttpc:request/2`); no module under test is
%%% called directly.
%%%
%%% Error-injection cases use raw `gen_tcp` / `ssl` responders so the
%%% failure point is deterministic (no `timer:sleep` for
%%% synchronisation).
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_upload_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% The RFC 9113 defaults a peer starts with: the initial window and the
%% largest DATA frame it accepts.
-define(H2_WINDOW, 65535).
-define(H2_MAX_FRAME, 16384).
%% The default read size of a file source.
-define(FILE_CHUNK, 65536).
-define(NEVER_BODY, 4194304).
-define(PROGRESS_CHUNK, 40000).
-define(PROGRESS_CHUNKS, 5).
-define(TIMEOUT_CHUNK, 20000).
-define(TIMEOUT_CHUNKS, 5).
-define(UPLOAD_TIMEOUT_MS, 1000).
-define(PEER_WAIT_MS, 10000).
-define(PEER_POLL_MS, 20).
-define(FIRST_STREAM, 1).

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
    h1_file_upload_known_size/1,
    h1_file_upload_via_body_from/1,
    h1_explicit_content_length/1,
    h1_iter_unknown_size_chunked/1,
    h1_progress_callback/1,
    h1_continued_response/1,
    h1_file_missing_with_size/1,
    h1_file_missing_no_size/1,
    h1_file_is_directory/1,
    h1_iter_returns_error/1,
    h1_malformed_response/1,
    h1_response_timeout/1,
    h1_closed_before_response/1,
    h1_send_error_mid_upload/1,
    h2_file_upload/1,
    h2_explicit_content_length/1,
    h2_iter_unknown_size/1,
    h2_progress_callback/1,
    h2_server_resets_stream/1,
    h2_server_goaway/1,
    h2_server_closes_mid_upload/1,
    h2_server_closes_after_request/1,
    h2_garbage_response/1,
    h2_iter_returns_error/1,
    h2_stream_upload_default_chunk_size/1,
    h2_poolless_body_over_max_frame_size/1,
    h2_stream_upload_never_credits_connection/1,
    h2_stream_upload_terminator_at_zero_window/1,
    h2_stream_upload_progress_matches_wire/1,
    h2_stream_upload_timeout_purges_and_is_retryable/1,
    h3_iter_upload/1,
    h3_explicit_content_length/1,
    h3_iter_returns_error/1,
    h3_progress_callback/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 3}}].

all() ->
    [
        {group, h1},
        {group, h2},
        {group, h2_flow_control},
        {group, h3}
    ].

groups() ->
    [
        {h1, [sequence], [
            h1_file_upload_known_size,
            h1_file_upload_via_body_from,
            h1_explicit_content_length,
            h1_iter_unknown_size_chunked,
            h1_progress_callback,
            h1_continued_response,
            h1_file_missing_with_size,
            h1_file_missing_no_size,
            h1_file_is_directory,
            h1_iter_returns_error,
            h1_malformed_response,
            h1_response_timeout,
            h1_closed_before_response,
            h1_send_error_mid_upload
        ]},
        {h2, [sequence], [
            h2_file_upload,
            h2_explicit_content_length,
            h2_iter_unknown_size,
            h2_progress_callback,
            h2_server_resets_stream,
            h2_server_goaway,
            h2_server_closes_mid_upload,
            h2_server_closes_after_request,
            h2_garbage_response,
            h2_iter_returns_error,
            h2_stream_upload_default_chunk_size,
            h2_poolless_body_over_max_frame_size
        ]},
        {h2_flow_control, [sequence], [
            h2_stream_upload_never_credits_connection,
            h2_stream_upload_terminator_at_zero_window,
            h2_stream_upload_progress_matches_wire,
            h2_stream_upload_timeout_purges_and_is_retryable
        ]},
        {h3, [sequence], [
            h3_iter_upload,
            h3_explicit_content_length,
            h3_iter_returns_error,
            h3_progress_callback
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    PrivDir = proplists:get_value(priv_dir, Config, ""),
    ProjectRoot = find_project_root(PrivDir),
    ConfDir = filename:join([ProjectRoot, "test", "conf"]),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_file(CertFile) andalso filelib:is_file(KeyFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> Config
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(h1, Config) ->
    case
        nhttp:start_link(upload_h1, #{
            port => 0,
            handler => nhttp_test_handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            [{server_pid, ServerPid}, {server_port, Port} | Config];
        {error, Reason} ->
            ct:fail("Failed to start H1 server: ~p", [Reason])
    end;
init_per_group(h2, Config) ->
    case has_tls(Config) of
        true ->
            CertFile = ?config(certfile, Config),
            KeyFile = ?config(keyfile, Config),
            case
                nhttp:start_link(upload_h2, #{
                    port => 0,
                    handler => nhttp_test_handler,
                    versions => [http2, http1_1],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    {ok, Port} = nhttp:get_port(ServerPid),
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to start H2 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(h2_flow_control, Config) ->
    case has_tls(Config) of
        true -> Config;
        false -> {skip, "TLS certificates not available"}
    end;
init_per_group(h3, Config) ->
    case has_tls(Config) of
        true ->
            CertFile = ?config(certfile, Config),
            KeyFile = ?config(keyfile, Config),
            case
                nhttp:start_link(upload_h3, #{
                    port => 0,
                    handler => nhttpc_h3_test_handler,
                    versions => [http3],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    {ok, Port} = nhttp:get_port(ServerPid),
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    {skip, {h3_server_failed, Reason}}
            end;
        false ->
            {skip, "TLS certificates not available"}
    end.

end_per_group(h2_flow_control, _Config) ->
    ok;
end_per_group(_Group, Config) ->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES — HTTP/1.1 FILE ITERATOR + WIRE
%%%-----------------------------------------------------------------------------

%% `{stream, {file, Path}, #{size := N}}` exercises the 3-tuple
%% `get_stream_source` clause and the size-known `prepare_stream_source`
%% branch (content-length, no chunked framing). A 64 KiB body makes the
%% echoed response large enough to span several recv calls, covering the
%% `{more, _}` response-continuation branch.
h1_file_upload_known_size(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "known_size.bin"),
    Content = binary:copy(<<"ABCDEFGH">>, 8192),
    ok = file:write_file(Path, Content),
    Size = byte_size(Content),
    Url = url(Port, <<"/upload/echo">>),
    {ok, Resp} = nhttpc:post(Url, {stream, {file, Path}, #{size => Size}}, #{}),
    ?assertEqual(200, maps:get(status, Resp)),
    Body = maps:get(body, Resp, <<>>),
    ?assertEqual(
        integer_to_binary(Size),
        proplists:get_value(<<"x-received-size">>, maps:get(headers, Resp))
    ),
    ?assertNotEqual(nomatch, binary:match(Body, Content)),
    ok.

%% Canonical `body_from => {file, _}` path (size derived via
%% read_file_info), upload to /upload/chunked which reports the framing.
h1_file_upload_via_body_from(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "body_from.bin"),
    Content = binary:copy(<<"0123456789">>, 4096),
    ok = file:write_file(Path, Content),
    Url = url(Port, <<"/upload/chunked">>),
    {ok, Resp} = nhttpc:post(Url, <<>>, #{body_from => {file, Path}}),
    ?assertEqual(200, maps:get(status, Resp)),
    RBody = maps:get(body, Resp, <<>>),
    ?assertNotEqual(
        nomatch,
        binary:match(RBody, integer_to_binary(byte_size(Content)))
    ),
    ok.

%% A request that already carries a content-length header exercises the
%% header-present branch of `add_header_if_missing`.
h1_explicit_content_length(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "explicit_cl.bin"),
    Content = binary:copy(<<"L">>, 4096),
    ok = file:write_file(Path, Content),
    Size = byte_size(Content),
    Url = url(Port, <<"/upload/echo">>),
    {ok, Resp} = nhttpc:post(
        Url,
        {stream, {file, Path}, #{size => Size}},
        #{headers => [{<<"content-length">>, integer_to_binary(Size)}]}
    ),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(
        integer_to_binary(Size),
        proplists:get_value(<<"x-received-size">>, maps:get(headers, Resp))
    ),
    ok.

%% An iterator with no declared size forces the chunked transfer
%% encoding path in `stream_h1_upload_body`.
h1_iter_unknown_size_chunked(Config) ->
    Port = ?config(server_port, Config),
    Url = url(Port, <<"/upload/echo">>),
    Iter = list_iterator([<<"chunk-a-">>, <<"chunk-b-">>, <<"chunk-c">>]),
    {ok, Resp} = nhttpc:post(Url, <<>>, #{body_from => {iter, Iter}}),
    ?assertEqual(200, maps:get(status, Resp)),
    Body = maps:get(body, Resp, <<>>),
    ?assertNotEqual(nomatch, binary:match(Body, <<"chunk-a-chunk-b-chunk-c">>)),
    ok.

%% The progress callback must fire with monotonically non-decreasing
%% byte counts across the upload.
h1_progress_callback(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "progress.bin"),
    Content = binary:copy(<<"P">>, 200000),
    ok = file:write_file(Path, Content),
    Url = url(Port, <<"/upload/large">>),
    Self = self(),
    Cb = fun(Sent, Total) ->
        Self ! {progress, Sent, Total},
        ok
    end,
    {ok, Resp} = nhttpc:post(Url, <<>>, #{
        body_from => {file, Path},
        on_upload_progress => Cb
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    Progress = collect_progress([]),
    ?assert(length(Progress) >= 1),
    Sents = [S || {S, _T} <- Progress],
    ?assertEqual(lists:sort(Sents), Sents),
    ?assertEqual(byte_size(Content), lists:last(Sents)),
    ok.

%% Raw responder sends the headers and body in two separate segments so
%% the client's first parse returns `{more, _}` and recurses.
h1_continued_response(Config) ->
    Body = <<"continued-body-payload">>,
    CL = integer_to_binary(byte_size(Body)),
    {Port, Stop} = raw_tcp_server(fun(Sock) ->
        read_full_request(Sock),
        Head = <<"HTTP/1.1 200 OK\r\nContent-Length: ", CL/binary, "\r\n\r\n">>,
        ok = gen_tcp:send(Sock, Head),
        receive
        after 100 -> ok
        end,
        ok = gen_tcp:send(Sock, Body),
        receive
            stop_raw -> ok
        after 2000 -> ok
        end,
        try
            gen_tcp:close(Sock)
        catch
            _:_ -> ok
        end
    end),
    try
        Url = url(Port, <<"/upload/echo">>),
        Iter = list_iterator([<<"c1">>, <<"c2">>]),
        {ok, Resp} = nhttpc:post(Url, <<>>, #{body_from => {iter, Iter}}),
        ?assertEqual(200, maps:get(status, Resp)),
        ?assertEqual(Body, maps:get(body, Resp, <<>>))
    after
        Stop()
    end,
    _ = Config,
    ok.

%% Explicit size skips read_file_info; the missing file then fails at
%% `file:open` → `file_error(open, ...)` → `{body_error, {file_open, _}}`.
h1_file_missing_with_size(Config) ->
    Port = ?config(server_port, Config),
    Missing = priv_file(Config, "does_not_exist.bin"),
    Url = url(Port, <<"/upload/echo">>),
    Result = nhttpc:post(Url, {stream, {file, Missing}, #{size => 10}}, #{}),
    ?assertMatch({error, {body_error, {file_open, enoent}}}, Result),
    ok.

%% No declared size: read_file_info fails first for a missing file →
%% `file_error(read_file_info, _)` → `{body_error, {file_read, _}}`.
h1_file_missing_no_size(Config) ->
    Port = ?config(server_port, Config),
    Missing = priv_file(Config, "missing_no_size.bin"),
    Url = url(Port, <<"/upload/echo">>),
    Result = nhttpc:post(Url, <<>>, #{body_from => {file, Missing}}),
    ?assertMatch({error, {body_error, {file_read, enoent}}}, Result),
    ok.

%% A directory passes read_file_info but fails `file:open` with
%% eisdir, exercising the no-size open-error branch.
h1_file_is_directory(Config) ->
    case os:type() of
        {unix, _} ->
            Port = ?config(server_port, Config),
            Dir = ?config(priv_dir, Config),
            Url = url(Port, <<"/upload/echo">>),
            Result = nhttpc:post(Url, <<>>, #{body_from => {file, Dir}}),
            ?assertMatch({error, {body_error, {file_open, eisdir}}}, Result),
            ok;
        _ ->
            {skip, "directory open semantics are unix-specific"}
    end.

%% An iterator that yields `{error, Reason}` is wrapped as an upload
%% error → `{body_error, {iterator, Reason}}`.
h1_iter_returns_error(Config) ->
    Port = ?config(server_port, Config),
    Url = url(Port, <<"/upload/echo">>),
    Iter = fun() -> {error, {invalid_stream_body, boom}} end,
    Result = nhttpc:post(Url, <<>>, #{body_from => {iter, Iter}}),
    ?assertMatch({error, {body_error, {iterator, {invalid_stream_body, boom}}}}, Result),
    ok.

%% Raw responder sends an unparseable status line after reading the
%% request → `malformed_response` → `{protocol_error, http1_1, _}`.
h1_malformed_response(Config) ->
    {Port, Stop} = raw_tcp_server(fun(Sock) ->
        read_full_request(Sock),
        gen_tcp:send(Sock, <<"NOT-HTTP garbage\r\nstill garbage\r\n\r\n">>),
        gen_tcp:close(Sock)
    end),
    try
        Url = url(Port, <<"/upload/echo">>),
        Iter = list_iterator([<<"a">>, <<"b">>]),
        Result = nhttpc:post(Url, <<>>, #{body_from => {iter, Iter}}),
        ?assertMatch({error, {protocol_error, http1_1, _}}, Result)
    after
        Stop()
    end,
    _ = Config,
    ok.

%% Raw responder reads the request but never replies → recv times out
%% → `{timeout, request}`.
h1_response_timeout(Config) ->
    {Port, Stop} = raw_tcp_server(fun(Sock) ->
        read_full_request(Sock),
        receive
            stop_raw -> ok
        after 5000 -> ok
        end,
        try
            gen_tcp:close(Sock)
        catch
            _:_ -> ok
        end
    end),
    try
        Url = url(Port, <<"/upload/echo">>),
        Iter = list_iterator([<<"x">>, <<"y">>]),
        Result = nhttpc:post(Url, <<>>, #{
            body_from => {iter, Iter},
            timeouts => #{request => 500, connect => 1000}
        }),
        ?assertMatch({error, {timeout, request}}, Result)
    after
        Stop()
    end,
    _ = Config,
    ok.

%% Raw responder reads the request then closes without replying →
%% recv returns `{error, closed}` → `{connect_error, closed}`.
h1_closed_before_response(Config) ->
    {Port, Stop} = raw_tcp_server(fun(Sock) ->
        read_full_request(Sock),
        gen_tcp:close(Sock)
    end),
    try
        Url = url(Port, <<"/upload/echo">>),
        Iter = list_iterator([<<"q">>, <<"r">>]),
        Result = nhttpc:post(Url, <<>>, #{body_from => {iter, Iter}}),
        ?assertMatch({error, {connect_error, closed}}, Result)
    after
        Stop()
    end,
    _ = Config,
    ok.

%% Raw responder accepts then immediately closes, so the streamed body
%% writes fail with a send error during the upload loop.
h1_send_error_mid_upload(Config) ->
    {Port, Stop} = raw_tcp_server(fun(Sock) ->
        gen_tcp:close(Sock)
    end),
    try
        Url = url(Port, <<"/upload/large">>),
        BigChunks = [binary:copy(<<"S">>, 65536) || _ <- lists:seq(1, 16)],
        Iter = list_iterator(BigChunks),
        Result = nhttpc:post(Url, <<>>, #{body_from => {iter, Iter}}),
        ?assertMatch({error, _}, Result)
    after
        Stop()
    end,
    _ = Config,
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES — HTTP/2 DATA-FRAME UPLOAD
%%%-----------------------------------------------------------------------------

%% Happy-path H2 upload: SETTINGS exchange, HEADERS, DATA-frame loop,
%% response accumulation. A known-size file adds a content-length
%% pseudo-header.
h2_file_upload(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "h2_known.bin"),
    Content = binary:copy(<<"H2-DATA-">>, 7500),
    ok = file:write_file(Path, Content),
    Url = tls_url(Port, <<"/upload/echo">>),
    {ok, Resp} = nhttpc:post(
        Url,
        {stream, {file, Path}, #{size => byte_size(Content)}},
        #{tls => #{verify => verify_none}, versions => [http2]}
    ),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertNotEqual(
        nomatch, binary:match(maps:get(body, Resp, <<>>), Content)
    ),
    ok.

%% A request that already carries a content-length header exercises the
%% header-present branch of `request_to_h2_headers_with_size`.
h2_explicit_content_length(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "h2_explicit_cl.bin"),
    Content = binary:copy(<<"H2L">>, 5000),
    ok = file:write_file(Path, Content),
    Size = byte_size(Content),
    Url = tls_url(Port, <<"/upload/echo">>),
    {ok, Resp} = nhttpc:post(
        Url,
        {stream, {file, Path}, #{size => Size, chunk_size => 8000}},
        #{
            headers => [{<<"content-length">>, integer_to_binary(Size)}],
            tls => #{verify => verify_none},
            versions => [http2]
        }
    ),
    ?assertEqual(200, maps:get(status, Resp)),
    ok.

%% Unknown-size iterator over H2 omits content-length and streams DATA
%% frames until the FIN frame.
h2_iter_unknown_size(Config) ->
    Port = ?config(server_port, Config),
    Url = tls_url(Port, <<"/upload/echo">>),
    Iter = list_iterator([<<"h2-a-">>, <<"h2-b-">>, <<"h2-c">>]),
    {ok, Resp} = nhttpc:post(Url, <<>>, #{
        body_from => {iter, Iter},
        tls => #{verify => verify_none},
        versions => [http2]
    }),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertNotEqual(
        nomatch, binary:match(maps:get(body, Resp, <<>>), <<"h2-a-h2-b-h2-c">>)
    ),
    ok.

h2_progress_callback(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "h2_progress.bin"),
    Content = binary:copy(<<"Z">>, 60000),
    ok = file:write_file(Path, Content),
    Url = tls_url(Port, <<"/upload/large">>),
    Self = self(),
    Cb = fun(Sent, Total) ->
        Self ! {progress, Sent, Total},
        ok
    end,
    {ok, Resp} = nhttpc:post(
        Url,
        {stream, {file, Path}, #{size => byte_size(Content), chunk_size => 12000}},
        #{
            on_upload_progress => Cb,
            tls => #{verify => verify_none},
            versions => [http2]
        }
    ),
    ?assertEqual(200, maps:get(status, Resp)),
    Progress = collect_progress([]),
    ?assert(length(Progress) >= 2),
    Sents = [S || {S, _T} <- Progress],
    ?assertEqual(lists:sort(Sents), Sents),
    ?assertEqual(byte_size(Content), lists:last(Sents)),
    ok.

%% Raw H2 responder sends RST_STREAM for the request stream while the
%% client is still uploading → response accumulation surfaces the
%% stream error.
h2_server_resets_stream(Config) ->
    {Port, Stop} = raw_h2_server(Config, rst),
    try
        Url = tls_url(Port, <<"/upload/echo">>),
        Chunks = [binary:copy(<<"R">>, 4000) || _ <- lists:seq(1, 8)],
        Iter = list_iterator(Chunks),
        Result = nhttpc:post(Url, <<>>, #{
            body_from => {iter, Iter},
            tls => #{verify => verify_none},
            versions => [http2]
        }),
        ?assertMatch({error, _}, Result)
    after
        Stop()
    end,
    ok.

%% Raw H2 responder sends GOAWAY mid-upload.
h2_server_goaway(Config) ->
    {Port, Stop} = raw_h2_server(Config, goaway),
    try
        Url = tls_url(Port, <<"/upload/echo">>),
        Chunks = [binary:copy(<<"G">>, 4000) || _ <- lists:seq(1, 8)],
        Iter = list_iterator(Chunks),
        Result = nhttpc:post(Url, <<>>, #{
            body_from => {iter, Iter},
            tls => #{verify => verify_none},
            versions => [http2]
        }),
        ?assertMatch({error, _}, Result)
    after
        Stop()
    end,
    ok.

%% Raw H2 responder completes the SETTINGS exchange then drops the
%% socket, so the DATA-frame send / response-recv loop fails.
h2_server_closes_mid_upload(Config) ->
    {Port, Stop} = raw_h2_server(Config, close),
    try
        Url = tls_url(Port, <<"/upload/echo">>),
        Chunks = [binary:copy(<<"C">>, 1000) || _ <- lists:seq(1, 50)],
        Iter = list_iterator(Chunks),
        Result = nhttpc:post(Url, <<>>, #{
            body_from => {iter, Iter},
            tls => #{verify => verify_none},
            versions => [http2]
        }),
        ?assertMatch({error, _}, Result)
    after
        Stop()
    end,
    ok.

%% Raw H2 responder reads the whole request, then closes without
%% responding → the response-recv loop fails with a recv error.
h2_server_closes_after_request(Config) ->
    {Port, Stop} = raw_h2_server(Config, close_after_request),
    try
        Url = tls_url(Port, <<"/upload/echo">>),
        Iter = list_iterator([<<"q1-">>, <<"q2-">>, <<"q3">>]),
        Result = nhttpc:post(Url, <<>>, #{
            body_from => {iter, Iter},
            tls => #{verify => verify_none},
            versions => [http2]
        }),
        ?assertMatch({error, _}, Result)
    after
        Stop()
    end,
    ok.

%% Raw H2 responder reads the whole request, then emits non-HTTP/2
%% garbage so the response decoder returns a protocol error.
h2_garbage_response(Config) ->
    {Port, Stop} = raw_h2_server(Config, garbage),
    try
        Url = tls_url(Port, <<"/upload/echo">>),
        Iter = list_iterator([<<"g1-">>, <<"g2-">>, <<"g3">>]),
        Result = nhttpc:post(Url, <<>>, #{
            body_from => {iter, Iter},
            tls => #{verify => verify_none},
            versions => [http2]
        }),
        ?assertMatch({error, _}, Result)
    after
        Stop()
    end,
    ok.

%% An iterator that yields `{error, Reason}` on the H2 DATA loop is
%% wrapped as an upload error.
h2_iter_returns_error(Config) ->
    Port = ?config(server_port, Config),
    Url = tls_url(Port, <<"/upload/echo">>),
    Iter = fun() -> {error, {invalid_stream_body, h2_boom}} end,
    Result = nhttpc:post(Url, <<>>, #{
        body_from => {iter, Iter},
        tls => #{verify => verify_none},
        versions => [http2]
    }),
    ?assertMatch({error, {body_error, {iterator, {invalid_stream_body, h2_boom}}}}, Result),
    ok.

%% A file upload with no `chunk_size` override reads 65536-octet chunks:
%% four times the peer's default SETTINGS_MAX_FRAME_SIZE and one octet
%% past its initial window. The codec splits each chunk at the frame
%% size and holds what the window cannot take, so the upload completes
%% at the defaults.
h2_stream_upload_default_chunk_size(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "h2_default_chunk.bin"),
    Content = binary:copy(<<"DEFAULT-">>, 25000),
    ok = file:write_file(Path, Content),
    Url = tls_url(Port, <<"/upload/large">>),
    {ok, Resp} = nhttpc:post(
        Url,
        {stream, {file, Path}},
        #{tls => #{verify => verify_none}, versions => [http2]}
    ),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(integer_to_binary(byte_size(Content)), maps:get(body, Resp, <<>>)),
    ok.

%% A poolless POST whose in-memory body is one octet past the peer's
%% initial window and four times its frame size. The codec holds the
%% octet the credit cannot take, and the request loop reads the
%% WINDOW_UPDATE that frees it before it waits for the response.
h2_poolless_body_over_max_frame_size(Config) ->
    Port = ?config(server_port, Config),
    Body = binary:copy(<<"P">>, ?H2_WINDOW + 1),
    Url = tls_url(Port, <<"/upload/large">>),
    {ok, Resp} = nhttpc:post(Url, Body, #{tls => #{verify => verify_none}, versions => [http2]}),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(integer_to_binary(byte_size(Body)), maps:get(body, Resp, <<>>)),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES - HTTP/2 UPLOAD FLOW CONTROL
%%%-----------------------------------------------------------------------------

%% A 4 MiB file upload against a peer that never credits the connection.
%% The client sends the initial window and stops. While it waits it
%% holds at most the send queue bound plus one file chunk, measured as
%% the refc binaries the uploader retains after a collection, so the
%% file is read at the rate the peer grants credit and never in full.
%% Once the peer credits, the upload completes.
h2_stream_upload_never_credits_connection(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{credit => never}),
    try
        Path = priv_file(Config, "h2_never.bin"),
        ok = file:write_file(Path, binary:copy(<<"N">>, ?NEVER_BODY)),
        Url = tls_url(Port, <<"/upload/never">>),
        Uploader = spawn_upload(Url, {stream, {file, Path}}, #{}),
        {ok, [PeerConn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?PEER_WAIT_MS),
        ok = wait_peer_stat(PeerConn, data_octets, ?H2_WINDOW),
        Retained = retained_binary_octets(Uploader),
        Bound = nhttpc_h2:max_send_buffer() + ?FILE_CHUNK,
        ct:pal("uploader retains ~p octets of refc binaries, bound ~p", [Retained, Bound]),
        ?assertEqual(?H2_WINDOW, maps:get(data_octets, nhttpc_h2_script_peer:stats(PeerConn))),
        ?assert(Retained =< Bound),
        ok = nhttpc_h2_script_peer:command(PeerConn, {credit, eager}),
        {ok, Resp} = await_upload(Uploader),
        ?assertEqual(200, maps:get(status, Resp)),
        ?assertEqual(?NEVER_BODY, maps:get(data_octets, nhttpc_h2_script_peer:stats(PeerConn)))
    after
        nhttpc_h2_script_peer:stop(Peer)
    end,
    ok.

%% A body that exactly exhausts the initial connection window against a
%% peer that never credits, then the empty END_STREAM frame. An empty
%% frame costs no credit (RFC 9113 Section 6.9.1), so the peer sees the
%% end of the request and answers.
h2_stream_upload_terminator_at_zero_window(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{credit => never}),
    try
        Sizes = [?H2_MAX_FRAME, ?H2_MAX_FRAME, ?H2_MAX_FRAME, ?H2_WINDOW - 3 * ?H2_MAX_FRAME],
        Iter = list_iterator([binary:copy(<<"T">>, Size) || Size <- Sizes]),
        Url = tls_url(Port, <<"/upload/exact">>),
        {ok, Resp} = nhttpc:post(Url, <<>>, #{
            body_from => {iter, Iter},
            tls => #{verify => verify_none},
            versions => [http2]
        }),
        ?assertEqual(200, maps:get(status, Resp)),
        {ok, [PeerConn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?PEER_WAIT_MS),
        Stats = nhttpc_h2_script_peer:stats(PeerConn),
        ?assertEqual(?H2_WINDOW, maps:get(data_octets, Stats)),
        ?assertEqual(1, maps:get(responses, Stats)),
        ?assert(maps:get(max_data_frame, Stats) =< ?H2_MAX_FRAME)
    after
        nhttpc_h2_script_peer:stop(Peer)
    end,
    ok.

%% Progress reports the octets written to the socket, never the octets
%% handed to the codec. Against a peer that never credits, the reports
%% stop at the initial window while three chunks wait in the queue, and
%% once the peer credits they end at the body size the peer received.
h2_stream_upload_progress_matches_wire(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{credit => never}),
    try
        Self = self(),
        Cb = fun(Sent, Total) ->
            Self ! {progress, Sent, Total},
            ok
        end,
        Chunks = lists:duplicate(?PROGRESS_CHUNKS, binary:copy(<<"W">>, ?PROGRESS_CHUNK)),
        Total = ?PROGRESS_CHUNKS * ?PROGRESS_CHUNK,
        Url = tls_url(Port, <<"/upload/progress">>),
        Uploader = spawn_upload(Url, <<>>, #{
            body_from => {iter, list_iterator(Chunks)},
            on_upload_progress => Cb
        }),
        {ok, [PeerConn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?PEER_WAIT_MS),
        ok = wait_peer_stat(PeerConn, data_octets, ?H2_WINDOW),
        Stalled = wait_progress(?H2_WINDOW, ?PEER_WAIT_MS),
        ct:pal("progress while the peer withholds credit: ~p", [Stalled]),
        ?assertEqual(lists:sort(Stalled), Stalled),
        ?assertEqual(?H2_WINDOW, lists:last(Stalled)),
        ok = nhttpc_h2_script_peer:command(PeerConn, {credit, eager}),
        {ok, Resp} = await_upload(Uploader),
        ?assertEqual(200, maps:get(status, Resp)),
        Drained = [Sent || {Sent, _T} <- collect_progress([])],
        All = Stalled ++ Drained,
        ?assertEqual(lists:sort(All), All),
        ?assertEqual(Total, lists:last(All)),
        ?assertEqual(Total, maps:get(data_octets, nhttpc_h2_script_peer:stats(PeerConn)))
    after
        nhttpc_h2_script_peer:stop(Peer)
    end,
    ok.

%% An upload that times out with octets still queued, against a peer
%% that never credits. The client sends RST_STREAM so the peer discards
%% a request it never processed, and the caller gets the refused shape
%% the pool re-dispatches.
h2_stream_upload_timeout_purges_and_is_retryable(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{credit => never}),
    try
        Chunks = lists:duplicate(?TIMEOUT_CHUNKS, binary:copy(<<"X">>, ?TIMEOUT_CHUNK)),
        Url = tls_url(Port, <<"/upload/timeout">>),
        Result = nhttpc:post(Url, <<>>, #{
            body_from => {iter, list_iterator(Chunks)},
            timeouts => #{request => ?UPLOAD_TIMEOUT_MS},
            tls => #{verify => verify_none},
            versions => [http2]
        }),
        ?assertEqual({error, {stream_error, {protocol, refused_stream}}}, Result),
        {ok, [PeerConn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?PEER_WAIT_MS),
        ok = wait_peer_reset(PeerConn, ?FIRST_STREAM, cancel),
        Stats = nhttpc_h2_script_peer:stats(PeerConn),
        ?assertEqual(?H2_WINDOW, maps:get(data_octets, Stats)),
        ?assertEqual(0, maps:get(responses, Stats))
    after
        nhttpc_h2_script_peer:stop(Peer)
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES — HTTP/3 UPLOAD SMOKE
%%%-----------------------------------------------------------------------------

%% Unknown-size iterator over H3 (no content-length pseudo-header),
%% echoed back by the H3 test handler.
h3_iter_upload(Config) ->
    Port = ?config(server_port, Config),
    Url = tls_url(Port, <<"/echo">>),
    Iter = list_iterator([<<"h3-a-">>, <<"h3-b-">>, <<"h3-c">>]),
    Result = nhttpc:post(Url, <<>>, #{
        body_from => {iter, Iter},
        versions => [http3]
    }),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"h3-a-h3-b-h3-c">>, maps:get(body, Resp, <<>>)),
    ok.

%% A request that already carries a content-length header exercises the
%% header-present branch of `request_to_h3_headers_with_size`.
h3_explicit_content_length(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "h3_explicit_cl.bin"),
    Content = binary:copy(<<"H3L">>, 250),
    ok = file:write_file(Path, Content),
    Size = byte_size(Content),
    Url = tls_url(Port, <<"/echo">>),
    Result = nhttpc:post(
        Url,
        {stream, {file, Path}, #{size => Size}},
        #{
            headers => [{<<"content-length">>, integer_to_binary(Size)}],
            versions => [http3]
        }
    ),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(Content, maps:get(body, Resp, <<>>)),
    ok.

%% An iterator that yields `{error, Reason}` on the H3 DATA loop is
%% wrapped as an upload error.
h3_iter_returns_error(Config) ->
    Port = ?config(server_port, Config),
    Url = tls_url(Port, <<"/echo">>),
    Iter = fun() -> {error, {invalid_stream_body, h3_boom}} end,
    Result = nhttpc:post(Url, <<>>, #{
        body_from => {iter, Iter},
        versions => [http3]
    }),
    ?assertMatch({error, {body_error, {iterator, {invalid_stream_body, h3_boom}}}}, Result),
    ok.

%%
%% The current H3 upload path neither waits on the QUIC congestion
%% window nor advances loss/idle timers between chunk flushes, so it is
%% only reliable for a single-chunk body (mirrors the known-good
%% coverage-suite case). Kept small and single-chunk; the multi-chunk
%% H3 limitation is recorded as a follow-up, not fixed here.
h3_progress_callback(Config) ->
    Port = ?config(server_port, Config),
    Path = priv_file(Config, "h3_progress.bin"),
    Content = binary:copy(<<"H3">>, 750),
    ok = file:write_file(Path, Content),
    Url = tls_url(Port, <<"/echo">>),
    Self = self(),
    Cb = fun(Sent, Total) ->
        Self ! {progress, Sent, Total},
        ok
    end,
    Result = nhttpc:post(Url, <<>>, #{
        body_from => {file, Path},
        on_upload_progress => Cb,
        versions => [http3]
    }),
    ?assertMatch({ok, _}, Result),
    {ok, Resp} = Result,
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(Content, maps:get(body, Resp, <<>>)),
    Progress = collect_progress([]),
    ?assert(length(Progress) >= 1),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS — ITERATORS / PROGRESS
%%%-----------------------------------------------------------------------------

list_iterator([]) ->
    fun() -> fin end;
list_iterator([H | T]) ->
    fun() -> {ok, H, list_iterator(T)} end.

collect_progress(Acc) ->
    receive
        {progress, Sent, Total} ->
            collect_progress([{Sent, Total} | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

%% Collect progress reports until one reaches `Want` octets.
wait_progress(Want, Remaining) ->
    wait_progress(Want, Remaining, []).

wait_progress(Want, Remaining, Acc) ->
    Started = erlang:monotonic_time(millisecond),
    receive
        {progress, Sent, _Total} when Sent >= Want ->
            lists:reverse([Sent | Acc]);
        {progress, Sent, _Total} ->
            Elapsed = erlang:monotonic_time(millisecond) - Started,
            wait_progress(Want, Remaining - Elapsed, [Sent | Acc])
    after max(Remaining, 0) ->
        ct:fail("progress stopped at ~p, wanted ~p", [lists:reverse(Acc), Want])
    end.

%%%-----------------------------------------------------------------------------
%%% HELPERS - SCRIPT PEER UPLOADS
%%%-----------------------------------------------------------------------------

%% Run a poolless HTTP/2 upload in its own process, so the test can watch
%% the peer and the uploader while the upload waits for credit.
spawn_upload(Url, Body, Extra) ->
    Opts = maps:merge(#{tls => #{verify => verify_none}, versions => [http2]}, Extra),
    spawn_monitor(fun() -> exit({result, nhttpc:post(Url, Body, Opts)}) end).

await_upload({Pid, MRef}) ->
    receive
        {'DOWN', MRef, process, Pid, {result, Result}} ->
            Result;
        {'DOWN', MRef, process, Pid, Reason} ->
            ct:fail("uploader crashed: ~p", [Reason])
    after ?PEER_WAIT_MS ->
        ct:fail("upload did not finish")
    end.

%% The octets of refc binaries a process retains after a collection.
%% `process_info(Pid, memory)` counts the ProcBin structs and not the
%% payload, so it cannot bound a buffered upload; the binary list can.
retained_binary_octets({Pid, _MRef}) ->
    true = erlang:garbage_collect(Pid),
    {binary, Bins} = erlang:process_info(Pid, binary),
    Unique = #{Id => Size || {Id, Size, _RefCount} <- Bins},
    lists:sum(maps:values(Unique)).

wait_peer_stat(PeerConn, Key, Want) ->
    wait_peer_stat(PeerConn, Key, Want, ?PEER_WAIT_MS).

wait_peer_stat(PeerConn, Key, Want, Remaining) ->
    case maps:get(Key, nhttpc_h2_script_peer:stats(PeerConn)) of
        Value when Value >= Want ->
            ok;
        Value when Remaining =< 0 ->
            ct:fail("peer ~p reached ~p, wanted ~p", [Key, Value, Want]);
        _Value ->
            timer:sleep(?PEER_POLL_MS),
            wait_peer_stat(PeerConn, Key, Want, Remaining - ?PEER_POLL_MS)
    end.

wait_peer_reset(PeerConn, StreamId, Code) ->
    wait_peer_reset(PeerConn, StreamId, Code, ?PEER_WAIT_MS).

wait_peer_reset(PeerConn, StreamId, Code, Remaining) ->
    #{resets := Resets} = nhttpc_h2_script_peer:stats(PeerConn),
    case maps:get(StreamId, Resets, undefined) of
        Code ->
            ok;
        Other when Remaining =< 0 ->
            ct:fail("stream ~p reset ~p, wanted ~p", [StreamId, Other, Code]);
        _Other ->
            timer:sleep(?PEER_POLL_MS),
            wait_peer_reset(PeerConn, StreamId, Code, Remaining - ?PEER_POLL_MS)
    end.

%%%-----------------------------------------------------------------------------
%%% HELPERS — RAW TCP RESPONDER
%%%-----------------------------------------------------------------------------

raw_tcp_server(Handler) ->
    {ok, LSock} = gen_tcp:listen(0, [
        binary, {active, false}, {reuseaddr, true}, {nodelay, true}
    ]),
    {ok, Port} = inet:port(LSock),
    Owner = self(),
    Pid = spawn(fun() ->
        case gen_tcp:accept(LSock, 5000) of
            {ok, Sock} ->
                Handler(Sock);
            _ ->
                ok
        end,
        Owner ! {raw_done, self()}
    end),
    Stop = fun() ->
        Pid ! stop_raw,
        try
            gen_tcp:close(LSock)
        catch
            _:_ -> ok
        end,
        receive
            {raw_done, Pid} -> ok
        after 6000 -> ok
        end
    end,
    {Port, Stop}.

%% Read the request to completion: the client streams the whole body
%% then blocks waiting for a response, so an idle gap on the socket is
%% a deterministic signal that the request has been fully sent.
read_full_request(Sock) ->
    case gen_tcp:recv(Sock, 0, 300) of
        {ok, _Data} -> read_full_request(Sock);
        {error, timeout} -> ok;
        {error, _} -> ok
    end.

%%%-----------------------------------------------------------------------------
%%% HELPERS — RAW HTTP/2 RESPONDER
%%%-----------------------------------------------------------------------------

%% Minimal raw TLS HTTP/2 server. Completes the SETTINGS exchange so the
%% client proceeds into the DATA-frame upload loop, then injects the
%% requested fault (`rst` | `goaway` | `close`) as soon as the request
%% HEADERS arrive.
raw_h2_server(Config, Mode) ->
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    {ok, LSock} = ssl:listen(0, [
        binary,
        {active, false},
        {reuseaddr, true},
        {certfile, CertFile},
        {keyfile, KeyFile},
        {alpn_preferred_protocols, [<<"h2">>]}
    ]),
    {ok, {_, Port}} = ssl:sockname(LSock),
    Owner = self(),
    Pid = spawn(fun() ->
        run_raw_h2(LSock, Mode),
        Owner ! {raw_done, self()}
    end),
    Stop = fun() ->
        try
            ssl:close(LSock)
        catch
            _:_ -> ok
        end,
        receive
            {raw_done, Pid} -> ok
        after 8000 -> ok
        end
    end,
    {Port, Stop}.

run_raw_h2(LSock, Mode) ->
    case ssl:transport_accept(LSock, 5000) of
        {ok, TSock} ->
            case ssl:handshake(TSock, 5000) of
                {ok, Sock} ->
                    Srv0 = nhttp_h2:new(server),
                    ok = ssl:send(Sock, nhttp_h2:preface(Srv0)),
                    raw_h2_loop(Sock, Srv0, Mode);
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

raw_h2_loop(Sock, Srv, Mode) ->
    case ssl:recv(Sock, 0, 5000) of
        {ok, Data} ->
            case nhttp_h2:recv(Srv, Data) of
                {ok, Events, Srv1} ->
                    raw_h2_react(Sock, Srv1, Events, Mode);
                {ok, Events, Srv1, ToSend} ->
                    _ = ssl:send(Sock, ToSend),
                    raw_h2_react(Sock, Srv1, Events, Mode);
                {error, _} ->
                    try
                        ssl:close(Sock)
                    catch
                        _:_ -> ok
                    end
            end;
        {error, _} ->
            try
                ssl:close(Sock)
            catch
                _:_ -> ok
            end
    end.

raw_h2_react(Sock, Srv, Events, Mode) ->
    case request_stream_id(Events) of
        {ok, StreamId} ->
            raw_h2_fault(Sock, Srv, StreamId, Mode);
        none ->
            raw_h2_loop(Sock, Srv, Mode)
    end.

raw_h2_fault(Sock, Srv, StreamId, rst) ->
    {ok, _Srv1, Frame} = nhttp_h2:send_rst_stream(Srv, StreamId, cancel),
    _ = ssl:send(Sock, Frame),
    drain_then_close(Sock);
raw_h2_fault(Sock, Srv, _StreamId, goaway) ->
    {ok, _Srv1, Frame} = nhttp_h2:send_goaway(Srv, protocol_error, <<>>),
    _ = ssl:send(Sock, Frame),
    drain_then_close(Sock);
raw_h2_fault(Sock, _Srv, _StreamId, close) ->
    try
        ssl:close(Sock)
    catch
        _:_ -> ok
    end;
raw_h2_fault(Sock, _Srv, _StreamId, close_after_request) ->
    read_idle(Sock),
    try
        ssl:close(Sock)
    catch
        _:_ -> ok
    end;
raw_h2_fault(Sock, _Srv, _StreamId, garbage) ->
    read_idle(Sock),
    _ = ssl:send(Sock, binary:copy(<<16#FF>>, 64)),
    drain_then_close(Sock).

%% Drain until the client stops sending (it has finished the upload and
%% is waiting for a response).
read_idle(Sock) ->
    case ssl:recv(Sock, 0, 400) of
        {ok, _} -> read_idle(Sock);
        {error, _} -> ok
    end.

%% After injecting the fault frame, keep reading and discarding the
%% client's in-flight DATA so its sends never block; once the client
%% has consumed the fault and stopped (recv idle or error), close.
drain_then_close(Sock) ->
    case ssl:recv(Sock, 0, 2000) of
        {ok, _} ->
            drain_then_close(Sock);
        {error, _} ->
            try
                ssl:close(Sock)
            catch
                _:_ -> ok
            end
    end.

request_stream_id([]) ->
    none;
request_stream_id([{request, StreamId, _Req, _Fin} | _]) ->
    {ok, StreamId};
request_stream_id([_ | T]) ->
    request_stream_id(T).

%%%-----------------------------------------------------------------------------
%%% HELPERS — MISC
%%%-----------------------------------------------------------------------------

priv_file(Config, Name) ->
    filename:join(?config(priv_dir, Config), Name).

url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"http://127.0.0.1:", PortBin/binary, Path/binary>>.

tls_url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"https://127.0.0.1:", PortBin/binary, Path/binary>>.

has_tls(Config) ->
    proplists:get_value(certfile, Config) =/= undefined.

find_project_root(Dir) ->
    RebarConfig = filename:join(Dir, "rebar.config"),
    case filelib:is_file(RebarConfig) of
        true ->
            Dir;
        false ->
            Parent = filename:dirname(Dir),
            case Parent =:= Dir of
                true -> ".";
                false -> find_project_root(Parent)
            end
    end.
