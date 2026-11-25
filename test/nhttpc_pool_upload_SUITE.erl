-module(nhttpc_pool_upload_SUITE).

-moduledoc """
A `{stream, Source}` request body on a pooled HTTP/2 connection and on a
connection from `nhttpc:open/2`, for `nhttpc:request/2` and
`nhttpc:stream/2`, against `nhttpc_h2_script_peer`.
""".

-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% EXPORTS - CT
%%%-----------------------------------------------------------------------------
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    suite/0
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - REQUEST
%%%-----------------------------------------------------------------------------
-export([
    body_from_iterator_uploads_on_a_pool/1,
    chunk_above_the_piece_size_is_split/1,
    file_source_uploads_on_a_pool/1,
    hundred_thousand_chunks_keep_the_connection_heap_bounded/1,
    request_gets_the_response_and_the_peer_gets_each_chunk/1,
    request_on_an_open_connection/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - FLOW
%%%-----------------------------------------------------------------------------
-export([
    full_send_queue_holds_a_piece/1,
    sleeping_iterator_does_not_delay_a_second_request/1,
    uncredited_stream_stops_the_iterator/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - STREAM
%%%-----------------------------------------------------------------------------
-export([
    cancel_stops_the_iterator/1,
    connection_exit_stops_the_iterator/1,
    owner_exit_stops_the_iterator/1,
    request_timeout_stops_the_iterator/1,
    response_before_the_end_of_the_body_stops_the_iterator/1,
    stream_gets_data_before_the_iterator_ends/1,
    stream_on_an_open_connection/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - ERRORS
%%%-----------------------------------------------------------------------------
-export([
    iterator_crash_resets_the_stream/1,
    iterator_error_ends_a_stream_with_an_error_event/1,
    iterator_error_resets_the_stream/1,
    redirect_is_not_followed/1,
    refused_stream_does_not_start_the_iterator_twice/1,
    retry_does_not_call_the_iterator_again/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - REJECT
%%%-----------------------------------------------------------------------------
-export([
    invalid_body_on_a_connection_is_rejected/1,
    invalid_body_on_a_pool_is_rejected/1,
    invalid_body_on_an_http1_pool_is_rejected/1,
    invalid_stream_source_is_rejected/1,
    iterator_on_an_http1_connection_is_rejected/1,
    iterator_on_an_http1_pool_is_rejected/1,
    missing_file_is_a_body_error/1,
    oneshot_stream_rejects_a_body_that_is_not_iodata/1
]).

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(WAIT_MS, 5000).
-define(POLL_MS, 20).
-define(QUIET_MS, 200).
-define(STOP_SETTLE_MS, 100).
-define(CHUNKS, 100000).
-define(CHUNK_SIZE, 85).
-define(HEAP_BOUND_WORDS, 262144).
-define(HEAP_SAMPLE_MS, 2).
-define(LONG_TIMEOUT_MS, 120000).
-define(SHORT_TIMEOUT_MS, 300).
-define(SMALL_WINDOW, 1000).
-define(STALL_CHUNK, 100).
-define(PIECE, 16384).
-define(LARGE_CHUNK, 100000).
-define(FILE_SIZE, 200000).
-define(SECOND_REQUEST_MS, 1000).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 3}}].

all() ->
    [{group, request}, {group, flow}, {group, stream}, {group, errors}, {group, reject}].

groups() ->
    [
        {request, [], [
            request_gets_the_response_and_the_peer_gets_each_chunk,
            body_from_iterator_uploads_on_a_pool,
            request_on_an_open_connection,
            chunk_above_the_piece_size_is_split,
            file_source_uploads_on_a_pool,
            hundred_thousand_chunks_keep_the_connection_heap_bounded
        ]},
        {flow, [], [
            uncredited_stream_stops_the_iterator,
            sleeping_iterator_does_not_delay_a_second_request,
            full_send_queue_holds_a_piece
        ]},
        {stream, [], [
            stream_gets_data_before_the_iterator_ends,
            stream_on_an_open_connection,
            cancel_stops_the_iterator,
            owner_exit_stops_the_iterator,
            response_before_the_end_of_the_body_stops_the_iterator,
            connection_exit_stops_the_iterator,
            request_timeout_stops_the_iterator
        ]},
        {errors, [], [
            iterator_error_resets_the_stream,
            iterator_error_ends_a_stream_with_an_error_event,
            iterator_crash_resets_the_stream,
            retry_does_not_call_the_iterator_again,
            redirect_is_not_followed,
            refused_stream_does_not_start_the_iterator_twice
        ]},
        {reject, [], [
            iterator_on_an_http1_pool_is_rejected,
            iterator_on_an_http1_connection_is_rejected,
            invalid_stream_source_is_rejected,
            missing_file_is_a_body_error,
            invalid_body_on_a_pool_is_rejected,
            invalid_body_on_a_connection_is_rejected,
            invalid_body_on_an_http1_pool_is_rejected,
            oneshot_stream_rejects_a_body_that_is_not_iodata
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started([ssl, telemetry, nhttpc]),
    ConfDir = filename:join([code:lib_dir(nhttpc), "test", "conf"]),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_file(CertFile) andalso filelib:is_file(KeyFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> {skip, "TLS certificates not available"}
    end.

end_per_suite(_Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% REQUEST
%%%-----------------------------------------------------------------------------

request_gets_the_response_and_the_peer_gets_each_chunk(Config) ->
    {Peer, Sup, Conn} = start(pu_request, Config, #{}),
    Body = {stream, chunks([<<"one">>, <<"two">>, <<"three">>])},
    ?assertMatch(
        {ok, #{status := 200, body := <<"onetwothree">>}},
        nhttpc:request(post(<<"/mirror">>, Body), #{via => {pool, pu_request}})
    ),
    Id = stream_id(Conn, 1),
    #{data_octets := Octets, data_frames := Frames} = peer_stats(Conn),
    ?assertEqual(11, Octets),
    ?assertEqual(4, maps:get(Id, Frames)),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(get_request(<<"/body/10">>), #{via => {pool, pu_request}})
    ),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

body_from_iterator_uploads_on_a_pool(Config) ->
    {Peer, Sup, _Conn} = start(pu_body_from, Config, #{}),
    Iterator = chunks([<<"alpha">>, [<<"be">>, "ta"]]),
    ?assertMatch(
        {ok, #{status := 200, body := <<"alphabeta">>}},
        nhttpc:request(post(<<"/mirror">>, <<>>), #{
            via => {pool, pu_body_from}, body_from => {iter, Iterator}
        })
    ),
    stop(Peer, Sup).

request_on_an_open_connection(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{}),
    {ok, Connection} = nhttpc:open(#{host => <<"127.0.0.1">>, port => Port}, #{
        versions => [http2], tls => #{verify => verify_none}
    }),
    Body = {stream, chunks([<<"one">>, <<"two">>])},
    ?assertMatch(
        {ok, #{status := 200, body := <<"onetwo">>}},
        nhttpc:request(post(<<"/mirror">>, Body), #{via => {conn, Connection}})
    ),
    ok = nhttpc:close(Connection),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

chunk_above_the_piece_size_is_split(Config) ->
    {Peer, Sup, Conn} = start(pu_split, Config, #{}),
    Body = {stream, chunks([binary:copy(<<$l>>, ?LARGE_CHUNK)])},
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(post(<<"/body/0">>, Body), #{via => {pool, pu_split}})
    ),
    #{data_octets := Octets, max_data_frame := MaxFrame} = peer_stats(Conn),
    ?assertEqual(?LARGE_CHUNK, Octets),
    ?assert(MaxFrame =< ?PIECE),
    stop(Peer, Sup).

file_source_uploads_on_a_pool(Config) ->
    {Peer, Sup, Conn} = start(pu_file, Config, #{}),
    Path = filename:join(proplists:get_value(priv_dir, Config), "upload.bin"),
    ok = file:write_file(Path, binary:copy(<<$f>>, ?FILE_SIZE)),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(post(<<"/body/0">>, {stream, {file, Path}}), #{via => {pool, pu_file}})
    ),
    ?assertEqual(?FILE_SIZE, maps:get(data_octets, peer_stats(Conn))),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(post(<<"/body/0">>, <<>>), #{
            via => {pool, pu_file}, body_from => {file, Path}
        })
    ),
    ?assertEqual(2 * ?FILE_SIZE, maps:get(data_octets, peer_stats(Conn))),
    stop(Peer, Sup).

hundred_thousand_chunks_keep_the_connection_heap_bounded(Config) ->
    {Peer, Sup, Conn} = start(pu_chunks, Config, #{}),
    [{ConnPid, _Slots}] = tuple_to_list(pool_connections(pu_chunks)),
    Sampler = start_heap_sampler(ConnPid),
    Body = {stream, repeat(?CHUNKS, binary:copy(<<$c>>, ?CHUNK_SIZE))},
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(post(<<"/body/0">>, Body), #{
            via => {pool, pu_chunks}, timeouts => #{request => ?LONG_TIMEOUT_MS}
        })
    ),
    MaxHeap = stop_heap_sampler(Sampler),
    #{data_octets := Octets} = peer_stats(Conn),
    ct:pal("octets ~p, largest connection heap ~p words", [Octets, MaxHeap]),
    ?assertEqual(?CHUNKS * ?CHUNK_SIZE, Octets),
    ?assert(MaxHeap < ?HEAP_BOUND_WORDS),
    stop(Peer, Sup).

%%%-----------------------------------------------------------------------------
%%% FLOW
%%%-----------------------------------------------------------------------------

%% The peer gives each stream a window of ?SMALL_WINDOW octets and no
%% credit. The iterator is called for the octets of the window and for
%% one chunk more, which waits in the send queue of the codec.
uncredited_stream_stops_the_iterator(Config) ->
    {Peer, Sup, Conn} = start(pu_uncredited, Config, #{
        settings => #{initial_window_size => ?SMALL_WINDOW}, credit => never
    }),
    Via = #{via => {pool, pu_uncredited}},
    Calls = counters:new(1, []),
    Chunk = binary:copy(<<$s>>, ?STALL_CHUNK),
    {Uploader, _MRef} = async(fun() ->
        nhttpc:request(post(<<"/body/0">>, {stream, counted(Calls, Chunk)}), Via)
    end),
    Id = stream_id(Conn, 1),
    ok = wait_until(fun() -> maps:get(data_octets, peer_stats(Conn)) =:= ?SMALL_WINDOW end),
    timer:sleep(?QUIET_MS),
    Bound = ?SMALL_WINDOW div ?STALL_CHUNK + 1,
    ?assertEqual(Bound, counters:get(Calls, 1)),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(post(<<"/body/0">>, binary:copy(<<$b>>, ?SMALL_WINDOW div 2)), Via)
    ),
    ?assertEqual(Bound, counters:get(Calls, 1)),
    ?assertEqual(?SMALL_WINDOW, maps:get(Id, sent_by_client(Conn))),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    exit(Uploader, kill),
    ok = wait_reset(Conn, Id),
    stop(Peer, Sup).

sleeping_iterator_does_not_delay_a_second_request(Config) ->
    {Peer, Sup, Conn} = start(pu_sleeping, Config, #{}),
    Via = #{via => {pool, pu_sleeping}},
    Test = self(),
    Body = {stream, chunks_then([<<"first">>], gate(Test, fin))},
    {Uploader, MRef} = async(fun() -> nhttpc:request(post(<<"/mirror">>, Body), Via) end),
    Feeder = gate_reached(),
    Started = erlang:monotonic_time(millisecond),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assert(erlang:monotonic_time(millisecond) - Started < ?SECOND_REQUEST_MS),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    Feeder ! go,
    ?assertMatch({ok, #{status := 200, body := <<"first">>}}, async_result(Uploader, MRef)),
    stop(Peer, Sup).

%% One stream can wait in the send queue. The second upload gets no entry,
%% so the connection holds its piece and offers it again when the queue
%% has room.
full_send_queue_holds_a_piece(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{initial_window_size => ?STALL_CHUNK}, credit => never
    }),
    Sup = start_pool(pu_held, Port, #{max_queued_streams => 1}),
    {ok, [Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    Via = #{via => {pool, pu_held}},
    Chunk = binary:copy(<<$h>>, ?STALL_CHUNK),
    Upload = fun() ->
        nhttpc:request(post(<<"/body/0">>, {stream, repeat(3, Chunk)}), Via)
    end,
    {First, FirstMRef} = async(Upload),
    FirstId = stream_id(Conn, 1),
    ok = wait_until(fun() -> maps:get(FirstId, sent_by_client(Conn), 0) =:= ?STALL_CHUNK end),
    {Second, SecondMRef} = async(Upload),
    _SecondId = stream_id(Conn, 2),
    timer:sleep(?QUIET_MS),
    ?assertEqual(?STALL_CHUNK, maps:get(data_octets, peer_stats(Conn))),
    ok = nhttpc_h2_script_peer:command(Conn, {credit, eager}),
    ?assertMatch({ok, #{status := 200}}, async_result(First, FirstMRef)),
    ?assertMatch({ok, #{status := 200}}, async_result(Second, SecondMRef)),
    ?assertEqual(6 * ?STALL_CHUNK, maps:get(data_octets, peer_stats(Conn))),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

%%%-----------------------------------------------------------------------------
%%% STREAM
%%%-----------------------------------------------------------------------------

stream_gets_data_before_the_iterator_ends(Config) ->
    {Peer, Sup, _Conn} = start(pu_duplex, Config, #{}),
    Test = self(),
    Body = {stream, chunks_then([<<"ping">>], gate(Test, {ok, <<"pong">>, chunks([])}))},
    {ok, H} = nhttpc:stream(post(<<"/mirror">>, Body), #{via => {pool, pu_duplex}}),
    ?assertMatch({headers, 200, _}, event(H)),
    ?assertEqual({data, <<"ping">>, more}, event(H)),
    Feeder = gate_reached(),
    ?assertEqual(quiet, quiet(H)),
    Feeder ! go,
    ?assertEqual({data, <<"pong">>, more}, event(H)),
    ?assertEqual({done, normal}, event(H)),
    ?assertEqual(quiet, quiet(H)),
    stop(Peer, Sup).

stream_on_an_open_connection(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{}),
    {ok, Connection} = nhttpc:open(#{host => <<"127.0.0.1">>, port => Port}, #{
        versions => [http2], tls => #{verify => verify_none}
    }),
    Body = {stream, chunks([<<"ping">>])},
    {ok, H} = nhttpc:stream(post(<<"/mirror">>, Body), #{via => {conn, Connection}}),
    ?assertMatch({headers, 200, _}, event(H)),
    ?assertEqual({data, <<"ping">>, more}, event(H)),
    ?assertEqual({done, normal}, event(H)),
    ok = nhttpc:close(Connection),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

cancel_stops_the_iterator(Config) ->
    {Peer, Sup, Conn} = start(pu_cancel, Config, #{}),
    Via = #{via => {pool, pu_cancel}},
    {ok, H} = nhttpc:stream(post(<<"/manual">>, {stream, gate(self(), fin)}), Via),
    Id = stream_id(Conn, 1),
    FeederMRef = monitor(process, gate_reached()),
    ok = nhttpc:cancel(H),
    ok = down(FeederMRef),
    ok = wait_reset(Conn, Id),
    ?assertEqual(cancel, maps:get(Id, maps:get(resets, peer_stats(Conn)))),
    ?assertEqual(quiet, quiet(H)),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

owner_exit_stops_the_iterator(Config) ->
    {Peer, Sup, Conn} = start(pu_owner_exit, Config, #{}),
    Via = #{via => {pool, pu_owner_exit}},
    Test = self(),
    Owner = spawn(fun() ->
        Test ! {opened, self(), nhttpc:stream(post(<<"/manual">>, {stream, gate(Test, fin)}), Via)},
        receive
            stop -> ok
        end
    end),
    receive
        {opened, Owner, {ok, _H}} -> ok
    after ?WAIT_MS -> ct:fail("the stream never opened")
    end,
    Id = stream_id(Conn, 1),
    FeederMRef = monitor(process, gate_reached()),
    Owner ! stop,
    ok = down(FeederMRef),
    ok = wait_reset(Conn, Id),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

response_before_the_end_of_the_body_stops_the_iterator(Config) ->
    {Peer, Sup, Conn} = start(pu_early, Config, #{}),
    Via = #{via => {pool, pu_early}},
    {ok, H} = nhttpc:stream(post(<<"/manual">>, {stream, gate(self(), fin)}), Via),
    Id = stream_id(Conn, 1),
    FeederMRef = monitor(process, gate_reached()),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, [{<<":status">>, <<"200">>}], fin}),
    ?assertMatch({headers, 200, _}, event(H)),
    ?assertEqual({done, normal}, event(H)),
    ok = down(FeederMRef),
    ok = wait_reset(Conn, Id),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

connection_exit_stops_the_iterator(Config) ->
    {Peer, Sup, _Conn} = start(pu_conn_exit, Config, #{}),
    Via = #{via => {pool, pu_conn_exit}},
    {ok, H} = nhttpc:stream(post(<<"/manual">>, {stream, gate(self(), fin)}), Via),
    FeederMRef = monitor(process, gate_reached()),
    exit(nhttpc_handle:pid(H), kill),
    ?assertEqual({error, {connect_error, closed}}, event(H)),
    ok = down(FeederMRef),
    stop(Peer, Sup).

request_timeout_stops_the_iterator(Config) ->
    {Peer, Sup, Conn} = start(pu_timeout, Config, #{}),
    {ok, H} = nhttpc:stream(post(<<"/manual">>, {stream, gate(self(), fin)}), #{
        via => {pool, pu_timeout}, timeouts => #{request => ?SHORT_TIMEOUT_MS}
    }),
    Id = stream_id(Conn, 1),
    FeederMRef = monitor(process, gate_reached()),
    ?assertEqual({error, {timeout, request}}, event(H)),
    ok = down(FeederMRef),
    ok = wait_reset(Conn, Id),
    stop(Peer, Sup).

%%%-----------------------------------------------------------------------------
%%% ERRORS
%%%-----------------------------------------------------------------------------

iterator_error_resets_the_stream(Config) ->
    {Peer, Sup, Conn} = start(pu_error, Config, #{}),
    Via = #{via => {pool, pu_error}},
    Body = {stream, chunks_then([<<"part">>], fun() -> {error, boom} end)},
    ?assertEqual(
        {error, {body_error, {iterator, boom}}},
        nhttpc:request(post(<<"/manual">>, Body), Via)
    ),
    Id = stream_id(Conn, 1),
    ok = wait_reset(Conn, Id),
    ?assertEqual(cancel, maps:get(Id, maps:get(resets, peer_stats(Conn)))),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

iterator_error_ends_a_stream_with_an_error_event(Config) ->
    {Peer, Sup, Conn} = start(pu_error_event, Config, #{}),
    Via = #{via => {pool, pu_error_event}},
    Body = {stream, fun() -> {error, boom} end},
    {ok, H} = nhttpc:stream(post(<<"/manual">>, Body), Via),
    ?assertEqual({error, {body_error, {iterator, boom}}}, event(H)),
    ?assertEqual(quiet, quiet(H)),
    ok = wait_reset(Conn, stream_id(Conn, 1)),
    stop(Peer, Sup).

iterator_crash_resets_the_stream(Config) ->
    {Peer, Sup, Conn} = start(pu_crash, Config, #{}),
    Via = #{via => {pool, pu_crash}},
    Body = {stream, fun() -> exit(iterator_crashed) end},
    ?assertMatch(
        {error, {body_error, {iterator, _}}},
        nhttpc:request(post(<<"/manual">>, Body), Via)
    ),
    ok = wait_reset(Conn, stream_id(Conn, 1)),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

retry_does_not_call_the_iterator_again(Config) ->
    {Peer, Sup, Conn} = start(pu_retry, Config, #{}),
    Calls = counters:new(1, []),
    Body = {stream, counted_chunks(Calls, [<<"once">>])},
    Retry = #{max_attempts => 3, base_delay => 0, should_retry => fun(_, _, _) -> true end},
    ?assertEqual(
        {error, {stream_error, {protocol, {h2, internal_error}}}},
        nhttpc:request(post(<<"/reset/internal_error">>, Body), #{
            via => {pool, pu_retry}, retry => Retry
        })
    ),
    timer:sleep(?QUIET_MS),
    ?assertEqual(1, maps:get(requests, peer_stats(Conn))),
    ?assertEqual(2, counters:get(Calls, 1)),
    stop(Peer, Sup).

redirect_is_not_followed(Config) ->
    {Peer, Sup, Conn} = start(pu_redirect, Config, #{}),
    Via = #{via => {pool, pu_redirect}},
    Calls = counters:new(1, []),
    Body = {stream, counted_chunks(Calls, [<<"once">>])},
    {Uploader, MRef} = async(fun() -> nhttpc:request(post(<<"/manual">>, Body), Via) end),
    Id = stream_id(Conn, 1),
    ok = wait_until(fun() -> maps:get(Id, maps:get(data_frames, peer_stats(Conn)), 0) =:= 2 end),
    Redirect = [{<<":status">>, <<"307">>}, {<<"location">>, <<"/body/5">>}],
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, Redirect, fin}),
    ?assertMatch({ok, #{status := 307}}, async_result(Uploader, MRef)),
    timer:sleep(?QUIET_MS),
    ?assertEqual(1, maps:get(requests, peer_stats(Conn))),
    ?assertEqual(2, counters:get(Calls, 1)),
    stop(Peer, Sup).

%% The connection takes one stream and refuses the second request before
%% it opens a stream for it. The pool sends that request on a second
%% connection, and its iterator runs there for the first time.
refused_stream_does_not_start_the_iterator_twice(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{}),
    Sup = start_pool(pu_refused, Port, #{max_streams_per_connection => 1}),
    {ok, [First]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    [{ConnPid, _Slots}] = tuple_to_list(pool_connections(pu_refused)),
    ok = sys:suspend(ConnPid),
    Counters = [counters:new(1, []), counters:new(1, [])],
    Uploads = [
        async(fun() ->
            Body = {stream, counted_chunks(Calls, [<<"a">>, <<"b">>])},
            nhttpc:request(post(<<"/mirror">>, Body), #{via => {pool, pu_refused}})
        end)
     || Calls <- Counters
    ],
    ok = wait_until(fun() ->
        {message_queue_len, Queued} = erlang:process_info(ConnPid, message_queue_len),
        Queued >= 2
    end),
    ok = sys:resume(ConnPid),
    Results = [async_result(Pid, MRef) || {Pid, MRef} <- Uploads],
    ?assertMatch(
        [{ok, #{status := 200, body := <<"ab">>}}, {ok, #{status := 200, body := <<"ab">>}}],
        Results
    ),
    {ok, [First, Second | _Replacement]} = nhttpc_h2_script_peer:wait_connections(
        Peer, 2, ?WAIT_MS
    ),
    ?assertEqual(1, maps:get(requests, peer_stats(First))),
    ?assertEqual(1, maps:get(requests, peer_stats(Second))),
    ?assertEqual([3, 3], [counters:get(Calls, 1) || Calls <- Counters]),
    stop(Peer, Sup).

%%%-----------------------------------------------------------------------------
%%% REJECT
%%%-----------------------------------------------------------------------------

iterator_on_an_http1_pool_is_rejected(_Config) ->
    {Server, Sup} = start_http1(pu_h1_server, pu_h1_pool),
    Via = #{via => {pool, pu_h1_pool}},
    [{ConnPid, _Info}] = pool_members(pu_h1_pool),
    Body = {stream, chunks([<<"one">>])},
    ?assertEqual(
        {error, {invalid_request, stream_body_requires_http2}},
        nhttpc:request(post(<<"/echo">>, Body), Via)
    ),
    ?assertEqual(
        {error, {invalid_request, stream_body_requires_http2}},
        nhttpc:stream(post(<<"/echo">>, Body), Via)
    ),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/echo">>), Via)),
    ?assert(is_process_alive(ConnPid)),
    stop_http1(Server, Sup).

iterator_on_an_http1_connection_is_rejected(_Config) ->
    {ok, Server} = nhttp:start_link(pu_h1_conn_server, #{
        port => 0, handler => nhttp_test_handler, versions => [http1_1]
    }),
    unlink(Server),
    {ok, Port} = nhttp:get_port(Server),
    {ok, Connection} = nhttpc:open(#{host => <<"127.0.0.1">>, port => Port}, #{
        versions => [http1_1]
    }),
    Via = #{via => {conn, Connection}},
    Body = {stream, chunks([<<"one">>])},
    ?assertEqual(
        {error, {invalid_request, stream_body_requires_http2}},
        nhttpc:request(post(<<"/echo">>, Body), Via)
    ),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/echo">>), Via)),
    ok = nhttpc:close(Connection),
    ok = nhttp:stop(Server).

invalid_stream_source_is_rejected(Config) ->
    {Peer, Sup, Conn} = start(pu_bad_source, Config, #{}),
    Via = #{via => {pool, pu_bad_source}},
    lists:foreach(
        fun(Body) ->
            Req = post(<<"/body/0">>, Body),
            ?assertEqual({error, {invalid_request, invalid_stream_body}}, nhttpc:request(Req, Via)),
            ?assertEqual({error, {invalid_request, invalid_stream_body}}, nhttpc:stream(Req, Via))
        end,
        [{stream, not_a_source}, {stream, fun(_Arity) -> fin end}, {stream, {file, 42}, #{}}]
    ),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    ?assertEqual(1, maps:get(requests, peer_stats(Conn))),
    stop(Peer, Sup).

missing_file_is_a_body_error(Config) ->
    {Peer, Sup, Conn} = start(pu_no_file, Config, #{}),
    Via = #{via => {pool, pu_no_file}},
    Path = filename:join(proplists:get_value(priv_dir, Config), "no_such_file.bin"),
    ?assertMatch(
        {error, {body_error, _}},
        nhttpc:request(post(<<"/body/0">>, {stream, {file, Path}}), Via)
    ),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assertEqual(1, maps:get(requests, peer_stats(Conn))),
    stop(Peer, Sup).

invalid_body_on_a_pool_is_rejected(Config) ->
    {Peer, Sup, Conn} = start(pu_bad_body, Config, #{}),
    Via = #{via => {pool, pu_bad_body}},
    [{ConnPid, _Slots}] = tuple_to_list(pool_connections(pu_bad_body)),
    lists:foreach(
        fun(Body) ->
            ?assertEqual(
                {Body, {error, {invalid_request, invalid_body}}},
                {Body, nhttpc:request(post(<<"/body/0">>, Body), Via)}
            )
        end,
        [not_a_body, [<<"improper">> | tail], {unknown, shape}, 42]
    ),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assert(is_process_alive(ConnPid)),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

invalid_body_on_a_connection_is_rejected(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{}),
    {ok, Connection} = nhttpc:open(#{host => <<"127.0.0.1">>, port => Port}, #{
        versions => [http2], tls => #{verify => verify_none}
    }),
    Via = #{via => {conn, Connection}},
    ConnPid = nhttpc:conn_pid(Connection),
    lists:foreach(
        fun(Body) ->
            ?assertEqual(
                {Body, {error, {invalid_request, invalid_body}}},
                {Body, nhttpc:request(post(<<"/body/0">>, Body), Via)}
            ),
            ok
        end,
        [not_a_body, [<<"improper">> | tail]]
    ),
    lists:foreach(
        fun(Body) ->
            {ok, Ref} = nhttpc:send_request(ConnPid, post(<<"/body/0">>, Body)),
            ?assertEqual(
                {Body, {error, {invalid_request, invalid_body}}},
                {Body, nhttpc:await_response(ConnPid, Ref, ?WAIT_MS)}
            )
        end,
        [not_a_body, [<<"improper">> | tail], {stream, chunks([<<"direct">>])}]
    ),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assert(is_process_alive(ConnPid)),
    ok = nhttpc:close(Connection),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

invalid_body_on_an_http1_pool_is_rejected(_Config) ->
    {Server, Sup} = start_http1(pu_h1_body_server, pu_h1_body_pool),
    Via = #{via => {pool, pu_h1_body_pool}},
    [{ConnPid, _Info}] = pool_members(pu_h1_body_pool),
    lists:foreach(
        fun(Body) ->
            ?assertEqual(
                {Body, {error, {invalid_request, invalid_body}}},
                {Body, nhttpc:request(post(<<"/echo">>, Body), Via)}
            )
        end,
        [not_a_body, [<<"improper">> | tail], {unknown, shape}]
    ),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/echo">>), Via)),
    ?assert(is_process_alive(ConnPid)),
    stop_http1(Server, Sup).

%% No server listens here. The reject comes before a connection opens.
oneshot_stream_rejects_a_body_that_is_not_iodata(_Config) ->
    {ok, Req} = nhttpc:url(post, <<"http://127.0.0.1:1/upload">>),
    ?assertEqual(
        {error, {invalid_request, stream_body_requires_shared_connection}},
        nhttpc:stream(Req#{body => {stream, chunks([<<"one">>])}}, #{})
    ),
    ?assertEqual(
        {error, {invalid_request, stream_body_requires_shared_connection}},
        nhttpc:stream(Req, #{body_from => {iter, chunks([<<"one">>])}})
    ),
    ?assertEqual(
        {error, {invalid_request, invalid_body}},
        nhttpc:stream(Req#{body => not_a_body}, #{})
    ).

%%%-----------------------------------------------------------------------------
%%% ITERATORS
%%%-----------------------------------------------------------------------------

-spec chunks([iodata()]) -> nhttpc:iterator().
chunks(Chunks) ->
    chunks_then(Chunks, fun() -> fin end).

-doc "The chunks of the list, and then what `Last` gives.".
-spec chunks_then([iodata()], nhttpc:iterator()) -> nhttpc:iterator().
chunks_then([], Last) ->
    Last;
chunks_then([Chunk | Rest], Last) ->
    fun() -> {ok, Chunk, chunks_then(Rest, Last)} end.

-spec repeat(non_neg_integer(), binary()) -> nhttpc:iterator().
repeat(0, _Chunk) ->
    fun() -> fin end;
repeat(Left, Chunk) ->
    fun() -> {ok, Chunk, repeat(Left - 1, Chunk)} end.

-doc "An iterator with no end that counts its calls.".
-spec counted(counters:counters_ref(), binary()) -> nhttpc:iterator().
counted(Calls, Chunk) ->
    fun() ->
        ok = counters:add(Calls, 1, 1),
        {ok, Chunk, counted(Calls, Chunk)}
    end.

-doc "The chunks of the list, with a count of each call, the last one too.".
-spec counted_chunks(counters:counters_ref(), [binary()]) -> nhttpc:iterator().
counted_chunks(Calls, Chunks) ->
    fun() ->
        ok = counters:add(Calls, 1, 1),
        case Chunks of
            [] -> fin;
            [Chunk | Rest] -> {ok, Chunk, counted_chunks(Calls, Rest)}
        end
    end.

-doc "An iterator that tells the test which process runs it and waits for `go`.".
-spec gate(pid(), Result) -> nhttpc:iterator() when
    Result :: {ok, iodata(), nhttpc:iterator()} | fin.
gate(Test, Result) ->
    fun() ->
        Test ! {gate, self()},
        receive
            go -> Result
        end
    end.

-spec gate_reached() -> pid().
gate_reached() ->
    receive
        {gate, Feeder} -> Feeder
    after ?WAIT_MS -> ct:fail("the iterator never ran")
    end.

%%%-----------------------------------------------------------------------------
%%% PEER AND POOL
%%%-----------------------------------------------------------------------------

-spec start(atom(), [tuple()], nhttpc_h2_script_peer:opts()) -> {pid(), pid(), pid()}.
start(Pool, Config, PeerOpts) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, PeerOpts),
    Sup = start_pool(Pool, Port, #{}),
    {ok, [Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    {Peer, Sup, Conn}.

-spec start_pool(atom(), inet:port_number(), map()) -> pid().
start_pool(Pool, Port, Extra) ->
    Opts = maps:merge(
        #{
            host => <<"127.0.0.1">>,
            port => Port,
            size => 1,
            max_overflow => 0,
            tls => #{verify => verify_none},
            versions => [http2]
        },
        Extra
    ),
    {ok, Sup} = nhttpc:start_link(Pool, Opts),
    unlink(Sup),
    ok = nhttpc_pool:wait_ready(Pool, ?WAIT_MS),
    Sup.

-spec stop(pid(), pid()) -> ok.
stop(Peer, Sup) ->
    exit(Sup, shutdown),
    timer:sleep(?STOP_SETTLE_MS),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

-spec start_http1(atom(), atom()) -> {pid(), pid()}.
start_http1(ServerName, Pool) ->
    {ok, Server} = nhttp:start_link(ServerName, #{
        port => 0, handler => nhttp_test_handler, versions => [http1_1]
    }),
    unlink(Server),
    {ok, Port} = nhttp:get_port(Server),
    {ok, Sup} = nhttpc:start_link(Pool, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0,
        versions => [http1_1]
    }),
    unlink(Sup),
    ok = nhttpc_pool:wait_ready(Pool, ?WAIT_MS),
    {Server, Sup}.

-spec stop_http1(pid(), pid()) -> ok.
stop_http1(Server, Sup) ->
    exit(Sup, shutdown),
    timer:sleep(?STOP_SETTLE_MS),
    nhttp:stop(Server).

-doc "The `{ConnPid, Slots}` pairs that the lock-free checkout of the pool reads.".
-spec pool_connections(atom()) -> tuple().
pool_connections(Pool) ->
    nhttpc_registry:get_multiplexed_conns(registry(Pool)).

-doc "The connection processes under the connection supervisor of the pool.".
-spec pool_members(atom()) -> [{pid(), term()}].
pool_members(Pool) ->
    ConnSup = nhttpc_pool:lookup_conn_sup(Pool),
    [{Pid, Id} || {Id, Pid, _Type, _Modules} <- supervisor:which_children(ConnSup), is_pid(Pid)].

-spec registry(atom()) -> ets:tid().
registry(Pool) ->
    Sup = nhttpc_pool:lookup_sup(Pool),
    [Tab] = [
        T
     || T <- ets:all(), ets:info(T, owner) =:= Sup, ets:info(T, name) =:= nhttpc_registry
    ],
    Tab.

-spec peer_stats(pid()) -> nhttpc_h2_script_peer:stats().
peer_stats(Conn) ->
    nhttpc_h2_script_peer:stats(Conn).

-doc "The request octets that the peer got on each stream.".
-spec sent_by_client(pid()) -> #{nhttp_lib:stream_id() => non_neg_integer()}.
sent_by_client(Conn) ->
    maps:get(request_octets_streams, peer_stats(Conn)).

-doc "The identifier of the `Nth` stream the client opened on this connection.".
-spec stream_id(pid(), pos_integer()) -> nhttp_lib:stream_id().
stream_id(Conn, Nth) ->
    ok = wait_until(fun() -> maps:get(requests, peer_stats(Conn)) >= Nth end),
    lists:nth(Nth, lists:reverse(maps:get(streams, peer_stats(Conn)))).

-spec wait_reset(pid(), nhttp_lib:stream_id()) -> ok.
wait_reset(Conn, Id) ->
    wait_until(fun() -> maps:is_key(Id, maps:get(resets, peer_stats(Conn))) end).

-spec wait_until(fun(() -> boolean())) -> ok.
wait_until(Check) ->
    wait_until(Check, ?WAIT_MS).

-spec wait_until(fun(() -> boolean()), integer()) -> ok.
wait_until(Check, Remaining) ->
    case Check() of
        true ->
            ok;
        false when Remaining =< 0 ->
            ct:fail("the condition never held");
        false ->
            timer:sleep(?POLL_MS),
            wait_until(Check, Remaining - ?POLL_MS)
    end.

%%%-----------------------------------------------------------------------------
%%% REQUESTS AND EVENTS
%%%-----------------------------------------------------------------------------

-spec get_request(binary()) -> nhttp_lib:request().
get_request(Path) ->
    #{method => get, path => Path, headers => []}.

-spec post(binary(), term()) -> map().
post(Path, Body) ->
    #{method => post, path => Path, headers => [], body => Body}.

-spec event(nhttpc:handle()) -> nhttpc:event().
event(H) ->
    receive
        {nhttpc, H, Event} -> Event
    after ?WAIT_MS -> ct:fail("no event")
    end.

-doc "`quiet` when the mailbox holds no event of the handle after a settle time.".
-spec quiet(nhttpc:handle()) -> quiet | nhttpc:event().
quiet(H) ->
    receive
        {nhttpc, H, Event} -> Event
    after ?QUIET_MS -> quiet
    end.

-doc "Run `Fun` in a process of its own. `async_result/2` gives what it returned.".
-spec async(fun(() -> term())) -> {pid(), reference()}.
async(Fun) ->
    spawn_monitor(fun() -> exit({async_result, Fun()}) end).

-spec async_result(pid(), reference()) -> term().
async_result(Pid, MRef) ->
    receive
        {'DOWN', MRef, process, Pid, {async_result, Result}} -> Result;
        {'DOWN', MRef, process, Pid, Reason} -> ct:fail({async_exit, Reason})
    after ?LONG_TIMEOUT_MS -> ct:fail("the call never returned")
    end.

-spec down(reference()) -> ok.
down(MRef) ->
    receive
        {'DOWN', MRef, process, _Pid, _Reason} -> ok
    after ?WAIT_MS -> ct:fail("the process never exited")
    end.

%%%-----------------------------------------------------------------------------
%%% HEAP SAMPLER
%%%-----------------------------------------------------------------------------

-spec start_heap_sampler(pid()) -> pid().
start_heap_sampler(ConnPid) ->
    Owner = self(),
    spawn_link(fun() -> heap_sampler(ConnPid, Owner, 0) end).

-spec stop_heap_sampler(pid()) -> non_neg_integer().
stop_heap_sampler(Sampler) ->
    Sampler ! {stop, self()},
    receive
        {heap, Sampler, Max} -> Max
    after ?WAIT_MS -> ct:fail("the heap sampler never answered")
    end.

-spec heap_sampler(pid(), pid(), non_neg_integer()) -> ok.
heap_sampler(ConnPid, Owner, Max) ->
    receive
        {stop, Owner} ->
            Owner ! {heap, self(), Max},
            ok
    after ?HEAP_SAMPLE_MS ->
        case erlang:process_info(ConnPid, total_heap_size) of
            {total_heap_size, Words} -> heap_sampler(ConnPid, Owner, max(Max, Words));
            undefined -> heap_sampler(ConnPid, Owner, Max)
        end
    end.
