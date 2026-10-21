%%%-----------------------------------------------------------------------------
%%% @doc Common Test suite covering streaming backpressure
%%% (`consume_initial`, `consume_low_water`, `nhttpc:consume/2`,
%%% `{pause, _}` / `{resume, _}` events).
%%%
%%% Coverage:
%%%   - consume_credit_pauses_and_resumes (H1)
%%%   - high_water_pause_event_arrives_for_slow_consumer (H1)
%%%   - resume_event_arrives_after_consume_drops_buffer (H1)
%%%   - fast_h1_producer_does_not_oom_slow_consumer (H1)
%%%   - fast_h2_producer_does_not_oom_slow_consumer (H2)
%%%   - fast_h3_producer_does_not_oom_slow_consumer (H3)
%%%   - consume_infinity_means_no_backpressure (H1)
%%%   - consume_is_noop_for_non_stream_handles
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_stream_backpressure_SUITE).

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
    consume_credit_pauses_and_resumes/1,
    high_water_pause_event_arrives_for_slow_consumer/1,
    resume_event_arrives_after_consume_drops_buffer/1,
    fast_h1_producer_does_not_oom_slow_consumer/1,
    fast_h2_producer_does_not_oom_slow_consumer/1,
    fast_h3_producer_does_not_oom_slow_consumer/1,
    consume_infinity_means_no_backpressure/1,
    consume_is_noop_for_non_stream_handles/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, http1},
        {group, http2},
        {group, http3},
        {group, misc}
    ].

groups() ->
    [
        {http1, [sequence], [
            consume_credit_pauses_and_resumes,
            high_water_pause_event_arrives_for_slow_consumer,
            resume_event_arrives_after_consume_drops_buffer,
            fast_h1_producer_does_not_oom_slow_consumer,
            consume_infinity_means_no_backpressure
        ]},
        {http2, [sequence], [
            fast_h2_producer_does_not_oom_slow_consumer
        ]},
        {http3, [sequence], [
            fast_h3_producer_does_not_oom_slow_consumer
        ]},
        {misc, [parallel], [
            consume_is_noop_for_non_stream_handles
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

init_per_group(http1, Config) ->
    case start_h1_server() of
        {ok, ServerPid, Port} ->
            [{server_pid, ServerPid}, {server_port, Port} | Config];
        {error, Reason} ->
            ct:fail("Failed to start H1 server: ~p", [Reason])
    end;
init_per_group(http2, Config) ->
    case has_tls(Config) of
        true ->
            CertFile = ?config(certfile, Config),
            KeyFile = ?config(keyfile, Config),
            case start_h2_server(CertFile, KeyFile) of
                {ok, ServerPid, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to start H2 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(http3, Config) ->
    case has_tls(Config) of
        true ->
            CertFile = ?config(certfile, Config),
            KeyFile = ?config(keyfile, Config),
            case start_h3_server(CertFile, KeyFile) of
                {ok, ServerPid, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    {skip, {h3_server_failed, Reason}}
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(misc, Config) ->
    Config.

end_per_group(misc, _Config) ->
    ok;
end_per_group(_Group, Config) ->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES — HTTP/1.1
%%%-----------------------------------------------------------------------------

%% A 10KB response with consume_initial = 4KB and low_water = 1KB:
%% the worker should pause after the first chunk that crosses the
%% credit boundary, and resume once the test calls consume/2.
consume_credit_pauses_and_resumes(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/progress">>)),
    {ok, Handle} = nhttpc:stream(Req, #{
        consume_initial => 4096,
        consume_low_water => 1024
    }),
    {Status, _Hs, Body, Trailers, Terminal, Events} = collect_with_consume(Handle, 10000, 8192),
    ?assertEqual(200, Status),
    ?assertEqual(byte_size(binary:copy(<<"progress">>, 1250)), byte_size(Body)),
    ?assertEqual(undefined, Trailers),
    ?assertEqual({done, normal}, Terminal),
    %% We expect at least one pause and a matching resume.
    PauseCount = lists:foldl(
        fun
            ({pause, _}, Acc) -> Acc + 1;
            (_, Acc) -> Acc
        end,
        0,
        Events
    ),
    ResumeCount = lists:foldl(
        fun
            ({resume, _}, Acc) -> Acc + 1;
            (_, Acc) -> Acc
        end,
        0,
        Events
    ),
    ?assert(PauseCount >= 1),
    ?assert(ResumeCount >= 1),
    ?assertEqual(PauseCount, ResumeCount),
    ok = nhttpc:flush(Handle),
    ok.

%% No consume call after the initial credit is depleted: the worker
%% must emit pause, and the data event for the buffered chunk must
%% not arrive until we explicitly consume.
high_water_pause_event_arrives_for_slow_consumer(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/large">>)),
    {ok, Handle} = nhttpc:stream(Req, #{
        consume_initial => 1024,
        consume_low_water => 256
    }),
    receive
        {nhttpc, Handle, {headers, _, _}} -> ok
    after 5000 ->
        ct:fail("no headers")
    end,
    {PauseBytes, BodyBefore} = wait_for_pause(Handle, 5000, <<>>),
    ?assert(PauseBytes > 0),
    NoData = no_data_within(Handle, 200),
    ?assertEqual([], NoData),
    ok = nhttpc:consume(Handle, 10 * 1024 * 1024),
    {_, _, _, BodyAll, Terminal, _Evts} = drain_to_done(Handle, 10000, BodyBefore, undefined),
    Expected = binary:copy(<<"0123456789ABCDEF">>, 6400),
    ?assertEqual(Expected, BodyAll),
    ?assertEqual({done, normal}, Terminal),
    ok = nhttpc:flush(Handle),
    ok.

%% Resume must fire after consume drops the buffer below low_water.
resume_event_arrives_after_consume_drops_buffer(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/large">>)),
    {ok, Handle} = nhttpc:stream(Req, #{
        consume_initial => 4096,
        consume_low_water => 8
    }),
    {_, _, _, _Body, Terminal, Events} = collect_with_consume(Handle, 10000, 65536),
    ?assertEqual({done, normal}, Terminal),
    Pauses = [P || {pause, _} = P <- Events],
    Resumes = [R || {resume, _} = R <- Events],
    ?assert(length(Pauses) >= 1),
    ?assertEqual(length(Pauses), length(Resumes)),
    ok = nhttpc:flush(Handle),
    ok.

%% Slow consumer should not OOM. The owner consumes one chunk per
%% 50ms. The worker's mailbox should stay bounded by consume_initial.
fast_h1_producer_does_not_oom_slow_consumer(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/large">>)),
    {ok, Handle} = nhttpc:stream(Req, #{
        consume_initial => 2048,
        consume_low_water => 512
    }),
    ok = slow_consumer_drain(Handle, 2048, 20, 30000),
    ok = nhttpc:flush(Handle),
    ok.

%% consume_initial => infinity disables all backpressure. No pause /
%% resume events should ever fire.
consume_infinity_means_no_backpressure(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/large">>)),
    {ok, Handle} = nhttpc:stream(Req, #{consume_initial => infinity}),
    {_, _, Body, _, Terminal, Events} = collect_with_consume(Handle, 10000, 0),
    ?assertEqual({done, normal}, Terminal),
    Expected = binary:copy(<<"0123456789ABCDEF">>, 6400),
    ?assertEqual(Expected, Body),
    ?assertEqual([], [E || {pause, _} = E <- Events]),
    ?assertEqual([], [E || {resume, _} = E <- Events]),
    ok = nhttpc:flush(Handle),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES — HTTP/2
%%%-----------------------------------------------------------------------------

fast_h2_producer_does_not_oom_slow_consumer(Config) ->
    Port = ?config(server_port, Config),
    Url = h2_url(Port, <<"/stream/large">>),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:stream(Req, #{
        tls => #{verify => verify_none},
        versions => [http2],
        consume_initial => 2048,
        consume_low_water => 512
    }),
    %% Sanity-check that the stream starts, then continue the drain.
    receive
        {nhttpc, Handle, {headers, _, _}} -> ok;
        {nhttpc, Handle, {error, R}} -> ct:fail("stream error: ~p", [R])
    after 10000 ->
        ct:fail("no headers from H2 server within 10s")
    end,
    ok = slow_consumer_drain(Handle, 16384, 20, 15000),
    ok = nhttpc:flush(Handle),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES — HTTP/3
%%%-----------------------------------------------------------------------------

%% H3 currently buffers the full response inside nhttpc_request:do_request/3,
%% so wire-level backpressure is not in scope (see plan 07c notes /
%% follow-up). We still verify the API surface: consume_initial caps
%% delivery and consume() unblocks the buffered chunk.
fast_h3_producer_does_not_oom_slow_consumer(Config) ->
    Port = ?config(server_port, Config),
    Url = h3_url(Port, <<"/hello">>),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:stream(Req, #{
        tls => #{verify => verify_none},
        versions => [http3],
        consume_initial => 2,
        consume_low_water => 1
    }),
    ok = slow_consumer_drain(Handle, 2, 50, 30000),
    ok = nhttpc:flush(Handle),
    ok.

%%%-----------------------------------------------------------------------------
%%% TEST CASES — MISC
%%%-----------------------------------------------------------------------------

%% consume/2 against a non-stream handle (or a finished handle) is a
%% silent no-op. Spec says "no-op for non-stream handles. Document,
%% don't error."
consume_is_noop_for_non_stream_handles(_Config) ->
    %% Synthesise a non-stream handle via the public type — anything
    %% that satisfies `is_handle/1` but is `request` kind.
    FakeHandle = nhttpc_handle:new(self(), make_ref(), request),
    ?assertEqual(ok, nhttpc:consume(FakeHandle, 1024)),
    %% Also accept that consume/2 is harmless when the handle's pid is dead.
    DeadPid = spawn(fun() -> ok end),
    timer:sleep(50),
    DeadHandle = nhttpc_handle:new(DeadPid, make_ref(), stream),
    ?assertEqual(ok, nhttpc:consume(DeadHandle, 1024)),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS — EVENT COLLECTION
%%%-----------------------------------------------------------------------------

%% Collect events while granting credit eagerly (ChunkSize at a time).
%% Returns {Status, Headers, Body, Trailers, Terminal, AllEvents}.
collect_with_consume(Handle, Timeout, ChunkSize) ->
    collect_with_consume(Handle, Timeout, ChunkSize, undefined, undefined, [], undefined, []).

collect_with_consume(Handle, Timeout, ChunkSize, Status, Headers, BodyAcc, Trailers, Events) ->
    receive
        {nhttpc, Handle, {headers, S, Hs}} ->
            collect_with_consume(Handle, Timeout, ChunkSize, S, Hs, BodyAcc, Trailers, [
                {headers, S} | Events
            ]);
        {nhttpc, Handle, {data, Chunk, _Fin}} ->
            maybe_consume(Handle, ChunkSize, byte_size(Chunk)),
            collect_with_consume(
                Handle,
                Timeout,
                ChunkSize,
                Status,
                Headers,
                [Chunk | BodyAcc],
                Trailers,
                [data | Events]
            );
        {nhttpc, Handle, {trailers, Ts}} ->
            collect_with_consume(Handle, Timeout, ChunkSize, Status, Headers, BodyAcc, Ts, [
                trailers | Events
            ]);
        {nhttpc, Handle, {pause, B} = P} ->
            %% Pause arrived because the next chunk did not fit credit.
            %% Grant enough credit to unblock and keep flow going.
            maybe_consume(Handle, ChunkSize, B),
            collect_with_consume(Handle, Timeout, ChunkSize, Status, Headers, BodyAcc, Trailers, [
                P | Events
            ]);
        {nhttpc, Handle, {resume, _} = R} ->
            collect_with_consume(Handle, Timeout, ChunkSize, Status, Headers, BodyAcc, Trailers, [
                R | Events
            ]);
        {nhttpc, Handle, {done, _} = Done} ->
            {Status, Headers, iolist_to_binary(lists:reverse(BodyAcc)), Trailers, Done,
                lists:reverse(Events)};
        {nhttpc, Handle, {error, _} = Err} ->
            {Status, Headers, iolist_to_binary(lists:reverse(BodyAcc)), Trailers, Err,
                lists:reverse(Events)}
    after Timeout ->
        ct:fail(
            "timeout collecting stream events; got status=~p body_bytes=~p events=~p",
            [Status, iolist_size(BodyAcc), lists:reverse(Events)]
        )
    end.

maybe_consume(_Handle, 0, _N) ->
    ok;
maybe_consume(Handle, ChunkSize, N) when N > 0 ->
    Grant = max(N, ChunkSize),
    nhttpc:consume(Handle, Grant),
    ok;
maybe_consume(Handle, ChunkSize, _N) ->
    nhttpc:consume(Handle, ChunkSize),
    ok.

%% Drain events that came in within initial credit, then expect a pause.
wait_for_pause(Handle, Timeout, Acc) ->
    receive
        {nhttpc, Handle, {pause, B}} ->
            {B, Acc};
        {nhttpc, Handle, {data, Chunk, _Fin}} ->
            wait_for_pause(Handle, Timeout, <<Acc/binary, Chunk/binary>>);
        {nhttpc, Handle, {done, _}} ->
            ct:fail("done arrived before pause");
        {nhttpc, Handle, {error, R}} ->
            ct:fail("error before pause: ~p", [R])
    after Timeout ->
        ct:fail("no pause event within timeout")
    end.

%% Confirm no {data, _, _} events arrive within Timeout ms.
no_data_within(Handle, Timeout) ->
    receive
        {nhttpc, Handle, {data, _, _} = E} -> [E];
        {nhttpc, Handle, {done, _}} -> [];
        {nhttpc, Handle, {error, R}} -> ct:fail("error: ~p", [R])
    after Timeout ->
        []
    end.

%% Drain remaining events until {done, _} arrives, granting unbounded
%% credit so further chunks flow.
drain_to_done(Handle, Timeout, BodyBytes, Trailers) ->
    drain_to_done(Handle, Timeout, BodyBytes, Trailers, []).

drain_to_done(Handle, Timeout, BodyBytes, Trailers, Events) ->
    receive
        {nhttpc, Handle, {data, Chunk, _Fin}} ->
            drain_to_done(Handle, Timeout, <<BodyBytes/binary, Chunk/binary>>, Trailers, [
                data | Events
            ]);
        {nhttpc, Handle, {trailers, Ts}} ->
            drain_to_done(Handle, Timeout, BodyBytes, Ts, [trailers | Events]);
        {nhttpc, Handle, {pause, _} = P} ->
            drain_to_done(Handle, Timeout, BodyBytes, Trailers, [P | Events]);
        {nhttpc, Handle, {resume, _} = R} ->
            drain_to_done(Handle, Timeout, BodyBytes, Trailers, [R | Events]);
        {nhttpc, Handle, {done, _} = Done} ->
            {ok, undefined, [], BodyBytes, Done, lists:reverse(Events)};
        {nhttpc, Handle, {error, _} = Err} ->
            {ok, undefined, [], BodyBytes, Err, lists:reverse(Events)}
    after Timeout ->
        ct:fail("timeout draining to done; body bytes=~p", [byte_size(BodyBytes)])
    end.

%% Slow-consumer loop: throttle consumer credit, while still ensuring
%% every paused chunk is eventually unblocked. Once per IntervalMs the
%% consumer either grants enough credit to release a buffered chunk
%% (when a pause event is in flight) or grants ChunkBytes of credit and
%% reads what arrived. The test fails if the worker mailbox grows
%% beyond a small multiple of consume_initial.
slow_consumer_drain(Handle, ChunkBytes, IntervalMs, OverallTimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + OverallTimeoutMs,
    WorkerPid = nhttpc_handle:pid(Handle),
    %% Bound: we expect at most "headers/trailers/done + one pending
    %% data event + a handful of consume cycles" outstanding in the
    %% worker mailbox.
    Bound = 16,
    slow_consumer_loop(Handle, WorkerPid, ChunkBytes, IntervalMs, Deadline, Bound).

slow_consumer_loop(Handle, WorkerPid, ChunkBytes, IntervalMs, Deadline, Bound) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            DbgQLen = process_info(WorkerPid, message_queue_len),
            ct:fail("slow consumer hit overall deadline; worker_qlen=~p", [DbgQLen]);
        false ->
            ok = nhttpc:consume(Handle, ChunkBytes),
            timer:sleep(IntervalMs),
            assert_mailbox_bounded(WorkerPid, Bound),
            case drain_pending_events(Handle, ChunkBytes) of
                done ->
                    ok;
                {error, Reason} ->
                    ct:fail("stream error during slow drain: ~p", [Reason]);
                continue ->
                    slow_consumer_loop(Handle, WorkerPid, ChunkBytes, IntervalMs, Deadline, Bound)
            end
    end.

%% Drain anything currently waiting in our mailbox. If a pause arrives
%% with a buffered chunk too large for ChunkBytes, grant exactly that
%% amount so progress continues without violating the slow-consumer
%% intent (one chunk per consume cycle).
drain_pending_events(Handle, ChunkBytes) ->
    receive
        {nhttpc, Handle, {pause, B}} when B > ChunkBytes ->
            nhttpc:consume(Handle, B - ChunkBytes),
            drain_pending_events(Handle, ChunkBytes);
        {nhttpc, Handle, {headers, _, _}} ->
            drain_pending_events(Handle, ChunkBytes);
        {nhttpc, Handle, {data, _Chunk, _Fin}} ->
            drain_pending_events(Handle, ChunkBytes);
        {nhttpc, Handle, {trailers, _}} ->
            drain_pending_events(Handle, ChunkBytes);
        {nhttpc, Handle, {pause, _}} ->
            drain_pending_events(Handle, ChunkBytes);
        {nhttpc, Handle, {resume, _}} ->
            drain_pending_events(Handle, ChunkBytes);
        {nhttpc, Handle, {done, _}} ->
            done;
        {nhttpc, Handle, {error, R}} ->
            {error, R}
    after 0 ->
        continue
    end.

assert_mailbox_bounded(Pid, Limit) ->
    case process_info(Pid, message_queue_len) of
        undefined ->
            ok;
        {message_queue_len, Len} ->
            case Len =< Limit of
                true ->
                    ok;
                false ->
                    ct:fail("worker mailbox exceeded bound: ~p > ~p", [Len, Limit])
            end
    end.

%%%-----------------------------------------------------------------------------
%%% HELPERS — SERVERS / URLS
%%%-----------------------------------------------------------------------------

start_h1_server() ->
    case
        nhttp:start_link(stream_bp_h1, #{
            port => 0,
            handler => nhttp_test_handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, _} = Error ->
            Error
    end.

start_h2_server(CertFile, KeyFile) ->
    case
        nhttp:start_link(stream_bp_h2, #{
            port => 0,
            handler => nhttp_test_handler,
            versions => [http2, http1_1],
            tls => #{certfile => CertFile, keyfile => KeyFile}
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, _} = Error ->
            Error
    end.

start_h3_server(CertFile, KeyFile) ->
    case
        nhttp:start_link(stream_bp_h3, #{
            port => 0,
            handler => nhttpc_h3_test_handler,
            versions => [http3],
            tls => #{certfile => CertFile, keyfile => KeyFile}
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, _} = Error ->
            Error
    end.

url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"http://127.0.0.1:", PortBin/binary, Path/binary>>.

h2_url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"https://127.0.0.1:", PortBin/binary, Path/binary>>.

h3_url(Port, Path) ->
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
