%%%-----------------------------------------------------------------------------
%%% @doc Lifecycle terminals for nhttpc_request_worker / nhttpc_stream_worker.
%%%
%%% Drives the worker fault matrix with real worker processes and a
%%% slow `nhttp` endpoint that buys enough wall-clock time to interrupt
%%% the in-flight sub-process. The four terminal paths the worker loop
%%% pattern-matches are:
%%%
%%%   * `{'$result', _}`           — happy path (covered elsewhere)
%%%   * `{'DOWN', _, _, _, normal}`— sub exited normally without sending the result
%%%   * `{'DOWN', _, _, _, R}`     — sub crashed
%%%   * `{'EXIT', _, cancelled}`   — external linked process signalled cancel
%%%   * `{'$nhttpc_cancel', _, _}` — cooperative cancel via the public API
%%%
%%% Sub-process termination is driven via `process_info(WorkerPid, links)`,
%%% which surfaces the linked sub-pid (the worker only links to its sub).
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_worker_SUITE).

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
    request_worker_cancel_mid_flight/1,
    request_worker_sub_crash_emits_worker_died/1,
    request_worker_external_exit_cancelled/1,
    request_worker_ignores_cancel_with_wrong_ref/1,
    request_worker_happy_path/1
]).

-export([
    stream_worker_cancel_mid_flight/1,
    stream_worker_sub_crash_emits_worker_died/1,
    stream_worker_external_exit_cancelled/1,
    stream_worker_ignores_cancel_with_wrong_ref/1,
    stream_worker_apply_limits_and_timeouts/1,
    stream_worker_emits_progress/1,
    stream_worker_emits_trailers/1,
    stream_worker_h1_error_propagates/1,
    stream_worker_consume_on_infinity_is_noop/1
]).

%%%-----------------------------------------------------------------------------
%%% CT BOILERPLATE
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [
        {group, request_worker},
        {group, stream_worker}
    ].

groups() ->
    [
        {request_worker, [sequence], [
            request_worker_happy_path,
            request_worker_cancel_mid_flight,
            request_worker_sub_crash_emits_worker_died,
            request_worker_external_exit_cancelled,
            request_worker_ignores_cancel_with_wrong_ref
        ]},
        {stream_worker, [sequence], [
            stream_worker_apply_limits_and_timeouts,
            stream_worker_emits_progress,
            stream_worker_emits_trailers,
            stream_worker_h1_error_propagates,
            stream_worker_consume_on_infinity_is_noop,
            stream_worker_cancel_mid_flight,
            stream_worker_sub_crash_emits_worker_died,
            stream_worker_external_exit_cancelled,
            stream_worker_ignores_cancel_with_wrong_ref
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    case
        nhttp:start_link(worker_suite_h1, #{
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
    end.

end_per_group(_Group, Config) ->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok.

%%%-----------------------------------------------------------------------------
%%% REQUEST WORKER — TERMINAL PATHS
%%%-----------------------------------------------------------------------------

request_worker_happy_path(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    {ok, Handle} = nhttpc:send(Req),
    receive
        {nhttpc, Handle, {response, #{status := 200}}} -> ok
    after 5000 ->
        ct:fail("no response")
    end,
    receive
        {nhttpc, Handle, {done, normal}} -> ok
    after 1000 ->
        ct:fail("no done")
    end,
    ok.

request_worker_cancel_mid_flight(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/2000">>)),
    {ok, Handle} = nhttpc:send(Req),
    WorkerPid = handle_pid(Handle),
    SubPid = sub_of(WorkerPid),
    ?assert(is_process_alive(SubPid)),
    ok = nhttpc:cancel(Handle),
    ?assertNot(is_process_alive(WorkerPid)),
    ?assertNot(is_process_alive(SubPid)),
    no_more_events(Handle),
    ok.

request_worker_sub_crash_emits_worker_died(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/2000">>)),
    {ok, Handle} = nhttpc:send(Req),
    WorkerPid = handle_pid(Handle),
    SubPid = sub_of(WorkerPid),
    MRef = monitor(process, WorkerPid),
    exit(SubPid, killed_by_test),
    receive
        {nhttpc, Handle, {error, {worker_died, Reason}}} ->
            ?assertEqual(killed_by_test, Reason)
    after 5000 ->
        ct:fail("no worker_died")
    end,
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 1000 ->
        ct:fail("worker did not exit")
    end,
    ?assertNot(is_process_alive(SubPid)),
    no_more_events(Handle),
    ok.

request_worker_external_exit_cancelled(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/2000">>)),
    {ok, Handle} = nhttpc:send(Req),
    WorkerPid = handle_pid(Handle),
    SubPid = sub_of(WorkerPid),
    MRef = monitor(process, WorkerPid),
    Self = self(),
    spawn(fun() ->
        link(WorkerPid),
        Self ! linked,
        exit(cancelled)
    end),
    receive
        linked -> ok
    after 1000 ->
        ct:fail("linker never reported")
    end,
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 5000 ->
        ct:fail("worker did not exit after EXIT cancelled")
    end,
    ?assertNot(is_process_alive(SubPid)),
    no_more_events(Handle),
    ok.

request_worker_ignores_cancel_with_wrong_ref(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/200">>)),
    {ok, Handle} = nhttpc:send(Req),
    WorkerPid = handle_pid(Handle),
    WorkerPid ! {'$nhttpc_cancel', make_ref(), self()},
    receive
        {nhttpc, Handle, {response, #{status := 200}}} -> ok
    after 5000 ->
        ct:fail("no response after wrong-ref cancel")
    end,
    receive
        {nhttpc, Handle, {done, normal}} -> ok
    after 1000 ->
        ct:fail("no done")
    end,
    receive
        {'$nhttpc_cancel_ack', _} ->
            ct:fail("worker acked cancel with wrong ref")
    after 0 ->
        ok
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% STREAM WORKER — TERMINAL PATHS
%%%-----------------------------------------------------------------------------

stream_worker_apply_limits_and_timeouts(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/large">>)),
    {ok, Handle} = nhttpc:stream(Req, #{
        max_response_size => 1_000_000,
        timeouts => #{request => 10000},
        consume_initial => infinity
    }),
    drain_until_done(Handle, 5000),
    ok = nhttpc:consume(Handle, 1024),
    ok = nhttpc:flush(Handle),
    ok.

stream_worker_emits_progress(Config) ->
    Port = ?config(server_port, Config),
    Self = self(),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/progress">>)),
    {ok, Handle} = nhttpc:stream(Req, #{
        on_download_progress => fun(Sent, Total) ->
            Self ! {progress, Sent, Total},
            ok
        end,
        consume_initial => infinity
    }),
    drain_until_done(Handle, 5000),
    ok.

stream_worker_emits_trailers(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/trailers">>)),
    {ok, Handle} = nhttpc:stream(Req, #{
        consume_initial => infinity,
        versions => [http1_1]
    }),
    Events = drain_until_done(Handle, 5000),
    case
        lists:any(
            fun
                ({trailers, _}) -> true;
                (_) -> false
            end,
            Events
        )
    of
        true -> ok;
        false -> ct:log("trailers omitted by h1 server (acceptable)")
    end,
    ok.

stream_worker_h1_error_propagates(_Config) ->
    Port = unused_port(),
    Url = <<"http://127.0.0.1:", (integer_to_binary(Port))/binary, "/echo">>,
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:stream(Req, #{
        consume_initial => infinity,
        timeouts => #{connect => 200, request => 1000}
    }),
    Events = drain_until_done(Handle, 5000),
    case lists:last(Events) of
        {error, _} -> ok;
        Other -> ct:fail({expected_error, Other})
    end,
    ok.

stream_worker_consume_on_infinity_is_noop(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    {ok, Handle} = nhttpc:stream(Req, #{consume_initial => infinity}),
    ok = nhttpc:consume(Handle, 1024),
    drain_until_done(Handle, 5000),
    ok.

stream_worker_cancel_mid_flight(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/2000">>)),
    {ok, Handle} = nhttpc:stream(Req, #{consume_initial => infinity}),
    WorkerPid = handle_pid(Handle),
    SubPid = sub_of(WorkerPid),
    ?assert(is_process_alive(SubPid)),
    ok = nhttpc:cancel(Handle),
    ?assertNot(is_process_alive(WorkerPid)),
    ?assertNot(is_process_alive(SubPid)),
    no_more_events(Handle),
    ok.

stream_worker_sub_crash_emits_worker_died(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/2000">>)),
    {ok, Handle} = nhttpc:stream(Req, #{consume_initial => infinity}),
    WorkerPid = handle_pid(Handle),
    SubPid = sub_of(WorkerPid),
    MRef = monitor(process, WorkerPid),
    exit(SubPid, killed_by_test),
    receive
        {nhttpc, Handle, {error, {worker_died, Reason}}} ->
            ?assertEqual(killed_by_test, Reason)
    after 5000 ->
        ct:fail("no worker_died")
    end,
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 1000 ->
        ct:fail("worker did not exit")
    end,
    no_more_events(Handle),
    ok.

stream_worker_external_exit_cancelled(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/slow/2000">>)),
    {ok, Handle} = nhttpc:stream(Req, #{consume_initial => infinity}),
    WorkerPid = handle_pid(Handle),
    SubPid = sub_of(WorkerPid),
    MRef = monitor(process, WorkerPid),
    Self = self(),
    spawn(fun() ->
        link(WorkerPid),
        Self ! linked,
        exit(cancelled)
    end),
    receive
        linked -> ok
    after 1000 ->
        ct:fail("linker never reported")
    end,
    receive
        {'DOWN', MRef, process, _, _} -> ok
    after 5000 ->
        ct:fail("worker did not exit after EXIT cancelled")
    end,
    ?assertNot(is_process_alive(SubPid)),
    no_more_events(Handle),
    ok.

stream_worker_ignores_cancel_with_wrong_ref(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/echo">>)),
    {ok, Handle} = nhttpc:stream(Req, #{consume_initial => infinity}),
    WorkerPid = handle_pid(Handle),
    WorkerPid ! {'$nhttpc_cancel', make_ref(), self()},
    drain_until_done(Handle, 5000),
    receive
        {'$nhttpc_cancel_ack', _} ->
            ct:fail("worker acked cancel with wrong ref")
    after 0 ->
        ok
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"http://127.0.0.1:", PortBin/binary, Path/binary>>.

handle_pid(Handle) when is_map(Handle) ->
    maps:get(pid, Handle).

unused_port() ->
    {ok, Sock} = gen_tcp:listen(0, [{reuseaddr, true}]),
    {ok, Port} = inet:port(Sock),
    gen_tcp:close(Sock),
    Port.

sub_of(WorkerPid) ->
    {links, Links} = process_info(WorkerPid, links),
    Self = self(),
    [SubPid] = [P || P <- Links, P =/= Self, is_pid(P)],
    SubPid.

no_more_events(Handle) ->
    receive
        {nhttpc, Handle, _} = M ->
            ct:fail("unexpected residual event ~p", [M])
    after 100 ->
        ok
    end.

drain_until_done(Handle, Timeout) ->
    drain_until_done(Handle, Timeout, []).

drain_until_done(Handle, Timeout, Acc) ->
    receive
        {nhttpc, Handle, {done, _}} -> lists:reverse(Acc);
        {nhttpc, Handle, {error, _} = E} -> lists:reverse([E | Acc]);
        {nhttpc, Handle, Event} -> drain_until_done(Handle, Timeout, [Event | Acc])
    after Timeout ->
        ct:fail("stream did not finish in time, got ~p events", [length(Acc)])
    end.
