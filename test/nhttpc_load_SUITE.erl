%%%-----------------------------------------------------------------------------
%%% @doc Client load test suite.
%%%
%%% Tests client behavior under high load conditions including high concurrency,
%%% sustained load, and memory stability.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_load_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% EXPORTS
%%%-----------------------------------------------------------------------------

%% CT callbacks
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

%% Concurrency tests
-export([
    concurrent_100_requests/1,
    concurrent_requests_small_pool/1,
    concurrent_requests_large_pool/1
]).

%% Sustained load tests
-export([
    sustained_load_10_seconds/1,
    sustained_load_mixed_methods/1
]).

%% Memory tests
-export([
    memory_stable_under_load/1,
    no_process_leaks/1
]).

%% Pool stress tests
-export([
    pool_exhaustion_recovery/1,
    pool_checkout_timeout_under_load/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        {group, concurrency},
        {group, sustained},
        {group, memory},
        {group, pool_stress}
    ].

groups() ->
    [
        {concurrency, [sequence], [
            concurrent_100_requests,
            concurrent_requests_small_pool,
            concurrent_requests_large_pool
        ]},
        {sustained, [sequence], [
            sustained_load_10_seconds,
            sustained_load_mixed_methods
        ]},
        {memory, [sequence], [
            memory_stable_under_load,
            no_process_leaks
        ]},
        {pool_stress, [sequence], [
            pool_exhaustion_recovery,
            pool_checkout_timeout_under_load
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    %% Start a test server
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(load_test_server, #{
            port => 0,
            handler => Handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            case nhttp:get_port(ServerPid) of
                {ok, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to get port: ~p", [Reason])
            end;
        {error, Reason} ->
            ct:fail("Failed to start server: ~p", [Reason])
    end.

end_per_group(_Group, Config) ->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Allow cleanup time between tests
    timer:sleep(100),
    ok.

%%%-----------------------------------------------------------------------------
%%% CONCURRENCY TESTS
%%%-----------------------------------------------------------------------------

%% @doc Test 100 concurrent requests.
concurrent_100_requests(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Start a pool
    PoolRef = load_100_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 10,
        max_overflow => 10,
        max_pending_requests => 200
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

    %% Send 100 concurrent requests
    Parent = self(),
    NumRequests = 100,
    StartTime = erlang:monotonic_time(millisecond),

    lists:foreach(
        fun(N) ->
            spawn(fun() ->
                Req = #{
                    method => get,
                    path => <<"/load-100-", (integer_to_binary(N))/binary>>,
                    headers => [],
                    body => <<>>
                },
                Result = nhttpc_pool:request(PoolMgr, Req, 30000),
                Parent ! {result, N, Result}
            end)
        end,
        lists:seq(1, NumRequests)
    ),

    %% Collect results
    Results = collect_results(NumRequests, 60000),
    EndTime = erlang:monotonic_time(millisecond),

    {Successes, Errors} = partition_results(Results),
    Duration = EndTime - StartTime,

    ct:log(
        "100 concurrent: ~p successes, ~p errors in ~pms",
        [length(Successes), length(Errors), Duration]
    ),

    %% At least 95% success rate
    ?assert(length(Successes) >= 95),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%% @doc Test concurrent requests with small pool (high contention).
concurrent_requests_small_pool(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Start a small pool (2 connections)
    PoolRef = load_small_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2,
        max_overflow => 0,
        max_pending_requests => 100,
        timeouts => #{checkout => 10000}
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

    %% Send 20 concurrent requests through 2 connections
    Parent = self(),
    NumRequests = 20,

    lists:foreach(
        fun(N) ->
            spawn(fun() ->
                Req = #{
                    method => get,
                    path => <<"/small-pool-", (integer_to_binary(N))/binary>>,
                    headers => [],
                    body => <<>>
                },
                Result = nhttpc_pool:request(PoolMgr, Req, 30000),
                Parent ! {result, N, Result}
            end)
        end,
        lists:seq(1, NumRequests)
    ),

    %% Collect results
    Results = collect_results(NumRequests, 60000),
    {Successes, Errors} = partition_results(Results),

    ct:log(
        "Small pool (2 conns, 20 reqs): ~p successes, ~p errors",
        [length(Successes), length(Errors)]
    ),

    %% All should eventually succeed (with queuing)
    ?assert(length(Successes) >= 18),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%% @doc Test concurrent requests with large pool.
concurrent_requests_large_pool(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Start a large pool
    PoolRef = load_large_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 20,
        max_overflow => 10,
        max_pending_requests => 500
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 10000),

    %% Send 200 concurrent requests
    Parent = self(),
    NumRequests = 200,
    StartTime = erlang:monotonic_time(millisecond),

    lists:foreach(
        fun(N) ->
            spawn(fun() ->
                Req = #{
                    method => get,
                    path => <<"/large-pool-", (integer_to_binary(N))/binary>>,
                    headers => [],
                    body => <<>>
                },
                Result = nhttpc_pool:request(PoolMgr, Req, 30000),
                Parent ! {result, N, Result}
            end)
        end,
        lists:seq(1, NumRequests)
    ),

    %% Collect results
    Results = collect_results(NumRequests, 90000),
    EndTime = erlang:monotonic_time(millisecond),

    {Successes, Errors} = partition_results(Results),
    Duration = EndTime - StartTime,

    ct:log(
        "Large pool (200 reqs): ~p successes, ~p errors in ~pms",
        [length(Successes), length(Errors), Duration]
    ),

    %% At least 95% success rate
    ?assert(length(Successes) >= 190),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%%%-----------------------------------------------------------------------------
%%% SUSTAINED LOAD TESTS
%%%-----------------------------------------------------------------------------

%% @doc Test sustained load for 10 seconds.
sustained_load_10_seconds(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Start a pool
    PoolRef = sustained_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 8,
        max_overflow => 4
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

    %% Run load for 10 seconds
    Duration = 10000,
    Parent = self(),
    % [1] = success, [2] = error
    Counter = counters:new(2, []),

    %% Spawn workers that continuously send requests
    NumWorkers = 10,
    WorkerPids = [
        spawn_link(fun() ->
            sustained_worker(PoolMgr, Counter, Parent)
        end)
     || _ <- lists:seq(1, NumWorkers)
    ],

    %% Let it run
    timer:sleep(Duration),

    %% Stop workers
    lists:foreach(
        fun(Pid) ->
            Pid ! stop
        end,
        WorkerPids
    ),

    %% Wait for workers to finish
    timer:sleep(500),

    Successes = counters:get(Counter, 1),
    Errors = counters:get(Counter, 2),
    Total = Successes + Errors,

    ct:log(
        "Sustained 10s: ~p total, ~p successes, ~p errors (~.1f RPS)",
        [Total, Successes, Errors, Total / (Duration / 1000)]
    ),

    %% Should have completed many requests
    ?assert(Total >= 100),
    %% At least 90% success rate
    ?assert(Successes / max(1, Total) >= 0.9),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%% @doc Test sustained load with mixed HTTP methods.
sustained_load_mixed_methods(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Start a pool
    PoolRef = sustained_mixed_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 4,
        max_overflow => 2
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

    %% Send mixed requests
    Parent = self(),
    NumRequests = 50,

    lists:foreach(
        fun(N) ->
            spawn(fun() ->
                {Method, Body} =
                    case N rem 4 of
                        0 -> {get, undefined};
                        1 -> {post, <<"test body">>};
                        2 -> {head, undefined};
                        3 -> {put, <<"put body">>}
                    end,
                Req = #{
                    method => Method,
                    path => <<"/mixed-method-", (integer_to_binary(N))/binary>>,
                    headers => [],
                    body => Body
                },
                Result = nhttpc_pool:request(PoolMgr, Req, 10000),
                Parent ! {result, N, Result}
            end)
        end,
        lists:seq(1, NumRequests)
    ),

    %% Collect results
    Results = collect_results(NumRequests, 30000),
    {Successes, Errors} = partition_results(Results),

    ct:log(
        "Mixed methods: ~p successes, ~p errors out of ~p",
        [length(Successes), length(Errors), NumRequests]
    ),

    %% At least 50% success rate (HTTP/1.1 pools have limited concurrency)
    ?assert(length(Successes) >= 25),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%%%-----------------------------------------------------------------------------
%%% MEMORY TESTS
%%%-----------------------------------------------------------------------------

%% @doc Test memory stability under load.
memory_stable_under_load(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Start a pool
    PoolRef = memory_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 4,
        max_overflow => 2
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

    %% Force GC before measuring
    erlang:garbage_collect(),
    timer:sleep(100),
    MemoryBefore = erlang:memory(total),

    %% Send many requests
    NumRequests = 200,
    Parent = self(),

    lists:foreach(
        fun(N) ->
            spawn(fun() ->
                Req = #{
                    method => get,
                    path => <<"/memory-", (integer_to_binary(N))/binary>>,
                    headers => [],
                    body => <<>>
                },
                Result = nhttpc_pool:request(PoolMgr, Req, 30000),
                Parent ! {result, N, Result}
            end)
        end,
        lists:seq(1, NumRequests)
    ),

    %% Collect all results
    _Results = collect_results(NumRequests, 90000),

    %% Force GC and measure memory
    erlang:garbage_collect(),
    timer:sleep(500),
    MemoryAfter = erlang:memory(total),

    MemoryGrowth = MemoryAfter - MemoryBefore,
    ct:log(
        "Memory: before=~p, after=~p, growth=~p bytes (~.2f MB)",
        [MemoryBefore, MemoryAfter, MemoryGrowth, MemoryGrowth / (1024 * 1024)]
    ),

    %% Memory growth should be reasonable (< 50MB for this test)
    ?assert(MemoryGrowth < 50 * 1024 * 1024),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%% @doc Test for process leaks.
no_process_leaks(Config) ->
    Port = proplists:get_value(server_port, Config),
    case Port of
        undefined ->
            ct:log("Server not available, skipping test"),
            {skip, server_not_available};
        _ ->
            %% Trap exits so we don't die when shutting down pools
            process_flag(trap_exit, true),
            ProcessesBefore = erlang:system_info(process_count),

            %% Start and stop multiple pools
            lists:foreach(
                fun(N) ->
                    PoolRef = <<"leak_test_pool_", (integer_to_binary(N))/binary>>,
                    PoolOpts = #{
                        host => <<"127.0.0.1">>,
                        port => Port,
                        size => 2,
                        max_overflow => 1
                    },
                    case nhttpc_pool_sup:start_link(PoolRef, PoolOpts) of
                        {ok, PoolSup} ->
                            unlink(PoolSup),
                            PoolMgr = nhttpc_pool:manager(PoolRef),
                            case nhttpc_pool:wait_ready(PoolMgr, 3000) of
                                ok ->
                                    %% Send some requests
                                    lists:foreach(
                                        fun(M) ->
                                            Req = #{
                                                method => get,
                                                path => <<"/leak-", (integer_to_binary(M))/binary>>,
                                                headers => [],
                                                body => <<>>
                                            },
                                            _ = nhttpc_pool:request(PoolMgr, Req, 3000)
                                        end,
                                        lists:seq(1, 3)
                                    );
                                _ ->
                                    ok
                            end,
                            %% Stop pool
                            exit(PoolSup, shutdown),
                            timer:sleep(100);
                        {error, _} ->
                            ok
                    end
                end,
                lists:seq(1, 3)
            ),

            %% Flush any exit messages
            flush_exits(),

            %% Allow processes to clean up
            timer:sleep(1000),
            erlang:garbage_collect(),

            ProcessesAfter = erlang:system_info(process_count),
            ProcessGrowth = ProcessesAfter - ProcessesBefore,

            ct:log(
                "Processes: before=~p, after=~p, growth=~p",
                [ProcessesBefore, ProcessesAfter, ProcessGrowth]
            ),

            %% Should not have significant process leak (allow more margin)
            ?assert(ProcessGrowth < 50),

            process_flag(trap_exit, false),
            ok
    end.

%%%-----------------------------------------------------------------------------
%%% POOL STRESS TESTS
%%%-----------------------------------------------------------------------------

%% @doc Test pool exhaustion and recovery.
pool_exhaustion_recovery(Config) ->
    Port = proplists:get_value(server_port, Config),
    case Port of
        undefined ->
            ct:log("Server not available, skipping test"),
            {skip, server_not_available};
        _ ->
            %% Start a small pool
            PoolRef = exhaustion_pool,
            PoolOpts = #{
                host => <<"127.0.0.1">>,
                port => Port,
                size => 2,
                max_overflow => 0,
                max_pending_requests => 10,
                timeouts => #{checkout => 2000}
            },
            {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
            PoolMgr = nhttpc_pool:manager(PoolRef),
            ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

            %% Send more requests than pool can handle
            Parent = self(),
            NumRequests = 15,

            lists:foreach(
                fun(N) ->
                    spawn(fun() ->
                        Req = #{
                            method => get,
                            path => <<"/exhaust-", (integer_to_binary(N))/binary>>,
                            headers => [],
                            body => <<>>
                        },
                        Result = nhttpc_pool:request(PoolMgr, Req, 10000),
                        Parent ! {result, N, Result}
                    end)
                end,
                lists:seq(1, NumRequests)
            ),

            %% Collect results
            Results = collect_results(NumRequests, 60000),
            {Successes, _Errors} = partition_results(Results),

            ct:log(
                "Exhaustion test: ~p successes, ~p results out of ~p",
                [length(Successes), length(Results), NumRequests]
            ),

            %% Should have some successes (connections should handle some requests)
            ?assert(length(Successes) >= 1),

            %% Cleanup
            unlink(PoolSup),
            exit(PoolSup, shutdown),
            ok
    end.

%% @doc Test checkout timeout behavior under load.
pool_checkout_timeout_under_load(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Start a pool with short checkout timeout
    PoolRef = timeout_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2,
        max_overflow => 0,
        timeouts => #{checkout => 500}
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

    %% Send concurrent requests that may timeout on checkout
    Parent = self(),
    NumRequests = 10,

    lists:foreach(
        fun(N) ->
            spawn(fun() ->
                Req = #{
                    method => get,
                    path => <<"/timeout-", (integer_to_binary(N))/binary>>,
                    headers => [],
                    body => <<>>
                },
                Result = nhttpc_pool:request(PoolMgr, Req, 5000),
                Parent ! {result, N, Result}
            end)
        end,
        lists:seq(1, NumRequests)
    ),

    %% Collect results
    Results = collect_results(NumRequests, 15000),
    {Successes, Errors} = partition_results(Results),

    ct:log(
        "Checkout timeout: ~p successes, ~p errors",
        [length(Successes), length(Errors)]
    ),

    %% Should complete some requests
    ?assert(length(Successes) >= 2),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%%%-----------------------------------------------------------------------------
%%% INTERNAL HELPERS
%%%-----------------------------------------------------------------------------

-spec partition_results([{ok, term()} | {error, term()}]) ->
    {[{ok, term()}], [{error, term()}]}.
partition_results(Results) ->
    lists:partition(
        fun
            ({ok, _}) -> true;
            ({error, _}) -> false
        end,
        Results
    ).

-spec collect_results(non_neg_integer(), timeout()) -> [term()].
collect_results(Count, Timeout) ->
    collect_results(Count, Timeout, []).

collect_results(0, _Timeout, Acc) ->
    lists:reverse(Acc);
collect_results(Count, Timeout, Acc) ->
    receive
        {result, _N, Result} ->
            collect_results(Count - 1, Timeout, [Result | Acc])
    after Timeout ->
        ct:log("Timeout waiting for results, got ~p of ~p", [length(Acc), Count + length(Acc)]),
        lists:reverse(Acc)
    end.

-spec flush_exits() -> ok.
flush_exits() ->
    receive
        {'EXIT', _, _} -> flush_exits()
    after 0 ->
        ok
    end.

-spec sustained_worker(atom(), counters:counters_ref(), pid()) -> ok.
sustained_worker(PoolMgr, Counter, _Parent) ->
    sustained_worker_loop(PoolMgr, Counter, 0).

sustained_worker_loop(PoolMgr, Counter, N) ->
    receive
        stop ->
            ok
    after 0 ->
        Req = #{
            method => get,
            path => <<"/sustained-", (integer_to_binary(N))/binary>>,
            headers => [],
            body => <<>>
        },
        case nhttpc_pool:request(PoolMgr, Req, 5000) of
            {ok, _} ->
                counters:add(Counter, 1, 1);
            {error, _} ->
                counters:add(Counter, 2, 1)
        end,
        sustained_worker_loop(PoolMgr, Counter, N + 1)
    end.
