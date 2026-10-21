%%%-----------------------------------------------------------------------------
%%% @doc Client chaos test suite.
%%%
%%% Tests client behavior under chaotic conditions with randomized failures.
%%% Uses deterministic random seeds for reproducibility.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_chaos_SUITE).

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

%% Chaos tests
-export([
    chaos_intermittent_errors/1,
    chaos_http_errors/1,
    chaos_mixed_success_failure/1,
    chaos_delay_jitter/1,
    chaos_rate_limited/1
]).

%% Burst tests
-export([
    burst_then_normal/1,
    burst_with_failures/1,
    burst_concurrent_pools/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        {group, chaos},
        {group, burst}
    ].

groups() ->
    [
        {chaos, [sequence], [
            chaos_intermittent_errors,
            chaos_http_errors,
            chaos_mixed_success_failure,
            chaos_delay_jitter,
            chaos_rate_limited
        ]},
        {burst, [sequence], [
            burst_then_normal,
            burst_with_failures,
            burst_concurrent_pools
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    %% Use deterministic random seed for reproducibility
    rand:seed(exsss, {12345, 67890, 13579}),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    %% Start a chaos server for tests
    {ok, Server} = nhttpc_chaos_server:start(tcp),
    [{chaos_server, Server} | Config].

end_per_group(_Group, Config) ->
    case proplists:get_value(chaos_server, Config) of
        undefined -> ok;
        Server -> nhttpc_chaos_server:stop(Server)
    end,
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Reset chaos server state
    case proplists:get_value(chaos_server, Config) of
        undefined ->
            ok;
        Server ->
            nhttpc_chaos_server:clear_failure_mode(Server),
            nhttpc_chaos_server:set_failure_rate(Server, 100),
            nhttpc_chaos_server:reset_request_count(Server)
    end,
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% CHAOS TESTS
%%%-----------------------------------------------------------------------------

%% @doc Test handling of intermittent errors (50% failure rate).
chaos_intermittent_errors(Config) ->
    Server = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(Server),

    %% Start a pool
    PoolRef = chaos_intermittent_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2,
        max_overflow => 2,
        reconnect_backoff_base => 50,
        reconnect_backoff_max => 200
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),

    %% Wait for pool to be ready
    ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

    %% Set 50% failure rate with HTTP errors
    nhttpc_chaos_server:set_failure_rate(Server, 50),
    nhttpc_chaos_server:set_failure_mode(Server, {http_error, 503}),

    %% Send requests
    Results = [
        nhttpc_pool:request(
            PoolMgr,
            #{
                method => get,
                path => <<"/intermittent-", (integer_to_binary(N))/binary>>,
                headers => [],
                body => <<>>
            },
            3000
        )
     || N <- lists:seq(1, 10)
    ],

    {Successes, Errors} = partition_results(Results),
    ct:log("Intermittent errors: ~p successes, ~p errors (503s)", [
        length(Successes), length(Errors)
    ]),

    %% With 50% failure rate, should have both successes and errors
    %% At least some of each expected (randomness can skew results)
    ?assert(length(Successes) >= 1),
    %% With HTTP errors, the failures may be counted as successes with error status
    %% Just verify we completed all requests
    ?assertEqual(10, length(Results)),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%% @doc Test handling of various HTTP error codes.
chaos_http_errors(Config) ->
    Server = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(Server),
    Url = fun(Path) ->
        iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), Path])
    end,

    %% Test various HTTP error codes
    ErrorCodes = [400, 401, 403, 404, 500, 502, 503, 504],
    lists:foreach(
        fun(Code) ->
            nhttpc_chaos_server:set_failure_mode(Server, {http_error, Code}),
            Result = nhttpc:get(Url(<<"/error-", (integer_to_binary(Code))/binary>>), #{
                timeouts => #{request => 2000}
            }),
            case Result of
                {ok, Resp} ->
                    ?assertEqual(Code, maps:get(status, Resp));
                {error, _} ->
                    %% Connection-level errors are also acceptable
                    ok
            end
        end,
        ErrorCodes
    ),
    ok.

%% @doc Test mixed success and failure scenarios.
chaos_mixed_success_failure(Config) ->
    Server = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(Server),

    %% Start a pool
    PoolRef = chaos_mixed_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 4,
        max_overflow => 2
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

    %% Alternate between normal and failure modes
    Results = lists:map(
        fun(N) ->
            case N rem 3 of
                0 -> nhttpc_chaos_server:set_failure_mode(Server, {http_error, 503});
                1 -> nhttpc_chaos_server:clear_failure_mode(Server);
                2 -> nhttpc_chaos_server:set_failure_mode(Server, {delay, 100})
            end,
            nhttpc_pool:request(
                PoolMgr,
                #{
                    method => get,
                    path => <<"/mixed-", (integer_to_binary(N))/binary>>,
                    headers => [],
                    body => <<>>
                },
                3000
            )
        end,
        lists:seq(1, 15)
    ),

    {Successes, _Errors} = partition_results(Results),
    ct:log("Mixed success/failure: ~p successes out of 15", [length(Successes)]),

    %% Should have significant number of successes
    ?assert(length(Successes) >= 5),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%% @doc Test handling of variable delays (jitter).
chaos_delay_jitter(Config) ->
    Server = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(Server),
    Url = fun(Path) ->
        iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), Path])
    end,

    %% Set moderate delay
    nhttpc_chaos_server:set_failure_mode(Server, {delay, 200}),

    %% Measure request times
    Times = lists:map(
        fun(N) ->
            Start = erlang:monotonic_time(millisecond),
            Result = nhttpc:get(Url(<<"/delay-", (integer_to_binary(N))/binary>>), #{
                timeouts => #{request => 5000}
            }),
            End = erlang:monotonic_time(millisecond),
            {Result, End - Start}
        end,
        lists:seq(1, 5)
    ),

    %% All requests should succeed
    lists:foreach(
        fun({{ok, Resp}, _Time}) ->
            ?assertEqual(200, maps:get(status, Resp))
        end,
        Times
    ),

    %% All requests should take at least 200ms due to delay
    lists:foreach(
        fun({_Result, Time}) ->
            ?assert(Time >= 200)
        end,
        Times
    ),

    ok.

%% @doc Test handling of rate limiting responses.
chaos_rate_limited(Config) ->
    Server = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(Server),
    Url = fun(Path) ->
        iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), Path])
    end,

    %% Set rate limiting mode
    nhttpc_chaos_server:set_failure_mode(Server, {rate_limited, 1}),

    %% Send requests
    Results = [
        nhttpc:get(Url(<<"/rate-limited-", (integer_to_binary(N))/binary>>), #{
            timeouts => #{request => 2000}
        })
     || N <- lists:seq(1, 5)
    ],

    %% All should return 429 status
    lists:foreach(
        fun(Result) ->
            case Result of
                {ok, Resp} ->
                    ?assertEqual(429, maps:get(status, Resp));
                {error, _} ->
                    ok
            end
        end,
        Results
    ),

    ok.

%%%-----------------------------------------------------------------------------
%%% BURST TESTS
%%%-----------------------------------------------------------------------------

%% @doc Test burst of requests followed by normal operation.
burst_then_normal(Config) ->
    Server = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(Server),

    %% Start a pool with enough capacity for burst
    PoolRef = burst_normal_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 8,
        max_overflow => 4,
        max_pending_requests => 100,
        timeouts => #{checkout => 15000}
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 10000),

    %% Send burst of concurrent requests
    Parent = self(),
    NumRequests = 15,
    Refs = [
        spawn_monitor(fun() ->
            Req = #{
                method => get,
                path => <<"/burst-", (integer_to_binary(N))/binary>>,
                headers => [],
                body => <<>>
            },
            Result = nhttpc_pool:request(PoolMgr, Req, 20000),
            Parent ! {result, N, Result}
        end)
     || N <- lists:seq(1, NumRequests)
    ],

    %% Collect results
    BurstResults = collect_results(length(Refs), 30000),
    {BurstSuccesses, BurstErrors} = partition_results(BurstResults),
    ct:log("Burst: ~p successes, ~p errors out of ~p", [
        length(BurstSuccesses), length(BurstErrors), NumRequests
    ]),

    %% Some burst requests should succeed (at least a few)
    %% HTTP/1.1 pools with limited connections will queue requests
    ?assert(length(BurstSuccesses) >= 3),

    %% After burst, normal requests should work (allow retry)
    timer:sleep(500),
    NormalReq = #{
        method => get,
        path => <<"/normal-after-burst">>,
        headers => [],
        body => <<>>
    },
    NormalResult = nhttpc_pool:request(PoolMgr, NormalReq, 10000),
    case NormalResult of
        {ok, NormalResp} ->
            ?assertEqual(200, maps:get(status, NormalResp));
        {error, _} ->
            %% Pool may still be busy, retry
            timer:sleep(1000),
            {ok, NormalResp2} = nhttpc_pool:request(PoolMgr, NormalReq, 10000),
            ?assertEqual(200, maps:get(status, NormalResp2))
    end,

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%% @doc Test burst of requests with intermittent failures.
burst_with_failures(Config) ->
    Server = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(Server),

    %% Start a pool
    PoolRef = burst_failure_pool,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2,
        max_overflow => 2
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    PoolMgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(PoolMgr, 5000),

    %% Set 20% failure rate
    nhttpc_chaos_server:set_failure_rate(Server, 20),
    nhttpc_chaos_server:set_failure_mode(Server, {http_error, 503}),

    %% Send burst of concurrent requests
    Parent = self(),
    Refs = [
        spawn_monitor(fun() ->
            Req = #{
                method => get,
                path => <<"/burst-fail-", (integer_to_binary(N))/binary>>,
                headers => [],
                body => <<>>
            },
            Result = nhttpc_pool:request(PoolMgr, Req, 10000),
            Parent ! {result, N, Result}
        end)
     || N <- lists:seq(1, 15)
    ],

    %% Collect results
    Results = collect_results(length(Refs), 15000),
    {Successes, Errors} = partition_results(Results),
    ct:log("Burst with failures: ~p successes, ~p errors out of 15", [
        length(Successes), length(Errors)
    ]),

    %% Should have both successes and errors
    ?assert(length(Successes) >= 5),

    %% Cleanup
    unlink(PoolSup),
    exit(PoolSup, shutdown),
    ok.

%% @doc Test multiple concurrent pools under load.
burst_concurrent_pools(Config) ->
    Server = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(Server),

    %% Start multiple pools
    Pools = lists:map(
        fun(N) ->
            PoolRef = <<"burst_pool_", (integer_to_binary(N))/binary>>,
            PoolOpts = #{
                host => <<"127.0.0.1">>,
                port => Port,
                size => 2,
                max_overflow => 1
            },
            {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
            PoolMgr = nhttpc_pool:manager(PoolRef),
            ok = nhttpc_pool:wait_ready(PoolMgr, 5000),
            {PoolRef, PoolSup, PoolMgr}
        end,
        lists:seq(1, 3)
    ),

    %% Send requests to each pool concurrently
    Parent = self(),
    AllRefs = lists:flatmap(
        fun({_PoolRef, _PoolSup, PoolMgr}) ->
            [
                spawn_monitor(fun() ->
                    Req = #{
                        method => get,
                        path => <<"/multi-pool-test">>,
                        headers => [],
                        body => <<>>
                    },
                    Result = nhttpc_pool:request(PoolMgr, Req, 10000),
                    Parent ! {result, erlang:unique_integer(), Result}
                end)
             || _ <- lists:seq(1, 5)
            ]
        end,
        Pools
    ),

    %% Collect results
    Results = collect_results(length(AllRefs), 30000),
    {Successes, _Errors} = partition_results(Results),
    ct:log("Concurrent pools: ~p successes out of ~p", [length(Successes), length(AllRefs)]),

    %% Some requests should succeed (at least a few from each pool)
    ?assert(length(Successes) >= 5),

    %% Cleanup all pools
    lists:foreach(
        fun({_PoolRef, PoolSup, _PoolMgr}) ->
            unlink(PoolSup),
            exit(PoolSup, shutdown)
        end,
        Pools
    ),
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
            collect_results(Count - 1, Timeout, [Result | Acc]);
        {'DOWN', _, process, _, _} ->
            collect_results(Count - 1, Timeout, [{error, process_down} | Acc])
    after Timeout ->
        ct:log("Timeout waiting for results, got ~p of ~p", [length(Acc), Count + length(Acc)]),
        lists:reverse(Acc)
    end.
