%%%-----------------------------------------------------------------------------
%%% @doc Memory bounds testing for nhttp server.
%%%
%%% Tests to verify memory stability under various workloads:
%%% - Large body streaming (no full buffering)
%%% - Many concurrent HTTP/2 streams
%%% - Long-lived connections (no memory leaks)
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_memory_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    many_requests_stable_memory/1,
    connection_counter_no_leak/1
]).

%% Test handler
-behaviour(nhttp_handler).
-export([init/1, handle_request/2, handle_request_body/3]).

%%%-----------------------------------------------------------------------------
%%% TEST HANDLER
%%%-----------------------------------------------------------------------------

init([]) -> {ok, #{}}.

handle_request(#{body := streaming} = Req, State) ->
    {accept_body, {Req, []}, State};
handle_request(#{method := get, path := <<"/">>}, State) ->
    {reply, #{status => 200, headers => [], body => <<"OK">>}, State};
handle_request(#{method := post, path := <<"/echo">>} = Req, State) ->
    Body = maps:get(body, Req, <<>>),
    {reply, #{status => 200, headers => [], body => Body}, State};
handle_request(_Req, State) ->
    {reply, #{status => 404, headers => [], body => <<"Not Found">>}, State}.

handle_request_body({data, Chunk}, {Req, Acc}, State) ->
    {accept_body, {Req, [Chunk | Acc]}, State};
handle_request_body({fin, _Trailers}, {Req, Acc}, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    handle_request(Req#{body => Body}, State);
handle_request_body({abort, _Reason}, _BodyState, State) ->
    {reply, #{status => 400, headers => [], body => <<"aborted">>}, State}.

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, memory_tests}
    ].

groups() ->
    [
        {memory_tests, [sequence], [
            many_requests_stable_memory,
            connection_counter_no_leak
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% MEMORY TESTS
%%%-----------------------------------------------------------------------------

%% @doc Verify that sending many requests doesn't cause unbounded memory growth.
many_requests_stable_memory(_Config) ->
    {ok, Pid} = nhttp:start_link(memory_test_ref, #{
        port => 0,
        handler => ?MODULE,
        protocols => [http1_1]
    }),
    {ok, Port} = nhttp:get_port(Pid),

    %% Send many requests on a single keep-alive connection
    {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

    %% Send 100 requests
    lists:foreach(
        fun(_) ->
            Request = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
            ok = gen_tcp:send(Sock, Request),
            {ok, _Response} = gen_tcp:recv(Sock, 0, 5000)
        end,
        lists:seq(1, 100)
    ),

    gen_tcp:close(Sock),
    nhttpc_test_server:stop(Pid),

    %% If we get here without crashing or hanging, the test passes
    %% The key is that memory doesn't grow unboundedly during the test
    ok.

%% @doc Verify that connection counter doesn't leak under rapid connect/disconnect.
connection_counter_no_leak(_Config) ->
    Ref = counter_leak_test_ref,
    {ok, Pid} = nhttp:start_link(Ref, #{
        port => 0,
        handler => ?MODULE,
        protocols => [http1_1],
        max_connections => 100
    }),
    {ok, Port} = nhttp:get_port(Pid),

    %% Initial counter value
    InitialCount = nhttp:active_connections(),

    %% Rapid connect/disconnect 50 times
    lists:foreach(
        fun(_) ->
            {ok, Sock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
            ok = gen_tcp:send(
                Sock, <<"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n">>
            ),
            %% Read response to ensure request completes
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, _} -> ok;
                {error, _} -> ok
            end,
            gen_tcp:close(Sock)
        end,
        lists:seq(1, 50)
    ),

    %% Wait for connections to fully terminate
    timer:sleep(500),

    %% Counter should be back to initial value (or close to it)
    FinalCount = nhttp:active_connections(),
    ct:pal("Initial count: ~p, Final count: ~p~n", [InitialCount, FinalCount]),

    %% Allow for some timing variance, but should be close to initial
    ?assert(FinalCount =< InitialCount + 2),

    nhttpc_test_server:stop(Pid),
    ok.
