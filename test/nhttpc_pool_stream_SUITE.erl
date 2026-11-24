-module(nhttpc_pool_stream_SUITE).

-moduledoc """
`nhttpc:stream/2` on a pooled HTTP/2 connection and on a connection from
`nhttpc:open/2`, against `nhttpc_h2_script_peer`.
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
%%% EXPORTS - EVENTS
%%%-----------------------------------------------------------------------------
-export([
    cancel_resets_the_stream_and_keeps_the_connection/1,
    connection_exit_reaches_the_owner/1,
    connection_exit_reaches_the_owner_after_a_guard_exit/1,
    first_data_arrives_while_the_peer_holds_the_second/1,
    goaway_lets_a_live_stream_finish/1,
    owner_exit_resets_the_stream/1,
    peer_reset_carries_the_reason_of_request/1,
    pool_stop_reaches_the_owner/1,
    request_timeout_ends_the_stream/1,
    response_above_the_size_bound_ends_the_stream/1,
    response_without_body_ends_with_done/1,
    trailers_come_before_done/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - CREDIT
%%%-----------------------------------------------------------------------------
-export([
    consume_initial_adds_to_the_stream_window/1,
    consume_initial_infinity_credits_on_arrival/1,
    hundred_thousand_frames_keep_the_connection_heap_bounded/1,
    stalled_consumer_stops_one_stream_only/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - DISPATCH
%%%-----------------------------------------------------------------------------
-export([
    connection_timeout_gives_the_stream_slot_back/1,
    http1_pool_rejects_a_stream/1,
    invalid_consume_initial_is_rejected/1,
    invalid_via_is_rejected/1,
    infinite_timeout_request_waits_for_a_connection/1,
    infinite_timeout_stream_waits_for_a_connection/1,
    iterator_body_is_rejected/1,
    open_timeout_cancels_the_stream/1,
    pool_without_a_request_timeout_serves/1,
    refused_stream_goes_to_another_connection/1,
    stream_on_an_open_connection/1,
    unknown_pool_is_an_error/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - ONESHOT
%%%-----------------------------------------------------------------------------
-export([
    oneshot_cleartext_http2_gets_a_response/1,
    oneshot_owner_exit_leaves_no_process_and_no_socket/1
]).

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(WINDOW, 65535).
-define(WAIT_MS, 5000).
-define(POLL_MS, 20).
-define(QUIET_MS, 200).
-define(STOP_SETTLE_MS, 100).
-define(STALL_BODY, 200000).
-define(SMALL_BODY, 1000).
-define(FRAMES, 100000).
-define(FRAME_SIZE, 85).
-define(HEAP_BOUND_WORDS, 262144).
-define(HEAP_SAMPLE_MS, 2).
-define(ADVANCE, 100000).
-define(SHORT_TIMEOUT_MS, 300).
-define(SLOW_PEER_MS, 2000).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [{group, events}, {group, credit}, {group, dispatch}, {group, oneshot}].

groups() ->
    [
        {events, [], [
            first_data_arrives_while_the_peer_holds_the_second,
            trailers_come_before_done,
            response_without_body_ends_with_done,
            cancel_resets_the_stream_and_keeps_the_connection,
            owner_exit_resets_the_stream,
            peer_reset_carries_the_reason_of_request,
            goaway_lets_a_live_stream_finish,
            request_timeout_ends_the_stream,
            response_above_the_size_bound_ends_the_stream,
            connection_exit_reaches_the_owner,
            connection_exit_reaches_the_owner_after_a_guard_exit,
            pool_stop_reaches_the_owner
        ]},
        {credit, [], [
            stalled_consumer_stops_one_stream_only,
            hundred_thousand_frames_keep_the_connection_heap_bounded,
            consume_initial_infinity_credits_on_arrival,
            consume_initial_adds_to_the_stream_window
        ]},
        {dispatch, [], [
            stream_on_an_open_connection,
            http1_pool_rejects_a_stream,
            unknown_pool_is_an_error,
            invalid_consume_initial_is_rejected,
            invalid_via_is_rejected,
            iterator_body_is_rejected,
            infinite_timeout_stream_waits_for_a_connection,
            infinite_timeout_request_waits_for_a_connection,
            connection_timeout_gives_the_stream_slot_back,
            pool_without_a_request_timeout_serves,
            open_timeout_cancels_the_stream,
            refused_stream_goes_to_another_connection
        ]},
        {oneshot, [], [
            oneshot_owner_exit_leaves_no_process_and_no_socket,
            oneshot_cleartext_http2_gets_a_response
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
%%% EVENTS
%%%-----------------------------------------------------------------------------

first_data_arrives_while_the_peer_holds_the_second(Config) ->
    {Peer, Sup, Conn} = start(ps_first_data, Config, #{}),
    {ok, H} = nhttpc:stream(manual(), #{via => {pool, ps_first_data}}),
    Id = stream_id(Conn, 1),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, ok_headers(), nofin}),
    ok = nhttpc_h2_script_peer:command(Conn, {data, Id, <<"one">>, nofin}),
    ?assertMatch({headers, 200, _}, event(H)),
    ?assertEqual({data, <<"one">>, more}, event(H)),
    ?assertEqual(quiet, quiet(H)),
    ok = nhttpc_h2_script_peer:command(Conn, {data, Id, <<"two">>, fin}),
    ?assertEqual({data, <<"two">>, more}, event(H)),
    ?assertEqual({done, normal}, event(H)),
    ?assertEqual(quiet, quiet(H)),
    stop(Peer, Sup).

trailers_come_before_done(Config) ->
    {Peer, Sup, Conn} = start(ps_trailers, Config, #{}),
    {ok, H} = nhttpc:stream(manual(), #{via => {pool, ps_trailers}}),
    Id = stream_id(Conn, 1),
    Trailers = [{<<"grpc-status">>, <<"0">>}],
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, ok_headers(), nofin}),
    ok = nhttpc_h2_script_peer:command(Conn, {data, Id, <<"message">>, nofin}),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, Trailers, fin}),
    ?assertMatch({headers, 200, _}, event(H)),
    ?assertEqual({data, <<"message">>, more}, event(H)),
    ?assertEqual({trailers, Trailers}, event(H)),
    ?assertEqual({done, normal}, event(H)),
    stop(Peer, Sup).

response_without_body_ends_with_done(Config) ->
    {Peer, Sup, _Conn} = start(ps_no_body, Config, #{}),
    {ok, H} = nhttpc:stream(get_request(<<"/body/0">>), #{via => {pool, ps_no_body}}),
    ?assertMatch({headers, 200, _}, event(H)),
    ?assertEqual({done, normal}, event(H)),
    ?assertEqual(quiet, quiet(H)),
    stop(Peer, Sup).

cancel_resets_the_stream_and_keeps_the_connection(Config) ->
    {Peer, Sup, Conn} = start(ps_cancel, Config, #{settings => #{max_concurrent_streams => 1}}),
    {ok, H} = nhttpc:stream(manual(), #{via => {pool, ps_cancel}}),
    Id = stream_id(Conn, 1),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, ok_headers(), nofin}),
    ok = nhttpc_h2_script_peer:command(Conn, {data, Id, <<"one">>, nofin}),
    ?assertMatch({headers, 200, _}, event(H)),
    ok = nhttpc:cancel(H),
    ?assertEqual(quiet, quiet(H)),
    ok = wait_reset(Conn, Id),
    ?assertEqual(cancel, maps:get(Id, maps:get(resets, nhttpc_h2_script_peer:stats(Conn)))),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(get_request(<<"/body/10">>), #{via => {pool, ps_cancel}})
    ),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    ?assert(is_process_alive(nhttpc_handle:pid(H))),
    stop(Peer, Sup).

owner_exit_resets_the_stream(Config) ->
    {Peer, Sup, Conn} = start(ps_owner_exit, Config, #{settings => #{max_concurrent_streams => 1}}),
    Test = self(),
    Owner = spawn(fun() ->
        Test ! {opened, self(), nhttpc:stream(manual(), #{via => {pool, ps_owner_exit}})},
        receive
            stop -> ok
        end
    end),
    receive
        {opened, Owner, {ok, _H}} -> ok
    after ?WAIT_MS -> ct:fail("the stream never opened")
    end,
    Id = stream_id(Conn, 1),
    exit(Owner, kill),
    ok = wait_reset(Conn, Id),
    ?assertEqual(cancel, maps:get(Id, maps:get(resets, nhttpc_h2_script_peer:stats(Conn)))),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(get_request(<<"/body/10">>), #{
            via => {pool, ps_owner_exit}, timeouts => #{request => ?WAIT_MS}
        })
    ),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

peer_reset_carries_the_reason_of_request(Config) ->
    {Peer, Sup, _Conn} = start(ps_peer_reset, Config, #{}),
    Via = #{via => {pool, ps_peer_reset}},
    lists:foreach(
        fun(Code) ->
            Req = get_request(<<"/reset/", (atom_to_binary(Code))/binary>>),
            {error, Reason} = nhttpc:request(Req, Via),
            {ok, H} = nhttpc:stream(Req, Via),
            ?assertEqual({Code, {error, Reason}}, {Code, event(H)}),
            ?assertEqual(quiet, quiet(H))
        end,
        [cancel, internal_error, enhance_your_calm, no_error, refused_stream]
    ),
    stop(Peer, Sup).

goaway_lets_a_live_stream_finish(Config) ->
    {Peer, Sup, Conn} = start(ps_goaway, Config, #{}),
    {ok, H} = nhttpc:stream(manual(), #{via => {pool, ps_goaway}}),
    Id = stream_id(Conn, 1),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, ok_headers(), nofin}),
    ?assertMatch({headers, 200, _}, event(H)),
    ok = nhttpc_h2_script_peer:command(Conn, {goaway, Id, no_error, <<>>}),
    ok = nhttpc_h2_script_peer:command(Conn, {data, Id, <<"late">>, nofin}),
    ?assertEqual({data, <<"late">>, more}, event(H)),
    ok = nhttpc:consume(H, 4),
    ok = nhttpc_h2_script_peer:command(Conn, {data, Id, <<"last">>, fin}),
    ?assertEqual({data, <<"last">>, more}, event(H)),
    ?assertEqual({done, normal}, event(H)),
    stop(Peer, Sup).

request_timeout_ends_the_stream(Config) ->
    {Peer, Sup, Conn} = start(ps_timeout, Config, #{settings => #{max_concurrent_streams => 1}}),
    {ok, H} = nhttpc:stream(manual(), #{
        via => {pool, ps_timeout}, timeouts => #{request => ?SHORT_TIMEOUT_MS}
    }),
    Id = stream_id(Conn, 1),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, ok_headers(), nofin}),
    ?assertMatch({headers, 200, _}, event(H)),
    ?assertEqual({error, {timeout, request}}, event(H)),
    ok = wait_reset(Conn, Id),
    ?assertEqual(cancel, maps:get(Id, maps:get(resets, nhttpc_h2_script_peer:stats(Conn)))),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(get_request(<<"/body/10">>), #{via => {pool, ps_timeout}})
    ),
    stop(Peer, Sup).

response_above_the_size_bound_ends_the_stream(Config) ->
    {Peer, Sup, _Conn} = start(ps_size, Config, #{}),
    Req = get_request(<<"/body/5000">>),
    {ok, H1} = nhttpc:stream(Req, #{via => {pool, ps_size}, max_response_size => 1000}),
    ?assertMatch({headers, 200, _}, event(H1)),
    ?assertEqual({error, {stream_error, exceeded_max_response_size}}, event(H1)),
    {ok, H2} = nhttpc:stream(Req, #{via => {pool, ps_size}, max_response_size => infinity}),
    ?assertEqual({ok, 5000}, download(H2)),
    stop(Peer, Sup).

connection_exit_reaches_the_owner(Config) ->
    {Peer, Sup, Conn} = start(ps_conn_exit, Config, #{}),
    {ok, H} = nhttpc:stream(manual(), #{via => {pool, ps_conn_exit}}),
    Id = stream_id(Conn, 1),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, ok_headers(), nofin}),
    ?assertMatch({headers, 200, _}, event(H)),
    exit(nhttpc_handle:pid(H), kill),
    ?assertEqual({error, {connect_error, closed}}, event(H)),
    ?assertEqual(quiet, quiet(H)),
    stop(Peer, Sup).

connection_exit_reaches_the_owner_after_a_guard_exit(Config) ->
    {Peer, Sup, Conn} = start(ps_guard_exit, Config, #{}),
    {ok, H} = nhttpc:stream(manual(), #{via => {pool, ps_guard_exit}}),
    ConnPid = nhttpc_handle:pid(H),
    Id = stream_id(Conn, 1),
    [Guard] = guards_of(ConnPid),
    exit(Guard, kill),
    ok = wait_until(fun() -> guards_of(ConnPid) =/= [] andalso guards_of(ConnPid) =/= [Guard] end),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, ok_headers(), nofin}),
    ?assertMatch({headers, 200, _}, event(H)),
    exit(ConnPid, kill),
    ?assertEqual({error, {connect_error, closed}}, event(H)),
    ?assertEqual(quiet, quiet(H)),
    stop(Peer, Sup).

pool_stop_reaches_the_owner(Config) ->
    {Peer, Sup, Conn} = start(ps_pool_stop, Config, #{}),
    {ok, H} = nhttpc:stream(manual(), #{via => {pool, ps_pool_stop}}),
    Id = stream_id(Conn, 1),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, Id, ok_headers(), nofin}),
    ?assertMatch({headers, 200, _}, event(H)),
    ok = nhttpc:stop(ps_pool_stop),
    ?assertEqual({error, {connect_error, closed}}, event(H)),
    ?assertEqual(quiet, quiet(H)),
    stop(Peer, Sup).

%%%-----------------------------------------------------------------------------
%%% CREDIT
%%%-----------------------------------------------------------------------------

stalled_consumer_stops_one_stream_only(Config) ->
    {Peer, Sup, Conn} = start(ps_stall, Config, #{}),
    Via = #{via => {pool, ps_stall}},
    {ok, Stalled} = nhttpc:stream(get_request(body_path(?STALL_BODY)), Via),
    Id = stream_id(Conn, 1),
    ok = wait_sent(Conn, Id, ?WINDOW),
    ?assertMatch(
        {ok, #{status := 200, body := <<_:?SMALL_BODY/binary>>}},
        nhttpc:request(get_request(body_path(?SMALL_BODY)), Via)
    ),
    {ok, Other} = nhttpc:stream(get_request(body_path(?STALL_BODY)), Via),
    ?assertEqual({ok, ?STALL_BODY}, download(Other)),
    timer:sleep(?QUIET_MS),
    ?assertEqual(?WINDOW, sent(Conn, Id)),
    ?assertEqual(0, maps:get(Id, maps:get(client_credit_streams, peer_stats(Conn)), 0)),
    ?assertEqual({ok, ?STALL_BODY}, download(Stalled)),
    stop(Peer, Sup).

hundred_thousand_frames_keep_the_connection_heap_bounded(Config) ->
    {Peer, Sup, Conn} = start(ps_frames, Config, #{}),
    Path =
        <<"/frames/", (integer_to_binary(?FRAMES))/binary, "/",
            (integer_to_binary(?FRAME_SIZE))/binary>>,
    {ok, H} = nhttpc:stream(get_request(Path), #{
        via => {pool, ps_frames}, max_response_size => infinity
    }),
    Sampler = start_heap_sampler(nhttpc_handle:pid(H)),
    ?assertMatch({headers, 200, _}, event(H)),
    {Events, Octets} = count_data(H, 0, 0),
    MaxHeap = stop_heap_sampler(Sampler),
    ct:pal("data events ~p, octets ~p, largest connection heap ~p words", [
        Events, Octets, MaxHeap
    ]),
    ?assertEqual(?FRAMES * ?FRAME_SIZE, Octets),
    ?assertEqual(?FRAMES, Events),
    ?assert(MaxHeap < ?HEAP_BOUND_WORDS),
    #{client_credit_streams := Credits, sent_octets_streams := Sents} = peer_stats(Conn),
    Id = stream_id(Conn, 1),
    ?assert(maps:get(Id, Credits) =< maps:get(Id, Sents)),
    stop(Peer, Sup).

consume_initial_infinity_credits_on_arrival(Config) ->
    {Peer, Sup, Conn} = start(ps_infinity, Config, #{}),
    {ok, H} = nhttpc:stream(get_request(body_path(?STALL_BODY)), #{
        via => {pool, ps_infinity}, consume_initial => infinity
    }),
    ?assertEqual({ok, ?STALL_BODY}, collect(H, 0)),
    Id = stream_id(Conn, 1),
    ?assert(maps:get(Id, maps:get(client_credit_streams, peer_stats(Conn))) > 0),
    stop(Peer, Sup).

consume_initial_adds_to_the_stream_window(Config) ->
    {Peer, Sup, Conn} = start(ps_advance, Config, #{}),
    {ok, H} = nhttpc:stream(get_request(body_path(?WINDOW + ?ADVANCE + ?STALL_BODY)), #{
        via => {pool, ps_advance}, consume_initial => ?ADVANCE
    }),
    Id = stream_id(Conn, 1),
    ok = wait_sent(Conn, Id, ?ADVANCE + ?WINDOW div 2),
    timer:sleep(?QUIET_MS),
    Sent = sent(Conn, Id),
    ?assert(Sent =< ?ADVANCE + ?WINDOW),
    timer:sleep(?QUIET_MS),
    ?assertEqual(Sent, sent(Conn, Id)),
    ok = nhttpc:cancel(H),
    stop(Peer, Sup).

%%%-----------------------------------------------------------------------------
%%% DISPATCH
%%%-----------------------------------------------------------------------------

stream_on_an_open_connection(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{}),
    {ok, Connection} = nhttpc:open(#{host => <<"127.0.0.1">>, port => Port}, #{
        versions => [http2], tls => #{verify => verify_none}
    }),
    {ok, [Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    Via = #{via => {conn, Connection}},
    {ok, H1} = nhttpc:stream(get_request(body_path(?SMALL_BODY)), Via),
    ?assertEqual({ok, ?SMALL_BODY}, download(H1)),
    {ok, H2} = nhttpc:stream(manual(), Via),
    Id = stream_id(Conn, 2),
    ok = nhttpc:cancel(H2),
    ok = wait_reset(Conn, Id),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ok = nhttpc:close(Connection),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

http1_pool_rejects_a_stream(_Config) ->
    {ok, Server} = nhttp:start_link(ps_h1_server, #{
        port => 0, handler => nhttp_test_handler, versions => [http1_1]
    }),
    unlink(Server),
    {ok, Port} = nhttp:get_port(Server),
    {ok, Sup} = nhttpc:start_link(ps_h1_pool, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0,
        versions => [http1_1],
        timeouts => #{request => infinity}
    }),
    unlink(Sup),
    ok = nhttpc_pool:wait_ready(ps_h1_pool, ?WAIT_MS),
    Via = #{via => {pool, ps_h1_pool}},
    Req = get_request(<<"/echo">>),
    ?assertEqual({error, {invalid_opts, stream_requires_http2}}, nhttpc:stream(Req, Via)),
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(Req, Via)),
    ?assertEqual(
        {error, {invalid_opts, stream_requires_http2}},
        nhttpc:stream(Req, #{
            via => {conn, #{pid => self(), version => http1_1, peer => {{127, 0, 0, 1}, Port}}}
        })
    ),
    ok = nhttpc:stop(ps_h1_pool),
    ok = nhttp:stop(Server),
    ok.

unknown_pool_is_an_error(_Config) ->
    ?assertEqual(
        nhttpc:request(get_request(<<"/x">>), #{via => {pool, ps_no_such_pool}}),
        nhttpc:stream(get_request(<<"/x">>), #{via => {pool, ps_no_such_pool}})
    ),
    ok.

invalid_consume_initial_is_rejected(_Config) ->
    lists:foreach(
        fun(Via) ->
            lists:foreach(
                fun(Credit) ->
                    ?assertEqual(
                        {error, {invalid_opts, invalid_consume_initial}},
                        nhttpc:stream(manual(), #{via => Via, consume_initial => Credit})
                    )
                end,
                [invalid, -1, 1.5]
            )
        end,
        [oneshot, {pool, ps_no_such_pool}]
    ).

invalid_via_is_rejected(_Config) ->
    ?assertEqual({error, {invalid_opts, invalid_via}}, nhttpc:stream(manual(), #{via => pool})).

iterator_body_is_rejected(Config) ->
    {Peer, Sup, _Conn} = start(ps_iterator, Config, #{}),
    Iterator = fun() -> fin end,
    Req = #{method => post, path => <<"/body/0">>, headers => [], body => {stream, Iterator}},
    ?assertMatch(
        {error, {invalid_request, _}}, nhttpc:stream(Req, #{via => {pool, ps_iterator}})
    ),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc:request(get_request(<<"/body/10">>), #{via => {pool, ps_iterator}})
    ),
    stop(Peer, Sup).

infinite_timeout_stream_waits_for_a_connection(Config) ->
    {Peer, Sup, Conn} = start(ps_wait_stream, Config, #{
        settings => #{max_concurrent_streams => 1}
    }),
    Mgr = nhttpc_pool:manager(ps_wait_stream),
    Via = #{via => {pool, ps_wait_stream}},
    {ok, First} = nhttpc:stream(manual(), Via),
    FirstId = stream_id(Conn, 1),
    Test = self(),
    Waiter = spawn_link(fun() ->
        Result = nhttpc:stream(manual(), Via#{timeouts => #{request => infinity}}),
        Test ! {waiter, self(), Result},
        case Result of
            {ok, H} -> Test ! {waiter_events, self(), [event(H), event(H)]};
            {error, _} -> ok
        end
    end),
    timer:sleep(?QUIET_MS),
    ?assertEqual(
        {error, {timeout, checkout}},
        nhttpc:request(get_request(<<"/body/10">>), Via#{
            timeouts => #{request => ?SHORT_TIMEOUT_MS}
        })
    ),
    ?assertEqual(Mgr, nhttpc_pool:manager(ps_wait_stream)),
    ?assert(is_process_alive(Mgr)),
    receive
        {waiter, Waiter, Early} -> ct:fail("the waiter left the queue with ~p", [Early])
    after 0 -> ok
    end,
    ok = nhttpc_h2_script_peer:command(Conn, {headers, FirstId, ok_headers(), fin}),
    ?assertMatch({headers, 200, _}, event(First)),
    ?assertEqual({done, normal}, event(First)),
    receive
        {waiter, Waiter, Result} -> ?assertMatch({ok, _}, Result)
    after ?WAIT_MS -> ct:fail("the waiter never got a connection")
    end,
    SecondId = stream_id(Conn, 2),
    ok = nhttpc_h2_script_peer:command(Conn, {headers, SecondId, ok_headers(), fin}),
    receive
        {waiter_events, Waiter, Events} ->
            ?assertMatch([{headers, 200, _}, {done, normal}], Events)
    after ?WAIT_MS -> ct:fail("the second stream never ended")
    end,
    stop(Peer, Sup).

infinite_timeout_request_waits_for_a_connection(Config) ->
    {Peer, Sup, Conn} = start(ps_wait_request, Config, #{
        settings => #{max_concurrent_streams => 1}, response_delay_ms => 500
    }),
    Mgr = nhttpc_pool:manager(ps_wait_request),
    Via = #{via => {pool, ps_wait_request}, timeouts => #{request => infinity}},
    Req = get_request(<<"/body/10">>),
    Workers = [spawn_monitor(fun() -> exit({result, nhttpc:request(Req, Via)}) end) || _ <- [1, 2]],
    Results = [
        receive
            {'DOWN', MRef, process, Pid, {result, Result}} -> Result
        after ?WAIT_MS -> ct:fail("a request never finished")
        end
     || {Pid, MRef} <- Workers
    ],
    ?assertMatch([{ok, #{status := 200}}, {ok, #{status := 200}}], Results),
    ?assertEqual(Mgr, nhttpc_pool:manager(ps_wait_request)),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

connection_timeout_gives_the_stream_slot_back(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => 1}, response_delay_ms => ?SLOW_PEER_MS
    }),
    Sup = start_pool(ps_slot_back, Port, #{timeouts => #{request => ?SHORT_TIMEOUT_MS}}),
    Req = get_request(<<"/body/10">>),
    ?assertEqual({error, {timeout, request}}, nhttpc_pool:request(ps_slot_back, Req, 1500)),
    ?assertEqual({error, {timeout, request}}, nhttpc_pool:request(ps_slot_back, Req, 1500)),
    stop(Peer, Sup).

pool_without_a_request_timeout_serves(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{}),
    Sup = start_pool(ps_no_timeout, Port, #{timeouts => #{request => infinity}}),
    Via = #{via => {pool, ps_no_timeout}},
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    {ok, H} = nhttpc:stream(get_request(body_path(?SMALL_BODY)), Via),
    ?assertEqual({ok, ?SMALL_BODY}, download(H)),
    stop(Peer, Sup).

open_timeout_cancels_the_stream(Config) ->
    {Peer, Sup, Conn} = start(ps_open_timeout, Config, #{
        settings => #{max_concurrent_streams => 1}
    }),
    Via = #{via => {pool, ps_open_timeout}},
    {ok, Probe} = nhttpc:stream(get_request(<<"/body/0">>), Via),
    ?assertEqual({ok, 0}, download(Probe)),
    ConnPid = nhttpc_handle:pid(Probe),
    ok = sys:suspend(ConnPid),
    Test = self(),
    Owner = spawn_link(fun() ->
        Result = nhttpc:stream(manual(), Via#{timeouts => #{request => ?SHORT_TIMEOUT_MS}}),
        Test ! {open_result, self(), Result, erlang:process_info(self(), messages)}
    end),
    timer:sleep(2 * ?SHORT_TIMEOUT_MS),
    ok = sys:resume(ConnPid),
    receive
        {open_result, Owner, Result, {messages, Left}} ->
            ?assertEqual({error, {timeout, request}}, Result),
            ?assertEqual([], Left)
    after 2 * ?WAIT_MS -> ct:fail("stream/2 never returned")
    end,
    ?assertMatch({ok, #{status := 200}}, nhttpc:request(get_request(<<"/body/10">>), Via)),
    ?assertEqual([Conn], nhttpc_h2_script_peer:connections(Peer)),
    stop(Peer, Sup).

%% Two sends reach one connection in one batch, and the connection retires
%% after one stream. The second send is refused before a stream opens, and
%% the pool opens that stream on the connection that replaces the first.
refused_stream_goes_to_another_connection(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{}),
    Sup = start_pool(ps_refused, Port, #{max_streams_per_connection => 1}),
    {ok, [First]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    [{ConnPid, _Slots}] = tuple_to_list(pool_connections(ps_refused)),
    ok = sys:suspend(ConnPid),
    Test = self(),
    Owners = [
        spawn_link(fun() ->
            Test ! {opened, self(), nhttpc:stream(manual(), #{via => {pool, ps_refused}})},
            receive
                stop -> ok
            end
        end)
     || _ <- [1, 2]
    ],
    ok = wait_until(fun() ->
        {message_queue_len, Queued} = erlang:process_info(ConnPid, message_queue_len),
        Queued >= 2
    end),
    ok = sys:resume(ConnPid),
    Results = [
        receive
            {opened, Owner, Result} -> Result
        after ?WAIT_MS -> ct:fail("a stream never opened")
        end
     || Owner <- Owners
    ],
    ?assertMatch([{ok, _}, {ok, _}], Results),
    {ok, [First, Second]} = nhttpc_h2_script_peer:wait_connections(Peer, 2, ?WAIT_MS),
    ?assertEqual(1, maps:get(requests, peer_stats(First))),
    ok = wait_until(fun() -> maps:get(requests, peer_stats(Second)) =:= 1 end),
    lists:foreach(fun(Owner) -> Owner ! stop end, Owners),
    stop(Peer, Sup).

%%%-----------------------------------------------------------------------------
%%% ONESHOT
%%%-----------------------------------------------------------------------------

oneshot_owner_exit_leaves_no_process_and_no_socket(_Config) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    Url = <<"http://127.0.0.1:", (integer_to_binary(Port))/binary, "/held">>,
    {ok, Req} = nhttpc:url(get, Url),
    Test = self(),
    Owner = spawn(fun() ->
        Test ! {opened, self(), nhttpc:stream(Req, #{})},
        receive
            stop -> ok
        end
    end),
    {ok, Socket} = gen_tcp:accept(Listen, ?WAIT_MS),
    H =
        receive
            {opened, Owner, {ok, Handle}} -> Handle
        after ?WAIT_MS -> ct:fail("the stream never opened")
        end,
    {ok, _RequestBytes} = gen_tcp:recv(Socket, 0, ?WAIT_MS),
    Worker = nhttpc_handle:pid(H),
    {links, Links} = erlang:process_info(Worker, links),
    Monitors = [monitor(process, Pid) || Pid <- [Worker | Links], is_pid(Pid)],
    exit(Owner, kill),
    lists:foreach(
        fun(MRef) ->
            receive
                {'DOWN', MRef, process, _Pid, _Reason} -> ok
            after ?WAIT_MS -> ct:fail("a process of the stream outlived its owner")
            end
        end,
        Monitors
    ),
    ?assertEqual({error, closed}, gen_tcp:recv(Socket, 0, ?WAIT_MS)),
    ok = gen_tcp:close(Socket),
    ok = gen_tcp:close(Listen),
    ok.

oneshot_cleartext_http2_gets_a_response(_Config) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    Test = self(),
    Server = spawn_link(fun() -> cleartext_h2_server(Listen, Test) end),
    Url = <<"http://127.0.0.1:", (integer_to_binary(Port))/binary, "/small">>,
    {ok, Req} = nhttpc:url(get, Url),
    {ok, H} = nhttpc:stream(Req, #{versions => [http2], consume_initial => infinity}),
    ?assertMatch({headers, 200, _}, event(H)),
    ?assertEqual({ok, ?SMALL_BODY}, collect(H, 0)),
    receive
        {cleartext_h2_request, Server, Seen} ->
            ?assertMatch(#{method := get, scheme := http, path := <<"/small">>}, Seen)
    after ?WAIT_MS -> ct:fail("the server saw no HTTP/2 request")
    end,
    ok = gen_tcp:close(Listen),
    ok.

%%%-----------------------------------------------------------------------------
%%% CLEARTEXT HTTP/2 SERVER
%%%-----------------------------------------------------------------------------

-doc "Accept one cleartext connection and speak HTTP/2 with prior knowledge on it.".
-spec cleartext_h2_server(gen_tcp:socket(), pid()) -> ok.
cleartext_h2_server(Listen, Test) ->
    {ok, Socket} = gen_tcp:accept(Listen, ?WAIT_MS),
    H2 = nhttp_h2:new(server, #{}),
    ok = gen_tcp:send(Socket, nhttp_h2:preface(H2)),
    cleartext_h2_loop(Socket, H2, Test).

-spec cleartext_h2_loop(gen_tcp:socket(), nhttp_h2:conn(), pid()) -> ok.
cleartext_h2_loop(Socket, H2, Test) ->
    case gen_tcp:recv(Socket, 0, ?WAIT_MS) of
        {ok, Bytes} ->
            case nhttp_h2:recv(H2, Bytes) of
                {ok, Events, H2a} ->
                    cleartext_h2_loop(Socket, cleartext_h2_answer(Events, Socket, H2a, Test), Test);
                {ok, Events, H2a, ToSend} ->
                    ok = gen_tcp:send(Socket, ToSend),
                    cleartext_h2_loop(Socket, cleartext_h2_answer(Events, Socket, H2a, Test), Test);
                {error, _Reason} ->
                    gen_tcp:close(Socket)
            end;
        {error, _Reason} ->
            gen_tcp:close(Socket)
    end.

-spec cleartext_h2_answer([nhttp_h2:event()], gen_tcp:socket(), nhttp_h2:conn(), pid()) ->
    nhttp_h2:conn().
cleartext_h2_answer([], _Socket, H2, _Test) ->
    H2;
cleartext_h2_answer([{request, Id, Request, fin} | Rest], Socket, H2, Test) ->
    Test ! {cleartext_h2_request, self(), Request},
    {ok, H2a, Headers} = nhttp_h2:send_headers(H2, Id, ok_headers(), nofin),
    {ok, H2b, Data} = nhttp_h2:send_data(H2a, Id, binary:copy(<<$c>>, ?SMALL_BODY), fin),
    ok = gen_tcp:send(Socket, [Headers, Data]),
    cleartext_h2_answer(Rest, Socket, H2b, Test);
cleartext_h2_answer([_Event | Rest], Socket, H2, Test) ->
    cleartext_h2_answer(Rest, Socket, H2, Test).

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

-doc "The guard processes that monitor the connection `ConnPid`.".
-spec guards_of(pid()) -> [pid()].
guards_of(ConnPid) ->
    [
        Pid
     || Pid <- erlang:processes(),
        proc_lib:initial_call(Pid) =:= {nhttpc_stream_guard, init, ['Argument__1']},
        erlang:process_info(Pid, monitors) =:= {monitors, [{process, ConnPid}]}
    ].

-doc "The `{ConnPid, Slots}` pairs that the lock-free checkout of the pool reads.".
-spec pool_connections(atom()) -> tuple().
pool_connections(Pool) ->
    Sup = nhttpc_pool:lookup_sup(Pool),
    [{registry, Tab}] = [
        {registry, T}
     || T <- ets:all(), ets:info(T, owner) =:= Sup, ets:info(T, name) =:= nhttpc_registry
    ],
    nhttpc_registry:get_multiplexed_conns(Tab).

-spec peer_stats(pid()) -> nhttpc_h2_script_peer:stats().
peer_stats(Conn) ->
    nhttpc_h2_script_peer:stats(Conn).

-doc "The identifier of the `Nth` stream the client opened on this connection.".
-spec stream_id(pid(), pos_integer()) -> nhttp_lib:stream_id().
stream_id(Conn, Nth) ->
    ok = wait_until(fun() -> maps:get(requests, peer_stats(Conn)) >= Nth end),
    lists:nth(Nth, lists:reverse(maps:get(streams, peer_stats(Conn)))).

-spec sent(pid(), nhttp_lib:stream_id()) -> non_neg_integer().
sent(Conn, Id) ->
    maps:get(Id, maps:get(sent_octets_streams, peer_stats(Conn)), 0).

-spec wait_sent(pid(), nhttp_lib:stream_id(), non_neg_integer()) -> ok.
wait_sent(Conn, Id, Octets) ->
    wait_until(fun() -> sent(Conn, Id) >= Octets end).

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

-spec manual() -> nhttp_lib:request().
manual() ->
    get_request(<<"/manual">>).

-spec get_request(binary()) -> nhttp_lib:request().
get_request(Path) ->
    #{method => get, path => Path, headers => []}.

-spec body_path(non_neg_integer()) -> binary().
body_path(Size) ->
    <<"/body/", (integer_to_binary(Size))/binary>>.

-spec ok_headers() -> nhttp_lib:headers().
ok_headers() ->
    [{<<":status">>, <<"200">>}].

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

-doc "Read a stream to its end and give the credit back for each chunk.".
-spec download(nhttpc:handle()) -> {ok, non_neg_integer()} | {error, term()}.
download(H) ->
    download(H, 0).

-spec download(nhttpc:handle(), non_neg_integer()) -> {ok, non_neg_integer()} | {error, term()}.
download(H, Total) ->
    case event(H) of
        {headers, 200, _Headers} ->
            download(H, Total);
        {data, Chunk, more} ->
            ok = nhttpc:consume(H, byte_size(Chunk)),
            download(H, Total + byte_size(Chunk));
        {done, normal} ->
            {ok, Total};
        {error, Reason} ->
            {error, Reason}
    end.

-doc "Read a stream to its end and give no credit back.".
-spec collect(nhttpc:handle(), non_neg_integer()) -> {ok, non_neg_integer()} | {error, term()}.
collect(H, Total) ->
    case event(H) of
        {headers, 200, _Headers} -> collect(H, Total);
        {data, Chunk, _Fin} -> collect(H, Total + byte_size(Chunk));
        {done, normal} -> {ok, Total};
        {error, Reason} -> {error, Reason}
    end.

-spec count_data(nhttpc:handle(), non_neg_integer(), non_neg_integer()) ->
    {non_neg_integer(), non_neg_integer()}.
count_data(H, Events, Octets) ->
    case event(H) of
        {data, Chunk, more} ->
            ok = nhttpc:consume(H, byte_size(Chunk)),
            count_data(H, Events + 1, Octets + byte_size(Chunk));
        {done, normal} ->
            {Events, Octets}
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
