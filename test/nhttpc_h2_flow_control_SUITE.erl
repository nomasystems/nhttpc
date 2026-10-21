-module(nhttpc_h2_flow_control_SUITE).

-moduledoc """
HTTP/2 flow-control conformance. Group `critical_path` drives the pooled
client against a peer that credits the way APNs credits, or not at all,
and against a scriptable peer that moves its stream cap, sends GOAWAY
with a reason and records every WINDOW_UPDATE the client returns. Group
`codec_queue` drives the pooled client with bodies in the codec send
queue: one large body beside fifty small requests on a trickle of credit,
and a pool whose every connection holds a queued body against a silent
peer. The bound on the send queue, `send_buffer_bytes/1` never past
`max_send_buffer`, is sampled inside the second case on every
connection rather than pinned by a case of its own.

Group `recv_side_credit` holds `credit_never_exceeds_consumption`: the
client credits every stream at most what that stream consumed, and the
connection exactly what the peer sent, on the pooled path and on the
streaming path alike. `stream_recv_credit_matches_conn_path` pins that
both paths return the same WINDOW_UPDATE pattern for the same body.
`oneshot_body_above_window_completes` pins that the poolless path that
accumulates the body credits the peer too, so a body above the initial
window completes instead of waiting out the request timeout.

Nothing here is tuned to the code. A failing case is a finding.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% EXPORTS - CT
%%%-----------------------------------------------------------------------------
-export([
    all/0,
    end_per_group/2,
    end_per_suite/1,
    end_per_testcase/2,
    groups/0,
    init_per_group/2,
    init_per_suite/1,
    init_per_testcase/2,
    suite/0
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - CRITICAL PATH
%%%-----------------------------------------------------------------------------
-export([
    apns_cap_moves_mid_run/1,
    apns_cold_start_one_stream/1,
    apns_goaway_with_reason/1,
    blocked_client_still_reads/1,
    credit_never_exceeds_consumption/1,
    hpack_state_is_flat/1,
    no_window_ever_exceeds_max/1,
    oneshot_body_above_window_completes/1,
    production_shape_credit_withheld/1,
    production_shape_grows_the_pool/1,
    production_shape_recovers_on_credit/1,
    settings_shrink_to_negative_and_recover/1,
    stream_id_rotation_under_load/1,
    stream_id_rotation_with_lagging_manager/1,
    stream_recv_credit_matches_conn_path/1
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - CODEC QUEUE
%%%-----------------------------------------------------------------------------
-export([
    fairness_under_contention/1,
    send_buffer_full_does_not_wedge_the_pool/1
]).

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(BODY_SIZE, 134).
-define(WINDOW, 65535).
-define(FITS, (?WINDOW div ?BODY_SIZE)).
-define(PEER_CAP, 1000).
-define(MAX_WINDOW, 16#7fffffff).

-define(WITHHELD_REQUESTS, 2000).
-define(WITHHELD_DELAY_MS, 300).
-define(WITHHELD_SIZE, 4).
-define(WITHHELD_OVERFLOW, 4).

-define(RECOVER_REQUESTS, 1500).
-define(RECOVER_CREDIT_DELAY_MS, 1000).

-define(GROW_SIZE, 1).
-define(GROW_OVERFLOW, 2).
-define(GROW_EXCESS, 40).
-define(GROW_CHECKOUT_MS, 1500).

-define(COLD_CONNS, 16).
-define(COLD_BURST, 3000).
-define(COLD_REDISPATCH_RATIO, 0.1).
-define(COLD_BOUND_MS, 20000).

-define(CAP_OPEN, 20).
-define(CAP_LOW, 5).
-define(CAP_PROBES, 6).
-define(CAP_HOLD_MS, 1500).
-define(CAP_PROBE_TIMEOUT_MS, 400).

-define(GOAWAY_REQUESTS, 10).
-define(GOAWAY_LAST_STREAM, 9).
-define(GOAWAY_HOLD_MS, 1500).
-define(GOAWAY_DEBUG, <<"{\"reason\":\"TooManyConnections\"}">>).

-define(CREDIT_BODY, 200000).
-define(CREDIT_POOLED, 24).
-define(CREDIT_STREAMED, 6).
-define(SAME_CREDIT_BODY, 20000).

-define(MAX_BODY, 100000).
-define(MAX_WORKERS, 16).
-define(MAX_PER_WORKER, 25).

-define(BLOCKED_BODY, 8192).
-define(BLOCKED_REQUESTS, 12).
-define(BLOCKED_FIT, (?WINDOW div ?BLOCKED_BODY)).

-define(SHRINK_OPEN, 5).
-define(SHRINK_HOLD_MS, 1000).

-define(ROTATE_THRESHOLD, 40).
-define(ROTATE_WORKERS, 16).
-define(ROTATE_PER_WORKER, 30).
-define(MANAGER_LAG_MS, 2000).

-define(HPACK_REQUESTS, 10000).
-define(HPACK_FIRST_SAMPLE, 1000).
-define(HPACK_WINDOW, 64).

-define(MAX_SEND_BUFFER, 131072).
-define(TRICKLE_QUANTUM, 32768).
-define(TRICKLE_INTERVAL_MS, 10).
-define(FAIR_SLOW_INTERVAL_MS, 100).
-define(FAIR_BIG_BODY, 1048576).
-define(FAIR_SMALL_BODY, 1024).
-define(FAIR_SMALL_REQUESTS, 50).
-define(FAIR_CHECKOUT_MS, 5000).
-define(FAIR_RELEASE_MS, 1000).

-define(WEDGE_BODY, 190000).
-define(WEDGE_CALLERS, 20).
-define(WEDGE_DEADLINE_MS, 1500).
-define(WEDGE_SLACK_MS, 500).
-define(WEDGE_SAMPLE_MS, 20).
-define(WEDGE_REDUCTION_BOUND, 120000).

-define(REQUEST_TIMEOUT_MS, 15000).
-define(LONG_TIMEOUT_MS, 30000).
-define(COLLECT_TIMEOUT_MS, 60000).
-define(WAIT_MS, 10000).
-define(POLL_MS, 20).
-define(SETTLE_MS, 100).
-define(STOP_SETTLE_MS, 100).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 10}}].

all() ->
    [{group, critical_path}, {group, recv_side_credit}, {group, codec_queue}].

groups() ->
    [
        {critical_path, [], [
            production_shape_credit_withheld,
            production_shape_recovers_on_credit,
            production_shape_grows_the_pool,
            apns_cold_start_one_stream,
            apns_cap_moves_mid_run,
            apns_goaway_with_reason,
            no_window_ever_exceeds_max,
            blocked_client_still_reads,
            settings_shrink_to_negative_and_recover,
            stream_id_rotation_under_load,
            stream_id_rotation_with_lagging_manager,
            hpack_state_is_flat
        ]},
        {recv_side_credit, [], [
            credit_never_exceeds_consumption,
            stream_recv_credit_matches_conn_path,
            oneshot_body_above_window_completes
        ]},
        {codec_queue, [], [
            fairness_under_contention,
            send_buffer_full_does_not_wedge_the_pool
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

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    detach_all(),
    ok.

%%%-----------------------------------------------------------------------------
%%% CRITICAL PATH
%%%-----------------------------------------------------------------------------

%% A peer that answers late and never credits the connection window. Every
%% body is admitted against reserved credit, so no connection dies, no
%% caller sees the connection go down, and every request completes or
%% carries the retryable refusal.
production_shape_credit_withheld(Config) ->
    {Server, Port, Counters} = start_nhttp(fc_withheld, Config, #{
        h2_connection_window_policy => never,
        h2_response_delay => ?WITHHELD_DELAY_MS,
        h2_settings => #{max_concurrent_streams => ?PEER_CAP}
    }),
    {Sup, Mgr} = start_pool(fc_withheld_pool, Port, #{
        size => ?WITHHELD_SIZE,
        max_overflow => ?WITHHELD_OVERFLOW,
        max_pending_requests => 2 * ?WITHHELD_REQUESTS,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    attach([[nhttpc, connect, error]]),
    Kinds = kinds(collect(spawn_requests(fc_withheld_pool, post(?BODY_SIZE), ?WITHHELD_REQUESTS))),
    Ok = length([K || {ok, 200} = K <- Kinds]),
    Refused = length([K || {stream_error, {protocol, refused_stream}} = K <- Kinds]),
    ?assertEqual([], [
        K
     || K <- Kinds, K =/= {ok, 200}, K =/= {stream_error, {protocol, refused_stream}}
    ]),
    ?assertEqual(?WITHHELD_REQUESTS, Ok + Refused),
    ?assertEqual(0, count_events([nhttpc, connect, error])),
    ConnPids = conn_pids(Mgr),
    ?assert(lists:all(fun erlang:is_process_alive/1, ConnPids)),
    ?assertEqual(length(ConnPids), total_created(Mgr)),
    ?assert(total_created(Mgr) =< ?WITHHELD_SIZE + ?WITHHELD_OVERFLOW),
    ?assert(total_created(Mgr) >= ceil_div(?WITHHELD_REQUESTS, ?FITS)),
    #{requests := Seen} = nhttpc_h2_flow_handler:read(Counters),
    ?assertEqual(Ok, Seen),
    stop_pool(Sup),
    stop_nhttp(Server),
    ok.

%% Credit that returns late releases the queued callers. The pool admits
%% only what the two connections can carry, holds the rest as waiters, and
%% serves them when the credit comes back. The peer saw one HEADERS per
%% completed request and the connection refused nothing.
production_shape_recovers_on_credit(Config) ->
    {Server, Port, Counters} = start_nhttp(fc_recover, Config, #{
        h2_connection_window_policy => {delay, ?RECOVER_CREDIT_DELAY_MS},
        h2_settings => #{max_concurrent_streams => ?PEER_CAP}
    }),
    {Sup, Mgr} = start_pool(fc_recover_pool, Port, #{
        size => 1,
        max_overflow => 1,
        max_pending_requests => 2 * ?RECOVER_REQUESTS,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    attach([[nhttpc, connection, send_credit_refused]]),
    Kinds = kinds(collect(spawn_requests(fc_recover_pool, post(?BODY_SIZE), ?RECOVER_REQUESTS))),
    Refusals = count_events([nhttpc, connection, send_credit_refused]),
    #{requests := Seen} = nhttpc_h2_flow_handler:read(Counters),
    ct:pal("recovery: ~p, ~p refusals re-dispatched, ~p HEADERS at the peer", [
        histogram(Kinds), Refusals, Seen
    ]),
    ?assertEqual(?RECOVER_REQUESTS, length([K || {ok, 200} = K <- Kinds])),
    ?assertEqual(?RECOVER_REQUESTS, Seen),
    ?assertEqual(2, total_created(Mgr)),
    ?assert(lists:all(fun erlang:is_process_alive/1, conn_pids(Mgr))),
    stop_pool(Sup),
    stop_nhttp(Server),
    ok.

%% Under `pack` the pool opens a connection only when credit runs short,
%% and stops at `size + max_overflow`. Three connections carry exactly
%% three windows of bodies; the callers past that time out on checkout.
production_shape_grows_the_pool(Config) ->
    {Server, Port, Counters} = start_nhttp(fc_grow, Config, #{
        h2_connection_window_policy => never,
        h2_settings => #{max_concurrent_streams => ?PEER_CAP}
    }),
    Bound = ?GROW_SIZE + ?GROW_OVERFLOW,
    Requests = Bound * ?FITS + ?GROW_EXCESS,
    {Sup, Mgr} = start_pool(fc_grow_pool, Port, #{
        size => ?GROW_SIZE,
        max_overflow => ?GROW_OVERFLOW,
        stream_selection => pack,
        max_pending_requests => 2 * Requests,
        timeouts => #{checkout => ?GROW_CHECKOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    Kinds = kinds(collect(spawn_requests(fc_grow_pool, post(?BODY_SIZE), Requests))),
    ?assertEqual(Bound * ?FITS, length([K || {ok, 200} = K <- Kinds])),
    ?assertEqual(?GROW_EXCESS, length([K || {timeout, checkout} = K <- Kinds])),
    ?assertEqual(Bound, total_created(Mgr)),
    ?assertEqual(Bound, length(conn_pids(Mgr))),
    #{requests := Seen, connections := Accepted} = nhttpc_h2_flow_handler:read(Counters),
    ?assertEqual(Bound * ?FITS, Seen),
    ?assertEqual(Bound, Accepted),
    stop_pool(Sup),
    stop_nhttp(Server),
    ok.

%% Sixteen connections each advertise one stream until their first
%% response, then a thousand. A burst of three thousand requests completes
%% with no failure, few re-dispatches, no extra connection, and inside a
%% bounded time.
apns_cold_start_one_stream(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => 1},
        after_first_response => [{settings, #{max_concurrent_streams => ?PEER_CAP}}]
    }),
    {Sup, Mgr} = start_pool(fc_cold_pool, Port, #{
        size => ?COLD_CONNS,
        max_overflow => 0,
        stream_selection => spread,
        max_pending_requests => 2 * ?COLD_BURST,
        timeouts => #{checkout => ?LONG_TIMEOUT_MS, request => ?LONG_TIMEOUT_MS}
    }),
    {ok, Conns} = nhttpc_h2_script_peer:wait_connections(Peer, ?COLD_CONNS, ?WAIT_MS),
    attach([[nhttpc, pool, checkout], [nhttpc, connect, start]]),
    T0 = erlang:monotonic_time(millisecond),
    Kinds = kinds(collect(spawn_requests(fc_cold_pool, post(?BODY_SIZE), ?COLD_BURST))),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ct:pal("cold start kinds after ~p ms: ~p", [Elapsed, histogram(Kinds)]),
    ?assertEqual(?COLD_BURST, length([K || {ok, 200} = K <- Kinds])),
    Checkouts = count_events([nhttpc, pool, checkout]),
    ?assert(Checkouts >= ?COLD_BURST),
    ?assert((Checkouts - ?COLD_BURST) / ?COLD_BURST =< ?COLD_REDISPATCH_RATIO),
    ?assertEqual(0, count_events([nhttpc, connect, start])),
    ?assertEqual(?COLD_CONNS, total_created(Mgr)),
    ?assertEqual(?COLD_CONNS, length(nhttpc_h2_script_peer:connections(Peer))),
    Seen = lists:sum([maps:get(requests, nhttpc_h2_script_peer:stats(C)) || C <- Conns]),
    ?assertEqual(?COLD_BURST, Seen),
    ?assert(Elapsed =< ?COLD_BOUND_MS),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%% The peer lowers its stream cap below the live count and raises it
%% again. The open streams complete, no new stream opens while the cap is
%% below the live count, and new requests flow once it is raised.
apns_cap_moves_mid_run(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        response_delay_ms => ?CAP_HOLD_MS
    }),
    {Sup, _Mgr} = start_pool(fc_cap_pool, Port, #{
        size => 1,
        max_overflow => 0,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    {ok, [Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    Open = spawn_requests(fc_cap_pool, post(?BODY_SIZE), ?CAP_OPEN),
    ok = wait_stat(Conn, requests, ?CAP_OPEN, ?WAIT_MS),
    ok = nhttpc_h2_script_peer:command(Conn, {settings, #{max_concurrent_streams => ?CAP_LOW}}),
    ok = barrier(Conn),
    timer:sleep(?SETTLE_MS),
    Probes = kinds(
        collect(spawn_requests(fc_cap_pool, post(?BODY_SIZE), ?CAP_PROBES, ?CAP_PROBE_TIMEOUT_MS))
    ),
    ?assertEqual(lists:duplicate(?CAP_PROBES, {timeout, checkout}), Probes),
    ?assertEqual(?CAP_OPEN, maps:get(requests, nhttpc_h2_script_peer:stats(Conn))),
    ok = nhttpc_h2_script_peer:command(Conn, {settings, #{max_concurrent_streams => ?PEER_CAP}}),
    ok = barrier(Conn),
    After = kinds(collect(spawn_requests(fc_cap_pool, post(?BODY_SIZE), ?CAP_PROBES))),
    ?assertEqual(lists:duplicate(?CAP_PROBES, {ok, 200}), After),
    ?assertEqual(lists:duplicate(?CAP_OPEN, {ok, 200}), kinds(collect(Open))),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%% A GOAWAY with a JSON payload. The streams at or below the last id
%% complete, the ones above it carry the peer's code and payload to the
%% caller, and the telemetry event carries the same three fields.
apns_goaway_with_reason(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        response_delay_ms => ?GOAWAY_HOLD_MS
    }),
    {Sup, _Mgr} = start_pool(fc_goaway_pool, Port, #{
        size => 1,
        max_overflow => 0,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    {ok, [Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    attach([[nhttpc, connection, goaway]]),
    Workers = spawn_requests(fc_goaway_pool, post(?BODY_SIZE), ?GOAWAY_REQUESTS),
    ok = wait_stat(Conn, requests, ?GOAWAY_REQUESTS, ?WAIT_MS),
    ok = nhttpc_h2_script_peer:command(
        Conn, {goaway, ?GOAWAY_LAST_STREAM, enhance_your_calm, ?GOAWAY_DEBUG}
    ),
    Kinds = kinds(collect(Workers)),
    Served = (?GOAWAY_LAST_STREAM + 1) div 2,
    ?assertEqual(Served, length([K || {ok, 200} = K <- Kinds])),
    ?assertEqual(
        ?GOAWAY_REQUESTS - Served,
        length([K || {protocol_error, http2, {h2, enhance_your_calm, ?GOAWAY_DEBUG}} = K <- Kinds])
    ),
    {ok, Meta} = wait_event([nhttpc, connection, goaway], ?WAIT_MS),
    ?assertMatch(
        #{
            error_code := enhance_your_calm,
            last_stream_id := ?GOAWAY_LAST_STREAM,
            debug_data := ?GOAWAY_DEBUG
        },
        Meta
    ),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%% The client never credits the peer for more than it consumed: the
%% stream WINDOW_UPDATE increments sum to at most that stream's octets,
%% and the connection increments to at most the connection total. A mixed
%% run of pooled responses and streamed downloads on one connection.
credit_never_exceeds_consumption(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        response_body => ?CREDIT_BODY
    }),
    {Sup, _Mgr} = start_pool(fc_credit_pool, Port, #{
        size => 1,
        max_overflow => 0,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    {ok, [_Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    Pooled = spawn_requests(fc_credit_pool, get_request(<<"/pooled">>), ?CREDIT_POOLED),
    Streamed = spawn_streams(Port, ?CREDIT_STREAMED),
    ?assertEqual(lists:duplicate(?CREDIT_POOLED, {ok, 200}), kinds(collect(Pooled))),
    ?assertEqual(lists:duplicate(?CREDIT_STREAMED, {ok, ?CREDIT_BODY}), collect(Streamed)),
    Conns = nhttpc_h2_script_peer:connections(Peer),
    ?assertEqual(1 + ?CREDIT_STREAMED, length(Conns)),
    Records = [nhttpc_h2_script_peer:stats(C) || C <- Conns],
    ct:pal("credit per connection: ~p", [
        [
            {maps:get(client_credit_conn, R), maps:get(sent_octets_conn, R), over_credit(R)}
         || R <- Records
        ]
    ]),
    ?assertEqual([], lists:append([over_credit(R) || R <- Records])),
    ?assertEqual(
        [],
        [
            {Credit, Sent}
         || #{client_credit_conn := Credit, sent_octets_conn := Sent} <- Records, Credit > Sent
        ]
    ),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%% One download on the pooled path and one on the streaming path, of the
%% same body from the same peer, return the same WINDOW_UPDATE pattern:
%% the connection is credited the octets it consumed, on the last frame,
%% and the stream is credited nothing, because its debt stays under the
%% batch threshold and is dropped on END_STREAM. One credit policy serves
%% both paths.
stream_recv_credit_matches_conn_path(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        response_body => ?SAME_CREDIT_BODY
    }),
    {Sup, _Mgr} = start_pool(fc_same_credit_pool, Port, #{
        size => 1,
        max_overflow => 0,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    {ok, [PooledConn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc_pool:request(fc_same_credit_pool, get_request(<<"/pooled">>), ?REQUEST_TIMEOUT_MS)
    ),
    ?assertEqual([{ok, ?SAME_CREDIT_BODY}], collect(spawn_streams(Port, 1))),
    {ok, [PooledConn, StreamedConn]} = nhttpc_h2_script_peer:wait_connections(Peer, 2, ?WAIT_MS),
    ok = wait_stat(PooledConn, client_credit_conn, ?SAME_CREDIT_BODY, ?WAIT_MS),
    ok = wait_stat(StreamedConn, client_credit_conn, ?SAME_CREDIT_BODY, ?WAIT_MS),
    Pooled = credit_pattern(nhttpc_h2_script_peer:stats(PooledConn)),
    Streamed = credit_pattern(nhttpc_h2_script_peer:stats(StreamedConn)),
    ct:pal("credit pattern pooled ~p streamed ~p", [Pooled, Streamed]),
    ?assertEqual({?SAME_CREDIT_BODY, #{}}, Pooled),
    ?assertEqual(Pooled, Streamed),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%% One GET on the poolless path with the default `body_to`, of a body
%% three windows long. The peer stops at the initial window until the
%% client credits it, so the request completes only if this path returns
%% credit as it consumes: the whole body arrives inside the timeout, the
%% connection is credited exactly the octets it sent, and the stream at
%% least the octets past the initial window and never more than it sent.
oneshot_body_above_window_completes(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        response_body => ?CREDIT_BODY
    }),
    Url = <<"https://127.0.0.1:", (integer_to_binary(Port))/binary, "/oneshot">>,
    Opts = #{
        tls => #{verify => verify_none},
        versions => [http2],
        timeouts => #{request => ?REQUEST_TIMEOUT_MS}
    },
    Result = nhttpc:get(Url, Opts),
    ?assertMatch({ok, #{status := 200}}, Result),
    {ok, #{body := Body}} = Result,
    ?assertEqual(?CREDIT_BODY, byte_size(Body)),
    {ok, [Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    ok = wait_stat(Conn, client_credit_conn, ?CREDIT_BODY, ?WAIT_MS),
    Stats = nhttpc_h2_script_peer:stats(Conn),
    ct:pal("oneshot credit pattern ~p", [credit_pattern(Stats)]),
    ?assertEqual([], over_credit(Stats)),
    ?assertEqual(?CREDIT_BODY, maps:get(client_credit_conn, Stats)),
    [StreamCredit] = maps:values(maps:get(client_credit_streams, Stats)),
    ?assert(StreamCredit >= ?CREDIT_BODY - ?WINDOW),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%% Over a long run of bodies no window the client advertises to the peer
%% passes 2^31-1, on the connection or on any stream.
no_window_ever_exceeds_max(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        response_body => ?MAX_BODY
    }),
    {Sup, _Mgr} = start_pool(fc_max_pool, Port, #{
        size => 1,
        max_overflow => 0,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    {ok, [Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    Workers = spawn_sequences(fc_max_pool, get_request(<<"/max">>), ?MAX_WORKERS, ?MAX_PER_WORKER),
    Results = collect(Workers),
    ?assertEqual(lists:duplicate(?MAX_WORKERS, {ok, ?MAX_PER_WORKER}), Results),
    #{max_conn_window := MaxConn, max_stream_window := MaxStream} =
        nhttpc_h2_script_peer:stats(Conn),
    ?assert(MaxConn =< ?MAX_WINDOW),
    ?assert(MaxStream =< ?MAX_WINDOW),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%% A peer that withholds all credit and then sends GOAWAY. The client
%% holds the excess callers as waiters, sees the GOAWAY while it is
%% starved, which proves it kept reading, and serves the waiters on the
%% replacement connection.
blocked_client_still_reads(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        credit => never
    }),
    {Sup, Mgr} = start_pool(fc_blocked_pool, Port, #{
        size => 1,
        max_overflow => 0,
        max_pending_requests => ?BLOCKED_REQUESTS,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    {ok, [Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    attach([[nhttpc, connection, goaway], [nhttpc, pool, credit_starved]]),
    Workers = spawn_requests(fc_blocked_pool, post(?BLOCKED_BODY), ?BLOCKED_REQUESTS),
    {ok, _Starved} = wait_event([nhttpc, pool, credit_starved], ?WAIT_MS),
    ok = wait_stat(Conn, responses, ?BLOCKED_FIT, ?WAIT_MS),
    ok = nhttpc_h2_script_peer:command(Conn, {goaway, ?MAX_WINDOW, no_error, <<>>}),
    {ok, _Meta} = wait_event([nhttpc, connection, goaway], ?WAIT_MS),
    Kinds = kinds(collect(Workers)),
    ?assertEqual(lists:duplicate(?BLOCKED_REQUESTS, {ok, 200}), Kinds),
    ?assertEqual(2, total_created(Mgr)),
    ?assertEqual(2, length(nhttpc_h2_script_peer:connections(Peer))),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%% The peer shrinks its initial stream window to zero with streams open,
%% which drives their windows negative, then restores it. The client sends
%% no DATA while the window is zero: a body that arrives meanwhile opens
%% its stream and waits in the send queue, and everything completes once
%% the window is back.
settings_shrink_to_negative_and_recover(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        response_delay_ms => ?SHRINK_HOLD_MS
    }),
    {Sup, _Mgr} = start_pool(fc_shrink_pool, Port, #{
        size => 1,
        max_overflow => 0,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    {ok, [Conn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    Open = spawn_requests(fc_shrink_pool, post(?BODY_SIZE), ?SHRINK_OPEN),
    ok = wait_stat(Conn, requests, ?SHRINK_OPEN, ?WAIT_MS),
    ok = nhttpc_h2_script_peer:command(Conn, {settings, #{initial_window_size => 0}}),
    ok = barrier(Conn),
    Blocked = spawn_requests(fc_shrink_pool, post(?BODY_SIZE), ?SHRINK_OPEN),
    ok = wait_stat(Conn, requests, 2 * ?SHRINK_OPEN, ?WAIT_MS),
    ok = barrier(Conn),
    #{data_octets_while_zero := Zero, data_octets := Octets} = nhttpc_h2_script_peer:stats(Conn),
    ?assertEqual(0, Zero),
    ?assertEqual(?SHRINK_OPEN * ?BODY_SIZE, Octets),
    ok = nhttpc_h2_script_peer:command(Conn, {settings, #{initial_window_size => ?WINDOW}}),
    ok = barrier(Conn),
    ?assertEqual(lists:duplicate(?SHRINK_OPEN, {ok, 200}), kinds(collect(Blocked))),
    ?assertEqual(lists:duplicate(?SHRINK_OPEN, {ok, 200}), kinds(collect(Open))),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%% Connections retire at a low stream threshold under load and the pool
%% replaces them. No request fails across the rotations.
stream_id_rotation_under_load(Config) ->
    {Server, Port, Counters} = start_nhttp(fc_rotate, Config, #{
        h2_settings => #{max_concurrent_streams => ?PEER_CAP}
    }),
    {Sup, _Mgr} = start_pool(fc_rotate_pool, Port, #{
        size => 2,
        max_overflow => 0,
        max_streams_per_connection => ?ROTATE_THRESHOLD,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    attach([[nhttpc, connection, rotated]]),
    Total = ?ROTATE_WORKERS * ?ROTATE_PER_WORKER,
    Workers = spawn_sequences(
        fc_rotate_pool, post(?BODY_SIZE), ?ROTATE_WORKERS, ?ROTATE_PER_WORKER
    ),
    ?assertEqual(lists:duplicate(?ROTATE_WORKERS, {ok, ?ROTATE_PER_WORKER}), collect(Workers)),
    Rotations = count_events([nhttpc, connection, rotated]),
    ?assert(Rotations >= Total div ?ROTATE_THRESHOLD - 2),
    #{requests := Seen, connections := Accepted} = nhttpc_h2_flow_handler:read(Counters),
    ?assertEqual(Total, Seen),
    ?assert(Accepted >= Rotations),
    stop_pool(Sup),
    stop_nhttp(Server),
    ok.

%% The manager does not process the draining cast before the retiring
%% connection serves its last stream. Every request a worker sent to that
%% connection on the lock-free path completes or is sent again elsewhere.
stream_id_rotation_with_lagging_manager(Config) ->
    {Server, Port, Counters} = start_nhttp(fc_rotate_lag, Config, #{
        h2_settings => #{max_concurrent_streams => ?PEER_CAP}
    }),
    {Sup, Mgr} = start_pool(fc_rotate_lag_pool, Port, #{
        size => 1,
        max_overflow => 0,
        max_streams_per_connection => ?ROTATE_THRESHOLD,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    attach([[nhttpc, connection, rotated]]),
    ok = sys:suspend(Mgr),
    Workers = spawn_sequences(
        fc_rotate_lag_pool, post(?BODY_SIZE), ?ROTATE_WORKERS, ?ROTATE_PER_WORKER
    ),
    timer:sleep(?MANAGER_LAG_MS),
    ok = sys:resume(Mgr),
    ?assertEqual(lists:duplicate(?ROTATE_WORKERS, {ok, ?ROTATE_PER_WORKER}), collect(Workers)),
    ?assert(count_events([nhttpc, connection, rotated]) >= 1),
    #{requests := Seen} = nhttpc_h2_flow_handler:read(Counters),
    ?assertEqual(?ROTATE_WORKERS * ?ROTATE_PER_WORKER, Seen),
    stop_pool(Sup),
    stop_nhttp(Server),
    ok.

%% Ten thousand requests with a unique path and a unique header on one
%% connection. The HPACK encoder state, measured as its flat size in words,
%% is the same after request 1000 and after request 10000.
hpack_state_is_flat(Config) ->
    {Server, Port, _Counters} = start_nhttp(fc_hpack, Config, #{
        h2_settings => #{max_concurrent_streams => ?PEER_CAP}
    }),
    {Sup, Mgr} = start_pool(fc_hpack_pool, Port, #{
        size => 1,
        max_overflow => 0,
        timeouts => #{checkout => ?REQUEST_TIMEOUT_MS, request => ?REQUEST_TIMEOUT_MS}
    }),
    [ConnPid] = conn_pids(Mgr),
    Positions = hpack_positions(),
    Windows = #{
        ?HPACK_FIRST_SAMPLE => lists:seq(
            ?HPACK_FIRST_SAMPLE - ?HPACK_WINDOW + 1, ?HPACK_FIRST_SAMPLE
        ),
        ?HPACK_REQUESTS => lists:seq(?HPACK_REQUESTS - ?HPACK_WINDOW + 1, ?HPACK_REQUESTS)
    },
    Sampled = sets:from_list(lists:append(maps:values(Windows))),
    Sizes = hpack_run(fc_hpack_pool, ConnPid, Positions, Sampled, ?HPACK_REQUESTS, #{}),
    Max = fun(N) -> lists:max([maps:get(I, Sizes) || I <- maps:get(N, Windows)]) end,
    First = Max(?HPACK_FIRST_SAMPLE),
    Last = Max(?HPACK_REQUESTS),
    Tolerance = erts_debug:flat_size(unique_headers(?HPACK_REQUESTS)),
    ct:pal("hpack encoder flat size: ~p words at ~p, ~p words at ~p, tolerance ~p", [
        First, ?HPACK_FIRST_SAMPLE, Last, ?HPACK_REQUESTS, Tolerance
    ]),
    ?assert(abs(Last - First) =< Tolerance),
    stop_pool(Sup),
    stop_nhttp(Server),
    ok.

%%%-----------------------------------------------------------------------------
%%% CODEC QUEUE
%%%-----------------------------------------------------------------------------

%% One 1 MiB body and fifty 1 KiB requests against a peer that credits the
%% connection window on a trickle. The queued body spends every grant
%% inside the `recv/2` call that reports it, so the connection is
%% credit-starved for the whole drain and takes no other body. A
%% `size => 2` pool serves the fifty on a second connection while the
%% body drains. A `size => 1` pool holds them as waiters and serves every
%% one after the drain, inside the drain time plus one checkout timeout,
%% and none completes before the peer holds the whole body. A switch of
%% the peer to `eager` then releases the whole debt at once: the body
%% completes inside a second, where the trickle would take seconds. The
%% body needs `max_send_buffer => infinity`: at the default bound the
%% pool refuses it, which the wedge case covers.
fairness_under_contention(Config) ->
    ok = fairness_run(Config, fc_fair_two_pool, 2, ?FAIR_SLOW_INTERVAL_MS),
    ok = fairness_run(Config, fc_fair_one_pool, 1, ?TRICKLE_INTERVAL_MS).

-spec fairness_run(ct_suite:ct_config(), atom(), pos_integer(), pos_integer()) -> ok.
fairness_run(Config, Ref, Size, IntervalMs) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        credit => {trickle, ?TRICKLE_QUANTUM, IntervalMs}
    }),
    {Sup, Mgr} = start_pool(Ref, Port, #{
        size => Size,
        max_overflow => 0,
        max_send_buffer => infinity,
        max_pending_requests => 2 * ?FAIR_SMALL_REQUESTS,
        timeouts => #{checkout => ?FAIR_CHECKOUT_MS, request => ?LONG_TIMEOUT_MS}
    }),
    {ok, [First]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?WAIT_MS),
    attach([[nhttpc, pool, credit_starved]]),
    Big = spawn_requests(Ref, post(?FAIR_BIG_BODY), 1, ?LONG_TIMEOUT_MS),
    ok = wait_stat(First, data_octets, ?WINDOW, ?WAIT_MS),
    T0 = erlang:monotonic_time(millisecond),
    Small = spawn_completions(
        Ref, post(?FAIR_SMALL_BODY), ?FAIR_SMALL_REQUESTS, ?FAIR_CHECKOUT_MS, First
    ),
    Results = collect(Small),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    DrainMs = ?FAIR_BIG_BODY div ?TRICKLE_QUANTUM * IntervalMs,
    Kinds = kinds([Result || {Result, _Octets} <- Results]),
    ct:pal("fairness at size ~p: ~p after ~p ms, drain bound ~p ms", [
        Size, histogram(Kinds), Elapsed, DrainMs
    ]),
    ?assertEqual(lists:duplicate(?FAIR_SMALL_REQUESTS, {ok, 200}), Kinds),
    ?assert(count_events([nhttpc, pool, credit_starved]) >= 1),
    ok = fairness_assert(Size, Peer, First, Results, Elapsed, DrainMs),
    ok = nhttpc_h2_script_peer:command(First, {credit, eager}),
    T1 = erlang:monotonic_time(millisecond),
    ?assertEqual([{ok, 200}], kinds(collect(Big))),
    Released = erlang:monotonic_time(millisecond) - T1,
    ct:pal("fairness at size ~p: the body completed ~p ms after the switch to eager", [
        Size, Released
    ]),
    ?assert(Released =< ?FAIR_RELEASE_MS),
    ok = wait_stat(First, data_octets, ?FAIR_BIG_BODY, ?WAIT_MS),
    ?assertEqual(Size, total_created(Mgr)),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    detach_all().

-doc """
What each pool size pins. At two the fifty complete on the second
connection while the first still drains. At one they complete after the
drain, inside the bound the credit rate gives.
""".
-spec fairness_assert(
    pos_integer(), pid(), pid(), [{term(), non_neg_integer()}], non_neg_integer(), pos_integer()
) -> ok.
fairness_assert(2, Peer, First, _Results, _Elapsed, _DrainMs) ->
    ?assert(maps:get(data_octets, nhttpc_h2_script_peer:stats(First)) < ?FAIR_BIG_BODY),
    {ok, [First, Second]} = nhttpc_h2_script_peer:wait_connections(Peer, 2, ?WAIT_MS),
    ?assertEqual(?FAIR_SMALL_REQUESTS, maps:get(requests, nhttpc_h2_script_peer:stats(Second))),
    ?assertEqual(1, maps:get(requests, nhttpc_h2_script_peer:stats(First))),
    ok;
fairness_assert(1, _Peer, _First, Results, Elapsed, DrainMs) ->
    ?assertEqual([], [Octets || {_Result, Octets} <- Results, Octets < ?FAIR_BIG_BODY]),
    ?assert(Elapsed =< DrainMs + ?FAIR_CHECKOUT_MS),
    ok.

%% Two connections each hold a queued body against a peer that credits
%% nothing, on the connection or on any stream. Twenty large and twenty
%% small callers find no credit anywhere: each waits for it and answers
%% `{timeout, checkout}` at its deadline, none is admitted, the pool
%% creates no connection and restarts none, and the manager's reductions
%% stay under a bound a spinning manager passes by orders of magnitude
%% (read from a passing run and pinned at ten times). A grant that makes
%% one slot whole admits a body whose remainder passes the send buffer
%% bound: the codec refuses it before any frame leaves, and the
%% re-dispatch waits on the other connection until the deadline. Every
%% 20 ms `send_buffer_bytes/1` on both connections is at or under
%% `max_send_buffer`. Once the peer credits, both bodies drain and a
%% second round of twenty large bodies completes with at most one body
%% queued per connection.
send_buffer_full_does_not_wedge_the_pool(Config) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, #{
        settings => #{max_concurrent_streams => ?PEER_CAP},
        credit => never
    }),
    {Sup, Mgr} = start_pool(fc_wedge_pool, Port, #{
        size => 2,
        max_overflow => 0,
        max_pending_requests => 4 * ?WEDGE_CALLERS,
        timeouts => #{checkout => ?WEDGE_DEADLINE_MS, request => ?LONG_TIMEOUT_MS}
    }),
    attach([[nhttpc, connection, send_credit_refused]]),
    Queued = spawn_requests(fc_wedge_pool, post(?WEDGE_BODY), 2, ?LONG_TIMEOUT_MS),
    {ok, [ConnA, ConnB]} = nhttpc_h2_script_peer:wait_connections(Peer, 2, ?WAIT_MS),
    ok = wait_stat(ConnA, data_octets, ?WINDOW, ?WAIT_MS),
    ok = wait_stat(ConnB, data_octets, ?WINDOW, ?WAIT_MS),
    ConnPids = lists:sort(conn_pids(Mgr)),
    H2Pos = position(nhttpc_conn, conn_data, h2_conn),
    ?assertEqual(
        lists:duplicate(2, ?WEDGE_BODY - ?WINDOW),
        [nhttp_h2:send_buffer_bytes(h2_conn_of(Pid, H2Pos)) || Pid <- ConnPids]
    ),
    Sampler = start_sampler(ConnPids, H2Pos),
    {reductions, R0} = erlang:process_info(Mgr, reductions),
    T0 = erlang:monotonic_time(millisecond),
    Large = spawn_requests(fc_wedge_pool, post(?WEDGE_BODY), ?WEDGE_CALLERS, ?WEDGE_DEADLINE_MS),
    Small = spawn_requests(fc_wedge_pool, post(?BODY_SIZE), ?WEDGE_CALLERS, ?WEDGE_DEADLINE_MS),
    LargeKinds = kinds(collect(Large)),
    SmallKinds = kinds(collect(Small)),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    {reductions, R1} = erlang:process_info(Mgr, reductions),
    ct:pal("wedge: large ~p, small ~p, after ~p ms, manager reductions ~p", [
        histogram(LargeKinds), histogram(SmallKinds), Elapsed, R1 - R0
    ]),
    ?assertEqual(lists:duplicate(?WEDGE_CALLERS, {timeout, checkout}), LargeKinds),
    ?assertEqual(lists:duplicate(?WEDGE_CALLERS, {timeout, checkout}), SmallKinds),
    ?assert(Elapsed =< ?WEDGE_DEADLINE_MS + ?WEDGE_SLACK_MS),
    ?assert(R1 - R0 =< ?WEDGE_REDUCTION_BOUND),
    ?assertEqual(0, count_events([nhttpc, connection, send_credit_refused])),
    ?assertEqual(2, total_created(Mgr)),
    ?assertEqual(ConnPids, lists:sort(conn_pids(Mgr))),
    ?assertEqual(0, consecutive_failures(Mgr)),
    ?assertEqual(?WINDOW, maps:get(data_octets, nhttpc_h2_script_peer:stats(ConnA))),
    ?assertEqual(?WINDOW, maps:get(data_octets, nhttpc_h2_script_peer:stats(ConnB))),
    ok = nhttpc_h2_script_peer:command(ConnA, {window_update, ?WINDOW}),
    ok = barrier(ConnA),
    [Refused] = kinds(
        collect(spawn_requests(fc_wedge_pool, post(?WEDGE_BODY), 1, ?WEDGE_DEADLINE_MS))
    ),
    ?assertEqual({timeout, checkout}, Refused),
    ?assertEqual(1, count_events([nhttpc, connection, send_credit_refused])),
    ?assertEqual(?WINDOW, maps:get(data_octets, nhttpc_h2_script_peer:stats(ConnA))),
    ok = nhttpc_h2_script_peer:broadcast(Peer, {credit, eager}),
    ?assertEqual([{ok, 200}, {ok, 200}], kinds(collect(Queued))),
    Round = spawn_requests(fc_wedge_pool, post(?WEDGE_BODY), ?WEDGE_CALLERS, ?LONG_TIMEOUT_MS),
    ?assertEqual(lists:duplicate(?WEDGE_CALLERS, {ok, 200}), kinds(collect(Round))),
    #{max_buffer := MaxBuffer, max_queued := MaxQueued} = stop_sampler(Sampler),
    ct:pal("wedge: sampled send buffer at most ~p octets, ~p queued streams", [
        MaxBuffer, MaxQueued
    ]),
    ?assert(MaxBuffer =< ?MAX_SEND_BUFFER),
    ?assert(MaxQueued =< 1),
    Octets = lists:sum([
        maps:get(data_octets, nhttpc_h2_script_peer:stats(Conn))
     || Conn <- [ConnA, ConnB]
    ]),
    ?assertEqual((2 + ?WEDGE_CALLERS) * ?WEDGE_BODY, Octets),
    ?assertEqual(2, total_created(Mgr)),
    stop_pool(Sup),
    nhttpc_h2_script_peer:stop(Peer),
    ok.

%%%-----------------------------------------------------------------------------
%%% SERVERS AND POOLS
%%%-----------------------------------------------------------------------------

-spec start_nhttp(atom(), [tuple()], map()) ->
    {pid(), inet:port_number(), nhttpc_h2_flow_handler:counters()}.
start_nhttp(Name, Config, Extra) ->
    Counters = nhttpc_h2_flow_handler:new_counters(),
    Opts = maps:merge(
        #{
            port => 0,
            handler => nhttpc_h2_flow_handler,
            handler_args => Counters,
            versions => [http2],
            tls => #{
                certfile => proplists:get_value(certfile, Config),
                keyfile => proplists:get_value(keyfile, Config)
            }
        },
        Extra
    ),
    {ok, Pid} = nhttp:start_link(Name, Opts),
    unlink(Pid),
    {ok, Port} = nhttp:get_port(Pid),
    {Pid, Port, Counters}.

-spec stop_nhttp(pid()) -> ok.
stop_nhttp(Pid) ->
    ok = nhttp:stop(Pid),
    ok.

-spec start_pool(atom(), inet:port_number(), map()) -> {pid(), pid()}.
start_pool(Ref, Port, Extra) ->
    Opts = maps:merge(
        #{
            host => <<"127.0.0.1">>,
            port => Port,
            tls => #{verify => verify_none},
            versions => [http2],
            otel => #{events => true, metrics => false, traces => false}
        },
        Extra
    ),
    {ok, Sup} = nhttpc_pool_sup:start_link(Ref, Opts),
    unlink(Sup),
    Mgr = nhttpc_pool:manager(Ref),
    ok = nhttpc_pool:wait_ready(Mgr, ?WAIT_MS),
    {Sup, Mgr}.

-spec stop_pool(pid()) -> ok.
stop_pool(Sup) ->
    exit(Sup, shutdown),
    timer:sleep(?STOP_SETTLE_MS),
    ok.

%%%-----------------------------------------------------------------------------
%%% REQUESTS
%%%-----------------------------------------------------------------------------

-spec post(non_neg_integer()) -> nhttp_lib:request().
post(Size) ->
    #{
        method => post,
        path => <<"/post">>,
        headers => [{<<"content-type">>, <<"application/octet-stream">>}],
        body => binary:copy(<<$x>>, Size)
    }.

-spec get_request(binary()) -> nhttp_lib:request().
get_request(Path) ->
    #{method => get, path => Path, headers => [], body => <<>>}.

-spec unique_headers(pos_integer()) -> nhttp_lib:headers().
unique_headers(N) ->
    [{<<"x-req-id">>, fixed_id(N)}, {<<"accept">>, <<"application/octet-stream">>}].

-doc "A zero-padded identifier, so that every HPACK table entry has the same size.".
-spec fixed_id(pos_integer()) -> binary().
fixed_id(N) ->
    iolist_to_binary(io_lib:format("~8..0B", [N])).

-spec spawn_requests(atom(), nhttp_lib:request(), pos_integer()) -> [{pid(), reference()}].
spawn_requests(Ref, Req, Count) ->
    spawn_requests(Ref, Req, Count, ?REQUEST_TIMEOUT_MS).

-spec spawn_requests(atom(), nhttp_lib:request(), pos_integer(), timeout()) ->
    [{pid(), reference()}].
spawn_requests(Ref, Req, Count, Timeout) ->
    [
        spawn_monitor(fun() -> exit({result, nhttpc_pool:request(Ref, Req, Timeout)}) end)
     || _ <- lists:seq(1, Count)
    ].

-doc "Workers that each run `PerWorker` requests in sequence and answer their success count.".
-spec spawn_sequences(atom(), nhttp_lib:request(), pos_integer(), pos_integer()) ->
    [{pid(), reference()}].
spawn_sequences(Ref, Req, Workers, PerWorker) ->
    [
        spawn_monitor(fun() -> exit({result, run_sequence(Ref, Req, PerWorker, 0)}) end)
     || _ <- lists:seq(1, Workers)
    ].

-spec run_sequence(atom(), nhttp_lib:request(), non_neg_integer(), non_neg_integer()) ->
    {ok, non_neg_integer()} | {error, term()}.
run_sequence(_Ref, _Req, 0, Ok) ->
    {ok, Ok};
run_sequence(Ref, Req, Left, Ok) ->
    case nhttpc_pool:request(Ref, Req, ?REQUEST_TIMEOUT_MS) of
        {ok, #{status := 200}} -> run_sequence(Ref, Req, Left - 1, Ok + 1);
        {ok, #{status := Status}} -> {error, {status, Status, Ok}};
        {error, Reason} -> {error, {kind(Reason), Ok}}
    end.

-doc "Streaming downloads through the pool, each answering the octets it consumed.".
-spec spawn_streams(inet:port_number(), pos_integer()) -> [{pid(), reference()}].
spawn_streams(Port, Count) ->
    Req = #{
        method => get,
        scheme => https,
        authority => <<"127.0.0.1:", (integer_to_binary(Port))/binary>>,
        path => <<"/streamed">>,
        headers => []
    },
    Opts = #{tls => #{verify => verify_none}, versions => [http2]},
    [
        spawn_monitor(fun() -> exit({result, stream_download(Req, Opts)}) end)
     || _ <- lists:seq(1, Count)
    ].

-spec stream_download(map(), map()) -> {ok, non_neg_integer()} | {error, term()}.
stream_download(Req, Opts) ->
    case nhttpc:stream(Req, Opts) of
        {ok, Handle} -> stream_loop(Handle, 0);
        {error, Reason} -> {error, Reason}
    end.

-spec stream_loop(nhttpc:handle(), non_neg_integer()) -> {ok, non_neg_integer()} | {error, term()}.
stream_loop(Handle, Total) ->
    receive
        {nhttpc, Handle, {headers, _Status, _Headers}} ->
            stream_loop(Handle, Total);
        {nhttpc, Handle, {data, Chunk, more}} ->
            ok = nhttpc:consume(Handle, byte_size(Chunk)),
            stream_loop(Handle, Total + byte_size(Chunk));
        {nhttpc, Handle, {data, Chunk, fin}} ->
            {ok, Total + byte_size(Chunk)};
        {nhttpc, Handle, {done, normal}} ->
            {ok, Total};
        {nhttpc, Handle, {pause, _Buffered}} ->
            stream_loop(Handle, Total);
        {nhttpc, Handle, {resume, _Buffered}} ->
            stream_loop(Handle, Total);
        {nhttpc, Handle, {trailers, _Trailers}} ->
            stream_loop(Handle, Total);
        {nhttpc, Handle, {error, Reason}} ->
            {error, Reason}
    after ?REQUEST_TIMEOUT_MS ->
        {error, {stream_timeout, Total}}
    end.

-spec collect([{pid(), reference()}]) -> [term()].
collect(Workers) ->
    [
        receive
            {'DOWN', MRef, process, Pid, {result, Result}} -> Result;
            {'DOWN', MRef, process, Pid, Other} -> ct:fail("worker died with ~p", [Other])
        after ?COLLECT_TIMEOUT_MS -> ct:fail("worker never finished")
        end
     || {Pid, MRef} <- Workers
    ].

-spec kinds([term()]) -> [term()].
kinds(Results) ->
    [result_kind(R) || R <- Results].

-spec result_kind({ok, nhttp_lib:response()} | {error, term()}) -> term().
result_kind({ok, #{status := Status}}) -> {ok, Status};
result_kind({error, Reason}) -> kind(Reason).

-spec kind(term()) -> nhttpc:error().
kind(Reason) ->
    {error, Kind} = nhttpc_error:wrap(Reason),
    Kind.

-spec ceil_div(pos_integer(), pos_integer()) -> pos_integer().
ceil_div(N, D) ->
    (N + D - 1) div D.

%%%-----------------------------------------------------------------------------
%%% TELEMETRY
%%%-----------------------------------------------------------------------------

-spec attach([telemetry:event_name()]) -> ok.
attach(Events) ->
    Self = self(),
    lists:foreach(
        fun(Event) ->
            ok = telemetry:attach(
                {?MODULE, Event},
                Event,
                fun(Name, Measurements, Meta, _Config) ->
                    Self ! {telemetry, Name, Measurements, Meta}
                end,
                undefined
            )
        end,
        Events
    ).

-spec detach_all() -> ok.
detach_all() ->
    lists:foreach(
        fun
            (#{id := {?MODULE, _} = Id}) -> telemetry:detach(Id);
            (_Other) -> ok
        end,
        telemetry:list_handlers([])
    ).

-spec count_events(telemetry:event_name()) -> non_neg_integer().
count_events(Event) ->
    count_events(Event, 0).

-spec count_events(telemetry:event_name(), non_neg_integer()) -> non_neg_integer().
count_events(Event, Acc) ->
    receive
        {telemetry, Event, _Measurements, _Meta} -> count_events(Event, Acc + 1)
    after 0 -> Acc
    end.

-spec wait_event(telemetry:event_name(), timeout()) -> {ok, map()} | timeout.
wait_event(Event, Timeout) ->
    receive
        {telemetry, Event, _Measurements, Meta} -> {ok, Meta}
    after Timeout -> timeout
    end.

%%%-----------------------------------------------------------------------------
%%% CODEC QUEUE PROBES
%%%-----------------------------------------------------------------------------

-doc "Callers that answer their result with the peer's request octets at that moment.".
-spec spawn_completions(atom(), nhttp_lib:request(), pos_integer(), timeout(), pid()) ->
    [{pid(), reference()}].
spawn_completions(Ref, Req, Count, Timeout, Conn) ->
    [
        spawn_monitor(fun() ->
            Result = nhttpc_pool:request(Ref, Req, Timeout),
            exit({result, {Result, maps:get(data_octets, nhttpc_h2_script_peer:stats(Conn))}})
        end)
     || _ <- lists:seq(1, Count)
    ].

-spec consecutive_failures(pid()) -> non_neg_integer().
consecutive_failures(Mgr) ->
    field(nhttpc_pool, state, consecutive_failures, sys:get_state(Mgr)).

-doc "The codec state of a pooled connection, at the `h2_conn` position of `#conn_data{}`.".
-spec h2_conn_of(pid(), pos_integer()) -> nhttp_h2:conn().
h2_conn_of(ConnPid, H2Pos) ->
    {_StateName, Data} = sys:get_state(ConnPid),
    element(H2Pos, Data).

-doc """
A process that reads the send queue of every connection every
`WEDGE_SAMPLE_MS` and keeps the largest occupancy and the largest
queued stream count it saw.
""".
-spec start_sampler([pid()], pos_integer()) -> pid().
start_sampler(ConnPids, H2Pos) ->
    Owner = self(),
    spawn_link(fun() -> sampler_loop(ConnPids, H2Pos, Owner, 0, 0) end).

-spec stop_sampler(pid()) -> #{max_buffer := non_neg_integer(), max_queued := non_neg_integer()}.
stop_sampler(Sampler) ->
    Sampler ! {stop, self()},
    receive
        {sampler, Sampler, Result} -> Result
    after ?WAIT_MS -> error(sampler_timeout)
    end.

-spec sampler_loop([pid()], pos_integer(), pid(), non_neg_integer(), non_neg_integer()) -> ok.
sampler_loop(ConnPids, H2Pos, Owner, MaxBuffer, MaxQueued) ->
    receive
        {stop, Owner} ->
            Owner ! {sampler, self(), #{max_buffer => MaxBuffer, max_queued => MaxQueued}},
            ok
    after ?WEDGE_SAMPLE_MS ->
        H2Conns = [h2_conn_of(Pid, H2Pos) || Pid <- ConnPids],
        sampler_loop(
            ConnPids,
            H2Pos,
            Owner,
            lists:max([MaxBuffer | [nhttp_h2:send_buffer_bytes(C) || C <- H2Conns]]),
            lists:max([MaxQueued | [nhttp_h2:queued_streams(C) || C <- H2Conns]])
        )
    end.

%%%-----------------------------------------------------------------------------
%%% PEER AND POOL STATE
%%%-----------------------------------------------------------------------------

-doc """
A PING round trip through the client. The client's codec answers a PING
in frame order, so the ACK proves that every frame sent before the PING
was processed. The codec reports only the first SETTINGS ACK, so a
SETTINGS change cannot be waited on directly.
""".
-spec barrier(pid()) -> ok | {timeout, term()}.
barrier(Conn) ->
    Acks = maps:get(ping_acks, nhttpc_h2_script_peer:stats(Conn)),
    ok = nhttpc_h2_script_peer:command(Conn, {ping, crypto:strong_rand_bytes(8)}),
    wait_stat(Conn, ping_acks, Acks + 1, ?WAIT_MS).

-spec histogram([term()]) -> [{term(), pos_integer()}].
histogram(Kinds) ->
    maps:to_list(
        lists:foldl(fun(K, Acc) -> maps:update_with(K, fun(N) -> N + 1 end, 1, Acc) end, #{}, Kinds)
    ).

-doc "The streams the client credited beyond what the peer sent on them.".
-spec over_credit(nhttpc_h2_script_peer:stats()) ->
    [{nhttp_lib:stream_id(), non_neg_integer(), non_neg_integer()}].
over_credit(#{client_credit_streams := Credits, sent_octets_streams := Sents}) ->
    [
        {Id, Credit, maps:get(Id, Sents, 0)}
     || {Id, Credit} <- maps:to_list(Credits), Credit > maps:get(Id, Sents, 0)
    ].

-doc "The credit a connection returned: the connection total and the per-stream totals.".
-spec credit_pattern(nhttpc_h2_script_peer:stats()) ->
    {non_neg_integer(), #{nhttp_lib:stream_id() => non_neg_integer()}}.
credit_pattern(#{client_credit_conn := Conn, client_credit_streams := Streams}) ->
    {Conn, Streams}.

-spec wait_stat(pid(), atom(), non_neg_integer(), integer()) -> ok | {timeout, term()}.
wait_stat(Conn, Key, Want, Remaining) ->
    case maps:get(Key, nhttpc_h2_script_peer:stats(Conn)) of
        Value when Value >= Want ->
            ok;
        Value when Remaining =< 0 ->
            {timeout, {Key, Value, Want}};
        _Value ->
            timer:sleep(?POLL_MS),
            wait_stat(Conn, Key, Want, Remaining - ?POLL_MS)
    end.

-spec conn_pids(pid()) -> [pid()].
conn_pids(Mgr) ->
    maps:keys(field(nhttpc_pool, state, connections, sys:get_state(Mgr))).

-spec total_created(pid()) -> non_neg_integer().
total_created(Mgr) ->
    field(nhttpc_pool, state, total_created, sys:get_state(Mgr)).

-doc """
The `#conn_data{}` and `#h2_conn{}` positions the encoder probe reads,
resolved by name from the abstract code, so a renamed field fails here
with its name and not with a wrong number.
""".
-spec hpack_positions() -> {pos_integer(), pos_integer()}.
hpack_positions() ->
    {position(nhttpc_conn, conn_data, h2_conn), position(nhttp_h2, h2_conn, hpack_enc)}.

-spec hpack_size(pid(), {pos_integer(), pos_integer()}) -> non_neg_integer().
hpack_size(ConnPid, {H2Pos, EncPos}) ->
    {_StateName, ConnData} = sys:get_state(ConnPid),
    erts_debug:flat_size(element(EncPos, element(H2Pos, ConnData))).

-spec hpack_run(
    atom(), pid(), {pos_integer(), pos_integer()}, sets:set(pos_integer()), pos_integer(), map()
) -> #{pos_integer() => non_neg_integer()}.
hpack_run(Ref, ConnPid, Positions, Sampled, Total, Sizes) ->
    hpack_run(Ref, ConnPid, Positions, Sampled, Total, 1, Sizes).

-spec hpack_run(
    atom(),
    pid(),
    {pos_integer(), pos_integer()},
    sets:set(pos_integer()),
    pos_integer(),
    pos_integer(),
    map()
) -> #{pos_integer() => non_neg_integer()}.
hpack_run(_Ref, _ConnPid, _Positions, _Sampled, Total, N, Sizes) when N > Total ->
    Sizes;
hpack_run(Ref, ConnPid, Positions, Sampled, Total, N, Sizes) ->
    Req = #{
        method => get,
        path => <<"/p/", (fixed_id(N))/binary>>,
        headers => unique_headers(N),
        body => <<>>
    },
    {ok, #{status := 200}} = nhttpc_pool:request(Ref, Req, ?REQUEST_TIMEOUT_MS),
    Sizes1 =
        case sets:is_element(N, Sampled) of
            true -> Sizes#{N => hpack_size(ConnPid, Positions)};
            false -> Sizes
        end,
    hpack_run(Ref, ConnPid, Positions, Sampled, Total, N + 1, Sizes1).

-spec field(module(), atom(), atom(), tuple()) -> term().
field(Module, Record, Field, Tuple) ->
    element(position(Module, Record, Field), Tuple).

-spec position(module(), atom(), atom()) -> pos_integer().
position(Module, Record, Field) ->
    {Module, Beam, _File} = code:get_object_code(Module),
    {ok, {Module, [{abstract_code, {_Version, Forms}}]}} = beam_lib:chunks(Beam, [abstract_code]),
    [Fields] = [Fs || {attribute, _Anno, record, {Name, Fs}} <- Forms, Name =:= Record],
    Names = [field_name(F) || F <- Fields],
    case lists:member(Field, Names) of
        true -> length(lists:takewhile(fun(N) -> N =/= Field end, Names)) + 2;
        false -> error({unknown_field, Module, Record, Field})
    end.

-spec field_name(term()) -> atom().
field_name({typed_record_field, Field, _Type}) -> field_name(Field);
field_name({record_field, _Anno, {atom, _, Name}}) -> Name;
field_name({record_field, _Anno, {atom, _, Name}, _Default}) -> Name.
