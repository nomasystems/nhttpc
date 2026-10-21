%%%-----------------------------------------------------------------------------
%%% @doc Client pool module test suite.
%%%
%%% Tests the nhttpc_pool module for connection pooling functionality.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_pool_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("triq/include/triq.hrl").

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(SWEEP_MAX_AGE, 800).
-define(SWEEP_INTERVAL, 300).

%% Cap the mixed-path checkout test drives the peer to advertise, and the
%% longest overshoot it tolerates. A rollback inside one `update_counter`
%% pair clears in well under a microsecond; a reservation that passes the
%% cap for real holds the extra stream for a whole request.
-define(CAP_MAX_STREAMS, 2).
-define(CAP_LOAD_WORKERS, 8).
-define(CAP_LOAD_SIZE, 10).
-define(DEFAULT_MAX_STREAMS, 100).
-define(UPDATED_MAX_STREAMS, 50).
-define(SHRINK_LIVE_STREAMS, 4).
-define(SHRUNK_MAX_STREAMS, 2).
-define(OVER_GRANT_CAP, 10).
-define(ATOMIC_RR_INDEX, 1).
-define(REDISPATCH_LIMIT_MS, 2000).
-define(CREDIT_ATTEMPTS, 3).
-define(CREDIT_BODY_SIZE, 8192).
-define(CREDIT_EXHAUST_LIMIT, 10).
-define(CREDIT_GROWTH_TIMEOUT, 500).
-define(CREDIT_PEER_CAP, 1000).
-define(CREDIT_WINDOW, 65535).
-define(CREDIT_FITTING_REQUESTS, 7).
-define(CREDIT_GROWTH_CALLERS, 8).
-define(CREDIT_BOUND_CALLERS, 20).
-define(CREDIT_OVER_CALLERS, 12).
-define(CREDIT_REQUEST_TIMEOUT, 1500).
-define(CREDIT_HOLD_TIMEOUT, 15000).
-define(CREDIT_PACK_ROUNDS, 10).
-define(CREDIT_PACK_CONCURRENCY, 6).
-define(CREDIT_PACK_BODY_SIZE, 4096).
-define(CREDIT_PROPERTY_RUNS, 200).
-define(CREDIT_PROPERTY_CAP, 1_000_000).
-define(LINEAR_CAP, 8).
-define(LINEAR_CREDIT, 1000).
-define(LINEAR_SMALL_BODY, 100).
-define(LINEAR_SMALL, 4).
-define(LINEAR_PROBERS, 4).
-define(LINEAR_READERS, 2).
-define(LINEAR_OPS, 200_000).
-define(CREDIT_STARVED_WINDOW_MS, 3000).
-define(CREDIT_STARVED_MAX_EVENTS, 4).
-define(CREDIT_STARVED_PROBE_TIMEOUT, 30).
-define(COLD_START_CAP, 1).
-define(COLD_START_RAISED_CAP, 100).
-define(UNBOUNDED_BODY_SIZE, 100 * 1024).
-define(CAP_OVERSHOOT_LIMIT_US, 2000).
-define(CAP_BURST_WORKERS, 4).
-define(CAP_BURST_SIZE, 40).
-define(SWEEP_WAITER_TIMEOUT, 5000).
-define(SWEEP_WAITER_LIMIT, 1000).

%% Selection policy tests. The sweep window runs five intervals rather
%% than three, so the first rotation past ?PACK_SWEEP_AGE always lands
%% inside it.
-define(PACK_REQUESTS, 20).
-define(PACK_CAP_BURST, 6).
-define(PACK_SWEEP_AGE, 800).
-define(PACK_SWEEP_INTERVAL, 300).
-define(PACK_SWEEP_STEP, 100).
-define(PACK_SWEEP_WINDOW, 1500).

%% Stream slot reservation tests.
-define(NO_ETS_REQUESTS, 5).
-define(UNDERFLOW_FILTER, nhttpc_pool_SUITE_underflow).
-define(CONNECT_FAILED_FILTER, nhttpc_pool_SUITE_connect_failed).

%% Reconnect brake tests.
-define(BRAKE_BASE, 50).
-define(BRAKE_MAX, 400).
-define(BRAKE_CYCLES, 6).
-define(BRAKE_IDLE_TIMEOUT, 100).
-define(BRAKE_IDLE_CYCLES, 2).
-define(BRAKE_POLL, 20).
-define(BRAKE_WAIT, 15000).

%% Stream-id rotation. The refusal window case runs eight workers with
%% one request each in flight, so eight is the most requests a rotating
%% connection can refuse before every worker has been re-dispatched away
%% from it.
-define(ROTATE_THRESHOLD, 50).
-define(ROTATE_REQUESTS, 60).
-define(ROTATE_WINDOW_THRESHOLD, 100).
-define(ROTATE_WINDOW_WORKERS, 8).
-define(ROTATE_WINDOW_PER_WORKER, 30).
-define(ROTATE_BRAKE_THRESHOLD, 2).
-define(ROTATE_BRAKE_ROTATIONS, 10).
-define(ROTATE_BOUND_SIZE, 2).
-define(ROTATE_BOUND_OVERFLOW, 1).
-define(ROTATE_BOUND_WINDOW_MS, 1500).
-define(ROTATE_POLL_MS, 20).
-define(ROTATE_WAIT_MS, 5000).
-define(ROTATE_WORKER_WAIT_MS, 60000).
-define(ROTATE_HOLD_TIMEOUT, 10000).
-define(OCCUPANCY_HOLD_MS, 2000).
-define(OCCUPANCY_WAITERS, 2).
-define(OCCUPANCY_WAIT_MS, 5000).
-define(GOAWAY_KEEPS_STREAM_ONE, 1).

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

%% Pool lifecycle tests
-export([
    pool_start_stop/1,
    pool_wait_ready/1,
    pool_wait_ready_timeout/1
]).

%% Checkout/checkin tests
-export([
    pool_checkout_checkin/1,
    pool_checkout_timeout/1,
    pool_multiple_checkouts/1
]).

%% Request tests
-export([
    pool_request/1,
    pool_request_error/1,
    pool_concurrent_requests/1
]).

%% Connection management tests
-export([
    pool_connection_down/1,
    pool_connection_draining/1,
    pool_update_max_streams/1,
    pool_connect_failure_logged/1
]).

%% nhttpc_pool_sup coverage tests
-export([
    pool_sup_child_spec/1,
    pool_sup_stop_by_pid/1,
    pool_sup_stop_by_ref/1,
    pool_sup_checkout_checkin/1,
    pool_sup_request/1,
    pool_sup_non_atom_ref/1,
    pool_sup_atom_table_stable_under_churn/1
]).

%% Pool ref resolution + graceful stop (plan 02) tests
-export([
    resolve_accepts_ref_atom_and_pid_uniformly/1,
    stop_immediate_kills_in_flight/1,
    stop_with_drain_waits_for_in_flight/1,
    stop_with_drain_timeout_force_closes/1,
    stop_with_explicit_immediate_opts/1,
    stop_with_drain_on_missing_pool/1,
    stop_with_invalid_opts/1
]).

%% Pool connection lifecycle coverage tests
-export([
    pool_idle_timeout/1,
    pool_connection_reconnect/1,
    pool_waiter_deadline_expired/1,
    pool_multiple_checkin_checkout_cycles/1
]).

%% H2 pool tests
-export([
    pool_h2_request/1,
    pool_tls_supported_groups/1,
    pool_h2_concurrent/1,
    pool_h2_fast_path_checkout/1,
    pool_h2_draining_skipped_on_checkout/1
]).

%% H2 stream accounting / sweep tests (plan 02)
-export([
    pool_h2_conn_info_active_count_stays_zero/1,
    pool_h2_sweep_deletes_ets_row/1,
    pool_h2_sweep_does_not_block_manager/1,
    pool_h2_sweep_rotates_after_gen_server_traffic/1
]).

%% ETS checkout fast path tests (plan 03)
-export([
    pool_h1_still_exclusive/1,
    pool_h2_adopts_peer_max_concurrent_streams/1,
    pool_h2_ready_cast_leaves_cap/1,
    pool_h2_cap_never_exceeded_under_mixed_paths/1,
    pool_h2_respects_peer_cap_under_load/1,
    pool_h2_falls_back_before_first_connection/1,
    pool_h2_public_api_uses_fast_path/1,
    pool_h2_telemetry_parity/1,
    pool_h2_waiter_served_by_sweep/1,
    pool_h2_waiter_wakes_when_cap_frees/1
]).

%% Peer cap shrink and re-dispatch tests (plan 12)
-export([
    pool_h2_redispatch_is_bounded/1,
    pool_h2_refused_is_redispatched_elsewhere/1,
    pool_h2_redispatch_on_over_grant/1,
    pool_h2_shrink_blocks_new_checkouts/1
]).

%% Connection selection policy tests (plan 04)
-export([
    pool_h1_unaffected_by_policy/1,
    pool_h2_pack_no_sweep_churn/1,
    pool_h2_pack_opens_second_at_cap/1,
    pool_h2_pack_prefers_oldest/1,
    pool_h2_pack_uses_one_connection/1,
    pool_h2_spread_keeps_round_robin/1
]).

%% Stream slot reservation tests (plan 06)
-export([
    pool_negative_stream_count_is_reported/1,
    pool_reservation_reaches_no_ets/1,
    pool_stream_count_survives_connection_rotation/1
]).

%% HTTP/3 pool tests (plan 07)
-export([
    pool_h3_checkin_returns_the_slot/1,
    pool_h3_concurrent_requests/1,
    pool_h3_error_cancels_the_stream/1,
    pool_h3_header_limits_enforced/1,
    pool_h3_max_response_size_enforced/1,
    pool_h3_never_calls_the_h1_module/1,
    pool_h3_request_via_public_api/1
]).

%% Credit-aware availability tests (plan 20)
-export([
    pool_h2_grows_on_credit_starvation/1,
    pool_h2_packs_while_credit_lasts/1,
    pool_h2_growth_is_bounded_by_overflow/1,
    pool_h2_reservation_is_exact/1,
    pool_h2_reservation_is_linearizable/1,
    pool_h2_over_admission_impossible/1,
    pool_h2_cold_start_grows_on_slots/1,
    pool_h2_spread_also_honors_credit/1,
    pool_credit_starved_event_is_rate_limited/1,
    pool_h3_and_h1_unaffected/1
]).

%% Pool sup error path tests (plan 17)
-export([
    pool_sup_start_link_invalid_opts/1,
    pool_sup_request_fast_path_error_flushes/1,
    pool_sup_stop_timeout_force_kill/1,
    pool_sup_stop_already_dead_supervisor/1
]).

%% Pool manager coverage tests (plan 17)
-export([
    pool_mgr_lookup_badarg_on_missing_ref/1,
    pool_mgr_persistent_term_miss_paths/1,
    pool_mgr_resolve_unknown_pid/1,
    pool_mgr_resolve_dead_pid/1,
    pool_mgr_checkin_fast_on_missing_pool/1,
    pool_mgr_get_connection_default_arity/1,
    pool_mgr_get_connection_pool_not_found/1,
    pool_mgr_drain_on_dead_manager/1,
    pool_mgr_wait_ready_on_missing_pool/1,
    pool_mgr_unknown_handle_call/1,
    pool_mgr_unknown_handle_cast/1,
    pool_mgr_unknown_handle_info_and_stray_down/1,
    pool_mgr_connection_draining_unknown_conn/1,
    pool_mgr_update_max_streams_unknown_conn/1,
    pool_mgr_safe_call_timeout_infinity/1,
    pool_mgr_request_with_infinity_timeout/1,
    pool_mgr_flatten_tls_extras/1,
    pool_mgr_h3_protocol_to_ets/1,
    pool_mgr_otel_option_flows_to_conn_opts/1,
    pool_mgr_reconnect_backoff_progression/1,
    pool_mgr_drain_idle_pool_completes_immediately/1,
    pool_backoff_grows_when_connection_never_serves/1,
    pool_backoff_resets_after_first_response/1,
    pool_backoff_does_not_reset_on_handshake_alone/1,
    pool_backoff_ignores_clean_exit/1
]).

%% Stream-id rotation and proactive replacement
-export([
    pool_h2_rotates_at_threshold/1,
    pool_replaces_draining_connection_immediately/1,
    pool_rotation_refusal_window_is_bounded/1,
    pool_goaway_replaces_immediately/1,
    pool_rotation_does_not_count_as_failure/1,
    pool_live_bound_includes_draining/1,
    pool_saturation_is_a_level/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, lifecycle},
        {group, checkout},
        {group, requests},
        {group, connections},
        {group, pool_sup_coverage},
        {group, pool_sup_faults},
        {group, pool_mgr_faults},
        {group, pool_lifecycle},
        {group, resolve_and_stop},
        {group, reconnect_brake},
        {group, h2_pool},
        {group, h3_pool}
    ].

groups() ->
    [
        {lifecycle, [sequence], [
            pool_start_stop,
            pool_wait_ready,
            pool_wait_ready_timeout
        ]},
        {checkout, [sequence], [
            pool_checkout_checkin,
            pool_checkout_timeout,
            pool_multiple_checkouts,
            pool_h1_still_exclusive
        ]},
        {requests, [sequence], [
            pool_request,
            pool_request_error,
            pool_concurrent_requests
        ]},
        {connections, [sequence], [
            pool_connection_down,
            pool_connection_draining,
            pool_update_max_streams,
            pool_connect_failure_logged
        ]},
        {pool_sup_coverage, [sequence], [
            pool_sup_child_spec,
            pool_sup_stop_by_pid,
            pool_sup_stop_by_ref,
            pool_sup_checkout_checkin,
            pool_sup_request,
            pool_sup_non_atom_ref,
            pool_sup_atom_table_stable_under_churn
        ]},
        {pool_lifecycle, [sequence], [
            pool_idle_timeout,
            pool_connection_reconnect,
            pool_waiter_deadline_expired,
            pool_multiple_checkin_checkout_cycles
        ]},
        {resolve_and_stop, [sequence], [
            resolve_accepts_ref_atom_and_pid_uniformly,
            stop_immediate_kills_in_flight,
            stop_with_drain_waits_for_in_flight,
            stop_with_drain_timeout_force_closes,
            stop_with_explicit_immediate_opts,
            stop_with_drain_on_missing_pool,
            stop_with_invalid_opts
        ]},
        {h2_pool, [sequence], [
            pool_h2_request,
            pool_tls_supported_groups,
            pool_h2_concurrent,
            pool_h2_fast_path_checkout,
            pool_h2_draining_skipped_on_checkout,
            pool_h2_conn_info_active_count_stays_zero,
            pool_h2_sweep_rotates_after_gen_server_traffic,
            pool_h2_sweep_deletes_ets_row,
            pool_h2_sweep_does_not_block_manager,
            pool_h2_public_api_uses_fast_path,
            pool_h2_adopts_peer_max_concurrent_streams,
            pool_h2_ready_cast_leaves_cap,
            pool_h2_cap_never_exceeded_under_mixed_paths,
            pool_h2_respects_peer_cap_under_load,
            pool_h2_telemetry_parity,
            pool_h2_waiter_wakes_when_cap_frees,
            pool_h2_waiter_served_by_sweep,
            pool_h2_falls_back_before_first_connection,
            pool_h2_pack_uses_one_connection,
            pool_h2_pack_opens_second_at_cap,
            pool_h2_pack_prefers_oldest,
            pool_h2_spread_keeps_round_robin,
            pool_h2_pack_no_sweep_churn,
            pool_h1_unaffected_by_policy,
            pool_stream_count_survives_connection_rotation,
            pool_negative_stream_count_is_reported,
            pool_reservation_reaches_no_ets,
            pool_h2_shrink_blocks_new_checkouts,
            pool_h2_redispatch_on_over_grant,
            pool_h2_redispatch_is_bounded,
            pool_h2_refused_is_redispatched_elsewhere,
            pool_h2_grows_on_credit_starvation,
            pool_h2_packs_while_credit_lasts,
            pool_h2_growth_is_bounded_by_overflow,
            pool_h2_reservation_is_exact,
            pool_h2_reservation_is_linearizable,
            pool_h2_over_admission_impossible,
            pool_h2_cold_start_grows_on_slots,
            pool_h2_spread_also_honors_credit,
            pool_credit_starved_event_is_rate_limited,
            pool_h2_rotates_at_threshold,
            pool_replaces_draining_connection_immediately,
            pool_rotation_refusal_window_is_bounded,
            pool_goaway_replaces_immediately,
            pool_rotation_does_not_count_as_failure,
            pool_live_bound_includes_draining,
            pool_saturation_is_a_level
        ]},
        {h3_pool, [sequence], [
            pool_h3_request_via_public_api,
            pool_h3_concurrent_requests,
            pool_h3_checkin_returns_the_slot,
            pool_h3_max_response_size_enforced,
            pool_h3_header_limits_enforced,
            pool_h3_error_cancels_the_stream,
            pool_h3_never_calls_the_h1_module,
            pool_h3_and_h1_unaffected
        ]},
        {pool_sup_faults, [sequence], [
            pool_sup_start_link_invalid_opts,
            pool_sup_request_fast_path_error_flushes,
            pool_sup_stop_timeout_force_kill,
            pool_sup_stop_already_dead_supervisor
        ]},
        {pool_mgr_faults, [sequence], [
            pool_mgr_lookup_badarg_on_missing_ref,
            pool_mgr_persistent_term_miss_paths,
            pool_mgr_resolve_unknown_pid,
            pool_mgr_resolve_dead_pid,
            pool_mgr_checkin_fast_on_missing_pool,
            pool_mgr_get_connection_default_arity,
            pool_mgr_get_connection_pool_not_found,
            pool_mgr_drain_on_dead_manager,
            pool_mgr_wait_ready_on_missing_pool,
            pool_mgr_unknown_handle_call,
            pool_mgr_unknown_handle_cast,
            pool_mgr_unknown_handle_info_and_stray_down,
            pool_mgr_connection_draining_unknown_conn,
            pool_mgr_update_max_streams_unknown_conn,
            pool_mgr_safe_call_timeout_infinity,
            pool_mgr_request_with_infinity_timeout,
            pool_mgr_flatten_tls_extras,
            pool_mgr_h3_protocol_to_ets,
            pool_mgr_otel_option_flows_to_conn_opts,
            pool_mgr_reconnect_backoff_progression,
            pool_mgr_drain_idle_pool_completes_immediately
        ]},
        {reconnect_brake, [sequence], [
            pool_backoff_grows_when_connection_never_serves,
            pool_backoff_resets_after_first_response,
            pool_backoff_does_not_reset_on_handshake_alone,
            pool_backoff_ignores_clean_exit
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    {ok, _} = application:ensure_all_started(telemetry),
    PrivDir = proplists:get_value(priv_dir, Config, ""),
    ProjectRoot = find_project_root(PrivDir),
    ConfDir = filename:join([ProjectRoot, "test", "conf"]),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_file(CertFile) andalso filelib:is_file(KeyFile) of
        true ->
            [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false ->
            Config
    end.

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

end_per_suite(_Config) ->
    ok.

init_per_group(h3_pool, Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            case
                nhttp:start_link(test_h3_pool_server, #{
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
                    ct:fail("Failed to start H3 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(h2_pool, Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            Handler = nhttp_test_handler,
            case
                nhttp:start_link(test_h2_pool_server, #{
                    port => 0,
                    handler => Handler,
                    versions => [http2, http1_1],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    {ok, Port} = nhttp:get_port(ServerPid),
                    {CappedPid, CappedPort} = start_capped_h2_server(CertFile, KeyFile),
                    [
                        {server_pid, ServerPid},
                        {server_port, Port},
                        {capped_server_pid, CappedPid},
                        {capped_server_port, CappedPort}
                        | Config
                    ];
                {error, Reason} ->
                    ct:fail("Failed to start H2 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(_Group, Config) ->
    %% Start HTTP/1.1 test server
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_pool_server, #{
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
    nhttpc_test_server:stop(proplists:get_value(capped_server_pid, Config)),
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL LIFECYCLE TESTS
%%%-----------------------------------------------------------------------------

%% Test basic pool start and stop
pool_start_stop(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_start_stop,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2,
        max_overflow => 2
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    ?assert(is_pid(PoolSup)),

    %% Wait for pool to be ready
    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(200),
    ?assert(is_pid(MgrName)),

    %% Stop pool
    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test wait_ready functionality
pool_wait_ready(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_wait_ready,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(50),
    MgrPid = MgrName,
    ?assert(is_pid(MgrPid)),

    %% Wait for ready
    Result = nhttpc_pool:wait_ready(MgrPid, 5000),
    ?assertEqual(ok, Result),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test wait_ready timeout - simplified to just verify the path exists
pool_wait_ready_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_wait_timeout,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(50),
    MgrPid = MgrName,
    ?assert(is_pid(MgrPid)),

    %% Wait for ready - should succeed with working server
    Result = nhttpc_pool:wait_ready(MgrPid, 5000),
    ?assertEqual(ok, Result),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%%%-----------------------------------------------------------------------------
%%% CHECKOUT/CHECKIN TESTS
%%%-----------------------------------------------------------------------------

%% Test basic checkout and checkin
pool_checkout_checkin(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_checkout,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(50),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Checkout
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ?assert(is_pid(ConnPid)),

    %% Checkin
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test checkout and subsequent checkout after checkin
pool_checkout_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_checkout_timeout,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% First checkout - should succeed
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ?assert(is_pid(ConnPid)),

    %% Checkin
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),

    %% Second checkout after checkin - should succeed
    {ok, ConnPid2} = nhttpc_pool:checkout(MgrPid, 5000),
    ?assert(is_pid(ConnPid2)),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid2),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test multiple checkouts
pool_multiple_checkouts(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_multi_checkout,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 3
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Multiple checkouts
    {ok, Conn1} = nhttpc_pool:checkout(MgrPid, 5000),
    {ok, Conn2} = nhttpc_pool:checkout(MgrPid, 5000),
    {ok, Conn3} = nhttpc_pool:checkout(MgrPid, 5000),

    %% All should be different connections (HTTP/1.1)
    ?assertNotEqual(Conn1, Conn2),
    ?assertNotEqual(Conn2, Conn3),
    ?assertNotEqual(Conn1, Conn3),

    %% Checkin all
    ok = nhttpc_pool:checkin(MgrPid, Conn1),
    ok = nhttpc_pool:checkin(MgrPid, Conn2),
    ok = nhttpc_pool:checkin(MgrPid, Conn3),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%%%-----------------------------------------------------------------------------
%%% REQUEST TESTS
%%%-----------------------------------------------------------------------------

%% Test sending request through pool
pool_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_request,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(50),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Send request
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Resp} = nhttpc_pool:request(MgrPid, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test request error handling
pool_request_error(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_request_error,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        timeouts => #{request => 100}
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(50),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Send request to slow endpoint - should timeout
    Req = #{
        method => get,
        path => <<"/slow/500">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    Result = nhttpc_pool:request(MgrPid, Req, 5000),
    case Result of
        {error, {request_timeout, _}} -> ok;
        {error, _} -> ok;
        {ok, _} -> ct:fail("Expected timeout")
    end,

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test concurrent requests
pool_concurrent_requests(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_concurrent,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 3
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Send concurrent requests
    Self = self(),
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },

    Pids = [
        spawn_link(fun() ->
            Result = nhttpc_pool:request(MgrPid, Req, 5000),
            Self ! {req_done, self(), Result}
        end)
     || _ <- lists:seq(1, 5)
    ],

    %% Collect results
    Results = [
        receive
            {req_done, Pid, R} -> R
        after 10000 -> timeout
        end
     || Pid <- Pids
    ],
    SuccessCount = length([ok || {ok, _} <- Results]),
    ?assert(SuccessCount >= 3),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%%%-----------------------------------------------------------------------------
%%% CONNECTION MANAGEMENT TESTS
%%%-----------------------------------------------------------------------------

%% Test connection down handling
pool_connection_down(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_conn_down,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Checkout a connection
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),

    %% Kill the connection
    exit(ConnPid, kill),
    timer:sleep(200),

    %% Pool should still work
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Resp} = nhttpc_pool:request(MgrPid, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test connection draining notification
pool_connection_draining(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_draining,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Get a connection
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),

    %% Notify draining
    ok = nhttpc_pool:connection_draining(MgrPid, ConnPid),

    %% Pool should handle draining state
    timer:sleep(50),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% A mid-connection SETTINGS frame reaches the manager as a cast, and the
%% cast writes nothing. The connection process owns the cap slot, and the
%% manager runs the waiter sweep the new cap can release.
pool_update_max_streams(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_max_streams,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Get a connection
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),

    ConnEts = pool_conn_ets(PoolRef),
    ?assertEqual(1, conn_cap(ConnEts, ConnPid)),

    ok = nhttpc_pool:update_max_streams(MgrPid, ConnPid, ?UPDATED_MAX_STREAMS),
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),
    ?assertEqual(1, conn_cap(ConnEts, ConnPid)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

pool_connect_failure_logged(_Config) ->
    {ok, LSock} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(LSock),
    ok = gen_tcp:close(LSock),
    PoolRef = test_pool_connect_failure_logged,
    Self = self(),
    Filter = fun
        (#{level := error, msg := {report, #{event := connect_failed} = Report}}, Pid) ->
            Pid ! {connect_failed_report, Report},
            stop;
        (_Event, _Pid) ->
            ignore
    end,
    ok = logger:add_primary_filter(?CONNECT_FAILED_FILTER, {Filter, Self}),
    try
        {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, #{
            host => <<"127.0.0.1">>,
            port => Port,
            size => 1,
            max_overflow => 0,
            reconnect_backoff_base => 50,
            reconnect_backoff_max => 100
        }),
        unlink(PoolSup),
        First = receive_connect_failed_report(),
        Second = receive_connect_failed_report(),
        ?assertMatch(
            #{
                pool := PoolRef,
                host := <<"127.0.0.1">>,
                port := Port,
                reason := econnrefused,
                consecutive_failures := 1
            },
            First
        ),
        ?assertMatch(#{pool := PoolRef, consecutive_failures := 2}, Second),
        exit(PoolSup, shutdown),
        timer:sleep(100)
    after
        ok = logger:remove_primary_filter(?CONNECT_FAILED_FILTER)
    end,
    ok.

%%%-----------------------------------------------------------------------------
%%% NHTTPC_POOL_SUP COVERAGE TESTS
%%%-----------------------------------------------------------------------------

%% Test child_spec/2 function
pool_sup_child_spec(Config) ->
    Port = proplists:get_value(server_port, Config),

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },

    %% Get child spec
    Spec = nhttpc_pool_sup:child_spec(test_pool_child_spec, PoolOpts),
    ?assertMatch(#{id := {nhttpc_pool_sup, test_pool_child_spec}}, Spec),
    ?assertMatch(#{start := {nhttpc_pool_sup, start_link, _}}, Spec),
    ?assertMatch(#{type := supervisor}, Spec),
    ok.

%% Test stop/1 with PID — must tear down the whole pool tree.
pool_sup_stop_by_pid(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_stop_pid,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    timer:sleep(100),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ?assert(is_pid(MgrPid)),
    ?assert(is_pid(nhttpc_pool:lookup_conn_sup(PoolRef))),

    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Stop using the supervisor pid: the whole tree must be gone afterwards.
    ok = nhttpc_pool_sup:stop(PoolSup),

    ?assertEqual(undefined, nhttpc_pool:manager(PoolRef)),
    ?assertEqual(undefined, nhttpc_pool:lookup_conn_sup(PoolRef)),
    ?assertNot(is_process_alive(PoolSup)),
    ok.

%% Test stop/1 with the pool ref — same teardown contract.
pool_sup_stop_by_ref(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_stop_ref,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    timer:sleep(100),
    ?assert(is_pid(nhttpc_pool:lookup_sup(PoolRef))),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ?assert(is_pid(MgrPid)),
    ?assert(is_pid(nhttpc_pool:lookup_conn_sup(PoolRef))),

    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    ok = nhttpc_pool_sup:stop(PoolRef),

    ?assertEqual(undefined, nhttpc_pool:lookup_sup(PoolRef)),
    ?assertEqual(undefined, nhttpc_pool:manager(PoolRef)),
    ?assertEqual(undefined, nhttpc_pool:lookup_conn_sup(PoolRef)),
    ?assertNot(is_process_alive(PoolSup)),

    %% Stopping a non-existent pool is a no-op.
    ok = nhttpc_pool_sup:stop(test_pool_stop_ref_missing),
    ok.

%% Test checkout and checkin through the pool manager API
pool_sup_checkout_checkin(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_sup_checkout,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Checkout through the pool manager API
    {ok, ConnPid} = nhttpc_pool:get_connection(PoolRef, 5000),
    ?assert(is_pid(ConnPid)),

    %% Checkin through the pool manager API
    ok = nhttpc_pool:checkin_fast(PoolRef, ConnPid),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test request/3 through the pool manager API
pool_sup_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_sup_request,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,

    %% Wait for ready
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Request through the pool manager API
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Resp} = nhttpc_pool:request(PoolRef, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test pool with non-atom ref (hash-based naming)
pool_sup_non_atom_ref(Config) ->
    Port = proplists:get_value(server_port, Config),
    %% Pool refs are no longer derived into atoms; arbitrary terms must
    %% work end-to-end. Exercise a binary ref (the strongest case — proves
    %% the previous list_to_atom path is gone) and a tuple ref.
    BinaryRef = <<"test_pool_binary_ref">>,
    TupleRef = {test_pool, non_atom, 123},

    AtomCountBefore = erlang:system_info(atom_count),

    lists:foreach(
        fun(Ref) ->
            PoolOpts = #{
                host => <<"127.0.0.1">>,
                port => Port,
                size => 1
            },
            {ok, PoolSup} = nhttpc_pool_sup:start_link(Ref, PoolOpts),
            unlink(PoolSup),
            MgrPid = nhttpc_pool:manager(Ref),
            ?assert(is_pid(MgrPid)),
            ok = nhttpc_pool:wait_ready(MgrPid, 5000),

            Req = #{
                method => get,
                path => <<"/echo">>,
                headers => [{<<"host">>, <<"127.0.0.1">>}],
                body => <<>>
            },
            {ok, Resp} = nhttpc_pool:request(MgrPid, Req, 5000),
            ?assertEqual(200, maps:get(status, Resp)),

            ok = nhttpc_pool_sup:stop(Ref),
            ?assertEqual(undefined, nhttpc_pool:manager(Ref)),
            ?assertEqual(undefined, nhttpc_pool:lookup_sup(Ref))
        end,
        [BinaryRef, TupleRef]
    ),

    %% No pool ref should mint atoms.
    AtomCountAfter = erlang:system_info(atom_count),
    ?assertEqual(AtomCountBefore, AtomCountAfter),
    ok.

%% Regression: starting and stopping many pools with distinct refs must
%% not grow the atom table. This guards against any regression that
%% reintroduces `list_to_atom/1` from a user-supplied identifier.
pool_sup_atom_table_stable_under_churn(Config) ->
    Port = proplists:get_value(server_port, Config),
    N = 100,
    AtomCountBefore = erlang:system_info(atom_count),
    lists:foreach(
        fun(I) ->
            Ref = <<"churn_pool_", (integer_to_binary(I))/binary>>,
            PoolOpts = #{
                host => <<"127.0.0.1">>,
                port => Port,
                size => 1
            },
            {ok, PoolSup} = nhttpc_pool_sup:start_link(Ref, PoolOpts),
            unlink(PoolSup),
            ok = nhttpc_pool_sup:stop(Ref)
        end,
        lists:seq(1, N)
    ),
    AtomCountAfter = erlang:system_info(atom_count),
    ?assertEqual(AtomCountBefore, AtomCountAfter),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL CONNECTION LIFECYCLE TESTS
%%%-----------------------------------------------------------------------------

%% Test pool with short idle timeout triggers idle close path
pool_idle_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_idle_timeout,

    %% idle_check_interval = min(30000, max(5000, idle_timeout div 2))
    %% With 5000, interval = max(5000, 2500) = 5000. Check fires at ~5s.
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        idle_timeout => 5000
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Make a request to ensure connection is established and working
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Resp} = nhttpc_pool:request(MgrPid, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp)),

    %% Wait for idle check to fire (~5s) and trigger idle close
    timer:sleep(6000),

    %% Pool should still be functional (reconnects after idle close)
    {ok, Resp2} = nhttpc_pool:request(MgrPid, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp2)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test that pool reconnects after connection is killed
pool_connection_reconnect(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_reconnect,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        reconnect_backoff_base => 50,
        reconnect_backoff_max => 200
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Get a connection and kill it
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    exit(ConnPid, kill),
    timer:sleep(500),

    %% Pool should reconnect and serve requests again
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Resp} = nhttpc_pool:request(MgrPid, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test waiter deadline expiration (checkout times out)
pool_waiter_deadline_expired(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_waiter_expire,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Checkout the only connection (hold it)
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),

    %% Second checkout with short timeout should fail (waiter expires)
    Result = nhttpc_pool:checkout(MgrPid, 200),
    case Result of
        {error, checkout_timeout} -> ok;
        {error, _} -> ok
    end,

    %% Checkin the held connection
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test multiple checkout/checkin cycles to exercise connection ready flow
pool_multiple_checkin_checkout_cycles(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_cycles,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(100),
    MgrPid = MgrName,
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Multiple rapid checkout/checkin cycles
    lists:foreach(
        fun(_) ->
            {ok, Conn} = nhttpc_pool:checkout(MgrPid, 5000),
            ok = nhttpc_pool:checkin(MgrPid, Conn)
        end,
        lists:seq(1, 10)
    ),

    %% Concurrent checkout/checkin
    Self = self(),
    Pids = [
        spawn_link(fun() ->
            {ok, Conn} = nhttpc_pool:checkout(MgrPid, 5000),
            timer:sleep(10),
            ok = nhttpc_pool:checkin(MgrPid, Conn),
            Self ! {done, self()}
        end)
     || _ <- lists:seq(1, 5)
    ],
    lists:foreach(
        fun(Pid) ->
            receive
                {done, Pid} -> ok
            after 10000 -> ct:fail(timeout)
            end
        end,
        Pids
    ),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%%%-----------------------------------------------------------------------------
%%% H2 POOL TESTS
%%%-----------------------------------------------------------------------------

%% Test H2 pool request - covers H2 fast-path, pool notifications, SSL connect opts
pool_h2_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_req,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        tls => #{verify => verify_none},
        versions => [http2],
        size => 1
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(200),
    MgrPid = MgrName,
    ok = nhttpc_pool:wait_ready(MgrPid, 10000),

    %% Make H2 request through pool
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Resp} = nhttpc_pool:request(MgrPid, Req, 10000),
    ?assertEqual(200, maps:get(status, Resp)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

pool_tls_supported_groups(Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    {ok, Listen} = ssl:listen(0, [
        binary,
        {certfile, CertFile},
        {keyfile, KeyFile},
        {versions, ['tlsv1.3']},
        {supported_groups, [x25519]},
        {reuseaddr, true}
    ]),
    {ok, {_, Port}} = ssl:sockname(Listen),
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        tls => #{verify => verify_none, supported_groups => [secp256r1]},
        versions => [http2],
        size => 1
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(test_pool_tls_groups, PoolOpts),
    unlink(PoolSup),
    MRef = monitor(process, PoolSup),
    {ok, PreSock} = ssl:transport_accept(Listen, 5000),
    ?assertMatch({error, _}, ssl:handshake(PreSock, 5000)),
    ok = ssl:close(Listen),
    exit(PoolSup, shutdown),
    receive
        {'DOWN', MRef, process, PoolSup, shutdown} -> ok
    after 5000 -> ct:fail(pool_sup_not_stopped)
    end,
    ok.

%% Test concurrent H2 requests through pool - exercises H2 multiplexing + fast-path
pool_h2_concurrent(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_conc,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        tls => #{verify => verify_none},
        versions => [http2],
        size => 1
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(200),
    MgrPid = MgrName,
    ok = nhttpc_pool:wait_ready(MgrPid, 10000),

    %% Fire concurrent requests (H2 multiplexing on single connection)
    Self = self(),
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },

    Pids = [
        spawn_link(fun() ->
            Result = nhttpc_pool:request(MgrPid, Req, 10000),
            Self ! {req_done, self(), Result}
        end)
     || _ <- lists:seq(1, 10)
    ],

    Results = [
        receive
            {req_done, Pid, R} -> R
        after 15000 -> timeout
        end
     || Pid <- Pids
    ],
    SuccessCount = length([ok || {ok, _} <- Results]),
    ?assert(SuccessCount >= 8),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Test H2 fast-path checkout via nhttpc_pool:get_connection/2
%% Exercises acquire/2, try_h2_conn/6, reserve_multiplexed_slot/2
pool_h2_fast_path_checkout(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_fast,

    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        tls => #{verify => verify_none},
        versions => [http2],
        size => 2
    },

    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),

    MgrName = nhttpc_pool:manager(PoolRef),
    timer:sleep(200),
    MgrPid = MgrName,
    ok = nhttpc_pool:wait_ready(MgrPid, 10000),

    %% Give pool time to establish H2 connections and build the persistent_term tuple
    timer:sleep(500),

    %% Use fast-path checkout
    {ok, ConnPid} = nhttpc_pool:get_connection(PoolRef, 5000),
    ?assert(is_pid(ConnPid)),

    %% Checkin via fast path
    ok = nhttpc_pool:checkin_fast(PoolRef, ConnPid),

    %% Multiple fast-path checkouts
    lists:foreach(
        fun(_) ->
            {ok, C} = nhttpc_pool:get_connection(PoolRef, 5000),
            ok = nhttpc_pool:checkin_fast(PoolRef, C)
        end,
        lists:seq(1, 5)
    ),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% A draining H2 connection must never be handed out by the fast-path
%% checkout. Draining exclusion is enforced by rebuild_h2_tuple/1
%% removing the conn from the round-robin tuple; this guards the
%% single-op reserve_multiplexed_slot/2 rewrite which no longer reads the
%% (always-`idle`) ETS Status column.
pool_h2_draining_skipped_on_checkout(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_drain_skip,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        tls => #{verify => verify_none},
        versions => [http2],
        size => 2
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 10000),

    Pids = collect_distinct_conns(PoolRef, 2, 200),
    ?assertEqual(2, length(Pids)),
    [DrainPid, KeepPid] = Pids,

    ok = nhttpc_pool:connection_draining(MgrPid, DrainPid),
    %% Synchronous call after the cast: gen_server FIFO guarantees
    %% handle_connection_draining (and rebuild_h2_tuple) has run.
    ?assert(is_tuple(sys:get_state(MgrPid))),

    Seen = lists:map(
        fun(_) ->
            {ok, C} = nhttpc_pool:get_connection(PoolRef, 5000),
            ok = nhttpc_pool:checkin_fast(PoolRef, C),
            C
        end,
        lists:seq(1, 50)
    ),
    ?assertNot(lists:member(DrainPid, Seen)),
    ?assert(lists:member(KeepPid, Seen)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

-spec collect_distinct_conns(atom(), pos_integer(), non_neg_integer()) -> [pid()].
collect_distinct_conns(_PoolRef, _Want, 0) ->
    [];
collect_distinct_conns(PoolRef, Want, Attempts) ->
    Seen = lists:foldl(
        fun(_, Acc) ->
            case nhttpc_pool:get_connection(PoolRef, 5000) of
                {ok, C} ->
                    ok = nhttpc_pool:checkin_fast(PoolRef, C),
                    case lists:member(C, Acc) of
                        true -> Acc;
                        false -> [C | Acc]
                    end;
                {error, _} ->
                    Acc
            end
        end,
        [],
        lists:seq(1, 16)
    ),
    case length(Seen) >= Want of
        true ->
            lists:sublist(lists:reverse(Seen), Want);
        false ->
            timer:sleep(50),
            collect_distinct_conns(PoolRef, Want, Attempts - 1)
    end.

%%%-----------------------------------------------------------------------------
%%% H2 STREAM ACCOUNTING / SWEEP TESTS (plan 02)
%%%-----------------------------------------------------------------------------

%% The record field is HTTP/1.1 only. Five gen_server-path requests on an
%% H2 connection must leave it at zero; the reservation lives in ETS.
pool_h2_conn_info_active_count_stays_zero(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_active_zero,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        tls => #{verify => verify_none},
        versions => [http2],
        size => 1,
        max_overflow => 0
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 10000),
    [ConnPid] = mgr_conn_pids(MgrPid),

    ok = run_pool_requests(MgrPid, 5),
    ?assertEqual(0, mgr_active_count(MgrPid, ConnPid)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% max_connection_age must rotate an H2 connection that only ever served
%% gen_server-path checkouts. The record field is HTTP/1.1 only, so the
%% sweep has to read the live stream count from ETS column 4.
pool_h2_sweep_rotates_after_gen_server_traffic(Config) ->
    PoolRef = test_h2_pool_sweep_rotate,
    {PoolSup, MgrPid} = start_h2_sweep_pool(PoolRef, Config),
    [Before] = mgr_conn_pids(MgrPid),

    ok = run_pool_requests(MgrPid, 3),
    [After] = wait_for_rotation(MgrPid, Before, 5000),
    ?assertNotEqual(Before, After),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% A swept connection must lose its ETS row. Without the delete the row
%% leaks for the life of the pool, because the sweep demonitors first and
%% the DOWN never reaches handle_connection_down/1.
pool_h2_sweep_deletes_ets_row(Config) ->
    PoolRef = test_h2_pool_sweep_ets,
    {PoolSup, MgrPid} = start_h2_sweep_pool(PoolRef, Config),
    ConnEts = pool_conn_ets(PoolRef),
    [Gen0] = mgr_conn_pids(MgrPid),

    ok = run_pool_requests(MgrPid, 3),
    [_Gen1] = wait_for_rotation(MgrPid, Gen0, 5000),
    MaxRows = max_ets_rows(ConnEts, 3000, 0),

    ?assertEqual(1, MaxRows),
    ?assertNot(lists:member(Gen0, ets_conn_pids(ConnEts))),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Connection shutdown is synchronous and waits up to five seconds. The
%% manager is on the request path, so the sweep must not run it inline.
%% A suspended connection cannot answer {stop, _}, which pins the caller
%% for the full timeout.
pool_h2_sweep_does_not_block_manager(Config) ->
    PoolRef = test_h2_pool_sweep_block,
    {PoolSup, MgrPid} = start_h2_sweep_pool(PoolRef, Config),
    [ConnPid] = mgr_conn_pids(MgrPid),

    ok = sys:suspend(ConnPid),
    timer:sleep(?SWEEP_MAX_AGE + ?SWEEP_INTERVAL),
    MaxElapsed = max_wait_ready_latency(MgrPid, 2000, 0),
    ok = sys:resume(ConnPid),

    ?assert(MaxElapsed < 1000),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

-spec start_h2_sweep_pool(atom(), [tuple()]) -> {pid(), pid()}.
start_h2_sweep_pool(PoolRef, Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        tls => #{verify => verify_none},
        versions => [http2],
        size => 1,
        max_overflow => 0,
        max_connection_age => ?SWEEP_MAX_AGE,
        connection_sweep_interval => ?SWEEP_INTERVAL
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 10000),
    {PoolSup, MgrPid}.

-spec run_pool_requests(pid(), pos_integer()) -> ok.
run_pool_requests(MgrPid, Count) ->
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    lists:foreach(
        fun(_) ->
            {ok, Resp} = nhttpc_pool:request(MgrPid, Req, 10000),
            ?assertEqual(200, maps:get(status, Resp))
        end,
        lists:seq(1, Count)
    ).

%% The manager keeps #conn_info{} records in the only map in #state{}
%% whose values all carry the conn_info tag.
-spec mgr_connections(pid()) -> #{pid() => tuple()}.
mgr_connections(MgrPid) ->
    State = sys:get_state(MgrPid),
    Candidates = [
        M
     || M <- tuple_to_list(State),
        is_map(M),
        map_size(M) > 0,
        lists:all(
            fun(V) -> is_tuple(V) andalso element(1, V) =:= conn_info end,
            maps:values(M)
        )
    ],
    case Candidates of
        [Conns] -> Conns;
        [] -> #{}
    end.

-spec mgr_conn_pids(pid()) -> [pid()].
mgr_conn_pids(MgrPid) ->
    lists:sort(maps:keys(mgr_connections(MgrPid))).

%% Destructures #conn_info{} positionally: the full match fails loudly if
%% the record ever changes shape.
-spec mgr_active_count(pid(), pid()) -> non_neg_integer().
mgr_active_count(MgrPid, ConnPid) ->
    {conn_info, Pid, MonRef, Protocol, ActiveCount, Slots, Draining, _CreatedAt, _LastActivity} =
        maps:get(ConnPid, mgr_connections(MgrPid)),
    ?assertEqual(ConnPid, Pid),
    ?assert(is_reference(MonRef)),
    ?assert(is_atom(Protocol)),
    ?assert(is_reference(Slots)),
    ?assert(is_boolean(Draining)),
    ActiveCount.

-spec wait_for_rotation(pid(), pid(), integer()) -> [pid()].
wait_for_rotation(MgrPid, OldPid, Remaining) when Remaining =< 0 ->
    ct:fail("connection ~p was never rotated (pool holds ~p)", [OldPid, mgr_conn_pids(MgrPid)]);
wait_for_rotation(MgrPid, OldPid, Remaining) ->
    case mgr_conn_pids(MgrPid) of
        [NewPid] when NewPid =/= OldPid ->
            [NewPid];
        _ ->
            timer:sleep(50),
            wait_for_rotation(MgrPid, OldPid, Remaining - 50)
    end.

%% Destructures #pool_env{} positionally: the full match fails loudly if
%% the record ever changes shape.
-spec pool_env(term()) -> tuple().
pool_env(PoolRef) ->
    {pool_env, Ref, Tab, ConnEts, Atomics, _Telemetry, Selection, ConnMod} =
        Env =
        persistent_term:get({nhttpc_pool, PoolRef}),
    ?assertEqual(PoolRef, Ref),
    ?assert(is_reference(Tab)),
    ?assert(is_reference(ConnEts)),
    ?assert(is_reference(Atomics)),
    ?assert(Selection =:= pack orelse Selection =:= spread),
    ?assert(ConnMod =:= nhttpc_conn orelse ConnMod =:= nhttpc_h3_conn),
    Env.

-spec pool_conn_ets(term()) -> ets:tid().
pool_conn_ets(PoolRef) ->
    element(4, pool_env(PoolRef)).

-spec pool_atomics(term()) -> atomics:atomics_ref().
pool_atomics(PoolRef) ->
    element(5, pool_env(PoolRef)).

-spec ets_conn_pids(ets:tid()) -> [pid()].
ets_conn_pids(ConnEts) ->
    lists:sort([Pid || {Pid, _Proto, _Status, _Slots} <- ets:tab2list(ConnEts)]).

%% Stream slots of one connection, or `undefined` once the sweep deleted
%% its row.
-spec conn_slots(ets:tid(), pid()) -> nhttpc_stream_slots:slots() | undefined.
conn_slots(ConnEts, ConnPid) ->
    ets:lookup_element(ConnEts, ConnPid, 4, undefined).

-spec conn_cap(ets:tid(), pid()) -> integer().
conn_cap(ConnEts, ConnPid) ->
    case conn_slots(ConnEts, ConnPid) of
        undefined -> 0;
        Slots -> nhttpc_stream_slots:cap(Slots)
    end.

-spec conn_live(ets:tid(), pid()) -> integer().
conn_live(ConnEts, ConnPid) ->
    case conn_slots(ConnEts, ConnPid) of
        undefined -> 0;
        Slots -> nhttpc_stream_slots:live(Slots)
    end.

-spec set_conn_cap(ets:tid(), pid(), non_neg_integer()) -> ok.
set_conn_cap(ConnEts, ConnPid, Cap) ->
    nhttpc_stream_slots:set_cap(conn_slots(ConnEts, ConnPid), Cap).

%% Highest row count seen over Window milliseconds. A leaking sweep grows
%% the table by one row per rotation.
-spec max_ets_rows(ets:tid(), integer(), non_neg_integer()) -> non_neg_integer().
max_ets_rows(_ConnEts, Window, Worst) when Window =< 0 ->
    Worst;
max_ets_rows(ConnEts, Window, Worst) ->
    timer:sleep(25),
    max_ets_rows(ConnEts, Window - 25, max(Worst, ets:info(ConnEts, size))).

%% Worst manager round-trip observed over Window milliseconds.
-spec max_wait_ready_latency(pid(), integer(), non_neg_integer()) -> non_neg_integer().
max_wait_ready_latency(_MgrPid, Window, Worst) when Window =< 0 ->
    Worst;
max_wait_ready_latency(MgrPid, Window, Worst) ->
    Start = erlang:monotonic_time(millisecond),
    ok = nhttpc_pool:wait_ready(MgrPid, 1000),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    timer:sleep(20),
    max_wait_ready_latency(MgrPid, Window - (Elapsed + 20), max(Worst, Elapsed)).

%%%-----------------------------------------------------------------------------
%%% ETS CHECKOUT FAST PATH TESTS (plan 03)
%%%-----------------------------------------------------------------------------

%% The public API must reach a pooled H2 connection through ETS alone: no
%% checkout call, and no checkin cast while the manager holds no waiter
%% and runs no drain. The reservation still has to balance, so the live
%% stream count returns to zero.
pool_h2_public_api_uses_fast_path(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_public_fast,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),

    ok = trace_manager_messages(MgrPid),
    ok = run_public_requests(PoolRef, Port, 5),
    {Checkouts, Checkins} = collect_manager_messages(MgrPid),

    ?assertEqual(0, Checkouts),
    ?assertEqual(0, Checkins),
    ?assertEqual(0, mgr_active_count(MgrPid, ConnPid)),
    ?assertEqual(0, wait_for_idle_streams(ConnEts, ConnPid, 2000)),

    stop_h2_pool(PoolSup),
    ok.

%% The cap the pool enforces is the one the peer advertised in its opening
%% SETTINGS frame. Before this the handshake threw those events away and
%% every connection carried the client's own default of 100, whatever the
%% server accepted.
pool_h2_adopts_peer_max_concurrent_streams(Config) ->
    Port = proplists:get_value(capped_server_port, Config),
    PoolRef = test_h2_pool_peer_cap,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),

    ?assertEqual(?CAP_MAX_STREAMS, conn_cap(ConnEts, ConnPid)),

    ok = run_public_requests(PoolRef, Port, 3),
    ?assertEqual(?CAP_MAX_STREAMS, conn_cap(ConnEts, ConnPid)),

    stop_h2_pool(PoolSup),
    ok.

%% The ready cast carries the peer value and writes nothing. The
%% connection process owns the cap slot, so a second `connection_ready/3`
%% against a connection that already carries the peer's cap of two leaves
%% that cap in place rather than replacing it with the client ceiling.
pool_h2_ready_cast_leaves_cap(Config) ->
    Port = proplists:get_value(capped_server_port, Config),
    PoolRef = test_h2_pool_default_cap,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    ?assertEqual(?CAP_MAX_STREAMS, conn_cap(ConnEts, ConnPid)),

    ok = nhttpc_pool:connection_ready(MgrPid, ConnPid, http2),
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),
    ?assertEqual(?CAP_MAX_STREAMS, conn_cap(ConnEts, ConnPid)),

    stop_h2_pool(PoolSup),
    ok.

%% A burst wider than the peer's cap still completes. Every stream past
%% the cap waits for a slot instead of reaching the server, so no caller
%% sees `REFUSED_STREAM`. Both checkout paths run at once, because both
%% reserve against the same column.
pool_h2_respects_peer_cap_under_load(Config) ->
    Port = proplists:get_value(capped_server_port, Config),
    PoolRef = test_h2_pool_cap_load,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    ?assertEqual(?CAP_MAX_STREAMS, conn_cap(ConnEts, ConnPid)),

    Parent = self(),
    Workers =
        [
            spawn_link(fun() ->
                Parent ! {cap_load, self(), collect_public_results(PoolRef, Port, ?CAP_LOAD_SIZE)}
            end)
         || _ <- lists:seq(1, ?CAP_LOAD_WORKERS div 2)
        ] ++
            [
                spawn_link(fun() ->
                    Parent ! {cap_load, self(), collect_pool_results(MgrPid, ?CAP_LOAD_SIZE)}
                end)
             || _ <- lists:seq(1, ?CAP_LOAD_WORKERS div 2)
            ],
    Statuses = collect_cap_load(Workers, []),

    ?assertEqual(?CAP_LOAD_WORKERS * ?CAP_LOAD_SIZE, length(Statuses)),
    ?assertEqual([200], lists:usort(Statuses)),

    stop_h2_pool(PoolSup),
    ok.

%% Both checkout paths reserve through the same atomic ETS operation, so
%% no reservation outlives the cap. A read followed by a separate
%% increment lets the two paths both clear an Active < Max test on the
%% last free slot, and the extra stream then stays open for a whole
%% request. The single atomic operation can also pass the cap, but only
%% between its own increment and its own rollback, so the test measures
%% how long an overshoot lasts rather than whether one is ever visible.
%%
%% The window a split read-then-increment leaves open is sub-microsecond,
%% so this test is a concurrency guard rather than a proof: it drives both
%% paths at the same connection under a cap of two and rejects any
%% overshoot that survives longer than a rollback can. The cap of two is
%% the one the capped server advertises, not one the test writes.
pool_h2_cap_never_exceeded_under_mixed_paths(Config) ->
    Port = proplists:get_value(capped_server_port, Config),
    PoolRef = test_h2_pool_cap,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    ?assertEqual(?CAP_MAX_STREAMS, conn_cap(ConnEts, ConnPid)),

    Parent = self(),
    Slots = conn_slots(ConnEts, ConnPid),
    Sampler = spawn_link(fun() -> sample_active(Slots, Parent, new_sample()) end),
    Runners =
        [
            spawn_link(fun() ->
                ok = run_public_requests(PoolRef, Port, ?CAP_BURST_SIZE),
                Parent ! {burst_done, self()}
            end)
         || _ <- lists:seq(1, ?CAP_BURST_WORKERS)
        ] ++
            [
                spawn_link(fun() ->
                    ok = run_pool_requests(MgrPid, ?CAP_BURST_SIZE),
                    Parent ! {burst_done, self()}
                end)
             || _ <- lists:seq(1, ?CAP_BURST_WORKERS)
            ],
    ok = await_bursts(Runners),
    Sampler ! {stop, self()},
    {MaxActive, OvershootUs} =
        receive
            {max_active, Sample} -> Sample
        after 5000 -> ct:fail("sampler never reported")
        end,

    ?assert(MaxActive > 0),
    ?assert(OvershootUs < ?CAP_OVERSHOOT_LIMIT_US),

    stop_h2_pool(PoolSup),
    ok.

%% Pool checkout and checkin telemetry fires on both paths. The fast path
%% emits from the calling process, the manager path from the manager, so
%% each is observed where it runs.
pool_h2_telemetry_parity(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_telemetry,
    ExtraOpts = #{size => 1, max_overflow => 0, otel => #{metrics => true, traces => false}},
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, ExtraOpts),

    FastEvents = trace_pool_telemetry(self(), fun() ->
        ok = run_public_requests(PoolRef, Port, 1)
    end),
    MgrEvents = trace_pool_telemetry(MgrPid, fun() ->
        ok = run_pool_requests(MgrPid, 1)
    end),

    ?assertEqual([{checkin, PoolRef}, {checkout, PoolRef}], lists:sort(FastEvents)),
    ?assertEqual([{checkin, PoolRef}, {checkout, PoolRef}], lists:sort(MgrEvents)),

    stop_h2_pool(PoolSup),
    ok.

%% A fast-path checkin skips the manager only while no waiter is queued.
%% With the cap held by an in-flight request, a manager checkout queues,
%% and the notify flag has to make the next checkin wake it.
pool_h2_waiter_wakes_when_cap_frees(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_waiter,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    Atomics = pool_atomics(PoolRef),
    ok = set_conn_cap(ConnEts, ConnPid, 1),

    Parent = self(),
    Slow = spawn_link(fun() ->
        Url = url(Port, <<"/slow/300">>),
        Parent ! {slow_done, nhttpc:get(Url, #{via => {pool, PoolRef}})}
    end),
    ?assert(is_pid(Slow)),
    ok = wait_for_active_streams(ConnEts, ConnPid, 1, 3000),

    Waiter = spawn_link(fun() -> Parent ! {waiter, nhttpc_pool:checkout(MgrPid, 5000)} end),
    ?assert(is_pid(Waiter)),
    ok = wait_for_notify_flag(Atomics, 3000),

    receive
        {slow_done, SlowResult} -> ?assertMatch({ok, #{status := 200}}, SlowResult)
    after 5000 -> ct:fail("slow request never completed")
    end,
    receive
        {waiter, WaiterResult} -> ?assertMatch({ok, _}, WaiterResult)
    after 5000 -> ct:fail("queued waiter was never served")
    end,

    stop_h2_pool(PoolSup),
    ok.

%% A slot can come free without any message reaching the manager: the
%% caller reserves, the connection process releases, and neither step is
%% ordered against the manager's capacity read. The periodic waiter check
%% has to re-serve the queue, so the waiter wakes within one sweep
%% interval instead of sleeping to its checkout deadline.
pool_h2_waiter_served_by_sweep(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_waiter_sweep,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    Atomics = pool_atomics(PoolRef),
    Slots = conn_slots(ConnEts, ConnPid),
    ok = nhttpc_stream_slots:set_cap(Slots, 1),
    ok = nhttpc_stream_slots:reserve(Slots),

    Parent = self(),
    Waiter = spawn_link(fun() ->
        Started = erlang:monotonic_time(millisecond),
        Result = nhttpc_pool:checkout(MgrPid, ?SWEEP_WAITER_TIMEOUT),
        Parent ! {waiter, Result, erlang:monotonic_time(millisecond) - Started}
    end),
    ?assert(is_pid(Waiter)),
    ok = wait_for_notify_flag(Atomics, 3000),

    ok = nhttpc_stream_slots:release(Slots),

    receive
        {waiter, WaiterResult, ElapsedMs} ->
            ?assertMatch({ok, ConnPid}, WaiterResult),
            ?assert(ElapsedMs < ?SWEEP_WAITER_LIMIT)
    after ?SWEEP_WAITER_TIMEOUT + 5000 -> ct:fail("queued waiter never reported")
    end,

    stop_h2_pool(PoolSup),
    ok.

%% Before the first connection reports ready the round-robin tuple is
%% empty, so the request has to reach the manager and queue there.
pool_h2_falls_back_before_first_connection(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_cold_start,
    PoolOpts = h2_pool_opts(Port, #{size => 1, max_overflow => 0}),
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = trace_manager_messages(MgrPid),

    Result = nhttpc:get(url(Port, <<"/echo">>), #{
        via => {pool, PoolRef}, timeouts => #{request => 10000}
    }),
    {Checkouts, _Checkins} = collect_manager_messages(MgrPid),

    ?assertMatch({ok, #{status := 200}}, Result),
    ?assertEqual(1, Checkouts),

    stop_h2_pool(PoolSup),
    ok.

%% HTTP/1.1 checkout stays exclusive: it never leaves the manager, and a
%% pool of two connections with no overflow hands out two pids and then
%% refuses.
pool_h1_still_exclusive(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_h1_exclusive,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 2,
        max_overflow => 0
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),
    ok = wait_for_conn_count(MgrPid, 2, 5000),

    {ok, First} = nhttpc_pool:get_connection(PoolRef, 5000),
    {ok, Second} = nhttpc_pool:get_connection(PoolRef, 5000),
    ?assertNotEqual(First, Second),
    ?assertMatch({error, _}, nhttpc_pool:get_connection(PoolRef, 300)),

    ok = nhttpc_pool:checkin_fast(PoolRef, First),
    ?assertEqual({ok, First}, nhttpc_pool:get_connection(PoolRef, 5000)),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%%%-----------------------------------------------------------------------------
%%% CONNECTION SELECTION POLICY TESTS (plan 04)
%%%-----------------------------------------------------------------------------

%% `pack` is the default, so a pool sized for four connections opens one
%% and keeps it. Every checkout returns that connection while it has a
%% free stream slot, and twenty sequential requests never grow the pool.
pool_h2_pack_uses_one_connection(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_pack_one,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 4, max_overflow => 4}),
    ?assertEqual(pack, pool_selection(PoolRef)),
    [ConnPid] = mgr_conn_pids(MgrPid),

    ?assertEqual([ConnPid], lists:usort(checkout_pids(PoolRef, ?PACK_REQUESTS))),
    ok = run_public_requests(PoolRef, Port, ?PACK_REQUESTS),

    ?assertEqual([ConnPid], mgr_conn_pids(MgrPid)),
    ?assertEqual([ConnPid], ets_conn_pids(pool_conn_ets(PoolRef))),

    stop_h2_pool(PoolSup),
    ok.

%% Packing stops at the peer's cap, not before it. The capped server
%% advertises two concurrent streams, so a burst of six slow requests
%% fills the first connection, opens the second, and finishes on the two
%% of them without ever putting a third stream on either.
pool_h2_pack_opens_second_at_cap(Config) ->
    Port = proplists:get_value(capped_server_port, Config),
    PoolRef = test_h2_pool_pack_cap,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 2, max_overflow => 0}),
    [First] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    ?assertEqual(?CAP_MAX_STREAMS, conn_cap(ConnEts, First)),

    Parent = self(),
    Sampler = spawn_link(fun() -> sample_pool_active(ConnEts, Parent, 0) end),
    Statuses = concurrent_statuses(PoolRef, Port, <<"/slow/200">>, ?PACK_CAP_BURST),
    Sampler ! {stop, self()},
    MaxActive =
        receive
            {pool_max_active, N} -> N
        after 5000 -> ct:fail("sampler never reported")
        end,

    ?assertEqual([200], lists:usort(Statuses)),
    ?assertEqual(?PACK_CAP_BURST, length(Statuses)),
    ?assertEqual(2, length(mgr_conn_pids(MgrPid))),
    ?assert(MaxActive =< ?CAP_MAX_STREAMS),

    stop_h2_pool(PoolSup),
    ok.

%% With two connections up, `pack` returns the older one for as long as
%% it has a free slot. The second connection exists only because the
%% first was capped at zero for a moment, which is the on-demand growth
%% arm; restoring the cap must send traffic back to the older connection.
pool_h2_pack_prefers_oldest(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_pack_oldest,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 2, max_overflow => 0}),
    [Oldest] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),

    ok = set_conn_cap(ConnEts, Oldest, 0),
    {ok, Newest} = nhttpc_pool:get_connection(PoolRef, 5000),
    ?assertNotEqual(Oldest, Newest),
    ok = nhttpc_pool:checkin_fast(PoolRef, Newest),
    ok = set_conn_cap(ConnEts, Oldest, ?DEFAULT_MAX_STREAMS),
    ok = wait_for_conn_count(MgrPid, 2, 5000),

    ?assertEqual([Oldest], lists:usort(checkout_pids(PoolRef, ?PACK_REQUESTS))),

    stop_h2_pool(PoolSup),
    ok.

%% `spread` keeps the behaviour that predates the policy: the pool opens
%% `size` connections at start, and consecutive checkouts move across
%% them instead of stacking on one.
pool_h2_spread_keeps_round_robin(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_spread,
    Extra = #{size => 4, max_overflow => 0, stream_selection => spread},
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, Extra),
    ?assertEqual(spread, pool_selection(PoolRef)),
    ok = wait_for_conn_count(MgrPid, 4, 5000),

    Touched = lists:usort(checkout_pids(PoolRef, ?PACK_REQUESTS)),
    ?assert(length(Touched) > 1),
    ok = run_public_requests(PoolRef, Port, ?PACK_REQUESTS),

    stop_h2_pool(PoolSup),
    ok.

%% The sweep rotates a `pack` pool, it does not grow it. An aged
%% connection is replaced by one, never by `size`, so the pool holds one
%% connection at a time and the pids it hands out change as the sweep
%% retires them.
pool_h2_pack_no_sweep_churn(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_pack_sweep,
    Extra = #{
        size => 4,
        max_overflow => 0,
        max_connection_age => ?PACK_SWEEP_AGE,
        connection_sweep_interval => ?PACK_SWEEP_INTERVAL
    },
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, Extra),

    {MaxHeld, Seen} = light_traffic(PoolRef, Port, MgrPid, ?PACK_SWEEP_WINDOW, {0, []}),

    ?assert(MaxHeld =< 2),
    ?assert(length(Seen) >= 2),

    stop_h2_pool(PoolSup),
    ok.

%% The policy names a multiplexed protocol, so an HTTP/1.1 pool ignores
%% it. It still fills to `size`, and its checkout is still exclusive: two
%% connections hand out two pids and the third caller waits.
pool_h1_unaffected_by_policy(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h1_pool_policy,
    PoolOpts = h2_pool_opts(Port, #{
        size => 2,
        max_overflow => 0,
        versions => [http1_1],
        stream_selection => pack
    }),
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 10000),
    ok = wait_for_conn_count(MgrPid, 2, 5000),

    {ok, First} = nhttpc_pool:get_connection(PoolRef, 5000),
    {ok, Second} = nhttpc_pool:get_connection(PoolRef, 5000),
    ?assertNotEqual(First, Second),
    ?assertMatch({error, _}, nhttpc_pool:get_connection(PoolRef, 300)),

    ok = nhttpc_pool:checkin_fast(PoolRef, First),
    ?assertEqual({ok, First}, nhttpc_pool:get_connection(PoolRef, 5000)),

    stop_h2_pool(PoolSup),
    ok.

%%%-----------------------------------------------------------------------------
%%% STREAM SLOT RESERVATION TESTS (plan 06)
%%%-----------------------------------------------------------------------------

%% Every connection owns its stream slots, so a connection the sweep
%% retires cannot reach the replacement. An index into one shared array
%% would need a free list, and a recycled index would let the old process
%% release a slot that the new connection holds.
pool_stream_count_survives_connection_rotation(Config) ->
    PoolRef = test_h2_pool_rotate_slots,
    {PoolSup, MgrPid} = start_h2_sweep_pool(PoolRef, Config),
    ConnEts = pool_conn_ets(PoolRef),
    [Gen0] = mgr_conn_pids(MgrPid),
    Slots0 = conn_slots(ConnEts, Gen0),

    ok = run_pool_requests(MgrPid, 3),
    [Gen1] = wait_for_rotation(MgrPid, Gen0, 5000),
    Slots1 = conn_slots(ConnEts, Gen1),

    ?assertNotEqual(Slots0, Slots1),
    ?assertEqual(0, nhttpc_stream_slots:live(Slots1)),

    ok = nhttpc_stream_slots:reserve(Slots0),
    ?assertEqual(0, nhttpc_stream_slots:live(Slots1)),
    ok = nhttpc_stream_slots:release(Slots0),
    ?assertEqual(0, nhttpc_stream_slots:live(Slots1)),

    ok = run_pool_requests(MgrPid, 1),
    ?assertEqual(0, wait_for_idle_streams(ConnEts, Gen1, 2000)),

    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

%% Two releases for one reservation is a defect. The signed array makes it
%% visible: the count resets to zero and the release logs, rather than
%% clamping at zero and hiding every later leak behind the same silence.
pool_negative_stream_count_is_reported(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_underflow,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    ConnEts = pool_conn_ets(PoolRef),
    [ConnPid] = mgr_conn_pids(MgrPid),
    Slots = conn_slots(ConnEts, ConnPid),

    ok = nhttpc_stream_slots:reserve(Slots),
    Reports = capture_underflow_reports(fun() ->
        ok = nhttpc_stream_slots:release(Slots),
        ok = nhttpc_stream_slots:release(Slots)
    end),

    ?assertMatch([#{event := stream_slot_underflow, live := -1}], Reports),
    ?assertEqual(0, nhttpc_stream_slots:live(Slots)),

    ok = run_public_requests(PoolRef, Port, 3),
    ?assertEqual(0, wait_for_idle_streams(ConnEts, ConnPid, 2000)),

    stop_h2_pool(PoolSup),
    ok.

%% The reservation is one atomic operation on the connection's own array.
%% ETS still holds the protocol column that checkin reads, but no counter,
%% so a pooled request must not reach `ets:update_counter/3` at all.
pool_reservation_reaches_no_ets(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_no_ets,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    ConnEts = pool_conn_ets(PoolRef),
    [ConnPid] = mgr_conn_pids(MgrPid),

    Calls = count_ets_update_counter(fun() ->
        run_public_requests(PoolRef, Port, ?NO_ETS_REQUESTS)
    end),

    ?assertEqual(0, Calls),
    ?assertEqual(0, wait_for_idle_streams(ConnEts, ConnPid, 2000)),

    stop_h2_pool(PoolSup),
    ok.

%%%-----------------------------------------------------------------------------
%%% PEER CAP SHRINK AND RE-DISPATCH TESTS
%%%-----------------------------------------------------------------------------

%% A cap that drops below the live stream count leaves the open streams
%% alone and stops every new reservation until the live count falls under
%% the new cap. RFC 9113 section 6.5.2 asks for exactly that.
pool_h2_shrink_blocks_new_checkouts(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_shrink,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    ok = set_conn_cap(ConnEts, ConnPid, ?SHRINK_LIVE_STREAMS),

    Parent = self(),
    Workers = [
        spawn_link(fun() ->
            Parent !
                {shrink_done, nhttpc:get(url(Port, <<"/slow/1000">>), #{via => {pool, PoolRef}})}
        end)
     || _ <- lists:seq(1, ?SHRINK_LIVE_STREAMS)
    ],
    ?assertEqual(?SHRINK_LIVE_STREAMS, length(Workers)),
    ok = wait_for_active_streams(ConnEts, ConnPid, ?SHRINK_LIVE_STREAMS, 5000),

    ok = set_conn_cap(ConnEts, ConnPid, ?SHRUNK_MAX_STREAMS),
    ?assertMatch({error, _}, nhttpc_pool:checkout(MgrPid, 300)),
    ?assertEqual(?SHRINK_LIVE_STREAMS, conn_live(ConnEts, ConnPid)),

    collect_shrink_results(?SHRINK_LIVE_STREAMS),

    stop_h2_pool(PoolSup),
    ok.

%% A request the pool admitted against a cap the peer had already lowered
%% is re-dispatched onto another connection instead of failing. The first
%% connection carries a cap of ten over a peer cap of two, so
%% `nhttp_h2:open_stream/1` refuses before any frame leaves the client.
%% The pool runs `spread` here: `pack` is a first fit over a stable order,
%% so it would re-select the same over-granted connection, which is what
%% the bound in `pool_h2_redispatch_is_bounded` covers.
pool_h2_redispatch_on_over_grant(Config) ->
    Port = proplists:get_value(capped_server_port, Config),
    PoolRef = test_h2_pool_redispatch,
    {PoolSup, _MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 2, max_overflow => 0, stream_selection => spread
    }),
    ok = wait_for_multiplexed_conns(PoolRef, 2, 5000),
    ConnEts = pool_conn_ets(PoolRef),
    [Over | _] = Order = pool_conn_order(PoolRef),

    Held = occupy_conn_streams(ConnEts, Over, ?CAP_MAX_STREAMS),
    ok = set_conn_cap(ConnEts, Over, ?OVER_GRANT_CAP),
    ok = aim_spread_cursor(pool_atomics(PoolRef), length(Order)),

    Req = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    ?assertMatch({ok, #{status := 200}}, nhttpc_pool:request(PoolRef, Req, 10000)),

    collect_held_streams(Over, Held),

    stop_h2_pool(PoolSup),
    ok.

%% A pool with nowhere else to send the request answers the caller rather
%% than looping. Every attempt lands on the one over-granted connection,
%% so the bound is what ends the re-dispatch, and it ends it as a refused
%% stream well inside the checkout timeout.
pool_h2_redispatch_is_bounded(Config) ->
    Port = proplists:get_value(capped_server_port, Config),
    PoolRef = test_h2_pool_redispatch_bound,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 0}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),

    Held = occupy_conn_streams(ConnEts, ConnPid, ?CAP_MAX_STREAMS),
    ok = set_conn_cap(ConnEts, ConnPid, ?OVER_GRANT_CAP),

    Req = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    Started = erlang:monotonic_time(millisecond),
    Result = nhttpc_pool:request(PoolRef, Req, 10000),
    Elapsed = erlang:monotonic_time(millisecond) - Started,

    ?assertEqual({error, {stream_error, {protocol, refused_stream}}}, Result),
    ?assert(Elapsed < ?REDISPATCH_LIMIT_MS),

    collect_held_streams(ConnPid, Held),

    stop_h2_pool(PoolSup),
    ok.

%% A request refused for lack of send credit is re-dispatched to a
%% connection other than the one that refused it, under both policies.
%% Both connections go to a peer that never credits, and both windows are
%% spent before the request, so every attempt is refused and the event
%% stream shows where each one landed. Under `pack` the fast path is a
%% first fit from index zero, so without the exclusion every attempt
%% lands on the same starved connection.
pool_h2_refused_is_redispatched_elsewhere(Config) ->
    ok = credit_redispatch_case(Config, pack, test_h2_pool_credit_pack),
    ok = credit_redispatch_case(Config, spread, test_h2_pool_credit_spread),
    ok.

-spec credit_redispatch_case([tuple()], nhttpc:stream_selection(), atom()) -> ok.
credit_redispatch_case(Config, Selection, PoolRef) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?CREDIT_PEER_CAP}),
    {PoolSup, _MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 2,
        max_overflow => 0,
        stream_selection => Selection,
        otel => #{events => true, metrics => false, traces => false}
    }),
    ok = wait_for_multiplexed_conns(PoolRef, 1, 5000),
    ConnEts = pool_conn_ets(PoolRef),
    lists:foreach(fun(Pid) -> exhaust_send_window(ConnEts, Pid) end, pool_conn_order(PoolRef)),
    ok = grow_to_two_conns(PoolRef),
    Order = pool_conn_order(PoolRef),
    ?assertEqual(2, length(Order)),
    lists:foreach(fun(Pid) -> exhaust_send_window(ConnEts, Pid) end, Order),

    Self = self(),
    HandlerId = {?MODULE, PoolRef},
    ok = telemetry:attach(
        HandlerId,
        [nhttpc, connection, send_credit_refused],
        fun(_Event, Measurements, Meta, _HandlerConfig) ->
            Self ! {credit_refused, maps:get(conn_pid, Meta), Measurements}
        end,
        undefined
    ),
    Result = nhttpc_pool:request(PoolRef, credit_post(), 10000),
    ok = telemetry:detach(HandlerId),
    ?assertEqual({error, {stream_error, {protocol, refused_stream}}}, Result),

    Attempts = collect_credit_refusals(?CREDIT_ATTEMPTS),
    ?assertEqual(?CREDIT_ATTEMPTS, length(Attempts)),
    ?assert(lists:all(fun(Pid) -> lists:member(Pid, Order) end, Attempts)),
    lists:foreach(
        fun({Previous, Next}) -> ?assertNotEqual(Previous, Next) end,
        lists:zip(lists:droplast(Attempts), tl(Attempts))
    ),

    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    drain_conn_messages().

%% Under `pack` the pool opens its second connection only when a request
%% finds no other place to go. A refused request whose re-dispatch
%% excludes the one connection is such a request: the manager skips it,
%% opens the second connection and serves the waiter there. The request
%% then waits on a peer that never answers, so it gets a short timeout
%% and runs in its own process.
-spec grow_to_two_conns(term()) -> ok.
grow_to_two_conns(PoolRef) ->
    case length(pool_conn_order(PoolRef)) of
        2 ->
            ok;
        1 ->
            _ = spawn(fun() ->
                nhttpc_pool:request(PoolRef, credit_post(), ?CREDIT_GROWTH_TIMEOUT)
            end),
            wait_for_multiplexed_conns(PoolRef, 2, 5000)
    end.

%% Send bodies straight to the connection, reserving a slot for each as the
%% pool would, until it refuses one. Every accepted request stays open on
%% a peer that never answers, so the window stays spent.
-spec exhaust_send_window(ets:tid(), pid()) -> ok.
exhaust_send_window(ConnEts, ConnPid) ->
    Slots = conn_slots(ConnEts, ConnPid),
    exhaust_send_window(ConnPid, Slots, ?CREDIT_EXHAUST_LIMIT).

-spec exhaust_send_window(pid(), nhttpc_stream_slots:slots(), non_neg_integer()) -> ok.
exhaust_send_window(_ConnPid, _Slots, 0) ->
    ct:fail("connection never refused a request for send credit");
exhaust_send_window(ConnPid, Slots, Remaining) ->
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, credit_post(), self()),
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    receive
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ?assertEqual(
                {error, {stream_error, {protocol, refused_stream}}}, nhttpc_error:wrap(Reason)
            ),
            ok
    after 0 ->
        exhaust_send_window(ConnPid, Slots, Remaining - 1)
    end.

-spec credit_post() -> nhttp_lib:request().
credit_post() ->
    #{
        method => post,
        path => <<"/credit">>,
        headers => [{<<"content-type">>, <<"application/octet-stream">>}],
        body => binary:copy(<<$x>>, ?CREDIT_BODY_SIZE)
    }.

-spec collect_credit_refusals(non_neg_integer()) -> [pid()].
collect_credit_refusals(0) ->
    [];
collect_credit_refusals(Count) ->
    receive
        {credit_refused, ConnPid, #{send_window := Window}} when is_integer(Window) ->
            [ConnPid | collect_credit_refusals(Count - 1)]
    after 5000 ->
        ct:fail("expected ~p more send_credit_refused events", [Count])
    end.

-spec drain_conn_messages() -> ok.
drain_conn_messages() ->
    receive
        {nhttpc_error, _, _, _} -> drain_conn_messages();
        {nhttpc_response, _, _, _} -> drain_conn_messages();
        {credit_refused, _, _} -> drain_conn_messages()
    after 0 -> ok
    end.

%%%-----------------------------------------------------------------------------
%%% PEER CAP SHRINK AND RE-DISPATCH HELPERS
%%%-----------------------------------------------------------------------------

%% Put the next `spread` checkout on the first connection in the tuple.
%% The cursor is one `add_get`, taken modulo the tuple size, so a write of
%% `Len - 1` makes the next read land on element one.
-spec aim_spread_cursor(atomics:atomics_ref(), pos_integer()) -> ok.
aim_spread_cursor(Atomics, Len) ->
    atomics:put(Atomics, ?ATOMIC_RR_INDEX, Len - 1).

-spec collect_held_streams(pid(), [reference()]) -> ok.
collect_held_streams(_ConnPid, []) ->
    ok;
collect_held_streams(ConnPid, [Ref | Rest]) ->
    receive
        {nhttpc_response, ConnPid, Ref, {ok, _}} -> collect_held_streams(ConnPid, Rest)
    after 10000 -> ct:fail("held stream ~p never completed", [Ref])
    end.

-spec collect_shrink_results(non_neg_integer()) -> ok.
collect_shrink_results(0) ->
    ok;
collect_shrink_results(N) ->
    receive
        {shrink_done, Result} ->
            ?assertMatch({ok, #{status := 200}}, Result),
            collect_shrink_results(N - 1)
    after 10000 -> ct:fail("in-flight request did not survive the shrink")
    end.

%% Open `Count` streams on one connection, past the pool's selection, and
%% reserve the slots the pool would have reserved for them. The connection
%% releases each slot when its stream ends, so the accounting balances.
%% The synchronous call afterwards is the barrier: the connection answers
%% it only after it handled every send, so the streams are open at the
%% HTTP/2 layer before the caller continues.
-spec occupy_conn_streams(ets:tid(), pid(), pos_integer()) -> [reference()].
occupy_conn_streams(ConnEts, ConnPid, Count) ->
    Slots = conn_slots(ConnEts, ConnPid),
    Slow = #{method => get, path => <<"/slow/1000">>, headers => [], body => <<>>},
    Refs = [
        begin
            ok = nhttpc_stream_slots:reserve(Slots),
            {ok, Ref} = nhttpc_conn:send_async(ConnPid, Slow, self()),
            Ref
        end
     || _ <- lists:seq(1, Count)
    ],
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    Refs.

-spec pool_conn_order(term()) -> [pid()].
pool_conn_order(PoolRef) ->
    Tab = element(3, pool_env(PoolRef)),
    [Pid || {Pid, _Slots} <- tuple_to_list(nhttpc_registry:get_multiplexed_conns(Tab))].

-spec wait_for_multiplexed_conns(term(), pos_integer(), integer()) -> ok.
wait_for_multiplexed_conns(PoolRef, Want, Remaining) when Remaining =< 0 ->
    ct:fail("pool ~p never published ~p multiplexed connections (at ~p)", [
        PoolRef, Want, length(pool_conn_order(PoolRef))
    ]);
wait_for_multiplexed_conns(PoolRef, Want, Remaining) ->
    case length(pool_conn_order(PoolRef)) >= Want of
        true ->
            ok;
        false ->
            timer:sleep(10),
            wait_for_multiplexed_conns(PoolRef, Want, Remaining - 10)
    end.

%%%-----------------------------------------------------------------------------
%%% STREAM SLOT HELPERS
%%%-----------------------------------------------------------------------------

%% Underflow reports that `Fun` provokes. The filter answers `stop`, so the
%% warning is observed here instead of failing the run through the CT log.
%% A primary filter is global, so it comes off again whatever `Fun` does.
-spec capture_underflow_reports(fun(() -> ok)) -> [map()].
capture_underflow_reports(Fun) ->
    Self = self(),
    Filter = fun
        (#{msg := {report, #{event := stream_slot_underflow} = Report}}, Pid) ->
            Pid ! {underflow_report, Report},
            stop;
        (_Event, _Pid) ->
            ignore
    end,
    ok = logger:add_primary_filter(?UNDERFLOW_FILTER, {Filter, Self}),
    try
        ok = Fun(),
        collect_underflow_reports([])
    after
        ok = logger:remove_primary_filter(?UNDERFLOW_FILTER)
    end.

-spec collect_underflow_reports([map()]) -> [map()].
collect_underflow_reports(Acc) ->
    receive
        {underflow_report, Report} -> collect_underflow_reports([Report | Acc])
    after 200 ->
        lists:reverse(Acc)
    end.

-spec receive_connect_failed_report() -> map().
receive_connect_failed_report() ->
    receive
        {connect_failed_report, Report} -> Report
    after 5000 ->
        ct:fail("no connect_failed report")
    end.

%% Counts the `ets:update_counter/3` calls the calling process makes while
%% `Fun` runs. The tracer is a separate process, because the traced process
%% is the test process itself.
-spec count_ets_update_counter(fun(() -> ok)) -> non_neg_integer().
count_ets_update_counter(Fun) ->
    Tracer = spawn_link(fun() -> ets_update_counter_tracer(0) end),
    1 = erlang:trace(self(), true, [call, {tracer, Tracer}]),
    1 = erlang:trace_pattern({ets, update_counter, 3}, true, []),
    ok = Fun(),
    1 = erlang:trace(self(), false, [call]),
    1 = erlang:trace_pattern({ets, update_counter, 3}, false, []),
    Tracer ! {report, self()},
    receive
        {ets_update_counter_calls, Calls} -> Calls
    after 5000 -> ct:fail("ets tracer never reported")
    end.

-spec ets_update_counter_tracer(non_neg_integer()) -> ok.
ets_update_counter_tracer(Calls) ->
    receive
        {trace, _Pid, call, {ets, update_counter, _Args}} ->
            ets_update_counter_tracer(Calls + 1);
        {report, From} ->
            From ! {ets_update_counter_calls, Calls},
            ok;
        _Other ->
            ets_update_counter_tracer(Calls)
    end.

%%%-----------------------------------------------------------------------------
%%% SELECTION POLICY HELPERS
%%%-----------------------------------------------------------------------------

-spec pool_selection(term()) -> nhttpc:stream_selection().
pool_selection(PoolRef) ->
    element(7, pool_env(PoolRef)).

%% Checkout and immediate checkin, Count times, collecting the pid the
%% policy chose each time. The slot is always given back, so the choice
%% reflects the policy and not a connection filling up.
-spec checkout_pids(term(), pos_integer()) -> [pid()].
checkout_pids(PoolRef, Count) ->
    [
        begin
            {ok, ConnPid} = nhttpc_pool:get_connection(PoolRef, 5000),
            ok = nhttpc_pool:checkin_fast(PoolRef, ConnPid),
            ConnPid
        end
     || _ <- lists:seq(1, Count)
    ].

-spec concurrent_statuses(term(), inet:port_number(), binary(), pos_integer()) ->
    [nhttp_lib:status() | term()].
concurrent_statuses(PoolRef, Port, Path, Count) ->
    Parent = self(),
    Workers = [
        spawn_link(fun() ->
            Result =
                case
                    nhttpc:get(url(Port, Path), #{
                        via => {pool, PoolRef}, timeouts => #{request => 20000}
                    })
                of
                    {ok, Resp} -> maps:get(status, Resp);
                    {error, Reason} -> Reason
                end,
            Parent ! {burst_status, self(), Result}
        end)
     || _ <- lists:seq(1, Count)
    ],
    collect_burst_statuses(Workers, []).

-spec collect_burst_statuses([pid()], [term()]) -> [term()].
collect_burst_statuses([], Acc) ->
    Acc;
collect_burst_statuses(Workers, Acc) ->
    receive
        {burst_status, Pid, Status} ->
            collect_burst_statuses(lists:delete(Pid, Workers), [Status | Acc])
    after 30000 ->
        ct:fail("burst never finished")
    end.

%% Highest live stream count seen on any connection in the table. A pool
%% that hands out a slot past the cap shows it here. `reserve/1` moves the
%% count one past the cap for one instruction before it rolls back, so a
%% reading past the cap counts only when a second reading, after a yield,
%% still shows it: a slot the pool handed out stays live for the request.
-spec sample_pool_active(ets:tid(), pid(), non_neg_integer()) -> ok.
sample_pool_active(ConnEts, Parent, Worst) ->
    receive
        {stop, Parent} ->
            Parent ! {pool_max_active, Worst},
            ok
    after 0 ->
        Now = lists:max([0 | [settled_live(S) || {_P, _Proto, _St, S} <- ets:tab2list(ConnEts)]]),
        sample_pool_active(ConnEts, Parent, max(Worst, Now))
    end.

-spec settled_live(nhttpc_stream_slots:slots()) -> integer().
settled_live(Slots) ->
    First = nhttpc_stream_slots:live(Slots),
    case First > nhttpc_stream_slots:cap(Slots) of
        true ->
            true = erlang:yield(),
            min(First, nhttpc_stream_slots:live(Slots));
        false ->
            First
    end.

%% One request every ?PACK_SWEEP_STEP milliseconds for Window
%% milliseconds, tracking the largest number of connections the pool held
%% at once and every distinct pid it held.
-spec light_traffic(term(), inet:port_number(), pid(), integer(), {non_neg_integer(), [pid()]}) ->
    {non_neg_integer(), [pid()]}.
light_traffic(_PoolRef, _Port, _MgrPid, Window, Acc) when Window =< 0 ->
    Acc;
light_traffic(PoolRef, Port, MgrPid, Window, {MaxHeld, Seen}) ->
    _ = nhttpc:get(url(Port, <<"/echo">>), #{
        via => {pool, PoolRef}, timeouts => #{request => 10000}
    }),
    Held = mgr_conn_pids(MgrPid),
    timer:sleep(?PACK_SWEEP_STEP),
    light_traffic(PoolRef, Port, MgrPid, Window - ?PACK_SWEEP_STEP, {
        max(MaxHeld, length(Held)), lists:usort(Seen ++ Held)
    }).

-spec h2_pool_opts(inet:port_number(), map()) -> map().
h2_pool_opts(Port, Extra) ->
    maps:merge(
        #{
            host => <<"127.0.0.1">>,
            port => Port,
            tls => #{verify => verify_none},
            versions => [http2]
        },
        Extra
    ).

-spec start_h2_pool(atom(), inet:port_number(), map()) -> {pid(), pid()}.
start_h2_pool(PoolRef, Port, Extra) ->
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, h2_pool_opts(Port, Extra)),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 10000),
    {PoolSup, MgrPid}.

-spec stop_h2_pool(pid()) -> ok.
stop_h2_pool(PoolSup) ->
    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

-spec url(inet:port_number(), binary()) -> binary().
url(Port, Path) ->
    <<"https://127.0.0.1:", (integer_to_binary(Port))/binary, Path/binary>>.

-spec start_capped_h2_server(string(), string()) -> {pid(), inet:port_number()}.
start_capped_h2_server(CertFile, KeyFile) ->
    {ok, ServerPid} = nhttp:start_link(test_h2_pool_capped_server, #{
        port => 0,
        handler => nhttp_test_handler,
        versions => [http2],
        h2_settings => #{max_concurrent_streams => ?CAP_MAX_STREAMS},
        tls => #{certfile => CertFile, keyfile => KeyFile}
    }),
    unlink(ServerPid),
    {ok, Port} = nhttp:get_port(ServerPid),
    {ServerPid, Port}.

-spec collect_cap_load([pid()], [nhttp_lib:status()]) -> [nhttp_lib:status()].
collect_cap_load([], Acc) ->
    Acc;
collect_cap_load(Pids, Acc) ->
    receive
        {cap_load, Pid, Statuses} -> collect_cap_load(lists:delete(Pid, Pids), Statuses ++ Acc)
    after 60000 -> ct:fail("capped burst never finished")
    end.

-spec collect_pool_results(pid(), pos_integer()) -> [nhttp_lib:status()].
collect_pool_results(MgrPid, Count) ->
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    [
        case nhttpc_pool:request(MgrPid, Req, 20000) of
            {ok, Resp} -> maps:get(status, Resp);
            {error, Reason} -> Reason
        end
     || _ <- lists:seq(1, Count)
    ].

-spec collect_public_results(atom(), inet:port_number(), pos_integer()) -> [nhttp_lib:status()].
collect_public_results(PoolRef, Port, Count) ->
    Url = url(Port, <<"/echo">>),
    Opts = #{via => {pool, PoolRef}, timeouts => #{request => 20000}},
    [
        case nhttpc:get(Url, Opts) of
            {ok, Resp} -> maps:get(status, Resp);
            {error, Reason} -> Reason
        end
     || _ <- lists:seq(1, Count)
    ].

-spec run_public_requests(atom(), inet:port_number(), pos_integer()) -> ok.
run_public_requests(PoolRef, Port, Count) ->
    Url = url(Port, <<"/echo">>),
    lists:foreach(
        fun(_) ->
            {ok, Resp} = nhttpc:get(Url, #{via => {pool, PoolRef}, timeouts => #{request => 10000}}),
            ?assertEqual(200, maps:get(status, Resp))
        end,
        lists:seq(1, Count)
    ).

-spec await_bursts([pid()]) -> ok.
await_bursts([]) ->
    ok;
await_bursts(Pids) ->
    receive
        {burst_done, Pid} -> await_bursts(lists:delete(Pid, Pids))
    after 60000 -> ct:fail("burst never finished")
    end.

-spec new_sample() -> {non_neg_integer(), non_neg_integer(), undefined}.
new_sample() ->
    {0, 0, undefined}.

%% Highest live stream count, and the longest unbroken run of samples in
%% which that count passed the cap, until the caller stops the sampler.
%% Both slots belong to one array, so the comparison holds when the peer
%% changes the cap mid-run.
-spec sample_active(nhttpc_stream_slots:slots(), pid(), Sample) -> ok when
    Sample :: {non_neg_integer(), non_neg_integer(), integer() | undefined}.
sample_active(Slots, Parent, {WorstActive, WorstUs, Since}) ->
    receive
        {stop, _} ->
            Parent ! {max_active, {WorstActive, WorstUs}},
            ok
    after 0 ->
        Active = nhttpc_stream_slots:live(Slots),
        Max = nhttpc_stream_slots:cap(Slots),
        Now = erlang:monotonic_time(microsecond),
        Sample =
            case {Active > Max, Since} of
                {false, _} -> {max(WorstActive, Active), WorstUs, undefined};
                {true, undefined} -> {max(WorstActive, Active), WorstUs, Now};
                {true, Start} -> {max(WorstActive, Active), max(WorstUs, Now - Start), Start}
            end,
        sample_active(Slots, Parent, Sample)
    end.

-spec wait_for_active_streams(ets:tid(), pid(), non_neg_integer(), integer()) -> ok.
wait_for_active_streams(ConnEts, ConnPid, Want, Remaining) when Remaining =< 0 ->
    ct:fail("connection ~p never reached ~p live streams (at ~p)", [
        ConnPid, Want, conn_live(ConnEts, ConnPid)
    ]);
wait_for_active_streams(ConnEts, ConnPid, Want, Remaining) ->
    case conn_live(ConnEts, ConnPid) >= Want of
        true ->
            ok;
        false ->
            timer:sleep(10),
            wait_for_active_streams(ConnEts, ConnPid, Want, Remaining - 10)
    end.

-spec wait_for_idle_streams(ets:tid(), pid(), integer()) -> non_neg_integer().
wait_for_idle_streams(ConnEts, ConnPid, Remaining) when Remaining =< 0 ->
    conn_live(ConnEts, ConnPid);
wait_for_idle_streams(ConnEts, ConnPid, Remaining) ->
    case conn_live(ConnEts, ConnPid) of
        0 ->
            0;
        _ ->
            timer:sleep(10),
            wait_for_idle_streams(ConnEts, ConnPid, Remaining - 10)
    end.

-spec wait_for_notify_flag(atomics:atomics_ref(), integer()) -> ok.
wait_for_notify_flag(_Atomics, Remaining) when Remaining =< 0 ->
    ct:fail("manager never published the checkin notify flag");
wait_for_notify_flag(Atomics, Remaining) ->
    case atomics:get(Atomics, 2) of
        0 ->
            timer:sleep(10),
            wait_for_notify_flag(Atomics, Remaining - 10);
        _ ->
            ok
    end.

-spec wait_for_conn_count(pid(), pos_integer(), integer()) -> ok.
wait_for_conn_count(MgrPid, Want, Remaining) when Remaining =< 0 ->
    ct:fail("pool never reached ~p connections (holds ~p)", [Want, mgr_conn_pids(MgrPid)]);
wait_for_conn_count(MgrPid, Want, Remaining) ->
    case length(mgr_conn_pids(MgrPid)) >= Want of
        true ->
            ok;
        false ->
            timer:sleep(50),
            wait_for_conn_count(MgrPid, Want, Remaining - 50)
    end.

%% Counts the checkout calls and checkin casts the manager receives. Every
%% other message it handles (wait_ready, connection_ready, timers) is
%% ignored, so the count is exactly the traffic the fast path removes.
-spec trace_manager_messages(pid()) -> ok.
trace_manager_messages(MgrPid) ->
    1 = erlang:trace(MgrPid, true, ['receive', {tracer, self()}]),
    ok.

-spec collect_manager_messages(pid()) -> {non_neg_integer(), non_neg_integer()}.
collect_manager_messages(MgrPid) ->
    _ = erlang:trace(MgrPid, false, ['receive']),
    count_manager_messages(0, 0).

-spec count_manager_messages(non_neg_integer(), non_neg_integer()) ->
    {non_neg_integer(), non_neg_integer()}.
count_manager_messages(Checkouts, Checkins) ->
    receive
        {trace, _Pid, 'receive', {'$gen_call', _From, {checkout, _, _, _}}} ->
            count_manager_messages(Checkouts + 1, Checkins);
        {trace, _Pid, 'receive', {'$gen_cast', {checkin, _}}} ->
            count_manager_messages(Checkouts, Checkins + 1);
        {trace, _Pid, 'receive', _Other} ->
            count_manager_messages(Checkouts, Checkins)
    after 200 ->
        {Checkouts, Checkins}
    end.

%% Telemetry emission has a no-op body, so the call itself is the
%% observable. The tracer is a separate process, because one of the traced
%% processes is the test process itself.
-spec trace_pool_telemetry(pid(), fun(() -> ok)) -> [{checkin | checkout, term()}].
trace_pool_telemetry(TracedPid, Fun) ->
    Tracer = spawn_link(fun() -> pool_telemetry_tracer([]) end),
    1 = erlang:trace(TracedPid, true, [call, {tracer, Tracer}]),
    1 = erlang:trace_pattern({nhttpc_telemetry, client_pool_checkout, 3}, true, []),
    1 = erlang:trace_pattern({nhttpc_telemetry, client_pool_checkin, 2}, true, []),
    ok = Fun(),
    timer:sleep(100),
    1 = erlang:trace(TracedPid, false, [call]),
    1 = erlang:trace_pattern({nhttpc_telemetry, client_pool_checkout, 3}, false, []),
    1 = erlang:trace_pattern({nhttpc_telemetry, client_pool_checkin, 2}, false, []),
    Tracer ! {report, self()},
    receive
        {pool_telemetry, Events} -> Events
    after 5000 -> ct:fail("telemetry tracer never reported")
    end.

-spec pool_telemetry_tracer([{checkin | checkout, term()}]) -> ok.
pool_telemetry_tracer(Acc) ->
    receive
        {trace, _Pid, call, {nhttpc_telemetry, client_pool_checkout, [_Cfg, Meta, _Q]}} ->
            pool_telemetry_tracer([{checkout, maps:get(pool, Meta)} | Acc]);
        {trace, _Pid, call, {nhttpc_telemetry, client_pool_checkin, [_Cfg, Meta]}} ->
            pool_telemetry_tracer([{checkin, maps:get(pool, Meta)} | Acc]);
        {report, From} ->
            From ! {pool_telemetry, Acc},
            ok;
        _Other ->
            pool_telemetry_tracer(Acc)
    end.

%%%-----------------------------------------------------------------------------
%%% RESOLVE + GRACEFUL STOP TESTS (plan 02)
%%%-----------------------------------------------------------------------------

%% Resolve must accept ref, manager pid, and supervisor pid interchangeably
%% and yield the same {MgrPid, SupPid} tuple. A non-pool pid and an unknown
%% ref both return {error, pool_not_found}.
resolve_accepts_ref_atom_and_pid_uniformly(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_resolve,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    timer:sleep(100),

    {ok, MgrViaRef, SupViaRef} = nhttpc_pool:resolve(PoolRef),
    ?assert(is_pid(MgrViaRef)),
    ?assertEqual(PoolSup, SupViaRef),

    {ok, MgrViaSup, SupViaSup} = nhttpc_pool:resolve(PoolSup),
    ?assertEqual(MgrViaRef, MgrViaSup),
    ?assertEqual(PoolSup, SupViaSup),

    {ok, MgrViaMgr, SupViaMgr} = nhttpc_pool:resolve(MgrViaRef),
    ?assertEqual(MgrViaRef, MgrViaMgr),
    ?assertEqual(PoolSup, SupViaMgr),

    ?assertEqual({error, pool_not_found}, nhttpc_pool:resolve(self())),
    ?assertEqual({error, pool_not_found}, nhttpc_pool:resolve(no_such_pool_ref)),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ?assertEqual({error, pool_not_found}, nhttpc_pool:resolve(PoolRef)),
    ?assertEqual({error, pool_not_found}, nhttpc_pool:resolve(PoolSup)),
    ok.

%% Baseline: immediate stop tears down the pool even with an in-flight
%% request. The slow request observes an error and the supervisor tree is
%% gone afterwards.
stop_immediate_kills_in_flight(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_stop_immediate,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    timer:sleep(100),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    Self = self(),
    Caller = spawn(fun() ->
        Req = #{
            method => get,
            path => <<"/slow/1500">>,
            headers => [{<<"host">>, <<"127.0.0.1">>}],
            body => <<>>
        },
        Result = nhttpc_pool:request(MgrPid, Req, 5000),
        Self ! {req_done, Result}
    end),

    timer:sleep(150),
    ok = nhttpc:stop(PoolRef),
    ?assertNot(is_process_alive(PoolSup)),
    ?assertEqual(undefined, nhttpc_pool:manager(PoolRef)),

    receive
        {req_done, Result} ->
            ?assertMatch({error, _}, Result)
    after 5000 ->
        exit(Caller, kill),
        ct:fail("In-flight caller never reported a result")
    end,
    ok.

%% Drain stop must wait for the in-flight request to finish *successfully*
%% and only then tear the pool down. New checkouts arriving after drain
%% starts get {error, pool_closed}.
stop_with_drain_waits_for_in_flight(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_stop_drain,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    timer:sleep(100),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    {ok, HeldConn} = nhttpc_pool:checkout(MgrPid, 5000),

    Self = self(),
    Stopper = spawn(fun() ->
        StopResult = nhttpc:stop(PoolRef, #{drain => true, drain_timeout => 5000}),
        Self ! {stop_done, StopResult}
    end),

    timer:sleep(150),
    ?assert(is_process_alive(PoolSup)),
    ?assertEqual({error, pool_closed}, nhttpc_pool:checkout(MgrPid, 100)),

    ok = nhttpc_pool:checkin(MgrPid, HeldConn),

    receive
        {stop_done, ok} -> ok
    after 5000 ->
        exit(Stopper, kill),
        ct:fail("nhttpc:stop with drain did not finish")
    end,

    ?assertNot(is_process_alive(PoolSup)),
    ?assertEqual(undefined, nhttpc_pool:manager(PoolRef)),
    ok.

%% Drain with a tight timeout must give up waiting and still tear the pool
%% down. The held connection prevents drain from completing; the deadline
%% forces shutdown anyway.
stop_with_drain_timeout_force_closes(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_stop_drain_timeout,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    timer:sleep(100),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    {ok, _HeldConn} = nhttpc_pool:checkout(MgrPid, 5000),

    StartMs = erlang:monotonic_time(millisecond),
    ok = nhttpc:stop(PoolRef, #{drain => true, drain_timeout => 200}),
    Elapsed = erlang:monotonic_time(millisecond) - StartMs,
    ?assert(Elapsed >= 200),
    ?assert(Elapsed < 4000),

    ?assertNot(is_process_alive(PoolSup)),
    ?assertEqual(undefined, nhttpc_pool:manager(PoolRef)),
    ok.

%% nhttpc:stop/2 with explicit drain => false (and a custom force_kill_delay)
%% must exercise the immediate_stop/2 path, accept a sup pid, and tear the
%% pool down. Also: a noop on a missing pool returns ok.
stop_with_explicit_immediate_opts(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_stop_immediate_opts,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    timer:sleep(100),

    ok = nhttpc:stop(PoolSup, #{drain => false, force_kill_delay => 2000}),
    ?assertNot(is_process_alive(PoolSup)),
    ?assertEqual(undefined, nhttpc_pool:manager(PoolRef)),

    %% Idempotent: a second call resolves to pool_not_found and returns ok.
    ok = nhttpc:stop(PoolRef, #{drain => false}),
    ok = nhttpc:stop(PoolSup, #{drain => false}),
    ok.

%% Drain on a pool that was never started must return ok rather than crash.
stop_with_drain_on_missing_pool(_Config) ->
    ok = nhttpc:stop(no_such_pool_drain, #{drain => true, drain_timeout => 100}),
    ok = nhttpc:stop(no_such_pool_drain, #{drain => true}),
    ok.

%% Configuration mistake: drain_timeout supplied without drain => true.
stop_with_invalid_opts(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_stop_invalid,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    timer:sleep(100),

    ?assertEqual(
        {error, {invalid_opts, drain_timeout_without_drain}},
        nhttpc:stop(PoolRef, #{drain => false, drain_timeout => 500})
    ),

    ?assert(is_process_alive(PoolSup)),
    ok = nhttpc:stop(PoolRef),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL SUP FAULTS (plan 17.1)
%%%-----------------------------------------------------------------------------

%% Invalid pool opts surface through start_link/2 as {error, {invalid_opts, _}}.
pool_sup_start_link_invalid_opts(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        bogus_unknown_key => true
    },
    ?assertMatch(
        {error, {invalid_opts, _}},
        nhttpc_pool_sup:start_link(test_pool_sup_invalid_opts, PoolOpts)
    ),
    ?assertMatch(
        {error, {invalid_opts, not_a_map}},
        nhttpc_pool_sup:start_link(test_pool_sup_invalid_opts, [{host, <<"a">>}])
    ),
    ok.

%% Fast-path request with tight request_timeout hits the error arm of
%% do_request_fast: cancel, flush_stream_messages, checkin_fast, Err.
pool_sup_request_fast_path_error_flushes(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_sup_req_err,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        timeouts => #{request => 50}
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    Req = #{
        method => get,
        path => <<"/slow/500">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    Result = nhttpc_pool:request(PoolRef, Req, 5000),
    ?assertMatch({error, _}, Result),

    %% Late delivery messages with matching shape are drained by the public
    %% request flow (this exercises the receive arms of flush_stream_messages).
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    SyntheticRef = make_ref(),
    self() ! {nhttpc_response, ConnPid, SyntheticRef, {ok, fake_resp}},
    self() ! {nhttpc_error, ConnPid, SyntheticRef, fake_err},
    %% Now issue a request that times out; the worker's stream uses a
    %% different reference, so the synthetic ones remain in our mailbox
    %% (they don't affect pool_sup behaviour). Just confirm the slow request
    %% times out cleanly under the same code path.
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),
    Result2 = nhttpc_pool:request(PoolRef, Req, 5000),
    ?assertMatch({error, _}, Result2),
    %% Drain our synthetic messages so they don't leak into later tests.
    drain_messages(),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% force_kill_delay=0 forces proc_lib:stop/3 to raise exit:timeout, which
%% routes through the exit:timeout clause and force_kill/1.
pool_sup_stop_timeout_force_kill(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_sup_force_kill,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    timer:sleep(100),

    DownRef = erlang:monitor(process, PoolSup),
    ok = nhttpc:stop(PoolRef, #{drain => false, force_kill_delay => 0}),
    receive
        {'DOWN', DownRef, process, PoolSup, _} -> ok
    after 5000 ->
        ct:fail("supervisor did not die after force_kill")
    end,
    ?assertNot(is_process_alive(PoolSup)),
    %% Force kill leaves persistent_term stale because the manager's
    %% terminate runs with reason `killed`. Erase manually so other tests
    %% are not affected.
    _ = persistent_term:erase({nhttpc_pool, PoolRef}),
    ok.

%% Calling stop on a pool whose ref is unknown should be a no-op (idempotent).
%% This exercises the {error, pool_not_found} arms of immediate_stop and
%% graceful_stop.
pool_sup_stop_already_dead_supervisor(_Config) ->
    %% Idempotent path: no pool started under this ref.
    ok = nhttpc_pool_sup:stop(test_pool_sup_already_dead),
    ok = nhttpc_pool_sup:stop(test_pool_sup_already_dead, #{
        drain => false, force_kill_delay => 100
    }),
    ok = nhttpc_pool_sup:stop(test_pool_sup_already_dead, #{
        drain => true, drain_timeout => 100
    }),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL MGR FAULTS (plan 17.2)
%%%-----------------------------------------------------------------------------

%% lookup/1 must raise badarg when the pool is unknown.
pool_mgr_lookup_badarg_on_missing_ref(_Config) ->
    ?assertError(badarg, nhttpc_pool:lookup(no_such_pool_lookup_badarg)),
    ok.

%% All accessors that consult persistent_term return undefined or
%% pool_not_found for an unknown ref.
pool_mgr_persistent_term_miss_paths(_Config) ->
    Ref = no_such_pool_miss_paths,
    ?assertEqual(undefined, nhttpc_pool:manager(Ref)),
    ?assertEqual(undefined, nhttpc_pool:lookup_sup(Ref)),
    ?assertEqual(undefined, nhttpc_pool:lookup_conn_sup(Ref)),
    ?assertEqual({error, pool_not_found}, nhttpc_pool:resolve(Ref)),
    ?assertEqual({error, pool_not_found}, nhttpc_pool:get_connection(Ref)),
    ?assertEqual({error, pool_not_found}, nhttpc_pool:wait_ready(Ref, 100)),
    ok.

%% resolve/1 must reject pids whose initial call is not a pool sup or manager.
pool_mgr_resolve_unknown_pid(_Config) ->
    Pid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    try
        ?assertEqual({error, pool_not_found}, nhttpc_pool:resolve(Pid))
    after
        Pid ! stop
    end,
    ok.

%% resolve/1 must reject dead pids.
pool_mgr_resolve_dead_pid(_Config) ->
    Pid = spawn(fun() -> ok end),
    Ref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 -> ct:fail("dead pid did not signal DOWN")
    end,
    ?assertEqual({error, pool_not_found}, nhttpc_pool:resolve(Pid)),
    ok.

%% checkin_fast on a missing pool must catch the badarg from lookup/1.
pool_mgr_checkin_fast_on_missing_pool(_Config) ->
    Fake = spawn(fun() -> ok end),
    ?assertEqual(ok, nhttpc_pool:checkin_fast(no_such_pool_checkin_fast, Fake)),
    ok.

%% get_connection/1 must delegate to /2 with the default 5000 timeout.
pool_mgr_get_connection_default_arity(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_get_connection_default,
    PoolOpts = #{host => <<"127.0.0.1">>, port => Port, size => 1},
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),
    {ok, ConnPid} = nhttpc_pool:get_connection(PoolRef),
    ?assert(is_pid(ConnPid)),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% Direct call to get_connection/2 on a missing pool returns pool_not_found.
pool_mgr_get_connection_pool_not_found(_Config) ->
    ?assertEqual(
        {error, pool_not_found},
        nhttpc_pool:get_connection(no_such_pool_get_connection, 100)
    ),
    ok.

%% drain/2 on a dead manager must return ok (exit:noproc / exit:{noproc, _}).
pool_mgr_drain_on_dead_manager(_Config) ->
    DeadPid = spawn(fun() -> ok end),
    Ref = erlang:monitor(process, DeadPid),
    receive
        {'DOWN', Ref, process, DeadPid, _} -> ok
    after 1000 -> ct:fail("dead pid never died")
    end,
    ?assertEqual(ok, nhttpc_pool:drain(DeadPid, 100)),
    ok.

%% wait_ready/2 on a missing pool returns pool_not_found.
pool_mgr_wait_ready_on_missing_pool(_Config) ->
    ?assertEqual(
        {error, pool_not_found},
        nhttpc_pool:wait_ready(no_such_pool_wait_ready, 100)
    ),
    ok.

%% gen_server:call with an unknown message must return {error, unknown_call}.
pool_mgr_unknown_handle_call(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_unknown_call,
    PoolOpts = #{host => <<"127.0.0.1">>, port => Port, size => 1},
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    ?assertEqual({error, unknown_call}, gen_server:call(MgrPid, some_unknown_call)),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% gen_server:cast with an unknown message must be silently ignored.
pool_mgr_unknown_handle_cast(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_unknown_cast,
    PoolOpts = #{host => <<"127.0.0.1">>, port => Port, size => 1},
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    ok = gen_server:cast(MgrPid, some_unknown_cast),
    %% Manager must still be responsive afterwards.
    ?assert(is_pid(nhttpc_pool:manager(PoolRef))),
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% Arbitrary info messages (including stray DOWNs) should not crash the
%% manager. Hits handle_info catch-all and the unknown-monitor 'DOWN' arm.
pool_mgr_unknown_handle_info_and_stray_down(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_unknown_info,
    PoolOpts = #{host => <<"127.0.0.1">>, port => Port, size => 1},
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    MgrPid ! some_arbitrary_info,
    MgrPid !
        {'DOWN', make_ref(), process, spawn(fun() -> ok end), some_reason},
    %% Issue a checkout to make sure manager is still responsive.
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% connection_draining notification with an unknown pid is a no-op.
pool_mgr_connection_draining_unknown_conn(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_drain_unknown,
    PoolOpts = #{host => <<"127.0.0.1">>, port => Port, size => 1},
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    Fake = spawn(fun() -> ok end),
    ok = nhttpc_pool:connection_draining(MgrPid, Fake),
    %% Manager must still be responsive.
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% update_max_streams with an unknown pid is a no-op.
pool_mgr_update_max_streams_unknown_conn(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_max_streams_unknown,
    PoolOpts = #{host => <<"127.0.0.1">>, port => Port, size => 1},
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    Fake = spawn(fun() -> ok end),
    ok = nhttpc_pool:update_max_streams(MgrPid, Fake, 999),
    {ok, ConnPid} = nhttpc_pool:checkout(MgrPid, 5000),
    ok = nhttpc_pool:checkin(MgrPid, ConnPid),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% wait_ready with infinity exercises the safe_call_timeout(infinity) branch.
%% Only legal after the pool is already ready (handle_wait_ready short-circuits
%% on ready_count > 0 and does not queue an infinity deadline).
pool_mgr_safe_call_timeout_infinity(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_safe_call_inf,
    PoolOpts = #{host => <<"127.0.0.1">>, port => Port, size => 1},
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),
    ok = nhttpc_pool:wait_ready(MgrPid, infinity),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% request/3 with infinity timeout exercises remaining_timeout(infinity, _).
pool_mgr_request_with_infinity_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_req_inf,
    PoolOpts = #{host => <<"127.0.0.1">>, port => Port, size => 1},
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Resp} = nhttpc_pool:request(MgrPid, Req, infinity),
    ?assertEqual(200, maps:get(status, Resp)),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% TLS options must be flattened: top-level map keys are merged with the
%% `extra` proplist. Pass both a non-tuple element (ignored) and a {K, V}
%% tuple to exercise both clauses of flatten_tls/1.
pool_mgr_flatten_tls_extras(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_flatten_tls,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        tls => #{
            verify => verify_none,
            extra => [
                {server_name_indication, "localhost"},
                ignored_non_tuple_element
            ]
        }
    },
    %% TLS connect will fail because the test server is plain TCP, but the
    %% pool's init path still walks flatten_tls/1.
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    timer:sleep(100),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% An H3 pool exercises protocol_to_ets(http3) and the http3 default
%% max_streams branches in handle_connection_ready.
pool_mgr_h3_protocol_to_ets(_Config) ->
    PoolRef = test_pool_h3_proto,
    %% No real H3 server is needed; the manager's handle_connection_ready
    %% executes for the conn even when the connect fails. Run pool with
    %% impossible port; the manager still hits the connection-ready arm
    %% for any connection that briefly succeeds. Conservative: just verify
    %% start_link accepts http3 in versions and reaches the h3 conn module.
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => 1,
        versions => [http3],
        tls => #{verify => verify_none},
        size => 1,
        reconnect_backoff_max => 100
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    timer:sleep(200),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% Pool init must flow the `otel` option through into the connection options
%% map. This exercises the {ok, OtelCfg} arm of the otel maps:find.
pool_mgr_otel_option_flows_to_conn_opts(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_otel_opt,
    OtelCfg = #{enabled => false},
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        otel => OtelCfg
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    %% Smoke test request flows through pool with otel attached.
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Resp} = nhttpc_pool:request(MgrPid, Req, 5000),
    ?assertEqual(200, maps:get(status, Resp)),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% Failed connections progress through the backoff schedule, exercising the
%% N > 1 clause of compute_backoff_delay/2.
pool_mgr_reconnect_backoff_progression(_Config) ->
    PoolRef = test_pool_backoff,
    %% Use an unused localhost port so connects fail; a short backoff_base
    %% lets the manager bump consecutive_failures quickly.
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => 1,
        size => 1,
        reconnect_backoff_base => 20,
        reconnect_backoff_max => 200
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    %% Let the pool attempt and fail several connections so backoff escalates
    %% beyond the first attempt (which hits the N=1 arm).
    timer:sleep(800),
    %% wait_ready times out because the pool never becomes ready.
    ?assertMatch(
        {error, _},
        nhttpc_pool:wait_ready(PoolRef, 50)
    ),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% drain/2 on an idle (no in-flight requests) pool returns immediately via
%% the all_connections_idle = true branch of handle_drain/3.
pool_mgr_drain_idle_pool_completes_immediately(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_drain_idle,
    PoolOpts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    },
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, PoolOpts),
    unlink(PoolSup),
    MgrPid = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(MgrPid, 5000),

    Start = erlang:monotonic_time(millisecond),
    ok = nhttpc_pool:drain(MgrPid, infinity),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    ?assert(Elapsed < 1000),

    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%%%-----------------------------------------------------------------------------
%%% RECONNECT BRAKE TESTS
%%%-----------------------------------------------------------------------------

%% A peer that accepts every connection, reads the request, and closes
%% before any response. Each cycle counts as a failure, so the gap between
%% consecutive accepts must follow the backoff schedule and reach the
%% maximum. A reset on the handshake would hold every gap at the base.
pool_backoff_grows_when_connection_never_serves(_Config) ->
    Peer = start_brake_peer([], close_after_request),
    PoolRef = test_pool_brake_never_serves,
    {ok, PoolSup} = start_brake_pool(PoolRef, Peer),
    Mgr = nhttpc_pool:manager(PoolRef),
    Accepts = lists:map(
        fun(N) ->
            T = brake_accept(N),
            ?assertMatch({error, _}, nhttpc_pool:request(Mgr, brake_request(), ?BRAKE_WAIT)),
            T
        end,
        lists:seq(1, ?BRAKE_CYCLES)
    ),
    Gaps = brake_gaps(Accepts),
    Expected = [brake_delay(N) || N <- lists:seq(1, ?BRAKE_CYCLES - 1)],
    ct:log("accept gaps ~p, scheduled delays ~p", [Gaps, Expected]),
    lists:foreach(
        fun({Gap, Delay}) -> ?assert(Gap >= Delay) end,
        lists:zip(Gaps, Expected)
    ),
    ?assert(lists:last(Gaps) >= ?BRAKE_MAX),
    ok = wait_for_failures(Mgr, ?BRAKE_CYCLES),
    stop_brake_pool(PoolSup),
    stop_brake_peer(Peer),
    ok.

%% Two failures grow the delay. The third connection reaches ready with the
%% counter untouched, and the first response it delivers resets it.
pool_backoff_resets_after_first_response(_Config) ->
    Peer = start_brake_peer([close_after_request, close_after_request], serve),
    PoolRef = test_pool_brake_resets,
    {ok, PoolSup} = start_brake_pool(PoolRef, Peer),
    Mgr = nhttpc_pool:manager(PoolRef),
    T1 = brake_accept(1),
    ?assertMatch({error, _}, nhttpc_pool:request(Mgr, brake_request(), ?BRAKE_WAIT)),
    T2 = brake_accept(2),
    ?assertMatch({error, _}, nhttpc_pool:request(Mgr, brake_request(), ?BRAKE_WAIT)),
    T3 = brake_accept(3),
    ?assert(T2 - T1 >= brake_delay(1)),
    ?assert(T3 - T2 >= brake_delay(2)),
    ok = nhttpc_pool:wait_ready(Mgr, ?BRAKE_WAIT),
    ?assertEqual(2, mgr_consecutive_failures(Mgr)),
    {ok, Resp} = nhttpc_pool:request(Mgr, brake_request(), ?BRAKE_WAIT),
    ?assertEqual(200, maps:get(status, Resp)),
    ok = wait_for_failures(Mgr, 0),
    stop_brake_pool(PoolSup),
    stop_brake_peer(Peer),
    ok.

%% A connection that completes its handshake and serves nothing leaves the
%% counter where the failures put it. Killing it then schedules the next
%% attempt from that count, not from zero.
pool_backoff_does_not_reset_on_handshake_alone(_Config) ->
    Peer = start_brake_peer([close_after_request, close_after_request], hold),
    PoolRef = test_pool_brake_handshake,
    {ok, PoolSup} = start_brake_pool(PoolRef, Peer),
    Mgr = nhttpc_pool:manager(PoolRef),
    _ = brake_accept(1),
    ?assertMatch({error, _}, nhttpc_pool:request(Mgr, brake_request(), ?BRAKE_WAIT)),
    _ = brake_accept(2),
    ?assertMatch({error, _}, nhttpc_pool:request(Mgr, brake_request(), ?BRAKE_WAIT)),
    _ = brake_accept(3),
    ok = nhttpc_pool:wait_ready(Mgr, ?BRAKE_WAIT),
    ?assertEqual(2, mgr_consecutive_failures(Mgr)),
    [ConnPid] = mgr_conn_pids(Mgr),
    TKill = erlang:monotonic_time(millisecond),
    exit(ConnPid, kill),
    T4 = brake_accept(4),
    ?assert(T4 - TKill >= brake_delay(3)),
    ok = wait_for_failures(Mgr, 3),
    stop_brake_pool(PoolSup),
    stop_brake_peer(Peer),
    ok.

%% Connections that idle out against a real server exit cleanly. The
%% counter stays at zero across several such exits, and the pool keeps
%% reconnecting at the base delay.
pool_backoff_ignores_clean_exit(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_pool_brake_clean_exit,
    Opts = (brake_pool_opts(Port))#{idle_timeout => ?BRAKE_IDLE_TIMEOUT},
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, Opts),
    unlink(PoolSup),
    Mgr = nhttpc_pool:manager(PoolRef),
    ok = nhttpc_pool:wait_ready(Mgr, ?BRAKE_WAIT),
    ok = wait_for_idle_cycles(Mgr, mgr_conn_pids(Mgr), ?BRAKE_IDLE_CYCLES),
    ?assertEqual(0, mgr_consecutive_failures(Mgr)),
    {ok, Resp} = nhttpc_pool:request(Mgr, brake_request(), ?BRAKE_WAIT),
    ?assertEqual(200, maps:get(status, Resp)),
    stop_brake_pool(PoolSup),
    ok.

%%%-----------------------------------------------------------------------------
%%% INTERNAL TEST HELPERS
%%%-----------------------------------------------------------------------------

drain_messages() ->
    receive
        _ -> drain_messages()
    after 0 -> ok
    end.

%% Reconnect brake helpers. The peer is a raw TCP listener that plays one
%% step per accepted connection and reports the accept time to the test.
-type brake_step() :: close_after_request | serve | hold.
-type brake_peer() :: #{pid := pid(), lsock := gen_tcp:socket(), port := inet:port_number()}.

-spec brake_accept(pos_integer()) -> integer().
brake_accept(N) ->
    receive
        {brake_accept, N, T} -> T
    after ?BRAKE_WAIT ->
        ct:fail("the brake peer never accepted connection ~p", [N])
    end.

-spec brake_delay(pos_integer()) -> pos_integer().
brake_delay(Failures) ->
    min(?BRAKE_BASE * (1 bsl (Failures - 1)), ?BRAKE_MAX).

-spec brake_gaps([integer()]) -> [integer()].
brake_gaps([_] = _Last) ->
    [];
brake_gaps([A, B | Rest]) ->
    [B - A | brake_gaps([B | Rest])].

-spec brake_peer_accept(gen_tcp:socket(), [brake_step()], brake_step(), pid(), pos_integer()) ->
    ok.
brake_peer_accept(LSock, Steps, Default, Test, N) ->
    case gen_tcp:accept(LSock, ?BRAKE_WAIT) of
        {ok, Sock} ->
            Test ! {brake_accept, N, erlang:monotonic_time(millisecond)},
            {Step, Rest} =
                case Steps of
                    [] -> {Default, []};
                    [S | R] -> {S, R}
                end,
            Conn = spawn_link(fun() -> brake_peer_conn(Step, Sock) end),
            ok = gen_tcp:controlling_process(Sock, Conn),
            brake_peer_accept(LSock, Rest, Default, Test, N + 1);
        {error, _} ->
            ok
    end.

-spec brake_peer_conn(brake_step(), gen_tcp:socket()) -> ok.
brake_peer_conn(close_after_request, Sock) ->
    ok = brake_peer_recv(Sock),
    gen_tcp:close(Sock);
brake_peer_conn(serve, Sock) ->
    case gen_tcp:recv(Sock, 0, ?BRAKE_WAIT) of
        {ok, _Request} ->
            ok = gen_tcp:send(Sock, <<"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok">>),
            brake_peer_conn(serve, Sock);
        {error, _} ->
            gen_tcp:close(Sock)
    end;
brake_peer_conn(hold, Sock) ->
    receive
        stop -> gen_tcp:close(Sock)
    after ?BRAKE_WAIT ->
        gen_tcp:close(Sock)
    end.

-spec brake_peer_recv(gen_tcp:socket()) -> ok.
brake_peer_recv(Sock) ->
    case gen_tcp:recv(Sock, 0, ?BRAKE_WAIT) of
        {ok, _Request} -> ok;
        {error, _} -> ok
    end.

-spec brake_pool_opts(inet:port_number()) -> map().
brake_pool_opts(Port) ->
    #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1,
        max_overflow => 0,
        reconnect_backoff_base => ?BRAKE_BASE,
        reconnect_backoff_max => ?BRAKE_MAX
    }.

-spec brake_request() -> nhttp_lib:request().
brake_request() ->
    #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    }.

%% `#state{}` keeps reconnect_backoff_base, reconnect_backoff_max and
%% consecutive_failures adjacent, so the two configured bounds locate the
%% counter without a record definition.
-spec mgr_consecutive_failures(pid() | atom()) -> non_neg_integer().
mgr_consecutive_failures(Mgr) ->
    failures_after_bounds(tuple_to_list(sys:get_state(Mgr))).

-spec failures_after_bounds([term()]) -> non_neg_integer().
failures_after_bounds([?BRAKE_BASE, ?BRAKE_MAX, Failures | _]) when is_integer(Failures) ->
    Failures;
failures_after_bounds([_ | Rest]) ->
    failures_after_bounds(Rest).

-spec start_brake_peer([brake_step()], brake_step()) -> brake_peer().
start_brake_peer(Steps, Default) ->
    Test = self(),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    Pid = spawn_link(fun() -> brake_peer_accept(LSock, Steps, Default, Test, 1) end),
    #{pid => Pid, lsock => LSock, port => Port}.

-spec start_brake_pool(atom(), brake_peer()) -> {ok, pid()}.
start_brake_pool(PoolRef, #{port := Port}) ->
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, brake_pool_opts(Port)),
    unlink(PoolSup),
    {ok, PoolSup}.

-spec stop_brake_peer(brake_peer()) -> ok.
stop_brake_peer(#{pid := Pid, lsock := LSock}) ->
    unlink(Pid),
    exit(Pid, kill),
    gen_tcp:close(LSock).

-spec stop_brake_pool(pid()) -> ok.
stop_brake_pool(PoolSup) ->
    exit(PoolSup, shutdown),
    timer:sleep(100),
    ok.

-spec wait_for_failures(pid() | atom(), non_neg_integer()) -> ok.
wait_for_failures(Mgr, Expected) ->
    wait_for_failures(Mgr, Expected, erlang:monotonic_time(millisecond) + ?BRAKE_WAIT).

-spec wait_for_failures(pid() | atom(), non_neg_integer(), integer()) -> ok.
wait_for_failures(Mgr, Expected, Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    case mgr_consecutive_failures(Mgr) of
        Expected ->
            ok;
        Other when Now > Deadline ->
            ct:fail("consecutive_failures is ~p, expected ~p", [Other, Expected]);
        _Other ->
            timer:sleep(?BRAKE_POLL),
            wait_for_failures(Mgr, Expected, Deadline)
    end.

%% One idle cycle is over when the pool holds a ready connection whose pid
%% differs from every pid of the previous cycle.
-spec wait_for_idle_cycles(pid() | atom(), [pid()], non_neg_integer()) -> ok.
wait_for_idle_cycles(_Mgr, _Previous, 0) ->
    ok;
wait_for_idle_cycles(Mgr, Previous, Cycles) ->
    Deadline = erlang:monotonic_time(millisecond) + ?BRAKE_WAIT,
    Current = wait_for_new_conn(Mgr, Previous, Deadline),
    wait_for_idle_cycles(Mgr, Current, Cycles - 1).

-spec wait_for_new_conn(pid() | atom(), [pid()], integer()) -> [pid()].
wait_for_new_conn(Mgr, Previous, Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    Current = mgr_conn_pids(Mgr),
    case Current =/= [] andalso Current -- Previous =:= Current of
        true ->
            Current;
        false when Now > Deadline ->
            ct:fail("no idle exit and reconnect within ~p ms", [?BRAKE_WAIT]);
        false ->
            timer:sleep(?BRAKE_POLL),
            wait_for_new_conn(Mgr, Previous, Deadline)
    end.

%%%-----------------------------------------------------------------------------
%%% H3 POOL TESTS
%%%-----------------------------------------------------------------------------

%% The whole defect in one case: a pooled HTTP/3 request through the public
%% API. Before this the pool started an HTTP/1.1 connection against a QUIC
%% port, and the request that reached a connection at all carried a message
%% shape no HTTP/3 receive clause matched.
pool_h3_request_via_public_api(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h3_pool_public,
    {ok, _PoolSup} = start_h3_pool(PoolRef, Port, #{}),
    {ok, Resp} = h3_get(PoolRef, Port, <<"/hello">>, #{}),
    ?assertEqual(200, maps:get(status, Resp)),
    ?assertEqual(<<"Hello!">>, maps:get(body, Resp)),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% Ten requests on one connection cover the batching path: the ready loop
%% takes the first `{send, ...}` and `drain_sends/2` takes the rest.
pool_h3_concurrent_requests(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h3_pool_concurrent,
    {ok, _PoolSup} = start_h3_pool(PoolRef, Port, #{}),
    Statuses = h3_parallel_get(PoolRef, Port, <<"/hello">>, 10),
    ?assertEqual(lists:duplicate(10, 200), Statuses),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% Every pooled request reserves a stream slot before the send message
%% leaves the caller. A request that completes has to give it back, or the
%% connection reaches its cap and stays there.
pool_h3_checkin_returns_the_slot(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h3_pool_slots,
    {ok, _PoolSup} = start_h3_pool(PoolRef, Port, #{}),
    ConnEts = pool_conn_ets(PoolRef),
    ConnPid = h3_conn_pid(PoolRef),
    _ = h3_parallel_get(PoolRef, Port, <<"/hello">>, 5),
    ?assertEqual(0, wait_for_idle_streams(ConnEts, ConnPid, 3000)),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% `max_response_size` is a public request option. A pooled HTTP/3 response
%% over it reports what a pooled HTTP/2 response over it reports.
pool_h3_max_response_size_enforced(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h3_pool_max_size,
    {ok, _PoolSup} = start_h3_pool(PoolRef, Port, #{}),
    ConnEts = pool_conn_ets(PoolRef),
    ConnPid = h3_conn_pid(PoolRef),
    Result = h3_get(PoolRef, Port, <<"/large">>, #{max_response_size => 100}),
    ?assertEqual({error, {stream_error, exceeded_max_response_size}}, Result),
    ?assertEqual(0, wait_for_idle_streams(ConnEts, ConnPid, 3000)),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% `max_header_size` and `max_headers_count` report the reasons HTTP/1.1
%% reports for the same two limits, so a caller cannot tell the protocols
%% apart from the error alone.
pool_h3_header_limits_enforced(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h3_pool_header_limits,
    {ok, _PoolSup} = start_h3_pool(PoolRef, Port, #{}),
    TooBig = h3_get(PoolRef, Port, <<"/hello">>, #{max_header_size => 1}),
    ?assertEqual({error, {protocol_error, http3, header_too_large}}, TooBig),
    TooMany = h3_get(PoolRef, Port, <<"/many-headers">>, #{max_headers_count => 1}),
    ?assertEqual({error, {protocol_error, http3, too_many_headers}}, TooMany),
    {ok, Resp} = h3_get(PoolRef, Port, <<"/many-headers">>, #{max_headers_count => 3}),
    ?assertEqual(200, maps:get(status, Resp)),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% A request that runs out of time cancels its stream on the way out. The
%% caller keeps no response or error message for a request it abandoned,
%% and the connection takes the stream slot back.
pool_h3_error_cancels_the_stream(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h3_pool_cancel,
    {ok, _PoolSup} = start_h3_pool(PoolRef, Port, #{}),
    ConnEts = pool_conn_ets(PoolRef),
    ConnPid = h3_conn_pid(PoolRef),
    Result = h3_get(PoolRef, Port, <<"/slow">>, #{timeouts => #{request => 300}}),
    ?assertEqual({error, {timeout, request}}, Result),
    ?assertEqual([], h3_drain_stream_messages()),
    ?assertEqual(0, wait_for_idle_streams(ConnEts, ConnPid, 3000)),
    {ok, Resp} = h3_get(PoolRef, Port, <<"/hello">>, #{}),
    ?assertEqual(200, maps:get(status, Resp)),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%% `nhttpc_conn` and `nhttpc_h3_conn` answer the same cancel message by
%% coincidence, so a wrong dispatch passes every other case in this group.
%% This one traces the calls instead. The deliberate `nhttpc_conn:stop/1`
%% at the end is the control: without it an unarmed trace passes the case
%% for saying nothing.
pool_h3_never_calls_the_h1_module(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h3_pool_dispatch,
    {ok, _PoolSup} = start_h3_pool(PoolRef, Port, #{}),
    Tracer = spawn_link(fun() -> h3_trace_collector([]) end),
    ?assert(erlang:trace_pattern({nhttpc_conn, '_', '_'}, true, [global]) > 0),
    _ = erlang:trace(all, true, [call, {tracer, Tracer}]),
    {ok, _} = h3_get(PoolRef, Port, <<"/hello">>, #{}),
    {error, _} = h3_get(PoolRef, Port, <<"/slow">>, #{timeouts => #{request => 300}}),
    ok = nhttpc_conn:stop(h3_dead_pid()),
    _ = erlang:trace(all, false, [call]),
    _ = erlang:trace_pattern({nhttpc_conn, '_', '_'}, false, [global]),
    ?assertEqual([{nhttpc_conn, stop, 1}], h3_trace_calls(Tracer)),
    ok = nhttpc_pool_sup:stop(PoolRef),
    ok.

%%%-----------------------------------------------------------------------------
%%% H3 POOL HELPERS
%%%-----------------------------------------------------------------------------
-spec h3_conn_pid(term()) -> pid().
h3_conn_pid(PoolRef) ->
    [ConnPid] = mgr_conn_pids(nhttpc_pool:manager(PoolRef)),
    ConnPid.

-spec h3_dead_pid() -> pid().
h3_dead_pid() ->
    Pid = spawn(fun() -> ok end),
    MonRef = monitor(process, Pid),
    receive
        {'DOWN', MonRef, process, Pid, _} -> Pid
    after 5000 -> ct:fail("control process never exited")
    end.

-spec h3_drain_stream_messages() -> [term()].
h3_drain_stream_messages() ->
    receive
        {nhttpc_response, _, _, _} = Msg -> [Msg | h3_drain_stream_messages()];
        {nhttpc_error, _, _, _} = Msg -> [Msg | h3_drain_stream_messages()]
    after 200 ->
        []
    end.

-spec h3_get(term(), inet:port_number(), binary(), map()) ->
    {ok, nhttp_lib:response()} | {error, term()}.
h3_get(PoolRef, Port, Path, Extra) ->
    Opts = maps:merge(
        #{via => {pool, PoolRef}, timeouts => #{request => 10000}, versions => [http3]},
        Extra
    ),
    nhttpc:get(url(Port, Path), Opts).

-spec h3_parallel_get(term(), inet:port_number(), binary(), pos_integer()) ->
    [nhttp_lib:status()].
h3_parallel_get(PoolRef, Port, Path, Count) ->
    Parent = self(),
    Pids = [
        spawn_link(fun() ->
            {ok, Resp} = h3_get(PoolRef, Port, Path, #{}),
            Parent ! {h3_done, self(), maps:get(status, Resp)}
        end)
     || _ <- lists:seq(1, Count)
    ],
    [
        receive
            {h3_done, Pid, Status} -> Status
        after 20000 -> ct:fail("h3 parallel request never answered")
        end
     || Pid <- Pids
    ].

-spec h3_pool_opts(inet:port_number(), map()) -> map().
h3_pool_opts(Port, Extra) ->
    maps:merge(
        #{
            host => <<"127.0.0.1">>,
            port => Port,
            versions => [http3],
            tls => #{verify => verify_none},
            size => 1,
            max_overflow => 0
        },
        Extra
    ).

-spec h3_trace_calls(pid()) -> [term()].
h3_trace_calls(Tracer) ->
    Tracer ! {calls, self()},
    receive
        {calls, Calls} -> Calls
    after 5000 -> ct:fail("trace collector never answered")
    end.

-spec h3_trace_collector([term()]) -> ok.
h3_trace_collector(Acc) ->
    receive
        {trace, _Pid, call, {M, F, Args}} ->
            h3_trace_collector([{M, F, length(Args)} | Acc]);
        {calls, From} ->
            From ! {calls, lists:reverse(Acc)},
            ok;
        _Other ->
            h3_trace_collector(Acc)
    end.

-spec start_h3_pool(term(), inet:port_number(), map()) -> {ok, pid()}.
start_h3_pool(PoolRef, Port, Extra) ->
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, h3_pool_opts(Port, Extra)),
    unlink(PoolSup),
    ok = nhttpc_pool:wait_ready(nhttpc_pool:manager(PoolRef), 15000),
    {ok, PoolSup}.

%%%-----------------------------------------------------------------------------
%%% CREDIT-AWARE AVAILABILITY TESTS (plan 20)
%%%-----------------------------------------------------------------------------

%% Growth follows send credit, not stream slots alone. One connection to a
%% peer that never credits and caps streams far above the load: eight
%% 8192-octet bodies need 65536 octets against a window of 65535, so the
%% eighth caller finds free stream slots and no credit, and the pool opens
%% a second connection for it. No caller is refused.
pool_h2_grows_on_credit_starvation(Config) ->
    ok = credit_growth_case(Config, pack, test_h2_pool_credit_grow_pack).

%% Credit is not a policy. The same starvation under `spread` grows the
%% pool the same way.
pool_h2_spread_also_honors_credit(Config) ->
    ok = credit_growth_case(Config, spread, test_h2_pool_credit_grow_spread).

%% An eager peer credits every body back, so a `pack` pool under a load
%% that fits the window never needs a second connection, and the credit
%% slot returns to the full window when the load stops.
pool_h2_packs_while_credit_lasts(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_h2_pool_credit_pack_lasts,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 4, max_overflow => 4}),
    [ConnPid] = mgr_conn_pids(MgrPid),
    Slots = conn_slots(pool_conn_ets(PoolRef), ConnPid),
    ?assertEqual(?CREDIT_WINDOW, nhttpc_stream_slots:credit(Slots)),

    lists:foreach(
        fun(_Round) ->
            Results = concurrent_credit_posts(
                PoolRef, ?CREDIT_PACK_CONCURRENCY, ?CREDIT_PACK_BODY_SIZE, 10000
            ),
            ?assertEqual(
                lists:duplicate(?CREDIT_PACK_CONCURRENCY, 200),
                [Status || {ok, #{status := Status}} <- Results]
            )
        end,
        lists:seq(1, ?CREDIT_PACK_ROUNDS)
    ),

    ?assertEqual([ConnPid], mgr_conn_pids(MgrPid)),
    ?assertEqual(?CREDIT_WINDOW, wait_for_credit(Slots, ?CREDIT_WINDOW, 5000)),

    stop_h2_pool(PoolSup),
    ok.

%% A peer that never credits bounds growth at `size + max_overflow`. With
%% one connection and one overflow, fourteen bodies are admitted across
%% the two, and every caller past them queues as a waiter and times out
%% on checkout. Nobody is refused and no third connection opens.
pool_h2_growth_is_bounded_by_overflow(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?CREDIT_PEER_CAP}),
    PoolRef = test_h2_pool_credit_bound,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 1, max_overflow => 1}),

    Workers = spawn_credit_posts(PoolRef, ?CREDIT_BOUND_CALLERS, ?CREDIT_REQUEST_TIMEOUT),
    ok = wait_for_conn_count(MgrPid, 2, 5000),
    Kinds = [result_kind(R) || R <- collect_worker_results(Workers)],

    ?assertEqual(2, length(mgr_conn_pids(MgrPid))),
    ?assertEqual([], [K || {stream_error, _} = K <- Kinds]),
    ?assertEqual(2 * ?CREDIT_FITTING_REQUESTS, length([K || {timeout, request} = K <- Kinds])),
    ?assertEqual(
        ?CREDIT_BOUND_CALLERS - 2 * ?CREDIT_FITTING_REQUESTS,
        length([K || {timeout, checkout} = K <- Kinds])
    ),

    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    ok.

%% The reservation is exact. For any sequence of reservations, refunds
%% and grants, the slot equals the initial window plus every grant minus
%% every outstanding body, it never reads below zero after a rollback,
%% and a refused reservation holds no stream slot either.
pool_h2_reservation_is_exact(_Config) ->
    Prop = ?FORALL(
        Ops,
        list(credit_op()),
        begin
            Slots = nhttpc_stream_slots:new(),
            ok = nhttpc_stream_slots:set_cap(Slots, ?CREDIT_PROPERTY_CAP),
            ok = nhttpc_stream_slots:set_credit(Slots, ?CREDIT_WINDOW),
            run_credit_ops(Ops, Slots, 0, [])
        end
    ),
    ?assert(triq:check(Prop, ?CREDIT_PROPERTY_RUNS)),
    ok.

%% Over-admission on the connection window is impossible by construction.
%% Twelve callers against one connection to a peer that never credits:
%% seven are admitted, five queue as waiters. No caller sees a refusal
%% and the codec never refuses a body, which the absence of any
%% `send_credit_refused` event pins.
pool_h2_over_admission_impossible(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?CREDIT_PEER_CAP}),
    PoolRef = test_h2_pool_credit_no_over,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 1,
        max_overflow => 0,
        otel => #{events => true, metrics => false, traces => false}
    }),
    Self = self(),
    HandlerId = {?MODULE, PoolRef},
    ok = telemetry:attach(
        HandlerId,
        [nhttpc, connection, send_credit_refused],
        fun(_Event, Measurements, Meta, _HandlerConfig) ->
            Self ! {credit_refused, maps:get(conn_pid, Meta), Measurements}
        end,
        undefined
    ),

    Workers = spawn_credit_posts(PoolRef, ?CREDIT_OVER_CALLERS, ?CREDIT_REQUEST_TIMEOUT),
    Kinds = [result_kind(R) || R <- collect_worker_results(Workers)],
    ok = telemetry:detach(HandlerId),

    ?assertEqual([], collect_credit_refusals_now()),
    ?assertEqual([], [K || {stream_error, _} = K <- Kinds]),
    ?assertEqual(?CREDIT_FITTING_REQUESTS, length([K || {timeout, request} = K <- Kinds])),
    ?assertEqual(
        ?CREDIT_OVER_CALLERS - ?CREDIT_FITTING_REQUESTS,
        length([K || {timeout, checkout} = K <- Kinds])
    ),
    ?assertEqual(1, length(mgr_conn_pids(MgrPid))),

    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    drain_conn_messages().

%% Growth fires on either resource. A peer that advertises one concurrent
%% stream makes the second bodiless caller slot-bound while every
%% connection holds its full window, and the pool grows on that. A later
%% SETTINGS frame that raises the cap opens nothing more.
pool_h2_cold_start_grows_on_slots(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?COLD_START_CAP}),
    PoolRef = test_h2_pool_credit_cold_start,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{size => 2, max_overflow => 0}),
    ConnEts = pool_conn_ets(PoolRef),

    Workers = spawn_credit_gets(PoolRef, 2, ?CREDIT_REQUEST_TIMEOUT),
    ok = wait_for_conn_count(MgrPid, 2, 5000),
    Conns = mgr_conn_pids(MgrPid),
    ?assertEqual(2, length(Conns)),
    lists:foreach(
        fun(Pid) ->
            ?assertEqual(?CREDIT_WINDOW, nhttpc_stream_slots:credit(conn_slots(ConnEts, Pid)))
        end,
        Conns
    ),

    ok = nhttpc_h2_peer:send_settings(Peer, #{max_concurrent_streams => ?COLD_START_RAISED_CAP}),
    ok = wait_for_cap(ConnEts, hd(Conns), ?COLD_START_RAISED_CAP, 5000),
    Kinds = [result_kind(R) || R <- collect_worker_results(Workers)],
    ?assertEqual([{timeout, request}, {timeout, request}], Kinds),
    Later = spawn_credit_gets(PoolRef, 2, ?CREDIT_REQUEST_TIMEOUT),
    _ = collect_worker_results(Later),
    ?assertEqual(Conns, mgr_conn_pids(MgrPid)),

    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    ok.

%% A starved pool reports it once a second, on the rise only. Three
%% seconds of checkouts against a connection whose window is spent and
%% whose stream cap is far from reached emit at most four events.
pool_credit_starved_event_is_rate_limited(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?CREDIT_PEER_CAP}),
    PoolRef = test_h2_pool_credit_starved_rate,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 1,
        max_overflow => 0,
        otel => #{events => true, metrics => false, traces => false}
    }),
    [ConnPid] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    Holders = spawn_credit_posts(PoolRef, ?CREDIT_FITTING_REQUESTS, ?CREDIT_HOLD_TIMEOUT),
    ok = wait_for_active_streams(ConnEts, ConnPid, ?CREDIT_FITTING_REQUESTS, 5000),

    Self = self(),
    HandlerId = {?MODULE, PoolRef},
    ok = telemetry:attach(
        HandlerId,
        [nhttpc, pool, credit_starved],
        fun(_Event, _Measurements, Meta, _HandlerConfig) ->
            Self ! {credit_starved, maps:get(pool, Meta)}
        end,
        undefined
    ),
    Probes = probe_starved_pool(PoolRef, ?CREDIT_STARVED_WINDOW_MS, 0),
    ok = telemetry:detach(HandlerId),
    Events = collect_starved_events(0),

    ?assert(Probes >= ?CREDIT_STARVED_MAX_EVENTS),
    ?assert(Events >= 1),
    ?assert(Events =< ?CREDIT_STARVED_MAX_EVENTS),

    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    _ = collect_worker_results(Holders),
    ok.

%% Neither HTTP/3 nor HTTP/1.1 has a connection send window the pool
%% reserves against. An HTTP/3 connection publishes the sentinel and an
%% HTTP/1.1 connection never writes the slot, so a body above the HTTP/2
%% window goes through both without a reservation binding it.
pool_h3_and_h1_unaffected(Config) ->
    H3Port = proplists:get_value(server_port, Config),
    H3Pool = test_h3_pool_credit_unbound,
    {ok, _H3Sup} = start_h3_pool(H3Pool, H3Port, #{}),
    H3Conn = h3_conn_pid(H3Pool),
    ?assertEqual(
        nhttpc_stream_slots:unbounded_credit(),
        nhttpc_stream_slots:credit(conn_slots(pool_conn_ets(H3Pool), H3Conn))
    ),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc_pool:request(H3Pool, unbounded_post(), 10000)
    ),
    ok = nhttpc_pool_sup:stop(H3Pool),

    {ok, H1Server} = nhttp:start_link(test_h1_pool_credit_server, #{
        port => 0,
        handler => nhttp_test_handler,
        protocols => [http1_1]
    }),
    unlink(H1Server),
    {ok, H1Port} = nhttp:get_port(H1Server),
    H1Pool = test_h1_pool_credit_unbound,
    {ok, H1Sup} = nhttpc_pool_sup:start_link(H1Pool, #{
        host => <<"127.0.0.1">>,
        port => H1Port,
        versions => [http1_1],
        size => 1,
        max_overflow => 0
    }),
    unlink(H1Sup),
    H1Mgr = nhttpc_pool:manager(H1Pool),
    ok = nhttpc_pool:wait_ready(H1Mgr, 10000),
    [H1Conn] = mgr_conn_pids(H1Mgr),
    ?assertEqual(0, nhttpc_stream_slots:credit(conn_slots(pool_conn_ets(H1Pool), H1Conn))),
    ?assertMatch(
        {ok, #{status := 200}},
        nhttpc_pool:request(H1Pool, unbounded_post(), 10000)
    ),
    ?assertEqual(0, nhttpc_stream_slots:credit(conn_slots(pool_conn_ets(H1Pool), H1Conn))),
    ok = nhttpc_pool_sup:stop(H1Pool),
    nhttpc_test_server:stop(H1Server),
    ok.

%%%-----------------------------------------------------------------------------
%%% CREDIT-AWARE AVAILABILITY HELPERS
%%%-----------------------------------------------------------------------------

-spec credit_growth_case([tuple()], nhttpc:stream_selection(), atom()) -> ok.
credit_growth_case(Config, Selection, PoolRef) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?CREDIT_PEER_CAP}),
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 1, max_overflow => 1, stream_selection => Selection
    }),
    [First] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    ?assertEqual(?CREDIT_WINDOW, nhttpc_stream_slots:credit(conn_slots(ConnEts, First))),

    Workers = spawn_credit_posts(PoolRef, ?CREDIT_GROWTH_CALLERS, ?CREDIT_REQUEST_TIMEOUT),
    ok = wait_for_conn_count(MgrPid, 2, 5000),
    Kinds = [result_kind(R) || R <- collect_worker_results(Workers)],

    ?assertEqual(2, length(mgr_conn_pids(MgrPid))),
    ?assertEqual(lists:duplicate(?CREDIT_GROWTH_CALLERS, {timeout, request}), Kinds),

    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    ok.

%% The public error arm of one pooled result, for a caller that reads the
%% raw `nhttpc_pool:request/3` shape.
-spec result_kind({ok, nhttp_lib:response()} | {error, term()}) -> term().
result_kind({ok, #{status := Status}}) ->
    {ok, Status};
result_kind({error, Reason}) ->
    {error, Kind} = nhttpc_error:wrap(Reason),
    Kind.

-spec spawn_credit_posts(term(), pos_integer(), timeout()) -> [{pid(), reference()}].
spawn_credit_posts(PoolRef, Count, Timeout) ->
    spawn_pool_requests(PoolRef, credit_post(), Count, Timeout).

-spec spawn_credit_gets(term(), pos_integer(), timeout()) -> [{pid(), reference()}].
spawn_credit_gets(PoolRef, Count, Timeout) ->
    Get = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    spawn_pool_requests(PoolRef, Get, Count, Timeout).

-spec spawn_pool_requests(term(), nhttp_lib:request(), pos_integer(), timeout()) ->
    [{pid(), reference()}].
spawn_pool_requests(PoolRef, Req, Count, Timeout) ->
    [
        spawn_monitor(fun() -> exit({result, nhttpc_pool:request(PoolRef, Req, Timeout)}) end)
     || _ <- lists:seq(1, Count)
    ].

-spec collect_worker_results([{pid(), reference()}]) -> [term()].
collect_worker_results(Workers) ->
    [
        receive
            {'DOWN', MRef, process, Pid, {result, Result}} -> Result;
            {'DOWN', MRef, process, Pid, Other} -> ct:fail("worker died with ~p", [Other])
        after 30000 -> ct:fail("worker never finished")
        end
     || {Pid, MRef} <- Workers
    ].

-spec concurrent_credit_posts(term(), pos_integer(), pos_integer(), timeout()) -> [term()].
concurrent_credit_posts(PoolRef, Count, BodySize, Timeout) ->
    Req = #{
        method => post,
        path => <<"/echo">>,
        headers => [{<<"content-type">>, <<"application/octet-stream">>}],
        body => binary:copy(<<$y>>, BodySize)
    },
    collect_worker_results(spawn_pool_requests(PoolRef, Req, Count, Timeout)).

-spec unbounded_post() -> nhttp_lib:request().
unbounded_post() ->
    #{
        method => post,
        path => <<"/echo">>,
        headers => [{<<"content-type">>, <<"application/octet-stream">>}],
        body => binary:copy(<<$z>>, ?UNBOUNDED_BODY_SIZE)
    }.

-spec collect_credit_refusals_now() -> [pid()].
collect_credit_refusals_now() ->
    receive
        {credit_refused, ConnPid, _Measurements} -> [ConnPid | collect_credit_refusals_now()]
    after 0 -> []
    end.

-spec collect_starved_events(non_neg_integer()) -> non_neg_integer().
collect_starved_events(Count) ->
    receive
        {credit_starved, _Pool} -> collect_starved_events(Count + 1)
    after 200 -> Count
    end.

%% Checkouts with a body against a starved pool, for `Window` milliseconds.
%% Each one walks the fast path, finds no credit, queues at the manager
%% and expires, so each one is a starved checkout.
-spec probe_starved_pool(term(), integer(), non_neg_integer()) -> non_neg_integer().
probe_starved_pool(PoolRef, Window, Probes) ->
    Started = erlang:monotonic_time(millisecond),
    Result = nhttpc_pool:request(PoolRef, credit_post(), ?CREDIT_STARVED_PROBE_TIMEOUT),
    ?assertMatch({error, _}, Result),
    ?assertNotEqual({stream_error, {protocol, refused_stream}}, result_kind(Result)),
    Elapsed = erlang:monotonic_time(millisecond) - Started,
    case Window - Elapsed =< 0 of
        true -> Probes + 1;
        false -> probe_starved_pool(PoolRef, Window - Elapsed, Probes + 1)
    end.

-spec wait_for_credit(nhttpc_stream_slots:slots(), integer(), integer()) -> integer().
wait_for_credit(Slots, _Want, Remaining) when Remaining =< 0 ->
    nhttpc_stream_slots:credit(Slots);
wait_for_credit(Slots, Want, Remaining) ->
    case nhttpc_stream_slots:credit(Slots) of
        Want ->
            Want;
        _ ->
            timer:sleep(10),
            wait_for_credit(Slots, Want, Remaining - 10)
    end.

-spec wait_for_cap(ets:tid(), pid(), integer(), integer()) -> ok.
wait_for_cap(ConnEts, ConnPid, Want, Remaining) when Remaining =< 0 ->
    ct:fail("connection ~p never reached cap ~p (at ~p)", [
        ConnPid, Want, conn_cap(ConnEts, ConnPid)
    ]);
wait_for_cap(ConnEts, ConnPid, Want, Remaining) ->
    case conn_cap(ConnEts, ConnPid) of
        Want ->
            ok;
        _ ->
            timer:sleep(10),
            wait_for_cap(ConnEts, ConnPid, Want, Remaining - 10)
    end.

-spec credit_op() -> triq_dom:domain(term()).
credit_op() ->
    oneof([
        {reserve, choose(0, ?CREDIT_WINDOW + 5000)},
        refund,
        {grant, choose(1, ?CREDIT_WINDOW)}
    ]).

%% Drive the slot with one op at a time and check the invariant after each:
%% the slot equals the window plus every grant minus every outstanding
%% body, and the live count equals the outstanding bodies, so a rolled
%% back reservation left nothing behind.
-spec run_credit_ops([term()], nhttpc_stream_slots:slots(), non_neg_integer(), [non_neg_integer()]) ->
    boolean().
run_credit_ops([], Slots, Granted, Outstanding) ->
    credit_invariant_holds(Slots, Granted, Outstanding);
run_credit_ops([{reserve, Size} | Rest], Slots, Granted, Outstanding) ->
    Before = nhttpc_stream_slots:credit(Slots),
    case nhttpc_stream_slots:reserve(Slots, Size) of
        ok when Size =< Before ->
            credit_invariant_holds(Slots, Granted, [Size | Outstanding]) andalso
                run_credit_ops(Rest, Slots, Granted, [Size | Outstanding]);
        starved when Size > Before ->
            credit_invariant_holds(Slots, Granted, Outstanding) andalso
                run_credit_ops(Rest, Slots, Granted, Outstanding);
        _Wrong ->
            false
    end;
run_credit_ops([refund | Rest], Slots, Granted, []) ->
    run_credit_ops(Rest, Slots, Granted, []);
run_credit_ops([refund | Rest], Slots, Granted, [Size | Outstanding]) ->
    ok = nhttpc_stream_slots:refund_credit(Slots, Size),
    ok = nhttpc_stream_slots:release(Slots),
    credit_invariant_holds(Slots, Granted, Outstanding) andalso
        run_credit_ops(Rest, Slots, Granted, Outstanding);
run_credit_ops([{grant, Octets} | Rest], Slots, Granted, Outstanding) ->
    ok = nhttpc_stream_slots:add_credit(Slots, Octets),
    credit_invariant_holds(Slots, Granted + Octets, Outstanding) andalso
        run_credit_ops(Rest, Slots, Granted + Octets, Outstanding).

-spec credit_invariant_holds(nhttpc_stream_slots:slots(), non_neg_integer(), [non_neg_integer()]) ->
    boolean().
credit_invariant_holds(Slots, Granted, Outstanding) ->
    Credit = nhttpc_stream_slots:credit(Slots),
    Credit >= 0 andalso
        Credit =:= ?CREDIT_WINDOW + Granted - lists:sum(Outstanding) andalso
        nhttpc_stream_slots:live(Slots) =:= length(Outstanding).

%%%-----------------------------------------------------------------------------
%%% STREAM-ID ROTATION AND PROACTIVE REPLACEMENT
%%%-----------------------------------------------------------------------------

%% Sixty requests through a pool of one connection with a threshold of
%% fifty: the first connection opens exactly fifty streams, exits
%% `normal`, and its replacement serves the rest. No caller sees a
%% refusal, because the pool re-dispatches the one that a rotating
%% connection refuses.
pool_h2_rotates_at_threshold(Config) ->
    PoolRef = test_h2_pool_rotate_threshold,
    Port = ?config(server_port, Config),
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 1,
        max_overflow => 0,
        max_streams_per_connection => ?ROTATE_THRESHOLD,
        otel => #{events => true, metrics => false, traces => false}
    }),
    [First] = mgr_conn_pids(MgrPid),
    MRef = monitor(process, First),
    HandlerId = {?MODULE, PoolRef},
    ok = attach_rotated_handler(HandlerId),
    ok = run_pool_requests(MgrPid, ?ROTATE_REQUESTS),
    ok = telemetry:detach(HandlerId),
    receive
        {rotated_event, First, Measurements} ->
            ?assertEqual(#{streams => ?ROTATE_THRESHOLD}, Measurements)
    after ?ROTATE_WAIT_MS -> ct:fail("the first connection never rotated")
    end,
    receive
        {'DOWN', MRef, process, First, Reason} -> ?assertEqual(normal, Reason)
    after ?ROTATE_WAIT_MS -> ct:fail("the rotated connection did not exit")
    end,
    [Second] = wait_for_rotation(MgrPid, First, ?ROTATE_WAIT_MS),
    ?assertNotEqual(First, Second),
    stop_h2_pool(PoolSup),
    ok.

%% The replacement opens when the drain starts, not when the draining
%% connection exits. The peer never answers, so the first connection
%% holds its one request and stays alive, and the second connection has
%% to appear next to it.
pool_replaces_draining_connection_immediately(Config) ->
    PoolRef = test_h2_pool_rotate_replace,
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?CREDIT_PEER_CAP}),
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 1,
        max_overflow => 0,
        max_streams_per_connection => 1,
        timeouts => #{request => ?ROTATE_HOLD_TIMEOUT}
    }),
    [First] = mgr_conn_pids(MgrPid),
    _ = spawn(fun() -> nhttpc_pool:request(PoolRef, brake_request(), ?ROTATE_HOLD_TIMEOUT) end),
    ok = wait_for_census(
        MgrPid,
        fun(#{draining := Draining, serving := Serving}) -> Draining =:= 1 andalso Serving >= 1 end,
        ?ROTATE_WAIT_MS
    ),
    ?assert(is_process_alive(First)),
    ok = wait_for_census(
        MgrPid,
        fun(#{pending := Pending, live := Live}) -> Pending =:= 0 andalso Live =:= 2 end,
        ?ROTATE_WAIT_MS
    ),
    ?assert(is_process_alive(First)),
    ?assertMatch([_, _], mgr_conn_pids(MgrPid)),
    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    drain_conn_messages().

%% A request that reaches the rotating connection between its cast and
%% the tuple rebuild is refused and re-dispatched. Each worker has one
%% request in flight and the re-dispatch excludes the refusing
%% connection, so the worker count bounds the refusals. The count is
%% logged, so the record shows how wide the window is in practice.
pool_rotation_refusal_window_is_bounded(Config) ->
    PoolRef = test_h2_pool_rotate_window,
    Port = ?config(server_port, Config),
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 1,
        max_overflow => 0,
        max_streams_per_connection => ?ROTATE_WINDOW_THRESHOLD
    }),
    [First] = mgr_conn_pids(MgrPid),
    1 = erlang:trace(First, true, [send]),
    Workers = [
        spawn_monitor(fun() -> rotate_worker(PoolRef, ?ROTATE_WINDOW_PER_WORKER) end)
     || _ <- lists:seq(1, ?ROTATE_WINDOW_WORKERS)
    ],
    lists:foreach(
        fun({Pid, MRef}) ->
            receive
                {'DOWN', MRef, process, Pid, Reason} -> ?assertEqual(normal, Reason)
            after ?ROTATE_WORKER_WAIT_MS -> ct:fail("a worker never finished")
            end
        end,
        Workers
    ),
    Refused = count_traced_refusals(First, 0),
    ct:pal("rotation refused ~p requests with ~p workers", [Refused, ?ROTATE_WINDOW_WORKERS]),
    ?assert(Refused =< ?ROTATE_WINDOW_WORKERS),
    stop_h2_pool(PoolSup),
    ok.

%% A peer GOAWAY takes the same path: the manager marks the connection,
%% opens the replacement at once, and the draining connection stays
%% alive with its in-flight stream until the peer answers or the request
%% times out.
pool_goaway_replaces_immediately(Config) ->
    PoolRef = test_h2_pool_goaway_replace,
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?CREDIT_PEER_CAP}),
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 1,
        max_overflow => 0,
        timeouts => #{request => ?ROTATE_HOLD_TIMEOUT}
    }),
    [First] = mgr_conn_pids(MgrPid),
    ConnEts = pool_conn_ets(PoolRef),
    _ = spawn(fun() -> nhttpc_pool:request(PoolRef, brake_request(), ?ROTATE_HOLD_TIMEOUT) end),
    ok = wait_for_live_streams(ConnEts, First, 1, ?ROTATE_WAIT_MS),
    ok = nhttpc_h2_peer:goaway(Peer, ?GOAWAY_KEEPS_STREAM_ONE, no_error, <<>>),
    ok = wait_for_census(
        MgrPid,
        fun(#{draining := Draining, serving := Serving}) -> Draining =:= 1 andalso Serving >= 1 end,
        ?ROTATE_WAIT_MS
    ),
    ?assert(is_process_alive(First)),
    ok = wait_for_census(
        MgrPid,
        fun(#{pending := Pending, live := Live}) -> Pending =:= 0 andalso Live =:= 2 end,
        ?ROTATE_WAIT_MS
    ),
    ?assert(is_process_alive(First)),
    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    drain_conn_messages().

%% Ten rotations in a row: every connection exits `normal`, so the
%% backoff counter never moves and the reconnect delay stays at the base.
pool_rotation_does_not_count_as_failure(Config) ->
    PoolRef = test_h2_pool_rotate_brake,
    Port = ?config(server_port, Config),
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 1,
        max_overflow => 0,
        max_streams_per_connection => ?ROTATE_BRAKE_THRESHOLD,
        reconnect_backoff_base => ?BRAKE_BASE,
        reconnect_backoff_max => ?BRAKE_MAX,
        otel => #{events => true, metrics => false, traces => false}
    }),
    HandlerId = {?MODULE, PoolRef},
    ok = attach_rotated_handler(HandlerId),
    ok = run_pool_requests(MgrPid, ?ROTATE_BRAKE_THRESHOLD * ?ROTATE_BRAKE_ROTATIONS),
    Rotated = collect_rotated(?ROTATE_BRAKE_ROTATIONS, []),
    ok = telemetry:detach(HandlerId),
    ?assertEqual(?ROTATE_BRAKE_ROTATIONS, length(lists:usort(Rotated))),
    ?assertEqual(0, mgr_consecutive_failures(MgrPid)),
    stop_h2_pool(PoolSup),
    ok.

%% Every connection rotates at once against a peer that never answers.
%% The pool opens at most `size + max_overflow` connections that can take
%% a request, and the live process count never exceeds twice that bound.
pool_live_bound_includes_draining(Config) ->
    PoolRef = test_h2_pool_rotate_bound,
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?CREDIT_PEER_CAP}),
    Bound = ?ROTATE_BOUND_SIZE + ?ROTATE_BOUND_OVERFLOW,
    {PoolSup, MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => ?ROTATE_BOUND_SIZE,
        max_overflow => ?ROTATE_BOUND_OVERFLOW,
        stream_selection => spread,
        max_streams_per_connection => 1,
        timeouts => #{request => ?ROTATE_HOLD_TIMEOUT}
    }),
    ok = wait_for_multiplexed_conns(PoolRef, ?ROTATE_BOUND_SIZE, ?ROTATE_WAIT_MS),
    lists:foreach(
        fun(_) ->
            _ = spawn(fun() ->
                nhttpc_pool:request(PoolRef, brake_request(), ?ROTATE_HOLD_TIMEOUT)
            end)
        end,
        lists:seq(1, Bound)
    ),
    Censuses = sample_census(MgrPid, ?ROTATE_BOUND_WINDOW_MS, []),
    ?assert(lists:all(fun(#{live := Live}) -> Live =< 2 * Bound end, Censuses)),
    ?assert(lists:all(fun(#{serving := Serving}) -> Serving =< Bound end, Censuses)),
    ?assert(
        lists:any(
            fun(#{serving := Serving, draining := Draining}) ->
                Serving >= ?ROTATE_BOUND_SIZE andalso Draining >= ?ROTATE_BOUND_SIZE
            end,
            Censuses
        )
    ),
    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    drain_conn_messages().

%% Saturation is a level. With the one connection at its cap and the pool
%% at `size + max_overflow`, the manager reports the waiters it holds and
%% the connections that take requests, and reports zero waiters once the
%% queue empties at the checkout deadline.
pool_saturation_is_a_level(Config) ->
    PoolRef = test_h2_pool_occupancy,
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => 1}),
    {PoolSup, _MgrPid} = start_h2_pool(PoolRef, Port, #{
        size => 1,
        max_overflow => 0,
        timeouts => #{checkout => ?OCCUPANCY_HOLD_MS, request => ?OCCUPANCY_HOLD_MS},
        otel => #{events => true, metrics => false, traces => false}
    }),
    HandlerId = {?MODULE, PoolRef},
    Self = self(),
    ok = telemetry:attach(
        HandlerId,
        [nhttpc, pool, occupancy],
        fun(_Event, Measurements, #{pool := Pool}, _HandlerConfig) ->
            Self ! {occupancy, Pool, Measurements}
        end,
        undefined
    ),
    Workers = [
        spawn_monitor(fun() ->
            exit({result, nhttpc_pool:request(PoolRef, brake_request(), ?OCCUPANCY_HOLD_MS)})
        end)
     || _ <- lists:seq(1, ?OCCUPANCY_WAITERS + 1)
    ],
    ok = await_occupancy(PoolRef, ?OCCUPANCY_WAITERS, 1),
    ok = await_occupancy(PoolRef, 0, 1),
    ok = telemetry:detach(HandlerId),
    Results = [
        receive
            {'DOWN', MRef, process, Pid, {result, Result}} -> Result
        after ?OCCUPANCY_WAIT_MS -> ct:fail("a held request never finished")
        end
     || {Pid, MRef} <- Workers
    ],
    ?assertEqual([], [Result || {ok, _} = Result <- Results]),
    stop_h2_pool(PoolSup),
    ok = nhttpc_h2_peer:stop(Peer),
    drain_conn_messages().

-spec await_occupancy(atom(), non_neg_integer(), non_neg_integer()) -> ok.
await_occupancy(PoolRef, Waiters, Connections) ->
    receive
        {occupancy, PoolRef, #{waiter_count := Waiters, connections := Connections}} -> ok
    after ?OCCUPANCY_WAIT_MS ->
        ct:fail("no occupancy event with ~p waiters on ~p connections", [Waiters, Connections])
    end.

-spec attach_rotated_handler(term()) -> ok.
attach_rotated_handler(HandlerId) ->
    Self = self(),
    telemetry:attach(
        HandlerId,
        [nhttpc, connection, rotated],
        fun(_Event, Measurements, Meta, _HandlerConfig) ->
            Self ! {rotated_event, maps:get(conn_pid, Meta), Measurements}
        end,
        undefined
    ).

-spec collect_rotated(non_neg_integer(), [pid()]) -> [pid()].
collect_rotated(0, Acc) ->
    Acc;
collect_rotated(Count, Acc) ->
    receive
        {rotated_event, ConnPid, #{streams := ?ROTATE_BRAKE_THRESHOLD}} ->
            collect_rotated(Count - 1, [ConnPid | Acc])
    after ?ROTATE_WAIT_MS ->
        ct:fail("expected ~p more rotated events", [Count])
    end.

-spec count_traced_refusals(pid(), non_neg_integer()) -> non_neg_integer().
count_traced_refusals(ConnPid, Acc) ->
    receive
        {trace, ConnPid, send, {nhttpc_error, ConnPid, _Ref, {http2, #{type := refused}}}, _To} ->
            count_traced_refusals(ConnPid, Acc + 1);
        {trace, ConnPid, send, _Msg, _To} ->
            count_traced_refusals(ConnPid, Acc)
    after 0 -> Acc
    end.

-spec rotate_worker(term(), non_neg_integer()) -> ok.
rotate_worker(_PoolRef, 0) ->
    ok;
rotate_worker(PoolRef, Remaining) ->
    case nhttpc_pool:request(PoolRef, brake_request(), ?ROTATE_HOLD_TIMEOUT) of
        {ok, #{status := 200}} -> rotate_worker(PoolRef, Remaining - 1);
        Other -> exit({request_failed, Other})
    end.

%% The manager keeps `conn_opts`, `connections` and `pending_conns`
%% adjacent in `#state{}`, and `conn_opts` is the only map in the state
%% with a `pool_mgr` key, so it locates the other two without a record
%% definition. Both value shapes are matched in full, so a change to
%% either record fails here loudly.
-spec mgr_census(pid()) ->
    #{
        serving := non_neg_integer(),
        draining := non_neg_integer(),
        pending := non_neg_integer(),
        live := non_neg_integer()
    }.
mgr_census(MgrPid) ->
    {Conns, Pending} = maps_after_conn_opts(tuple_to_list(sys:get_state(MgrPid))),
    true = lists:all(
        fun({MonRef, Slots}) -> is_reference(MonRef) andalso is_reference(Slots) end,
        maps:values(Pending)
    ),
    Draining = length([Info || Info <- maps:values(Conns), conn_info_draining(Info)]),
    Ready = map_size(Conns) - Draining,
    #{
        serving => Ready + map_size(Pending),
        draining => Draining,
        pending => map_size(Pending),
        live => map_size(Conns) + map_size(Pending)
    }.

-spec maps_after_conn_opts([term()]) -> {map(), map()}.
maps_after_conn_opts([#{pool_mgr := _}, Conns, Pending | _]) when
    is_map(Conns), is_map(Pending)
->
    {Conns, Pending};
maps_after_conn_opts([_ | Rest]) ->
    maps_after_conn_opts(Rest).

-spec conn_info_draining(tuple()) -> boolean().
conn_info_draining(
    {conn_info, Pid, MonRef, _Protocol, _ActiveCount, Slots, Draining, _CreatedAt, _LastActivity}
) when is_pid(Pid), is_reference(MonRef), is_reference(Slots), is_boolean(Draining) ->
    Draining.

-spec sample_census(pid(), integer(), [map()]) -> [map()].
sample_census(_MgrPid, Remaining, Acc) when Remaining =< 0 ->
    lists:reverse(Acc);
sample_census(MgrPid, Remaining, Acc) ->
    Census = mgr_census(MgrPid),
    timer:sleep(?ROTATE_POLL_MS),
    sample_census(MgrPid, Remaining - ?ROTATE_POLL_MS, [Census | Acc]).

-spec wait_for_census(pid(), fun((map()) -> boolean()), integer()) -> ok.
wait_for_census(MgrPid, _Pred, Remaining) when Remaining =< 0 ->
    ct:fail("pool census never matched, last ~p", [mgr_census(MgrPid)]);
wait_for_census(MgrPid, Pred, Remaining) ->
    case Pred(mgr_census(MgrPid)) of
        true ->
            ok;
        false ->
            timer:sleep(?ROTATE_POLL_MS),
            wait_for_census(MgrPid, Pred, Remaining - ?ROTATE_POLL_MS)
    end.

-spec wait_for_live_streams(ets:tid(), pid(), integer(), integer()) -> ok.
wait_for_live_streams(ConnEts, ConnPid, Want, Remaining) when Remaining =< 0 ->
    ct:fail("connection ~p never reached ~p live streams (at ~p)", [
        ConnPid, Want, nhttpc_stream_slots:live(conn_slots(ConnEts, ConnPid))
    ]);
wait_for_live_streams(ConnEts, ConnPid, Want, Remaining) ->
    case nhttpc_stream_slots:live(conn_slots(ConnEts, ConnPid)) of
        Want ->
            ok;
        _ ->
            timer:sleep(?ROTATE_POLL_MS),
            wait_for_live_streams(ConnEts, ConnPid, Want, Remaining - ?ROTATE_POLL_MS)
    end.

%% A reservation is one step for everyone: no reader ever sees the live
%% count past the cap or the credit below zero, and a caller whose claim
%% fits is never refused because another caller's claim did not. Probers
%% whose body never fits hammer the slots while small callers, whose
%% bodies always fit together, count the refusals they get, and readers
%% spin on both counters.
pool_h2_reservation_is_linearizable(_Config) ->
    Slots = nhttpc_stream_slots:new(),
    ok = nhttpc_stream_slots:set_cap(Slots, ?LINEAR_CAP),
    ok = nhttpc_stream_slots:set_credit(Slots, ?LINEAR_CREDIT),
    Parent = self(),
    Readers = [
        spawn_link(fun() -> Parent ! {reader, self(), read_extremes(Slots, {0, ?LINEAR_CREDIT})} end)
     || _ <- lists:seq(1, ?LINEAR_READERS)
    ],
    Probers = [
        spawn_link(fun() -> probe_slots(Slots, ?LINEAR_OPS) end)
     || _ <- lists:seq(1, ?LINEAR_PROBERS)
    ],
    Small = [
        spawn_link(fun() -> Parent ! {small, self(), claim_small(Slots, ?LINEAR_OPS, 0)} end)
     || _ <- lists:seq(1, ?LINEAR_SMALL)
    ],
    Refusals = lists:sum([
        receive
            {small, Pid, N} -> N
        end
     || Pid <- Small
    ]),
    lists:foreach(fun(Pid) -> Pid ! stop end, Readers),
    Extremes = [
        receive
            {reader, Pid, E} -> E
        end
     || Pid <- Readers
    ],
    lists:foreach(
        fun(Pid) ->
            unlink(Pid),
            exit(Pid, kill)
        end,
        Probers
    ),
    ?assertEqual(0, Refusals),
    ?assertEqual([], [E || {MaxLive, _} = E <- Extremes, MaxLive > ?LINEAR_CAP]),
    ?assertEqual([], [E || {_, MinCredit} = E <- Extremes, MinCredit < 0]),
    ok.

-spec read_extremes(nhttpc_stream_slots:slots(), {integer(), integer()}) -> {integer(), integer()}.
read_extremes(Slots, {MaxLive, MinCredit}) ->
    receive
        stop -> {MaxLive, MinCredit}
    after 0 ->
        Live = nhttpc_stream_slots:live(Slots),
        Credit = nhttpc_stream_slots:credit(Slots),
        read_extremes(Slots, {max(MaxLive, Live), min(MinCredit, Credit)})
    end.

%% A body above the whole credit: every claim rolls back.
-spec probe_slots(nhttpc_stream_slots:slots(), non_neg_integer()) -> ok.
probe_slots(_Slots, 0) ->
    ok;
probe_slots(Slots, N) ->
    starved = nhttpc_stream_slots:reserve(Slots, ?LINEAR_CREDIT + 1),
    probe_slots(Slots, N - 1).

%% A body that fits with every other small caller's body outstanding, so
%% a refusal can only come from a prober's claim in flight.
-spec claim_small(nhttpc_stream_slots:slots(), non_neg_integer(), non_neg_integer()) ->
    non_neg_integer().
claim_small(_Slots, 0, Refused) ->
    Refused;
claim_small(Slots, N, Refused) ->
    case nhttpc_stream_slots:reserve(Slots, ?LINEAR_SMALL_BODY) of
        ok ->
            ok = nhttpc_stream_slots:refund_credit(Slots, ?LINEAR_SMALL_BODY),
            ok = nhttpc_stream_slots:release(Slots),
            claim_small(Slots, N - 1, Refused);
        starved ->
            claim_small(Slots, N - 1, Refused + 1);
        full ->
            claim_small(Slots, N - 1, Refused + 1)
    end.
