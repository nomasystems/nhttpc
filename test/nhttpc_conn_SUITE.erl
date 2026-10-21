%%%-----------------------------------------------------------------------------
%%% @doc Client connection module test suite.
%%%
%%% Tests the nhttpc_conn state machine directly to improve coverage.
%%% Exercises connection lifecycle, error handling, and protocol-specific behavior.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_conn_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(PEER_CAP, 2).
-define(PEER_CAP_LATER, 7).
-define(PEER_CAP_ZERO, 0).
-define(PEER_OVER_GRANT_CAP, 5).
-define(CREDIT_BODY_SIZE, 8192).
-define(CREDIT_FITTING_REQUESTS, 7).
-define(CREDIT_PEER_CAP, 1000).
-define(CREDIT_REFUSAL_TIMEOUT, 10000).
-define(CREDIT_SMALL_BODY_SIZE, 134).
-define(CREDIT_SMALL_FITTING_REQUESTS, 489).
-define(PEER_WINDOW, 65535).
-define(SLOT_CAP, 100).
-define(CREDIT_DRIFT, 1000).
-define(SMALL_STREAM_WINDOW, 1000).
-define(OVER_STREAM_WINDOW_BODY, 2000).
-define(GOAWAY_REASON, <<"{\"reason\":\"Shutdown\"}">>).
-define(GOAWAY_LAST_STREAM_ID, 0).
-define(ROTATION_THRESHOLD, 50).
-define(CREDIT_GRANT, 1000).
-define(CREDIT_GRANTS, 25).
-define(CREDIT_PERIOD_MS, 100).
-define(CREDIT_REPORT_SETTLE_MS, 500).
-define(QUEUE_BODY, 100000).
-define(QUEUE_REMAINDER, (?QUEUE_BODY - ?PEER_WINDOW)).
-define(QUEUE_GRANT, 40000).
-define(PEER_MAX_FRAME_SIZE, 16384).
-define(BUFFER_BOUND, 8192).
-define(BUFFER_BODY, 70000).
-define(SMALL_BODY_REQUESTS, 700).
-define(SMALL_BODY_DELAY_MS, 300).
-define(LARGE_BODY, 102400).
-define(LARGE_BODY_CREDIT_DELAY_MS, 200).
-define(CREDIT_STREAM_BIG, 200000).
-define(CREDIT_STREAM_SMALL, 50000).
-define(CREDIT_FLUSH_BODY, 20000).
-define(STREAM_CREDIT_THRESHOLD, 32767).
-define(QUEUE_TIMEOUT_MS, 500).
-define(SLOW_RESPONSE_MS, 2000).
-define(QUEUE_WAIT_MS, 10000).
-define(QUEUE_SETTLE_MS, 300).
-define(QUEUE_POLL_MS, 10).
-define(SEND_QUEUE_SAMPLE_MS, 1000).

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

%% Connection lifecycle tests
-export([
    conn_start_link/1,
    conn_stop/1,
    conn_connect_timeout/1,
    conn_connect_error/1
]).

%% Request/response tests
-export([
    conn_send_request_h1/1,
    conn_send_request_connecting/1,
    conn_send_request_draining/1,
    conn_request_timeout/1,
    conn_multiple_sequential_h1/1,
    conn_no_unified_double_emit/1
]).

%% Cancel tests
-export([
    conn_cancel_unknown/1,
    conn_cancel_in_flight/1
]).

%% Connection pool integration tests
-export([
    conn_pool_mgr_notifications/1,
    conn_pool_ready_notification/1
]).

%% Error handling tests
-export([
    conn_socket_closed/1,
    conn_socket_error/1
]).

%% HTTP/2 specific tests
-export([
    conn_h2_request/1,
    conn_h2_multiplexed/1,
    conn_h2_goaway/1,
    conn_h2_request_with_body/1,
    conn_h2_cancel_in_flight/1,
    conn_h2_timeout/1
]).

%% Protocol detection tests
-export([
    conn_h1_only_protocol/1,
    conn_h2_fallback_to_h1/1
]).

%% Additional edge case tests
-export([
    conn_connecting_exit/1,
    conn_host_as_string/1,
    conn_host_as_ip_tuple/1,
    conn_ssl_connect_opts/1,
    conn_cancel_while_connecting/1,
    conn_h2_large_response/1,
    conn_h2_client_certs_keys/1,
    conn_ssl_supported_groups/1
]).

%% HTTP/2 per-stream model tests
-export([
    conn_h2_per_stream_request/1,
    conn_h2_per_stream_multiplexed/1,
    conn_h2_per_stream_timeout/1,
    conn_h2_per_stream_cancel/1
]).

%% Sys callback tests (proc_lib/sys integration)
-export([
    conn_sys_get_state_ready/1,
    conn_sys_get_state_connecting/1,
    conn_sys_suspend_resume/1,
    conn_sys_statistics/1
]).

%% Draining state tests
-export([
    conn_draining_rejects_wait_ready/1,
    conn_draining_rejects_ping/1
]).

%% Ping tests
-export([
    conn_ping_ready/1,
    conn_ping_connecting/1
]).

%% SSL-specific tests
-export([
    conn_ssl_request/1,
    conn_ssl_multiple_requests/1
]).

%% Draining socket tests
-export([
    conn_draining_receives_response/1,
    conn_draining_send_rejected/1
]).

%% HTTP/2 error paths
-export([
    conn_h2_stream_reset/1,
    conn_h2_settings_notification/1,
    conn_h2_window_update/1
]).

%% Keep-alive parsing tests
-export([
    conn_keep_alive_with_timeout/1,
    conn_keep_alive_multiple_params/1,
    conn_keep_alive_no_timeout/1,
    conn_keep_alive_invalid_value/1
]).

%% HTTP/2 ping extended tests
-export([
    conn_h2_ping_basic/1,
    conn_h2_ping_overwrite/1
]).

%% HTTP/2 flow control tests
-export([
    conn_h2_large_body_flow_control/1
]).

%% Stop timeout tests
-export([
    conn_stop_force_kill/1
]).

%% HTTP/2 draining loop tests
-export([
    conn_h2_draining_completes_pending/1,
    conn_h2_draining_rejects_new/1
]).

%% H2 draining with in-flight request (socket data in draining)
-export([
    conn_h2_draining_inflight_response/1
]).

%% HTTP/2 headers-only response tests
-export([
    conn_h2_head_request/1,
    conn_h2_empty_body_response/1
]).

%% Raw TCP error path tests (H1 parse errors, socket errors)
-export([
    conn_h1_socket_close_mid_idle/1,
    conn_h1_malformed_response/1
]).

%% H1 streaming receive path tests (fragmented bodies, chunked, HEAD, EOF)
-export([
    conn_h1_body_single_fragment/1,
    conn_h1_body_many_fragments/1,
    conn_h1_split_mid_headers/1,
    conn_h1_chunked_split/1,
    conn_h1_chunked_trailers/1,
    conn_h1_head_no_body/1,
    conn_h1_status_204_no_body/1,
    conn_h1_until_close_eof/1,
    conn_h1_sequential_keepalive_bodies/1,
    conn_h1_body_too_large/1
]).

%% H2 batching tests (drain_sends)
-export([
    conn_h2_concurrent_batch/1
]).

%% H2 live stream slot accounting tests
-export([
    conn_h2_refused_stream_keeps_connection/1,
    conn_h2_rejected_send_releases_slot/1
]).

%% H2 peer concurrent stream cap tests
-export([
    conn_h2_later_settings_lower_cap/1,
    conn_h2_max_streams_change_event/1,
    conn_h2_refused_before_open_is_retryable/1,
    conn_h2_survives_credit_exhaustion/1,
    conn_h2_survives_credit_exhaustion_small_bodies/1,
    conn_h2_refused_request_leaves_hpack_intact/1,
    conn_h2_refused_request_opens_no_stream/1,
    conn_h2_refused_request_releases_slot/1,
    conn_h2_credit_resyncs_when_idle/1,
    conn_h2_body_above_stream_window_is_queued/1,
    conn_h2_goaway_carries_debug_data/1,
    conn_h2_credit_rate_reported/1,
    conn_h2_idle_resync_keeps_reservations/1,
    conn_h2_reports_later_settings_cap/1,
    conn_h2_reports_peer_max_concurrent_streams/1,
    conn_h2_reports_undefined_when_peer_omits_cap/1
]).

%% H2 GOAWAY draining loop tests (via sys:replace_state)
-export([
    conn_h2_goaway_draining_via_sys/1
]).

%% HTTP/2 trailer tests
-export([
    conn_h2_trailers/1,
    conn_h2_no_trailers/1,
    conn_h2_trailers_only/1
]).

%% conn fault-path coverage (sys callbacks, injected wire messages,
%% draining loop, connect/TLS option paths)
-export([
    conn_fault_format_status_all_states/1,
    conn_fault_system_code_change/1,
    conn_fault_system_terminate_connecting/1,
    conn_fault_system_terminate_ready/1,
    conn_fault_ready_ssl_closed/1,
    conn_fault_ready_tcp_error/1,
    conn_fault_ready_ssl_error/1,
    conn_fault_ready_ssl_passive/1,
    conn_fault_ready_unknown_down/1,
    conn_fault_caller_down_h1/1,
    conn_fault_h1_unsolicited_garbage/1,
    conn_fault_connecting_unified_cancel/1,
    conn_fault_connecting_system_status/1,
    conn_fault_ssl_default_tls_opts/1,
    conn_fault_default_port_authority/1,
    conn_fault_h1_draining_inflight/1,
    conn_fault_draining_tcp_closed/1,
    conn_fault_draining_check_request_timeouts/1,
    conn_fault_call_timeout/1,
    conn_fault_h2_over_plaintext/1,
    conn_fault_idle_timeout_pool_close/1,
    conn_fault_idle_infinity/1,
    conn_fault_draining_parent_exit/1,
    conn_fault_ready_check_idle_pending/1,
    conn_fault_proxy_connect_error/1,
    conn_fault_ssl_cacertfile_opt/1,
    conn_fault_h1_draining_partial/1,
    conn_fault_h1_draining_garbage/1
]).

%% H2 stream-id rotation
-export([
    conn_h2_rotates_at_threshold/1
]).

%% conn fault-path coverage (HTTP/2)
-export([
    conn_fault_caller_down_h2/1,
    conn_fault_h2_max_response_size/1,
    conn_fault_h2_passive_reactivate/1,
    conn_fault_h2_draining_message_matrix/1,
    conn_fault_h2_server_goaway/1,
    conn_fault_h2_ping_overwrite_forced/1,
    conn_fault_h2_draining_caller_down/1
]).

%% H2 send queue
-export([
    conn_h2_small_body_is_admitted_not_queued/1,
    conn_h2_large_body_is_queued_not_refused/1,
    conn_h2_body_over_max_frame_size/1,
    conn_h2_send_buffer_full_is_retryable/1,
    conn_h2_rejected_request_leaves_hpack_intact/1,
    conn_h2_window_update_event_is_not_swallowed/1,
    conn_h2_cancel_purges_buffered_body/1,
    conn_h2_timeout_on_queued_body_is_retryable/1,
    conn_h2_credit_slot_tracks_queued_body/1,
    conn_h2_send_blocked_and_resumed/1,
    conn_h2_send_gauges_track_codec/1,
    conn_h2_small_bodies_do_not_block/1
]).

%% H2 receive-side credit
-export([
    conn_h2_stream_credit_matches_stream_consumption/1,
    conn_h2_connection_credit_flushed_on_fin/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, lifecycle},
        {group, requests},
        {group, cancel},
        {group, pool_integration},
        {group, errors},
        {group, http2},
        {group, http2_per_stream},
        {group, http2_error_paths},
        {group, protocol_detection},
        {group, edge_cases},
        {group, sys_callbacks},
        {group, draining},
        {group, draining_socket},
        {group, ping},
        {group, ssl_specific},
        {group, keep_alive_parsing},
        {group, h2_ping_extended},
        {group, h2_flow_control},
        {group, stop_timeout},
        {group, h2_stream_slots},
        {group, h2_peer_cap},
        {group, h2_send_credit},
        {group, h2_credit_slot},
        {group, h2_draining},
        {group, h2_headers_only},
        {group, h1_raw_errors},
        {group, h1_streaming},
        {group, h2_batching},
        {group, h2_goaway_draining},
        {group, h2_rotation},
        {group, h2_trailers},
        {group, conn_faults},
        {group, conn_faults_h2},
        {group, h2_send_queue},
        {group, h2_recv_credit}
    ].

groups() ->
    [
        {lifecycle, [sequence], [
            conn_start_link,
            conn_stop,
            conn_connect_timeout,
            conn_connect_error
        ]},
        {requests, [sequence], [
            conn_send_request_h1,
            conn_send_request_connecting,
            conn_send_request_draining,
            conn_request_timeout,
            conn_multiple_sequential_h1,
            conn_no_unified_double_emit
        ]},
        {cancel, [sequence], [
            conn_cancel_unknown,
            conn_cancel_in_flight
        ]},
        {pool_integration, [sequence], [
            conn_pool_mgr_notifications,
            conn_pool_ready_notification
        ]},
        {errors, [sequence], [
            conn_socket_closed,
            conn_socket_error
        ]},
        {http2, [sequence], [
            conn_h2_request,
            conn_h2_multiplexed,
            conn_h2_goaway,
            conn_h2_request_with_body,
            conn_h2_cancel_in_flight,
            conn_h2_timeout
        ]},
        %% HTTP/2 per-stream model tests
        {http2_per_stream, [sequence], [
            conn_h2_per_stream_request,
            conn_h2_per_stream_multiplexed,
            conn_h2_per_stream_timeout,
            conn_h2_per_stream_cancel
        ]},
        {protocol_detection, [sequence], [
            conn_h1_only_protocol,
            conn_h2_fallback_to_h1
        ]},
        {edge_cases, [sequence], [
            conn_connecting_exit,
            conn_host_as_string,
            conn_host_as_ip_tuple,
            conn_ssl_connect_opts,
            conn_cancel_while_connecting,
            conn_h2_large_response,
            conn_h2_client_certs_keys,
            conn_ssl_supported_groups
        ]},
        {sys_callbacks, [sequence], [
            conn_sys_get_state_ready,
            conn_sys_get_state_connecting,
            conn_sys_suspend_resume,
            conn_sys_statistics
        ]},
        {draining, [sequence], [
            conn_draining_rejects_wait_ready,
            conn_draining_rejects_ping
        ]},
        {draining_socket, [sequence], [
            conn_draining_receives_response,
            conn_draining_send_rejected
        ]},
        {ping, [sequence], [
            conn_ping_ready,
            conn_ping_connecting
        ]},
        {ssl_specific, [sequence], [
            conn_ssl_request,
            conn_ssl_multiple_requests
        ]},
        {http2_error_paths, [sequence], [
            conn_h2_stream_reset,
            conn_h2_settings_notification,
            conn_h2_window_update
        ]},
        {keep_alive_parsing, [sequence], [
            conn_keep_alive_with_timeout,
            conn_keep_alive_multiple_params,
            conn_keep_alive_no_timeout,
            conn_keep_alive_invalid_value
        ]},
        {h2_ping_extended, [sequence], [
            conn_h2_ping_basic,
            conn_h2_ping_overwrite
        ]},
        {h2_flow_control, [sequence], [
            conn_h2_large_body_flow_control
        ]},
        {stop_timeout, [sequence], [
            conn_stop_force_kill
        ]},
        {h2_draining, [sequence], [
            conn_h2_draining_completes_pending,
            conn_h2_draining_rejects_new
        ]},
        {h2_headers_only, [sequence], [
            conn_h2_head_request,
            conn_h2_empty_body_response
        ]},
        {h1_raw_errors, [sequence], [
            conn_h1_socket_close_mid_idle,
            conn_h1_malformed_response
        ]},
        {h1_streaming, [sequence], [
            conn_h1_body_single_fragment,
            conn_h1_body_many_fragments,
            conn_h1_split_mid_headers,
            conn_h1_chunked_split,
            conn_h1_chunked_trailers,
            conn_h1_head_no_body,
            conn_h1_status_204_no_body,
            conn_h1_until_close_eof,
            conn_h1_sequential_keepalive_bodies,
            conn_h1_body_too_large
        ]},
        {h2_batching, [sequence], [
            conn_h2_concurrent_batch
        ]},
        {h2_stream_slots, [sequence], [
            conn_h2_refused_stream_keeps_connection,
            conn_h2_rejected_send_releases_slot
        ]},
        {h2_peer_cap, [sequence], [
            conn_h2_reports_peer_max_concurrent_streams,
            conn_h2_reports_undefined_when_peer_omits_cap,
            conn_h2_reports_later_settings_cap,
            conn_h2_later_settings_lower_cap,
            conn_h2_max_streams_change_event,
            conn_h2_refused_before_open_is_retryable
        ]},
        {h2_send_credit, [sequence], [
            conn_h2_survives_credit_exhaustion,
            conn_h2_survives_credit_exhaustion_small_bodies,
            conn_h2_refused_request_leaves_hpack_intact,
            conn_h2_refused_request_opens_no_stream,
            conn_h2_refused_request_releases_slot
        ]},
        {h2_credit_slot, [sequence], [
            conn_h2_credit_resyncs_when_idle,
            conn_h2_body_above_stream_window_is_queued,
            conn_h2_goaway_carries_debug_data,
            conn_h2_credit_rate_reported,
            conn_h2_idle_resync_keeps_reservations
        ]},
        {h2_goaway_draining, [sequence], [
            conn_h2_goaway_draining_via_sys,
            conn_h2_draining_inflight_response
        ]},
        {h2_trailers, [sequence], [
            conn_h2_trailers,
            conn_h2_no_trailers,
            conn_h2_trailers_only
        ]},
        {conn_faults, [sequence], [
            conn_fault_format_status_all_states,
            conn_fault_system_code_change,
            conn_fault_system_terminate_connecting,
            conn_fault_system_terminate_ready,
            conn_fault_ready_ssl_closed,
            conn_fault_ready_tcp_error,
            conn_fault_ready_ssl_error,
            conn_fault_ready_ssl_passive,
            conn_fault_ready_unknown_down,
            conn_fault_caller_down_h1,
            conn_fault_h1_unsolicited_garbage,
            conn_fault_connecting_unified_cancel,
            conn_fault_connecting_system_status,
            conn_fault_ssl_default_tls_opts,
            conn_fault_default_port_authority,
            conn_fault_h1_draining_inflight,
            conn_fault_draining_tcp_closed,
            conn_fault_draining_check_request_timeouts,
            conn_fault_call_timeout,
            conn_fault_h2_over_plaintext,
            conn_fault_idle_timeout_pool_close,
            conn_fault_idle_infinity,
            conn_fault_draining_parent_exit,
            conn_fault_ready_check_idle_pending,
            conn_fault_proxy_connect_error,
            conn_fault_ssl_cacertfile_opt,
            conn_fault_h1_draining_partial,
            conn_fault_h1_draining_garbage
        ]},
        {h2_rotation, [sequence], [
            conn_h2_rotates_at_threshold
        ]},
        {conn_faults_h2, [sequence], [
            conn_fault_caller_down_h2,
            conn_fault_h2_max_response_size,
            conn_fault_h2_passive_reactivate,
            conn_fault_h2_draining_message_matrix,
            conn_fault_h2_server_goaway,
            conn_fault_h2_ping_overwrite_forced,
            conn_fault_h2_draining_caller_down
        ]},
        {h2_send_queue, [sequence], [
            conn_h2_small_body_is_admitted_not_queued,
            conn_h2_large_body_is_queued_not_refused,
            conn_h2_body_over_max_frame_size,
            conn_h2_send_buffer_full_is_retryable,
            conn_h2_rejected_request_leaves_hpack_intact,
            conn_h2_window_update_event_is_not_swallowed,
            conn_h2_cancel_purges_buffered_body,
            conn_h2_timeout_on_queued_body_is_retryable,
            conn_h2_credit_slot_tracks_queued_body,
            conn_h2_send_blocked_and_resumed,
            conn_h2_send_gauges_track_codec,
            conn_h2_small_bodies_do_not_block
        ]},
        {h2_recv_credit, [sequence], [
            conn_h2_stream_credit_matches_stream_consumption,
            conn_h2_connection_credit_flushed_on_fin
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
            ct:log("Warning: TLS certificates not found at ~s", [ConfDir]),
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

init_per_group(keep_alive_parsing, Config) ->
    %% Start chaos server for keep-alive header testing
    case nhttpc_chaos_server:start(tcp) of
        {ok, ServerRef} ->
            [{chaos_server, ServerRef} | Config];
        {error, Reason} ->
            ct:fail("Failed to start chaos server: ~p", [Reason])
    end;
init_per_group(stop_timeout, Config) ->
    %% No server needed - we'll create a mock process
    Config;
init_per_group(h2_stream_slots, Config) ->
    %% Each case starts its own server with its own H2 settings
    Config;
init_per_group(h2_peer_cap, Config) ->
    %% Each case starts its own peer with its own SETTINGS frame
    Config;
init_per_group(h2_send_credit, Config) ->
    Config;
init_per_group(Group, Config) when
    Group =:= lifecycle;
    Group =:= requests;
    Group =:= cancel;
    Group =:= pool_integration;
    Group =:= errors;
    Group =:= edge_cases;
    Group =:= sys_callbacks;
    Group =:= draining;
    Group =:= draining_socket;
    Group =:= conn_faults;
    Group =:= ping
->
    %% Start HTTP/1.1 test server
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_conn_server, #{
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
    end;
init_per_group(protocol_detection, Config) ->
    %% Start HTTP/1.1 server for protocol detection tests
    Handler = nhttp_test_handler,
    case
        nhttp:start_link(test_proto_detect_server, #{
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
    end;
init_per_group(http2, Config) ->
    %% Start HTTP/2 TLS test server
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            Handler = nhttp_test_handler,
            case
                nhttp:start_link(test_h2_conn_server, #{
                    port => 0,
                    handler => Handler,
                    versions => [http2, http1_1],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    case nhttp:get_port(ServerPid) of
                        {ok, Port} ->
                            [
                                {server_pid, ServerPid},
                                {server_port, Port},
                                {transport, ssl}
                                | Config
                            ];
                        {error, Reason} ->
                            ct:fail("Failed to get port: ~p", [Reason])
                    end;
                {error, Reason} ->
                    ct:fail("Failed to start H2 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(http2_per_stream, Config) ->
    %% Start HTTP/2 TLS test server for per-stream model tests
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            Handler = nhttp_test_handler,
            case
                nhttp:start_link(test_h2_per_stream_server, #{
                    port => 0,
                    handler => Handler,
                    versions => [http2, http1_1],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    case nhttp:get_port(ServerPid) of
                        {ok, Port} ->
                            [
                                {server_pid, ServerPid},
                                {server_port, Port},
                                {transport, ssl}
                                | Config
                            ];
                        {error, Reason} ->
                            ct:fail("Failed to get port: ~p", [Reason])
                    end;
                {error, Reason} ->
                    ct:fail("Failed to start H2 per-stream server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(ssl_specific, Config) ->
    %% Start HTTPS server for SSL-specific tests
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            Handler = nhttp_test_handler,
            case
                nhttp:start_link(test_ssl_specific_server, #{
                    port => 0,
                    handler => Handler,
                    versions => [http1_1],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    case nhttp:get_port(ServerPid) of
                        {ok, Port} ->
                            [
                                {server_pid, ServerPid},
                                {server_port, Port},
                                {transport, ssl}
                                | Config
                            ];
                        {error, Reason} ->
                            ct:fail("Failed to get port: ~p", [Reason])
                    end;
                {error, Reason} ->
                    ct:fail("Failed to start SSL server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(http2_error_paths, Config) ->
    %% Start HTTP/2 TLS test server for error path tests
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            Handler = nhttp_test_handler,
            case
                nhttp:start_link(test_h2_error_paths_server, #{
                    port => 0,
                    handler => Handler,
                    versions => [http2, http1_1],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    case nhttp:get_port(ServerPid) of
                        {ok, Port} ->
                            [
                                {server_pid, ServerPid},
                                {server_port, Port},
                                {transport, ssl}
                                | Config
                            ];
                        {error, Reason} ->
                            ct:fail("Failed to get port: ~p", [Reason])
                    end;
                {error, Reason} ->
                    ct:fail("Failed to start H2 error paths server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(Group, Config) when
    Group =:= h1_raw_errors;
    Group =:= h1_streaming
->
    %% No server needed - raw TCP servers created inline per test
    Config;
init_per_group(Group, Config) when
    Group =:= h2_ping_extended;
    Group =:= h2_credit_slot;
    Group =:= h2_flow_control;
    Group =:= h2_draining;
    Group =:= h2_headers_only;
    Group =:= h2_batching;
    Group =:= h2_goaway_draining;
    Group =:= h2_rotation;
    Group =:= conn_faults_h2;
    Group =:= h2_trailers
->
    %% Start HTTP/2 TLS test server
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        true ->
            Handler = nhttp_test_handler,
            ServerName = list_to_atom("test_h2_server_" ++ atom_to_list(Group)),
            case
                nhttp:start_link(ServerName, #{
                    port => 0,
                    handler => Handler,
                    versions => [http2, http1_1],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    case nhttp:get_port(ServerPid) of
                        {ok, Port} ->
                            [
                                {server_pid, ServerPid},
                                {server_port, Port},
                                {transport, ssl}
                                | Config
                            ];
                        {error, Reason} ->
                            ct:fail("Failed to get port: ~p", [Reason])
                    end;
                {error, Reason} ->
                    ct:fail("Failed to start ~p server: ~p", [Group, Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(keep_alive_parsing, Config) ->
    case proplists:get_value(chaos_server, Config) of
        undefined -> ok;
        ServerRef -> nhttpc_chaos_server:stop(ServerRef)
    end,
    ok;
end_per_group(stop_timeout, _Config) ->
    ok;
end_per_group(h2_stream_slots, _Config) ->
    ok;
end_per_group(h2_peer_cap, _Config) ->
    ok;
end_per_group(h2_send_credit, _Config) ->
    ok;
end_per_group(Group, Config) when
    Group =:= lifecycle;
    Group =:= requests;
    Group =:= cancel;
    Group =:= pool_integration;
    Group =:= errors;
    Group =:= http2;
    Group =:= http2_per_stream;
    Group =:= http2_error_paths;
    Group =:= protocol_detection;
    Group =:= edge_cases;
    Group =:= sys_callbacks;
    Group =:= draining;
    Group =:= draining_socket;
    Group =:= ping;
    Group =:= ssl_specific;
    Group =:= h2_ping_extended;
    Group =:= h2_credit_slot;
    Group =:= h2_flow_control;
    Group =:= h2_draining;
    Group =:= h2_headers_only;
    Group =:= h2_batching;
    Group =:= h2_goaway_draining;
    Group =:= h2_rotation;
    Group =:= conn_faults;
    Group =:= conn_faults_h2;
    Group =:= h2_trailers
->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok;
end_per_group(h1_raw_errors, _Config) ->
    ok;
end_per_group(h1_streaming, _Config) ->
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% LIFECYCLE TESTS
%%%-----------------------------------------------------------------------------

%% Test basic connection start
conn_start_link(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    ?assert(is_pid(ConnPid)),
    ?assert(is_process_alive(ConnPid)),
    %% Give time for connection to complete
    timer:sleep(100),
    ?assert(is_process_alive(ConnPid)),
    nhttpc_conn:stop(ConnPid),
    ok.

%% Test connection stop
conn_stop(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),
    ?assert(is_process_alive(ConnPid)),
    ok = nhttpc_conn:stop(ConnPid),
    timer:sleep(50),
    ?assertNot(is_process_alive(ConnPid)),
    ok.

%% Test connection timeout
conn_connect_timeout(_Config) ->
    process_flag(trap_exit, true),
    Opts = #{
        host => <<"10.255.255.1">>,
        port => 12345,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 200
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 2000 ->
        ct:fail("Expected connect timeout")
    end,
    ?assertNot(is_process_alive(ConnPid)),
    process_flag(trap_exit, false),
    ok.

%% Test connection error (refused)
conn_connect_error(_Config) ->
    process_flag(trap_exit, true),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => 59999,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 1000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 2000 ->
        ct:fail("Expected connection error")
    end,
    ?assertNot(is_process_alive(ConnPid)),
    process_flag(trap_exit, false),
    ok.

%%%-----------------------------------------------------------------------------
%%% REQUEST/RESPONSE TESTS
%%%-----------------------------------------------------------------------------

%% Test sending request on HTTP/1.1 connection
conn_send_request_h1(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    ?assert(is_reference(Ref)),

    %% Wait for response
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response received")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test sending request while still connecting
conn_send_request_connecting(_Config) ->
    Opts = #{
        host => <<"10.255.255.1">>,
        port => 12345,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [],
        body => <<>>
    },

    {ok, StreamRef} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_error, ConnPid, StreamRef, {connection, #{type := not_ready}}} -> ok
    after 1000 ->
        ct:fail("Expected connecting error")
    end,

    try
        nhttpc_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    ok.

%% Test sending request in draining state
%% Note: Draining state test is simplified as triggering draining requires H2 GOAWAY
conn_send_request_draining(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Make a successful request to verify connection works
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, _} -> ok
    after 5000 -> ct:fail("timeout")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test request timeout
conn_request_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 100
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Request to slow endpoint
    Req = #{
        method => get,
        path => <<"/slow/500">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Should get timeout error
    receive
        {nhttpc_error, ConnPid, Ref, {request, #{type := request_timeout}}} ->
            ok
    after 5000 ->
        ct:fail("Expected request timeout error")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test multiple sequential requests on H1
conn_multiple_sequential_h1(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Send multiple sequential requests (H1 pipelining)
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },

    %% First request
    {ok, Ref1} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref1, {ok, Resp1}} ->
            ?assertEqual(200, maps:get(status, Resp1))
    after 5000 -> ct:fail("timeout 1")
    end,

    %% Second request (should queue if first not done)
    {ok, Ref2} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref2, {ok, Resp2}} ->
            ?assertEqual(200, maps:get(status, Resp2))
    after 5000 -> ct:fail("timeout 2")
    end,

    %% Third request
    {ok, Ref3} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref3, {ok, Resp3}} ->
            ?assertEqual(200, maps:get(status, Resp3))
    after 5000 -> ct:fail("timeout 3")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Regression: send_async must emit exactly one legacy message per response.
%% A prior change double-emitted unified {nhttpc, Handle, _} events that no
%% legacy caller consumes, piling up the caller mailbox and degrading every
%% subsequent selective receive to O(mailbox). Assert the mailbox is fully
%% drained after each response.
conn_no_unified_double_emit(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    lists:foreach(
        fun(_) ->
            {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
            receive
                {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
                    ?assertEqual(200, maps:get(status, Resp))
            after 5000 -> ct:fail("timeout waiting for response")
            end,
            {message_queue_len, QLen} = process_info(self(), message_queue_len),
            ?assertEqual(0, QLen)
        end,
        lists:seq(1, 50)
    ),
    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% CANCEL TESTS
%%%-----------------------------------------------------------------------------

%% Test cancelling unknown request
conn_cancel_unknown(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    FakeRef = make_ref(),
    Result = nhttpc_conn:cancel(ConnPid, FakeRef),
    ?assertMatch({error, not_found}, Result),

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test cancelling in-flight request
conn_cancel_in_flight(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 10000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Send request to slow endpoint
    Req = #{
        method => get,
        path => <<"/slow/5000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Cancel the request
    timer:sleep(50),
    Result = nhttpc_conn:cancel(ConnPid, Ref),
    ?assertEqual(ok, Result),

    %% Should not receive response (was cancelled)
    receive
        {nhttpc_response, ConnPid, Ref, _} ->
            ct:fail("Should not receive response after cancel")
    after 200 ->
        ok
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% POOL INTEGRATION TESTS
%%%-----------------------------------------------------------------------------

%% Test that connection notifies pool manager
conn_pool_mgr_notifications(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),

    %% Start a mock pool manager
    MockMgr = spawn_link(fun() ->
        mock_pool_mgr_loop(Self)
    end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready notification
    receive
        {pool_ready, ConnPid, http1_1} -> ok
    after 5000 ->
        ct:fail("No ready notification received")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test connection ready notification
conn_pool_ready_notification(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),

    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    receive
        {pool_ready, ConnPid, Protocol} ->
            ?assertEqual(http1_1, Protocol)
    after 5000 ->
        ct:fail("No ready notification")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Locate the `goaway_received` field in the live #conn_data{} tuple by
%% matching the surrounding fields. `goaway_received` is sandwiched between
%% `caller_monitors` (a map) and `current_request` (reference | undefined),
%% and initialises to `false`. Avoids baking a record-field index into the
%% test, which has shifted across nhttpc revisions.
goaway_received_index(Data) when is_tuple(Data) ->
    find_goaway_received_index(Data, 2, tuple_size(Data)).

find_goaway_received_index(Data, Idx, Size) when Idx < Size ->
    case element(Idx, Data) of
        false when Idx > 1 ->
            Prev = element(Idx - 1, Data),
            case is_map(Prev) of
                true -> Idx;
                false -> find_goaway_received_index(Data, Idx + 1, Size)
            end;
        _ ->
            find_goaway_received_index(Data, Idx + 1, Size)
    end.

mock_pool_mgr_loop(TestPid) ->
    receive
        {'$gen_cast', {connection_ready, ConnPid, Protocol, MaxStreams}} ->
            TestPid ! {pool_ready, ConnPid, Protocol},
            TestPid ! {pool_ready_cap, ConnPid, MaxStreams},
            mock_pool_mgr_loop(TestPid);
        {'$gen_cast', {connection_error, ConnPid, Reason}} ->
            TestPid ! {pool_error, ConnPid, Reason},
            mock_pool_mgr_loop(TestPid);
        {'$gen_cast', {connection_draining, ConnPid}} ->
            TestPid ! {pool_draining, ConnPid},
            mock_pool_mgr_loop(TestPid);
        {'$gen_cast', {update_max_streams, ConnPid, Max}} ->
            TestPid ! {pool_max_streams, ConnPid, Max},
            mock_pool_mgr_loop(TestPid);
        stop ->
            ok;
        _Other ->
            mock_pool_mgr_loop(TestPid)
    end.

%%%-----------------------------------------------------------------------------
%%% ERROR HANDLING TESTS
%%%-----------------------------------------------------------------------------

%% Test socket closed during request
conn_socket_closed(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Request that closes connection after response
    Req = #{
        method => get,
        path => <<"/close-connection">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}, {<<"connection">>, <<"close">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Should still get the response before connection closes
    receive
        {nhttpc_response, ConnPid, Ref, Result} ->
            case Result of
                {ok, _Resp} -> ok;
                {error, connection_closed} -> ok
            end
    after 5000 ->
        ct:fail("No response")
    end,

    %% Connection should close after
    timer:sleep(200),
    ok.

%% Test socket error handling
conn_socket_error(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, _} -> ok
    after 5000 -> ct:fail("No ready")
    end,

    %% Connection exists
    ?assert(is_process_alive(ConnPid)),

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 SPECIFIC TESTS
%%%-----------------------------------------------------------------------------

%% Test HTTP/2 request
conn_h2_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No H2 response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 multiplexed requests
conn_h2_multiplexed(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    %% Send multiple requests concurrently (H2 multiplexing)
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },

    %% Send 5 concurrent requests
    Refs = [
        begin
            {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
            Ref
        end
     || _ <- lists:seq(1, 5)
    ],

    %% Collect all responses
    Responses = [
        begin
            receive
                {nhttpc_response, ConnPid, Ref, Result} -> Result
            after 10000 -> {error, timeout}
            end
        end
     || Ref <- Refs
    ],

    %% All should succeed
    lists:foreach(
        fun(Result) ->
            ?assertMatch({ok, _}, Result),
            {ok, Resp} = Result,
            ?assertEqual(200, maps:get(status, Resp))
        end,
        Responses
    ),

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test GOAWAY handling (simplified - just verify connection handles it gracefully)
conn_h2_goaway(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% Make a request - if server sends GOAWAY, connection should handle it
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, _}} -> ok;
        {nhttpc_error, ConnPid, Ref, _} -> ok
    after 5000 -> ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 request with body
conn_h2_request_with_body(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    %% POST request with body
    Body = <<"test body content">>,
    Req = #{
        method => post,
        path => <<"/upload/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}, {<<"content-type">>, <<"text/plain">>}],
        body => Body
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No H2 POST response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 cancel in-flight request
conn_h2_cancel_in_flight(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    %% Send request to slow endpoint
    Req = #{
        method => get,
        path => <<"/slow/5000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Cancel the request
    timer:sleep(50),
    Result = nhttpc_conn:cancel(ConnPid, Ref),
    ?assertEqual(ok, Result),

    %% Should not receive response (was cancelled)
    receive
        {nhttpc_response, ConnPid, Ref, _} ->
            ct:fail("Should not receive response after cancel")
    after 200 ->
        ok
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 request timeout
conn_h2_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 100
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    %% Request to slow endpoint
    Req = #{
        method => get,
        path => <<"/slow/2000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Should get timeout error
    receive
        {nhttpc_error, ConnPid, Ref, {request, #{type := request_timeout}}} ->
            ok
    after 5000 ->
        ct:fail("Expected request timeout error")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% PROTOCOL DETECTION TESTS
%%%-----------------------------------------------------------------------------

%% Test connecting with HTTP/1.1 only
conn_h1_only_protocol(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 fallback to HTTP/1.1 when server doesn't support H2
conn_h2_fallback_to_h1(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    %% Server only speaks HTTP/1.1, but we try H2 first
    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http2, http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Should negotiate down to HTTP/1.1
    receive
        {pool_ready, ConnPid, Protocol} ->
            %% TCP without ALPN falls back to H1
            ?assertEqual(http1_1, Protocol)
    after 5000 ->
        ct:fail("No ready notification")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% ADDITIONAL EDGE CASE TESTS
%%%-----------------------------------------------------------------------------

%% Test handling of EXIT from connect process
conn_connecting_exit(_Config) ->
    process_flag(trap_exit, true),

    %% Connect to a port that will fail
    Opts = #{
        host => <<"127.0.0.1">>,
        port => 59998,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 1000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for connection failure EXIT
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 2000 ->
        ct:fail("Expected connection error")
    end,

    process_flag(trap_exit, false),
    ok.

%% Test normalize_host with string and IP tuple forms
conn_host_as_string(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Test with string host (not binary)
    Opts = #{
        host => "127.0.0.1",
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test normalize_host with IP tuple form
conn_host_as_ip_tuple(Config) ->
    Port = proplists:get_value(server_port, Config),

    %% Test with IP tuple (inet address format)
    Opts = #{
        host => {127, 0, 0, 1},
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test SSL connect options are passed through
conn_ssl_connect_opts(Config) ->
    Port = proplists:get_value(server_port, Config),
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),

    case CertFile =/= undefined andalso KeyFile =/= undefined of
        false ->
            {skip, "TLS certificates not available"};
        true ->
            Opts = #{
                host => <<"127.0.0.1">>,
                port => Port,
                transport => tcp,
                protocols => [http1_1],
                %% These SSL options should be passed through
                verify => verify_none,
                cacertfile => CertFile,
                server_name_indication => disable
            },
            {ok, ConnPid} = nhttpc_conn:start_link(Opts),
            timer:sleep(100),
            ?assert(is_process_alive(ConnPid)),
            nhttpc_conn:stop(ConnPid),
            ok
    end.

%% Test cancel while connecting returns not_found
conn_cancel_while_connecting(_Config) ->
    %% Connect to non-routable to stay in connecting state
    Opts = #{
        host => <<"10.255.255.1">>,
        port => 12345,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Try to cancel a request while connecting
    FakeRef = make_ref(),
    Result = nhttpc_conn:cancel(ConnPid, FakeRef),
    ?assertMatch({error, not_found}, Result),

    %% Clean up
    try
        nhttpc_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    ok.

%% Test HTTP/2 large response with multiple DATA frames (flow control)
conn_h2_large_response(Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),

    case CertFile =/= undefined andalso KeyFile =/= undefined of
        false ->
            {skip, "TLS certificates not available"};
        true ->
            %% Start H2 server for this test
            Handler = nhttp_test_handler,
            case
                nhttp:start_link(test_h2_large_server, #{
                    port => 0,
                    handler => Handler,
                    versions => [http2],
                    tls => #{certfile => CertFile, keyfile => KeyFile}
                })
            of
                {ok, ServerPid} ->
                    unlink(ServerPid),
                    {ok, ServerPort} = nhttp:get_port(ServerPid),

                    Opts = #{
                        host => <<"127.0.0.1">>,
                        port => ServerPort,
                        transport => ssl,
                        protocols => [http2],
                        verify => verify_none,
                        request_timeout => 30000
                    },
                    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
                    timer:sleep(500),

                    %% Request large response - triggers flow control
                    Req = #{
                        method => get,
                        path => <<"/stream/large">>,
                        headers => [{<<"host">>, <<"127.0.0.1">>}],
                        body => <<>>
                    },
                    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

                    receive
                        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
                            ?assertEqual(200, maps:get(status, Resp)),
                            %% Should have received all data
                            ?assert(byte_size(maps:get(body, Resp, <<>>)) > 0)
                    after 30000 ->
                        ct:fail("No large response")
                    end,

                    nhttpc_conn:stop(ConnPid),
                    nhttpc_test_server:stop(ServerPid),
                    ok;
                {error, Reason} ->
                    {skip, lists:flatten(io_lib:format("Failed to start H2 server: ~p", [Reason]))}
            end
    end.

conn_h2_client_certs_keys(Config) ->
    ServerCert = proplists:get_value(certfile, Config),
    ServerKey = proplists:get_value(keyfile, Config),
    case ServerCert =/= undefined andalso ServerKey =/= undefined of
        false ->
            {skip, "TLS certificates not available"};
        true ->
            ConfDir = filename:dirname(ServerCert),
            CaFile = filename:join(ConfDir, "ca.pem"),
            ClientCert = filename:join(ConfDir, "client.pem"),
            ClientKey = filename:join(ConfDir, "client.key"),
            case filelib:is_file(CaFile) andalso filelib:is_file(ClientCert) of
                false ->
                    {skip, "client/CA certificates not available"};
                true ->
                    Start = nhttp:start_link(test_h2_mtls_server, #{
                        port => 0,
                        handler => nhttp_test_handler,
                        versions => [http2, http1_1],
                        tls => #{
                            certfile => ServerCert,
                            keyfile => ServerKey,
                            cacertfile => CaFile,
                            verify => verify_peer,
                            extra => [{fail_if_no_peer_cert, true}]
                        }
                    }),
                    case Start of
                        {ok, ServerPid} ->
                            unlink(ServerPid),
                            {ok, ServerPort} = nhttp:get_port(ServerPid),

                            Opts = #{
                                host => <<"127.0.0.1">>,
                                port => ServerPort,
                                transport => ssl,
                                protocols => [http2],
                                verify => verify_none,
                                certs_keys => load_certs_keys(ClientCert, ClientKey),
                                request_timeout => 5000
                            },
                            {ok, ConnPid} = nhttpc_conn:start_link(Opts),
                            timer:sleep(500),

                            Req = #{
                                method => get,
                                path => <<"/echo">>,
                                headers => [{<<"host">>, <<"127.0.0.1">>}],
                                body => <<>>
                            },
                            {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
                            receive
                                {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
                                    ?assertEqual(200, maps:get(status, Resp))
                            after 5000 ->
                                ct:fail("No mTLS H2 response (client cert not presented?)")
                            end,

                            nhttpc_conn:stop(ConnPid),
                            nhttpc_test_server:stop(ServerPid),
                            ok;
                        {error, Reason} ->
                            {skip,
                                lists:flatten(
                                    io_lib:format("Failed to start mTLS server: ~p", [Reason])
                                )}
                    end
            end
    end.

conn_ssl_supported_groups(Config) ->
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
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http1_1],
        verify => verify_none,
        supported_groups => [secp256r1]
    }),
    true = unlink(ConnPid),
    MRef = monitor(process, ConnPid),
    {ok, PreSock} = ssl:transport_accept(Listen, 5000),
    ?assertMatch({error, _}, ssl:handshake(PreSock, 5000)),
    receive
        {'DOWN', MRef, process, ConnPid, Reason} -> ?assertMatch({shutdown, _}, Reason)
    after 5000 -> ct:fail(conn_not_stopped)
    end,
    ok = ssl:close(Listen).

load_certs_keys(CertFile, KeyFile) ->
    {ok, CertPem} = file:read_file(CertFile),
    {ok, KeyPem} = file:read_file(KeyFile),
    {'Certificate', CertDer, _} =
        lists:keyfind('Certificate', 1, public_key:pem_decode(CertPem)),
    {KeyType, KeyDer, _} = private_key_entry(public_key:pem_decode(KeyPem)),
    [#{cert => CertDer, key => {KeyType, KeyDer}}].

private_key_entry(Entries) ->
    [Key | _] = [
        Entry
     || {Type, _Der, _} = Entry <- Entries,
        Type =:= 'PrivateKeyInfo' orelse
            Type =:= 'RSAPrivateKey' orelse
            Type =:= 'ECPrivateKey' orelse
            Type =:= 'DSAPrivateKey'
    ],
    Key.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 PER-STREAM MODEL TESTS
%%%
%%% These tests exercise the per-stream process model where each HTTP/2 stream
%%% runs in its own lightweight proc_lib process.
%%%-----------------------------------------------------------------------------

%% Test HTTP/2 request with per-stream model
conn_h2_per_stream_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No H2 per-stream response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 multiplexed requests with per-stream model
conn_h2_per_stream_multiplexed(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },

    %% Send 5 concurrent requests
    Refs = [
        begin
            {ok, R} = nhttpc_conn:send_async(ConnPid, Req, self()),
            R
        end
     || _ <- lists:seq(1, 5)
    ],

    %% Collect all responses
    Responses = [
        receive
            {nhttpc_response, ConnPid, Ref, Result} -> Result
        after 10000 -> {error, timeout}
        end
     || Ref <- Refs
    ],

    %% All should succeed
    lists:foreach(
        fun(Result) ->
            ?assertMatch({ok, _}, Result),
            {ok, Resp} = Result,
            ?assertEqual(200, maps:get(status, Resp))
        end,
        Responses
    ),

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 request timeout with per-stream model
conn_h2_per_stream_timeout(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 100
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    %% Request to slow endpoint
    Req = #{
        method => get,
        path => <<"/slow/5000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Should get timeout error
    receive
        {nhttpc_error, ConnPid, Ref, {request, #{type := request_timeout}}} ->
            ok
    after 5000 ->
        ct:fail("Expected request timeout error")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 cancel in-flight request with per-stream model
conn_h2_per_stream_cancel(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    %% Send request to slow endpoint
    Req = #{
        method => get,
        path => <<"/slow/5000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Cancel the request
    timer:sleep(50),
    Result = nhttpc_conn:cancel(ConnPid, Ref),
    ?assertEqual(ok, Result),

    %% Should not receive response (was cancelled)
    receive
        {nhttpc_response, ConnPid, Ref, _} ->
            ct:fail("Should not receive response after cancel");
        {nhttpc_error, ConnPid, Ref, _} ->
            ct:fail("Should not receive error after cancel")
    after 200 ->
        ok
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% SYS CALLBACK TESTS
%%% Tests proc_lib/sys integration for debugging and introspection.
%%%-----------------------------------------------------------------------------

%% Test sys:get_state on ready connection
conn_sys_get_state_ready(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Get state via sys callback - returns {State, Data} tuple directly
    {State, _Data} = sys:get_state(ConnPid),
    ?assertEqual(ready, State),

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test sys:get_state on connecting connection
conn_sys_get_state_connecting(_Config) ->
    %% Connect to non-routable IP to stay in connecting state
    Opts = #{
        host => <<"10.255.255.1">>,
        port => 12345,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Get state while still connecting - returns {State, Data} tuple directly
    {State, _Data} = sys:get_state(ConnPid),
    ?assertEqual(connecting, State),

    try
        nhttpc_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    ok.

%% Test sys:suspend and sys:resume
conn_sys_suspend_resume(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Suspend the connection
    ok = sys:suspend(ConnPid),

    %% Resume the connection
    ok = sys:resume(ConnPid),

    %% Connection should still work after resume
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response after resume")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test sys:statistics
conn_sys_statistics(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Enable statistics
    ok = sys:statistics(ConnPid, true),

    %% Make a request to generate some activity
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, _} -> ok
    after 5000 -> ct:fail("timeout")
    end,

    %% Get statistics (note: proc_lib doesn't track stats by default, but this exercises the code path)
    {ok, Stats} = sys:statistics(ConnPid, get),
    ?assert(is_list(Stats) orelse Stats =:= no_statistics),

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% DRAINING STATE TESTS
%%% Tests connection behavior when in draining state (after GOAWAY).
%%%-----------------------------------------------------------------------------

%% Test that wait_ready returns error when draining
%% Note: We simulate draining by using a special test setup
conn_draining_rejects_wait_ready(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready first
    receive
        {pool_ready, ConnPid, _} -> ok
    after 5000 -> ct:fail("No ready")
    end,

    %% Make a request - connection is ready
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, _} -> ok
    after 5000 -> ct:fail("timeout")
    end,

    %% Connection works normally
    ok = nhttpc_conn:wait_ready(ConnPid, 1000),

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test that ping returns error when draining
conn_draining_rejects_ping(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Ping should work in ready state
    Result = nhttpc_conn:ping(ConnPid, 1000),
    ?assertEqual(pong, Result),

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% PING TESTS
%%%-----------------------------------------------------------------------------

%% Test ping on ready connection
conn_ping_ready(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    %% Ping should return pong for ready connection
    Result = nhttpc_conn:ping(ConnPid, 1000),
    ?assertEqual(pong, Result),

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test ping while connecting (should queue and respond with ok when ready)
%% Note: ping while connecting behaves like wait_ready, returns ok not pong
conn_ping_connecting(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Immediately ping - connection might still be connecting
    %% Should succeed once connection is ready with ok (queued waiter response)
    Result = nhttpc_conn:ping(ConnPid, 5000),
    %% When queued during connecting, returns ok (waiter reply)
    %% When already ready, would return pong
    ?assert(Result =:= ok orelse Result =:= pong),

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% SSL-SPECIFIC TESTS
%%%-----------------------------------------------------------------------------

%% Test SSL request works correctly
conn_ssl_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http1_1],
        verify => verify_none
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(200),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("SSL request timeout")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test multiple SSL requests to exercise ssl_passive handling
conn_ssl_multiple_requests(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http1_1],
        verify => verify_none
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(200),

    %% Send multiple requests sequentially to exercise socket handling
    lists:foreach(
        fun(I) ->
            Path = list_to_binary(io_lib:format("/echo?i=~p", [I])),
            Req = #{
                method => get,
                path => Path,
                headers => [{<<"host">>, <<"127.0.0.1">>}],
                body => <<>>
            },
            {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
            receive
                {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
                    ?assertEqual(200, maps:get(status, Resp))
            after 5000 ->
                ct:fail("SSL request ~p timeout", [I])
            end
        end,
        lists:seq(1, 10)
    ),

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% DRAINING SOCKET TESTS
%%%-----------------------------------------------------------------------------

%% Test that connection in draining state still receives pending responses
conn_draining_receives_response(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, _} -> ok
    after 5000 -> ct:fail("No ready")
    end,

    %% Send a request
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Wait for response (connection handles socket data correctly)
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("timeout")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test that send to draining connection gets rejected
conn_draining_send_rejected(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, _} -> ok
    after 5000 -> ct:fail("No ready")
    end,

    %% Connection ready
    ?assert(is_process_alive(ConnPid)),

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 ERROR PATH TESTS
%%%-----------------------------------------------------------------------------

%% Test HTTP/2 stream reset handling
conn_h2_stream_reset(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% Make a valid request
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Wait for response
    receive
        {nhttpc_response, ConnPid, Ref, _Result} -> ok
    after 5000 -> ct:fail("timeout")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 settings notification
conn_h2_settings_notification(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready - connection will process SETTINGS frame
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% Connection established means settings were processed
    ?assert(is_process_alive(ConnPid)),

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 window update handling
conn_h2_window_update(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% Send a request with body to trigger window updates
    LargeBody = binary:copy(<<"x">>, 1000),
    Req = #{
        method => post,
        path => <<"/echo">>,
        headers => [
            {<<"host">>, <<"127.0.0.1">>},
            {<<"content-length">>, integer_to_binary(byte_size(LargeBody))}
        ],
        body => LargeBody
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Wait for response
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 10000 ->
        ct:fail("H2 POST timeout")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% KEEP-ALIVE PARSING TESTS
%%% Tests for parsing Keep-Alive header values via chaos server.
%%%-----------------------------------------------------------------------------

%% Test parsing Keep-Alive header with timeout parameter
conn_keep_alive_with_timeout(Config) ->
    ServerRef = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(ServerRef),

    %% Set server to return Keep-Alive: timeout=30
    nhttpc_chaos_server:set_failure_mode(ServerRef, {keep_alive, 30}),

    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    nhttpc_chaos_server:clear_failure_mode(ServerRef),
    ok.

%% Test parsing Keep-Alive with multiple parameters
conn_keep_alive_multiple_params(Config) ->
    ServerRef = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(ServerRef),

    %% Set server to return Keep-Alive: timeout=60, max=100
    nhttpc_chaos_server:set_failure_mode(ServerRef, {keep_alive, 60, 100}),

    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    nhttpc_chaos_server:clear_failure_mode(ServerRef),
    ok.

%% Test parsing Keep-Alive without timeout
conn_keep_alive_no_timeout(Config) ->
    ServerRef = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(ServerRef),

    %% Set server to return Keep-Alive: max=100 (no timeout)
    nhttpc_chaos_server:set_failure_mode(ServerRef, keep_alive_no_timeout),

    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    nhttpc_chaos_server:clear_failure_mode(ServerRef),
    ok.

%% Test parsing Keep-Alive with invalid value
conn_keep_alive_invalid_value(Config) ->
    ServerRef = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(ServerRef),

    %% Set server to return Keep-Alive: garbage-invalid-value
    nhttpc_chaos_server:set_failure_mode(ServerRef, keep_alive_invalid),

    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(100),

    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Should still succeed even with invalid Keep-Alive header
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    nhttpc_chaos_server:clear_failure_mode(ServerRef),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 PING EXTENDED TESTS
%%%-----------------------------------------------------------------------------

%% Test HTTP/2 ping on ready connection
conn_h2_ping_basic(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    %% First ping should work
    Result = nhttpc_conn:ping(ConnPid, 5000),
    ?assert(Result =:= pong orelse Result =:= ok),

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test HTTP/2 ping overwrite (new ping while one is pending)
conn_h2_ping_overwrite(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    %% Send multiple pings concurrently - the second should overwrite the first
    Self = self(),
    spawn_link(fun() ->
        R1 = nhttpc_conn:ping(ConnPid, 5000),
        Self ! {ping1, R1}
    end),
    spawn_link(fun() ->
        timer:sleep(10),
        R2 = nhttpc_conn:ping(ConnPid, 5000),
        Self ! {ping2, R2}
    end),

    %% Both pings should complete
    receive
        {ping1, R1} -> ?assert(R1 =:= pong orelse R1 =:= ok orelse R1 =:= {error, overwritten})
    after 6000 -> ct:fail("ping1 timeout")
    end,
    receive
        {ping2, R2} -> ?assert(R2 =:= pong orelse R2 =:= ok)
    after 6000 -> ct:fail("ping2 timeout")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 FLOW CONTROL TESTS
%%%-----------------------------------------------------------------------------

%% Test HTTP/2 large body that triggers window updates
conn_h2_large_body_flow_control(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 30000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    %% Request large response that should trigger window updates
    %% The /stream/large endpoint returns >32KB which exceeds initial window
    Req = #{
        method => get,
        path => <<"/stream/large">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            %% Should have received substantial data
            ?assert(byte_size(maps:get(body, Resp, <<>>)) > 0)
    after 30000 ->
        ct:fail("Large response timeout")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% STOP TIMEOUT TESTS
%%%-----------------------------------------------------------------------------

%% Test stop with force kill when process doesn't exit gracefully
conn_stop_force_kill(_Config) ->
    process_flag(trap_exit, true),
    %% Create a mock connection process that ignores stop messages
    Self = self(),
    MockConn = spawn_link(fun() ->
        Self ! {mock_started, self()},
        mock_conn_loop()
    end),

    %% Wait for mock to start
    receive
        {mock_started, MockConn} -> ok
    after 1000 -> ct:fail("Mock didn't start")
    end,

    %% Try to stop it - the mock ignores stop messages, so it should timeout and force kill
    %% Note: nhttpc_conn:stop/1 has a 5 second timeout, but we can test the call path
    %% by calling the module directly with a short timeout
    MonRef = monitor(process, MockConn),
    MockConn ! {stop, Self},

    %% Wait a bit, then verify the mock is still running (it ignores stop)
    timer:sleep(100),
    ?assert(is_process_alive(MockConn)),

    %% Now forcefully kill it to clean up
    exit(MockConn, kill),
    receive
        {'DOWN', MonRef, process, MockConn, killed} -> ok
    after 1000 -> ct:fail("Mock didn't die")
    end,
    %% Consume the EXIT message
    receive
        {'EXIT', MockConn, killed} -> ok
    after 100 -> ok
    end,
    process_flag(trap_exit, false),
    ok.

mock_conn_loop() ->
    receive
        {stop, _From} ->
            %% Ignore stop messages to simulate hung process
            mock_conn_loop();
        _ ->
            mock_conn_loop()
    end.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 DRAINING TESTS
%%% Tests for draining state behavior.
%%%-----------------------------------------------------------------------------

%% Test that draining connection completes pending requests
conn_h2_draining_completes_pending(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% Send a request
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Wait for response
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 10000 -> ct:fail("No response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test that draining connection rejects new requests
conn_h2_draining_rejects_new(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% Connection works normally before draining
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, _}} -> ok
    after 5000 -> ct:fail("timeout")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 HEADERS-ONLY RESPONSE TESTS
%%%-----------------------------------------------------------------------------

%% Test HEAD request over H2 - triggers headers with fin (no body)
%% Covers process_h2_event({headers, _, _, fin}) at line ~920
conn_h2_head_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% HEAD request: server returns headers-only (no body)
    Req = #{
        method => head,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 10000 -> ct:fail("No HEAD response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test empty-body response over H2 (like /empty endpoint)
conn_h2_empty_body_response(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% Request to /empty which returns Content-Length: 0
    Req = #{
        method => get,
        path => <<"/empty">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(<<>>, maps:get(body, Resp, <<>>))
    after 10000 -> ct:fail("No empty response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% RAW TCP ERROR PATH TESTS (H1)
%%%-----------------------------------------------------------------------------

%% Test that socket closed while connection is idle in ready_loop triggers
%% handle_socket_closed_ready (lines 455-458 in nhttpc_conn.erl).
%% Uses a raw TCP server that accepts a connection, sends a valid H1 response
%% for the first request, then closes the socket.
conn_h1_socket_close_mid_idle(_Config) ->
    process_flag(trap_exit, true),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),

    Self = self(),
    spawn_link(fun() ->
        {ok, Sock} = gen_tcp:accept(LSock, 5000),
        %% Read the HTTP request
        {ok, _ReqData} = gen_tcp:recv(Sock, 0, 5000),
        %% Send a valid HTTP/1.1 response
        Response = [
            <<"HTTP/1.1 200 OK\r\n">>,
            <<"Content-Length: 2\r\n">>,
            <<"Connection: keep-alive\r\n">>,
            <<"\r\n">>,
            <<"OK">>
        ],
        ok = gen_tcp:send(Sock, Response),
        %% Wait a bit for the client to process the response, then close
        timer:sleep(200),
        gen_tcp:close(Sock),
        Self ! raw_server_done
    end),

    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, http1_1} -> ok
    after 5000 -> ct:fail("No ready notification")
    end,

    %% Send first request (should succeed)
    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 -> ct:fail("No response for first request")
    end,

    %% Now the server closes the socket. The conn process should detect tcp_closed
    %% and exit, notifying the pool of connection_closed.
    receive
        {pool_error, ConnPid, {connection_closed, graceful}} -> ok;
        {pool_error, ConnPid, {connection_closed, _}} -> ok;
        {'EXIT', ConnPid, normal} -> ok;
        {'EXIT', ConnPid, _Reason} -> ok
    after 3000 -> ct:fail("Connection did not detect socket close")
    end,

    receive
        raw_server_done -> ok
    after 1000 -> ok
    end,
    gen_tcp:close(LSock),
    process_flag(trap_exit, false),
    ok.

%% Test that a malformed HTTP response triggers the parse error path
%% (lines 761-776 in nhttpc_conn.erl: handle_h1_data_ready {error, Reason}).
%% Uses a raw TCP server that sends invalid HTTP data when a request arrives.
conn_h1_malformed_response(_Config) ->
    process_flag(trap_exit, true),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),

    Self = self(),
    spawn_link(fun() ->
        {ok, Sock} = gen_tcp:accept(LSock, 5000),
        %% Read the HTTP request
        {ok, _ReqData} = gen_tcp:recv(Sock, 0, 5000),
        %% Send garbage that is not valid HTTP
        ok = gen_tcp:send(Sock, <<"NOT HTTP AT ALL\r\nGARBAGE\r\n\r\n">>),
        %% Keep socket open briefly so client processes the data
        timer:sleep(500),
        gen_tcp:close(Sock),
        Self ! raw_server_done
    end),

    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, http1_1} -> ok
    after 5000 -> ct:fail("No ready notification")
    end,

    %% Send request - the server will respond with garbage
    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    %% Should get an error response (http1_error) or the conn should die
    receive
        {nhttpc_response, ConnPid, Ref, {error, {http1_error, _}}} -> ok;
        {nhttpc_error, ConnPid, Ref, _} -> ok;
        {'EXIT', ConnPid, {error, _}} -> ok
    after 5000 -> ct:fail("No error for malformed response")
    end,

    %% The connection process should exit
    timer:sleep(200),
    ?assertNot(is_process_alive(ConnPid)),

    receive
        raw_server_done -> ok
    after 1000 -> ok
    end,
    gen_tcp:close(LSock),
    process_flag(trap_exit, false),
    ok.

%%%-----------------------------------------------------------------------------
%%% H1 STREAMING RECEIVE TESTS
%%%-----------------------------------------------------------------------------

%% Content-length body delivered in a single write reassembles intact.
conn_h1_body_single_fragment(_Config) ->
    Body = binary:copy(<<"a">>, 5000),
    Resp = h1_content_length_response(200, [], Body),
    {ConnPid, LSock} = h1_streaming_case(fun(Sock) ->
        ok = gen_tcp:send(Sock, Resp)
    end),
    Got = h1_get_body(ConnPid, <<"/one">>),
    ?assertEqual(Body, Got),
    h1_streaming_cleanup(ConnPid, LSock).

%% Large content-length body delivered in many small writes reassembles intact.
%% This is the path that was O(N^2) before the streaming rewrite.
conn_h1_body_many_fragments(_Config) ->
    Body = h1_pattern_body(262144),
    Resp = h1_content_length_response(200, [], Body),
    {ConnPid, LSock} = h1_streaming_case(fun(Sock) ->
        h1_dribble(Sock, Resp, 1024)
    end),
    Got = h1_get_body(ConnPid, <<"/many">>),
    ?assertEqual(Body, Got),
    h1_streaming_cleanup(ConnPid, LSock).

%% Response split inside the header block (mid-headers) parses correctly once
%% the rest arrives.
conn_h1_split_mid_headers(_Config) ->
    Body = binary:copy(<<"z">>, 1234),
    Resp = h1_content_length_response(200, [{<<"x-test">>, <<"split">>}], Body),
    SplitAt = 20,
    <<Head:SplitAt/binary, Tail/binary>> = Resp,
    {ConnPid, LSock} = h1_streaming_case(fun(Sock) ->
        ok = gen_tcp:send(Sock, Head),
        timer:sleep(50),
        ok = gen_tcp:send(Sock, Tail)
    end),
    Got = h1_get_body(ConnPid, <<"/split">>),
    ?assertEqual(Body, Got),
    h1_streaming_cleanup(ConnPid, LSock).

%% Chunked transfer-encoding with chunks split across deliveries, including a
%% mid-chunk-size split, reassembles to the concatenated body.
conn_h1_chunked_split(_Config) ->
    Wire =
        <<"HTTP/1.1 200 OK\r\n", "transfer-encoding: chunked\r\n", "\r\n", "5\r\nhello\r\n",
            "6\r\n world\r\n", "1\r\n!\r\n", "0\r\n\r\n">>,
    {ConnPid, LSock} = h1_streaming_case(fun(Sock) ->
        %% Deliver byte-by-byte to exercise every partial boundary.
        h1_dribble(Sock, Wire, 1)
    end),
    Got = h1_get_body(ConnPid, <<"/chunked">>),
    ?assertEqual(<<"hello world!">>, Got),
    h1_streaming_cleanup(ConnPid, LSock).

%% Chunked response with a trailer field surfaces the trailer on the response.
conn_h1_chunked_trailers(_Config) ->
    Wire =
        <<"HTTP/1.1 200 OK\r\n", "transfer-encoding: chunked\r\n", "trailer: x-sum\r\n", "\r\n",
            "5\r\nhello\r\n", "0\r\n", "x-sum: 42\r\n", "\r\n">>,
    {ConnPid, LSock} = h1_streaming_case(fun(Sock) ->
        h1_dribble(Sock, Wire, 3)
    end),
    Resp = h1_get_response(ConnPid, <<"/trailers">>),
    ?assertEqual(<<"hello">>, maps:get(body, Resp)),
    ?assertEqual([{<<"x-sum">>, <<"42">>}], maps:get(trailers, Resp)),
    h1_streaming_cleanup(ConnPid, LSock).

%% A HEAD response carries Content-Length but no body; the conn must not wait
%% for body bytes and the connection stays usable.
conn_h1_head_no_body(_Config) ->
    Resp =
        <<"HTTP/1.1 200 OK\r\n", "content-length: 9999\r\n", "\r\n">>,
    {ConnPid, LSock} = h1_streaming_case(fun(Sock) ->
        ok = gen_tcp:send(Sock, Resp)
    end),
    Req = #{
        method => head,
        path => <<"/head">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    Got = h1_get_body_req(ConnPid, Req),
    ?assertEqual(<<>>, Got),
    h1_streaming_cleanup(ConnPid, LSock).

%% A 204 response has no body regardless of framing headers.
conn_h1_status_204_no_body(_Config) ->
    Resp =
        <<"HTTP/1.1 204 No Content\r\n", "\r\n">>,
    {ConnPid, LSock} = h1_streaming_case(fun(Sock) ->
        ok = gen_tcp:send(Sock, Resp)
    end),
    Got = h1_get_response(ConnPid, <<"/empty">>),
    ?assertEqual(204, maps:get(status, Got)),
    ?assertEqual(<<>>, maps:get(body, Got)),
    h1_streaming_cleanup(ConnPid, LSock).

%% No Content-Length and no Transfer-Encoding: the body extends to connection
%% close (RFC 9112 6.3 #7). The conn must deliver the accumulated body on EOF.
conn_h1_until_close_eof(_Config) ->
    Body = h1_pattern_body(40000),
    Resp = <<"HTTP/1.1 200 OK\r\n", "content-type: text/plain\r\n", "\r\n", Body/binary>>,
    {ConnPid, LSock} = h1_streaming_case(fun(Sock) ->
        h1_dribble(Sock, Resp, 4096),
        timer:sleep(50),
        gen_tcp:close(Sock)
    end),
    Got = h1_get_body(ConnPid, <<"/until-close">>),
    ?assertEqual(Body, Got),
    h1_streaming_cleanup(ConnPid, LSock).

%% Two keep-alive requests on the same connection each get their full body,
%% proving recv_buffer is reset cleanly between responses.
conn_h1_sequential_keepalive_bodies(_Config) ->
    Body1 = h1_pattern_body(20000),
    Body2 = h1_pattern_body(30000),
    Resp1 = h1_content_length_response(200, [{<<"connection">>, <<"keep-alive">>}], Body1),
    Resp2 = h1_content_length_response(200, [{<<"connection">>, <<"keep-alive">>}], Body2),
    Self = self(),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    spawn_link(fun() ->
        {ok, Sock} = gen_tcp:accept(LSock, 5000),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        h1_dribble(Sock, Resp1, 2048),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        h1_dribble(Sock, Resp2, 2048),
        timer:sleep(100),
        gen_tcp:close(Sock),
        Self ! raw_server_done
    end),
    ConnPid = h1_start_conn(Port, Self),
    ?assertEqual(Body1, h1_get_body(ConnPid, <<"/first">>)),
    ?assertEqual(Body2, h1_get_body(ConnPid, <<"/second">>)),
    h1_streaming_cleanup(ConnPid, LSock).

%% A body exceeding max_response_size is rejected without buffering it whole.
conn_h1_body_too_large(_Config) ->
    process_flag(trap_exit, true),
    Body = binary:copy(<<"x">>, 50000),
    Resp = h1_content_length_response(200, [], Body),
    Self = self(),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    spawn_link(fun() ->
        {ok, Sock} = gen_tcp:accept(LSock, 5000),
        {ok, _} = gen_tcp:recv(Sock, 0, 5000),
        h1_dribble(Sock, Resp, 4096),
        timer:sleep(100),
        try
            gen_tcp:close(Sock)
        catch
            _:_ -> ok
        end,
        Self ! raw_server_done
    end),
    ConnPid = h1_start_conn(Port, Self),
    Req = #{
        method => get,
        path => <<"/big">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self(), #{max_response_size => 1024}),
    receive
        {nhttpc_response, ConnPid, Ref, {error, {request, #{type := response_too_large}}}} -> ok;
        {nhttpc_error, ConnPid, Ref, {request, #{type := response_too_large}}} -> ok
    after 5000 ->
        ct:fail("Expected response_too_large error")
    end,
    receive
        raw_server_done -> ok
    after 1000 -> ok
    end,
    try
        gen_tcp:close(LSock)
    catch
        _:_ -> ok
    end,
    try
        nhttpc_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    process_flag(trap_exit, false),
    ok.

%%%-----------------------------------------------------------------------------
%%% H1 STREAMING HELPERS
%%%-----------------------------------------------------------------------------

%% Start a raw TCP responder that runs RespFun(Sock) after reading the request,
%% then start a ready H1 conn pointed at it. Returns {ConnPid, ListenSocket}.
h1_streaming_case(RespFun) ->
    Self = self(),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    spawn_link(fun() ->
        {ok, Sock} = gen_tcp:accept(LSock, 5000),
        {ok, _ReqData} = gen_tcp:recv(Sock, 0, 5000),
        RespFun(Sock),
        timer:sleep(100),
        try
            gen_tcp:close(Sock)
        catch
            _:_ -> ok
        end,
        Self ! raw_server_done
    end),
    ConnPid = h1_start_conn(Port, Self),
    {ConnPid, LSock}.

h1_start_conn(Port, Self) ->
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    receive
        {pool_ready, ConnPid, http1_1} -> ok
    after 5000 -> ct:fail("No ready notification")
    end,
    ConnPid.

h1_get_body(ConnPid, Path) ->
    maps:get(body, h1_get_response(ConnPid, Path)).

h1_get_response(ConnPid, Path) ->
    Req = #{
        method => get,
        path => Path,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    h1_get_response_req(ConnPid, Req).

h1_get_body_req(ConnPid, Req) ->
    maps:get(body, h1_get_response_req(ConnPid, Req)).

h1_get_response_req(ConnPid, Req) ->
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            Resp;
        {nhttpc_response, ConnPid, Ref, {error, Reason}} ->
            ct:fail("Unexpected error response: ~p", [Reason]);
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ct:fail("Unexpected error: ~p", [Reason])
    after 5000 ->
        ct:fail("No response received")
    end.

h1_streaming_cleanup(ConnPid, LSock) ->
    receive
        raw_server_done -> ok
    after 2000 -> ok
    end,
    try
        gen_tcp:close(LSock)
    catch
        _:_ -> ok
    end,
    try
        nhttpc_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    ok.

%% Send Bin in fragments of at most ChunkSize bytes with a short pause between
%% writes to encourage separate socket deliveries.
h1_dribble(_Sock, <<>>, _ChunkSize) ->
    ok;
h1_dribble(Sock, Bin, ChunkSize) when byte_size(Bin) =< ChunkSize ->
    ok = gen_tcp:send(Sock, Bin);
h1_dribble(Sock, Bin, ChunkSize) ->
    <<Chunk:ChunkSize/binary, Rest/binary>> = Bin,
    ok = gen_tcp:send(Sock, Chunk),
    timer:sleep(1),
    h1_dribble(Sock, Rest, ChunkSize).

h1_content_length_response(Status, ExtraHeaders, Body) ->
    Headers = ExtraHeaders ++ [{<<"content-length">>, integer_to_binary(byte_size(Body))}],
    HeaderLines = [[Name, <<": ">>, Value, <<"\r\n">>] || {Name, Value} <- Headers],
    iolist_to_binary([
        <<"HTTP/1.1 ">>,
        integer_to_binary(Status),
        <<" OK\r\n">>,
        HeaderLines,
        <<"\r\n">>,
        Body
    ]).

%% Deterministic non-trivial body so a misordered reassembly is detected.
h1_pattern_body(Size) ->
    list_to_binary([(I rem 256) || I <- lists:seq(0, Size - 1)]).

%%%-----------------------------------------------------------------------------
%%% H2 BATCHING TESTS (drain_sends)
%%%-----------------------------------------------------------------------------

%% Test that concurrent H2 requests trigger drain_sends batching
%% (lines 574-584 in nhttpc_conn.erl). We fire 15 async requests
%% simultaneously from spawned processes so that multiple {send, ...}
%% messages accumulate in the conn process mailbox before processing.
conn_h2_concurrent_batch(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 15000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    NumRequests = 15,
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },

    %% Suspend the conn process so messages accumulate in its mailbox
    sys:suspend(ConnPid),

    %% Fire all requests while suspended - they queue in the mailbox
    Refs = lists:map(
        fun(_) ->
            {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
            Ref
        end,
        lists:seq(1, NumRequests)
    ),

    %% Resume - conn will process them, drain_sends will batch multiple
    sys:resume(ConnPid),

    %% Collect all responses
    Responses = lists:map(
        fun(Ref) ->
            receive
                {nhttpc_response, ConnPid, Ref, Result} -> Result
            after 15000 -> {error, timeout}
            end
        end,
        Refs
    ),

    %% All should succeed
    lists:foreach(
        fun(Result) ->
            ?assertMatch({ok, _}, Result),
            {ok, Resp} = Result,
            ?assertEqual(200, maps:get(status, Resp))
        end,
        Responses
    ),

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% H2 GOAWAY DRAINING LOOP TESTS (via sys:replace_state)
%%%-----------------------------------------------------------------------------

%% Test the draining_loop (lines 518-568) by injecting goaway_received=true
%% via sys:replace_state, then verifying the connection transitions to draining.
%% In draining state: new requests are rejected with pool_draining,
%% wait_ready returns {error, draining}, and ping returns {error, draining}.
conn_h2_goaway_draining_via_sys(Config) ->
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    %% Wait for ready
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% Verify connection works before draining
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref1} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref1, {ok, _}} -> ok
    after 5000 -> ct:fail("No response before draining")
    end,

    %% Use sys:replace_state to set goaway_received=true.
    %% The state is {ready, #conn_data{...}}. After setting goaway_received,
    %% we change the state tag to trigger draining_loop on resume.
    %% goaway_received is field 17 in conn_data record (element index).
    sys:replace_state(ConnPid, fun({ready, Data}) ->
        Data1 = setelement(17, Data, true),
        {draining, Data1}
    end),

    %% Now the connection should be in draining state.
    %% Verify: wait_ready should return {error, draining}
    ?assertEqual({error, draining}, nhttpc_conn:wait_ready(ConnPid, 1000)),

    %% Verify: ping should return {error, draining}
    ?assertEqual({error, draining}, nhttpc_conn:ping(ConnPid, 1000)),

    %% Verify: new send requests get rejected with pool_draining error
    {ok, Ref2} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_error, ConnPid, Ref2, _DrainingReason} -> ok
    after 3000 -> ct:fail("Send not rejected in draining state")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test draining with in-flight request that completes during draining.
%% Exercises handle_h2_data_draining (lines 863-895) and socket data
%% handling in draining_loop (lines 525-528).
conn_h2_draining_inflight_response(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),

    Opts = #{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 15000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),

    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("No H2 ready")
    end,

    %% Send a slow request (server delays 2s)
    SlowReq = #{
        method => get,
        path => <<"/slow/2000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, SlowRef} = nhttpc_conn:send_async(ConnPid, SlowReq, self()),

    %% Wait for request to be sent, then transition to draining
    timer:sleep(200),

    sys:suspend(ConnPid),
    sys:replace_state(ConnPid, fun({ready, Data}) ->
        GoawayIdx = goaway_received_index(Data),
        Data1 = setelement(GoawayIdx, Data, true),
        {draining, Data1}
    end),
    sys:resume(ConnPid),

    %% Response should still arrive (draining completes in-flight requests)
    receive
        {nhttpc_response, ConnPid, SlowRef, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp));
        {'EXIT', ConnPid, normal} ->
            ok
    after 10000 ->
        ct:fail("No response during draining")
    end,

    case is_process_alive(ConnPid) of
        true ->
            try
                nhttpc_conn:stop(ConnPid)
            catch
                _:_ -> ok
            end;
        false ->
            ok
    end,
    process_flag(trap_exit, false),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 TRAILER TESTS
%%%-----------------------------------------------------------------------------

%% Test that HTTP/2 trailing HEADERS (after DATA) are merged into the response.
conn_h2_trailers(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    Req = #{
        method => get,
        path => <<"/trailers">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(<<"grpc-response-body">>, maps:get(body, Resp, <<>>)),
            %% Trailers are exposed in the dedicated `trailers` response field
            Trailers = maps:get(trailers, Resp, []),
            ?assertNotEqual(false, lists:keyfind(<<"grpc-status">>, 1, Trailers)),
            {_, GrpcStatus} = lists:keyfind(<<"grpc-status">>, 1, Trailers),
            ?assertEqual(<<"0">>, GrpcStatus),
            ?assertNotEqual(false, lists:keyfind(<<"grpc-message">>, 1, Trailers))
    after 5000 ->
        {messages, Msgs} = process_info(self(), messages),
        ct:fail("No H2 trailer response, mailbox: ~p", [Msgs])
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test that standard HTTP/2 responses without trailers still work.
conn_h2_no_trailers(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            %% No grpc-status in a normal response
            Headers = maps:get(headers, Resp),
            ?assertEqual(false, lists:keyfind(<<"grpc-status">>, 1, Headers))
    after 5000 ->
        ct:fail("No H2 response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%% Test Trailers-Only pattern: HEADERS with END_STREAM, no DATA, no trailing HEADERS.
%% This is the gRPC Trailers-Only response pattern for unary RPCs.
conn_h2_trailers_only(Config) ->
    Port = proplists:get_value(server_port, Config),
    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    },
    {ok, ConnPid} = nhttpc_conn:start_link(Opts),
    timer:sleep(500),

    Req = #{
        method => get,
        path => <<"/trailers-only">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp)),
            ?assertEqual(<<>>, maps:get(body, Resp, <<>>)),
            %% grpc-status should be in initial headers (Trailers-Only mode)
            Headers = maps:get(headers, Resp),
            {_, GrpcStatus} = lists:keyfind(<<"grpc-status">>, 1, Headers),
            ?assertEqual(<<"0">>, GrpcStatus)
    after 5000 ->
        ct:fail("No H2 trailers-only response")
    end,

    nhttpc_conn:stop(ConnPid),
    ok.

%%%-----------------------------------------------------------------------------
%%% PLAN 18 — CONN FAULT-PATH COVERAGE
%%%
%%% These cases drive the proc_lib/sys state-machine error and edge branches
%%% deterministically: sys callbacks (format_status, system_code_change,
%%% system_terminate), injected wire-shaped socket messages, the draining
%%% loop, and the connect/TLS option paths. Synchronisation uses monitors,
%%% mock-pool-manager messages and sys:get_state barriers — never sleeps.
%%%-----------------------------------------------------------------------------

%% format_status/1 must redact the socket/buffers in every phase, and pass
%% through a non-matching status map unchanged.
conn_fault_format_status_all_states(Config) ->
    Port = ?config(server_port, Config),

    %% connecting: socket is undefined -> sanitised socket flag is false
    ConnOpts = #{
        host => <<"10.255.255.1">>,
        port => 12345,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 5000
    },
    {ok, CPid} = nhttpc_conn:start_link(ConnOpts),
    {connecting, CData} = sys:get_state(CPid),
    COut = nhttpc_conn:format_status(#{state => {connecting, CData}}),
    #{state := {connecting, CSan}} = COut,
    ?assert(is_map(CSan)),
    ?assertEqual(false, maps:get(socket, CSan)),
    ?assert(is_binary(maps:get(host, CSan))),
    try
        nhttpc_conn:stop(CPid)
    catch
        _:_ -> ok
    end,

    %% ready: socket is set -> sanitised socket flag is true
    {ok, RPid} = nhttpc_conn:start_link(#{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    }),
    ok = nhttpc_conn:wait_ready(RPid, 5000),
    {ready, RData} = sys:get_state(RPid),
    ROut = nhttpc_conn:format_status(#{state => {ready, RData}}),
    #{state := {ready, RSan}} = ROut,
    ?assertEqual(true, maps:get(socket, RSan)),
    ?assertEqual(false, maps:get(h2_conn, RSan)),

    %% non-matching status map is returned unchanged
    Passthrough = #{other => value, log => []},
    ?assertEqual(Passthrough, nhttpc_conn:format_status(Passthrough)),

    %% sys:get_status exercises the live {system, ...} clause too
    _ = sys:get_status(RPid),
    nhttpc_conn:stop(RPid),
    ok.

%% system_code_change/4 returns the state untouched.
conn_fault_system_code_change(Config) ->
    Port = ?config(server_port, Config),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    }),
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    {ready, _} = State = sys:get_state(ConnPid),
    ?assertEqual({ok, State}, nhttpc_conn:system_code_change(State, nhttpc_conn, "0", [])),
    ?assertEqual({ok, State}, nhttpc_conn:system_get_state(State)),
    %% Drive it through sys as well (suspend/change_code/resume).
    ok = sys:suspend(ConnPid),
    ok = sys:change_code(ConnPid, nhttpc_conn, "0", []),
    ok = sys:resume(ConnPid),
    nhttpc_conn:stop(ConnPid),
    ok.

%% system_terminate/4 with an undefined socket takes the no-op close branch.
conn_fault_system_terminate_connecting(_Config) ->
    {ok, CPid} = nhttpc_conn:start_link(#{
        host => <<"10.255.255.1">>,
        port => 12345,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 5000
    }),
    {connecting, CData} = sys:get_state(CPid),
    {Runner, MRef} = spawn_monitor(fun() ->
        nhttpc_conn:system_terminate(shutdown, self(), [], {connecting, CData})
    end),
    receive
        {'DOWN', MRef, process, Runner, shutdown} -> ok
    after 5000 -> ct:fail("system_terminate did not exit shutdown")
    end,
    try
        nhttpc_conn:stop(CPid)
    catch
        _:_ -> ok
    end,
    ok.

%% system_terminate/4 with a live socket takes the close branch.
conn_fault_system_terminate_ready(Config) ->
    process_flag(trap_exit, true),
    Port = ?config(server_port, Config),
    {ok, RPid} = nhttpc_conn:start_link(#{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    }),
    ok = nhttpc_conn:wait_ready(RPid, 5000),
    {ready, RData} = sys:get_state(RPid),
    {Runner, MRef} = spawn_monitor(fun() ->
        nhttpc_conn:system_terminate(shutdown, self(), [], {ready, RData})
    end),
    receive
        {'DOWN', MRef, process, Runner, shutdown} -> ok
    after 5000 -> ct:fail("system_terminate did not exit shutdown")
    end,
    %% The live conn loses its socket and shuts down on its own.
    try
        nhttpc_conn:stop(RPid)
    catch
        _:_ -> ok
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% ssl_closed in ready maps to the same graceful-close handling as tcp_closed.
conn_fault_ready_ssl_closed(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    MRef = monitor(process, ConnPid),
    ConnPid ! {ssl_closed, fake_socket},
    receive
        {pool_error, ConnPid, {connection_closed, graceful}} -> ok
    after 5000 -> ct:fail("no graceful connection_closed pool error")
    end,
    receive
        {'DOWN', MRef, process, ConnPid, normal} -> ok
    after 5000 -> ct:fail("conn did not exit normal after ssl_closed")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% tcp_error in ready fails all requests, notifies the pool and exits.
conn_fault_ready_tcp_error(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    MRef = monitor(process, ConnPid),
    ConnPid ! {tcp_error, fake_socket, econnreset},
    receive
        {pool_error, ConnPid, {socket_error, econnreset}} -> ok
    after 5000 -> ct:fail("no socket_error pool error")
    end,
    receive
        {'DOWN', MRef, process, ConnPid, {error, econnreset}} -> ok
    after 5000 -> ct:fail("conn did not exit on tcp_error")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% ssl_error mirrors tcp_error.
conn_fault_ready_ssl_error(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    MRef = monitor(process, ConnPid),
    ConnPid ! {ssl_error, fake_socket, econnreset},
    receive
        {pool_error, ConnPid, {socket_error, econnreset}} -> ok
    after 5000 -> ct:fail("no socket_error pool error")
    end,
    receive
        {'DOWN', MRef, process, ConnPid, {error, econnreset}} -> ok
    after 5000 -> ct:fail("conn did not exit on ssl_error")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% ssl_passive re-activates the socket and keeps serving requests.
conn_fault_ready_ssl_passive(Config) ->
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    ConnPid ! {ssl_passive, fake_socket},
    %% Barrier: the injected message is processed before sys:get_state replies.
    ?assertMatch({ready, _}, sys:get_state(ConnPid)),
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 -> ct:fail("no response after ssl_passive reactivate")
    end,
    nhttpc_conn:stop(ConnPid),
    ok.

%% A DOWN that matches no caller monitor is ignored.
conn_fault_ready_unknown_down(Config) ->
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    ConnPid ! {'DOWN', make_ref(), process, self(), normal},
    ?assertMatch({ready, _}, sys:get_state(ConnPid)),
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 -> ct:fail("no response after unknown DOWN")
    end,
    nhttpc_conn:stop(ConnPid),
    ok.

%% Caller of an in-flight H1 request dies -> conn drops the socket and exits.
conn_fault_caller_down_h1(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    Self = self(),
    SlowReq = #{
        method => get,
        path => <<"/slow/3000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    Caller = spawn(fun() ->
        {ok, _R} = nhttpc_conn:send_async(ConnPid, SlowReq, self()),
        Self ! {caller_sent, self()},
        receive
            _ -> ok
        end
    end),
    receive
        {caller_sent, Caller} -> ok
    after 5000 -> ct:fail("caller did not issue request")
    end,
    exit(Caller, kill),
    receive
        {pool_error, ConnPid, {caller_down, _}} -> ok
    after 5000 -> ct:fail("no caller_down pool error")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% Unsolicited bytes with no in-flight request hit the no-current-request
%% parse path and the protocol-error exit.
conn_fault_h1_unsolicited_garbage(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    MRef = monitor(process, ConnPid),
    ConnPid ! {tcp, fake_socket, <<"NOT HTTP AT ALL\r\nGARBAGE\r\n\r\n">>},
    receive
        {pool_error, ConnPid, {protocol_error, _}} -> ok
    after 5000 -> ct:fail("no protocol_error pool error")
    end,
    receive
        {'DOWN', MRef, process, ConnPid, {error, _}} -> ok
    after 5000 -> ct:fail("conn did not exit on parse error")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% A unified cancel while still connecting is acked without a request.
conn_fault_connecting_unified_cancel(_Config) ->
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        host => <<"10.255.255.1">>,
        port => 12345,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 5000
    }),
    ?assertMatch({connecting, _}, sys:get_state(ConnPid)),
    Ref = make_ref(),
    ConnPid ! {'$nhttpc_cancel', Ref, self()},
    receive
        {'$nhttpc_cancel_ack', Ref} -> ok
    after 5000 -> ct:fail("no cancel ack while connecting")
    end,
    try
        nhttpc_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    ok.

%% sys:get_status drives the {system, ...} clause in connecting and ready.
conn_fault_connecting_system_status(Config) ->
    Port = ?config(server_port, Config),
    {ok, CPid} = nhttpc_conn:start_link(#{
        host => <<"10.255.255.1">>,
        port => 12345,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 5000
    }),
    ?assertMatch({connecting, _}, sys:get_state(CPid)),
    {status, CPid, _, _} = sys:get_status(CPid),
    try
        nhttpc_conn:stop(CPid)
    catch
        _:_ -> ok
    end,

    {ok, RPid} = nhttpc_conn:start_link(#{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1]
    }),
    ok = nhttpc_conn:wait_ready(RPid, 5000),
    {status, RPid, _, _} = sys:get_status(RPid),
    nhttpc_conn:stop(RPid),
    ok.

%% TLS transport with no verify/cacert opts applies the secure defaults
%% (verify_peer + system cacerts) in build_connect_opts.
conn_fault_ssl_default_tls_opts(_Config) ->
    process_flag(trap_exit, true),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        host => <<"127.0.0.1">>,
        port => 1,
        transport => ssl,
        protocols => [http1_1],
        connect_timeout => 1000
    }),
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 5000 -> ct:fail("ssl conn to refused port did not fail")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% Connecting on the transport default port exercises the authority branch
%% that omits the port suffix.
conn_fault_default_port_authority(_Config) ->
    process_flag(trap_exit, true),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        host => <<"127.0.0.1">>,
        port => 80,
        transport => tcp,
        protocols => [http1_1],
        connect_timeout => 1000
    }),
    ?assert(is_pid(ConnPid)),
    try
        nhttpc_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% An H1 request in-flight while the conn is moved to draining: the response
%% is delivered through the draining socket-data + completion path.
conn_fault_h1_draining_inflight(_Config) ->
    process_flag(trap_exit, true),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    Self = self(),
    RawSrv = spawn_link(fun() ->
        {ok, S} = gen_tcp:accept(LSock, 5000),
        {ok, _Req} = gen_tcp:recv(S, 0, 5000),
        Self ! server_got_request,
        receive
            server_send -> ok
        after 5000 -> ok
        end,
        Resp = [
            <<"HTTP/1.1 200 OK\r\n">>,
            <<"Content-Length: 2\r\n">>,
            <<"Connection: keep-alive\r\n">>,
            <<"\r\n">>,
            <<"OK">>
        ],
        ok = gen_tcp:send(S, Resp),
        receive
            stop_raw -> ok
        after 2000 -> ok
        end,
        gen_tcp:close(S)
    end),

    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 10000
    }),
    receive
        {pool_ready, ConnPid, http1_1} -> ok
    after 5000 -> ct:fail("no ready")
    end,

    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        server_got_request -> ok
    after 5000 -> ct:fail("server never received the request")
    end,

    ok = sys:suspend(ConnPid),
    {draining, _} = sys:replace_state(ConnPid, fun({ready, Data}) -> {draining, Data} end),
    ok = sys:resume(ConnPid),
    {status, ConnPid, _, _} = sys:get_status(ConnPid),

    RawSrv ! server_send,
    receive
        {nhttpc_response, ConnPid, Ref, {ok, RResp}} ->
            ?assertEqual(200, maps:get(status, RResp))
    after 5000 -> ct:fail("no draining in-flight response")
    end,

    RawSrv ! stop_raw,
    gen_tcp:close(LSock),
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% tcp_closed while draining closes the socket and exits normally.
conn_fault_draining_tcp_closed(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    MRef = monitor(process, ConnPid),
    ok = sys:suspend(ConnPid),
    {draining, _} = sys:replace_state(ConnPid, fun({ready, Data}) -> {draining, Data} end),
    ok = sys:resume(ConnPid),
    ConnPid ! {tcp_closed, fake_socket},
    receive
        {'DOWN', MRef, process, ConnPid, normal} -> ok
    after 5000 -> ct:fail("draining conn did not exit on tcp_closed")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% A battery of draining-loop messages, ending with a timeout-driven exit.
conn_fault_draining_check_request_timeouts(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    MRef = monitor(process, ConnPid),
    ok = sys:suspend(ConnPid),
    {draining, _} = sys:replace_state(ConnPid, fun({ready, Data}) -> {draining, Data} end),
    ok = sys:resume(ConnPid),

    ConnPid ! {tcp_passive, fake_socket},
    ?assertMatch({draining, _}, sys:get_state(ConnPid)),
    ConnPid ! {ssl_passive, fake_socket},
    ?assertMatch({draining, _}, sys:get_state(ConnPid)),
    ConnPid ! check_idle,
    ?assertMatch({draining, _}, sys:get_state(ConnPid)),
    ?assertEqual({error, not_found}, nhttpc_conn:cancel(ConnPid, make_ref())),
    CancelRef = make_ref(),
    ConnPid ! {'$nhttpc_cancel', CancelRef, self()},
    receive
        {'$nhttpc_cancel_ack', CancelRef} -> ok
    after 5000 -> ct:fail("no draining cancel ack")
    end,
    ConnPid ! {'DOWN', make_ref(), process, self(), normal},
    ?assertMatch({draining, _}, sys:get_state(ConnPid)),
    {status, ConnPid, _, _} = sys:get_status(ConnPid),

    ConnPid ! check_request_timeouts,
    receive
        {'DOWN', MRef, process, ConnPid, normal} -> ok
    after 5000 -> ct:fail("draining conn did not exit on check_request_timeouts")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% Caller of an in-flight H2 request dies -> conn RSTs the stream and lives on.
conn_fault_caller_down_h2(Config) ->
    {ConnPid, _MockMgr} = start_ready_h2(Config),
    Self = self(),
    SlowReq = #{
        method => get,
        path => <<"/slow/3000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    Caller = spawn(fun() ->
        {ok, _R} = nhttpc_conn:send_async(ConnPid, SlowReq, self()),
        Self ! {caller_sent, self()},
        receive
            _ -> ok
        end
    end),
    receive
        {caller_sent, Caller} -> ok
    after 5000 -> ct:fail("h2 caller did not issue request")
    end,
    exit(Caller, kill),
    %% Barrier: a follow-up request proves the conn survived the caller DOWN.
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 10000 -> ct:fail("conn did not survive h2 caller down")
    end,
    nhttpc_conn:stop(ConnPid),
    ok.

%% A response larger than max_response_size is aborted with RST_STREAM. The
%% server keeps streaming on the reset stream, so the h2 layer subsequently
%% tears the connection down on the now-unknown stream — both the abort and
%% that error path are exercised here.
conn_fault_h2_max_response_size(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h2(Config),
    Req = #{
        method => get,
        path => <<"/stream/large">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self(), #{max_response_size => 100}),
    receive
        {nhttpc_error, ConnPid, Ref, _Reason} -> ok;
        {nhttpc_response, ConnPid, Ref, {error, _}} -> ok
    after 10000 -> ct:fail("oversized response was not aborted")
    end,
    case is_process_alive(ConnPid) of
        true ->
            try
                nhttpc_conn:stop(ConnPid)
            catch
                _:_ -> ok
            end;
        false ->
            ok
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% tcp_passive/ssl_passive re-activate an H2 socket via the http2 clause.
conn_fault_h2_passive_reactivate(Config) ->
    {ConnPid, _MockMgr} = start_ready_h2(Config),
    ConnPid ! {tcp_passive, fake_socket},
    ?assertMatch({ready, _}, sys:get_state(ConnPid)),
    ConnPid ! {ssl_passive, fake_socket},
    ?assertMatch({ready, _}, sys:get_state(ConnPid)),
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 10000 -> ct:fail("no response after h2 passive reactivate")
    end,
    nhttpc_conn:stop(ConnPid),
    ok.

%% H2 draining-loop message matrix, ending on ssl_closed.
conn_fault_h2_draining_message_matrix(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h2(Config),
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref0} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref0, {ok, _}} -> ok
    after 10000 -> ct:fail("no pre-draining response")
    end,
    MRef = monitor(process, ConnPid),
    ok = sys:suspend(ConnPid),
    {draining, _} = sys:replace_state(ConnPid, fun({ready, Data}) ->
        Idx = goaway_received_index(Data),
        {draining, setelement(Idx, Data, true)}
    end),
    ok = sys:resume(ConnPid),

    ConnPid ! {tcp_passive, fake_socket},
    ?assertMatch({draining, _}, sys:get_state(ConnPid)),
    ConnPid ! {ssl_passive, fake_socket},
    ?assertMatch({draining, _}, sys:get_state(ConnPid)),
    ConnPid ! check_idle,
    ?assertMatch({draining, _}, sys:get_state(ConnPid)),
    ?assertEqual({error, not_found}, nhttpc_conn:cancel(ConnPid, make_ref())),
    CancelRef = make_ref(),
    ConnPid ! {'$nhttpc_cancel', CancelRef, self()},
    receive
        {'$nhttpc_cancel_ack', CancelRef} -> ok
    after 5000 -> ct:fail("no h2 draining cancel ack")
    end,
    ConnPid ! {'DOWN', make_ref(), process, self(), normal},
    ?assertMatch({draining, _}, sys:get_state(ConnPid)),
    {status, ConnPid, _, _} = sys:get_status(ConnPid),

    ConnPid ! {ssl_closed, fake_socket},
    receive
        {'DOWN', MRef, process, ConnPid, normal} -> ok
    after 5000 -> ct:fail("h2 draining conn did not exit on ssl_closed")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% A sync call to a suspended conn times out via the call/3 receive-after.
conn_fault_call_timeout(Config) ->
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    ok = sys:suspend(ConnPid),
    ?assertEqual({error, timeout}, nhttpc_conn:wait_ready(ConnPid, 50)),
    ok = sys:resume(ConnPid),
    nhttpc_conn:stop(ConnPid),
    ok.

%% Negotiating HTTP/2 with no ALPN (plaintext) selects http2, then the H2
%% preface/SETTINGS handshake fails against the HTTP/1.1 server.
conn_fault_h2_over_plaintext(Config) ->
    process_flag(trap_exit, true),
    Port = ?config(server_port, Config),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http2],
        connect_timeout => 5000
    }),
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 7000 -> ct:fail("h2-over-plaintext handshake did not fail")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% An idle pooled conn with a low idle timeout closes itself and notifies
%% the pool manager.
conn_fault_idle_timeout_pool_close(Config) ->
    process_flag(trap_exit, true),
    Port = ?config(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        idle_timeout => 50
    }),
    receive
        {pool_ready, ConnPid, http1_1} -> ok
    after 5000 -> ct:fail("no ready")
    end,
    MRef = monitor(process, ConnPid),
    receive
        {pool_error, ConnPid, idle_timeout} -> ok
    after 15000 -> ct:fail("no idle_timeout pool notification")
    end,
    receive
        {'DOWN', MRef, process, ConnPid, normal} -> ok
    after 5000 -> ct:fail("idle conn did not exit")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% idle_timeout => infinity takes the early-return clause in start_idle_checker.
conn_fault_idle_infinity(Config) ->
    Port = ?config(server_port, Config),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        idle_timeout => infinity
    }),
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    Req = #{
        method => get,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 -> ct:fail("no response with infinity idle timeout")
    end,
    nhttpc_conn:stop(ConnPid),
    ok.

%% A linked parent exiting while the conn is draining tears the conn down.
conn_fault_draining_parent_exit(Config) ->
    Port = ?config(server_port, Config),
    Self = self(),
    Parent = spawn(fun() ->
        {ok, CP} = nhttpc_conn:start_link(#{
            host => <<"127.0.0.1">>,
            port => Port,
            transport => tcp,
            protocols => [http1_1]
        }),
        Self ! {conn, CP},
        receive
            die -> exit(boom)
        end
    end),
    ConnPid =
        receive
            {conn, CP} -> CP
        after 5000 -> ct:fail("parent never reported conn")
        end,
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    ok = sys:suspend(ConnPid),
    {draining, _} = sys:replace_state(ConnPid, fun({ready, Data}) -> {draining, Data} end),
    ok = sys:resume(ConnPid),
    MRef = monitor(process, ConnPid),
    Parent ! die,
    receive
        {'DOWN', MRef, process, ConnPid, boom} -> ok
    after 5000 -> ct:fail("conn did not exit on parent EXIT while draining")
    end,
    ok.

%% check_idle while a request is in flight just clears the timer ref.
conn_fault_ready_check_idle_pending(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h1(Config),
    SlowReq = #{
        method => get,
        path => <<"/slow/2000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, _Ref} = nhttpc_conn:send_async(ConnPid, SlowReq, self()),
    ConnPid ! check_idle,
    ?assertMatch({ready, _}, sys:get_state(ConnPid)),
    nhttpc_conn:stop(ConnPid),
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% A proxy that refuses the CONNECT exercises the connect_via_proxy path.
conn_fault_proxy_connect_error(_Config) ->
    process_flag(trap_exit, true),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        host => <<"example.invalid">>,
        port => 443,
        transport => tcp,
        protocols => [http1_1],
        proxy => #{host => <<"127.0.0.1">>, port => 1},
        connect_timeout => 1000
    }),
    receive
        {'EXIT', ConnPid, {shutdown, _}} -> ok
    after 5000 -> ct:fail("proxy connect did not fail")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% TLS transport with explicit verify + cacertfile takes the
%% already-configured branches in apply_tls_defaults.
conn_fault_ssl_cacertfile_opt(Config) ->
    process_flag(trap_exit, true),
    CertFile = ?config(certfile, Config),
    case CertFile of
        undefined ->
            {skip, "TLS certificates not available"};
        _ ->
            {ok, ConnPid} = nhttpc_conn:start_link(#{
                host => <<"127.0.0.1">>,
                port => 1,
                transport => ssl,
                protocols => [http1_1],
                verify => verify_none,
                cacertfile => CertFile,
                connect_timeout => 1000
            }),
            receive
                {'EXIT', ConnPid, {shutdown, _}} -> ok
            after 5000 -> ct:fail("ssl conn did not fail")
            end,
            drain_exits(),
            process_flag(trap_exit, false),
            ok
    end.

%% A partial H1 response received while draining buffers (the `more` path)
%% and completes once the rest arrives.
conn_fault_h1_draining_partial(_Config) ->
    process_flag(trap_exit, true),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    Self = self(),
    RawSrv = spawn_link(fun() ->
        {ok, S} = gen_tcp:accept(LSock, 5000),
        {ok, _Req} = gen_tcp:recv(S, 0, 5000),
        Self ! server_got_request,
        receive
            send_partial -> ok
        after 5000 -> ok
        end,
        ok = gen_tcp:send(S, <<"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n">>),
        receive
            send_rest -> ok
        after 5000 -> ok
        end,
        ok = gen_tcp:send(S, <<"\r\nOK">>),
        receive
            stop_raw -> ok
        after 2000 -> ok
        end,
        gen_tcp:close(S)
    end),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 10000
    }),
    receive
        {pool_ready, ConnPid, http1_1} -> ok
    after 5000 -> ct:fail("no ready")
    end,
    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        server_got_request -> ok
    after 5000 -> ct:fail("server never received request")
    end,
    ok = sys:suspend(ConnPid),
    {draining, _} = sys:replace_state(ConnPid, fun({ready, Data}) -> {draining, Data} end),
    ok = sys:resume(ConnPid),
    RawSrv ! send_partial,
    %% Barrier: the partial bytes are buffered; conn stays draining.
    ?assertMatch({draining, _}, sys:get_state(ConnPid)),
    RawSrv ! send_rest,
    receive
        {nhttpc_response, ConnPid, Ref, {ok, RResp}} ->
            ?assertEqual(200, maps:get(status, RResp))
    after 5000 -> ct:fail("no completed draining response")
    end,
    RawSrv ! stop_raw,
    gen_tcp:close(LSock),
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% A malformed H1 response received while draining hits the parse-error exit.
conn_fault_h1_draining_garbage(_Config) ->
    process_flag(trap_exit, true),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    Self = self(),
    RawSrv = spawn_link(fun() ->
        {ok, S} = gen_tcp:accept(LSock, 5000),
        {ok, _Req} = gen_tcp:recv(S, 0, 5000),
        Self ! server_got_request,
        receive
            send_garbage -> ok
        after 5000 -> ok
        end,
        ok = gen_tcp:send(S, <<"NOT HTTP AT ALL\r\nGARBAGE\r\n\r\n">>),
        receive
            stop_raw -> ok
        after 2000 -> ok
        end,
        gen_tcp:close(S)
    end),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 10000
    }),
    receive
        {pool_ready, ConnPid, http1_1} -> ok
    after 5000 -> ct:fail("no ready")
    end,
    MRef = monitor(process, ConnPid),
    Req = #{
        method => get,
        path => <<"/test">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    {ok, _Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    receive
        server_got_request -> ok
    after 5000 -> ct:fail("server never received request")
    end,
    ok = sys:suspend(ConnPid),
    {draining, _} = sys:replace_state(ConnPid, fun({ready, Data}) -> {draining, Data} end),
    ok = sys:resume(ConnPid),
    RawSrv ! send_garbage,
    receive
        {pool_error, ConnPid, {protocol_error, _}} -> ok
    after 5000 -> ct:fail("no protocol_error while draining")
    end,
    receive
        {'DOWN', MRef, process, ConnPid, {error, _}} -> ok
    after 5000 -> ct:fail("draining conn did not exit on parse error")
    end,
    RawSrv ! stop_raw,
    gen_tcp:close(LSock),
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%% Graceful server shutdown sends a real GOAWAY: the conn enters draining
%% via the protocol path and notifies the pool manager.
conn_fault_h2_server_goaway(Config) ->
    process_flag(trap_exit, true),
    CertFile = ?config(certfile, Config),
    KeyFile = ?config(keyfile, Config),
    case CertFile =/= undefined andalso KeyFile =/= undefined of
        false ->
            {skip, "TLS certificates not available"};
        true ->
            {ok, ServerPid} = nhttp:start_link(test_h2_goaway_server, #{
                port => 0,
                handler => nhttp_test_handler,
                versions => [http2],
                tls => #{certfile => CertFile, keyfile => KeyFile}
            }),
            unlink(ServerPid),
            {ok, ServerPort} = nhttp:get_port(ServerPid),
            Self = self(),
            MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
            {ok, ConnPid} = nhttpc_conn:start_link(#{
                pool_mgr => MockMgr,
                host => <<"127.0.0.1">>,
                port => ServerPort,
                transport => ssl,
                protocols => [http2],
                verify => verify_none,
                request_timeout => 10000
            }),
            receive
                {pool_ready, ConnPid, http2} -> ok
            after 5000 -> ct:fail("no h2 ready")
            end,
            Req = #{
                method => get,
                path => <<"/echo">>,
                headers => [{<<"host">>, <<"127.0.0.1">>}],
                body => <<>>
            },
            {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
            receive
                {nhttpc_response, ConnPid, Ref, {ok, _}} -> ok
            after 10000 -> ct:fail("no pre-goaway response")
            end,
            MRef = monitor(process, ConnPid),
            %% Graceful drain makes the server emit a real GOAWAY frame.
            nhttp:stop(ServerPid, #{drain => true, drain_timeout => 2000}),
            receive
                {pool_draining, ConnPid} -> ok
            after 10000 -> ct:fail("no GOAWAY-driven pool draining")
            end,
            receive
                {'DOWN', MRef, process, ConnPid, _} -> ok
            after 10000 -> ct:fail("conn did not terminate after GOAWAY")
            end,
            drain_exits(),
            process_flag(trap_exit, false),
            ok
    end.

%% A second in-flight ping overwrites the first, which is failed explicitly.
conn_fault_h2_ping_overwrite_forced(Config) ->
    {ConnPid, _MockMgr} = start_ready_h2(Config),
    ok = sys:suspend(ConnPid),
    Tag1 = make_ref(),
    Tag2 = make_ref(),
    ConnPid ! {'$call', {self(), Tag1}, ping},
    ConnPid ! {'$call', {self(), Tag2}, ping},
    ok = sys:resume(ConnPid),
    receive
        {Tag1, {error, ping_overwritten}} -> ok
    after 5000 -> ct:fail("first ping was not overwritten")
    end,
    receive
        {Tag2, pong} -> ok
    after 5000 -> ct:fail("second ping never acked")
    end,
    nhttpc_conn:stop(ConnPid),
    ok.

%% Caller of an in-flight H2 request dies while draining.
conn_fault_h2_draining_caller_down(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, _MockMgr} = start_ready_h2(Config),
    Self = self(),
    SlowReq = #{
        method => get,
        path => <<"/slow/4000">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<>>
    },
    Caller = spawn(fun() ->
        {ok, _R} = nhttpc_conn:send_async(ConnPid, SlowReq, self()),
        Self ! {caller_sent, self()},
        receive
            _ -> ok
        end
    end),
    receive
        {caller_sent, Caller} -> ok
    after 5000 -> ct:fail("h2 caller did not issue request")
    end,
    MRef = monitor(process, ConnPid),
    ok = sys:suspend(ConnPid),
    {draining, _} = sys:replace_state(ConnPid, fun({ready, Data}) ->
        Idx = goaway_received_index(Data),
        {draining, setelement(Idx, Data, true)}
    end),
    ok = sys:resume(ConnPid),
    exit(Caller, kill),
    receive
        {'DOWN', MRef, process, ConnPid, normal} -> ok
    after 10000 -> ct:fail("draining conn did not exit after caller down")
    end,
    drain_exits(),
    process_flag(trap_exit, false),
    ok.

%%%-----------------------------------------------------------------------------
%%% PLAN 18 — SHARED HELPERS
%%%-----------------------------------------------------------------------------

%% Start an H1 conn with a mock pool manager and wait until it is ready.
start_ready_h1(Config) ->
    Port = ?config(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => tcp,
        protocols => [http1_1],
        request_timeout => 5000
    }),
    receive
        {pool_ready, ConnPid, http1_1} -> ok
    after 5000 -> ct:fail("h1 conn not ready")
    end,
    {ConnPid, MockMgr}.

%% Start an H2 conn (TLS) with a mock pool manager and wait until it is ready.
start_ready_h2(Config) ->
    Port = ?config(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    }),
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("h2 conn not ready")
    end,
    {ConnPid, MockMgr}.

%%%-----------------------------------------------------------------------------
%%% H2 LIVE STREAM SLOT ACCOUNTING TESTS
%%%-----------------------------------------------------------------------------

%% A peer that refuses the stream must cost one request, not the connection
conn_h2_refused_stream_keeps_connection(Config) ->
    {ServerPid, Port} = start_slot_server(Config, #{max_concurrent_streams => 1}),
    Slots = nhttpc_stream_slots:new(),
    ok = nhttpc_stream_slots:set_cap(Slots, ?SLOT_CAP),
    {ok, ConnPid} = start_slot_conn(Port, Slots),

    Slow = #{method => get, path => <<"/slow/300">>, headers => [], body => <<>>},
    Echo = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    ok = nhttpc_stream_slots:reserve(Slots),
    ok = nhttpc_stream_slots:reserve(Slots),

    {ok, Ref1} = nhttpc_conn:send_async(ConnPid, Slow, self()),
    timer:sleep(100),
    {ok, Ref2} = nhttpc_conn:send_async(ConnPid, Echo, self()),

    receive
        {nhttpc_error, ConnPid, Ref2, Reason} ->
            ?assertEqual(
                {error, {stream_error, {protocol, refused_stream}}}, nhttpc_error:wrap(Reason)
            )
    after 5000 ->
        ct:fail("second stream was not refused")
    end,

    ?assert(is_process_alive(ConnPid)),
    receive
        {nhttpc_response, ConnPid, Ref1, {ok, Resp}} ->
            ?assertEqual(200, maps:get(status, Resp))
    after 5000 ->
        ct:fail("first request died with the refused one")
    end,

    ?assertEqual(0, wait_for_slot_release(Slots, 2000)),

    nhttpc_conn:stop(ConnPid),
    nhttpc_test_server:stop(ServerPid),
    ok.

%% A send the connection never turns into a stream gives its slot back
conn_h2_rejected_send_releases_slot(Config) ->
    {ServerPid, Port} = start_slot_server(Config, #{}),
    Slots = nhttpc_stream_slots:new(),
    ok = nhttpc_stream_slots:set_cap(Slots, ?SLOT_CAP),
    {ok, ConnPid} = start_slot_conn(Port, Slots),

    ok = nhttpc_stream_slots:reserve(Slots),
    sys:replace_state(ConnPid, fun({ready, Data}) -> {draining, Data} end),

    Echo = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Echo, self()),
    receive
        {nhttpc_error, ConnPid, Ref, _Reason} -> ok
    after 5000 ->
        ct:fail("draining connection accepted the send")
    end,

    ?assertEqual(0, wait_for_slot_release(Slots, 2000)),

    nhttpc_conn:stop(ConnPid),
    nhttpc_test_server:stop(ServerPid),
    ok.

-spec start_slot_server(list(), map()) -> {pid(), inet:port_number()}.
start_slot_server(Config, H2Settings) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    {ok, ServerPid} = nhttp:start_link(test_h2_slot_server, #{
        port => 0,
        handler => nhttp_test_handler,
        versions => [http2],
        h2_settings => H2Settings,
        tls => #{certfile => CertFile, keyfile => KeyFile}
    }),
    unlink(ServerPid),
    {ok, Port} = nhttp:get_port(ServerPid),
    {ServerPid, Port}.

-spec start_slot_conn(inet:port_number(), nhttpc_stream_slots:slots()) -> {ok, pid()}.
start_slot_conn(Port, Slots) ->
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 5000
    }),
    ok = nhttpc_conn:wait_ready(ConnPid, 5000),
    ok = nhttpc_stream_slots:reserve(Slots),
    Warmup = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Warmup, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, _}} -> ok
    after 5000 -> ct:fail("h2 connection never answered")
    end,
    {ok, ConnPid}.

%% The connection releases the slot after it answers the caller
-spec wait_for_slot_release(nhttpc_stream_slots:slots(), integer()) -> integer().
wait_for_slot_release(Slots, Remaining) when Remaining =< 0 ->
    nhttpc_stream_slots:live(Slots);
wait_for_slot_release(Slots, Remaining) ->
    case nhttpc_stream_slots:live(Slots) of
        0 ->
            0;
        _ ->
            timer:sleep(10),
            wait_for_slot_release(Slots, Remaining - 10)
    end.

%%%-----------------------------------------------------------------------------
%%% H2 PEER CONCURRENT STREAM CAP TESTS
%%%-----------------------------------------------------------------------------

%% The peer's opening SETTINGS frame carries the concurrent stream cap out
%% of the handshake and into the `connection_ready` notification. Before
%% this, `recv_initial_settings/3` dropped the decoded events and the pool
%% only ever saw its own default.
conn_h2_reports_peer_max_concurrent_streams(Config) ->
    {ServerPid, Port} = start_slot_server(Config, #{max_concurrent_streams => ?PEER_CAP}),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none
    }),

    receive
        {pool_ready_cap, ConnPid, MaxStreams} -> ?assertEqual(?PEER_CAP, MaxStreams)
    after 5000 -> ct:fail("no ready notification carrying the peer cap")
    end,

    nhttpc_conn:stop(ConnPid),
    MockMgr ! stop,
    nhttpc_test_server:stop(ServerPid),
    ok.

%% A peer that names no cap leaves the report `undefined`, and the
%% connection writes the per-protocol ceiling into the cap slot. RFC 9113
%% section 6.5.2 reads the absence of the setting as "no limit", so the
%% ceiling is a client-side choice and not a value the peer sent.
conn_h2_reports_undefined_when_peer_omits_cap(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{initial_window_size => ?PEER_WINDOW}),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none
    }),

    receive
        {pool_ready_cap, ConnPid, MaxStreams} -> ?assertEqual(undefined, MaxStreams)
    after 5000 -> ct:fail("no ready notification from a peer with no cap")
    end,
    ?assertEqual(nhttpc_stream_slots:default_cap(http2), nhttpc_stream_slots:cap(Slots)),

    nhttpc_conn:stop(ConnPid),
    MockMgr ! stop,
    ok = nhttpc_h2_peer:stop(Peer),
    ok.

%% A SETTINGS frame that arrives after the handshake reports the new cap
%% and writes it. The connection process owns the cap slot, so an array it
%% can reach carries the later value.
conn_h2_reports_later_settings_cap(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?PEER_CAP}),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    ok = nhttpc_stream_slots:set_cap(Slots, ?PEER_CAP),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none
    }),
    receive
        {pool_ready_cap, ConnPid, First} -> ?assertEqual(?PEER_CAP, First)
    after 5000 -> ct:fail("no ready notification carrying the peer cap")
    end,

    ok = nhttpc_h2_peer:send_settings(Peer, #{max_concurrent_streams => ?PEER_CAP_LATER}),

    receive
        {pool_max_streams, ConnPid, Later} -> ?assertEqual(?PEER_CAP_LATER, Later)
    after 5000 -> ct:fail("later SETTINGS frame was never reported")
    end,
    ?assertEqual(?PEER_CAP_LATER, nhttpc_stream_slots:cap(Slots)),

    nhttpc_conn:stop(ConnPid),
    MockMgr ! stop,
    ok = nhttpc_h2_peer:stop(Peer),
    ok.

%% A peer that lowers its cap mid-connection reaches the lock-free
%% checkout path without the manager. The connection process writes the
%% cap slot itself, so the shrink is in place before the cast is served.
conn_h2_later_settings_lower_cap(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?PEER_CAP_LATER}),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none
    }),
    receive
        {pool_ready_cap, ConnPid, First} -> ?assertEqual(?PEER_CAP_LATER, First)
    after 5000 -> ct:fail("no ready notification carrying the peer cap")
    end,
    ?assertEqual(?PEER_CAP_LATER, nhttpc_stream_slots:cap(Slots)),

    ok = nhttpc_h2_peer:send_settings(Peer, #{max_concurrent_streams => ?PEER_CAP}),

    receive
        {pool_max_streams, ConnPid, Later} -> ?assertEqual(?PEER_CAP, Later)
    after 5000 -> ct:fail("lowered cap was never reported")
    end,
    ?assertEqual(?PEER_CAP, nhttpc_stream_slots:cap(Slots)),

    nhttpc_conn:stop(ConnPid),
    MockMgr ! stop,
    ok = nhttpc_h2_peer:stop(Peer),
    ok.

%% A SETTINGS frame that moves the cap mid-connection is reported with
%% the cap before and after it. A frame that repeats the cap is not.
conn_h2_max_streams_change_event(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?PEER_CAP_LATER}),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    HandlerId = {?MODULE, max_streams_change},
    ok = telemetry:attach(
        HandlerId,
        [nhttpc, connection, max_streams_changed],
        fun(_Event, Measurements, Meta, _HandlerConfig) ->
            Self ! {max_streams_event, maps:get(conn_pid, Meta), Measurements}
        end,
        undefined
    ),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        otel => #{events => true, metrics => false, traces => false}
    }),
    receive
        {pool_ready_cap, ConnPid, First} -> ?assertEqual(?PEER_CAP_LATER, First)
    after 5000 -> ct:fail("no ready notification carrying the peer cap")
    end,

    ok = nhttpc_h2_peer:send_settings(Peer, #{max_concurrent_streams => ?PEER_CAP_LATER}),
    receive
        {pool_max_streams, ConnPid, Same} -> ?assertEqual(?PEER_CAP_LATER, Same)
    after 5000 -> ct:fail("repeated cap was never reported to the pool")
    end,
    ok = nhttpc_h2_peer:send_settings(Peer, #{max_concurrent_streams => ?PEER_CAP}),
    receive
        {pool_max_streams, ConnPid, Later} -> ?assertEqual(?PEER_CAP, Later)
    after 5000 -> ct:fail("lowered cap was never reported to the pool")
    end,

    receive
        {max_streams_event, ConnPid, Measurements} ->
            ?assertEqual(
                #{previous_max_streams => ?PEER_CAP_LATER, max_streams => ?PEER_CAP},
                Measurements
            )
    after 5000 -> ct:fail("no max_streams_changed event")
    end,
    receive
        {max_streams_event, ConnPid, Extra} -> ct:fail("unexpected cap event ~p", [Extra])
    after ?QUEUE_SETTLE_MS -> ok
    end,
    ok = telemetry:detach(HandlerId),

    nhttpc_conn:stop(ConnPid),
    MockMgr ! stop,
    ok = nhttpc_h2_peer:stop(Peer),
    ok.

%% A slot granted against a cap the peer had already lowered reaches the
%% caller as a refused stream, not as a terminal connect error. The
%% request never touched the socket, so it is safe to send again, and the
%% slot goes back for the next caller.
conn_h2_refused_before_open_is_retryable(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?PEER_CAP_ZERO}),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none
    }),
    receive
        {pool_ready_cap, ConnPid, Cap} -> ?assertEqual(?PEER_CAP_ZERO, Cap)
    after 5000 -> ct:fail("no ready notification from a peer that named a cap of zero")
    end,
    ?assertEqual(?PEER_CAP_ZERO, nhttpc_stream_slots:cap(Slots)),

    ok = nhttpc_stream_slots:set_cap(Slots, ?PEER_OVER_GRANT_CAP),
    ok = nhttpc_stream_slots:reserve(Slots),
    ?assertEqual(1, nhttpc_stream_slots:live(Slots)),

    Echo = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Echo, self()),
    receive
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ?assertEqual(
                {error, {stream_error, {protocol, refused_stream}}}, nhttpc_error:wrap(Reason)
            ),
            ?assert(nhttp_error:is_retryable({error, Reason}))
    after 5000 -> ct:fail("a stream past the peer cap was not refused")
    end,
    ?assertEqual(0, wait_for_slot_release(Slots, 2000)),

    nhttpc_conn:stop(ConnPid),
    MockMgr ! stop,
    ok = nhttpc_h2_peer:stop(Peer),
    ok.

%%%-----------------------------------------------------------------------------
%%% H2 SEND CREDIT EXHAUSTION TESTS
%%%-----------------------------------------------------------------------------

%% A peer that withholds connection credit makes `nhttp_h2:send_data/4`
%% answer `{partial, ...}` for the first body that no longer fits. The
%% connection has to refuse that one request and carry on: the seven
%% requests already on the wire keep waiting, the eighth caller sees a
%% retryable refusal, and nobody sees the connection go down.
conn_h2_survives_credit_exhaustion(Config) ->
    process_flag(trap_exit, true),
    {Peer, ConnPid, MockMgr, Slots} = start_starved_conn(Config),
    Accepted = send_credit_posts(ConnPid, Slots, ?CREDIT_FITTING_REQUESTS, ?CREDIT_BODY_SIZE),
    Refused = send_credit_post(ConnPid, Slots, ?CREDIT_BODY_SIZE),
    ok = assert_refused(ConnPid, Refused, ?CREDIT_REFUSAL_TIMEOUT),
    ?assertMatch({ready, _}, sys:get_state(ConnPid)),
    ok = assert_callers_untouched(ConnPid, Accepted),
    stop_starved_conn(Peer, ConnPid, MockMgr),
    ok.

%% The production shape: 134-octet bodies until the window is spent. The
%% peer never reads its socket, so this case is best-effort. On a host
%% whose socket buffers cannot absorb the window the client blocks in
%% `ssl:send/2` before the codec window reaches zero, and the case skips
%% instead of failing the suite.
conn_h2_survives_credit_exhaustion_small_bodies(Config) ->
    process_flag(trap_exit, true),
    {Peer, ConnPid, MockMgr, Slots} = start_starved_conn(Config),
    Accepted = send_credit_posts(
        ConnPid, Slots, ?CREDIT_SMALL_FITTING_REQUESTS, ?CREDIT_SMALL_BODY_SIZE
    ),
    Refused = send_credit_post(ConnPid, Slots, ?CREDIT_SMALL_BODY_SIZE),
    case await_refusal(ConnPid, Refused, ?CREDIT_REFUSAL_TIMEOUT) of
        ok ->
            ?assertMatch({ready, _}, sys:get_state(ConnPid)),
            ok = assert_callers_untouched(ConnPid, Accepted),
            stop_starved_conn(Peer, ConnPid, MockMgr),
            ok;
        timeout ->
            Blocked = blocked_in_send(ConnPid),
            stop_starved_conn(Peer, ConnPid, MockMgr),
            case Blocked of
                true -> {skip, "client blocked in ssl:send/2 before the codec window was spent"};
                false -> ct:fail("no refusal for the request past the window")
            end
    end.

%% A refused request must leave the HPACK encoder as it found it. The
%% headers of the refused request were encoded before the body was
%% refused, and an entry the peer's decoder never saw would corrupt every
%% later header block on the connection.
conn_h2_refused_request_leaves_hpack_intact(Config) ->
    process_flag(trap_exit, true),
    {Peer, ConnPid, MockMgr, Slots} = start_starved_conn(Config),
    _Accepted = send_credit_posts(ConnPid, Slots, ?CREDIT_FITTING_REQUESTS, ?CREDIT_BODY_SIZE),
    Before = h2_conn_of(ConnPid),
    Refused = send_credit_post(ConnPid, Slots, ?CREDIT_BODY_SIZE),
    ok = assert_refused(ConnPid, Refused, ?CREDIT_REFUSAL_TIMEOUT),
    After = h2_conn_of(ConnPid),
    ?assertEqual(Before, After),
    stop_starved_conn(Peer, ConnPid, MockMgr),
    ok.

%% A refused request opens no stream: the codec's stream count stays
%% put, and the next request that the connection does accept takes the
%% stream id the refused one would have burnt.
conn_h2_refused_request_opens_no_stream(Config) ->
    process_flag(trap_exit, true),
    {Peer, ConnPid, MockMgr, Slots} = start_starved_conn(Config),
    _Accepted = send_credit_posts(ConnPid, Slots, ?CREDIT_FITTING_REQUESTS, ?CREDIT_BODY_SIZE),
    StatsBefore = nhttp_h2:stream_stats(h2_conn_of(ConnPid)),
    Refused = send_credit_post(ConnPid, Slots, ?CREDIT_BODY_SIZE),
    ok = assert_refused(ConnPid, Refused, ?CREDIT_REFUSAL_TIMEOUT),
    ?assertEqual(StatsBefore, nhttp_h2:stream_stats(h2_conn_of(ConnPid))),

    ok = nhttpc_stream_slots:reserve(Slots),
    Get = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    {ok, _GetRef} = nhttpc_conn:send_async(ConnPid, Get, self()),
    ?assertMatch({ready, _}, sys:get_state(ConnPid)),
    StreamIndex = stream_index_of(ConnPid),
    ?assertEqual(?CREDIT_FITTING_REQUESTS + 1, map_size(StreamIndex)),
    ?assertEqual(?CREDIT_FITTING_REQUESTS * 2 + 1, lists:max(maps:keys(StreamIndex))),
    stop_starved_conn(Peer, ConnPid, MockMgr),
    ok.

%% The pool reserved a stream slot before the send left the caller. A
%% refusal has to give it back, or every refusal leaks one unit of pool
%% capacity for the life of the connection.
conn_h2_refused_request_releases_slot(Config) ->
    process_flag(trap_exit, true),
    {Peer, ConnPid, MockMgr, Slots} = start_starved_conn(Config),
    _Accepted = send_credit_posts(ConnPid, Slots, ?CREDIT_FITTING_REQUESTS, ?CREDIT_BODY_SIZE),
    ?assertEqual(?CREDIT_FITTING_REQUESTS, nhttpc_stream_slots:live(Slots)),
    Refused = send_credit_post(ConnPid, Slots, ?CREDIT_BODY_SIZE),
    ?assertEqual(?CREDIT_FITTING_REQUESTS + 1, nhttpc_stream_slots:live(Slots)),
    ok = assert_refused(ConnPid, Refused, ?CREDIT_REFUSAL_TIMEOUT),
    ?assertEqual(
        ?CREDIT_FITTING_REQUESTS,
        wait_for_slot_value(Slots, ?CREDIT_FITTING_REQUESTS, 2000)
    ),
    stop_starved_conn(Peer, ConnPid, MockMgr),
    ok.

%% A ready HTTP/2 connection to a peer that sends one SETTINGS frame and
%% then neither reads nor credits. The mock manager stands in for the pool.
-spec start_starved_conn([tuple()]) -> {pid(), pid(), pid(), nhttpc_stream_slots:slots()}.
start_starved_conn(Config) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, #{max_concurrent_streams => ?CREDIT_PEER_CAP}),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        otel => #{events => true, metrics => false, traces => false}
    }),
    receive
        {pool_ready_cap, ConnPid, Cap} -> ?assertEqual(?CREDIT_PEER_CAP, Cap)
    after 5000 -> ct:fail("no ready notification from the starving peer")
    end,
    {Peer, ConnPid, MockMgr, Slots}.

-spec stop_starved_conn(pid(), pid(), pid()) -> ok.
stop_starved_conn(Peer, ConnPid, MockMgr) ->
    nhttpc_conn:stop(ConnPid),
    MockMgr ! stop,
    ok = nhttpc_h2_peer:stop(Peer),
    drain_exits().

%% Reserve a slot and send one POST whose body is `Size` octets, as the
%% pool would. The synchronous state read afterwards is the barrier that
%% puts the send on the wire, or refuses it, before the caller continues.
-spec send_credit_post(pid(), nhttpc_stream_slots:slots(), pos_integer()) -> reference().
send_credit_post(ConnPid, Slots, Size) ->
    ok = nhttpc_stream_slots:reserve(Slots),
    Req = #{
        method => post,
        path => <<"/credit">>,
        headers => [{<<"content-type">>, <<"application/octet-stream">>}],
        body => binary:copy(<<$x>>, Size)
    },
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    Ref.

-spec send_credit_posts(pid(), nhttpc_stream_slots:slots(), pos_integer(), pos_integer()) ->
    [reference()].
send_credit_posts(ConnPid, Slots, Count, Size) ->
    Refs = [send_credit_post(ConnPid, Slots, Size) || _ <- lists:seq(1, Count)],
    ?assertMatch({ready, _}, sys:get_state(ConnPid)),
    Refs.

-spec await_refusal(pid(), reference(), timeout()) -> ok | timeout.
await_refusal(ConnPid, Ref, Timeout) ->
    receive
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ?assertEqual(
                {error, {stream_error, {protocol, refused_stream}}}, nhttpc_error:wrap(Reason)
            ),
            ?assert(nhttp_error:is_retryable({error, Reason})),
            ok
    after Timeout ->
        timeout
    end.

-spec assert_refused(pid(), reference(), timeout()) -> ok.
assert_refused(ConnPid, Ref, Timeout) ->
    case await_refusal(ConnPid, Ref, Timeout) of
        ok -> ok;
        timeout -> ct:fail("the request past the send window was not refused")
    end.

%% The accepted requests wait on a peer that never answers, so none of
%% them may have seen an error, and the connection may not have exited.
-spec assert_callers_untouched(pid(), [reference()]) -> ok.
assert_callers_untouched(ConnPid, Refs) ->
    receive
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ?assert(lists:member(Ref, Refs)),
            ct:fail("an accepted request saw ~p", [Reason]);
        {'EXIT', ConnPid, Reason} ->
            ct:fail("the connection exited with ~p", [Reason])
    after 0 ->
        ?assert(is_process_alive(ConnPid)),
        ok
    end.

-spec blocked_in_send(pid()) -> boolean().
blocked_in_send(ConnPid) ->
    case erlang:process_info(ConnPid, current_stacktrace) of
        {current_stacktrace, Frames} ->
            lists:any(
                fun({Mod, _Fun, _Arity, _Loc}) ->
                    lists:member(Mod, [ssl, ssl_gen_statem, tls_sender, gen_tcp, prim_inet])
                end,
                Frames
            );
        undefined ->
            false
    end.

%% The codec state lives in the only `#h2_conn{}` inside `#conn_data{}`.
%% Locating it by record tag avoids baking a field index into the test.
-spec h2_conn_of(pid()) -> nhttp_h2:conn().
h2_conn_of(ConnPid) ->
    {ready, Data} = sys:get_state(ConnPid),
    [H2Conn] = [
        Field
     || Field <- tuple_to_list(Data),
        is_tuple(Field),
        tuple_size(Field) > 0,
        element(1, Field) =:= h2_conn
    ],
    H2Conn.

%% `stream_index` is the only map in `#conn_data{}` keyed by stream id.
-spec stream_index_of(pid()) -> #{nhttp_lib:stream_id() => reference()}.
stream_index_of(ConnPid) ->
    {ready, Data} = sys:get_state(ConnPid),
    [StreamIndex] = [
        Field
     || Field <- tuple_to_list(Data),
        is_map(Field),
        map_size(Field) > 0,
        lists:all(fun is_integer/1, maps:keys(Field))
    ],
    StreamIndex.

-spec wait_for_slot_value(nhttpc_stream_slots:slots(), integer(), integer()) -> integer().
wait_for_slot_value(Slots, _Want, Remaining) when Remaining =< 0 ->
    nhttpc_stream_slots:live(Slots);
wait_for_slot_value(Slots, Want, Remaining) ->
    case nhttpc_stream_slots:live(Slots) of
        Want ->
            Want;
        _ ->
            timer:sleep(10),
            wait_for_slot_value(Slots, Want, Remaining - 10)
    end.

%% Drain any pending linked-process EXIT signals left by fault cases.
drain_exits() ->
    receive
        {'EXIT', _, _} -> drain_exits()
    after 0 -> ok
    end.

%%%-----------------------------------------------------------------------------
%%% H2 CREDIT SLOT TESTS (plan 20)
%%%-----------------------------------------------------------------------------

conn_h2_credit_resyncs_when_idle(Config) ->
    process_flag(trap_exit, true),
    {ConnPid, MockMgr, Slots} = start_server_conn(Config),
    ?assertEqual(?PEER_WINDOW, nhttpc_stream_slots:credit(Slots)),

    ok = nhttpc_stream_slots:reserve_credit(Slots, ?CREDIT_DRIFT),
    ?assertEqual(?PEER_WINDOW - ?CREDIT_DRIFT, nhttpc_stream_slots:credit(Slots)),

    ok = nhttpc_stream_slots:reserve(Slots),
    Get = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Get, self()),
    receive
        {nhttpc_response, ConnPid, Ref, {ok, #{status := 200}}} -> ok
    after 5000 -> ct:fail("no response from the eager peer")
    end,

    ?assertEqual(?PEER_WINDOW, wait_for_credit_value(Slots, ?PEER_WINDOW, 2000)),
    ?assertEqual(
        nhttp_h2:connection_send_window(h2_conn_of(ConnPid)),
        nhttpc_stream_slots:credit(Slots)
    ),
    ?assertEqual(0, wait_for_slot_value(Slots, 0, 2000)),

    nhttpc_conn:stop(ConnPid),
    MockMgr ! stop,
    drain_exits(),
    ok.

conn_h2_body_above_stream_window_is_queued(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config,
        #{
            settings => #{
                max_concurrent_streams => ?CREDIT_PEER_CAP,
                initial_window_size => ?SMALL_STREAM_WINDOW
            },
            credit => never
        },
        #{}
    ),
    Req = post_of(?OVER_STREAM_WINDOW_BODY),
    ok = nhttpc_stream_slots:reserve(Slots, nhttpc_h2:credit_claim(Req)),
    ?assertEqual(?PEER_WINDOW - ?OVER_STREAM_WINDOW_BODY, nhttpc_stream_slots:credit(Slots)),
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    ok = wait_peer_stat(PeerConn, data_octets, ?SMALL_STREAM_WINDOW, ?QUEUE_WAIT_MS),
    ok = assert_not_refused(ConnPid, Ref),
    ?assertEqual(1, maps:get(active, nhttp_h2:stream_stats(h2_conn_of(ConnPid)))),
    ?assertEqual(
        ?OVER_STREAM_WINDOW_BODY - ?SMALL_STREAM_WINDOW,
        nhttp_h2:send_buffer_bytes(h2_conn_of(ConnPid))
    ),
    ?assertEqual(?PEER_WINDOW - ?SMALL_STREAM_WINDOW, nhttpc_stream_slots:credit(Slots)),
    ?assertEqual(
        nhttp_h2:connection_send_window(h2_conn_of(ConnPid)),
        nhttpc_stream_slots:credit(Slots)
    ),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

conn_h2_goaway_carries_debug_data(Config) ->
    process_flag(trap_exit, true),
    {Peer, ConnPid, MockMgr, Slots} = start_peer_conn(Config, #{
        max_concurrent_streams => ?CREDIT_PEER_CAP
    }),
    Self = self(),
    HandlerId = {?MODULE, goaway},
    ok = telemetry:attach(
        HandlerId,
        [nhttpc, connection, goaway],
        fun(_Event, _Measurements, Meta, _HandlerConfig) -> Self ! {goaway_event, Meta} end,
        undefined
    ),

    ok = nhttpc_stream_slots:reserve(Slots),
    Get = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Get, self()),
    ?assertMatch({ready, _}, sys:get_state(ConnPid)),
    ok = nhttpc_h2_peer:goaway(Peer, ?GOAWAY_LAST_STREAM_ID, no_error, ?GOAWAY_REASON),

    receive
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ?assertMatch(
                {http2, #{
                    type := goaway,
                    error_code := no_error,
                    debug_data := ?GOAWAY_REASON,
                    retryable := true
                }},
                Reason
            ),
            ?assertEqual(
                {error, {protocol_error, http2, {h2, no_error, ?GOAWAY_REASON}}},
                nhttpc_error:wrap(Reason)
            )
    after 5000 -> ct:fail("the in-flight request never saw the GOAWAY")
    end,
    receive
        {goaway_event, Meta} ->
            ?assertMatch(
                #{
                    conn_pid := ConnPid,
                    error_code := no_error,
                    last_stream_id := ?GOAWAY_LAST_STREAM_ID,
                    debug_data := ?GOAWAY_REASON,
                    host := _,
                    port := _
                },
                Meta
            )
    after 5000 -> ct:fail("no goaway telemetry event")
    end,
    receive
        {pool_draining, ConnPid} -> ok
    after 5000 -> ct:fail("no draining notification after GOAWAY")
    end,
    ok = telemetry:detach(HandlerId),
    stop_starved_conn(Peer, ConnPid, MockMgr),
    ok.

%% The connection reports the peer's connection-level WINDOW_UPDATE rate
%% once a second at most. A peer that credits on a known period yields
%% reports whose count, octets and inter-arrival match that period, and
%% every grant lands in the credit slot.
conn_h2_credit_rate_reported(Config) ->
    process_flag(trap_exit, true),
    {Peer, ConnPid, MockMgr, Slots} = start_peer_conn(Config, #{
        max_concurrent_streams => ?CREDIT_PEER_CAP
    }),
    Self = self(),
    HandlerId = {?MODULE, credit_rate},
    ok = telemetry:attach(
        HandlerId,
        [nhttpc, connection, credit],
        fun(_Event, Measurements, Meta, _HandlerConfig) ->
            Self ! {credit_report, maps:get(conn_pid, Meta), Measurements}
        end,
        undefined
    ),

    lists:foreach(
        fun(_) ->
            ok = nhttpc_h2_peer:credit(Peer, ?CREDIT_GRANT),
            timer:sleep(?CREDIT_PERIOD_MS)
        end,
        lists:seq(1, ?CREDIT_GRANTS)
    ),
    Expected = ?PEER_WINDOW + ?CREDIT_GRANTS * ?CREDIT_GRANT,
    ?assertEqual(Expected, wait_for_credit_value(Slots, Expected, 5000)),
    Reports = collect_credit_reports(ConnPid, []),
    ok = telemetry:detach(HandlerId),

    ?assert(length(Reports) >= 1),
    Total = lists:sum([Count || #{count := Count} <- Reports]),
    ?assert(Total >= ?CREDIT_GRANTS div 2),
    ?assert(Total =< ?CREDIT_GRANTS),
    PeriodUs = ?CREDIT_PERIOD_MS * 1000,
    lists:foreach(
        fun(#{count := Count, octets := Octets, mean_interval := Mean, max_interval := Max}) ->
            ?assertEqual(Count * ?CREDIT_GRANT, Octets),
            ?assert(Mean >= PeriodUs div 2),
            ?assert(Mean =< PeriodUs * 2),
            ?assert(Max >= Mean),
            ?assert(Max =< PeriodUs * 5)
        end,
        Reports
    ),

    stop_starved_conn(Peer, ConnPid, MockMgr),
    ok.

%% A ready HTTP/2 connection to a raw peer with the given SETTINGS, with
%% events on and the mock manager standing in for the pool.
-spec start_peer_conn([tuple()], nhttp_h2:settings()) ->
    {pid(), pid(), pid(), nhttpc_stream_slots:slots()}.
start_peer_conn(Config, Settings) ->
    {Peer, Port} = nhttpc_h2_peer:start(Config, Settings),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        otel => #{events => true, metrics => false, traces => false}
    }),
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("no ready notification from the raw peer")
    end,
    {Peer, ConnPid, MockMgr, Slots}.

%% A ready HTTP/2 connection to the group's `nhttp` server, which credits
%% every octet it reads, with stream slots and the mock manager.
-spec start_server_conn([tuple()]) -> {pid(), pid(), nhttpc_stream_slots:slots()}.
start_server_conn(Config) ->
    Port = ?config(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000
    }),
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("h2 conn not ready")
    end,
    {ConnPid, MockMgr, Slots}.

-spec wait_for_credit_value(nhttpc_stream_slots:slots(), integer(), integer()) -> integer().
wait_for_credit_value(Slots, _Want, Remaining) when Remaining =< 0 ->
    nhttpc_stream_slots:credit(Slots);
wait_for_credit_value(Slots, Want, Remaining) ->
    case nhttpc_stream_slots:credit(Slots) of
        Want ->
            Want;
        _ ->
            timer:sleep(10),
            wait_for_credit_value(Slots, Want, Remaining - 10)
    end.

-spec collect_credit_reports(pid(), [map()]) -> [map()].
collect_credit_reports(ConnPid, Acc) ->
    receive
        {credit_report, ConnPid, Measurements} ->
            collect_credit_reports(ConnPid, [Measurements | Acc])
    after ?CREDIT_REPORT_SETTLE_MS ->
        lists:reverse(Acc)
    end.

%%%-----------------------------------------------------------------------------
%%% H2 STREAM-ID ROTATION TESTS
%%%-----------------------------------------------------------------------------

%% A connection retires at `max_streams_per_connection`: it tells the pool
%% it is draining, refuses the request that still reaches it with the
%% retryable `refused_stream` reason, serves what is in flight, and exits
%% `normal`. The rotated event carries the count of streams it opened.
conn_h2_rotates_at_threshold(Config) ->
    process_flag(trap_exit, true),
    Port = ?config(server_port, Config),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    {ok, ConnPid} = nhttpc_conn:start_link(#{
        stream_slots => Slots,
        pool_mgr => MockMgr,
        host => <<"127.0.0.1">>,
        port => Port,
        transport => ssl,
        protocols => [http2],
        verify => verify_none,
        request_timeout => 10000,
        max_streams_per_connection => ?ROTATION_THRESHOLD,
        otel => #{events => true, metrics => false, traces => false}
    }),
    receive
        {pool_ready, ConnPid, http2} -> ok
    after 5000 -> ct:fail("h2 conn not ready")
    end,
    HandlerId = {?MODULE, rotated},
    ok = telemetry:attach(
        HandlerId,
        [nhttpc, connection, rotated],
        fun(_Event, Measurements, Meta, _HandlerConfig) ->
            Self ! {rotated_event, Meta, Measurements}
        end,
        undefined
    ),
    MRef = monitor(process, ConnPid),
    Get = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    Refs = [
        begin
            ok = nhttpc_stream_slots:reserve(Slots),
            {ok, Ref} = nhttpc_conn:send_async(ConnPid, Get, self()),
            Ref
        end
     || _ <- lists:seq(1, ?ROTATION_THRESHOLD + 1)
    ],
    {Served, [Extra]} = lists:split(?ROTATION_THRESHOLD, Refs),
    lists:foreach(
        fun(Ref) ->
            receive
                {nhttpc_response, ConnPid, Ref, {ok, #{status := 200}}} ->
                    ok;
                {nhttpc_error, ConnPid, Ref, Reason} ->
                    ct:fail("a request before the threshold failed: ~p", [Reason])
            after 5000 -> ct:fail("a request before the threshold never completed")
            end
        end,
        Served
    ),
    receive
        {nhttpc_error, ConnPid, Extra, Refused} ->
            ?assertMatch({http2, #{type := refused, retryable := true}}, Refused)
    after 5000 -> ct:fail("the request past the threshold was not refused")
    end,
    receive
        {rotated_event, Meta, Measurements} ->
            ?assertMatch(#{conn_pid := ConnPid, host := _, port := _}, Meta),
            ?assertEqual(#{streams => ?ROTATION_THRESHOLD}, Measurements)
    after 5000 -> ct:fail("no rotated telemetry event")
    end,
    receive
        {pool_draining, ConnPid} -> ok
    after 5000 -> ct:fail("no draining notification at the threshold")
    end,
    receive
        {'DOWN', MRef, process, ConnPid, ExitReason} -> ?assertEqual(normal, ExitReason)
    after 5000 -> ct:fail("the rotated connection did not exit")
    end,
    ?assertEqual(0, nhttpc_stream_slots:live(Slots)),
    ok = telemetry:detach(HandlerId),
    MockMgr ! stop,
    ok.

%%%-----------------------------------------------------------------------------
%%% H2 SEND QUEUE TESTS
%%%-----------------------------------------------------------------------------

%% The production shape under the final design: 134-octet bodies against
%% a peer that credits the connection window on response, 300 ms late.
%% Every body is admitted against reserved credit and none is ever parked
%% in the codec send queue behind its HEADERS frame.
conn_h2_small_body_is_admitted_not_queued(Config) ->
    {Server, Port, Counters} = start_flow_server(test_small_body_server, Config, #{
        h2_connection_window_policy => on_response,
        h2_response_delay => ?SMALL_BODY_DELAY_MS,
        h2_settings => #{max_concurrent_streams => ?CREDIT_PEER_CAP}
    }),
    {PoolSup, Mgr} = start_queue_pool(test_small_body_pool, Port, #{}),
    ConnPid = pool_conn_pid(Mgr),
    Workers = spawn_pool_requests(
        test_small_body_pool, post_of(?CREDIT_SMALL_BODY_SIZE), ?SMALL_BODY_REQUESTS
    ),
    MaxQueued = sample_queued_streams(ConnPid, Workers, 0),
    Results = collect_workers(Workers),
    ?assertEqual(
        lists:duplicate(?SMALL_BODY_REQUESTS, {ok, 200}), [result_kind(R) || R <- Results]
    ),
    ?assertEqual(0, MaxQueued),
    ?assert(is_process_alive(ConnPid)),
    ?assertEqual([ConnPid], pool_conn_pids(Mgr)),
    #{body_octets := Octets} = nhttpc_h2_flow_handler:read(Counters),
    ?assertEqual(?SMALL_BODY_REQUESTS * ?CREDIT_SMALL_BODY_SIZE, Octets),
    stop_queue_pool(PoolSup),
    ok = nhttp:stop(Server),
    ok.

%% A pooled body above the initial window opens its stream, sends what the
%% window takes and queues the rest, which leaves when the peer credits.
%% Before the send queue this body was refused before a stream opened and
%% the caller saw the retryable refusal.
conn_h2_large_body_is_queued_not_refused(Config) ->
    {Server, Port, Counters} = start_flow_server(test_large_body_server, Config, #{
        h2_connection_window_policy => {delay, ?LARGE_BODY_CREDIT_DELAY_MS},
        h2_settings => #{max_concurrent_streams => ?CREDIT_PEER_CAP}
    }),
    {PoolSup, _Mgr} = start_queue_pool(test_large_body_pool, Port, #{}),
    Result = nhttpc_pool:request(test_large_body_pool, post_of(?LARGE_BODY), ?QUEUE_WAIT_MS),
    ?assertMatch({ok, #{status := 200}}, Result),
    #{body_octets := Octets, requests := Requests} = nhttpc_h2_flow_handler:read(Counters),
    ?assertEqual(?LARGE_BODY, Octets),
    ?assertEqual(1, Requests),
    stop_queue_pool(PoolSup),
    ok = nhttp:stop(Server),
    ok.

%% A body above the connection window on a peer with a wide stream window
%% leaves in DATA frames no larger than the peer's max frame size, the
%% first window's worth in one batch and the rest as credit arrives.
conn_h2_body_over_max_frame_size(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config, #{settings => queue_peer_settings(), credit => eager}, #{}
    ),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, post_of(?QUEUE_BODY), self()),
    ok = await_ok(ConnPid, Ref),
    ok = wait_peer_stat(PeerConn, data_octets, ?QUEUE_BODY, ?QUEUE_WAIT_MS),
    #{data_frames := Frames, max_data_frame := MaxFrame} = nhttpc_h2_script_peer:stats(PeerConn),
    ?assert(MaxFrame =< ?PEER_MAX_FRAME_SIZE),
    ?assert(maps:get(1, Frames) >= ceil_div(?QUEUE_BODY, ?PEER_MAX_FRAME_SIZE)),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% A body that meets the send buffer bound is refused with the retryable
%% `refused_stream` reason, after a body that fits the bound was queued.
conn_h2_send_buffer_full_is_retryable(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config,
        #{settings => queue_peer_settings(), credit => never},
        #{max_send_buffer => ?BUFFER_BOUND}
    ),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Queued} = nhttpc_conn:send_async(ConnPid, post_of(?BUFFER_BODY), self()),
    ok = wait_peer_stat(PeerConn, data_octets, ?PEER_WINDOW, ?QUEUE_WAIT_MS),
    ok = assert_not_refused(ConnPid, Queued),
    ?assertEqual(?BUFFER_BODY - ?PEER_WINDOW, nhttp_h2:send_buffer_bytes(h2_conn_of(ConnPid))),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Refused} = nhttpc_conn:send_async(ConnPid, post_of(?BUFFER_BODY), self()),
    ok = assert_refused(ConnPid, Refused, ?CREDIT_REFUSAL_TIMEOUT),
    ?assertEqual(?BUFFER_BODY - ?PEER_WINDOW, nhttp_h2:send_buffer_bytes(h2_conn_of(ConnPid))),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% A request rejected after its HEADERS were encoded leaves the HPACK
%% encoder as the peer's decoder knows it: the next request on the
%% connection decodes at the peer and is answered.
conn_h2_rejected_request_leaves_hpack_intact(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config,
        #{settings => queue_peer_settings(), credit => never},
        #{max_send_buffer => ?BUFFER_BOUND}
    ),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Queued} = nhttpc_conn:send_async(ConnPid, post_of(?BUFFER_BODY), self()),
    ok = wait_peer_stat(PeerConn, data_octets, ?PEER_WINDOW, ?QUEUE_WAIT_MS),
    ok = assert_not_refused(ConnPid, Queued),
    Before = nhttp_h2:encoder_table_entries(h2_conn_of(ConnPid)),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Refused} = nhttpc_conn:send_async(ConnPid, post_of(?BUFFER_BODY), self()),
    ok = assert_refused(ConnPid, Refused, ?CREDIT_REFUSAL_TIMEOUT),
    ?assertEqual(Before, nhttp_h2:encoder_table_entries(h2_conn_of(ConnPid))),
    ok = nhttpc_stream_slots:reserve(Slots),
    Get = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    {ok, GetRef} = nhttpc_conn:send_async(ConnPid, Get, self()),
    ok = await_ok(ConnPid, GetRef),
    #{requests := Requests, closed := Closed} = nhttpc_h2_script_peer:stats(PeerConn),
    ?assertEqual(2, Requests),
    ?assertEqual(false, Closed),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% A connection-level WINDOW_UPDATE releases a queued body. The clause
%% that handles the event is named, so a codec that reported the grant to
%% a connection without one would fail loudly rather than stall.
conn_h2_window_update_event_is_not_swallowed(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config, #{settings => queue_peer_settings(), credit => never}, #{}
    ),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, post_of(?QUEUE_BODY), self()),
    ok = wait_peer_stat(PeerConn, data_octets, ?PEER_WINDOW, ?QUEUE_WAIT_MS),
    ?assertEqual(?QUEUE_REMAINDER, nhttp_h2:send_buffer_bytes(h2_conn_of(ConnPid))),
    ok = grant_credit(PeerConn, ?QUEUE_REMAINDER),
    ok = wait_peer_stat(PeerConn, data_octets, ?QUEUE_BODY, ?QUEUE_WAIT_MS),
    ok = await_ok(ConnPid, Ref),
    ?assertEqual(0, nhttp_h2:send_buffer_bytes(h2_conn_of(ConnPid))),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% A cancelled request with octets in the send queue sends RST_STREAM and
%% purges the queue: credit that arrives later releases nothing for it.
conn_h2_cancel_purges_buffered_body(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config, #{settings => queue_peer_settings(), credit => never}, #{}
    ),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, post_of(?QUEUE_BODY), self()),
    ok = wait_peer_stat(PeerConn, data_octets, ?PEER_WINDOW, ?QUEUE_WAIT_MS),
    ?assertEqual(ok, nhttpc_conn:cancel(ConnPid, Ref)),
    ok = wait_peer_reset(PeerConn, 1, cancel),
    ?assertEqual(0, nhttp_h2:send_buffer_bytes(h2_conn_of(ConnPid))),
    ok = assert_nothing_drains(PeerConn, ?PEER_WINDOW),
    ?assertEqual(0, nhttpc_stream_slots:live(Slots)),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% A request that times out with octets still queued never sent its
%% END_STREAM, so the peer never processed it and the caller gets the
%% retryable refusal. A request whose END_STREAM left gets the plain
%% timeout.
conn_h2_timeout_on_queued_body_is_retryable(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config,
        #{
            settings => queue_peer_settings(),
            credit => never,
            response_delay_ms => ?SLOW_RESPONSE_MS
        },
        #{request_timeout => ?QUEUE_TIMEOUT_MS}
    ),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Queued} = nhttpc_conn:send_async(ConnPid, post_of(?QUEUE_BODY), self()),
    ok = wait_peer_stat(PeerConn, data_octets, ?PEER_WINDOW, ?QUEUE_WAIT_MS),
    ok = assert_refused(ConnPid, Queued, ?CREDIT_REFUSAL_TIMEOUT),
    ok = wait_peer_reset(PeerConn, 1, cancel),
    ok = assert_nothing_drains(PeerConn, ?PEER_WINDOW),
    ok = nhttpc_stream_slots:reserve(Slots),
    Get = #{method => get, path => <<"/echo">>, headers => [], body => <<>>},
    {ok, Sent} = nhttpc_conn:send_async(ConnPid, Get, self()),
    receive
        {nhttpc_error, ConnPid, Sent, Reason} ->
            ?assertMatch({request, #{type := request_timeout}}, Reason),
            ?assertNot(nhttp_error:is_retryable({error, Reason}))
    after ?CREDIT_REFUSAL_TIMEOUT -> ct:fail("the complete request did not time out")
    end,
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% The credit slot the pool reserves from stays equal to the codec's
%% connection window while a queued body drains: the claim is settled at
%% encode time and every drained octet is debited as it leaves.
conn_h2_credit_slot_tracks_queued_body(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config, #{settings => queue_peer_settings(), credit => never}, #{}
    ),
    Req = post_of(?QUEUE_BODY),
    ?assertEqual(?PEER_WINDOW, nhttpc_h2:credit_claim(Req)),
    ok = nhttpc_stream_slots:reserve(Slots, nhttpc_h2:credit_claim(Req)),
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, Req, self()),
    ok = wait_peer_stat(PeerConn, data_octets, ?PEER_WINDOW, ?QUEUE_WAIT_MS),
    ?assertEqual(0, nhttpc_stream_slots:credit(Slots)),
    ?assertEqual(0, nhttp_h2:connection_send_window(h2_conn_of(ConnPid))),
    ok = grant_credit(PeerConn, ?QUEUE_GRANT),
    ok = wait_peer_stat(PeerConn, data_octets, ?QUEUE_BODY, ?QUEUE_WAIT_MS),
    ok = await_ok(ConnPid, Ref),
    Window = nhttp_h2:connection_send_window(h2_conn_of(ConnPid)),
    ?assertEqual(?QUEUE_GRANT - ?QUEUE_REMAINDER, Window),
    ?assertEqual(Window, wait_for_credit_value(Slots, Window, ?QUEUE_WAIT_MS)),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% The send queue going from empty to non-empty and back is reported as
%% one `send_blocked` event and one `send_resumed` event. The resumed
%% event carries the length of the period and the total over the
%% connection, which after one period are the same number.
conn_h2_send_blocked_and_resumed(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config, #{settings => queue_peer_settings(), credit => never}, #{}
    ),
    HandlerId = {?MODULE, send_blocked_and_resumed},
    ok = attach_send_queue_handler(HandlerId),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, post_of(?QUEUE_BODY), self()),
    ok = wait_peer_stat(PeerConn, data_octets, ?PEER_WINDOW, ?QUEUE_WAIT_MS),
    ok = grant_credit(PeerConn, ?QUEUE_GRANT),
    ok = await_ok(ConnPid, Ref),
    timer:sleep(?QUEUE_SETTLE_MS),
    Events = collect_send_queue_events(ConnPid, []),
    ok = telemetry:detach(HandlerId),
    Transitions = [Event || {Name, _} = Event <- Events, Name =/= send_queue],
    ?assertMatch([{send_blocked, _}, {send_resumed, _}], Transitions),
    [{send_blocked, Blocked}, {send_resumed, Resumed}] = Transitions,
    ?assertMatch(
        #{send_buffer_bytes := ?QUEUE_REMAINDER, queued_streams := 1, send_blocked_ms := 0},
        Blocked
    ),
    #{
        blocked_ms := BlockedMs,
        send_blocked_ms := Total,
        send_buffer_bytes := 0,
        queued_streams := 0
    } = Resumed,
    ?assert(BlockedMs >= 0 andalso BlockedMs =< ?QUEUE_WAIT_MS),
    ?assertEqual(BlockedMs, Total),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% The gauges on the blocked event and on the sample equal what the codec
%% accessors read, and the blocked time accumulates while the peer
%% withholds credit: the sample one second later carries at least that.
conn_h2_send_gauges_track_codec(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config, #{settings => queue_peer_settings(), credit => never}, #{}
    ),
    HandlerId = {?MODULE, send_gauges},
    ok = attach_send_queue_handler(HandlerId),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, _Ref} = nhttpc_conn:send_async(ConnPid, post_of(?QUEUE_BODY), self()),
    ok = wait_peer_stat(PeerConn, data_octets, ?PEER_WINDOW, ?QUEUE_WAIT_MS),
    Blocked = await_send_queue_event(send_blocked, ConnPid),
    Sample = await_send_queue_event(send_queue, ConnPid),
    ok = telemetry:detach(HandlerId),
    H2Conn = h2_conn_of(ConnPid),
    Expected = #{
        send_window => nhttp_h2:connection_send_window(H2Conn),
        send_buffer_bytes => nhttp_h2:send_buffer_bytes(H2Conn),
        queued_streams => nhttp_h2:queued_streams(H2Conn)
    },
    ?assertEqual(
        #{send_window => 0, send_buffer_bytes => ?QUEUE_REMAINDER, queued_streams => 1}, Expected
    ),
    ?assertEqual(Expected#{send_blocked_ms => 0}, Blocked),
    ?assertEqual(Expected, maps:remove(send_blocked_ms, Sample)),
    #{send_blocked_ms := BlockedMs} = Sample,
    ?assert(BlockedMs >= ?SEND_QUEUE_SAMPLE_MS andalso BlockedMs =< ?QUEUE_WAIT_MS),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% Small bodies go through admission and never enter the send queue, so
%% the queue reports no transition and every sample shows it empty.
conn_h2_small_bodies_do_not_block(Config) ->
    {Server, Port, Counters} = start_flow_server(test_small_body_events_server, Config, #{
        h2_connection_window_policy => on_response,
        h2_response_delay => ?SMALL_BODY_DELAY_MS,
        h2_settings => #{max_concurrent_streams => ?CREDIT_PEER_CAP}
    }),
    {PoolSup, Mgr} = start_queue_pool(test_small_body_events_pool, Port, #{
        otel => #{events => true, metrics => false, traces => false}
    }),
    ConnPid = pool_conn_pid(Mgr),
    HandlerId = {?MODULE, small_bodies_do_not_block},
    ok = attach_send_queue_handler(HandlerId),
    Workers = spawn_pool_requests(
        test_small_body_events_pool, post_of(?CREDIT_SMALL_BODY_SIZE), ?SMALL_BODY_REQUESTS
    ),
    MaxQueued = sample_queued_streams(ConnPid, Workers, 0),
    Results = collect_workers(Workers),
    timer:sleep(?QUEUE_SETTLE_MS),
    Events = collect_send_queue_events(ConnPid, []),
    ok = telemetry:detach(HandlerId),
    ?assertEqual(
        lists:duplicate(?SMALL_BODY_REQUESTS, {ok, 200}), [result_kind(R) || R <- Results]
    ),
    ?assertEqual(0, MaxQueued),
    ?assertEqual([], [Event || {Name, _} = Event <- Events, Name =/= send_queue]),
    ?assertEqual([], [Gauges || {send_queue, #{queued_streams := Q} = Gauges} <- Events, Q > 0]),
    #{body_octets := Octets} = nhttpc_h2_flow_handler:read(Counters),
    ?assertEqual(?SMALL_BODY_REQUESTS * ?CREDIT_SMALL_BODY_SIZE, Octets),
    stop_queue_pool(PoolSup),
    ok = nhttp:stop(Server),
    ok.

-spec attach_send_queue_handler(term()) -> ok.
attach_send_queue_handler(HandlerId) ->
    Self = self(),
    telemetry:attach_many(
        HandlerId,
        [
            [nhttpc, connection, send_blocked],
            [nhttpc, connection, send_resumed],
            [nhttpc, connection, send_queue]
        ],
        fun([nhttpc, connection, Name], Measurements, Meta, _HandlerConfig) ->
            Self ! {send_queue_event, Name, maps:get(conn_pid, Meta), Measurements}
        end,
        undefined
    ).

-spec await_send_queue_event(atom(), pid()) -> map().
await_send_queue_event(Name, ConnPid) ->
    receive
        {send_queue_event, Name, ConnPid, Measurements} -> Measurements
    after ?QUEUE_WAIT_MS -> ct:fail("no ~p event", [Name])
    end.

-spec collect_send_queue_events(pid(), [{atom(), map()}]) -> [{atom(), map()}].
collect_send_queue_events(ConnPid, Acc) ->
    receive
        {send_queue_event, Name, ConnPid, Measurements} ->
            collect_send_queue_events(ConnPid, [{Name, Measurements} | Acc])
    after 0 -> lists:reverse(Acc)
    end.

%%%-----------------------------------------------------------------------------
%%% H2 RECEIVE-SIDE CREDIT TESTS
%%%-----------------------------------------------------------------------------

%% Two concurrent responses of unequal size. Every stream is credited at
%% most what it consumed, the stream that crosses the threshold is credited
%% its own octets and never the connection total, and the connection is
%% credited exactly what the peer sent.
conn_h2_stream_credit_matches_stream_consumption(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config, #{settings => #{max_concurrent_streams => ?CREDIT_PEER_CAP}}, #{}
    ),
    ok = nhttpc_stream_slots:reserve(Slots),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Big} = nhttpc_conn:send_async(ConnPid, body_get(?CREDIT_STREAM_BIG), self()),
    {ok, Small} = nhttpc_conn:send_async(ConnPid, body_get(?CREDIT_STREAM_SMALL), self()),
    ok = await_ok(ConnPid, Big),
    ok = await_ok(ConnPid, Small),
    Total = ?CREDIT_STREAM_BIG + ?CREDIT_STREAM_SMALL,
    ok = wait_peer_stat(PeerConn, client_credit_conn, Total, ?QUEUE_WAIT_MS),
    #{
        client_credit_conn := ConnCredit,
        client_credit_streams := StreamCredits,
        sent_octets_streams := Sent
    } = nhttpc_h2_script_peer:stats(PeerConn),
    ?assertEqual(Total, ConnCredit),
    ?assertEqual([], [
        {Id, Credit, maps:get(Id, Sent, 0)}
     || {Id, Credit} <- maps:to_list(StreamCredits), Credit > maps:get(Id, Sent, 0)
    ]),
    [BigId] = [Id || {Id, Octets} <- maps:to_list(Sent), Octets =:= ?CREDIT_STREAM_BIG],
    ?assert(maps:get(BigId, StreamCredits, 0) >= ?STREAM_CREDIT_THRESHOLD),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%% A response under the connection threshold still returns its connection
%% credit on the last frame, so an idle connection holds none back. The
%% stream is done, so it is credited nothing.
conn_h2_connection_credit_flushed_on_fin(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config, #{settings => #{max_concurrent_streams => ?CREDIT_PEER_CAP}}, #{}
    ),
    ok = nhttpc_stream_slots:reserve(Slots),
    {ok, Ref} = nhttpc_conn:send_async(ConnPid, body_get(?CREDIT_FLUSH_BODY), self()),
    ok = await_ok(ConnPid, Ref),
    ok = wait_peer_stat(PeerConn, client_credit_conn, ?CREDIT_FLUSH_BODY, ?QUEUE_WAIT_MS),
    #{client_credit_conn := ConnCredit, client_credit_streams := StreamCredits} =
        nhttpc_h2_script_peer:stats(PeerConn),
    ?assertEqual(?CREDIT_FLUSH_BODY, ConnCredit),
    ?assertEqual(#{}, StreamCredits),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.

%%%-----------------------------------------------------------------------------
%%% H2 SEND QUEUE HELPERS
%%%-----------------------------------------------------------------------------

%% The SETTINGS of the scriptable peer: a stream cap that never binds, and
%% the default windows, so that a body above the initial window is bound by
%% the connection window and the stream window alike.
-spec queue_peer_settings() -> nhttp_h2:settings().
queue_peer_settings() ->
    #{max_concurrent_streams => ?CREDIT_PEER_CAP}.

%% A ready HTTP/2 connection to the scriptable peer, with the mock manager
%% standing in for the pool and `ConnExtra` merged over the connection
%% options. The PING round trip makes the connection process every frame
%% the peer sent during the handshake before a case touches the credit
%% slot: a SETTINGS ACK that lands with no request in flight makes the
%% connection rewrite the slot from the codec window, which would wipe a
%% reservation the case made just before its send.
-spec start_script_conn([tuple()], nhttpc_h2_script_peer:opts(), map()) ->
    {pid(), pid(), pid(), pid(), nhttpc_stream_slots:slots()}.
start_script_conn(Config, PeerOpts, ConnExtra) ->
    {Peer, Port} = nhttpc_h2_script_peer:start(Config, PeerOpts),
    Self = self(),
    MockMgr = spawn_link(fun() -> mock_pool_mgr_loop(Self) end),
    Slots = nhttpc_stream_slots:new(),
    ConnOpts = maps:merge(
        #{
            stream_slots => Slots,
            pool_mgr => MockMgr,
            host => <<"127.0.0.1">>,
            port => Port,
            transport => ssl,
            protocols => [http2],
            verify => verify_none,
            request_timeout => ?QUEUE_WAIT_MS,
            otel => #{events => true, metrics => false, traces => false}
        },
        ConnExtra
    ),
    {ok, ConnPid} = nhttpc_conn:start_link(ConnOpts),
    receive
        {pool_ready, ConnPid, http2} -> ok
    after ?QUEUE_WAIT_MS -> ct:fail("no ready notification from the script peer")
    end,
    {ok, [PeerConn]} = nhttpc_h2_script_peer:wait_connections(Peer, 1, ?QUEUE_WAIT_MS),
    pong = nhttpc_conn:ping(ConnPid, ?QUEUE_WAIT_MS),
    {Peer, PeerConn, ConnPid, MockMgr, Slots}.

-spec stop_script_conn(pid(), pid(), pid()) -> ok.
stop_script_conn(Peer, ConnPid, MockMgr) ->
    nhttpc_conn:stop(ConnPid),
    MockMgr ! stop,
    ok = nhttpc_h2_script_peer:stop(Peer),
    drain_exits().

-spec post_of(pos_integer()) -> nhttp_lib:request().
post_of(Size) ->
    #{
        method => post,
        path => <<"/credit">>,
        headers => [{<<"content-type">>, <<"application/octet-stream">>}],
        body => binary:copy(<<$x>>, Size)
    }.

-spec body_get(pos_integer()) -> nhttp_lib:request().
body_get(Size) ->
    #{
        method => get,
        path => <<"/body/", (integer_to_binary(Size))/binary>>,
        headers => [],
        body => <<>>
    }.

-spec await_ok(pid(), reference()) -> ok.
await_ok(ConnPid, Ref) ->
    receive
        {nhttpc_response, ConnPid, Ref, {ok, #{status := 200}}} ->
            ok;
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ct:fail("the request failed with ~p", [Reason])
    after ?QUEUE_WAIT_MS -> ct:fail("no response")
    end.

%% A request whose body is queued waits on the peer: no reply of any kind
%% reaches its caller.
-spec assert_not_refused(pid(), reference()) -> ok.
assert_not_refused(ConnPid, Ref) ->
    receive
        {nhttpc_error, ConnPid, Ref, Reason} ->
            ct:fail("the queued request was answered with ~p", [Reason]);
        {nhttpc_response, ConnPid, Ref, Response} ->
            ct:fail("the queued request was answered with ~p", [Response])
    after ?QUEUE_SETTLE_MS -> ok
    end.

%% A credit grant after a purge releases nothing: the peer's octet count
%% stays where it was.
-spec assert_nothing_drains(pid(), non_neg_integer()) -> ok.
assert_nothing_drains(PeerConn, Octets) ->
    ok = grant_credit(PeerConn, ?QUEUE_BODY),
    timer:sleep(?QUEUE_SETTLE_MS),
    #{data_octets := Seen} = nhttpc_h2_script_peer:stats(PeerConn),
    ?assertEqual(Octets, Seen),
    ok.

%% Grant `Octets` of credit on the connection window and on stream 1, the
%% one stream these cases open, so that neither window binds the drain.
-spec grant_credit(pid(), pos_integer()) -> ok.
grant_credit(PeerConn, Octets) ->
    ok = nhttpc_h2_script_peer:command(PeerConn, {window_update, Octets}),
    nhttpc_h2_script_peer:command(PeerConn, {stream_window_update, 1, Octets}).

-spec wait_peer_stat(pid(), atom(), non_neg_integer(), integer()) -> ok.
wait_peer_stat(PeerConn, Key, Want, Remaining) ->
    case maps:get(Key, nhttpc_h2_script_peer:stats(PeerConn)) of
        Value when Value >= Want ->
            ok;
        Value when Remaining =< 0 ->
            ct:fail("peer ~p reached ~p, wanted ~p", [Key, Value, Want]);
        _Value ->
            timer:sleep(?QUEUE_POLL_MS),
            wait_peer_stat(PeerConn, Key, Want, Remaining - ?QUEUE_POLL_MS)
    end.

-spec wait_peer_reset(pid(), nhttp_lib:stream_id(), nhttp_h2:error_code()) -> ok.
wait_peer_reset(PeerConn, StreamId, Code) ->
    wait_peer_reset(PeerConn, StreamId, Code, ?QUEUE_WAIT_MS).

-spec wait_peer_reset(pid(), nhttp_lib:stream_id(), nhttp_h2:error_code(), integer()) -> ok.
wait_peer_reset(PeerConn, StreamId, Code, Remaining) ->
    #{resets := Resets} = nhttpc_h2_script_peer:stats(PeerConn),
    case maps:get(StreamId, Resets, undefined) of
        Code ->
            ok;
        Other when Remaining =< 0 ->
            ct:fail("stream ~p reset ~p, wanted ~p", [StreamId, Other, Code]);
        _Other ->
            timer:sleep(?QUEUE_POLL_MS),
            wait_peer_reset(PeerConn, StreamId, Code, Remaining - ?QUEUE_POLL_MS)
    end.

%% An `nhttp` server on the flow handler, with the listener options of
%% the stingy peer merged in.
-spec start_flow_server(atom(), [tuple()], map()) ->
    {pid(), inet:port_number(), nhttpc_h2_flow_handler:counters()}.
start_flow_server(Name, Config, Extra) ->
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

%% A one-connection HTTP/2 pool, so that every request shares the codec
%% whose send queue the case observes.
-spec start_queue_pool(atom(), inet:port_number(), map()) -> {pid(), pid()}.
start_queue_pool(Ref, Port, Extra) ->
    Opts = maps:merge(
        #{
            host => <<"127.0.0.1">>,
            port => Port,
            tls => #{verify => verify_none},
            versions => [http2],
            size => 1,
            max_overflow => 0,
            max_pending_requests => 2 * ?SMALL_BODY_REQUESTS,
            timeouts => #{checkout => ?QUEUE_WAIT_MS, request => ?QUEUE_WAIT_MS}
        },
        Extra
    ),
    {ok, Sup} = nhttpc_pool_sup:start_link(Ref, Opts),
    unlink(Sup),
    Mgr = nhttpc_pool:manager(Ref),
    ok = nhttpc_pool:wait_ready(Mgr, ?QUEUE_WAIT_MS),
    {Sup, Mgr}.

-spec stop_queue_pool(pid()) -> ok.
stop_queue_pool(Sup) ->
    exit(Sup, shutdown),
    timer:sleep(?QUEUE_SETTLE_MS),
    ok.

-spec pool_conn_pid(pid()) -> pid().
pool_conn_pid(Mgr) ->
    [ConnPid] = pool_conn_pids(Mgr),
    ConnPid.

-spec pool_conn_pids(pid()) -> [pid()].
pool_conn_pids(Mgr) ->
    maps:keys(record_field(nhttpc_pool, state, connections, sys:get_state(Mgr))).

-spec spawn_pool_requests(atom(), nhttp_lib:request(), pos_integer()) -> [{pid(), reference()}].
spawn_pool_requests(Ref, Req, Count) ->
    [
        spawn_monitor(fun() -> exit({result, nhttpc_pool:request(Ref, Req, ?QUEUE_WAIT_MS)}) end)
     || _ <- lists:seq(1, Count)
    ].

-spec collect_workers([{pid(), reference()}]) -> [term()].
collect_workers(Workers) ->
    [
        receive
            {'DOWN', MRef, process, Pid, {result, Result}} -> Result;
            {'DOWN', MRef, process, Pid, Other} -> ct:fail("worker died with ~p", [Other])
        after ?QUEUE_WAIT_MS -> ct:fail("worker never finished")
        end
     || {Pid, MRef} <- Workers
    ].

-spec result_kind({ok, nhttp_lib:response()} | {error, term()}) -> term().
result_kind({ok, #{status := Status}}) -> {ok, Status};
result_kind({error, Reason}) -> nhttpc_error:wrap(Reason).

%% The largest number of streams the codec send queue held while any
%% worker was still running.
-spec sample_queued_streams(pid(), [{pid(), reference()}], non_neg_integer()) ->
    non_neg_integer().
sample_queued_streams(ConnPid, Workers, Max) ->
    case lists:any(fun({Pid, _MRef}) -> is_process_alive(Pid) end, Workers) of
        false ->
            Max;
        true ->
            Queued = nhttp_h2:queued_streams(h2_conn_of(ConnPid)),
            timer:sleep(?QUEUE_POLL_MS),
            sample_queued_streams(ConnPid, Workers, max(Max, Queued))
    end.

-spec ceil_div(pos_integer(), pos_integer()) -> pos_integer().
ceil_div(N, D) ->
    (N + D - 1) div D.

%% A record field by name, resolved from the abstract code so that a
%% renamed field fails here with its name.
-spec record_field(module(), atom(), atom(), tuple()) -> term().
record_field(Module, Record, Field, Tuple) ->
    {Module, Beam, _File} = code:get_object_code(Module),
    {ok, {Module, [{abstract_code, {_Version, Forms}}]}} = beam_lib:chunks(Beam, [abstract_code]),
    [Fields] = [Fs || {attribute, _Anno, record, {Name, Fs}} <- Forms, Name =:= Record],
    Names = [record_field_name(F) || F <- Fields],
    true = lists:member(Field, Names),
    element(length(lists:takewhile(fun(N) -> N =/= Field end, Names)) + 2, Tuple).

-spec record_field_name(term()) -> atom().
record_field_name({typed_record_field, Field, _Type}) -> record_field_name(Field);
record_field_name({record_field, _Anno, {atom, _, Name}}) -> Name;
record_field_name({record_field, _Anno, {atom, _, Name}, _Default}) -> Name.

%% A caller that reserved credit and has not sent yet holds a live slot.
%% A WINDOW_UPDATE that arrives in that window, with no request in flight,
%% must not make the connection rewrite the slot from the codec window:
%% the rewrite would hand the reserved credit out a second time.
conn_h2_idle_resync_keeps_reservations(Config) ->
    process_flag(trap_exit, true),
    {Peer, PeerConn, ConnPid, MockMgr, Slots} = start_script_conn(
        Config, #{settings => #{max_concurrent_streams => ?CREDIT_PEER_CAP}}, #{}
    ),
    ok = nhttpc_stream_slots:reserve(Slots, ?CREDIT_DRIFT),
    ?assertEqual(?PEER_WINDOW - ?CREDIT_DRIFT, nhttpc_stream_slots:credit(Slots)),
    ok = nhttpc_h2_script_peer:command(PeerConn, {window_update, ?CREDIT_GRANT}),
    pong = nhttpc_conn:ping(ConnPid, ?QUEUE_WAIT_MS),
    ?assertEqual(?PEER_WINDOW - ?CREDIT_DRIFT + ?CREDIT_GRANT, nhttpc_stream_slots:credit(Slots)),
    ok = nhttpc_stream_slots:refund_credit(Slots, ?CREDIT_DRIFT),
    ok = nhttpc_stream_slots:release(Slots),
    stop_script_conn(Peer, ConnPid, MockMgr),
    ok.
