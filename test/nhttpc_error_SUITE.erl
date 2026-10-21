%%%-----------------------------------------------------------------------------
%%% @doc Tests for `nhttpc_error' and the canonical error taxonomy.
%%%
%%% Two halves:
%%%   * direct unit coverage of `nhttpc_error' constructors, mappers, and
%%%     `wrap/1' dispatch (plan 14).
%%%   * cross-module audit (plan 10): every public function in `nhttpc',
%%%     `nhttpc_cookie_jar', and `nhttpc_url' must surface errors as the
%%%     canonical `nhttpc:error/0' union, and the runtime values produced
%%%     by representative failure modes must match the documented arms.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_error_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    connect_error_to_unreachable_host_is_typed/1,
    tls_error_on_hostname_mismatch_is_typed/1,
    invalid_url_returns_invalid_request/1,
    invalid_opts_returns_invalid_opts/1,
    redirect_loop_returns_redirect_error/1,
    retries_exhausted_carries_typed_inner_error/1,
    body_iterator_failure_returns_body_error/1,
    file_open_failure_returns_body_error/1,
    timeout_request_is_typed/1,
    no_public_spec_returns_bare_term_error/1,
    constructor_round_trip/1,
    wrap_connection_map_shapes/1,
    wrap_request_map_shapes/1,
    wrap_http2_map_shapes/1,
    wrap_pool_map_shapes/1,
    wrap_passthrough_canonical/1,
    wrap_posix_atoms_become_connect_error/1,
    from_h1_maps_protocol_error/1,
    from_h2_maps_protocol_and_stream/1,
    from_h3_maps_protocol_and_stream/1,
    from_ssl_maps_tls_taxonomy/1,
    from_file_maps_file_error/1,
    direct_classifier_constructors/1,
    direct_classifier_mappers/1,
    wrap_handles_redirect_and_connection_shapes/1,
    wrap_handles_unmatched_atom_and_term/1,
    wrap_handles_bare_reason_and_ok_passthrough/1,
    wrap_connection_map_tls_and_unknown/1,
    wrap_request_map_send_error_and_unknown/1,
    wrap_http2_map_remaining_shapes/1,
    wrap_pool_map_unknown_branch/1,
    classify_stream_stopped_branches/1,
    classify_stream_stopped_foreign_file_error/1,
    classify_tls_alert_shapes/1,
    posix_atom_table_round_trip/1,
    error_goaway_map_shape/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
        connect_error_to_unreachable_host_is_typed,
        tls_error_on_hostname_mismatch_is_typed,
        invalid_url_returns_invalid_request,
        invalid_opts_returns_invalid_opts,
        redirect_loop_returns_redirect_error,
        retries_exhausted_carries_typed_inner_error,
        body_iterator_failure_returns_body_error,
        file_open_failure_returns_body_error,
        timeout_request_is_typed,
        no_public_spec_returns_bare_term_error,
        constructor_round_trip,
        wrap_connection_map_shapes,
        wrap_request_map_shapes,
        wrap_http2_map_shapes,
        wrap_pool_map_shapes,
        wrap_passthrough_canonical,
        wrap_posix_atoms_become_connect_error,
        from_h1_maps_protocol_error,
        from_h2_maps_protocol_and_stream,
        from_h3_maps_protocol_and_stream,
        from_ssl_maps_tls_taxonomy,
        from_file_maps_file_error,
        direct_classifier_constructors,
        direct_classifier_mappers,
        wrap_handles_redirect_and_connection_shapes,
        wrap_handles_unmatched_atom_and_term,
        wrap_handles_bare_reason_and_ok_passthrough,
        wrap_connection_map_tls_and_unknown,
        wrap_request_map_send_error_and_unknown,
        wrap_http2_map_remaining_shapes,
        wrap_pool_map_unknown_branch,
        classify_stream_stopped_branches,
        classify_stream_stopped_foreign_file_error,
        classify_tls_alert_shapes,
        posix_atom_table_round_trip,
        error_goaway_map_shape
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(nhttpc),
    {ok, ServerPid} = nhttp:start_link(taxonomy_test_server, #{
        port => 0,
        handler => nhttp_test_handler,
        protocols => [http1]
    }),
    unlink(ServerPid),
    {ok, Port} = nhttp:get_port(ServerPid),
    [{server_pid, ServerPid}, {server_port, Port} | Config].

end_per_suite(Config) ->
    nhttpc_test_server:stop(?config(server_pid, Config)),
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% TESTS
%%%-----------------------------------------------------------------------------

connect_error_to_unreachable_host_is_typed(_Config) ->
    %% Connecting to a port nothing listens on must surface
    %% `{connect_error, _}` (canonical), not a bare posix atom or the
    %% internal `{connection, #{type := _}}` map shape.
    Url = <<"http://127.0.0.1:1/">>,
    Result = nhttpc:get(Url, #{timeouts => #{connect => 500, request => 1000}}),
    ?assertMatch({error, {connect_error, _}}, Result).

tls_error_on_hostname_mismatch_is_typed(Config) ->
    %% Talking TLS to a plaintext server fails handshake — surfaces as a
    %% canonical `{tls_error, _}` or `{connect_error, _}` (transport-level
    %% failure). Either is valid; we just demand a canonical shape.
    Port = ?config(server_port, Config),
    Url = iolist_to_binary([
        <<"https://127.0.0.1:">>, integer_to_binary(Port), <<"/echo">>
    ]),
    Result = nhttpc:get(Url, #{
        timeouts => #{connect => 1000, request => 2000},
        tls => #{verify => verify_peer}
    }),
    case Result of
        {error, {tls_error, _}} -> ok;
        {error, {connect_error, _}} -> ok;
        {error, {protocol_error, _, _}} -> ok;
        Other -> ct:fail("expected canonical error, got ~p", [Other])
    end.

invalid_url_returns_invalid_request(_Config) ->
    Result = nhttpc:parse_url(<<"ftp://example.com">>),
    ?assertMatch({error, {invalid_request, _}}, Result).

invalid_opts_returns_invalid_opts(_Config) ->
    Result = nhttpc:get(<<"http://example.com">>, #{not_a_real_key => 1}),
    ?assertMatch({error, {invalid_opts, _}}, Result).

redirect_loop_returns_redirect_error(Config) ->
    Port = ?config(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/redirect-loop">>
    ]),
    Result = nhttpc:get(Url, #{follow_redirects => true, max_redirects => 5}),
    case Result of
        {error, {redirect_error, _}} -> ok;
        Other -> ct:fail("expected redirect_error, got ~p", [Other])
    end.

retries_exhausted_carries_typed_inner_error(_Config) ->
    %% Force retry exhaustion by hitting an unreachable port with retry=>2.
    Url = <<"http://127.0.0.1:1/">>,
    Result = nhttpc:get(Url, #{
        retry => #{
            max_attempts => 2,
            base_delay => 1,
            max_delay => 5,
            backoff => constant,
            jitter => false,
            retry_on => [connection_refused, connection_error]
        },
        timeouts => #{connect => 200, request => 500}
    }),
    case Result of
        {error, {retries_exhausted, 2, Inner}} ->
            %% Inner must itself be a canonical error.
            ?assert(is_canonical_error(Inner));
        {error, {connect_error, _}} ->
            %% Connection refused may short-circuit without exhaustion; tolerated.
            ok;
        Other ->
            ct:fail("expected retries_exhausted or connect_error, got ~p", [Other])
    end.

body_iterator_failure_returns_body_error(Config) ->
    %% An upload iterator that returns {error, _} must surface a
    %% canonical `{body_error, _}` at the public boundary.
    Port = ?config(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/echo">>
    ]),
    BadIter = fun() -> {error, {iter_blew_up, deliberate}} end,
    Result = nhttpc:post(Url, <<>>, #{body_from => {iter, BadIter}}),
    case Result of
        {error, {body_error, _}} -> ok;
        {error, {connect_error, _}} -> ok;
        Other -> ct:fail("expected body_error, got ~p", [Other])
    end.

file_open_failure_returns_body_error(Config) ->
    Port = ?config(server_port, Config),
    Url = iolist_to_binary([
        <<"http://127.0.0.1:">>, integer_to_binary(Port), <<"/upload/echo">>
    ]),
    NonExistentPath = "/tmp/this_file_should_not_exist_taxonomy_suite_xyz",
    Result = nhttpc:post(Url, <<>>, #{body_from => {file, NonExistentPath}}),
    case Result of
        {error, {body_error, _}} -> ok;
        {error, {file_error, _}} -> ok;
        Other -> ct:fail("expected body_error or file_error, got ~p", [Other])
    end.

timeout_request_is_typed(_Config) ->
    %% await/2 on an unmatched handle must time out with
    %% `{error, {timeout, request}}`.
    FakeHandle = #{ref => make_ref(), pid => self()},
    Result = nhttpc:await(FakeHandle, 50),
    ?assertMatch({error, {timeout, request}}, Result).

%%%-----------------------------------------------------------------------------
%%% INTROSPECTION: every public spec returns nhttpc:error() (or nothing)
%%%-----------------------------------------------------------------------------

no_public_spec_returns_bare_term_error(_Config) ->
    Modules = [nhttpc, nhttpc_cookie_jar, nhttpc_url],
    Offenders = lists:flatmap(fun audit_module/1, Modules),
    ?assertEqual([], Offenders).

%%%-----------------------------------------------------------------------------
%%% UNIT TESTS — DIRECT MAPPER COVERAGE
%%%-----------------------------------------------------------------------------

constructor_round_trip(_Config) ->
    %% Every constructor must produce an error tuple that the
    %% canonical-shape recogniser accepts.
    Constructors = [
        nhttpc_error:body_error({iterator, foo}),
        nhttpc_error:connect_error(econnrefused),
        nhttpc_error:connection_closed(),
        nhttpc_error:cookie_jar_error(stopped),
        nhttpc_error:file_error(enoent),
        nhttpc_error:http_error(500, []),
        nhttpc_error:invalid_opts(bad_key),
        nhttpc_error:invalid_request(bad_shape),
        nhttpc_error:pool_closed(),
        nhttpc_error:pool_full(),
        nhttpc_error:protocol_error(http2, {h2, internal_error}),
        nhttpc_error:redirect_error(max_redirects_exceeded),
        nhttpc_error:retries_exhausted(3, {connect_error, econnrefused}),
        nhttpc_error:stream_error(cancelled),
        nhttpc_error:timeout(connect),
        nhttpc_error:tls_error({tls_alert, {bad_certificate, "msg"}})
    ],
    lists:foreach(
        fun({error, Reason}) -> ?assert(is_canonical_error(Reason)) end,
        Constructors
    ).

wrap_connection_map_shapes(_Config) ->
    [
        ?assertEqual(
            {error, {connect_error, econnrefused}},
            nhttpc_error:wrap(
                {error, {connection, #{type => connect_refused, posix => econnrefused}}}
            )
        ),
        ?assertEqual(
            {error, {connect_error, enetunreach}},
            nhttpc_error:wrap(
                {error, {connection, #{type => connect_failed, posix => enetunreach}}}
            )
        ),
        ?assertEqual(
            {error, {timeout, connect}},
            nhttpc_error:wrap({error, {connection, #{type => connect_timeout}}})
        ),
        ?assertEqual(
            {error, {tls_error, {handshake, {alpn, no_protocols}}}},
            nhttpc_error:wrap(
                {error, {connection, #{type => alpn_error, reason => no_protocols}}}
            )
        ),
        ?assertEqual(
            {error, {connect_error, not_ready}},
            nhttpc_error:wrap({error, {connection, #{type => not_ready}}})
        )
    ].

wrap_request_map_shapes(_Config) ->
    [
        ?assertEqual(
            {error, {timeout, request}},
            nhttpc_error:wrap({error, {request, #{type => request_timeout}}})
        ),
        ?assertEqual(
            {error, {timeout, body}},
            nhttpc_error:wrap({error, {request, #{type => body_timeout}}})
        ),
        ?assertEqual(
            {error, {connect_error, closed}},
            nhttpc_error:wrap({error, {request, #{type => connection_closed}}})
        ),
        ?assertEqual(
            {error, {protocol_error, http1_1, bad_status_line}},
            nhttpc_error:wrap(
                {error, {request, #{type => malformed_response, parse_error => bad_status_line}}}
            )
        ),
        ?assertEqual(
            {error, {redirect_error, max_redirects_exceeded}},
            nhttpc_error:wrap({error, {request, #{type => max_redirects_exceeded, count => 3}}})
        ),
        ?assertEqual(
            {error, {redirect_error, {redirect_loop, <<"http://x/">>}}},
            nhttpc_error:wrap(
                {error, {request, #{type => redirect_loop, url => <<"http://x/">>}}}
            )
        ),
        ?assertEqual(
            {error, {connect_error, econnreset}},
            nhttpc_error:wrap(
                {error, {request, #{type => recv_error, posix => econnreset}}}
            )
        ),
        ?assertEqual(
            {error, {stream_error, exceeded_max_response_size}},
            nhttpc_error:wrap({error, {request, #{type => response_too_large, size => 100}}})
        ),
        ?assertEqual(
            {error, {body_error, {file_open, enoent}}},
            nhttpc_error:wrap(
                {error, {request, #{type => file_error, operation => open, reason => enoent}}}
            )
        ),
        ?assertEqual(
            {error, {body_error, {file_read, enospc}}},
            nhttpc_error:wrap(
                {error, {request, #{type => file_error, operation => read, reason => enospc}}}
            )
        ),
        ?assertEqual(
            {error, {body_error, {iterator, foo}}},
            nhttpc_error:wrap({error, {request, #{type => upload_error, reason => foo}}})
        ),
        ?assertEqual(
            {error, {stream_error, cancelled}},
            nhttpc_error:wrap(
                {error, {request, #{type => stream_stopped, reason => cancel_requested}}}
            )
        )
    ].

wrap_http2_map_shapes(_Config) ->
    [
        ?assertEqual(
            {error, {protocol_error, http2, {h2, internal_error}}},
            nhttpc_error:wrap({error, {http2, #{type => goaway, error_code => internal_error}}})
        ),
        ?assertEqual(
            {error, {stream_error, {protocol, {h2, refused_stream}}}},
            nhttpc_error:wrap(
                {error, {http2, #{type => stream_reset, error_code => refused_stream}}}
            )
        ),
        ?assertEqual(
            {error, {stream_error, cancelled}},
            nhttpc_error:wrap({error, {http2, #{type => cancelled}}})
        ),
        ?assertEqual(
            {error, {stream_error, {flow_control, exhausted}}},
            nhttpc_error:wrap(
                {error, {http2, #{type => flow_control_error, reason => exhausted}}}
            )
        )
    ].

wrap_pool_map_shapes(_Config) ->
    [
        ?assertEqual(
            {error, {timeout, checkout}},
            nhttpc_error:wrap({error, {pool, #{type => checkout_timeout}}})
        ),
        ?assertEqual(
            {error, pool_closed},
            nhttpc_error:wrap({error, {pool, #{type => draining}}})
        ),
        ?assertEqual(
            {error, pool_full},
            nhttpc_error:wrap({error, {pool, #{type => exhausted}}})
        ),
        ?assertEqual(
            {error, pool_full},
            nhttpc_error:wrap({error, {pool, #{type => no_connections_available}}})
        )
    ].

wrap_passthrough_canonical(_Config) ->
    %% Canonical values must round-trip unchanged.
    Canonical = [
        {invalid_request, bad_shape},
        {invalid_opts, bad_key},
        {connect_error, econnrefused},
        {tls_error, {hostname_mismatch, <<"x">>}},
        {timeout, request},
        {protocol_error, http2, {h2, internal_error}},
        {http_error, 500, []},
        {redirect_error, max_redirects_exceeded},
        {retries_exhausted, 3, {connect_error, econnrefused}},
        {stream_error, cancelled},
        {body_error, {iterator, foo}},
        {cookie_jar_error, stopped},
        {file_error, enoent},
        pool_full,
        pool_closed
    ],
    lists:foreach(
        fun(Reason) ->
            ?assertEqual({error, Reason}, nhttpc_error:wrap({error, Reason}))
        end,
        Canonical
    ).

wrap_posix_atoms_become_connect_error(_Config) ->
    [
        ?assertEqual(
            {error, {connect_error, econnrefused}},
            nhttpc_error:wrap({error, econnrefused})
        ),
        ?assertEqual(
            {error, {connect_error, econnreset}},
            nhttpc_error:wrap({error, econnreset})
        ),
        ?assertEqual(
            {error, {connect_error, ehostunreach}},
            nhttpc_error:wrap({error, ehostunreach})
        ),
        ?assertEqual(
            {error, {connect_error, nxdomain}},
            nhttpc_error:wrap({error, nxdomain})
        ),
        ?assertEqual(
            {error, {connect_error, closed}},
            nhttpc_error:wrap({error, closed})
        ),
        ?assertEqual(
            {error, {timeout, request}},
            nhttpc_error:wrap({error, timeout})
        )
    ].

from_h1_maps_protocol_error(_Config) ->
    ?assertEqual(
        {error, {protocol_error, http1_1, bad_status_line}},
        nhttpc_error:from_h1(bad_status_line)
    ),
    ?assertEqual(
        {error, {protocol_error, http1_1, bad_header}},
        nhttpc_error:from_h1(bad_header)
    ).

from_h2_maps_protocol_and_stream(_Config) ->
    ?assertEqual(
        {error, {protocol_error, http2, {h2, internal_error}}},
        nhttpc_error:from_h2({connection_error, internal_error, <<>>})
    ),
    ?assertEqual(
        {error, {stream_error, {protocol, {h2, refused_stream}}}},
        nhttpc_error:from_h2({stream_error, 1, refused_stream, <<>>})
    ),
    ?assertEqual(
        {error, {protocol_error, http2, {protocol_violation, foo, <<>>}}},
        nhttpc_error:from_h2({protocol_violation, foo, <<>>})
    ),
    ?assertEqual(
        {error, {protocol_error, http2, {h2, no_error}}},
        nhttpc_error:from_h2(no_error)
    ).

from_h3_maps_protocol_and_stream(_Config) ->
    ?assertEqual(
        {error, {protocol_error, http3, {h3, h3_general_protocol_error}}},
        nhttpc_error:from_h3({connection_error, h3_general_protocol_error, <<>>})
    ),
    ?assertEqual(
        {error, {stream_error, {protocol, {h3, h3_request_cancelled}}}},
        nhttpc_error:from_h3({stream_error, 1, h3_request_cancelled, <<>>})
    ),
    ?assertEqual(
        {error, {protocol_error, http3, {protocol_violation, foo, <<>>}}},
        nhttpc_error:from_h3({protocol_violation, foo, <<>>})
    ),
    ?assertEqual(
        {error, {protocol_error, http3, {h3, h3_no_error}}},
        nhttpc_error:from_h3(h3_no_error)
    ).

from_ssl_maps_tls_taxonomy(_Config) ->
    ?assertEqual(
        {error, {tls_error, {tls_alert, {bad_certificate, "msg"}}}},
        nhttpc_error:from_ssl({tls_alert, {bad_certificate, "msg"}})
    ),
    ?assertEqual(
        {error, {tls_error, {options, bad_opt}}},
        nhttpc_error:from_ssl({options, bad_opt})
    ),
    ?assertEqual(
        {error, {tls_error, {handshake, bad}}},
        nhttpc_error:from_ssl({handshake_failure, bad})
    ),
    ?assertEqual(
        {error, {tls_error, {hostname_mismatch, <<"example.com">>}}},
        nhttpc_error:from_ssl({hostname_check_failed, <<"example.com">>})
    ),
    ?assertEqual(
        {error, {tls_error, {hostname_mismatch, <<"example.com">>}}},
        nhttpc_error:from_ssl({hostname_check_failed, "example.com"})
    ),
    ?assertEqual(
        {error, {connect_error, closed}},
        nhttpc_error:from_ssl(closed)
    ),
    ?assertEqual(
        {error, {tls_error, {handshake, some_reason}}},
        nhttpc_error:from_ssl(some_reason)
    ).

from_file_maps_file_error(_Config) ->
    ?assertEqual({error, {file_error, enoent}}, nhttpc_error:from_file(enoent)),
    ?assertEqual({error, {file_error, badarg}}, nhttpc_error:from_file(badarg)),
    ?assertEqual({error, {file_error, system_limit}}, nhttpc_error:from_file(system_limit)),
    ?assertEqual({error, {file_error, terminated}}, nhttpc_error:from_file(terminated)).

%%%-----------------------------------------------------------------------------
%%% DIRECT CLASSIFIER COVERAGE (plan 14)
%%%-----------------------------------------------------------------------------

direct_classifier_constructors(_Config) ->
    Cases = [
        {nhttpc_error:body_error({file_read, enospc}), {error, {body_error, {file_read, enospc}}}},
        {nhttpc_error:body_error({file_open, enoent}), {error, {body_error, {file_open, enoent}}}},
        {nhttpc_error:body_error({iterator, blew_up}), {error, {body_error, {iterator, blew_up}}}},
        {nhttpc_error:body_error(size_mismatch), {error, {body_error, size_mismatch}}},
        {nhttpc_error:connect_error(econnrefused), {error, {connect_error, econnrefused}}},
        {nhttpc_error:connect_error(enetunreach), {error, {connect_error, enetunreach}}},
        {nhttpc_error:connect_error(some_atom), {error, {connect_error, some_atom}}},
        {nhttpc_error:connect_error({pool, draining}), {error, {connect_error, {pool, draining}}}},
        {nhttpc_error:connect_error({inet, einval}), {error, {connect_error, {inet, einval}}}},
        {nhttpc_error:connection_closed(), {error, {connect_error, closed}}},
        {nhttpc_error:cookie_jar_error(stopped), {error, {cookie_jar_error, stopped}}},
        {
            nhttpc_error:cookie_jar_error(persist_timeout),
            {error, {cookie_jar_error, persist_timeout}}
        },
        {
            nhttpc_error:cookie_jar_error({file, eacces}),
            {error, {cookie_jar_error, {file, eacces}}}
        },
        {nhttpc_error:file_error(eacces), {error, {file_error, eacces}}},
        {nhttpc_error:file_error(badarg), {error, {file_error, badarg}}},
        {nhttpc_error:file_error(system_limit), {error, {file_error, system_limit}}},
        {nhttpc_error:file_error(terminated), {error, {file_error, terminated}}},
        {
            nhttpc_error:http_error(404, [{<<"server">>, <<"x">>}]),
            {error, {http_error, 404, [{<<"server">>, <<"x">>}]}}
        },
        {nhttpc_error:invalid_opts(unknown_key), {error, {invalid_opts, unknown_key}}},
        {nhttpc_error:invalid_request(bad_shape), {error, {invalid_request, bad_shape}}},
        {nhttpc_error:pool_closed(), {error, pool_closed}},
        {nhttpc_error:pool_full(), {error, pool_full}},
        {
            nhttpc_error:protocol_error(http1_1, bad_status_line),
            {error, {protocol_error, http1_1, bad_status_line}}
        },
        {
            nhttpc_error:protocol_error(http2, {h2, protocol_error}),
            {error, {protocol_error, http2, {h2, protocol_error}}}
        },
        {
            nhttpc_error:protocol_error(http3, {h3, h3_internal_error}),
            {error, {protocol_error, http3, {h3, h3_internal_error}}}
        },
        {
            nhttpc_error:redirect_error(max_redirects_exceeded),
            {error, {redirect_error, max_redirects_exceeded}}
        },
        {
            nhttpc_error:redirect_error({redirect_loop, <<"http://x/">>}),
            {error, {redirect_error, {redirect_loop, <<"http://x/">>}}}
        },
        {
            nhttpc_error:redirect_error({invalid_redirect_location, <<"bad">>}),
            {error, {redirect_error, {invalid_redirect_location, <<"bad">>}}}
        },
        {
            nhttpc_error:redirect_error({cross_origin_blocked, <<"https://x/">>}),
            {error, {redirect_error, {cross_origin_blocked, <<"https://x/">>}}}
        },
        {
            nhttpc_error:retries_exhausted(5, {connect_error, econnreset}),
            {error, {retries_exhausted, 5, {connect_error, econnreset}}}
        },
        {nhttpc_error:stream_error(rst_stream), {error, {stream_error, rst_stream}}},
        {
            nhttpc_error:stream_error({flow_control, exhausted}),
            {error, {stream_error, {flow_control, exhausted}}}
        },
        {nhttpc_error:stream_error(cancelled), {error, {stream_error, cancelled}}},
        {
            nhttpc_error:stream_error(exceeded_max_response_size),
            {error, {stream_error, exceeded_max_response_size}}
        },
        {nhttpc_error:timeout(connect), {error, {timeout, connect}}},
        {nhttpc_error:timeout(request), {error, {timeout, request}}},
        {nhttpc_error:timeout(idle), {error, {timeout, idle}}},
        {nhttpc_error:timeout(body), {error, {timeout, body}}},
        {
            nhttpc_error:tls_error({tls_alert, {bad_certificate, "msg"}}),
            {error, {tls_error, {tls_alert, {bad_certificate, "msg"}}}}
        },
        {nhttpc_error:tls_error({options, bad}), {error, {tls_error, {options, bad}}}},
        {nhttpc_error:tls_error({handshake, oops}), {error, {tls_error, {handshake, oops}}}},
        {
            nhttpc_error:tls_error({hostname_mismatch, <<"x">>}),
            {error, {tls_error, {hostname_mismatch, <<"x">>}}}
        }
    ],
    [?assertEqual(Expected, Actual) || {Actual, Expected} <- Cases].

direct_classifier_mappers(_Config) ->
    H1Cases = [
        bad_request_line,
        bad_status_line,
        bad_header,
        header_too_large,
        too_many_headers,
        {body_too_large, 1024, 512},
        invalid_content_length,
        duplicate_content_length,
        conflicting_framing,
        unsupported_transfer_encoding,
        invalid_chunk_size,
        incomplete_chunk,
        invalid_method,
        invalid_version,
        unexpected_eof,
        {protocol_error, oops}
    ],
    [
        ?assertEqual({error, {protocol_error, http1_1, Reason}}, nhttpc_error:from_h1(Reason))
     || Reason <- H1Cases
    ],
    H2Codes = [
        protocol_error,
        internal_error,
        refused_stream,
        enhance_your_calm,
        cancel,
        no_error,
        compression_error
    ],
    [
        ?assertEqual(
            {error, {protocol_error, http2, {h2, Code}}},
            nhttpc_error:from_h2(Code)
        )
     || Code <- H2Codes
    ],
    ?assertEqual(
        {error, {protocol_error, http2, {h2, 7}}},
        nhttpc_error:from_h2(7)
    ),
    H3Codes = [
        h3_no_error,
        h3_general_protocol_error,
        h3_internal_error,
        h3_request_cancelled,
        h3_request_incomplete,
        h3_excessive_load,
        h3_id_error,
        h3_frame_unexpected
    ],
    [
        ?assertEqual(
            {error, {protocol_error, http3, {h3, Code}}},
            nhttpc_error:from_h3(Code)
        )
     || Code <- H3Codes
    ],
    SslCases = [
        {
            {tls_alert, {handshake_failure, "ssl_record"}},
            {error, {tls_error, {tls_alert, {handshake_failure, "ssl_record"}}}}
        },
        {
            {options, {certfile, "/no/such"}},
            {error, {tls_error, {options, {certfile, "/no/such"}}}}
        },
        {
            {handshake_failure, {certificate, unknown_ca}},
            {error, {tls_error, {handshake, {certificate, unknown_ca}}}}
        },
        {
            {hostname_check_failed, <<"example.com">>},
            {error, {tls_error, {hostname_mismatch, <<"example.com">>}}}
        },
        {
            {hostname_check_failed, "host.example.com"},
            {error, {tls_error, {hostname_mismatch, <<"host.example.com">>}}}
        },
        {closed, {error, {connect_error, closed}}},
        {econnrefused, {error, {tls_error, {handshake, econnrefused}}}}
    ],
    [
        ?assertEqual(Expected, nhttpc_error:from_ssl(Input))
     || {Input, Expected} <- SslCases
    ],
    FileCases = [enoent, eacces, enospc, eio, badarg, terminated, system_limit, eperm],
    [
        ?assertEqual({error, {file_error, Reason}}, nhttpc_error:from_file(Reason))
     || Reason <- FileCases
    ].

wrap_handles_redirect_and_connection_shapes(_Config) ->
    ?assertEqual(
        {error, {redirect_error, {redirect_loop, <<"http://loop/">>}}},
        nhttpc_error:wrap({error, {redirect_loop, <<"http://loop/">>}})
    ),
    ?assertEqual(
        {error, {redirect_error, {invalid_redirect_location, <<"bad">>}}},
        nhttpc_error:wrap({error, {invalid_redirect_location, <<"bad">>}})
    ),
    ?assertEqual(
        {error, {connect_error, closed}},
        nhttpc_error:wrap({error, {connection_down, normal}})
    ),
    ?assertEqual(
        {error, {connect_error, enetunreach}},
        nhttpc_error:wrap({error, enetunreach})
    ).

wrap_handles_unmatched_atom_and_term(_Config) ->
    %% Atom present in the posix table but not pattern-matched explicitly —
    %% falls through to the is_posix_atom guard with `true`.
    ?assertEqual(
        {error, {connect_error, eacces}},
        nhttpc_error:wrap({error, eacces})
    ),
    %% Atom not in the posix table — falls through with `false`.
    ?assertEqual(
        {error, {connect_error, not_a_posix_atom}},
        nhttpc_error:wrap({error, not_a_posix_atom})
    ),
    %% Non-atom term falls into the final catch-all of wrap_reason/1.
    ?assertEqual(
        {error, {connect_error, 42}},
        nhttpc_error:wrap({error, 42})
    ),
    ?assertEqual(
        {error, {connect_error, [1, 2, 3]}},
        nhttpc_error:wrap({error, [1, 2, 3]})
    ),
    ?assertEqual(
        {error, {connect_error, <<"opaque">>}},
        nhttpc_error:wrap({error, <<"opaque">>})
    ),
    ?assertEqual(
        {error, {connect_error, #{a => 1}}},
        nhttpc_error:wrap({error, #{a => 1}})
    ).

wrap_handles_bare_reason_and_ok_passthrough(_Config) ->
    %% wrap/1 accepts raw reasons (not wrapped in `{error, _}`).
    ?assertEqual(
        {error, {connect_error, econnrefused}},
        nhttpc_error:wrap(econnrefused)
    ),
    ?assertEqual(
        {error, {timeout, request}},
        nhttpc_error:wrap(timeout)
    ),
    ?assertEqual(
        {error, pool_full},
        nhttpc_error:wrap(pool_full)
    ),
    %% wrap_result/1 passes ok / {ok, _} through unchanged.
    ?assertEqual(ok, nhttpc_error:wrap_result(ok)),
    ?assertEqual({ok, foo}, nhttpc_error:wrap_result({ok, foo})),
    ?assertEqual(
        {error, {connect_error, closed}},
        nhttpc_error:wrap_result({error, closed})
    ).

wrap_connection_map_tls_and_unknown(_Config) ->
    %% Every classify_tls/1 arm reached via the connection map.
    ?assertEqual(
        {error, {tls_error, {tls_alert, {bad_certificate, "msg"}}}},
        nhttpc_error:wrap(
            {error,
                {connection, #{
                    type => tls_error, reason => {tls_alert, {bad_certificate, "msg"}}
                }}}
        )
    ),
    ?assertEqual(
        {error, {tls_error, {options, {certfile, "/nope"}}}},
        nhttpc_error:wrap(
            {error, {connection, #{type => tls_error, reason => {options, {certfile, "/nope"}}}}}
        )
    ),
    ?assertEqual(
        {error, {tls_error, {handshake, bad}}},
        nhttpc_error:wrap(
            {error, {connection, #{type => tls_error, reason => {handshake_failure, bad}}}}
        )
    ),
    ?assertEqual(
        {error, {tls_error, {hostname_mismatch, <<"example.com">>}}},
        nhttpc_error:wrap(
            {error,
                {connection, #{
                    type => tls_error, reason => {hostname_check_failed, <<"example.com">>}
                }}}
        )
    ),
    ?assertEqual(
        {error, {tls_error, {hostname_mismatch, <<"host.example.com">>}}},
        nhttpc_error:wrap(
            {error,
                {connection, #{
                    type => tls_error, reason => {hostname_check_failed, "host.example.com"}
                }}}
        )
    ),
    ?assertEqual(
        {error, {tls_error, {handshake, mystery_reason}}},
        nhttpc_error:wrap(
            {error, {connection, #{type => tls_error, reason => mystery_reason}}}
        )
    ),
    %% Unknown connection-map type falls to the catch-all.
    ?assertEqual(
        {error, {connect_error, #{type => completely_unknown}}},
        nhttpc_error:wrap(
            {error, {connection, #{type => completely_unknown}}}
        )
    ).

wrap_request_map_send_error_and_unknown(_Config) ->
    ?assertEqual(
        {error, {connect_error, epipe}},
        nhttpc_error:wrap(
            {error, {request, #{type => send_error, posix => epipe}}}
        )
    ),
    ?assertEqual(
        {error, {body_error, {file_read, eio}}},
        nhttpc_error:wrap(
            {error, {request, #{type => file_error, reason => eio}}}
        )
    ),
    ?assertEqual(
        {error, {stream_error, {protocol, #{type => mystery_request_kind}}}},
        nhttpc_error:wrap(
            {error, {request, #{type => mystery_request_kind}}}
        )
    ).

wrap_http2_map_remaining_shapes(_Config) ->
    ?assertEqual(
        {error, {stream_error, {protocol, internal_error}}},
        nhttpc_error:wrap(
            {error, {http2, #{type => stream_closed, reason => internal_error}}}
        )
    ),
    ?assertEqual(
        {error, {stream_error, {protocol, refused_stream}}},
        nhttpc_error:wrap(
            {error, {http2, #{type => refused}}}
        )
    ),
    ?assertEqual(
        {error, {protocol_error, http2, {h2, enhance_your_calm}}},
        nhttpc_error:wrap(
            {error, {http2, #{type => rate_limited}}}
        )
    ),
    ?assertEqual(
        {error, {protocol_error, http2, #{type => unknown_http2_kind}}},
        nhttpc_error:wrap(
            {error, {http2, #{type => unknown_http2_kind}}}
        )
    ).

wrap_pool_map_unknown_branch(_Config) ->
    ?assertEqual(
        {error, {connect_error, {pool, #{type => unfamiliar}}}},
        nhttpc_error:wrap(
            {error, {pool, #{type => unfamiliar}}}
        )
    ).

classify_stream_stopped_branches(_Config) ->
    ?assertEqual(
        {error, {body_error, {file_read, enoent}}},
        nhttpc_error:wrap(
            {error, {request, #{type => stream_stopped, reason => {file_error, enoent}}}}
        )
    ),
    ?assertEqual(
        {error, {stream_error, exceeded_max_response_size}},
        nhttpc_error:wrap(
            {error,
                {request, #{
                    type => stream_stopped, reason => {response_too_large, 100, 50}
                }}}
        )
    ),
    ?assertEqual(
        {error, {stream_error, cancelled}},
        nhttpc_error:wrap(
            {error, {request, #{type => stream_stopped, reason => user_requested_stop}}}
        )
    ),
    ?assertEqual(
        {error, {stream_error, cancelled}},
        nhttpc_error:wrap(
            {error, {request, #{type => stream_stopped, reason => cancelled}}}
        )
    ),
    ?assertEqual(
        {error, {stream_error, cancelled}},
        nhttpc_error:wrap(
            {error, {request, #{type => stream_stopped, reason => something_else}}}
        )
    ).

classify_stream_stopped_foreign_file_error(_Config) ->
    ?assertEqual(
        {error, {stream_error, cancelled}},
        nhttpc_error:wrap(
            {error, {request, #{type => stream_stopped, reason => {file_error, foo}}}}
        )
    ).

classify_tls_alert_shapes(_Config) ->
    %% from_ssl/1 round-trip across every shape classify_tls/1 understands.
    Cases = [
        {
            {tls_alert, {handshake_failure, "ssl"}},
            {error, {tls_error, {tls_alert, {handshake_failure, "ssl"}}}}
        },
        {{options, {versions, []}}, {error, {tls_error, {options, {versions, []}}}}},
        {{handshake_failure, expired_cert}, {error, {tls_error, {handshake, expired_cert}}}},
        {
            {hostname_check_failed, <<"a.example">>},
            {error, {tls_error, {hostname_mismatch, <<"a.example">>}}}
        },
        {
            {hostname_check_failed, "b.example"},
            {error, {tls_error, {hostname_mismatch, <<"b.example">>}}}
        },
        {closed, {error, {connect_error, closed}}},
        {mystery_reason, {error, {tls_error, {handshake, mystery_reason}}}}
    ],
    [?assertEqual(Expected, nhttpc_error:from_ssl(In)) || {In, Expected} <- Cases].

posix_atom_table_round_trip(_Config) ->
    %% Every POSIX atom listed in is_posix_atom/1 must round-trip through
    %% wrap/1 as {connect_error, Atom}. The atoms with explicit clauses in
    %% wrap_reason/1 still need coverage of that entry path.
    Posix = [
        eacces,
        eaddrinuse,
        eaddrnotavail,
        eafnosupport,
        eagain,
        eaisconn,
        econnaborted,
        econnrefused,
        econnreset,
        edestaddrreq,
        efault,
        ehostdown,
        ehostunreach,
        einprogress,
        eintr,
        einval,
        eio,
        eisconn,
        emfile,
        emsgsize,
        enetdown,
        enetreset,
        enetunreach,
        enfile,
        enobufs,
        enoent,
        enomem,
        enoprotoopt,
        enospc,
        enotconn,
        enotsock,
        eopnotsupp,
        epipe,
        eproto,
        eprotonosupport,
        eprototype,
        erange,
        eshutdown,
        esocktnosupport,
        etimedout,
        eusers,
        ewouldblock,
        exbadport,
        exbadseq,
        nxdomain
    ],
    [
        ?assertEqual(
            {error, {connect_error, Atom}},
            nhttpc_error:wrap({error, Atom})
        )
     || Atom <- Posix
    ],
    %% Synthetic atom that is intentionally NOT in the posix table — drives
    %% the `false` clause of is_posix_atom/1.
    ?assertEqual(
        {error, {connect_error, definitely_not_a_posix_atom}},
        nhttpc_error:wrap({error, definitely_not_a_posix_atom})
    ).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

is_canonical_error({invalid_request, _}) -> true;
is_canonical_error({invalid_opts, _}) -> true;
is_canonical_error({connect_error, _}) -> true;
is_canonical_error({tls_error, _}) -> true;
is_canonical_error({timeout, _}) -> true;
is_canonical_error({protocol_error, _, _}) -> true;
is_canonical_error({http_error, _, _}) -> true;
is_canonical_error({redirect_error, _}) -> true;
is_canonical_error({retries_exhausted, _, _}) -> true;
is_canonical_error({stream_error, _}) -> true;
is_canonical_error({body_error, _}) -> true;
is_canonical_error({cookie_jar_error, _}) -> true;
is_canonical_error({file_error, _}) -> true;
is_canonical_error(pool_full) -> true;
is_canonical_error(pool_closed) -> true;
is_canonical_error(_) -> false.

audit_module(Module) ->
    BeamPath = locate_beam(Module),
    {ok, {Module, [{abstract_code, {raw_abstract_v1, Forms}}]}} =
        beam_lib:chunks(BeamPath, [abstract_code]),
    Exports = sets:from_list(Module:module_info(exports)),
    Specs = collect_specs(Forms),
    lists:flatmap(
        fun({{Mod, F, A}, FunType}) ->
            case Mod =:= Module andalso sets:is_element({F, A}, Exports) of
                true -> audit_fun_type(Module, F, A, FunType);
                false -> []
            end
        end,
        Specs
    ).

locate_beam(Module) ->
    %% code:which/1 returns cover_compiled when running under cover; in that
    %% case fall back to the on-disk beam from the rebar build tree.
    case code:which(Module) of
        cover_compiled ->
            Filename = atom_to_list(Module) ++ ".beam",
            case code:where_is_file(Filename) of
                non_existing ->
                    AppDir = code:lib_dir(nhttpc),
                    filename:join([AppDir, "ebin", Filename]);
                Path ->
                    Path
            end;
        Path when is_list(Path) ->
            Path
    end.

collect_specs(Forms) ->
    lists:flatmap(
        fun
            ({attribute, _, spec, {{F, A}, [FunType]}}) ->
                [{{undefined, F, A}, FunType}];
            ({attribute, _, spec, {{Mod, F, A}, [FunType]}}) ->
                [{{Mod, F, A}, FunType}];
            (_) ->
                []
        end,
        Forms
    ).

audit_fun_type(Module, Name, Arity, FunType) ->
    Return = extract_return_type(FunType),
    case has_bare_term_error(Return) of
        true -> [{Module, Name, Arity}];
        false -> []
    end.

extract_return_type({type, _, 'fun', [_Args, Return]}) -> Return;
extract_return_type({type, _, bounded_fun, [InnerFun, _]}) -> extract_return_type(InnerFun);
extract_return_type(_) -> none.

%% Look for `{error, term()}` as a tuple type in the return.
has_bare_term_error({type, _, union, Members}) ->
    lists:any(fun has_bare_term_error/1, Members);
has_bare_term_error({type, _, tuple, [{atom, _, error}, {type, _, term, []}]}) ->
    true;
has_bare_term_error(_) ->
    false.

%% A GOAWAY that carries debug data reaches the caller with the exact
%% bytes, unparsed, as a third element of the `h2` detail. Every existing
%% shape is unchanged: a GOAWAY without debug data, or with an empty
%% payload, keeps the two-element detail.
error_goaway_map_shape(_Config) ->
    Reason = <<"{\"reason\":\"Shutdown\"}">>,
    ?assertEqual(
        {error, {protocol_error, http2, {h2, no_error, Reason}}},
        nhttpc_error:wrap(
            {error, {http2, #{type => goaway, error_code => no_error, debug_data => Reason}}}
        )
    ),
    ?assertEqual(
        {error, {protocol_error, http2, {h2, no_error, Reason}}},
        nhttpc_error:wrap(
            {error,
                {http2, #{
                    type => goaway, error_code => no_error, debug_data => Reason, retryable => true
                }}}
        )
    ),
    ?assertEqual(
        {error, {protocol_error, http2, {h2, no_error}}},
        nhttpc_error:wrap(
            {error, {http2, #{type => goaway, error_code => no_error, debug_data => <<>>}}}
        )
    ),
    ?assertEqual(
        {error, {protocol_error, http2, {h2, internal_error}}},
        nhttpc_error:wrap({error, {http2, #{type => goaway, error_code => internal_error}}})
    ),
    ?assertEqual(
        {error, {protocol_error, http2, {h2, internal_error}}},
        nhttpc_error:wrap(
            {error, {http2, #{type => goaway, error_code => internal_error, retryable => true}}}
        )
    ),
    ok.
