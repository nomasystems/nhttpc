%%%-----------------------------------------------------------------------------
%%% @doc nhttpc_telemetry test suite.
%%%
%%% Tests for telemetry configuration and event emission.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_telemetry_SUITE).

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

%% Configuration tests
-export([
    config_false/1,
    config_true/1,
    config_map_both/1,
    config_map_traces_only/1,
    config_map_metrics_only/1,
    config_empty_opts/1
]).

%% Helper function tests
-export([
    traces_enabled_true/1,
    traces_enabled_false/1,
    traces_enabled_disabled_config/1,
    metrics_enabled_true/1,
    metrics_enabled_false/1,
    metrics_enabled_disabled_config/1
]).

%% Event emission tests
-export([
    emit_request_start/1,
    emit_request_stop/1,
    emit_request_error/1,
    emit_connect_start/1,
    emit_connect_stop/1,
    emit_connect_error/1,
    emit_pool_checkout/1,
    emit_pool_checkin/1,
    emit_pool_timeout/1,
    emit_pool_exhausted/1,
    emit_idle_timeout/1
]).

%% Disabled config tests
-export([
    emit_disabled_config/1
]).

%% Event dispatch tests
-export([
    dispatch_request_start/1,
    dispatch_request_stop/1,
    dispatch_request_error/1,
    dispatch_disabled_emits_nothing/1,
    dispatch_events_only/1,
    dispatch_traces_only/1,
    dispatch_pooled_request/1,
    dispatch_pooled_request_disabled/1
]).

%% Metrics tests
-export([
    enabled_all_false/1,
    active_requests_round_trip/1,
    active_requests_survives_crash/1,
    active_requests_untouched_without_metrics/1,
    instruments_registered_with_meter/1,
    connect_duration_carries_authority/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
        {group, configuration},
        {group, helpers},
        {group, events},
        {group, disabled},
        {group, dispatch},
        {group, metrics}
    ].

groups() ->
    [
        {configuration, [sequence], [
            config_false,
            config_true,
            config_map_both,
            config_map_traces_only,
            config_map_metrics_only,
            config_empty_opts
        ]},
        {helpers, [sequence], [
            traces_enabled_true,
            traces_enabled_false,
            traces_enabled_disabled_config,
            metrics_enabled_true,
            metrics_enabled_false,
            metrics_enabled_disabled_config
        ]},
        {events, [sequence], [
            emit_request_start,
            emit_request_stop,
            emit_request_error,
            emit_connect_start,
            emit_connect_stop,
            emit_connect_error,
            emit_pool_checkout,
            emit_pool_checkin,
            emit_pool_timeout,
            emit_pool_exhausted,
            emit_idle_timeout
        ]},
        {disabled, [sequence], [
            emit_disabled_config
        ]},
        %% Not `sequence': each case sets up and tears down its own handler,
        %% pool and mailbox expectations, so one failure should not auto-skip
        %% the rest and hide independent breakage.
        {dispatch, [], [
            dispatch_request_start,
            dispatch_request_stop,
            dispatch_request_error,
            dispatch_disabled_emits_nothing,
            dispatch_events_only,
            dispatch_traces_only,
            dispatch_pooled_request,
            dispatch_pooled_request_disabled
        ]},
        %% Likewise independent: every case reads the counter first and asserts
        %% its own delta, so none of them depends on the absolute value or on
        %% the case before it.
        {metrics, [], [
            enabled_all_false,
            active_requests_round_trip,
            active_requests_survives_crash,
            active_requests_untouched_without_metrics,
            instruments_registered_with_meter,
            connect_duration_carries_authority
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) when Group =:= dispatch; Group =:= metrics ->
    {ok, _} = application:ensure_all_started(telemetry),
    case
        nhttp:start_link(list_to_atom("test_telemetry_server_" ++ atom_to_list(Group)), #{
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
            ct:fail("Failed to start test server: ~p", [Reason])
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= dispatch; Group =:= metrics ->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% CONFIGURATION TESTS
%%%-----------------------------------------------------------------------------

%% Test config/1 with otel => false
config_false(_Config) ->
    Opts = #{otel => false},
    Result = nhttpc_telemetry:config(Opts),
    ?assertEqual(false, Result),
    ok.

%% Test config/1 with otel => true (shorthand for both enabled)
config_true(_Config) ->
    Opts = #{otel => true},
    Result = nhttpc_telemetry:config(Opts),
    ?assertMatch(#{traces := true, metrics := true}, Result),
    ok.

%% Test config/1 with explicit map config (both enabled)
config_map_both(_Config) ->
    Opts = #{otel => #{traces => true, metrics => true}},
    Result = nhttpc_telemetry:config(Opts),
    ?assertMatch(#{traces := true, metrics := true}, Result),
    ok.

%% Test config/1 with traces only
config_map_traces_only(_Config) ->
    Opts = #{otel => #{traces => true}},
    Result = nhttpc_telemetry:config(Opts),
    ?assertMatch(#{traces := true, metrics := false}, Result),
    ok.

%% Test config/1 with metrics only
config_map_metrics_only(_Config) ->
    Opts = #{otel => #{metrics => true}},
    Result = nhttpc_telemetry:config(Opts),
    ?assertMatch(#{traces := false, metrics := true}, Result),
    ok.

%% Test config/1 with no otel key (defaults to false)
config_empty_opts(_Config) ->
    Opts = #{},
    Result = nhttpc_telemetry:config(Opts),
    ?assertEqual(false, Result),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPER FUNCTION TESTS
%%%-----------------------------------------------------------------------------

%% Test traces_enabled/1 returns true when traces enabled
traces_enabled_true(_Config) ->
    Config = #{traces => true, metrics => false},
    ?assertEqual(true, nhttpc_telemetry:traces_enabled(Config)),
    ok.

%% Test traces_enabled/1 returns false when traces disabled
traces_enabled_false(_Config) ->
    Config = #{traces => false, metrics => true},
    ?assertEqual(false, nhttpc_telemetry:traces_enabled(Config)),
    ok.

%% Test traces_enabled/1 returns false when config is false
traces_enabled_disabled_config(_Config) ->
    ?assertEqual(false, nhttpc_telemetry:traces_enabled(false)),
    ok.

%% Test metrics_enabled/1 returns true when metrics enabled
metrics_enabled_true(_Config) ->
    Config = #{traces => false, metrics => true},
    ?assertEqual(true, nhttpc_telemetry:metrics_enabled(Config)),
    ok.

%% Test metrics_enabled/1 returns false when metrics disabled
metrics_enabled_false(_Config) ->
    Config = #{traces => true, metrics => false},
    ?assertEqual(false, nhttpc_telemetry:metrics_enabled(Config)),
    ok.

%% Test metrics_enabled/1 returns false when config is false
metrics_enabled_disabled_config(_Config) ->
    ?assertEqual(false, nhttpc_telemetry:metrics_enabled(false)),
    ok.

%%%-----------------------------------------------------------------------------
%%% EVENT EMISSION TESTS
%%%-----------------------------------------------------------------------------

%% Test request start event emission
emit_request_start(_Config) ->
    Config = #{traces => true, metrics => true},
    Meta = #{method => <<"GET">>, url => <<"https://example.com">>},
    StartTime = nhttpc_telemetry:client_request_start(Config, Meta),
    ?assert(is_integer(StartTime)),
    ok.

%% Test request stop event emission
emit_request_stop(_Config) ->
    Config = #{traces => true, metrics => true},
    StartTime = erlang:monotonic_time(),
    Result = #{status => 200},
    ?assertEqual(ok, nhttpc_telemetry:client_request_stop(Config, StartTime, Result)),
    ok.

%% Test request error event emission
emit_request_error(_Config) ->
    Config = #{traces => true, metrics => true},
    StartTime = erlang:monotonic_time(),
    Meta = #{reason => timeout},
    ?assertEqual(ok, nhttpc_telemetry:client_request_error(Config, StartTime, Meta)),
    ok.

%% Test connect start event emission
emit_connect_start(_Config) ->
    Config = #{traces => true, metrics => true},
    Meta = #{host => <<"example.com">>, port => 443, transport => ssl},
    StartTime = nhttpc_telemetry:client_connect_start(Config, Meta),
    ?assert(is_integer(StartTime)),
    ok.

%% Test connect stop event emission
emit_connect_stop(_Config) ->
    Config = #{traces => true, metrics => true},
    StartTime = erlang:monotonic_time(),
    Result = #{host => <<"example.com">>, port => 443, protocol => http2},
    ?assertEqual(ok, nhttpc_telemetry:client_connect_stop(Config, StartTime, Result)),
    ok.

%% Test connect error event emission
emit_connect_error(_Config) ->
    Config = #{traces => true, metrics => true},
    StartTime = erlang:monotonic_time(),
    Meta = #{host => <<"example.com">>, port => 443, reason => econnrefused},
    ?assertEqual(ok, nhttpc_telemetry:client_connect_error(Config, StartTime, Meta)),
    ok.

%% Test pool checkout event emission
emit_pool_checkout(_Config) ->
    Config = #{traces => true, metrics => true},
    Meta = #{pool => my_pool},
    ?assertEqual(ok, nhttpc_telemetry:client_pool_checkout(Config, Meta, 100)),
    ok.

%% Test pool checkin event emission
emit_pool_checkin(_Config) ->
    Config = #{traces => true, metrics => true},
    Meta = #{pool => my_pool},
    ?assertEqual(ok, nhttpc_telemetry:client_pool_checkin(Config, Meta)),
    ok.

%% Test pool timeout event emission
emit_pool_timeout(_Config) ->
    Config = #{traces => true, metrics => true},
    Meta = #{pool => my_pool},
    ?assertEqual(ok, nhttpc_telemetry:client_pool_timeout(Config, Meta)),
    ok.

%% Test pool exhausted event emission
emit_pool_exhausted(_Config) ->
    Config = #{traces => true, metrics => true},
    Meta = #{pool => my_pool},
    ?assertEqual(ok, nhttpc_telemetry:client_pool_exhausted(Config, Meta)),
    ok.

%% Test idle timeout event emission
emit_idle_timeout(_Config) ->
    Config = #{traces => true, metrics => true},
    Meta = #{host => <<"example.com">>, port => 443},
    ?assertEqual(ok, nhttpc_telemetry:client_idle_timeout(Config, Meta)),
    ok.

%%%-----------------------------------------------------------------------------
%%% DISABLED CONFIG TESTS
%%%-----------------------------------------------------------------------------

%% Test event emission when config is false (disabled). Nothing is published
%% and nothing is recorded, but every call still returns cleanly.
emit_disabled_config(_Config) ->
    Config = false,
    %% All events should return ok without error when disabled
    Meta = #{method => <<"GET">>, url => <<"https://example.com">>},
    StartTime = nhttpc_telemetry:client_request_start(Config, Meta),
    ?assert(is_integer(StartTime)),
    ?assertEqual(ok, nhttpc_telemetry:client_request_stop(Config, StartTime, #{status => 200})),
    ?assertEqual(
        ok, nhttpc_telemetry:client_request_error(Config, StartTime, #{reason => timeout})
    ),

    ConnMeta = #{host => <<"example.com">>, port => 443, transport => ssl},
    ConnStart = nhttpc_telemetry:client_connect_start(Config, ConnMeta),
    ?assertEqual(
        ok,
        nhttpc_telemetry:client_connect_stop(Config, ConnStart, #{
            host => <<"example.com">>, port => 443, protocol => http2
        })
    ),
    ?assertEqual(
        ok,
        nhttpc_telemetry:client_connect_error(Config, ConnStart, #{
            host => <<"example.com">>, port => 443, reason => timeout
        })
    ),

    PoolMeta = #{pool => test_pool},
    ?assertEqual(ok, nhttpc_telemetry:client_pool_checkout(Config, PoolMeta, 0)),
    ?assertEqual(ok, nhttpc_telemetry:client_pool_checkin(Config, PoolMeta)),
    ?assertEqual(ok, nhttpc_telemetry:client_pool_timeout(Config, PoolMeta)),
    ?assertEqual(ok, nhttpc_telemetry:client_pool_exhausted(Config, PoolMeta)),
    ?assertEqual(ok, nhttpc_telemetry:client_idle_timeout(Config, #{host => <<"x">>, port => 80})),
    ok.

%%%-----------------------------------------------------------------------------
%%% EVENT DISPATCH TESTS
%%%-----------------------------------------------------------------------------

%% Test that [nhttpc, request, start] reaches an attached handler.
dispatch_request_start(_Config) ->
    Meta = #{
        method => <<"GET">>,
        url => <<"https://example.com/things/42">>,
        scheme => https,
        host => <<"example.com">>,
        port => 443,
        path => <<"/things/42">>,
        pool => undefined
    },
    {ok, StartTime} = with_handler([nhttpc, request, start], fun() ->
        nhttpc_telemetry:client_request_start(events_config(), Meta)
    end),
    ?assert(is_integer(StartTime)),
    {Measurements, Metadata} = recv_event([nhttpc, request, start]),
    ?assertEqual(Meta, Metadata),
    ?assert(is_integer(maps:get(monotonic_time, Measurements))),
    ok.

%% Test that [nhttpc, request, stop] carries the status and the byte counts.
dispatch_request_stop(_Config) ->
    StartTime = erlang:monotonic_time(),
    Meta = #{method => <<"POST">>, url => <<"https://example.com/">>, status => 200},
    {ok, ok} = with_handler([nhttpc, request, stop], fun() ->
        nhttpc_telemetry:client_request_stop(
            events_config(), StartTime, Meta, #{req_body_length => 12, resp_body_length => 34}
        )
    end),
    {Measurements, Metadata} = recv_event([nhttpc, request, stop]),
    ?assertEqual(Meta, Metadata),
    ?assertEqual(12, maps:get(req_body_length, Measurements)),
    ?assertEqual(34, maps:get(resp_body_length, Measurements)),
    ?assert(maps:get(duration, Measurements) >= 0),
    ok.

%% Test that [nhttpc, request, error] carries the failure reason.
dispatch_request_error(_Config) ->
    StartTime = erlang:monotonic_time(),
    Meta = #{method => <<"GET">>, url => <<"https://example.com/">>, reason => timeout},
    {ok, ok} = with_handler([nhttpc, request, error], fun() ->
        nhttpc_telemetry:client_request_error(
            events_config(), StartTime, Meta, #{req_body_length => 0, resp_body_length => 0}
        )
    end),
    {Measurements, Metadata} = recv_event([nhttpc, request, error]),
    ?assertEqual(timeout, maps:get(reason, Metadata)),
    ?assert(maps:get(duration, Measurements) >= 0),
    ok.

%% A pooled request carries a path-only URL, so the authority must come from
%% the pool target rather than from the placeholder url_info.
dispatch_pooled_request(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_telemetry_pool,
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    }),
    unlink(PoolSup),
    ok = nhttpc_pool:wait_ready(nhttpc_pool:manager(PoolRef), 5000),

    Body = <<"telemetry-body">>,
    Req = #{
        method => post,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => Body
    },
    Opts = #{
        via => {pool, PoolRef},
        follow_redirects => false,
        otel => #{events => true}
    },
    {ok, {ok, #{status := 200}}} = with_handler(
        [[nhttpc, request, start], [nhttpc, request, stop]],
        fun() -> nhttpc:request(Req, Opts) end
    ),

    {_StartMeasurements, StartMetadata} = recv_event([nhttpc, request, start]),
    ?assertEqual(<<"127.0.0.1">>, maps:get(host, StartMetadata)),
    ?assertEqual(Port, maps:get(port, StartMetadata)),
    ?assertEqual(http, maps:get(scheme, StartMetadata)),
    ?assertEqual(<<"/echo">>, maps:get(path, StartMetadata)),
    ?assertEqual(<<"POST">>, maps:get(method, StartMetadata)),
    ?assertEqual(PoolRef, maps:get(pool, StartMetadata)),

    {StopMeasurements, StopMetadata} = recv_event([nhttpc, request, stop]),
    ?assertEqual(200, maps:get(status, StopMetadata)),
    ?assertEqual(<<"127.0.0.1">>, maps:get(host, StopMetadata)),
    ?assertEqual(Port, maps:get(port, StopMetadata)),
    ?assertEqual(byte_size(Body), maps:get(req_body_length, StopMeasurements)),
    ?assert(maps:get(resp_body_length, StopMeasurements) > 0),

    ok = nhttpc:stop(PoolRef),
    ok.

%% With no otel option nothing is published, however many handlers are attached.
dispatch_disabled_emits_nothing(_Config) ->
    Meta = #{method => <<"GET">>, url => <<"https://example.com/">>},
    {ok, StartTime} = with_handler(
        [
            [nhttpc, request, start],
            [nhttpc, request, stop],
            [nhttpc, request, error],
            [nhttpc, connect, start],
            [nhttpc, connect, stop],
            [nhttpc, pool, checkout]
        ],
        fun() ->
            Start = nhttpc_telemetry:client_request_start(false, Meta),
            ok = nhttpc_telemetry:client_request_stop(false, Start, Meta#{status => 200}),
            ok = nhttpc_telemetry:client_request_error(false, Start, Meta#{reason => timeout}),
            ConnStart = nhttpc_telemetry:client_connect_start(false, #{
                host => <<"example.com">>, port => 443, transport => ssl
            }),
            ok = nhttpc_telemetry:client_connect_stop(false, ConnStart, #{
                host => <<"example.com">>, port => 443, protocol => http2
            }),
            ok = nhttpc_telemetry:client_pool_checkout(false, #{pool => test_pool}, 0),
            Start
        end
    ),
    ?assert(is_integer(StartTime)),
    ?assertEqual(no_event, recv_nothing()),
    ok.

%% `traces` and `metrics` on their own must not publish telemetry events.
dispatch_events_only(_Config) ->
    Meta = #{method => <<"GET">>, url => <<"https://example.com/">>},
    MetricsOnly = #{events => false, metrics => true, traces => true},
    {ok, _} = with_handler([nhttpc, request, start], fun() ->
        nhttpc_telemetry:client_request_start(MetricsOnly, Meta)
    end),
    ?assertEqual(no_event, recv_nothing()),

    {ok, _} = with_handler([nhttpc, request, start], fun() ->
        nhttpc_telemetry:client_request_start(events_config(), Meta)
    end),
    {_Measurements, Metadata} = recv_event([nhttpc, request, start]),
    ?assertEqual(Meta, Metadata),
    ok.

%% `traces' on its own must not publish telemetry events either. nhttpc emits
%% no spans yet, so this configuration is silent all round.
dispatch_traces_only(_Config) ->
    Meta = #{method => <<"GET">>, url => <<"https://example.com/">>},
    TracesOnly = #{events => false, metrics => false, traces => true},
    ?assertEqual(true, nhttpc_telemetry:enabled(TracesOnly)),
    {ok, _} = with_handler(
        [[nhttpc, request, start], [nhttpc, request, stop]],
        fun() ->
            Start = nhttpc_telemetry:client_request_start(TracesOnly, Meta),
            nhttpc_telemetry:client_request_stop(TracesOnly, Start, Meta#{status => 200})
        end
    ),
    ?assertEqual(no_event, recv_nothing()),
    ok.

%% The same pooled request with no otel option must stay silent end to end.
dispatch_pooled_request_disabled(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_telemetry_pool_off,
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    }),
    unlink(PoolSup),
    ok = nhttpc_pool:wait_ready(nhttpc_pool:manager(PoolRef), 5000),

    Req = #{
        method => post,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<"telemetry-body">>
    },
    Opts = #{via => {pool, PoolRef}, follow_redirects => false},
    {ok, {ok, #{status := 200}}} = with_handler(
        [
            [nhttpc, request, start],
            [nhttpc, request, stop],
            [nhttpc, pool, checkout],
            [nhttpc, pool, checkin]
        ],
        fun() -> nhttpc:request(Req, Opts) end
    ),
    ?assertEqual(no_event, recv_nothing()),

    ok = nhttpc:stop(PoolRef),
    ok.

%%%-----------------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
events_config() ->
    #{events => true, metrics => false, traces => false}.

%% Attach a handler that forwards every event to the calling process, run Fun,
%% and detach again. Events are read back with recv_event/1.
with_handler(Event, Fun) when is_atom(hd(Event)) ->
    with_handler([Event], Fun);
with_handler(Events, Fun) ->
    Self = self(),
    HandlerId = {?MODULE, erlang:unique_integer()},
    ok = telemetry:attach_many(
        HandlerId,
        Events,
        fun(Event, Measurements, Metadata, _HandlerConfig) ->
            Self ! {telemetry_event, Event, Measurements, Metadata}
        end,
        undefined
    ),
    try
        {ok, Fun()}
    after
        telemetry:detach(HandlerId)
    end.

recv_event(Event) ->
    receive
        {telemetry_event, Event, Measurements, Metadata} ->
            {Measurements, Metadata}
    after 5000 ->
        ct:fail("timeout waiting for ~p", [Event])
    end.

%% Assert the mailbox stays empty. The handler forwards synchronously, so by the
%% time the instrumented call returns any event would already be queued.
recv_nothing() ->
    receive
        {telemetry_event, Event, _Measurements, _Metadata} ->
            ct:fail("unexpected event ~p", [Event])
    after 100 ->
        no_event
    end.

%%%-----------------------------------------------------------------------------
%%% METRICS TESTS
%%%-----------------------------------------------------------------------------

%% A map with every flag off must read as disabled, so callers can skip the
%% instrumented path on it just like they do on `false'.
enabled_all_false(_Config) ->
    ?assertEqual(false, nhttpc_telemetry:enabled(false)),
    ?assertEqual(
        false, nhttpc_telemetry:enabled(#{events => false, metrics => false, traces => false})
    ),
    ?assertEqual(
        true, nhttpc_telemetry:enabled(#{events => false, metrics => false, traces => true})
    ),
    ?assertEqual(
        true, nhttpc_telemetry:enabled(#{events => true, metrics => false, traces => false})
    ),
    ok.

%% The in-flight gauge has to come back to where it started. This is what
%% catches an asymmetric increment/decrement.
active_requests_round_trip(_Config) ->
    Config = metrics_config(),
    Before = nhttpc_telemetry:active_requests(),

    lists:foreach(
        fun(N) ->
            Meta = request_meta(),
            StartTime = nhttpc_telemetry:client_request_start(Config, Meta),
            case N rem 2 of
                0 ->
                    nhttpc_telemetry:client_request_stop(
                        Config, StartTime, Meta#{status => 200}, #{}
                    );
                1 ->
                    nhttpc_telemetry:client_request_error(
                        Config, StartTime, Meta#{reason => timeout}, #{}
                    )
            end
        end,
        lists:seq(1, 10)
    ),

    ?assertEqual(Before, nhttpc_telemetry:active_requests()),
    ok.

%% A request that raises between the increment and the normal end event still
%% has to give the counter back. A transport failure is not enough here: it
%% comes back as a plain {error, _} and takes the ordinary path, so the raise
%% is injected where the request path actually runs it.
active_requests_survives_crash(Config) ->
    Port = proplists:get_value(server_port, Config),
    PoolRef = test_telemetry_pool_crash,
    {ok, PoolSup} = nhttpc_pool_sup:start_link(PoolRef, #{
        host => <<"127.0.0.1">>,
        port => Port,
        size => 1
    }),
    unlink(PoolSup),
    ok = nhttpc_pool:wait_ready(nhttpc_pool:manager(PoolRef), 5000),

    Before = nhttpc_telemetry:active_requests(),
    Req = #{
        method => post,
        path => <<"/echo">>,
        headers => [{<<"host">>, <<"127.0.0.1">>}],
        body => <<"telemetry-body">>
    },
    Opts = #{
        via => {pool, PoolRef},
        follow_redirects => false,
        otel => #{metrics => true}
    },

    ok = meck:new(nhttpc_retry, [no_link, passthrough]),
    ok = meck:expect(nhttpc_retry, should_retry, fun(_Result, _Attempt, _RetryOpts) ->
        erlang:error(telemetry_test_crash)
    end),
    try
        ?assertError(telemetry_test_crash, nhttpc:request(Req, Opts))
    after
        meck:unload(nhttpc_retry)
    end,

    ?assertEqual(Before, nhttpc_telemetry:active_requests()),

    ok = nhttpc:stop(PoolRef),
    ok.

%% Events on their own must not move the gauge.
active_requests_untouched_without_metrics(_Config) ->
    Config = #{events => true, metrics => false, traces => false},
    Before = nhttpc_telemetry:active_requests(),
    Meta = request_meta(),
    StartTime = nhttpc_telemetry:client_request_start(Config, Meta),
    nhttpc_telemetry:client_request_stop(Config, StartTime, Meta#{status => 200}, #{}),
    ?assertEqual(Before, nhttpc_telemetry:active_requests()),
    ok.

%% Recording by name only reaches a backend when the instrument was registered
%% with the meter first, so assert the registration actually happens.
instruments_registered_with_meter(_Config) ->
    Self = self(),
    ok = meck:new(otel_meter, [no_link, passthrough]),
    ok = meck:expect(otel_meter, create_histogram, fun(_Meter, Name, Opts) ->
        Self ! {created, histogram, Name, Opts},
        ok
    end),
    ok = meck:expect(otel_meter, create_counter, fun(_Meter, Name, Opts) ->
        Self ! {created, counter, Name, Opts},
        ok
    end),
    ok = meck:expect(otel_meter, create_updown_counter, fun(_Meter, Name, Opts) ->
        Self ! {created, updown_counter, Name, Opts},
        ok
    end),
    %% Pretend an SDK is running: the noop meter is skipped on purpose.
    ok = meck:new(opentelemetry_experimental, [no_link, passthrough]),
    ok = meck:expect(opentelemetry_experimental, get_meter, fun(_Name) ->
        {fake_meter_module, []}
    end),
    %% The recording side would dispatch into the fake meter module; this test
    %% is about registration, so stub it out.
    ok = meck:new(otel_histogram, [no_link, passthrough]),
    ok = meck:expect(otel_histogram, record, fun(_Ctx, _Meter, _Name, _Value, _Attrs) -> ok end),
    ok = meck:new(otel_updown_counter, [no_link, passthrough]),
    ok = meck:expect(otel_updown_counter, add, fun(_Ctx, _Meter, _Name, _Value, _Attrs) -> ok end),
    _ = persistent_term:erase(nhttpc_instruments_registered),

    try
        Config = metrics_config(),
        Meta = request_meta(),
        StartTime = nhttpc_telemetry:client_request_start(Config, Meta),
        nhttpc_telemetry:client_request_stop(Config, StartTime, Meta#{status => 200}, #{}),

        Created = collect_created([]),
        ?assert(lists:member('nhttpc.http.request.duration', Created)),
        ?assert(lists:member('nhttpc.http.active_requests', Created)),
        ?assert(lists:member('nhttpc.http.request.body.size', Created)),
        ?assert(lists:member('nhttpc.http.response.body.size', Created)),
        ?assert(lists:member('nhttpc.http.pool.queue_time', Created)),
        ?assert(lists:member('nhttpc.http.connect.duration', Created))
    after
        meck:unload(otel_updown_counter),
        meck:unload(otel_histogram),
        meck:unload(opentelemetry_experimental),
        meck:unload(otel_meter),
        _ = persistent_term:erase(nhttpc_instruments_registered)
    end,
    ok.

%% The connect duration histogram is only useful if it can be broken down by
%% upstream, so assert the authority reaches the recorded attributes.
connect_duration_carries_authority(_Config) ->
    Self = self(),
    ok = meck:new(otel_meter, [no_link, passthrough]),
    ok = meck:expect(otel_meter, create_histogram, fun(_Meter, _Name, _Opts) -> ok end),
    ok = meck:expect(otel_meter, create_counter, fun(_Meter, _Name, _Opts) -> ok end),
    ok = meck:expect(otel_meter, create_updown_counter, fun(_Meter, _Name, _Opts) -> ok end),
    ok = meck:new(opentelemetry_experimental, [no_link, passthrough]),
    ok = meck:expect(opentelemetry_experimental, get_meter, fun(_Name) ->
        {fake_meter_module, []}
    end),
    ok = meck:new(otel_histogram, [no_link, passthrough]),
    ok = meck:expect(otel_histogram, record, fun(_Ctx, _Meter, Name, _Value, Attrs) ->
        Self ! {recorded, Name, Attrs},
        ok
    end),
    _ = persistent_term:erase(nhttpc_instruments_registered),

    try
        Config = metrics_config(),
        ConnMeta = #{host => <<"example.com">>, port => 443, transport => ssl},
        StartTime = nhttpc_telemetry:client_connect_start(Config, ConnMeta),
        ok = nhttpc_telemetry:client_connect_stop(Config, StartTime, #{
            host => <<"example.com">>, port => 443, protocol => http2
        }),
        StopAttrs = recv_recorded('nhttpc.http.connect.duration'),
        ?assertEqual(<<"example.com">>, maps:get('server.address', StopAttrs)),
        ?assertEqual(443, maps:get('server.port', StopAttrs)),
        ?assertEqual(<<"2">>, maps:get('network.protocol.version', StopAttrs)),

        ok = nhttpc_telemetry:client_connect_error(Config, StartTime, #{
            host => <<"example.com">>, port => 443, reason => econnrefused
        }),
        ErrorAttrs = recv_recorded('nhttpc.http.connect.duration'),
        ?assertEqual(<<"example.com">>, maps:get('server.address', ErrorAttrs)),
        ?assertEqual(443, maps:get('server.port', ErrorAttrs))
    after
        meck:unload(otel_histogram),
        meck:unload(opentelemetry_experimental),
        meck:unload(otel_meter),
        _ = persistent_term:erase(nhttpc_instruments_registered)
    end,
    ok.

recv_recorded(Name) ->
    receive
        {recorded, Name, Attrs} -> Attrs;
        {recorded, _Other, _Attrs} -> recv_recorded(Name)
    after 1000 ->
        ct:fail({no_measurement_recorded, Name})
    end.
collect_created(Acc) ->
    receive
        {created, _Kind, Name, _Opts} -> collect_created([Name | Acc])
    after 200 ->
        Acc
    end.

metrics_config() ->
    #{events => false, metrics => true, traces => false}.

%% Full metadata, matching client_request_meta(). The narrower maps used by the
%% dispatch tests would violate the spec.
request_meta() ->
    #{
        method => <<"GET">>,
        url => <<"https://example.com/things/42">>,
        scheme => https,
        host => <<"example.com">>,
        port => 443,
        path => <<"/things/42">>,
        pool => undefined
    }.
