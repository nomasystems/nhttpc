%%%-----------------------------------------------------------------------------
%%% @doc Retry logic test suite.
%%%
%%% Tests the retry backoff strategies and retry decision logic.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_retry_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------
-export([
    all/0,
    groups/0
]).

%%%-----------------------------------------------------------------------------
%%% TEST CASES
%%%-----------------------------------------------------------------------------
-export([
    %% normalize_opts tests
    normalize_false/1,
    normalize_integer/1,
    normalize_map/1,
    normalize_invalid/1,
    normalize_method_aware_idempotent/1,
    normalize_method_aware_post/1,
    normalize_method_aware_explicit_overrides/1,
    %% should_retry tests
    should_retry_false_opts/1,
    should_retry_max_attempts/1,
    should_retry_custom_callback/1,
    should_retry_retryable_error/1,
    should_retry_non_retryable/1,
    %% compute_delay tests
    delay_exponential/1,
    delay_exponential_capped/1,
    delay_linear/1,
    delay_linear_capped/1,
    delay_constant/1,
    delay_jitter_disabled/1,
    delay_jitter_zero/1,
    %% is_retryable tests
    retryable_timeout/1,
    retryable_connection_refused/1,
    retryable_connection_reset/1,
    retryable_connection_error/1,
    retryable_status_code/1,
    retryable_non_retryable_error/1,
    retryable_non_retryable_status/1,
    %% Plan 15 additions
    retryable_auto_status_match/1,
    retryable_auto_status_miss/1,
    retryable_auto_error_classification/1,
    retryable_auto_non_response_non_error/1,
    retry_auto_respects_idempotency/1,
    is_idempotent_method_each/1,
    is_idempotent_method_undefined/1,
    delay_jitter_small_delay/1,
    delay_jitter_bounds/1,
    normalize_max_attempts_default/1,
    normalize_should_retry_callback_passthrough/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        {group, normalize},
        {group, should_retry},
        {group, compute_delay},
        {group, is_retryable},
        {group, idempotence}
    ].

groups() ->
    [
        {normalize, [parallel], [
            normalize_false,
            normalize_integer,
            normalize_map,
            normalize_invalid,
            normalize_method_aware_idempotent,
            normalize_method_aware_post,
            normalize_method_aware_explicit_overrides,
            normalize_max_attempts_default,
            normalize_should_retry_callback_passthrough
        ]},
        {should_retry, [parallel], [
            should_retry_false_opts,
            should_retry_max_attempts,
            should_retry_custom_callback,
            should_retry_retryable_error,
            should_retry_non_retryable
        ]},
        {compute_delay, [parallel], [
            delay_exponential,
            delay_exponential_capped,
            delay_linear,
            delay_linear_capped,
            delay_constant,
            delay_jitter_disabled,
            delay_jitter_zero,
            delay_jitter_small_delay,
            delay_jitter_bounds
        ]},
        {is_retryable, [parallel], [
            retryable_timeout,
            retryable_connection_refused,
            retryable_connection_reset,
            retryable_connection_error,
            retryable_status_code,
            retryable_non_retryable_error,
            retryable_non_retryable_status,
            retryable_auto_status_match,
            retryable_auto_status_miss,
            retryable_auto_error_classification,
            retryable_auto_non_response_non_error,
            retry_auto_respects_idempotency
        ]},
        {idempotence, [parallel], [
            is_idempotent_method_each,
            is_idempotent_method_undefined
        ]}
    ].

%%%-----------------------------------------------------------------------------
%%% NORMALIZE_OPTS TESTS
%%%-----------------------------------------------------------------------------

normalize_false(_Config) ->
    ?assertEqual(false, nhttpc_retry:normalize_opts(false)).

normalize_integer(_Config) ->
    Opts = nhttpc_retry:normalize_opts(3),
    ?assertEqual(3, maps:get(max_attempts, Opts)),
    ?assertEqual(100, maps:get(base_delay, Opts)),
    ?assertEqual(5000, maps:get(max_delay, Opts)),
    ?assertEqual(exponential, maps:get(backoff, Opts)),
    ?assertEqual(true, maps:get(jitter, Opts)).

normalize_map(_Config) ->
    Input = #{max_attempts => 5, base_delay => 200, backoff => linear},
    Opts = nhttpc_retry:normalize_opts(Input),
    ?assertEqual(5, maps:get(max_attempts, Opts)),
    ?assertEqual(200, maps:get(base_delay, Opts)),
    ?assertEqual(5000, maps:get(max_delay, Opts)),
    ?assertEqual(linear, maps:get(backoff, Opts)).

normalize_invalid(_Config) ->
    ?assertEqual(false, nhttpc_retry:normalize_opts(invalid)),
    ?assertEqual(false, nhttpc_retry:normalize_opts("string")).

normalize_method_aware_idempotent(_Config) ->
    %% GET keeps the permissive default set so timeouts and connection errors
    %% are retried.
    Opts = nhttpc_retry:normalize_opts(3, get),
    Conditions = maps:get(retry_on, Opts),
    ?assert(lists:member(timeout, Conditions)),
    ?assert(lists:member(connection_error, Conditions)),
    ?assert(lists:member(connection_refused, Conditions)),
    ?assert(lists:member(503, Conditions)),
    ?assertMatch({true, _}, nhttpc_retry:should_retry({error, timeout}, 1, Opts)).

normalize_method_aware_post(_Config) ->
    %% POST is non-idempotent: only connection_refused (request never left the
    %% host) is retried by default. Timeout / connection_error / 5xx must NOT
    %% trigger automatic retries — that would silently introduce
    %% at-least-once semantics for the user.
    Opts = nhttpc_retry:normalize_opts(3, post),
    Conditions = maps:get(retry_on, Opts),
    ?assertEqual([connection_refused], Conditions),
    ?assertEqual(
        false, nhttpc_retry:should_retry({error, timeout}, 1, Opts)
    ),
    ?assertEqual(
        false,
        nhttpc_retry:should_retry({error, {connect_error, econnreset}}, 1, Opts)
    ),
    ?assertEqual(
        false,
        nhttpc_retry:should_retry({ok, #{status => 503}}, 1, Opts)
    ),
    ?assertMatch(
        {true, _},
        nhttpc_retry:should_retry(
            {error, {connect_error, econnrefused}}, 1, Opts
        )
    ).

normalize_method_aware_explicit_overrides(_Config) ->
    %% A user who passes retry_on => [...] explicitly opts in regardless of
    %% method.
    Input = #{max_attempts => 3, retry_on => [timeout, 503]},
    Opts = nhttpc_retry:normalize_opts(Input, post),
    ?assertMatch({true, _}, nhttpc_retry:should_retry({error, timeout}, 1, Opts)),
    ?assertMatch(
        {true, _},
        nhttpc_retry:should_retry({ok, #{status => 503}}, 1, Opts)
    ).

%%%-----------------------------------------------------------------------------
%%% SHOULD_RETRY TESTS
%%%-----------------------------------------------------------------------------

should_retry_false_opts(_Config) ->
    ?assertEqual(false, nhttpc_retry:should_retry({error, timeout}, 1, false)).

should_retry_max_attempts(_Config) ->
    Opts = nhttpc_retry:normalize_opts(3),
    %% At attempt 3 (max_attempts), should not retry
    ?assertEqual(false, nhttpc_retry:should_retry({error, timeout}, 3, Opts)),
    %% At attempt 4 (> max_attempts), should not retry
    ?assertEqual(false, nhttpc_retry:should_retry({error, timeout}, 4, Opts)).

should_retry_custom_callback(_Config) ->
    CustomFn = fun
        ({error, custom_error}, _Attempt, _Opts) -> true;
        (_, _, _) -> false
    end,
    Opts = #{
        max_attempts => 3,
        base_delay => 100,
        max_delay => 5000,
        backoff => exponential,
        jitter => false,
        retry_on => [],
        should_retry => CustomFn
    },
    ?assertMatch({true, _}, nhttpc_retry:should_retry({error, custom_error}, 1, Opts)),
    ?assertEqual(false, nhttpc_retry:should_retry({error, other_error}, 1, Opts)).

should_retry_retryable_error(_Config) ->
    Opts = nhttpc_retry:normalize_opts(3),
    ?assertMatch({true, _}, nhttpc_retry:should_retry({error, timeout}, 1, Opts)),
    ?assertMatch(
        {true, _}, nhttpc_retry:should_retry({error, {connect_error, econnrefused}}, 1, Opts)
    ).

should_retry_non_retryable(_Config) ->
    Opts = nhttpc_retry:normalize_opts(3),
    ?assertEqual(false, nhttpc_retry:should_retry({ok, #{status => 200}}, 1, Opts)),
    ?assertEqual(false, nhttpc_retry:should_retry({error, some_unknown_error}, 1, Opts)).

%%%-----------------------------------------------------------------------------
%%% COMPUTE_DELAY TESTS
%%%-----------------------------------------------------------------------------

delay_exponential(_Config) ->
    Opts = #{
        backoff => exponential,
        base_delay => 100,
        max_delay => 10000,
        jitter => false
    },
    %% 100 * 2^0 = 100
    ?assertEqual(100, nhttpc_retry:compute_delay(1, Opts)),
    %% 100 * 2^1 = 200
    ?assertEqual(200, nhttpc_retry:compute_delay(2, Opts)),
    %% 100 * 2^2 = 400
    ?assertEqual(400, nhttpc_retry:compute_delay(3, Opts)),
    %% 100 * 2^3 = 800
    ?assertEqual(800, nhttpc_retry:compute_delay(4, Opts)).

delay_exponential_capped(_Config) ->
    Opts = #{
        backoff => exponential,
        base_delay => 1000,
        max_delay => 5000,
        jitter => false
    },
    %% 1000 * 2^2 = 4000
    ?assertEqual(4000, nhttpc_retry:compute_delay(3, Opts)),
    %% 1000 * 2^3 = 8000, capped at 5000
    ?assertEqual(5000, nhttpc_retry:compute_delay(4, Opts)),
    %% 1000 * 2^4 = 16000, capped at 5000
    ?assertEqual(5000, nhttpc_retry:compute_delay(5, Opts)).

delay_linear(_Config) ->
    Opts = #{
        backoff => linear,
        base_delay => 100,
        max_delay => 10000,
        jitter => false
    },
    %% 100 * 1 = 100
    ?assertEqual(100, nhttpc_retry:compute_delay(1, Opts)),
    %% 100 * 2 = 200
    ?assertEqual(200, nhttpc_retry:compute_delay(2, Opts)),
    %% 100 * 3 = 300
    ?assertEqual(300, nhttpc_retry:compute_delay(3, Opts)),
    %% 100 * 4 = 400
    ?assertEqual(400, nhttpc_retry:compute_delay(4, Opts)).

delay_linear_capped(_Config) ->
    Opts = #{
        backoff => linear,
        base_delay => 1000,
        max_delay => 3000,
        jitter => false
    },
    %% 1000 * 3 = 3000
    ?assertEqual(3000, nhttpc_retry:compute_delay(3, Opts)),
    %% 1000 * 4 = 4000, capped at 3000
    ?assertEqual(3000, nhttpc_retry:compute_delay(4, Opts)).

delay_constant(_Config) ->
    Opts = #{
        backoff => constant,
        base_delay => 500,
        max_delay => 10000,
        jitter => false
    },
    ?assertEqual(500, nhttpc_retry:compute_delay(1, Opts)),
    ?assertEqual(500, nhttpc_retry:compute_delay(2, Opts)),
    ?assertEqual(500, nhttpc_retry:compute_delay(5, Opts)),
    ?assertEqual(500, nhttpc_retry:compute_delay(10, Opts)).

delay_jitter_disabled(_Config) ->
    Opts = #{
        backoff => exponential,
        base_delay => 100,
        max_delay => 10000,
        jitter => false
    },
    %% Without jitter, should be deterministic
    ?assertEqual(100, nhttpc_retry:compute_delay(1, Opts)),
    ?assertEqual(100, nhttpc_retry:compute_delay(1, Opts)).

delay_jitter_zero(_Config) ->
    Opts = #{
        backoff => constant,
        base_delay => 0,
        max_delay => 10000,
        jitter => true
    },
    %% Zero delay should stay zero even with jitter enabled
    ?assertEqual(0, nhttpc_retry:compute_delay(1, Opts)).

%%%-----------------------------------------------------------------------------
%%% IS_RETRYABLE TESTS
%%%-----------------------------------------------------------------------------

retryable_timeout(_Config) ->
    Conditions = [timeout],
    ?assert(nhttpc_retry:is_retryable({error, {request_timeout, 5000}}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({error, timeout}, Conditions)).

retryable_connection_refused(_Config) ->
    Conditions = [connection_refused],
    ?assert(nhttpc_retry:is_retryable({error, {connect_error, econnrefused}}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({error, econnrefused}, Conditions)).

retryable_connection_reset(_Config) ->
    Conditions = [connection_reset],
    ?assert(nhttpc_retry:is_retryable({error, {connect_error, econnreset}}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({error, {recv_error, econnreset}}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({error, econnreset}, Conditions)).

retryable_connection_error(_Config) ->
    Conditions = [connection_error],
    ?assert(nhttpc_retry:is_retryable({error, {connect_error, ehostunreach}}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({error, {recv_error, closed}}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({error, {send_error, closed}}, Conditions)).

retryable_status_code(_Config) ->
    Conditions = [502, 503, 504],
    ?assert(nhttpc_retry:is_retryable({ok, #{status => 502}}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({ok, #{status => 503}}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({ok, #{status => 504}}, Conditions)),
    ?assertNot(nhttpc_retry:is_retryable({ok, #{status => 500}}, Conditions)).

retryable_non_retryable_error(_Config) ->
    Conditions = [timeout],
    ?assertNot(nhttpc_retry:is_retryable({error, some_other_error}, Conditions)),
    ?assertNot(nhttpc_retry:is_retryable({error, {badarg, whatever}}, Conditions)).

retryable_non_retryable_status(_Config) ->
    Conditions = [503],
    ?assertNot(nhttpc_retry:is_retryable({ok, #{status => 200}}, Conditions)),
    ?assertNot(nhttpc_retry:is_retryable({ok, #{status => 400}}, Conditions)),
    ?assertNot(nhttpc_retry:is_retryable({ok, #{status => 500}}, Conditions)).

%%%-----------------------------------------------------------------------------
%%% AUTO CLASSIFICATION TESTS
%%%-----------------------------------------------------------------------------

retryable_auto_status_match(_Config) ->
    Conditions = [auto, 503, 504],
    ?assert(nhttpc_retry:is_retryable({ok, #{status => 503}}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({ok, #{status => 504}}, Conditions)).

retryable_auto_status_miss(_Config) ->
    Conditions = [auto, 503],
    ?assertNot(nhttpc_retry:is_retryable({ok, #{status => 200}}, Conditions)),
    ?assertNot(nhttpc_retry:is_retryable({ok, #{status => 404}}, Conditions)).

retryable_auto_error_classification(_Config) ->
    %% With `auto` in the condition list, errors are routed through
    %% nhttp_error:is_retryable/1. econnrefused / timeout are retryable per
    %% the canonical classification.
    Conditions = [auto],
    ?assert(nhttpc_retry:is_retryable({error, econnrefused}, Conditions)),
    ?assert(nhttpc_retry:is_retryable({error, timeout}, Conditions)),
    ?assertNot(
        nhttpc_retry:is_retryable({error, {invalid_request, bad_shape}}, Conditions)
    ).

retryable_auto_non_response_non_error(_Config) ->
    %% Auto path with a result shape that is neither `{ok, #{status...}}` nor
    %% `{error, _}` falls through to the catch-all `false` clause.
    ?assertNot(nhttpc_retry:is_retryable(some_atom, [auto])),
    ?assertNot(nhttpc_retry:is_retryable({ok, not_a_response_map}, [auto])).

%% `auto` follows the protocol classification only for an idempotent
%% method. A mid-flight connection close or a GOAWAY cannot say whether the
%% server processed a POST, so `auto` shrinks to connection_refused there.
%% Status codes listed next to `auto` stay explicit for every method.
retry_auto_respects_idempotency(_Config) ->
    Closed = nhttp_error:connection_closed(retryable),
    Goaway = nhttp_error:goaway(no_error, retryable),
    Refused = {error, {connect_error, econnrefused}},
    Post = nhttpc_retry:normalize_opts(#{max_attempts => 3, retry_on => [auto]}, post),
    Get = nhttpc_retry:normalize_opts(#{max_attempts => 3, retry_on => [auto]}, get),
    ?assertEqual(false, nhttpc_retry:should_retry(Closed, 1, Post)),
    ?assertEqual(false, nhttpc_retry:should_retry(Goaway, 1, Post)),
    ?assertMatch({true, _}, nhttpc_retry:should_retry(Refused, 1, Post)),
    ?assertMatch({true, _}, nhttpc_retry:should_retry(Closed, 1, Get)),
    ?assertMatch({true, _}, nhttpc_retry:should_retry(Goaway, 1, Get)),
    PostWith503 = nhttpc_retry:normalize_opts(#{max_attempts => 3, retry_on => [auto, 503]}, post),
    ?assertMatch({true, _}, nhttpc_retry:should_retry({ok, #{status => 503}}, 1, PostWith503)),
    ?assertEqual(false, nhttpc_retry:should_retry(Closed, 1, PostWith503)),
    ?assertNot(nhttpc_retry:is_retryable(Closed, [auto], post)),
    ?assert(nhttpc_retry:is_retryable(Closed, [auto], get)),
    ?assert(nhttpc_retry:is_retryable(Closed, [auto], undefined)),
    ?assert(nhttpc_retry:is_retryable(Closed, [auto])),
    IntegerPost = nhttpc_retry:normalize_opts(3, post),
    ?assertEqual(post, maps:get(method, IntegerPost)),
    ?assertEqual(false, nhttpc_retry:should_retry(Closed, 1, IntegerPost)).

%%%-----------------------------------------------------------------------------
%%% IDEMPOTENCE TESTS
%%%-----------------------------------------------------------------------------

is_idempotent_method_each(_Config) ->
    [
        ?assert(nhttpc_retry:is_idempotent_method(M))
     || M <- [get, head, put, delete, options, trace, query]
    ],
    [?assertNot(nhttpc_retry:is_idempotent_method(M)) || M <- [post, patch, connect]],
    ok.

is_idempotent_method_undefined(_Config) ->
    %% `undefined` (no method context) is treated as the permissive set; the
    %% method itself is not idempotent.
    ?assertNot(nhttpc_retry:is_idempotent_method(undefined)),
    Opts = nhttpc_retry:normalize_opts(3, undefined),
    Conditions = maps:get(retry_on, Opts),
    ?assert(lists:member(timeout, Conditions)),
    ?assert(lists:member(503, Conditions)),
    ok.

%%%-----------------------------------------------------------------------------
%%% JITTER BOUNDS TESTS
%%%-----------------------------------------------------------------------------

delay_jitter_small_delay(_Config) ->
    %% Base 3 gives Jitter = 3 div 4 = 0, so the inner `Jitter = 0` arm
    %% returns Delay unchanged regardless of repetition.
    Opts = #{
        backoff => constant,
        base_delay => 3,
        max_delay => 100,
        jitter => true
    },
    [?assertEqual(3, nhttpc_retry:compute_delay(1, Opts)) || _ <- lists:seq(1, 16)],
    ok.

delay_jitter_bounds(_Config) ->
    %% With jitter enabled the returned delay must be within +/- 25% of the
    %% deterministic value. 100 ± 25 ⇒ [75, 125].
    Opts = #{
        backoff => constant,
        base_delay => 100,
        max_delay => 5000,
        jitter => true
    },
    Samples = [nhttpc_retry:compute_delay(1, Opts) || _ <- lists:seq(1, 64)],
    lists:foreach(
        fun(D) ->
            ?assert(D >= 75),
            ?assert(D =< 125)
        end,
        Samples
    ),
    ok.

%%%-----------------------------------------------------------------------------
%%% NORMALIZE — extra branches
%%%-----------------------------------------------------------------------------

normalize_max_attempts_default(_Config) ->
    %% A map without `max_attempts` falls back to the default of 3.
    Opts = nhttpc_retry:normalize_opts(#{base_delay => 50}),
    ?assertEqual(3, maps:get(max_attempts, Opts)),
    ?assertEqual(50, maps:get(base_delay, Opts)),
    ok.

normalize_should_retry_callback_passthrough(_Config) ->
    %% Custom should_retry callback is preserved verbatim through the map
    %% normalisation path.
    Fn = fun(_R, _A, _O) -> true end,
    Opts = nhttpc_retry:normalize_opts(#{max_attempts => 2, should_retry => Fn}),
    ?assertEqual(Fn, maps:get(should_retry, Opts)),
    ok.
