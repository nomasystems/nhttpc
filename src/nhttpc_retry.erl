-module(nhttpc_retry).

-moduledoc """
Retry logic for nhttpc HTTP client.

Provides configurable automatic retry with backoff for transient failures.

## Configuration

```erlang
%% Simple retry: 3 attempts with method-aware defaults
{ok, Resp} = nhttpc:get(Url, #{retry => 3}).

%% Full configuration
{ok, Resp} = nhttpc:get(Url, #{
    retry => #{
        max_attempts => 3,
        base_delay => 100,
        max_delay => 5000,
        backoff => exponential,
        jitter => true,
        retry_on => [timeout, connection_error, 503, 502, 504]
    }
}).
```

## Default retry conditions are method-aware

When the user passes `retry => N` (or a map without `retry_on`), the default
set of conditions depends on the request method:

- **Idempotent methods** (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE`):
  retry on `timeout`, `connection_error`, `connection_refused`,
  `connection_reset`, and 502/503/504. This matches the historical default.
- **Non-idempotent methods** (`POST`, `PATCH`, anything else): retry only on
  `connection_refused`, the single condition where we can be certain the
  request never reached the server. Retrying a `POST` after a timeout or a
  mid-flight connection error would silently introduce at-least-once
  semantics, so the library refuses to do it by default.

If you need at-least-once retry semantics for non-idempotent methods,
opt in explicitly with `retry_on => [...]` or with a `should_retry` callback.

## The `auto` condition

`retry_on => [auto]` retries whatever `nhttp_error:is_retryable/1` calls
retryable, subject to method idempotency. For an idempotent method that
is the protocol layer's full classification: a closed connection, a
GOAWAY, a refused stream, a checkout timeout. For a non-idempotent method
`auto` shrinks to `connection_refused`, the one condition that proves the
request never reached the server, because a mid-flight close or a GOAWAY
cannot say whether the server processed the request. Status codes and
other conditions listed next to `auto` stay explicit and are honored for
every method.

A caller that wants at-least-once delivery on a `POST` states it with an
explicit condition list or a `should_retry` callback. `auto` never
implies it.

## Backoff Strategies

- `exponential` - delay = min(base * 2^(attempt-1), max)
- `linear` - delay = min(base * attempt, max)
- `constant` - delay = base
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    compute_delay/2,
    is_idempotent_method/1,
    is_retryable/2,
    is_retryable/3,
    normalize_opts/1,
    normalize_opts/2,
    should_retry/3
]).

-export_type([backoff_strategy/0, retry_callback/0, retry_condition/0, retry_opts/0]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-type backoff_strategy() :: exponential | linear | constant.

-type retry_condition() ::
    timeout
    | connection_error
    | connection_refused
    | connection_reset
    | auto
    | nhttp_lib:status().

-type retry_callback() ::
    fun((Result :: term(), Attempt :: pos_integer(), Opts :: retry_opts()) -> boolean()).

-type retry_opts() :: #{
    max_attempts := pos_integer(),
    base_delay := non_neg_integer(),
    max_delay := non_neg_integer(),
    backoff := backoff_strategy(),
    jitter := boolean(),
    retry_on := [retry_condition()],
    should_retry => retry_callback(),
    method => nhttp_lib:method() | undefined
}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "Compute delay for attempt N (1-indexed).".
-spec compute_delay(Attempt :: pos_integer(), Opts :: retry_opts()) -> non_neg_integer().
compute_delay(Attempt, #{backoff := exponential, base_delay := Base, max_delay := Max} = Opts) ->
    Delay = min(Base * (1 bsl (Attempt - 1)), Max),
    maybe_add_jitter(Delay, maps:get(jitter, Opts, true));
compute_delay(Attempt, #{backoff := linear, base_delay := Base, max_delay := Max} = Opts) ->
    Delay = min(Base * Attempt, Max),
    maybe_add_jitter(Delay, maps:get(jitter, Opts, true));
compute_delay(_Attempt, #{backoff := constant, base_delay := Base} = Opts) ->
    maybe_add_jitter(Base, maps:get(jitter, Opts, true)).

-doc """
Whether a result is retryable under a condition list, with no method
context. `auto` gets the permissive reading, as for an idempotent method.
""".
-spec is_retryable(Result :: term(), Conditions :: [retry_condition()]) -> boolean().
is_retryable(Result, Conditions) ->
    is_retryable(Result, Conditions, undefined).

-doc """
Whether a result is retryable under a condition list for a request of
the given method. A list that holds `auto` delegates errors to
`nhttp_error:is_retryable/1` for an idempotent method, and shrinks to
`connection_refused` for every other method. Without `auto`, the list is
read literally for every method.
""".
-spec is_retryable(
    Result :: term(),
    Conditions :: [retry_condition()],
    Method :: nhttp_lib:method() | undefined
) -> boolean().
is_retryable(Result, Conditions, Method) ->
    case lists:member(auto, Conditions) of
        true ->
            is_retryable_auto(Result, Conditions, Method);
        false ->
            is_retryable_explicit(Result, Conditions)
    end.

-spec is_retryable_auto(
    Result :: term(),
    Conditions :: [retry_condition()],
    Method :: nhttp_lib:method() | undefined
) -> boolean().
is_retryable_auto({ok, #{status := Status}}, Conditions, _Method) ->
    lists:member(Status, Conditions);
is_retryable_auto({error, _} = Error, Conditions, Method) ->
    case is_idempotent_method(Method) orelse Method =:= undefined of
        true -> nhttp_error:is_retryable(Error);
        false -> is_retryable_explicit(Error, [connection_refused | Conditions])
    end;
is_retryable_auto(_, _, _) ->
    false.

-doc "Check retryability using explicit conditions list.".
-spec is_retryable_explicit(Result :: term(), Conditions :: [retry_condition()]) -> boolean().
is_retryable_explicit({error, {request_timeout, _}}, Conditions) ->
    lists:member(timeout, Conditions);
is_retryable_explicit({error, timeout}, Conditions) ->
    lists:member(timeout, Conditions);
is_retryable_explicit({error, {connect_error, econnrefused}}, Conditions) ->
    lists:member(connection_refused, Conditions);
is_retryable_explicit({error, econnrefused}, Conditions) ->
    lists:member(connection_refused, Conditions);
is_retryable_explicit({error, {connect_error, econnreset}}, Conditions) ->
    lists:member(connection_reset, Conditions);
is_retryable_explicit({error, {recv_error, econnreset}}, Conditions) ->
    lists:member(connection_reset, Conditions);
is_retryable_explicit({error, econnreset}, Conditions) ->
    lists:member(connection_reset, Conditions);
is_retryable_explicit({error, {connect_error, _}}, Conditions) ->
    lists:member(connection_error, Conditions);
is_retryable_explicit({error, {recv_error, _}}, Conditions) ->
    lists:member(connection_error, Conditions);
is_retryable_explicit({error, {send_error, _}}, Conditions) ->
    lists:member(connection_error, Conditions);
is_retryable_explicit({ok, #{status := Status}}, Conditions) ->
    lists:member(Status, Conditions);
is_retryable_explicit(_, _) ->
    false.

-doc """
Normalize retry options without method context. Defaults assume the
permissive idempotent condition set. Internal callers should prefer
`normalize_opts/2` to get method-aware defaults.
""".
-spec normalize_opts(false | pos_integer() | retry_opts()) -> false | retry_opts().
normalize_opts(Spec) ->
    normalize_opts(Spec, undefined).

-doc """
Normalize retry options with method context. The default `retry_on`
list is computed from the method (see moduledoc).
""".
-spec normalize_opts(
    false | pos_integer() | retry_opts(),
    nhttp_lib:method() | undefined
) -> false | retry_opts().
normalize_opts(false, _Method) ->
    false;
normalize_opts(MaxAttempts, Method) when is_integer(MaxAttempts), MaxAttempts > 0 ->
    default_opts(MaxAttempts, default_retry_conditions(Method), Method);
normalize_opts(Opts, Method) when is_map(Opts) ->
    MaxAttempts = maps:get(max_attempts, Opts, 3),
    #{
        max_attempts => MaxAttempts,
        base_delay => maps:get(base_delay, Opts, 100),
        max_delay => maps:get(max_delay, Opts, 5000),
        backoff => maps:get(backoff, Opts, exponential),
        jitter => maps:get(jitter, Opts, true),
        retry_on => maps:get(retry_on, Opts, default_retry_conditions(Method)),
        should_retry => maps:get(should_retry, Opts, undefined),
        method => Method
    };
normalize_opts(_, _Method) ->
    false.

-doc """
Whether to retry after `Result` on attempt `Attempt`, and the delay to
wait first. The method the options were normalized with gates `auto`.
""".
-spec should_retry(Result :: term(), Attempt :: pos_integer(), Opts :: retry_opts()) ->
    {true, non_neg_integer()} | false.
should_retry(_Result, _Attempt, false) ->
    false;
should_retry(_Result, Attempt, #{max_attempts := MaxAttempts}) when Attempt >= MaxAttempts ->
    false;
should_retry(Result, Attempt, #{should_retry := CustomFn} = Opts) when
    is_function(CustomFn, 3)
->
    case CustomFn(Result, Attempt, Opts) of
        true ->
            Delay = compute_delay(Attempt, Opts),
            {true, Delay};
        false ->
            false
    end;
should_retry(Result, Attempt, #{retry_on := Conditions} = Opts) ->
    case is_retryable(Result, Conditions, maps:get(method, Opts, undefined)) of
        true ->
            Delay = compute_delay(Attempt, Opts),
            {true, Delay};
        false ->
            false
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec default_opts(pos_integer(), [retry_condition()], nhttp_lib:method() | undefined) ->
    retry_opts().
default_opts(MaxAttempts, Conditions, Method) ->
    #{
        max_attempts => MaxAttempts,
        base_delay => 100,
        max_delay => 5000,
        backoff => exponential,
        jitter => true,
        retry_on => Conditions,
        method => Method
    }.

-doc """
Method-aware default conditions that trigger a retry.
`undefined` (no method context) yields the permissive set.
""".
-spec default_retry_conditions(nhttp_lib:method() | undefined) -> [retry_condition()].
default_retry_conditions(undefined) ->
    permissive_conditions();
default_retry_conditions(Method) ->
    case is_idempotent_method(Method) of
        true -> permissive_conditions();
        false -> [connection_refused]
    end.

-doc """
Whether the HTTP method is safe to retry without changing observable
server state more than once. Idempotency follows RFC 9110 §9.2.2.
""".
-spec is_idempotent_method(nhttp_lib:method() | undefined) -> boolean().
is_idempotent_method(get) -> true;
is_idempotent_method(head) -> true;
is_idempotent_method(put) -> true;
is_idempotent_method(delete) -> true;
is_idempotent_method(options) -> true;
is_idempotent_method(trace) -> true;
is_idempotent_method(_) -> false.

-doc "Add jitter to delay (+/- 25%).".
-spec maybe_add_jitter(non_neg_integer(), boolean()) -> non_neg_integer().
maybe_add_jitter(0, _) ->
    0;
maybe_add_jitter(Delay, false) ->
    Delay;
maybe_add_jitter(Delay, true) ->
    Jitter = Delay div 4,
    case Jitter of
        0 -> Delay;
        _ -> Delay - Jitter + rand:uniform(Jitter * 2)
    end.

-spec permissive_conditions() -> [retry_condition()].
permissive_conditions() ->
    [
        timeout,
        connection_error,
        connection_refused,
        connection_reset,
        502,
        503,
        504
    ].
