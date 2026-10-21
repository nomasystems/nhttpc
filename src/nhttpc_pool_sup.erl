-module(nhttpc_pool_sup).

-moduledoc """
Client connection pool supervisor.

Supervises the pool manager and connection supervisor for a single pool.
Each pool manages connections to a specific host:port target.

## Supervision Tree

```
nhttpc_pool_sup (supervisor)
├── nhttpc_pool (gen_server) - manages checkout/checkin
└── nhttpc_pool_conn_sup (simple_one_for_one) - connection processes
```
""".

-behaviour(supervisor).

-export([
    child_spec/2,
    start_link/2,

    stop/1,
    stop/2
]).

%% Supervisor callbacks
-export([init/1]).

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(DEFAULT_DRAIN_TIMEOUT, 10_000).

%% Default delay before falling back to `exit(Pid, kill)` during shutdown.
-define(DEFAULT_FORCE_KILL_DELAY, 5_000).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "Return a child spec for embedding in supervision trees.".
-spec child_spec(term(), nhttpc:pool_opts()) -> supervisor:child_spec().
child_spec(Ref, Opts) ->
    #{
        id => {nhttpc_pool_sup, Ref},
        start => {?MODULE, start_link, [Ref, Opts]},
        type => supervisor,
        shutdown => 5000
    }.

-spec force_kill(pid()) -> ok.
force_kill(Pid) ->
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 1000 ->
        demonitor(Ref, [flush]),
        ok
    end.

-spec graceful_stop(pid() | term(), timeout(), non_neg_integer()) -> ok.
graceful_stop(PidOrRef, DrainTimeout, KillDelay) ->
    case nhttpc_pool:resolve(PidOrRef) of
        {error, pool_not_found} ->
            ok;
        {ok, MgrPid, SupPid} ->
            ok = nhttpc_pool:drain(MgrPid, DrainTimeout),
            stop_pid_with_delay(SupPid, KillDelay)
    end.

-spec immediate_stop(pid() | term(), non_neg_integer()) -> ok.
immediate_stop(PidOrRef, KillDelay) ->
    case nhttpc_pool:resolve(PidOrRef) of
        {ok, _MgrPid, SupPid} -> stop_pid_with_delay(SupPid, KillDelay);
        {error, pool_not_found} -> ok
    end.

-doc "Start a pool supervisor linked to caller.".
-spec start_link(term(), nhttpc:pool_opts()) -> {ok, pid()} | {error, nhttpc_error:any_reason()}.
start_link(Ref, Opts) ->
    case nhttpc_opts:normalize_pool(Opts) of
        {ok, Normalized} ->
            supervisor:start_link(?MODULE, {Ref, Normalized});
        {error, _} = Error ->
            Error
    end.

-doc """
Stop a pool immediately. Terminates the pool supervisor (and therefore the
pool manager and connection supervisor) cleanly. Accepts either the
supervisor pid returned by `start_link/2`, the pool ref passed to
`start_link/2`, or the manager pid.
""".
-spec stop(pid() | term()) -> ok.
stop(PidOrRef) ->
    case nhttpc_pool:resolve(PidOrRef) of
        {ok, _MgrPid, SupPid} -> stop_pid(SupPid);
        {error, pool_not_found} -> ok
    end.

-doc """
Stop a pool with options.
  * `drain => false` (default) - stop immediately. Equivalent to `stop/1`.
    `drain_timeout` is rejected in this mode to surface configuration errors.
  * `drain => true` - block new checkouts, wait for in-flight requests to
    finish (up to `drain_timeout`, default 10s), then stop the supervisor.
  * `drain_timeout => timeout()` - override the drain deadline. Only
    meaningful when `drain => true`.
  * `force_kill_delay => non_neg_integer()` - milliseconds to wait for a
    polite supervisor shutdown before falling back to `exit(Pid, kill)`.
    Default: 5000.
""".
-spec stop(pid() | term(), nhttpc:stop_opts()) ->
    ok | {error, {invalid_stop_opts, atom()}}.
stop(PidOrRef, Opts) when is_map(Opts) ->
    Drain = maps:get(drain, Opts, false),
    HasDrainTimeout = maps:is_key(drain_timeout, Opts),
    KillDelay = maps:get(force_kill_delay, Opts, ?DEFAULT_FORCE_KILL_DELAY),
    case {Drain, HasDrainTimeout} of
        {false, true} ->
            {error, {invalid_stop_opts, drain_timeout_without_drain}};
        {false, false} ->
            immediate_stop(PidOrRef, KillDelay);
        {true, _} ->
            Timeout = maps:get(drain_timeout, Opts, ?DEFAULT_DRAIN_TIMEOUT),
            graceful_stop(PidOrRef, Timeout, KillDelay)
    end.

-spec stop_pid(pid()) -> ok.
stop_pid(Pid) ->
    stop_pid_with_delay(Pid, ?DEFAULT_FORCE_KILL_DELAY).

-spec stop_pid_with_delay(pid(), non_neg_integer()) -> ok.
stop_pid_with_delay(Pid, KillDelay) ->
    case is_process_alive(Pid) of
        false ->
            ok;
        true ->
            try proc_lib:stop(Pid, shutdown, KillDelay) of
                _ -> ok
            catch
                exit:noproc -> ok;
                exit:{noproc, _} -> ok;
                exit:timeout -> force_kill(Pid);
                exit:{timeout, _} -> force_kill(Pid)
            end
    end.

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init({term(), nhttpc:pool_opts()}) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({Ref, Opts}) ->
    Tab = nhttpc_registry:new(),
    ok = nhttpc_registry:register_sup(Tab, self()),

    SupFlags = #{
        strategy => rest_for_one,
        intensity => 5,
        period => 60
    },

    ConnSupSpec = #{
        id => nhttpc_pool_conn_sup,
        start => {nhttpc_pool_conn_sup, start_link, [Tab, Opts]},
        type => supervisor,
        shutdown => 5000
    },

    MgrSpec = #{
        id => nhttpc_pool,
        start => {nhttpc_pool, start_link, [Ref, Tab, Opts]},
        type => worker,
        shutdown => 5000
    },

    {ok, {SupFlags, [ConnSupSpec, MgrSpec]}}.
