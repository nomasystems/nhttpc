-module(nhttpc_registry).

-moduledoc """
Per-pool pid registry for nhttpc.

Thin typed wrapper over an ETS table that holds the pids of a pool's
supervisor, manager, and connection supervisor, plus the tuple of
multiplexed connections that the checkout fast path walks. The table is
created by `nhttpc_pool_sup` in its `init/1`, owned by that supervisor
process, and dies with it. No manual cleanup required.

The table reference itself scopes per-pool, so keys are bare role
atoms (`sup`, `mgr`, `conn_sup`, `multiplexed_conns`). No user-supplied
identifier is ever used as a key, which removes the dynamic-atom
leak path that the previous `list_to_atom/1`-derived registered
names had.

Pid storage lives here, not in `persistent_term`, so connection
churn (`connection_ready` / `connection_down` / `connection_draining`)
no longer triggers a global GC scan.

Lookups are O(1) ETS reads, comparable in cost to `whereis/1`. The
table is configured for read-concurrency because the multiplexed fast path
reads the connection tuple from it on every request.
""".

-export([
    get_multiplexed_conns/1,
    lookup_conn_sup/1,
    lookup_mgr/1,
    lookup_sup/1,

    lookup_target/1,
    new/0,
    put_multiplexed_conns/2,

    register_conn_sup/2,
    register_mgr/2,
    register_sup/2,
    register_target/2
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([tab/0, target/0]).

-type tab() :: ets:tid().

-type target() :: #{
    scheme := nhttp_lib:scheme(),
    host := binary(),
    port := inet:port_number()
}.

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(SUP, sup).
-define(MGR, mgr).
-define(CONN_SUP, conn_sup).
-define(MULTIPLEXED_CONNS, multiplexed_conns).
-define(TARGET, target).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Tuple of `{ConnPid, Slots}` pairs for the multiplexed connections of this
pool, or `{}` when none is registered. `Slots` is the `atomics` array that
holds the live stream count and the concurrent stream cap of that one
connection.
""".
-spec get_multiplexed_conns(tab()) -> tuple().
get_multiplexed_conns(Tab) ->
    case ets:lookup(Tab, ?MULTIPLEXED_CONNS) of
        [{_, Tuple}] -> Tuple;
        [] -> {}
    end.

-doc "Connection supervisor pid for this pool, or `undefined`.".
-spec lookup_conn_sup(tab()) -> pid() | undefined.
lookup_conn_sup(Tab) ->
    lookup_pid(Tab, ?CONN_SUP).

-doc "Pool manager pid for this pool, or `undefined`.".
-spec lookup_mgr(tab()) -> pid() | undefined.
lookup_mgr(Tab) ->
    lookup_pid(Tab, ?MGR).

-doc "Pool supervisor pid for this pool, or `undefined`.".
-spec lookup_sup(tab()) -> pid() | undefined.
lookup_sup(Tab) ->
    lookup_pid(Tab, ?SUP).

-doc """
Connection target (`scheme`, `host`, `port`) for this pool, or
`undefined` if none is registered. Pooled requests carry a path-only
URL, so this is the only place the real authority is available.
""".
-spec lookup_target(tab()) -> target() | undefined.
lookup_target(Tab) ->
    case ets:lookup(Tab, ?TARGET) of
        [{_, Target}] -> Target;
        [] -> undefined
    end.

-doc """
Create a new per-pool registry table.
The calling process becomes the owner; the table dies with it.
""".
-spec new() -> tab().
new() ->
    ets:new(?MODULE, [public, set, {read_concurrency, true}]).

-doc "Replace the tuple of multiplexed connections.".
-spec put_multiplexed_conns(tab(), tuple()) -> ok.
put_multiplexed_conns(Tab, Tuple) when is_tuple(Tuple) ->
    true = ets:insert(Tab, {?MULTIPLEXED_CONNS, Tuple}),
    ok.

-doc "Register the connection supervisor pid for this pool.".
-spec register_conn_sup(tab(), pid()) -> ok.
register_conn_sup(Tab, Pid) when is_pid(Pid) ->
    insert_pid(Tab, ?CONN_SUP, Pid).

-doc "Register the pool manager pid for this pool.".
-spec register_mgr(tab(), pid()) -> ok.
register_mgr(Tab, Pid) when is_pid(Pid) ->
    insert_pid(Tab, ?MGR, Pid).

-doc "Register the pool supervisor pid for this pool.".
-spec register_sup(tab(), pid()) -> ok.
register_sup(Tab, Pid) when is_pid(Pid) ->
    insert_pid(Tab, ?SUP, Pid).

-doc "Register the connection target (`scheme`, `host`, `port`) for this pool.".
-spec register_target(tab(), target()) -> ok.
register_target(Tab, Target) when is_map(Target) ->
    true = ets:insert(Tab, {?TARGET, Target}),
    ok.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec insert_pid(tab(), atom(), pid()) -> ok.
insert_pid(Tab, Key, Pid) ->
    true = ets:insert(Tab, {Key, Pid}),
    ok.

-spec lookup_pid(tab(), atom()) -> pid() | undefined.
lookup_pid(Tab, Key) ->
    case ets:lookup(Tab, Key) of
        [{_, Pid}] -> Pid;
        [] -> undefined
    end.
