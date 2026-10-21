-module(nhttpc_pool_conn_sup).

-moduledoc """
Pool connection supervisor.

A simple_one_for_one supervisor that manages client connection processes
for a single pool.
""".

-behaviour(supervisor).

-export([
    conn_module/1, start_connection/2, start_link/2
]).

%% Supervisor callbacks
-export([init/1]).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Connection module a pool with these options runs.

The supervisor starts one child type for the life of the pool, so this
answer is a property of the pool and not of a connection. The pool
manager reads it once and dispatches every request on it.
""".
-spec conn_module(nhttpc:pool_opts()) -> nhttpc_conn | nhttpc_h3_conn.
conn_module(Opts) ->
    case lists:member(http3, nhttpc_opts:versions(Opts)) of
        true -> nhttpc_h3_conn;
        false -> nhttpc_conn
    end.

-doc "Start a new connection under this supervisor.".
-spec start_connection(pid(), nhttpc_conn:conn_opts()) ->
    {ok, pid()} | {error, nhttpc_error:any_reason()}.
start_connection(Sup, ConnOpts) ->
    supervisor:start_child(Sup, [ConnOpts]).

-doc "Start the connection supervisor.".
-spec start_link(nhttpc_registry:tab(), nhttpc:pool_opts()) ->
    {ok, pid()} | {error, nhttpc_error:any_reason()}.
start_link(Tab, Opts) ->
    supervisor:start_link(?MODULE, {Tab, Opts}).

%%%-----------------------------------------------------------------------------
%% SUPERVISOR CALLBACKS
%%%-----------------------------------------------------------------------------
-spec init({nhttpc_registry:tab(), nhttpc:pool_opts()}) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({Tab, Opts}) ->
    ok = nhttpc_registry:register_conn_sup(Tab, self()),

    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 60
    },

    Module = conn_module(Opts),
    ChildSpec = #{
        id => Module,
        start => {Module, start_link, []},
        type => worker,
        restart => temporary,
        shutdown => 5000
    },

    {ok, {SupFlags, [ChildSpec]}}.
