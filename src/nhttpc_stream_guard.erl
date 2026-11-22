-module(nhttpc_stream_guard).

-moduledoc """
Tells the owners of the response streams of one connection that the
connection is gone.

`nhttpc_conn` sends stream events to the owner itself, and the owner
does not monitor the connection. The guard monitors it and sends
`{error, {connect_error, closed}}` for each stream that is still open
when the connection exits. A connection that sent every last event
calls `release/1` first.

The guard has no supervisor. The guard and the connection monitor each
other, and the connection starts a new guard when one exits.
""".

-behaviour(gen_server).

%%%-----------------------------------------------------------------------------
%% EXPORTS - API
%%%-----------------------------------------------------------------------------
-export([
    closed/2,
    opened/3,
    release/1,
    start/1
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - GEN_SERVER
%%%-----------------------------------------------------------------------------
-export([
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1
]).

%%%-----------------------------------------------------------------------------
%% RECORDS
%%%-----------------------------------------------------------------------------
-record(state, {
    conn_mref :: reference(),
    streams = #{} :: #{nhttpc:handle() => pid()}
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "The connection sent the last event of this stream.".
-spec closed(pid(), nhttpc:handle()) -> ok.
closed(Guard, Handle) ->
    gen_server:cast(Guard, {closed, Handle}).

-doc "A response stream opened, with `Owner` as the process that gets its events.".
-spec opened(pid(), nhttpc:handle(), pid()) -> ok.
opened(Guard, Handle, Owner) ->
    gen_server:cast(Guard, {opened, Handle, Owner}).

-doc "The connection sent the last event of each open stream, and exits.".
-spec release(pid()) -> ok.
release(Guard) ->
    gen_server:cast(Guard, release).

-doc "Start the guard of the connection `ConnPid`.".
-spec start(pid()) -> {ok, pid()}.
start(ConnPid) ->
    {ok, _Guard} = gen_server:start(?MODULE, ConnPid, []).

%%%-----------------------------------------------------------------------------
%% GEN_SERVER
%%%-----------------------------------------------------------------------------
-doc false.
-spec handle_call(term(), gen_server:from(), #state{}) -> {reply, {error, unknown_call}, #state{}}.
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

-doc false.
-spec handle_cast({opened, nhttpc:handle(), pid()} | {closed, nhttpc:handle()} | release, #state{}) ->
    {noreply, #state{}} | {stop, normal, #state{}}.
handle_cast({opened, Handle, Owner}, #state{streams = Streams} = State) ->
    {noreply, State#state{streams = Streams#{Handle => Owner}}};
handle_cast({closed, Handle}, #state{streams = Streams} = State) ->
    {noreply, State#state{streams = maps:remove(Handle, Streams)}};
handle_cast(release, State) ->
    {stop, normal, State}.

-doc false.
-spec handle_info({'DOWN', reference(), process, pid(), term()}, #state{}) ->
    {stop, normal, #state{}}.
handle_info({'DOWN', MRef, process, _ConnPid, _Reason}, #state{conn_mref = MRef} = State) ->
    {error, Reason} = nhttpc_error:connection_closed(),
    ok = maps:foreach(
        fun(Handle, Owner) -> Owner ! {nhttpc, Handle, {error, Reason}} end, State#state.streams
    ),
    {stop, normal, State}.

-doc false.
-spec init(pid()) -> {ok, #state{}}.
init(ConnPid) ->
    {ok, #state{conn_mref = monitor(process, ConnPid)}}.
