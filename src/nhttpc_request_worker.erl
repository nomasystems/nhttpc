-module(nhttpc_request_worker).

-moduledoc """
Per-request worker process for the nhttpc 1.0 unified async protocol.

Each call to `nhttpc:send/2` spawns one worker. The worker drives
`nhttpc_request:do_request/3` in an isolated sub-process so a cancel
signal can interrupt it without affecting the owner. When the
sub-process completes the worker forwards the outcome to the owner as
`{nhttpc, Handle, _}` events; when the owner cancels via the
`'$nhttpc_cancel'` protocol the worker tears down the sub-process,
acks, and exits before returning from `nhttpc:cancel/1` so the
no-leak contract holds.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    start/4
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-record(state, {
    owner :: pid(),
    handle :: nhttpc:handle(),
    sub_pid :: pid(),
    sub_mref :: reference()
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec start(pid(), nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts()) ->
    {ok, nhttpc:handle()}.
start(Owner, Req, UrlInfo, Opts) ->
    Ref = make_ref(),
    Self = self(),
    Pid = erlang:spawn(fun() -> bootstrap(Self, Ref, Owner, Req, UrlInfo, Opts) end),
    receive
        {ready, Pid} -> ok
    after 5000 ->
        exit(Pid, kill),
        error(worker_bootstrap_timeout)
    end,
    {ok, nhttpc_handle:new(Pid, Ref, request)}.

%%%-----------------------------------------------------------------------------
%% INTERNAL
%%%-----------------------------------------------------------------------------
-spec bootstrap(
    pid(),
    reference(),
    pid(),
    nhttp_lib:request(),
    nhttpc_url:http_url_info(),
    nhttpc:opts()
) -> no_return().
bootstrap(Starter, Ref, Owner, Req, UrlInfo, Opts) ->
    process_flag(trap_exit, true),
    Handle = nhttpc_handle:new(self(), Ref, request),
    Starter ! {ready, self()},
    Self = self(),
    {SubPid, SubMRef} = erlang:spawn_opt(
        fun() ->
            Result = nhttpc_request:do_request(Req, UrlInfo, Opts),
            Self ! {'$result', Result}
        end,
        [link, monitor]
    ),
    loop(#state{
        owner = Owner,
        handle = Handle,
        sub_pid = SubPid,
        sub_mref = SubMRef
    }).

-spec deliver(
    pid(),
    nhttpc:handle(),
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}
) -> ok.
deliver(Owner, Handle, {ok, Resp}) ->
    Owner ! {nhttpc, Handle, {response, Resp}},
    Owner ! {nhttpc, Handle, {done, normal}},
    ok;
deliver(Owner, Handle, {error, Reason}) ->
    Owner ! {nhttpc, Handle, {error, Reason}},
    ok.

-spec do_cancel(#state{}, pid()) -> no_return().
do_cancel(#state{sub_pid = SubPid, sub_mref = SubMRef, handle = Handle}, From) ->
    exit(SubPid, cancelled),
    drain_sub(SubPid, SubMRef),
    From ! {'$nhttpc_cancel_ack', nhttpc_handle:ref(Handle)},
    exit(normal).

-spec drain_exit(pid()) -> ok.
drain_exit(Pid) ->
    receive
        {'EXIT', Pid, _} -> ok
    after 0 ->
        ok
    end.

-spec drain_sub(pid(), reference()) -> ok.
drain_sub(SubPid, SubMRef) ->
    receive
        {'DOWN', SubMRef, process, _, _} -> drain_exit(SubPid)
    after 1000 ->
        ok
    end.

-spec loop(#state{}) -> no_return().
loop(
    #state{sub_mref = SubMRef, sub_pid = SubPid, owner = Owner, handle = Handle} = State
) ->
    receive
        {'$result', Result} ->
            erlang:demonitor(SubMRef, [flush]),
            drain_exit(SubPid),
            deliver(Owner, Handle, Result),
            exit(normal);
        {'DOWN', SubMRef, process, _, normal} ->
            drain_exit(SubPid),
            deliver(Owner, Handle, {error, no_result}),
            exit(normal);
        {'DOWN', SubMRef, process, _, Reason} ->
            drain_exit(SubPid),
            deliver(Owner, Handle, {error, {worker_died, Reason}}),
            exit(normal);
        {'EXIT', SubPid, _Reason} ->
            loop(State);
        {'EXIT', _From, cancelled} ->
            exit(SubPid, cancelled),
            drain_sub(SubPid, SubMRef),
            exit(normal);
        {'$nhttpc_cancel', RcvRef, From} ->
            HandleRef = nhttpc_handle:ref(Handle),
            case RcvRef of
                HandleRef -> do_cancel(State, From);
                _ -> loop(State)
            end
    end.
