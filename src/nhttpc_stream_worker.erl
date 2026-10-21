-module(nhttpc_stream_worker).

-moduledoc """
Per-stream worker process for the nhttpc 1.0 unified async protocol.

Each call to `nhttpc:stream/2` spawns one worker. The worker drives the
request in an isolated sub-process and forwards per-chunk events to the
owner as `{nhttpc, Handle, _}` messages, matching the proposal vocabulary:

```
{nhttpc, Handle, {headers, Status, Headers}}
{nhttpc, Handle, {data, Chunk, more}}
{nhttpc, Handle, {trailers, Headers}}
{nhttpc, Handle, {pause, BytesBuffered}}
{nhttpc, Handle, {resume, BytesBuffered}}
{nhttpc, Handle, {done, normal}}
{nhttpc, Handle, {error, Reason}}
```

For HTTP/1.1 and HTTP/2 the underlying transport delivers chunks via the
existing `nhttpc_stream_recv` pipeline; HTTP/3 falls through the buffered
`nhttpc_request:do_request/3` path and the worker synthesizes the event
sequence from the resulting response.

Backpressure: data events are routed through the worker, which tracks
per-stream consumer credit (initialized from `consume_initial`). When a
chunk does not fit credit, the worker buffers it and emits `pause`;
`nhttpc:consume/2` restores credit and may flush the buffer + emit
`resume`. The sub-process delivering chunks runs synchronously, so when
credit is exhausted it blocks waiting for `nhttpc:consume/2` — that
back-propagates as TCP backpressure on H1 and as HTTP/2 stream-window
backpressure on H2.

Cancellation follows the same `'$nhttpc_cancel'` protocol as
`nhttpc_request_worker`: the worker kills the linked sub-process,
acknowledges, then exits before `nhttpc:cancel/1` returns so the
cancel-no-leak contract holds.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    deliver_data/4,
    start/4
]).

-type fin_flag() :: more | fin | nofin.

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-define(DEFAULT_CONSUME_INITIAL, 1048576).
-define(DEFAULT_CONSUME_LOW_WATER, 262144).

-record(state, {
    owner :: pid(),
    handle :: nhttpc:handle(),
    sub_pid :: pid(),
    sub_mref :: reference(),
    credit :: non_neg_integer() | infinity,
    low_water :: pos_integer(),
    paused = false :: boolean(),
    buffered_bytes = 0 :: non_neg_integer(),
    pending = undefined ::
        undefined
        | {SubPid :: pid(), Ref :: reference(), Bin :: binary(), Fin :: fin_flag()}
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Synchronously route a data chunk through the worker for credit accounting.

Called from the sub-process that drives the underlying request. Blocks
until the worker has either delivered the chunk to the owner or has
buffered it pending more consumer credit. Crashes if the worker dies
(the link teardown propagates `cancelled` to the sub-process).
""".
-spec deliver_data(pid(), binary(), fin_flag(), reference()) -> ok.
deliver_data(WorkerPid, Bin, Fin, Ref) ->
    WorkerPid ! {'$nhttpc_data', self(), Ref, Bin, Fin},
    receive
        {'$nhttpc_data_ack', Ref} -> ok
    end.

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
        error(stream_worker_bootstrap_timeout)
    end,
    {ok, nhttpc_handle:new(Pid, Ref, stream)}.

%%%-----------------------------------------------------------------------------
%% INTERNAL
%%%-----------------------------------------------------------------------------
-spec apply_limits(map(), nhttpc:opts()) -> map().
apply_limits(StreamOpts, Opts) ->
    StreamOpts1 =
        case maps:get(on_download_progress, Opts, undefined) of
            undefined -> StreamOpts;
            Progress when is_function(Progress, 2) -> StreamOpts#{on_progress => Progress}
        end,
    StreamOpts2 =
        case maps:get(max_response_size, Opts, undefined) of
            undefined -> StreamOpts1;
            Max -> StreamOpts1#{max_response_size => Max}
        end,
    apply_timeouts(StreamOpts2, Opts).

-spec apply_timeouts(map(), nhttpc:opts()) -> map().
apply_timeouts(StreamOpts, Opts) ->
    Timeouts = maps:get(timeouts, Opts, #{}),
    case maps:get(request, Timeouts, undefined) of
        undefined -> StreamOpts;
        T -> StreamOpts#{timeout => T}
    end.

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
    Handle = nhttpc_handle:new(self(), Ref, stream),
    Starter ! {ready, self()},
    Self = self(),
    {Credit, LowWater} = credit_opts(Opts),
    {SubPid, SubMRef} = erlang:spawn_opt(
        fun() ->
            Result = do_stream(Self, Owner, Handle, Req, UrlInfo, Opts),
            Self ! {'$result', Result}
        end,
        [link, monitor]
    ),
    loop(#state{
        owner = Owner,
        handle = Handle,
        sub_pid = SubPid,
        sub_mref = SubMRef,
        credit = Credit,
        low_water = LowWater
    }).

-spec build_stream_opts(pid(), pid(), nhttpc:handle(), nhttpc:opts()) -> map().
build_stream_opts(WorkerPid, Owner, Handle, Opts) ->
    Base = #{
        initial_acc => undefined,
        on_headers => fun(Headers, Status) ->
            Owner ! {nhttpc, Handle, {headers, Status, Headers}},
            continue
        end,
        on_data => fun(Chunk, Acc) when is_binary(Chunk) ->
            ok = deliver_data(WorkerPid, Chunk, more, make_ref()),
            {continue, Acc}
        end,
        on_trailers => fun(Trailers) ->
            Owner ! {nhttpc, Handle, {trailers, Trailers}},
            ok
        end,
        on_complete => fun(_Acc) -> {ok, ok} end
    },
    apply_limits(carry_transport_opts(Base, Opts), Opts).

-spec carry_transport_opts(map(), nhttpc:opts()) -> map().
carry_transport_opts(StreamOpts, Opts) ->
    case maps:get(tls, Opts, undefined) of
        undefined -> StreamOpts;
        V -> StreamOpts#{tls => V}
    end.

-spec credit_opts(nhttpc:opts()) -> {non_neg_integer() | infinity, pos_integer()}.
credit_opts(Opts) ->
    Credit =
        case maps:get(consume_initial, Opts, ?DEFAULT_CONSUME_INITIAL) of
            infinity -> infinity;
            N when is_integer(N), N >= 0 -> N
        end,
    LowWater = maps:get(consume_low_water, Opts, ?DEFAULT_CONSUME_LOW_WATER),
    {Credit, LowWater}.

-spec deliver_terminal(pid(), nhttpc:handle(), ok | {error, nhttpc_error:any_reason()}) -> ok.
deliver_terminal(Owner, Handle, ok) ->
    Owner ! {nhttpc, Handle, {done, normal}},
    ok;
deliver_terminal(Owner, Handle, {error, Reason}) ->
    Owner ! {nhttpc, Handle, {error, Reason}},
    ok.

-spec do_cancel(#state{}, pid()) -> no_return().
do_cancel(#state{sub_pid = SubPid, sub_mref = SubMRef, handle = Handle}, From) ->
    exit(SubPid, cancelled),
    drain_sub(SubPid, SubMRef),
    From ! {'$nhttpc_cancel_ack', nhttpc_handle:ref(Handle)},
    exit(normal).

-spec do_stream(
    pid(), pid(), nhttpc:handle(), nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts()
) ->
    ok | {error, nhttpc_error:any_reason()}.
do_stream(WorkerPid, Owner, Handle, Req, UrlInfo, Opts) ->
    case nhttpc_opts:request_transport(UrlInfo, Opts) of
        quic ->
            stream_h3(WorkerPid, Owner, Handle, Req, UrlInfo, Opts);
        _ ->
            stream_h1_h2(WorkerPid, Owner, Handle, Req, UrlInfo, Opts)
    end.

-spec drain_exit(pid()) -> ok.
drain_exit(Pid) ->
    receive
        {'EXIT', Pid, _} -> ok
    after 0 ->
        ok
    end.

-spec drain_pending(#state{}) -> #state{}.
drain_pending(#state{pending = undefined} = State) ->
    maybe_emit_resume(State);
drain_pending(
    #state{
        pending = {From, Ref, Bin, Fin},
        credit = Credit,
        owner = Owner,
        handle = Handle
    } = State
) ->
    Size = byte_size(Bin),
    case Credit >= Size of
        true ->
            Owner ! {nhttpc, Handle, {data, Bin, Fin}},
            From ! {'$nhttpc_data_ack', Ref},
            maybe_emit_resume(State#state{
                pending = undefined,
                buffered_bytes = 0,
                credit = Credit - Size
            });
        false ->
            State
    end.

-spec drain_sub(pid(), reference()) -> ok.
drain_sub(SubPid, SubMRef) ->
    receive
        {'DOWN', SubMRef, process, _, _} -> drain_exit(SubPid)
    after 1000 ->
        ok
    end.

-spec emit_response_as_events(pid(), pid(), nhttpc:handle(), nhttp_lib:response(), nhttpc:opts()) ->
    ok.
emit_response_as_events(WorkerPid, Owner, Handle, Resp, Opts) ->
    #{status := Status, headers := Headers} = Resp,
    Owner ! {nhttpc, Handle, {headers, Status, Headers}},
    Body = maps:get(body, Resp, <<>>),
    Trailers = maps:get(trailers, Resp, undefined),
    case iolist_size(Body) of
        0 ->
            ok;
        _ ->
            Bin = iolist_to_binary(Body),
            Fin =
                case Trailers of
                    undefined -> fin;
                    _ -> more
                end,
            ok = deliver_data(WorkerPid, Bin, Fin, make_ref()),
            maybe_emit_progress(Opts, byte_size(Bin), byte_size(Bin))
    end,
    case Trailers of
        undefined ->
            ok;
        Ts ->
            Owner ! {nhttpc, Handle, {trailers, Ts}},
            ok
    end.

-spec handle_consume(#state{}, pos_integer()) -> #state{}.
handle_consume(#state{credit = infinity} = State, _N) ->
    State;
handle_consume(#state{credit = Credit} = State, N) when is_integer(Credit) ->
    drain_pending(State#state{credit = Credit + N}).

-spec handle_data(#state{}, pid(), reference(), binary(), fin_flag()) -> #state{}.
handle_data(#state{credit = infinity, owner = Owner, handle = Handle} = State, From, Ref, Bin, Fin) ->
    Owner ! {nhttpc, Handle, {data, Bin, Fin}},
    From ! {'$nhttpc_data_ack', Ref},
    State;
handle_data(
    #state{credit = Credit, owner = Owner, handle = Handle} = State, From, Ref, Bin, Fin
) when
    is_integer(Credit), byte_size(Bin) =< Credit
->
    Size = byte_size(Bin),
    Owner ! {nhttpc, Handle, {data, Bin, Fin}},
    From ! {'$nhttpc_data_ack', Ref},
    State#state{credit = Credit - Size};
handle_data(
    #state{owner = Owner, handle = Handle, paused = Paused} = State, From, Ref, Bin, Fin
) ->
    Size = byte_size(Bin),
    State1 = State#state{
        pending = {From, Ref, Bin, Fin},
        buffered_bytes = Size
    },
    case Paused of
        true ->
            State1;
        false ->
            Owner ! {nhttpc, Handle, {pause, Size}},
            State1#state{paused = true}
    end.

-spec loop(#state{}) -> no_return().
loop(
    #state{
        sub_mref = SubMRef,
        sub_pid = SubPid,
        owner = Owner,
        handle = Handle
    } = State
) ->
    receive
        {'$nhttpc_data', From, Ref, Bin, Fin} when From =:= SubPid ->
            loop(handle_data(State, From, Ref, Bin, Fin));
        {'$nhttpc_consume', RcvRef, N} ->
            HandleRef = nhttpc_handle:ref(Handle),
            case RcvRef of
                HandleRef -> loop(handle_consume(State, N));
                _ -> loop(State)
            end;
        {'$result', Result} ->
            erlang:demonitor(SubMRef, [flush]),
            drain_exit(SubPid),
            deliver_terminal(Owner, Handle, Result),
            exit(normal);
        {'DOWN', SubMRef, process, _, normal} ->
            drain_exit(SubPid),
            deliver_terminal(Owner, Handle, {error, no_result}),
            exit(normal);
        {'DOWN', SubMRef, process, _, Reason} ->
            drain_exit(SubPid),
            deliver_terminal(Owner, Handle, {error, {worker_died, Reason}}),
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

-spec maybe_emit_progress(nhttpc:opts(), non_neg_integer(), non_neg_integer()) -> ok.
maybe_emit_progress(Opts, Bytes, Total) ->
    case maps:get(on_download_progress, Opts, undefined) of
        undefined ->
            ok;
        Fun when is_function(Fun, 2) ->
            _ = Fun(Bytes, Total),
            ok
    end.

-spec maybe_emit_resume(#state{}) -> #state{}.
maybe_emit_resume(
    #state{
        paused = true,
        buffered_bytes = Buffered,
        low_water = LowWater,
        owner = Owner,
        handle = Handle
    } =
        State
) when Buffered =< LowWater ->
    Owner ! {nhttpc, Handle, {resume, Buffered}},
    State#state{paused = false};
maybe_emit_resume(State) ->
    State.

-spec stream_h1_h2(
    pid(), pid(), nhttpc:handle(), nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts()
) ->
    ok | {error, nhttpc_error:any_reason()}.
stream_h1_h2(WorkerPid, Owner, Handle, Req, UrlInfo, Opts) ->
    StreamOpts = build_stream_opts(WorkerPid, Owner, Handle, Opts),
    case nhttpc_stream_recv:do_stream_request(Req, UrlInfo, StreamOpts) of
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.

-spec stream_h3(
    pid(), pid(), nhttpc:handle(), nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts()
) ->
    ok | {error, nhttpc_error:any_reason()}.
stream_h3(WorkerPid, Owner, Handle, Req, UrlInfo, Opts) ->
    case nhttpc_request:do_request(Req, UrlInfo, strip_streaming_opts(Opts)) of
        {ok, Resp} ->
            emit_response_as_events(WorkerPid, Owner, Handle, Resp, Opts),
            ok;
        {error, _} = Error ->
            Error
    end.

-spec strip_streaming_opts(nhttpc:opts()) -> nhttpc:opts().
strip_streaming_opts(Opts) ->
    maps:without([body_to, on_download_progress], Opts).
