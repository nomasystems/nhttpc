-module(nhttpc_upload).

-moduledoc """
Runs the source of a `{stream, Source}` request body for one stream of a
shared HTTP/2 connection.

The source does not run in the connection process, because a slow
source would stop each stream of that connection. One process for each
upload runs it. `nhttpc_conn` asks that process for one piece when the
send queue of the stream is empty, so the stream window of the peer is
the backpressure on the source.

The process uses `proc_lib` and a selective receive, not `gen_server`.
Its mailbox belongs to the iterator: an iterator can wait for messages,
and a `gen_server` loop would take them between two calls.

The process stops after the last piece. Before that, `nhttpc_conn`
stops it when the stream ends, `nhttpc_stream_guard` stops it when the
connection exits, and it stops by itself when its owner exits while no
call of the iterator runs.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS - API
%%%-----------------------------------------------------------------------------
-export([
    attach/2,
    demand/3,
    release/2,
    stop/1
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - PROC_LIB
%%%-----------------------------------------------------------------------------
-export([
    init/3
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([piece/0]).

-doc "What the process answers to one demand, as `{'$nhttpc_upload', Ref, Piece}`.".
-type piece() :: {data, binary()} | fin | {error, term()}.

%%%-----------------------------------------------------------------------------
%% RECORDS
%%%-----------------------------------------------------------------------------
-record(state, {
    owner_mref :: reference(),
    iterator :: nhttpc_stream_send:stream_iterator(),
    rest = <<>> :: binary()
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Start the upload process of a request whose body is `{stream, Source}`,
with the caller as its owner. The request comes back with no body, and
with a `content-length` when the size of the source is known. The
stream options carry the process. A request with another body comes
back as it is.
""".
-spec attach(map(), nhttpc_conn:send_opts()) ->
    {ok, nhttp_lib:request(), nhttpc_conn:send_opts()} | {error, nhttpc_error:any_reason()}.
attach(#{body := {stream, Source}} = Req, SendOpts) ->
    attach(Source, #{}, Req, SendOpts);
attach(#{body := {stream, Source, BodyOpts}} = Req, SendOpts) ->
    attach(Source, BodyOpts, Req, SendOpts);
attach(Req, SendOpts) ->
    {ok, Req, SendOpts}.

-doc "Ask for the next piece of at most `Max` octets. The answer goes to the caller.".
-spec demand(pid(), reference(), pos_integer()) -> ok.
demand(Feeder, Ref, Max) ->
    Feeder ! {'$nhttpc_upload_demand', self(), Ref, Max},
    ok.

-doc """
Stop the upload process of a stream that did not open. A stream that
opened belongs to its connection, which stops the process.
""".
-spec release({ok, term()} | {error, term()}, nhttpc_conn:send_opts()) -> ok.
release({error, _Reason}, #{stream := #{upload := Feeder}}) ->
    stop(Feeder);
release(_Result, _SendOpts) ->
    ok.

-doc "Stop the process, in a call of the iterator too. A process that is gone is not an error.".
-spec stop(pid()) -> ok.
stop(Feeder) ->
    true = exit(Feeder, shutdown),
    ok.

%%%-----------------------------------------------------------------------------
%% PROC_LIB
%%%-----------------------------------------------------------------------------
-doc false.
-spec init(pid(), nhttpc_stream_send:stream_source(), nhttpc_stream_send:stream_body_opts()) ->
    ok.
init(Owner, Source, BodyOpts) ->
    case nhttpc_stream_send:prepare_stream_source(Source, BodyOpts) of
        {ok, Iterator, Size} ->
            State = #state{owner_mref = monitor(process, Owner), iterator = Iterator},
            ok = proc_lib:init_ack({ok, self(), Size}),
            loop(State);
        {error, _Reason} = Error ->
            proc_lib:init_fail(Error, {exit, normal})
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec attach(term(), term(), map(), nhttpc_conn:send_opts()) ->
    {ok, nhttp_lib:request(), nhttpc_conn:send_opts()} | {error, nhttpc_error:any_reason()}.
attach(Source, BodyOpts, Req, #{stream := StreamOpts} = SendOpts) ->
    case valid_source(Source) andalso is_map(BodyOpts) of
        true ->
            maybe
                {ok, Feeder, Size} ?= proc_lib:start(?MODULE, init, [self(), Source, BodyOpts]),
                Sized = with_content_length(maps:remove(body, Req), Size),
                {ok, Sized, SendOpts#{stream := StreamOpts#{upload => Feeder}}}
            end;
        false ->
            nhttpc_error:invalid_request(invalid_stream_body)
    end.

-spec loop(#state{}) -> ok.
loop(#state{owner_mref = OwnerMRef} = State) ->
    receive
        {'$nhttpc_upload_demand', ConnPid, Ref, Max} ->
            case next_piece(Max, State) of
                {{data, _Octets} = Piece, State1} ->
                    ConnPid ! {'$nhttpc_upload', Ref, Piece},
                    loop(State1);
                {Last, _State1} ->
                    ConnPid ! {'$nhttpc_upload', Ref, Last},
                    ok
            end;
        {'DOWN', OwnerMRef, process, _Owner, _Reason} ->
            ok
    end.

-doc """
The next piece of the body. A chunk of the iterator above `Max` octets
goes out in pieces, and a chunk with no octets is skipped.
""".
-spec next_piece(pos_integer(), #state{}) -> {piece(), #state{}}.
next_piece(Max, #state{rest = <<>>, iterator = Iterator} = State) ->
    case Iterator() of
        {ok, Chunk, Next} ->
            next_piece(Max, State#state{rest = iolist_to_binary(Chunk), iterator = Next});
        fin ->
            {fin, State};
        {error, Reason} ->
            {{error, Reason}, State}
    end;
next_piece(Max, #state{rest = Rest} = State) when byte_size(Rest) =< Max ->
    {{data, Rest}, State#state{rest = <<>>}};
next_piece(Max, #state{rest = Rest} = State) ->
    <<Piece:Max/binary, Tail/binary>> = Rest,
    {{data, Piece}, State#state{rest = Tail}}.

-spec valid_source(term()) -> boolean().
valid_source({file, Path}) -> is_list(Path) orelse is_binary(Path);
valid_source(Source) -> is_function(Source, 0).

-spec with_content_length(map(), non_neg_integer() | unknown) -> map().
with_content_length(Req, unknown) ->
    Req;
with_content_length(Req, Size) ->
    Headers = maps:get(headers, Req, []),
    case nhttp_headers:has(<<"content-length">>, Headers) of
        true -> Req;
        false -> Req#{headers => Headers ++ [{<<"content-length">>, integer_to_binary(Size)}]}
    end.
