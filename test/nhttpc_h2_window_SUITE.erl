-module(nhttpc_h2_window_SUITE).

-moduledoc """
The receive credit policy of `nhttpc_h2_window`: a held stream is never
credited past what it received or past what its consumer returned.
""".

-include_lib("stdlib/include/assert.hrl").
-include_lib("triq/include/triq.hrl").

%%%-----------------------------------------------------------------------------
%%% EXPORTS - CT
%%%-----------------------------------------------------------------------------
-export([
    all/0
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - CASES
%%%-----------------------------------------------------------------------------
-export([
    held_stream_credit_is_bounded/1,
    last_frame_drops_the_held_stream/1,
    release_on_a_stream_without_a_consumer_owes_nothing/1
]).

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(WINDOW, 65535).
-define(STREAM, 1).
-define(RUNS, 500).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

all() ->
    [
        held_stream_credit_is_bounded,
        last_frame_drops_the_held_stream,
        release_on_a_stream_without_a_consumer_owes_nothing
    ].

%%%-----------------------------------------------------------------------------
%%% CASES
%%%-----------------------------------------------------------------------------

held_stream_credit_is_bounded(_Config) ->
    Prop = ?FORALL(
        {Advance, Steps},
        {choose(0, 3 * ?WINDOW), list(step())},
        bounded(Advance, Steps)
    ),
    ?assertEqual(true, triq:check(Prop, ?RUNS)).

last_frame_drops_the_held_stream(_Config) ->
    Held = nhttpc_h2_window:hold(?STREAM, 0, nhttpc_h2_window:new(?WINDOW)),
    {[], Open} = nhttpc_h2_window:consume(?STREAM, 100, nofin, Held),
    {[{connection, 200}], Ended} = nhttpc_h2_window:consume(?STREAM, 100, fin, Open),
    ?assertEqual({[], Ended}, nhttpc_h2_window:release(?STREAM, ?WINDOW, Ended)).

release_on_a_stream_without_a_consumer_owes_nothing(_Config) ->
    Window = nhttpc_h2_window:new(?WINDOW),
    {[], Open} = nhttpc_h2_window:consume(?STREAM, 100, nofin, Window),
    ?assertEqual({[], Open}, nhttpc_h2_window:release(?STREAM, 100, Open)).

%%%-----------------------------------------------------------------------------
%%% PROPERTY
%%%-----------------------------------------------------------------------------

step() ->
    oneof([{arrive, choose(1, 20000)}, {release, choose(1, 40000)}]).

-spec bounded(non_neg_integer(), [{arrive | release, pos_integer()}]) -> boolean().
bounded(Advance, Steps) ->
    Start = #{
        window => nhttpc_h2_window:hold(?STREAM, Advance, nhttpc_h2_window:new(?WINDOW)),
        received => 0,
        released => Advance,
        stream => 0,
        connection => 0
    },
    lists:foldl(fun step/2, Start, Steps) =/= violated.

-spec step({arrive | release, pos_integer()}, map() | violated) -> map() | violated.
step(_Step, violated) ->
    violated;
step({arrive, Octets}, #{window := Window, received := Received} = State) ->
    {Credits, Window1} = nhttpc_h2_window:consume(?STREAM, Octets, nofin, Window),
    holds(credited(Credits, State#{window := Window1, received := Received + Octets}));
step({release, Octets}, #{window := Window, released := Released} = State) ->
    {Credits, Window1} = nhttpc_h2_window:release(?STREAM, Octets, Window),
    holds(credited(Credits, State#{window := Window1, released := Released + Octets})).

-spec credited([nhttpc_h2_window:credit()], map()) -> map().
credited(Credits, State) ->
    lists:foldl(
        fun
            ({connection, N}, #{connection := Sum} = Acc) -> Acc#{connection := Sum + N};
            ({?STREAM, N}, #{stream := Sum} = Acc) -> Acc#{stream := Sum + N}
        end,
        State,
        Credits
    ).

-spec holds(map()) -> map() | violated.
holds(
    #{received := Received, released := Released, stream := Stream, connection := Connection} =
        State
) ->
    Due = min(Received, Released),
    Holds =
        Stream =< Due andalso
            Due - Stream < ?WINDOW div 2 andalso
            Connection =< Received andalso
            Received - Connection < nhttpc_h2_window:connection_threshold(),
    case Holds of
        true -> State;
        false -> violated
    end.
