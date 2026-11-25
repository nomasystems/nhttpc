-module(nhttpc_h2_window).

-moduledoc """
Receive-side credit policy for one HTTP/2 connection.

The peer spends its send windows on every DATA frame it delivers, and
this module decides when the client hands that credit back with
WINDOW_UPDATE frames (RFC 9113 Section 6.9). It holds two debts. The
connection debt is the octets received on every stream since the last
connection-level update. Each stream debt is the octets received on
that stream since its last stream-level update.

Connection credit goes back in batches of `connection_threshold/0`
octets, and on the last frame of a stream whatever the debt, so that an
idle connection never holds credit the peer needs for the next request.
Stream credit goes back in batches of half the stream window that the
client advertises, and never for more than the stream consumed. The
stream debt is dropped on the last frame, because the peer has no more
data to send on a stream it closed.

A stream that ends without a last frame, by RST_STREAM or by a local
cancel, leaves its debt behind unless the caller drops it with
`forget/2`.

## Streams with a consumer

`hold/3` gives the stream credit of one stream to its consumer. An octet
of that stream is credited when the consumer returned it with
`release/3`, and never before it arrived. Credit returned ahead pays for
octets on arrival. The connection credit still goes back on arrival.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS - API
%%%-----------------------------------------------------------------------------
-export([
    connection_threshold/0,
    consume/4,
    forget/2,
    hold/3,
    new/1,
    release/3
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    credit/0,
    t/0
]).

-doc "One WINDOW_UPDATE the connection process owes the peer.".
-type credit() :: {connection | nhttp_lib:stream_id(), pos_integer()}.

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(CONNECTION_THRESHOLD, 32768).

-record(held, {
    advance :: non_neg_integer(),
    unread = 0 :: non_neg_integer(),
    debt = 0 :: non_neg_integer()
}).

-record(window, {
    stream_threshold :: pos_integer(),
    connection_debt = 0 :: non_neg_integer(),
    streams = #{} :: #{nhttp_lib:stream_id() => pos_integer() | #held{}}
}).

-opaque t() :: #window{}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "The connection debt at which the connection credit goes back.".
-spec connection_threshold() -> pos_integer().
connection_threshold() ->
    ?CONNECTION_THRESHOLD.

-doc """
Account for `Octets` of DATA payload received on `StreamId`, and return
the credits that are due after it. `Fin` is `fin` on the last frame of
the stream.
""".
-spec consume(nhttp_lib:stream_id(), non_neg_integer(), nhttp_h2:fin(), t()) -> {[credit()], t()}.
consume(_StreamId, 0, nofin, Window) ->
    {[], Window};
consume(StreamId, Octets, Fin, #window{connection_debt = Debt} = Window) ->
    {ConnCredit, Window1} = connection_credit(Fin, Debt + Octets, Window),
    {StreamCredit, Window2} = stream_credit(StreamId, Octets, Fin, Window1),
    {ConnCredit ++ StreamCredit, Window2}.

-doc """
Drop the debt of a stream that ended without a last DATA frame. The
peer closed it or the client reset it, so no credit is owed on it.
""".
-spec forget(nhttp_lib:stream_id(), t()) -> t().
forget(StreamId, #window{streams = Streams} = Window) ->
    Window#window{streams = maps:remove(StreamId, Streams)}.

-doc "Give the stream credit of `StreamId` to its consumer, with `Advance` octets returned ahead.".
-spec hold(nhttp_lib:stream_id(), non_neg_integer(), t()) -> t().
hold(StreamId, Advance, #window{streams = Streams} = Window) ->
    Window#window{streams = Streams#{StreamId => #held{advance = Advance}}}.

-doc """
A window policy for a connection whose streams open with `StreamWindow`
octets of receive credit, the value the client advertises as
SETTINGS_INITIAL_WINDOW_SIZE.
""".
-spec new(pos_integer()) -> t().
new(StreamWindow) ->
    #window{stream_threshold = max(1, StreamWindow div 2)}.

-doc "Account for `Octets` that the consumer of a held stream returned.".
-spec release(nhttp_lib:stream_id(), pos_integer(), t()) -> {[credit()], t()}.
release(StreamId, Octets, #window{streams = Streams} = Window) ->
    case Streams of
        #{StreamId := #held{advance = Advance, unread = Unread, debt = Debt} = Held} ->
            Paid = min(Unread, Octets),
            held_credit(
                StreamId,
                Held#held{
                    advance = Advance + Octets - Paid, unread = Unread - Paid, debt = Debt + Paid
                },
                Window
            );
        #{} ->
            {[], Window}
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL
%%%-----------------------------------------------------------------------------
-spec connection_credit(nhttp_h2:fin(), non_neg_integer(), #window{}) -> {[credit()], #window{}}.
connection_credit(_Fin, 0, Window) ->
    {[], Window};
connection_credit(nofin, Debt, Window) when Debt < ?CONNECTION_THRESHOLD ->
    {[], Window#window{connection_debt = Debt}};
connection_credit(_Fin, Debt, Window) ->
    {[{connection, Debt}], Window#window{connection_debt = 0}}.

-spec held_credit(nhttp_lib:stream_id(), #held{}, #window{}) -> {[credit()], #window{}}.
held_credit(
    StreamId,
    #held{debt = Debt} = Held,
    #window{streams = Streams, stream_threshold = Threshold} =
        Window
) when Debt >= Threshold ->
    {[{StreamId, Debt}], Window#window{streams = Streams#{StreamId => Held#held{debt = 0}}}};
held_credit(StreamId, Held, #window{streams = Streams} = Window) ->
    {[], Window#window{streams = Streams#{StreamId => Held}}}.

-spec stream_credit(nhttp_lib:stream_id(), non_neg_integer(), nhttp_h2:fin(), #window{}) ->
    {[credit()], #window{}}.
stream_credit(StreamId, _Octets, fin, Window) ->
    {[], forget(StreamId, Window)};
stream_credit(
    StreamId, Octets, nofin, #window{streams = Streams, stream_threshold = Threshold} = Window
) ->
    case maps:get(StreamId, Streams, 0) of
        #held{advance = Advance, unread = Unread, debt = Debt} = Held ->
            Paid = min(Advance, Octets),
            held_credit(
                StreamId,
                Held#held{
                    advance = Advance - Paid, unread = Unread + Octets - Paid, debt = Debt + Paid
                },
                Window
            );
        Debt when Debt + Octets >= Threshold ->
            {[{StreamId, Debt + Octets}], forget(StreamId, Window)};
        Debt ->
            {[], Window#window{streams = Streams#{StreamId => Debt + Octets}}}
    end.
