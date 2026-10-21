-module(nhttpc_stream_slots).

-moduledoc """
Live stream count, concurrent stream cap and unclaimed send credit for
one multiplexed connection.

The three values live in one `atomics` array. The pool manager creates
the array with the connection, keeps it in `#conn_info{}` and in the
connection tuple, and hands the same reference to the connection
process. A stream reservation and a credit reservation are each one
lock-free compare-and-swap on the same cache line, and a release is one
`sub_get` or `add`.

The connection process is the only writer of the cap and the only
process that adds credit. It learns both values first, and a write there
reaches the lock-free checkout path without the manager mailbox in
between.

The credit slot holds the octets of HTTP/2 connection send credit that
no request has claimed yet. The connection writes the peer's window at
ready, adds every connection-level WINDOW_UPDATE, refunds a request it
refused before any octet reached the socket, and rewrites the slot from
the codec window when it is idle. A checkout swaps the value for the
value less the body size only if nobody moved it first, so N concurrent
checkouts cannot all pass on one reading of the value, and no reader
ever sees a value the claim will retract. A connection without a
send window to respect, HTTP/3 today, writes `unbounded_credit/0` once
and the reservation never binds it.

One array per connection, not one index into a shared array. ETS derives
its write lock from the key, so every caller that reserves a stream on
one connection serialises on one lock, whatever the table options say. A
shared array needs a free list, and a recycled index is a correctness
hazard: the pool stops an aged connection asynchronously, so the old
process can still release a slot after a new connection takes its index.
A per-connection array has no index to recycle. The old process releases
its own array, which nothing reads any more.

The array is signed. A release that drives the live count below zero is a
double release. `release/1` resets the count to zero and logs the defect,
because a silent clamp hides it for the life of the connection.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS - API
%%%-----------------------------------------------------------------------------
-export([
    add_credit/2,
    cap/1,
    credit/1,
    debit_credit/2,
    default_cap/1,
    live/1,
    new/0,
    refund_credit/2,
    release/1,
    reserve/1,
    reserve/2,
    reserve_credit/2,
    resolve_cap/2,
    set_cap/2,
    set_credit/2,
    unbounded_credit/0
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    slots/0
]).

-type slots() :: atomics:atomics_ref().

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(DEFAULT_MAX_CONCURRENT_STREAMS, 100).
-define(SLOT_COUNT, 3).
-define(SLOT_LIVE, 1).
-define(SLOT_CAP, 2).
-define(SLOT_CREDIT, 3).
-define(CREDIT_UNBOUNDED, 1 bsl 62).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Add send credit the peer granted. The connection process calls this for
every connection-level WINDOW_UPDATE frame.
""".
-spec add_credit(slots(), non_neg_integer()) -> ok.
add_credit(Slots, Octets) ->
    atomics:add(Slots, ?SLOT_CREDIT, Octets).

-doc """
The concurrent stream cap. A new array carries a cap of zero, so every
reservation fails until the connection process writes the value that the
peer advertised.
""".
-spec cap(slots()) -> integer().
cap(Slots) ->
    atomics:get(Slots, ?SLOT_CAP).

-doc """
Send credit no request has claimed yet. A new array carries zero, so a
checkout with a body fails until the connection process writes the
peer's window. A reader can see a negative value between one caller's
subtraction and its own rollback.
""".
-spec credit(slots()) -> integer().
credit(Slots) ->
    atomics:get(Slots, ?SLOT_CREDIT).

-doc """
Take send credit that the codec spent on octets no checkout reserved.
The connection process calls this for the octets its codec send queue
emits when the peer's credit arrives, and for the octets a body sent
beyond its claim: the WINDOW_UPDATE that paid for them went into the
slot in full, and they never pass through a checkout.
""".
-spec debit_credit(slots(), non_neg_integer()) -> ok.
debit_credit(Slots, Octets) ->
    atomics:sub(Slots, ?SLOT_CREDIT, Octets).

-doc """
The client-side ceiling for one protocol. RFC 9113 section 6.5.2 reads
the absence of `SETTINGS_MAX_CONCURRENT_STREAMS` as "no limit", so a
multiplexed peer that names none gets a client-side ceiling rather than
an unbounded one. An exclusive protocol carries one stream.
""".
-spec default_cap(nhttp_lib:version() | undefined) -> pos_integer().
default_cap(http2) -> ?DEFAULT_MAX_CONCURRENT_STREAMS;
default_cap(http3) -> ?DEFAULT_MAX_CONCURRENT_STREAMS;
default_cap(_Exclusive) -> 1.

-doc "Live streams on this connection right now.".
-spec live(slots()) -> integer().
live(Slots) ->
    atomics:get(Slots, ?SLOT_LIVE).

-doc "A new array for one connection. Live count zero, cap zero, credit zero.".
-spec new() -> slots().
new() ->
    atomics:new(?SLOT_COUNT, [{signed, true}]).

-doc """
Give a credit reservation back. The connection process calls this for a
request it refused before any octet of it reached the socket, because
the reservation covered a body the codec never debited.
""".
-spec refund_credit(slots(), non_neg_integer()) -> ok.
refund_credit(Slots, Octets) ->
    add_credit(Slots, Octets).

-doc """
Give one stream slot back. The connection process calls this when a
stream ends, and for a send that never became a stream.
""".
-spec release(slots()) -> ok.
release(Slots) ->
    case atomics:sub_get(Slots, ?SLOT_LIVE, 1) of
        Live when Live >= 0 -> ok;
        Live -> report_underflow(Slots, Live)
    end.

-doc """
Claim one stream slot, or answer `full` when the connection is at its
cap.
The claim reads the count and swaps it for one more only while nobody
moved it in between, so the count never holds a value a claim will
retract: a concurrent caller is never answered `full` for a slot that
another caller was about to give back, and a reader never sees the
count past the cap. A swap that loses to another caller reads again.
""".
-spec reserve(slots()) -> ok | full.
reserve(Slots) ->
    Cap = atomics:get(Slots, ?SLOT_CAP),
    claim_slot(Slots, Cap, atomics:get(Slots, ?SLOT_LIVE)).

-doc """
Claim one stream slot and `Octets` of send credit for one request, or
answer why the connection cannot take it. A connection is available
only when both halves succeed: `full` is a connection at its stream cap,
`starved` is one that cannot carry the body. A stream slot claimed
ahead of a starved credit reservation goes back before the answer, so
an unavailable connection holds nothing for the caller.
""".
-spec reserve(slots(), non_neg_integer()) -> ok | full | starved.
reserve(Slots, Octets) ->
    case reserve(Slots) of
        ok ->
            case reserve_credit(Slots, Octets) of
                ok ->
                    ok;
                starved ->
                    ok = release(Slots),
                    starved
            end;
        full ->
            full
    end.

-doc """
Claim `Octets` of send credit for one body, or answer `starved` when the
connection cannot carry it.
The claim has the shape of `reserve/1`: a read and a swap that holds
only while nobody moved the value, so two callers cannot both pass on
one reading of it, a body that fits is never refused for a body that
did not, and the credit never reads below zero. A body of zero octets
claims nothing and never starves.
""".
-spec reserve_credit(slots(), non_neg_integer()) -> ok | starved.
reserve_credit(_Slots, 0) ->
    ok;
reserve_credit(Slots, Octets) ->
    claim_credit(Slots, Octets, atomics:get(Slots, ?SLOT_CREDIT)).

-doc """
The cap to enforce for one connection. A multiplexed peer that advertised
`max_concurrent_streams` sets it. Everything else falls back to
`default_cap/1`.
""".
-spec resolve_cap(non_neg_integer() | undefined, nhttp_lib:version() | undefined) ->
    non_neg_integer().
resolve_cap(Cap, Protocol) when
    is_integer(Cap), (Protocol =:= http2 orelse Protocol =:= http3)
->
    Cap;
resolve_cap(_PeerCap, Protocol) ->
    default_cap(Protocol).

-doc """
Write the concurrent stream cap. The connection process is the only
writer: it resolves the value the peer advertised before it reports
ready, and writes again for every later SETTINGS frame that carries one.
The manager creates the array with a cap of zero and never writes it, so
a checkout cannot succeed against a cap the peer never sent.
""".
-spec set_cap(slots(), non_neg_integer()) -> ok.
set_cap(Slots, Cap) ->
    atomics:put(Slots, ?SLOT_CAP, Cap).

-doc """
Write the unclaimed send credit outright. The connection process writes
the peer's connection window before it reports ready, and rewrites the
slot from the codec window whenever it is idle: with no request in
flight and no send in its mailbox the two values are equal, and the
write corrects any drift left by a caller that died between checkout
and send.
""".
-spec set_credit(slots(), integer()) -> ok.
set_credit(Slots, Octets) ->
    atomics:put(Slots, ?SLOT_CREDIT, Octets).

-doc """
The credit a connection without a send window to respect writes at
ready. The value is far above any body the pool admits, so a
reservation subtracts from it for the life of the connection and never
rolls back. The fast path stays one compare-and-swap with no branch on
protocol.
""".
-spec unbounded_credit() -> pos_integer().
unbounded_credit() ->
    ?CREDIT_UNBOUNDED.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
-spec claim_credit(slots(), pos_integer(), integer()) -> ok | starved.
claim_credit(_Slots, Octets, Credit) when Credit < Octets ->
    starved;
claim_credit(Slots, Octets, Credit) ->
    case atomics:compare_exchange(Slots, ?SLOT_CREDIT, Credit, Credit - Octets) of
        ok -> ok;
        Moved -> claim_credit(Slots, Octets, Moved)
    end.

-spec claim_slot(slots(), non_neg_integer(), integer()) -> ok | full.
claim_slot(_Slots, Cap, Live) when Live >= Cap ->
    full;
claim_slot(Slots, Cap, Live) ->
    case atomics:compare_exchange(Slots, ?SLOT_LIVE, Live, Live + 1) of
        ok -> ok;
        Moved -> claim_slot(Slots, Cap, Moved)
    end.

-doc false.
-spec report_underflow(slots(), neg_integer()) -> ok.
report_underflow(Slots, Live) ->
    ok = atomics:put(Slots, ?SLOT_LIVE, 0),
    logger:warning(
        #{event => stream_slot_underflow, live => Live, connection => self()},
        #{domain => [nhttpc, pool]}
    ).
