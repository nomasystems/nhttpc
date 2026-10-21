-module(nhttpc_h2_script_peer).

-moduledoc """
A scriptable HTTP/2 peer on the `nhttp_h2` server codec, over TLS with
ALPN `h2`. It advertises the SETTINGS a test names, answers every
request with a 200 after an optional delay, with an optional body, and
credits the connection window and the stream windows for request bodies
eagerly, on a trickle, or not at all. `{trickle, Quantum, IntervalMs}`
credits the stream windows eagerly and the connection window `Quantum`
octets at most every `IntervalMs`, on a timer that starts with the first
DATA frame. That is the shape of a peer that credits on processing, in
batches, on the connection alone. A switch to `eager` credits the whole
outstanding debt at once.
A test sends it SETTINGS, GOAWAY and WINDOW_UPDATE frames at any moment.
A request whose path is `/body/N` is answered with a body of `N` octets,
whatever `response_body` says.

Every connection records what the client did: the streams it opened, the
request octets it sent, the DATA frames it sent and the largest of them,
the streams it reset, every WINDOW_UPDATE increment it returned, per
stream and on the connection, and the octets the peer sent it. That is
what the flow-control conformance cases assert on. A connection whose
socket closes keeps its process alive so the record survives.

`nhttp` cannot change its SETTINGS after the handshake, cannot send a
GOAWAY with a chosen last stream id, and credits on read. This peer can.
""".

%%%-----------------------------------------------------------------------------
%%% EXPORTS - API
%%%-----------------------------------------------------------------------------
-export([
    broadcast/2,
    command/2,
    connections/1,
    start/2,
    stats/1,
    stop/1,
    wait_connections/3
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - INTERNAL PROCESSES
%%%-----------------------------------------------------------------------------
-export([
    acceptor/3,
    conn/2,
    controller/2
]).

%%%-----------------------------------------------------------------------------
%%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    command/0,
    opts/0,
    stats/0
]).

-type credit_policy() :: eager | never | {trickle, pos_integer(), pos_integer()}.

-type command() ::
    {settings, nhttp_h2:settings()}
    | {goaway, nhttp_lib:stream_id(), nhttp_h2:error_code(), binary()}
    | {credit, credit_policy()}
    | {window_update, pos_integer()}
    | {stream_window_update, nhttp_lib:stream_id(), pos_integer()}
    | {ping, binary()}.

-type opts() :: #{
    settings => nhttp_h2:settings(),
    credit => credit_policy(),
    response_delay_ms => non_neg_integer(),
    response_body => non_neg_integer(),
    after_first_response => [command()]
}.

-type stats() :: #{
    requests := non_neg_integer(),
    responses := non_neg_integer(),
    streams := [nhttp_lib:stream_id()],
    data_octets := non_neg_integer(),
    data_octets_while_zero := non_neg_integer(),
    data_frames := #{nhttp_lib:stream_id() => non_neg_integer()},
    max_data_frame := non_neg_integer(),
    resets := #{nhttp_lib:stream_id() => nhttp_h2:error_code()},
    client_credit_conn := non_neg_integer(),
    client_credit_streams := #{nhttp_lib:stream_id() => non_neg_integer()},
    sent_octets_conn := non_neg_integer(),
    sent_octets_streams := #{nhttp_lib:stream_id() => non_neg_integer()},
    settings_acks := non_neg_integer(),
    ping_acks := non_neg_integer(),
    max_conn_window := integer(),
    max_stream_window := integer(),
    closed := boolean()
}.

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(ACCEPT_TIMEOUT, 1000).
-define(HANDSHAKE_TIMEOUT, 10000).
-define(ACTIVE_N, 64).
-define(CALL_TIMEOUT, 5000).
-define(DEFAULT_SETTINGS, #{max_concurrent_streams => 1000}).
-define(DEFAULT_WINDOW, 65535).
-define(DEFAULT_MAX_FRAME_SIZE, 16384).
-define(POLL_MS, 20).

%%%-----------------------------------------------------------------------------
%%% RECORDS
%%%-----------------------------------------------------------------------------
-record(stream, {
    responded = false :: boolean(),
    body = undefined :: undefined | non_neg_integer(),
    rest = undefined :: undefined | binary()
}).

-record(conn, {
    sock :: ssl:sslsocket(),
    h2 :: nhttp_h2:conn(),
    opts :: opts(),
    credit :: credit_policy(),
    consumed = 0 :: non_neg_integer(),
    consumed_streams = #{} :: #{nhttp_lib:stream_id() => pos_integer()},
    streams = #{} :: #{nhttp_lib:stream_id() => #stream{}},
    zero_window = false :: boolean(),
    tick_ref = undefined :: undefined | reference(),
    goaway_last = undefined :: undefined | nhttp_lib:stream_id(),
    client_initial_window = ?DEFAULT_WINDOW :: pos_integer(),
    first_response_done = false :: boolean(),
    closed = false :: boolean(),
    out = [] :: iodata(),
    raw = <<>> :: binary(),
    stats :: stats()
}).

%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

-doc "Send a command to every connection the peer holds.".
-spec broadcast(pid(), command()) -> ok.
broadcast(Peer, Command) ->
    lists:foreach(fun(Conn) -> command(Conn, Command) end, connections(Peer)).

-doc "Send a command to one connection.".
-spec command(pid(), command()) -> ok.
command(Conn, Command) ->
    Conn ! {command, Command},
    ok.

-doc "The connection processes, in accept order.".
-spec connections(pid()) -> [pid()].
connections(Peer) ->
    Ref = make_ref(),
    Peer ! {connections, self(), Ref},
    receive
        {Ref, Conns} -> Conns
    after ?CALL_TIMEOUT -> []
    end.

-doc "Start the peer on a free port with the suite's certificate.".
-spec start([tuple()], opts()) -> {pid(), inet:port_number()}.
start(Config, Opts) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    {ok, LSock} = ssl:listen(0, [
        binary,
        {active, false},
        {reuseaddr, true},
        {certfile, CertFile},
        {keyfile, KeyFile},
        {alpn_preferred_protocols, [<<"h2">>]}
    ]),
    {ok, {_Addr, Port}} = ssl:sockname(LSock),
    Peer = spawn_link(?MODULE, controller, [LSock, Opts]),
    {Peer, Port}.

-doc "What one connection recorded so far.".
-spec stats(pid()) -> stats().
stats(Conn) ->
    Ref = make_ref(),
    Conn ! {stats, self(), Ref},
    receive
        {Ref, Stats} -> Stats
    after ?CALL_TIMEOUT -> error({peer_stats_timeout, Conn})
    end.

-doc "Stop the peer and every connection it holds.".
-spec stop(pid()) -> ok.
stop(Peer) ->
    Peer ! stop,
    ok.

-doc "Wait until the peer holds at least `Count` connections.".
-spec wait_connections(pid(), non_neg_integer(), non_neg_integer()) -> {ok, [pid()]} | timeout.
wait_connections(Peer, Count, Remaining) ->
    case connections(Peer) of
        Conns when length(Conns) >= Count ->
            {ok, Conns};
        _Fewer when Remaining =< 0 ->
            timeout;
        _Fewer ->
            timer:sleep(?POLL_MS),
            wait_connections(Peer, Count, Remaining - ?POLL_MS)
    end.

%%%-----------------------------------------------------------------------------
%%% CONTROLLER
%%%-----------------------------------------------------------------------------

-doc false.
-spec controller(ssl:sslsocket(), opts()) -> ok.
controller(LSock, Opts) ->
    process_flag(trap_exit, true),
    Self = self(),
    Acceptor = spawn_link(?MODULE, acceptor, [LSock, Opts, Self]),
    controller_loop(LSock, Acceptor, []).

-spec controller_loop(ssl:sslsocket(), pid(), [pid()]) -> ok.
controller_loop(LSock, Acceptor, Conns) ->
    receive
        {accepted, Conn} ->
            controller_loop(LSock, Acceptor, Conns ++ [Conn]);
        {connections, From, Ref} ->
            From ! {Ref, Conns},
            controller_loop(LSock, Acceptor, Conns);
        {'EXIT', Acceptor, _Reason} ->
            controller_loop(LSock, Acceptor, Conns);
        {'EXIT', Conn, _Reason} ->
            controller_loop(LSock, Acceptor, lists:delete(Conn, Conns));
        stop ->
            lists:foreach(fun(Conn) -> Conn ! stop end, Conns),
            exit(Acceptor, kill),
            ok = ssl:close(LSock)
    end.

-doc false.
-spec acceptor(ssl:sslsocket(), opts(), pid()) -> ok.
acceptor(LSock, Opts, Controller) ->
    case ssl:transport_accept(LSock, ?ACCEPT_TIMEOUT) of
        {ok, TSock} ->
            case ssl:handshake(TSock, ?HANDSHAKE_TIMEOUT) of
                {ok, Sock} ->
                    Conn = spawn_link(?MODULE, conn, [Sock, Opts]),
                    ok = ssl:controlling_process(Sock, Conn),
                    Conn ! go,
                    Controller ! {accepted, Conn},
                    acceptor(LSock, Opts, Controller);
                {error, _Reason} ->
                    acceptor(LSock, Opts, Controller)
            end;
        {error, timeout} ->
            acceptor(LSock, Opts, Controller);
        {error, _Reason} ->
            ok
    end.

%%%-----------------------------------------------------------------------------
%%% CONNECTION
%%%-----------------------------------------------------------------------------

-doc false.
-spec conn(ssl:sslsocket(), opts()) -> ok.
conn(Sock, Opts) ->
    receive
        go -> ok
    end,
    Settings = maps:merge(?DEFAULT_SETTINGS, maps:get(settings, Opts, #{})),
    H2 = nhttp_h2:new(server, Settings#{max_concurrent_streams => infinity}),
    {ok, SettingsFrame} = nhttp_h2_frame:settings(Settings),
    ok = ssl:send(Sock, SettingsFrame),
    ok = ssl:setopts(Sock, [{active, ?ACTIVE_N}]),
    conn_loop(#conn{
        sock = Sock,
        h2 = H2,
        opts = Opts,
        credit = maps:get(credit, Opts, eager),
        zero_window = maps:get(initial_window_size, Settings, ?DEFAULT_WINDOW) =:= 0,
        stats = new_stats()
    }).

-spec new_stats() -> stats().
new_stats() ->
    #{
        requests => 0,
        responses => 0,
        streams => [],
        data_octets => 0,
        data_octets_while_zero => 0,
        data_frames => #{},
        max_data_frame => 0,
        resets => #{},
        client_credit_conn => 0,
        client_credit_streams => #{},
        sent_octets_conn => 0,
        sent_octets_streams => #{},
        settings_acks => 0,
        ping_acks => 0,
        max_conn_window => ?DEFAULT_WINDOW,
        max_stream_window => ?DEFAULT_WINDOW,
        closed => false
    }.

-spec conn_loop(#conn{}) -> ok.
conn_loop(#conn{sock = Sock} = Conn) ->
    receive
        {ssl, Sock, Data} ->
            conn_loop(on_data(Data, Conn));
        {ssl_passive, Sock} ->
            ok = ssl:setopts(Sock, [{active, ?ACTIVE_N}]),
            conn_loop(Conn);
        {respond, StreamId} ->
            conn_loop(flush(respond(StreamId, Conn)));
        {command, Command} ->
            conn_loop(flush(on_command(Command, Conn)));
        {timeout, Ref, tick} when Ref =:= Conn#conn.tick_ref ->
            conn_loop(flush(on_tick(Conn)));
        {timeout, _StaleRef, tick} ->
            conn_loop(Conn);
        {stats, From, Ref} ->
            From ! {Ref, (Conn#conn.stats)#{closed => Conn#conn.closed}},
            conn_loop(Conn);
        {ssl_closed, Sock} ->
            conn_loop(Conn#conn{closed = true});
        {ssl_error, Sock, _Reason} ->
            conn_loop(Conn#conn{closed = true});
        stop ->
            _ = ssl:close(Sock),
            ok
    end.

-spec on_data(binary(), #conn{}) -> #conn{}.
on_data(Data, #conn{h2 = H2} = Conn0) ->
    Conn = count_frames(Data, Conn0),
    case nhttp_h2:recv(H2, Data) of
        {ok, Events, H2b} ->
            after_events(lists:foldl(fun on_event/2, Conn#conn{h2 = H2b}, Events));
        {ok, Events, H2b, ToSend} ->
            Conn1 = Conn#conn{h2 = H2b, out = [Conn#conn.out, ToSend]},
            after_events(lists:foldl(fun on_event/2, Conn1, Events));
        {error, _Reason} ->
            Conn#conn{closed = true}
    end.

-spec after_events(#conn{}) -> #conn{}.
after_events(Conn) ->
    flush(retry_pending(credit(Conn))).

-spec on_event(nhttp_h2:event(), #conn{}) -> #conn{}.
on_event({request, StreamId, Request, Fin}, #conn{stats = Stats, streams = Streams} = Conn) ->
    #{requests := Requests, streams := Seen} = Stats,
    Conn1 = Conn#conn{
        streams = Streams#{StreamId => #stream{body = path_body_size(Request)}},
        stats = Stats#{requests => Requests + 1, streams => [StreamId | Seen]}
    },
    maybe_schedule(StreamId, Fin, Conn1);
on_event({data, StreamId, Payload, Fin}, #conn{stats = Stats, consumed = Consumed} = Conn) ->
    Size = byte_size(Payload),
    #{data_octets := Octets, data_octets_while_zero := Zero} = Stats,
    ConsumedStreams = Conn#conn.consumed_streams,
    Conn1 = Conn#conn{
        consumed = Consumed + Size,
        consumed_streams = ConsumedStreams#{
            StreamId => maps:get(StreamId, ConsumedStreams, 0) + Size
        },
        stats = Stats#{
            data_octets => Octets + Size,
            data_octets_while_zero => Zero + while_zero(Conn#conn.zero_window, Size)
        }
    },
    maybe_schedule(StreamId, Fin, Conn1);
on_event({window_update, 0, Increment}, #conn{stats = Stats} = Conn) ->
    #{client_credit_conn := Credit, sent_octets_conn := Sent, max_conn_window := Max} = Stats,
    NewCredit = Credit + Increment,
    Window = ?DEFAULT_WINDOW + NewCredit - Sent,
    Conn#conn{
        stats = Stats#{client_credit_conn => NewCredit, max_conn_window => max(Max, Window)}
    };
on_event({window_update, StreamId, Increment}, #conn{stats = Stats} = Conn) ->
    #{
        client_credit_streams := Credits,
        sent_octets_streams := Sents,
        max_stream_window := Max
    } = Stats,
    NewCredit = maps:get(StreamId, Credits, 0) + Increment,
    Window = Conn#conn.client_initial_window + NewCredit - maps:get(StreamId, Sents, 0),
    Conn#conn{
        stats = Stats#{
            client_credit_streams => Credits#{StreamId => NewCredit},
            max_stream_window => max(Max, Window)
        }
    };
on_event({settings, ClientSettings}, Conn) ->
    Conn#conn{
        client_initial_window = maps:get(
            initial_window_size, ClientSettings, Conn#conn.client_initial_window
        )
    };
on_event(settings_ack, #conn{stats = #{settings_acks := Acks} = Stats} = Conn) ->
    Conn#conn{stats = Stats#{settings_acks => Acks + 1}};
on_event({stream_reset, StreamId, Code}, #conn{streams = Streams, stats = Stats} = Conn) ->
    #{resets := Resets} = Stats,
    Conn#conn{
        streams = maps:remove(StreamId, Streams),
        stats = Stats#{resets => Resets#{StreamId => Code}}
    };
on_event({stream_closed, _StreamId, _Code}, Conn) ->
    Conn;
on_event({stream_refused, _StreamId}, Conn) ->
    Conn;
on_event({trailers, _StreamId, _Trailers}, Conn) ->
    Conn;
on_event({goaway, _StreamId, _Code, _Debug}, Conn) ->
    Conn;
on_event({response, _StreamId, _Response, _Fin}, Conn) ->
    Conn;
on_event({ping, _Data}, Conn) ->
    Conn;
on_event({ping_ack, _Data}, #conn{stats = #{ping_acks := Acks} = Stats} = Conn) ->
    Conn#conn{stats = Stats#{ping_acks => Acks + 1}}.

-spec while_zero(boolean(), non_neg_integer()) -> non_neg_integer().
while_zero(true, Size) -> Size;
while_zero(false, _Size) -> 0.

-spec maybe_schedule(nhttp_lib:stream_id(), nhttp_h2:fin(), #conn{}) -> #conn{}.
maybe_schedule(_StreamId, nofin, Conn) ->
    Conn;
maybe_schedule(StreamId, fin, #conn{goaway_last = Last} = Conn) when
    Last =/= undefined, StreamId > Last
->
    Conn;
maybe_schedule(StreamId, fin, #conn{opts = Opts} = Conn) ->
    case maps:get(response_delay_ms, Opts, 0) of
        0 ->
            respond(StreamId, Conn);
        Delay ->
            _ = erlang:send_after(Delay, self(), {respond, StreamId}),
            Conn
    end.

-spec respond(nhttp_lib:stream_id(), #conn{}) -> #conn{}.
respond(StreamId, #conn{goaway_last = Last} = Conn) when Last =/= undefined, StreamId > Last ->
    Conn;
respond(StreamId, #conn{streams = Streams} = Conn) ->
    case maps:find(StreamId, Streams) of
        {ok, #stream{responded = false} = Stream} ->
            after_first_response(send_response(StreamId, Stream, Conn));
        _Gone ->
            Conn
    end.

-spec send_response(nhttp_lib:stream_id(), #stream{}, #conn{}) -> #conn{}.
send_response(StreamId, Stream, #conn{h2 = H2, opts = Opts, stats = Stats} = Conn) ->
    BodySize = response_body_size(Stream, Opts),
    Headers = [{<<":status">>, <<"200">>}, {<<"x-stream">>, integer_to_binary(StreamId)}],
    Fin = fin_for(BodySize),
    case nhttp_h2:send_headers(H2, StreamId, Headers, Fin) of
        {ok, H2b, Frame} ->
            #{responses := Responses} = Stats,
            Conn1 = Conn#conn{
                h2 = H2b,
                out = [Conn#conn.out, Frame],
                stats = Stats#{responses => Responses + 1},
                streams = (Conn#conn.streams)#{StreamId => Stream#stream{responded = true}}
            },
            send_body(StreamId, BodySize, Conn1);
        {error, _Reason} ->
            Conn#conn{streams = maps:remove(StreamId, Conn#conn.streams)}
    end.

-spec fin_for(non_neg_integer()) -> nhttp_h2:fin().
fin_for(0) -> fin;
fin_for(_Size) -> nofin.

-spec send_body(nhttp_lib:stream_id(), non_neg_integer(), #conn{}) -> #conn{}.
send_body(_StreamId, 0, Conn) ->
    Conn;
send_body(StreamId, BodySize, Conn) ->
    send_data(StreamId, binary:copy(<<$b>>, BodySize), Conn).

-spec send_data(nhttp_lib:stream_id(), binary(), #conn{}) -> #conn{}.
send_data(StreamId, Body, #conn{h2 = H2, streams = Streams} = Conn) ->
    case nhttp_h2:send_data(H2, StreamId, Body, fin) of
        {ok, H2b, Frames} ->
            Conn1 = note_sent(StreamId, byte_size(Body), Conn#conn{h2 = H2b}),
            Conn1#conn{
                out = [Conn1#conn.out, Frames],
                streams = maps:remove(StreamId, Streams)
            };
        {partial, H2b, Frames, Rest, fin, _Window} ->
            Conn1 = note_sent(StreamId, byte_size(Body) - byte_size(Rest), Conn#conn{h2 = H2b}),
            Stream = maps:get(StreamId, Streams, #stream{responded = true}),
            Conn1#conn{
                out = [Conn1#conn.out, Frames],
                streams = Streams#{StreamId => Stream#stream{rest = Rest}}
            };
        {error, _Reason} ->
            Conn#conn{streams = maps:remove(StreamId, Streams)}
    end.

-spec note_sent(nhttp_lib:stream_id(), non_neg_integer(), #conn{}) -> #conn{}.
note_sent(StreamId, Size, #conn{stats = Stats} = Conn) ->
    #{sent_octets_conn := Sent, sent_octets_streams := Sents} = Stats,
    Conn#conn{
        stats = Stats#{
            sent_octets_conn => Sent + Size,
            sent_octets_streams => Sents#{StreamId => maps:get(StreamId, Sents, 0) + Size}
        }
    }.

-spec retry_pending(#conn{}) -> #conn{}.
retry_pending(#conn{streams = Streams} = Conn) ->
    Pending = [
        {Id, Rest}
     || {Id, #stream{rest = Rest}} <- maps:to_list(Streams), Rest =/= undefined
    ],
    lists:foldl(fun({Id, Rest}, Acc) -> send_data(Id, Rest, Acc) end, Conn, Pending).

-spec after_first_response(#conn{}) -> #conn{}.
after_first_response(#conn{first_response_done = true} = Conn) ->
    Conn;
after_first_response(#conn{opts = Opts} = Conn) ->
    Commands = maps:get(after_first_response, Opts, []),
    lists:foldl(fun on_command/2, Conn#conn{first_response_done = true}, Commands).

-spec on_command(command(), #conn{}) -> #conn{}.
on_command({settings, Settings}, #conn{out = Out} = Conn) ->
    {ok, Frame} = nhttp_h2_frame:settings(Settings),
    ZeroWindow =
        case maps:find(initial_window_size, Settings) of
            {ok, Size} -> Size =:= 0;
            error -> Conn#conn.zero_window
        end,
    Conn#conn{out = [Out, Frame], zero_window = ZeroWindow};
on_command({goaway, LastStreamId, ErrorCode, Debug}, #conn{out = Out} = Conn) ->
    {ok, Frame} = nhttp_h2_frame:goaway(LastStreamId, ErrorCode, Debug),
    Conn#conn{out = [Out, Frame], goaway_last = LastStreamId};
on_command({credit, Policy}, Conn) ->
    credit(Conn#conn{credit = Policy});
on_command({window_update, Increment}, #conn{h2 = H2, out = Out} = Conn) ->
    case nhttp_h2:send_window_update(H2, connection, Increment) of
        {ok, H2b, Frame} -> Conn#conn{h2 = H2b, out = [Out, Frame]};
        {error, _Reason} -> Conn
    end;
on_command({stream_window_update, StreamId, Increment}, #conn{h2 = H2, out = Out} = Conn) ->
    case nhttp_h2:send_window_update(H2, StreamId, Increment) of
        {ok, H2b, Frame} -> Conn#conn{h2 = H2b, out = [Out, Frame]};
        {error, _Reason} -> Conn
    end;
on_command({ping, Data}, #conn{out = Out} = Conn) ->
    {ok, Frame} = nhttp_h2_frame:ping(Data),
    Conn#conn{out = [Out, Frame]}.

-spec credit(#conn{}) -> #conn{}.
credit(#conn{consumed = 0} = Conn) ->
    Conn;
credit(#conn{credit = never} = Conn) ->
    Conn;
credit(#conn{credit = eager, consumed = Consumed} = Conn) ->
    credit_streams(credit_connection(Consumed, Conn));
credit(#conn{credit = {trickle, _Quantum, IntervalMs}} = Conn) ->
    credit_streams(start_tick(IntervalMs, Conn)).

-spec credit_connection(non_neg_integer(), #conn{}) -> #conn{}.
credit_connection(0, Conn) ->
    Conn;
credit_connection(Octets, #conn{consumed = Consumed, h2 = H2, out = Out} = Conn) ->
    case nhttp_h2:send_window_update(H2, connection, Octets) of
        {ok, H2b, Frame} -> Conn#conn{h2 = H2b, out = [Out, Frame], consumed = Consumed - Octets};
        {error, _Reason} -> Conn#conn{consumed = Consumed - Octets}
    end.

-spec credit_streams(#conn{}) -> #conn{}.
credit_streams(#conn{consumed_streams = ConsumedStreams} = Conn) ->
    maps:fold(fun credit_stream/3, Conn#conn{consumed_streams = #{}}, ConsumedStreams).

-doc """
One trickle tick: credit the connection window `Quantum` octets at most,
never more than the peer consumed, and arm the next tick. A tick that
lands after a switch to another policy credits nothing and arms nothing.
""".
-spec on_tick(#conn{}) -> #conn{}.
on_tick(#conn{credit = {trickle, Quantum, IntervalMs}, consumed = Consumed} = Conn) ->
    Conn1 = credit_connection(min(Quantum, Consumed), Conn#conn{tick_ref = undefined}),
    start_tick(IntervalMs, Conn1);
on_tick(Conn) ->
    Conn#conn{tick_ref = undefined}.

-spec start_tick(pos_integer(), #conn{}) -> #conn{}.
start_tick(_IntervalMs, #conn{tick_ref = Ref} = Conn) when Ref =/= undefined ->
    Conn;
start_tick(IntervalMs, Conn) ->
    Conn#conn{tick_ref = erlang:start_timer(IntervalMs, self(), tick)}.

-spec credit_stream(nhttp_lib:stream_id(), pos_integer(), #conn{}) -> #conn{}.
credit_stream(StreamId, Octets, #conn{h2 = H2, out = Out} = Conn) ->
    case nhttp_h2:send_window_update(H2, StreamId, Octets) of
        {ok, H2b, Frame} -> Conn#conn{h2 = H2b, out = [Out, Frame]};
        {error, _Reason} -> Conn
    end.

-spec flush(#conn{}) -> #conn{}.
flush(#conn{out = []} = Conn) ->
    Conn;
flush(#conn{closed = true} = Conn) ->
    Conn#conn{out = []};
flush(#conn{out = Out, sock = Sock} = Conn) ->
    case ssl:send(Sock, Out) of
        ok -> Conn#conn{out = []};
        {error, _Reason} -> Conn#conn{out = [], closed = true}
    end.

%%%-----------------------------------------------------------------------------
%%% FRAME RECORD
%%%-----------------------------------------------------------------------------

-doc """
Count the DATA frames in the raw bytes, ahead of the codec, so that a
case can assert how the client chunked a body. The codec coalesces the
payloads of one read into one event, which hides the frame boundaries.
""".
-spec count_frames(binary(), #conn{}) -> #conn{}.
count_frames(Data, #conn{raw = Raw, opts = Opts} = Conn) ->
    Settings = maps:get(settings, Opts, #{}),
    MaxFrameSize = maps:get(max_frame_size, Settings, ?DEFAULT_MAX_FRAME_SIZE),
    count_frames_loop(<<Raw/binary, Data/binary>>, MaxFrameSize, Conn).

-spec count_frames_loop(binary(), pos_integer(), #conn{}) -> #conn{}.
count_frames_loop(Bytes, MaxFrameSize, Conn) ->
    case nhttp_h2_frame:decode(Bytes, MaxFrameSize) of
        {ok, Frame, Consumed} ->
            <<_:Consumed/binary, Rest/binary>> = Bytes,
            count_frames_loop(Rest, MaxFrameSize, note_frame(Frame, Conn));
        {more, _Missing} ->
            Conn#conn{raw = Bytes};
        {error, _Reason} ->
            Conn#conn{raw = <<>>}
    end.

-spec note_frame(nhttp_h2_frame:t(), #conn{}) -> #conn{}.
note_frame({data, StreamId, _Fin, Payload}, #conn{stats = Stats} = Conn) ->
    #{data_frames := Frames, max_data_frame := Max} = Stats,
    Count = maps:get(StreamId, Frames, 0) + 1,
    Conn#conn{
        stats = Stats#{
            data_frames => Frames#{StreamId => Count},
            max_data_frame => max(Max, byte_size(Payload))
        }
    };
note_frame(_Frame, Conn) ->
    Conn.

-spec path_body_size(nhttp_lib:request()) -> undefined | non_neg_integer().
path_body_size(#{path := <<"/body/", Size/binary>>}) ->
    binary_to_integer(Size);
path_body_size(_Request) ->
    undefined.

-spec response_body_size(#stream{}, opts()) -> non_neg_integer().
response_body_size(#stream{body = undefined}, Opts) ->
    maps:get(response_body, Opts, 0);
response_body_size(#stream{body = Size}, _Opts) ->
    Size.
