-module(nhttpc_h2_peer).

-moduledoc """
A TLS listener that speaks just enough HTTP/2 to complete the client's
handshake: it negotiates `h2` over ALPN, sends one SETTINGS frame on every
accepted connection, and then sends more frames on request. It never
reads the socket and never credits on its own, so every connection to it
starves for send credit once the initial window is spent. A test drives
credit with `credit/2` and a GOAWAY with `goaway/4`.

`nhttp` merges its own defaults into every SETTINGS frame it sends, so it
cannot leave `max_concurrent_streams` out, cannot send a second frame on
demand, and credits both windows for every octet it reads. All three are
needed here.
""".

-export([
    credit/2,
    goaway/4,
    send_settings/2,
    start/2,
    stop/1
]).

-define(ACCEPT_TIMEOUT, 10000).
-define(IDLE_TIMEOUT, 30000).

-spec start([tuple()], nhttp_h2:settings()) -> {pid(), inet:port_number()}.
start(Config, Settings) ->
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
    Peer = spawn_link(fun() -> controller(LSock, Settings) end),
    {Peer, Port}.

-doc "Grant `Increment` octets of connection-level send credit on every connection.".
-spec credit(pid(), pos_integer()) -> ok.
credit(Peer, Increment) ->
    {ok, Frame} = nhttp_h2_frame:window_update(Increment),
    Peer ! {send_frame, Frame},
    ok.

-doc "Send a GOAWAY frame with debug data on every connection.".
-spec goaway(pid(), nhttp_lib:stream_id(), nhttp_h2:error_code(), iodata()) -> ok.
goaway(Peer, LastStreamId, ErrorCode, DebugData) ->
    {ok, Frame} = nhttp_h2_frame:goaway(LastStreamId, ErrorCode, DebugData),
    Peer ! {send_frame, Frame},
    ok.

-spec send_settings(pid(), nhttp_h2:settings()) -> ok.
send_settings(Peer, Settings) ->
    {ok, Frame} = nhttp_h2_frame:settings(Settings),
    Peer ! {send_frame, Frame},
    ok.

-spec stop(pid()) -> ok.
stop(Peer) ->
    Peer ! stop,
    ok.

-spec controller(ssl:sslsocket(), nhttp_h2:settings()) -> ok.
controller(LSock, Settings) ->
    process_flag(trap_exit, true),
    Self = self(),
    Acceptor = spawn_link(fun() -> acceptor(LSock, Settings, Self) end),
    controller_loop(LSock, Acceptor, []).

-spec controller_loop(ssl:sslsocket(), pid(), [pid()]) -> ok.
controller_loop(LSock, Acceptor, Conns) ->
    receive
        {accepted, Conn} ->
            controller_loop(LSock, Acceptor, [Conn | Conns]);
        {send_frame, _} = Msg ->
            lists:foreach(fun(Conn) -> Conn ! Msg end, Conns),
            controller_loop(LSock, Acceptor, Conns);
        {'EXIT', Acceptor, _} ->
            controller_loop(LSock, Acceptor, Conns);
        {'EXIT', Conn, _} ->
            controller_loop(LSock, Acceptor, lists:delete(Conn, Conns));
        stop ->
            lists:foreach(fun(Conn) -> Conn ! stop end, Conns),
            exit(Acceptor, kill),
            ok = ssl:close(LSock)
    end.

-spec acceptor(ssl:sslsocket(), nhttp_h2:settings(), pid()) -> ok.
acceptor(LSock, Settings, Controller) ->
    case ssl:transport_accept(LSock, ?ACCEPT_TIMEOUT) of
        {ok, TSock} ->
            case ssl:handshake(TSock, ?ACCEPT_TIMEOUT) of
                {ok, Sock} ->
                    Conn = spawn_link(fun() -> conn_init(Sock, Settings) end),
                    ok = ssl:controlling_process(Sock, Conn),
                    Controller ! {accepted, Conn},
                    acceptor(LSock, Settings, Controller);
                {error, _} ->
                    acceptor(LSock, Settings, Controller)
            end;
        {error, timeout} ->
            acceptor(LSock, Settings, Controller);
        {error, _} ->
            ok
    end.

-spec conn_init(ssl:sslsocket(), nhttp_h2:settings()) -> ok.
conn_init(Sock, Settings) ->
    {ok, Frame} = nhttp_h2_frame:settings(Settings),
    ok = ssl:send(Sock, Frame),
    conn_loop(Sock).

-spec conn_loop(ssl:sslsocket()) -> ok.
conn_loop(Sock) ->
    receive
        {send_frame, Frame} ->
            ok = ssl:send(Sock, Frame),
            conn_loop(Sock);
        stop ->
            ok = ssl:close(Sock)
    after ?IDLE_TIMEOUT ->
        ok = ssl:close(Sock)
    end.
