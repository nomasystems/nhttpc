-module(nhttpc_ws).

-moduledoc """
WebSocket client connection process (RFC 6455).

Implementation layer beneath `nhttpc:ws_open/2` / `nhttpc:ws_send/2`.
Messages reach the owner through the unified `{nhttpc, Handle, Event}`
envelope; no parallel tag stream is emitted.
""".

%%%-----------------------------------------------------------------------------
%% INCLUDES
%%%-----------------------------------------------------------------------------
-include_lib("kernel/include/logger.hrl").

%%%-----------------------------------------------------------------------------
%% API EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    start_link/2
]).

%%%-----------------------------------------------------------------------------
%% INTERNAL EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    format_status/1,
    init/3,
    system_code_change/4,
    system_continue/3,
    system_get_state/1,
    system_terminate/4
]).

%%%-----------------------------------------------------------------------------
%% TYPE EXPORTS
%%%-----------------------------------------------------------------------------
-export_type([
    conn_info/0,
    opts/0,
    uri/0
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-type uri() :: binary() | string().

-type opts() :: #{
    owner => pid(),
    protocols => [binary()],
    headers => [{binary(), binary()}],
    transport => tcp | ssl,
    connect_timeout => timeout(),
    idle_timeout => timeout(),
    ping_interval => pos_integer() | undefined,
    ssl_opts => [ssl:tls_client_option()],
    handle_ref => reference()
}.

-type conn_info() :: #{
    protocol => binary() | undefined,
    extensions => [binary()],
    transport => tcp | ssl
}.

-type parsed_uri() :: #{
    transport := tcp | ssl,
    host := binary(),
    port := inet:port_number(),
    path := binary()
}.

-type after_messages() :: open | {protocol_error, nhttp_ws:decode_error()}.

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(DEFAULT_CONNECT_TIMEOUT, 5000).
-define(DEFAULT_IDLE_TIMEOUT, 60000).
-define(CLOSE_TIMEOUT, 5000).
-define(WS_VERSION, <<"13">>).

%%%-----------------------------------------------------------------------------
%% RECORDS
%%%-----------------------------------------------------------------------------
-record(state, {
    socket :: nhttp_sock:t(),
    transport :: tcp | ssl,
    owner :: pid(),
    owner_ref :: reference(),
    idle_timeout :: timeout(),
    ping_interval :: pos_integer() | undefined,
    ping_timer :: reference() | undefined,
    ping_outstanding = 0 :: non_neg_integer(),
    decoder :: nhttp_ws:ws_decoder(),
    conn_info :: conn_info(),
    handle :: nhttpc_handle:handle() | undefined,
    remote_close_code :: 0..65535 | undefined
}).

%%%-----------------------------------------------------------------------------
%% API FUNCTIONS
%%%-----------------------------------------------------------------------------
-doc false.
-spec start_link(uri(), opts()) -> {ok, pid(), conn_info()} | {error, nhttpc_error:any_reason()}.
start_link(URI, Opts) ->
    proc_lib:start_link(?MODULE, init, [self(), URI, Opts]).

%%%-----------------------------------------------------------------------------
%% INTERNAL EXPORTS (PROC_LIB + SYS)
%%%-----------------------------------------------------------------------------
-spec build_handle(opts()) -> nhttpc_handle:handle() | undefined.
build_handle(#{handle_ref := Ref}) when is_reference(Ref) ->
    nhttpc_handle:new(self(), Ref, ws);
build_handle(_Opts) ->
    undefined.

-spec emit_ws_ready(#state{}, conn_info()) -> ok.
emit_ws_ready(#state{handle = undefined}, _ConnInfo) ->
    ok;
emit_ws_ready(#state{handle = Handle, owner = Owner}, ConnInfo) ->
    Owner ! {nhttpc, Handle, {ws_ready, ConnInfo}},
    ok.

-doc false.
-spec format_status(map()) -> map().
format_status(Status) ->
    case Status of
        #{state := #state{} = State} ->
            Status#{state := sanitize_state(State)};
        _ ->
            Status
    end.

-doc false.
-spec init(pid(), uri(), opts()) -> no_return().
init(Parent, URI, Opts) ->
    Debug = sys:debug_options([]),
    Owner = maps:get(owner, Opts, Parent),
    ConnectTimeout = maps:get(connect_timeout, Opts, ?DEFAULT_CONNECT_TIMEOUT),
    case do_connect(URI, Opts, ConnectTimeout) of
        {ok, Socket, Transport, ConnInfo} ->
            OwnerRef = erlang:monitor(process, Owner),
            IdleTimeout = maps:get(idle_timeout, Opts, ?DEFAULT_IDLE_TIMEOUT),
            PingInterval = maps:get(ping_interval, Opts, undefined),
            Handle = build_handle(Opts),
            ok = nhttp_sock:setopts(Socket, [{active, once}]),
            State = #state{
                socket = Socket,
                transport = Transport,
                owner = Owner,
                owner_ref = OwnerRef,
                idle_timeout = IdleTimeout,
                ping_interval = PingInterval,
                decoder = nhttp_ws:decoder_new(client),
                conn_info = ConnInfo,
                handle = Handle
            },
            proc_lib:init_ack(Parent, {ok, self(), ConnInfo}),
            emit_ws_ready(State, ConnInfo),
            PingState = maybe_start_ping_timer(State),
            open_loop(Parent, Debug, PingState);
        {error, Reason} ->
            proc_lib:init_fail(Parent, {error, Reason}, {exit, normal})
    end.

-spec is_set(undefined | term()) -> boolean().
is_set(undefined) -> false;
is_set(_) -> true.

-spec sanitize_state(#state{}) -> map().
sanitize_state(#state{} = S) ->
    #{
        transport => S#state.transport,
        owner => S#state.owner,
        socket => is_set(S#state.socket),
        frame_pending => nhttp_ws:frame_pending(S#state.decoder),
        idle_timeout => S#state.idle_timeout,
        ping_interval => S#state.ping_interval,
        ping_timer => is_set(S#state.ping_timer),
        ping_outstanding => S#state.ping_outstanding,
        decoder => is_set(S#state.decoder),
        conn_info => S#state.conn_info
    }.

-doc false.
-spec system_code_change(#state{}, module(), term(), term()) -> {ok, #state{}}.
system_code_change(State, _Module, _OldVsn, _Extra) ->
    {ok, State}.

-doc false.
-spec system_continue(pid(), [sys:debug_option()], #state{}) -> no_return().
system_continue(Parent, Debug, State) ->
    open_loop(Parent, Debug, State).

-doc false.
-spec system_get_state(#state{}) -> {ok, #state{}}.
system_get_state(State) ->
    {ok, State}.

-doc false.
-spec system_terminate(term(), pid(), [sys:debug_option()], #state{}) -> no_return().
system_terminate(Reason, _Parent, _Debug, State) ->
    terminate(Reason, State).

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - CONNECTION
%%%-----------------------------------------------------------------------------
-spec build_connect_opts(tcp | ssl, opts()) -> nhttp_sock:connect_opts().
build_connect_opts(tcp, _Opts) ->
    #{transport => tcp};
build_connect_opts(ssl, Opts) ->
    UserSslOpts = maps:get(ssl_opts, Opts, []),
    BaseSslOpts = #{
        transport => ssl,
        alpn_advertised_protocols => [<<"http/1.1">>],
        verify => verify_peer,
        versions => ['tlsv1.2', 'tlsv1.3']
    },
    case UserSslOpts of
        [] -> BaseSslOpts;
        _ -> lists:foldl(fun({K, V}, Acc) -> Acc#{K => V} end, BaseSslOpts, UserSslOpts)
    end.

-spec build_host_header(binary(), inet:port_number(), tcp | ssl) -> binary().
build_host_header(Host, 80, tcp) -> Host;
build_host_header(Host, 443, ssl) -> Host;
build_host_header(Host, Port, _) -> <<Host/binary, ":", (integer_to_binary(Port))/binary>>.

-spec build_path(binary(), binary() | undefined) -> binary().
build_path(<<>>, undefined) -> <<"/">>;
build_path(<<>>, Query) -> <<"/?", Query/binary>>;
build_path(Path, undefined) -> Path;
build_path(Path, Query) -> <<Path/binary, "?", Query/binary>>.

-spec build_upgrade_request(binary(), binary(), binary(), [{binary(), binary()}], opts()) ->
    iodata().
build_upgrade_request(Host, Path, Key, ExtraHeaders, Opts) ->
    Protocols = maps:get(protocols, Opts, []),
    BaseHeaders = [
        [<<"GET ">>, Path, <<" HTTP/1.1\r\n">>],
        [<<"Host: ">>, Host, <<"\r\n">>],
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        [<<"Sec-WebSocket-Key: ">>, Key, <<"\r\n">>],
        [<<"Sec-WebSocket-Version: ">>, ?WS_VERSION, <<"\r\n">>]
    ],
    ProtocolHeader =
        case Protocols of
            [] ->
                [];
            _ ->
                Joined = lists:join(<<", ">>, Protocols),
                [[<<"Sec-WebSocket-Protocol: ">>, Joined, <<"\r\n">>]]
        end,
    UserHeaders = [[Name, <<": ">>, Value, <<"\r\n">>] || {Name, Value} <- ExtraHeaders],
    [BaseHeaders, ProtocolHeader, UserHeaders, <<"\r\n">>].

-spec deadline(timeout()) -> integer() | infinity.
deadline(infinity) -> infinity;
deadline(Timeout) -> erlang:monotonic_time(millisecond) + Timeout.

-spec do_connect(uri(), opts(), timeout()) ->
    {ok, nhttp_sock:t(), tcp | ssl, conn_info()} | {error, nhttpc_error:any_reason()}.
do_connect(URI, Opts, Timeout) ->
    maybe
        {ok, Parsed} ?= parse_ws_uri(URI),
        #{transport := UriTransport, host := Host, port := Port, path := Path} = Parsed,
        Transport = maps:get(transport, Opts, UriTransport),
        ConnectOpts = build_connect_opts(Transport, Opts),
        Deadline = deadline(Timeout),
        {ok, Socket} ?= nhttp_sock:connect(Host, Port, ConnectOpts, Timeout),
        Key = nhttp_ws:generate_key(),
        HostHeader = build_host_header(Host, Port, Transport),
        ExtraHeaders = maps:get(headers, Opts, []),
        Request = build_upgrade_request(HostHeader, Path, Key, ExtraHeaders, Opts),
        ok ?= nhttp_sock:send(Socket, Request),
        RemainingTimeout = remaining_timeout(Deadline),
        case recv_handshake_response(Socket, <<>>, RemainingTimeout) of
            {ok, Status, Headers, _Rest} ->
                case validate_handshake_response(Status, Headers, Key, Opts) of
                    {ok, ConnInfo} ->
                        {ok, Socket, Transport, ConnInfo};
                    {error, Reason} ->
                        nhttp_sock:close(Socket),
                        {error, Reason}
                end;
            {error, Reason} ->
                nhttp_sock:close(Socket),
                {error, Reason}
        end
    end.

-spec get_response_accept([{binary(), binary()}]) ->
    {ok, binary()} | {error, missing_accept}.
get_response_accept(Headers) ->
    case nhttp_headers:get(<<"sec-websocket-accept">>, Headers) of
        undefined -> {error, missing_accept};
        Value -> {ok, Value}
    end.

-spec parse_ws_uri(uri()) -> {ok, parsed_uri()} | {error, invalid_ws_uri}.
parse_ws_uri(URI) when is_list(URI) ->
    parse_ws_uri(list_to_binary(URI));
parse_ws_uri(URI) when is_binary(URI) ->
    case uri_string:parse(URI) of
        #{scheme := <<"ws">>, host := Host} = Parsed ->
            Port = maps:get(port, Parsed, 80),
            Path = maps:get(path, Parsed, <<>>),
            Query = maps:get(query, Parsed, undefined),
            {ok, #{
                transport => tcp,
                host => Host,
                port => Port,
                path => build_path(Path, Query)
            }};
        #{scheme := <<"wss">>, host := Host} = Parsed ->
            Port = maps:get(port, Parsed, 443),
            Path = maps:get(path, Parsed, <<>>),
            Query = maps:get(query, Parsed, undefined),
            {ok, #{
                transport => ssl,
                host => Host,
                port => Port,
                path => build_path(Path, Query)
            }};
        _ ->
            {error, invalid_ws_uri}
    end.

-spec recv_handshake_response(nhttp_sock:t(), binary(), timeout()) ->
    {ok, non_neg_integer(), [{binary(), binary()}], binary()} | {error, nhttpc_error:any_reason()}.
recv_handshake_response(Socket, Buffer, Timeout) ->
    case nhttp_h1:parse_response_headers(Buffer, nhttpc_request:h1_parse_opts(#{})) of
        {ok, Status, Headers, Rest} ->
            {ok, Status, Headers, Rest};
        {more, _} ->
            case nhttp_sock:recv(Socket, 0, Timeout) of
                {ok, Data} ->
                    recv_handshake_response(Socket, <<Buffer/binary, Data/binary>>, Timeout);
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec remaining_timeout(integer() | infinity) -> timeout().
remaining_timeout(infinity) -> infinity;
remaining_timeout(Deadline) -> max(0, Deadline - erlang:monotonic_time(millisecond)).

-spec validate_handshake_response(
    non_neg_integer(), [{binary(), binary()}], binary(), opts()
) -> {ok, conn_info()} | {error, nhttpc_error:any_reason()}.
validate_handshake_response(101, Headers, Key, Opts) ->
    maybe
        ok ?= validate_response_upgrade(Headers),
        ok ?= validate_response_connection(Headers),
        {ok, Accept} ?= get_response_accept(Headers),
        ok ?= nhttp_ws:validate_accept(Key, Accept),
        Protocol = validate_response_protocol(Headers, Opts),
        {ok, #{
            protocol => Protocol,
            extensions => [],
            transport => maps:get(transport, Opts, tcp)
        }}
    end;
validate_handshake_response(Status, _Headers, _Key, _Opts) ->
    {error, {unexpected_status, Status}}.

-spec validate_response_connection([{binary(), binary()}]) ->
    ok | {error, invalid_connection}.
validate_response_connection(Headers) ->
    case nhttp_headers:get(<<"connection">>, Headers) of
        undefined ->
            {error, invalid_connection};
        Value ->
            LowerValue = nhttp_headers:to_lower(Value),
            Tokens = binary:split(LowerValue, <<",">>, [global, trim_all]),
            Trimmed = [string:trim(T) || T <- Tokens],
            case lists:member(<<"upgrade">>, Trimmed) of
                true -> ok;
                false -> {error, invalid_connection}
            end
    end.

-spec validate_response_protocol([{binary(), binary()}], opts()) -> binary() | undefined.
validate_response_protocol(Headers, Opts) ->
    Offered = maps:get(protocols, Opts, []),
    case nhttp_headers:get(<<"sec-websocket-protocol">>, Headers) of
        undefined ->
            undefined;
        Selected ->
            case lists:member(Selected, Offered) of
                true -> Selected;
                false -> undefined
            end
    end.

-spec validate_response_upgrade([{binary(), binary()}]) ->
    ok | {error, invalid_upgrade}.
validate_response_upgrade(Headers) ->
    case nhttp_headers:get(<<"upgrade">>, Headers) of
        undefined ->
            {error, invalid_upgrade};
        Value ->
            case nhttp_headers:to_lower(Value) of
                <<"websocket">> -> ok;
                _ -> {error, invalid_upgrade}
            end
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - ENVELOPE EMISSION
%%%-----------------------------------------------------------------------------
-spec emit_envelope_ws(#state{}, nhttp_ws:ws_message()) -> #state{}.
emit_envelope_ws(#state{handle = undefined} = State, _Message) ->
    State;
emit_envelope_ws(#state{handle = Handle, owner = Owner} = State, Message) ->
    case envelope_frame(Message) of
        {frame, Frame} ->
            Owner ! {nhttpc, Handle, {ws, Frame}},
            State;
        {remote_close, Code} ->
            State#state{remote_close_code = Code}
    end.

-spec emit_terminal_for(#state{}, terminal_reason()) -> #state{}.
emit_terminal_for(State, Reason) ->
    emit_terminal(State, terminal_event(State, Reason)).

-spec envelope_frame(nhttp_ws:ws_message()) ->
    {frame, nhttp_lib:ws_frame()} | {remote_close, 0..65535 | undefined}.
envelope_frame({text, _} = F) -> {frame, F};
envelope_frame({binary, _} = F) -> {frame, F};
envelope_frame({ping, _} = F) -> {frame, F};
envelope_frame({pong, _} = F) -> {frame, F};
envelope_frame(ping) -> {frame, {ping, <<>>}};
envelope_frame(pong) -> {frame, {pong, <<>>}};
envelope_frame({close, Code, _Reason}) -> {remote_close, Code};
envelope_frame(close) -> {remote_close, undefined}.

-spec terminal_event(#state{}, terminal_reason()) -> nhttpc:event().
terminal_event(#state{remote_close_code = Code}, normal) when Code =/= undefined ->
    {done, {remote_closed, Code}};
terminal_event(#state{remote_close_code = Code}, remote_closed) ->
    {done, {remote_closed, Code}};
terminal_event(_State, normal) ->
    {done, normal};
terminal_event(_State, close_timeout) ->
    {error, {timeout, idle}};
terminal_event(_State, idle_timeout) ->
    {error, {timeout, idle}};
terminal_event(_State, ping_timeout) ->
    {error, {timeout, idle}};
terminal_event(_State, protocol_error) ->
    {error, {stream_error, {protocol, undefined}}};
terminal_event(_State, {socket_error, Reason}) ->
    {error, {stream_error, {protocol, Reason}}}.

-type terminal_reason() ::
    normal
    | remote_closed
    | close_timeout
    | idle_timeout
    | ping_timeout
    | protocol_error
    | {socket_error, term()}.
-spec emit_terminal(#state{}, nhttpc:event()) -> #state{}.
emit_terminal(#state{handle = undefined} = State, _Event) ->
    State;
emit_terminal(#state{handle = Handle, owner = Owner} = State, Event) ->
    Owner ! {nhttpc, Handle, Event},
    State#state{handle = undefined}.

%%%-----------------------------------------------------------------------------
%% INTERNAL FUNCTIONS - MESSAGE LOOPS
%%%-----------------------------------------------------------------------------
-spec cancel_ping_timer(#state{}) -> #state{}.
cancel_ping_timer(#state{ping_timer = undefined} = State) ->
    State;
cancel_ping_timer(#state{ping_timer = Ref} = State) ->
    erlang:cancel_timer(Ref),
    State#state{ping_timer = undefined}.

-spec closing_loop(pid(), [sys:debug_option()], #state{}) -> no_return().
closing_loop(Parent, Debug, #state{socket = Socket} = State) ->
    receive
        {tcp, _Socket, Data} ->
            handle_closing_data(Parent, Debug, State, Data);
        {ssl, _Socket, Data} ->
            handle_closing_data(Parent, Debug, State, Data);
        {tcp_closed, _} ->
            State1 = emit_terminal_for(State, normal),
            terminate(normal, State1);
        {ssl_closed, _} ->
            State1 = emit_terminal_for(State, normal),
            terminate(normal, State1);
        {tcp_error, _, Reason} ->
            State1 = emit_terminal_for(State, {socket_error, Reason}),
            terminate({socket_error, Reason}, State1);
        {ssl_error, _, Reason} ->
            State1 = emit_terminal_for(State, {socket_error, Reason}),
            terminate({socket_error, Reason}, State1);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
        {'EXIT', Parent, Reason} ->
            terminate(Reason, State)
    after ?CLOSE_TIMEOUT ->
        nhttp_sock:close(Socket),
        State1 = emit_terminal_for(State, close_timeout),
        terminate(close_timeout, State1)
    end.

-spec finish_close(#state{}) -> no_return().
finish_close(State) ->
    State1 = emit_terminal_for(State, normal),
    terminate(normal, State1).

-spec handle_client_data(pid(), [sys:debug_option()], #state{}, binary()) -> no_return().
handle_client_data(Parent, Debug, #state{decoder = Dec} = State, Data) ->
    case nhttp_ws:feed(Data, Dec) of
        {ok, Messages, NewDec} ->
            handle_messages(Messages, open, Parent, Debug, State#state{decoder = NewDec});
        {error, Reason, Messages} ->
            handle_messages(Messages, {protocol_error, Reason}, Parent, Debug, State)
    end.

-spec handle_closing_data(pid(), [sys:debug_option()], #state{}, binary()) -> no_return().
handle_closing_data(Parent, Debug, #state{decoder = Dec} = State, Data) ->
    case nhttp_ws:feed(Data, Dec) of
        {ok, Messages, NewDec} ->
            case has_close(Messages) of
                true ->
                    finish_close(State);
                false ->
                    ok = nhttp_sock:setopts(State#state.socket, [{active, once}]),
                    closing_loop(Parent, Debug, State#state{decoder = NewDec})
            end;
        {error, _Reason, Messages} ->
            case has_close(Messages) of
                true ->
                    finish_close(State);
                false ->
                    State1 = emit_terminal_for(State, protocol_error),
                    terminate(protocol_error, State1)
            end
    end.

-spec handle_message(nhttp_ws:ws_message(), #state{}) -> {open | closing, #state{}}.
handle_message(ping, State) ->
    nhttp_sock:send(State#state.socket, nhttp_ws:encode_masked({pong, <<>>})),
    {open, emit_envelope_ws(State, ping)};
handle_message({ping, PingData}, State) ->
    nhttp_sock:send(State#state.socket, nhttp_ws:encode_masked({pong, PingData})),
    {open, emit_envelope_ws(State, {ping, PingData})};
handle_message(pong, State) ->
    NewState = State#state{ping_outstanding = max(0, State#state.ping_outstanding - 1)},
    {open, emit_envelope_ws(NewState, pong)};
handle_message({pong, PongData}, State) ->
    NewState = State#state{ping_outstanding = max(0, State#state.ping_outstanding - 1)},
    {open, emit_envelope_ws(NewState, {pong, PongData})};
handle_message(close, State) ->
    nhttp_sock:send(State#state.socket, nhttp_ws:encode_masked_close()),
    {closing, emit_envelope_ws(State, close)};
handle_message({close, Code, Reason}, State) ->
    nhttp_sock:send(State#state.socket, nhttp_ws:encode_masked_close(Code, <<>>)),
    {closing, emit_envelope_ws(State, {close, Code, Reason})};
handle_message(Message, State) ->
    {open, emit_envelope_ws(State, Message)}.

-doc """
Act on the messages of one read in wire order, then apply `Then`. A close
ends the batch: the messages after it are dropped (RFC 6455 Section 5.5.1).
""".
-spec handle_messages(
    [nhttp_ws:ws_message()], after_messages(), pid(), [sys:debug_option()], #state{}
) -> no_return().
handle_messages([], open, Parent, Debug, #state{socket = Socket} = State) ->
    ok = nhttp_sock:setopts(Socket, [{active, once}]),
    open_loop(Parent, Debug, State);
handle_messages([], {protocol_error, Reason}, _Parent, _Debug, #state{socket = Socket} = State) ->
    nhttp_sock:send(Socket, nhttp_ws:encode_masked_close(1002, <<"Protocol error">>)),
    State1 = emit_terminal_for(State, protocol_error),
    terminate({protocol_error, Reason}, State1);
handle_messages([Message | Rest], Then, Parent, Debug, State) ->
    case handle_message(Message, State) of
        {open, State1} ->
            handle_messages(Rest, Then, Parent, Debug, State1);
        {closing, State1} ->
            ok = nhttp_sock:setopts(State1#state.socket, [{active, once}]),
            closing_loop(Parent, Debug, cancel_ping_timer(State1))
    end.

-spec has_close([nhttp_ws:ws_message()]) -> boolean().
has_close([]) -> false;
has_close([close | _]) -> true;
has_close([{close, _Code, _Reason} | _]) -> true;
has_close([_ | Rest]) -> has_close(Rest).

-spec maybe_start_ping_timer(#state{}) -> #state{}.
maybe_start_ping_timer(#state{ping_interval = undefined} = State) ->
    State;
maybe_start_ping_timer(#state{ping_interval = Interval} = State) ->
    Ref = erlang:send_after(Interval, self(), ws_ping_tick),
    State#state{ping_timer = Ref}.

-spec open_loop(pid(), [sys:debug_option()], #state{}) -> no_return().
open_loop(Parent, Debug, #state{idle_timeout = IdleTimeout} = State) ->
    receive
        {tcp, _Socket, Data} ->
            handle_client_data(Parent, Debug, State, Data);
        {ssl, _Socket, Data} ->
            handle_client_data(Parent, Debug, State, Data);
        {ws_send, Message} ->
            Frame = nhttp_ws:encode_masked(Message),
            nhttp_sock:send(State#state.socket, Frame),
            open_loop(Parent, Debug, State);
        {ws_close, Code, Reason} ->
            Frame = nhttp_ws:encode_masked_close(Code, Reason),
            nhttp_sock:send(State#state.socket, Frame),
            closing_loop(Parent, Debug, cancel_ping_timer(State));
        {ws_info, From, Ref} ->
            From ! {ws_info_reply, Ref, State#state.conn_info},
            open_loop(Parent, Debug, State);
        {tcp_closed, _} ->
            State1 = emit_terminal_for(State, remote_closed),
            terminate(normal, State1);
        {ssl_closed, _} ->
            State1 = emit_terminal_for(State, remote_closed),
            terminate(normal, State1);
        {tcp_error, _, Reason} ->
            State1 = emit_terminal_for(State, {socket_error, Reason}),
            terminate({socket_error, Reason}, State1);
        {ssl_error, _, Reason} ->
            State1 = emit_terminal_for(State, {socket_error, Reason}),
            terminate({socket_error, Reason}, State1);
        {'DOWN', Ref, process, Owner, _Reason} when
            Ref =:= State#state.owner_ref, Owner =:= State#state.owner
        ->
            Frame = nhttp_ws:encode_masked_close(1001, <<"Owner terminated">>),
            nhttp_sock:send(State#state.socket, Frame),
            terminate(normal, State);
        ws_ping_tick ->
            case State#state.ping_outstanding >= 2 of
                true ->
                    Frame = nhttp_ws:encode_masked_close(1001, <<"Ping timeout">>),
                    nhttp_sock:send(State#state.socket, Frame),
                    State1 = emit_terminal_for(State, ping_timeout),
                    terminate(ping_timeout, State1);
                false ->
                    nhttp_sock:send(State#state.socket, nhttp_ws:encode_masked_ping()),
                    NewState = State#state{
                        ping_outstanding = State#state.ping_outstanding + 1
                    },
                    open_loop(Parent, Debug, maybe_start_ping_timer(NewState))
            end;
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
        {'EXIT', Parent, Reason} ->
            terminate(Reason, State)
    after IdleTimeout ->
        Frame = nhttp_ws:encode_masked_close(1001, <<"Idle timeout">>),
        nhttp_sock:send(State#state.socket, Frame),
        State1 = emit_terminal_for(State, idle_timeout),
        terminate(idle_timeout, State1)
    end.

-spec terminate(term(), #state{}) -> no_return().
terminate(Reason, #state{socket = Socket} = State) ->
    _ = cancel_ping_timer(State),
    ok = nhttp_sock:close(Socket),
    exit(Reason).
