-module(nhttpc_proxy).

-moduledoc """
HTTP proxy support for nhttpc.

Implements HTTP CONNECT tunneling for proxying HTTPS connections
through HTTP proxies. Supports Basic authentication.

Flow:

1. Connect to proxy server (TCP)
2. Send CONNECT request to target host:port
3. Receive 200 response from proxy
4. Upgrade to TLS if target is HTTPS
5. Return socket for normal HTTP/HTTPS communication
""".

-export([
    connect/4,
    connect/5
]).

-export_type([
    proxy_opts/0
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-type proxy_opts() :: #{
    host := binary() | string() | inet:ip_address(),
    port := inet:port_number(),
    username => binary(),
    password => binary()
}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc """
Connect to target through HTTP proxy.

Establishes a TCP connection to the proxy, sends CONNECT request,
and optionally upgrades to TLS for HTTPS targets.
""".
-spec connect(
    TargetHost :: binary(),
    TargetPort :: inet:port_number(),
    ProxyOpts :: proxy_opts(),
    Timeout :: timeout()
) ->
    {ok, gen_tcp:socket()} | {error, nhttpc_error:any_reason()}.
connect(TargetHost, TargetPort, ProxyOpts, Timeout) ->
    connect(TargetHost, TargetPort, ProxyOpts, #{}, Timeout).

-doc """
Connect to target through HTTP proxy with TLS options.
If SslOpts is non-empty, the connection will be upgraded to TLS
after the CONNECT tunnel is established.
""".
-spec connect(
    TargetHost :: binary(),
    TargetPort :: inet:port_number(),
    ProxyOpts :: proxy_opts(),
    SslOpts :: map(),
    Timeout :: timeout()
) ->
    {ok, gen_tcp:socket() | ssl:sslsocket()} | {error, nhttpc_error:any_reason()}.
connect(TargetHost, TargetPort, ProxyOpts, SslOpts, Timeout) ->
    ProxyHost = normalize_host(maps:get(host, ProxyOpts)),
    ProxyPort = maps:get(port, ProxyOpts),

    Deadline = deadline(Timeout),

    maybe
        {ok, Socket} ?= connect_to_proxy(ProxyHost, ProxyPort, remaining_time(Deadline)),
        ok ?= send_connect_request(Socket, TargetHost, TargetPort, ProxyOpts),
        ok ?= recv_connect_response(Socket, remaining_time(Deadline)),
        upgrade_if_tls(Socket, TargetHost, SslOpts, remaining_time(Deadline))
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL
%%%-----------------------------------------------------------------------------
-spec build_authority(binary(), inet:port_number()) -> binary().
build_authority(Host, Port) ->
    <<Host/binary, ":", (integer_to_binary(Port))/binary>>.

-spec build_basic_auth(binary(), binary()) -> binary().
build_basic_auth(Username, Password) ->
    Credentials = <<Username/binary, ":", Password/binary>>,
    Encoded = base64:encode(Credentials),
    <<"Basic ", Encoded/binary>>.

-spec build_connect_headers(binary(), proxy_opts()) -> [{binary(), binary()}].
build_connect_headers(Authority, ProxyOpts) ->
    BaseHeaders = [
        {<<"Host">>, Authority},
        {<<"Proxy-Connection">>, <<"keep-alive">>}
    ],
    case maps:get(username, ProxyOpts, undefined) of
        undefined ->
            BaseHeaders;
        Username ->
            Password = maps:get(password, ProxyOpts, <<>>),
            AuthValue = build_basic_auth(Username, Password),
            [{<<"Proxy-Authorization">>, AuthValue} | BaseHeaders]
    end.

-spec build_connect_request(binary(), [{binary(), binary()}]) -> iolist().
build_connect_request(Authority, Headers) ->
    HeaderLines = [[Name, <<": ">>, Value, <<"\r\n">>] || {Name, Value} <- Headers],
    [<<"CONNECT ">>, Authority, <<" HTTP/1.1\r\n">>, HeaderLines, <<"\r\n">>].

-spec build_ssl_options(binary(), map()) -> [ssl:tls_client_option()].
build_ssl_options(TargetHost, SslOpts) ->
    Verify = maps:get(verify, SslOpts, verify_peer),
    SNI = maps:get(server_name_indication, SslOpts, binary_to_list(TargetHost)),

    BaseOpts = [
        {verify, Verify},
        {server_name_indication, SNI}
    ],

    CaOpts =
        case maps:get(cacerts, SslOpts, undefined) of
            undefined ->
                case maps:get(cacertfile, SslOpts, undefined) of
                    undefined when Verify =:= verify_peer ->
                        [{cacerts, public_key:cacerts_get()}];
                    undefined ->
                        [];
                    CaFile ->
                        [{cacertfile, CaFile}]
                end;
            CaCerts ->
                [{cacerts, CaCerts}]
        end,

    AlpnOpts =
        case maps:get(alpn_advertised_protocols, SslOpts, undefined) of
            undefined -> [];
            Protocols -> [{alpn_advertised_protocols, Protocols}]
        end,

    BaseOpts ++ CaOpts ++ AlpnOpts.

-spec connect_to_proxy(binary(), inet:port_number(), timeout()) ->
    {ok, gen_tcp:socket()} | {error, nhttpc_error:any_reason()}.
connect_to_proxy(ProxyHost, ProxyPort, Timeout) ->
    TcpOpts = [
        binary,
        {active, false},
        {packet, raw},
        {nodelay, true}
    ],
    Host = binary_to_list(ProxyHost),
    gen_tcp:connect(Host, ProxyPort, TcpOpts, Timeout).

-spec deadline(timeout()) -> integer() | infinity.
deadline(infinity) -> infinity;
deadline(Timeout) -> erlang:monotonic_time(millisecond) + Timeout.

-spec normalize_host(binary() | string() | inet:ip_address()) -> binary().
normalize_host(Host) when is_binary(Host) -> Host;
normalize_host(Host) when is_list(Host) -> list_to_binary(Host);
normalize_host(Host) when is_tuple(Host) -> list_to_binary(inet:ntoa(Host)).

-spec parse_header_line(binary()) -> {true, {binary(), binary()}} | false.
parse_header_line(<<>>) ->
    false;
parse_header_line(Line) ->
    case binary:split(Line, <<":">>) of
        [Name, Value] ->
            {true, {nhttp_headers:to_lower(string:trim(Name)), string:trim(Value)}};
        _ ->
            false
    end.

-spec parse_headers(binary()) -> nhttp_lib:headers().
parse_headers(<<>>) ->
    [];
parse_headers(HeaderLines) ->
    Lines = binary:split(HeaderLines, <<"\r\n">>, [global]),
    lists:filtermap(fun parse_header_line/1, Lines).

-spec parse_response(binary()) ->
    {ok, nhttp_lib:status(), nhttp_lib:headers(), binary()}
    | incomplete
    | {error, nhttpc_error:any_reason()}.
parse_response(Data) ->
    case binary:split(Data, <<"\r\n\r\n">>) of
        [_Incomplete] ->
            incomplete;
        [HeaderPart, Rest] ->
            case parse_status_line(HeaderPart) of
                {ok, Status, HeaderLines} ->
                    Headers = parse_headers(HeaderLines),
                    {ok, Status, Headers, Rest};
                {error, _} = Error ->
                    Error
            end
    end.

-spec parse_status(binary()) -> {ok, nhttp_lib:status()} | {error, nhttpc:error()}.
parse_status(StatusLine) ->
    case StatusLine of
        <<"HTTP/1.", _, " ", StatusCode:3/binary, _/binary>> ->
            try
                {ok, binary_to_integer(StatusCode)}
            catch
                error:badarg -> nhttpc_error:protocol_error(http1_1, bad_status_line)
            end;
        <<"HTTP/1.1 ", StatusCode:3/binary, _/binary>> ->
            try
                {ok, binary_to_integer(StatusCode)}
            catch
                error:badarg -> nhttpc_error:protocol_error(http1_1, bad_status_line)
            end;
        _ ->
            nhttpc_error:protocol_error(http1_1, bad_status_line)
    end.

-spec parse_status_line(binary()) ->
    {ok, nhttp_lib:status(), binary()} | {error, nhttpc_error:any_reason()}.
parse_status_line(HeaderPart) ->
    case binary:split(HeaderPart, <<"\r\n">>) of
        [StatusLine | HeaderLines] ->
            case parse_status(StatusLine) of
                {ok, Status} ->
                    {ok, Status, iolist_to_binary(lists:join(<<"\r\n">>, HeaderLines))};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {error, invalid_response}
    end.

-spec recv_connect_response(gen_tcp:socket(), timeout()) ->
    ok | {error, nhttpc_error:any_reason()}.
recv_connect_response(Socket, Timeout) ->
    case recv_http_response(Socket, <<>>, Timeout) of
        {ok, Status, _Headers} when Status >= 200, Status < 300 ->
            ok;
        {ok, 407, _Headers} ->
            {error, {proxy_auth_required, 407}};
        {ok, Status, _Headers} ->
            {error, {proxy_error, Status}};
        {error, _} = Error ->
            Error
    end.

-spec recv_http_response(gen_tcp:socket(), binary(), timeout()) ->
    {ok, nhttp_lib:status(), nhttp_lib:headers()} | {error, nhttpc_error:any_reason()}.
recv_http_response(Socket, Buffer, Timeout) ->
    case gen_tcp:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case parse_response(NewBuffer) of
                {ok, Status, Headers, _Rest} ->
                    {ok, Status, Headers};
                incomplete ->
                    recv_http_response(Socket, NewBuffer, Timeout);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

-spec remaining_time(integer() | infinity) -> timeout().
remaining_time(infinity) ->
    infinity;
remaining_time(Deadline) ->
    Remaining = Deadline - erlang:monotonic_time(millisecond),
    max(0, Remaining).

-spec send_connect_request(
    gen_tcp:socket(),
    binary(),
    inet:port_number(),
    proxy_opts()
) ->
    ok | {error, nhttpc_error:any_reason()}.
send_connect_request(Socket, TargetHost, TargetPort, ProxyOpts) ->
    Authority = build_authority(TargetHost, TargetPort),
    Headers = build_connect_headers(Authority, ProxyOpts),
    Request = build_connect_request(Authority, Headers),
    gen_tcp:send(Socket, Request).

-spec upgrade_if_tls(
    gen_tcp:socket(),
    binary(),
    map(),
    timeout()
) ->
    {ok, gen_tcp:socket() | ssl:sslsocket()} | {error, nhttpc_error:any_reason()}.
upgrade_if_tls(Socket, _TargetHost, SslOpts, _Timeout) when map_size(SslOpts) =:= 0 ->
    {ok, Socket};
upgrade_if_tls(Socket, TargetHost, SslOpts, Timeout) ->
    SslOptions = build_ssl_options(TargetHost, SslOpts),
    ssl:connect(Socket, SslOptions, Timeout).
