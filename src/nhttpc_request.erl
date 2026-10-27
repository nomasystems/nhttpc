-module(nhttpc_request).

-moduledoc """
Request execution for nhttpc client.

Extracted from nhttpc.erl to improve module cohesion.
This module handles:
- Request execution with retry and redirect handling
- HTTP/1.1 and HTTP/2 request/response handling
- Compression (Accept-Encoding, decompression)
- Cookie jar integration
""".

-include("nhttpc.hrl").

-export([
    do_request/3
]).

%% Shared helpers (for stream modules)
-export([
    add_host_header/4,
    build_connect_opts/2,
    detect_protocol/1,

    get_content_encoding/1,
    get_header_value/2,
    h1_parse_opts/1,
    maybe_add_accept_encoding/2,

    recv_h2_settings/3
]).

%% H3 helpers (for stream modules)
-export([
    execute_h3_actions/2,
    init_h3_streams/1,
    recv_h3_response/5
]).

%% Redirect helpers (for stream modules)
-export([
    url_info_to_url/1
]).

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(H2_LOCAL_INITIAL_WINDOW, 65535).

%%%-----------------------------------------------------------------------------
%% MAIN ENTRY POINT
%%%-----------------------------------------------------------------------------
-doc """
Byte size of a body. `undefined` when it cannot be known up front, as
for a streaming source: reporting 0 there would be indistinguishable from an
genuinely empty body and would drag the low percentiles of the size
histogram down.
""".
-spec body_length(term()) -> non_neg_integer() | undefined.
body_length(Body) when is_binary(Body) ->
    byte_size(Body);
body_length(Body) when is_list(Body) ->
    try
        iolist_size(Body)
    catch
        _:_ -> undefined
    end;
body_length(_Other) ->
    undefined.

-spec body_measurements(non_neg_integer() | undefined, non_neg_integer() | undefined) -> map().
body_measurements(ReqBodyLength, RespBodyLength) ->
    maps:filter(
        fun(_Key, Value) -> Value =/= undefined end,
        #{req_body_length => ReqBodyLength, resp_body_length => RespBodyLength}
    ).

-spec do_request(nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_request(#{method := Method} = Req, UrlInfo, Opts) ->
    TelemetryConfig = nhttpc_telemetry:config(Opts),
    case nhttpc_telemetry:enabled(TelemetryConfig) of
        false ->
            do_request_uninstrumented(Req, UrlInfo, Opts, Method);
        true ->
            do_request_instrumented(Req, UrlInfo, Opts, Method, TelemetryConfig)
    end.

-spec do_request_instrumented(
    nhttp_lib:request(),
    nhttpc_url:http_url_info(),
    nhttpc:opts(),
    nhttp_lib:method(),
    nhttpc_telemetry:otel_config()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_request_instrumented(Req, UrlInfo, Opts, Method, TelemetryConfig) ->
    ReqBodyLength = body_length(maps:get(body, Req, <<>>)),
    TelemetryMeta = telemetry_meta(Method, UrlInfo, Opts),
    StartTime = nhttpc_telemetry:client_request_start(TelemetryConfig, TelemetryMeta),
    RetryOpts = nhttpc_retry:normalize_opts(maps:get(retry, Opts, false), Method),
    try
        Result = do_request_with_retry(Req, UrlInfo, Opts, RetryOpts, 1),
        emit_request_end(TelemetryConfig, StartTime, TelemetryMeta, ReqBodyLength, Result),
        Result
    catch
        Class:Reason:Stacktrace ->
            emit_request_end(
                TelemetryConfig,
                StartTime,
                TelemetryMeta,
                ReqBodyLength,
                {error, {Class, Reason}}
            ),
            erlang:raise(Class, Reason, Stacktrace)
    end.

-spec do_request_uninstrumented(
    nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts(), nhttp_lib:method()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_request_uninstrumented(Req, UrlInfo, Opts, Method) ->
    RetryOpts = nhttpc_retry:normalize_opts(maps:get(retry, Opts, false), Method),
    do_request_with_retry(Req, UrlInfo, Opts, RetryOpts, 1).

-doc "Emit request end telemetry event.".
-spec emit_request_end(
    nhttpc_telemetry:otel_config(),
    integer(),
    nhttpc_telemetry:client_request_meta(),
    non_neg_integer() | undefined,
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}
) ->
    ok.
emit_request_end(Config, StartTime, Meta, ReqBodyLength, {ok, #{status := Status} = Resp}) ->
    Measurements = body_measurements(
        ReqBodyLength, body_length(maps:get(body, Resp, <<>>))
    ),
    nhttpc_telemetry:client_request_stop(
        Config, StartTime, Meta#{status => Status}, Measurements
    );
emit_request_end(Config, StartTime, Meta, ReqBodyLength, {error, Reason}) ->
    Measurements = body_measurements(ReqBodyLength, 0),
    nhttpc_telemetry:client_request_error(
        Config, StartTime, Meta#{reason => Reason}, Measurements
    ).

-doc """
Build the metadata shared by the request start, stop and error events.
A pooled request carries a path-only URL, so its authority comes from the
pool target rather than from the (dummy) url_info.
""".
-spec telemetry_meta(nhttp_lib:method(), nhttpc_url:http_url_info(), nhttpc:opts()) ->
    nhttpc_telemetry:client_request_meta().
telemetry_meta(Method, #{path := Path} = UrlInfo, Opts) ->
    PoolRef = telemetry_pool_ref(Opts),
    #{scheme := Scheme, host := Host, port := Port} = telemetry_target(PoolRef, UrlInfo),
    #{
        method => nhttp_lib:encode_method(Method),
        url => url_info_to_url(UrlInfo),
        scheme => Scheme,
        host => Host,
        port => Port,
        path => Path,
        pool => PoolRef
    }.

-spec telemetry_target(term() | undefined, nhttpc_url:http_url_info()) ->
    nhttpc_registry:target().
telemetry_target(undefined, UrlInfo) ->
    url_info_target(UrlInfo);
telemetry_target(PoolRef, UrlInfo) ->
    case nhttpc_pool:target(PoolRef) of
        undefined -> url_info_target(UrlInfo);
        Target -> Target
    end.

-spec url_info_target(nhttpc_url:http_url_info()) -> nhttpc_registry:target().
url_info_target(#{scheme := Scheme, host := Host, port := Port}) ->
    #{scheme => Scheme, host => Host, port => Port}.

%%%-----------------------------------------------------------------------------
%% INTERNAL: RETRY HANDLING
%%%-----------------------------------------------------------------------------
-doc "Execute request with retry logic.".
-spec do_request_with_retry(
    nhttp_lib:request(),
    nhttpc_url:http_url_info(),
    nhttpc:opts(),
    false | nhttpc_retry:retry_opts(),
    pos_integer()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_request_with_retry(Req, UrlInfo, Opts, RetryOpts, Attempt) ->
    Result = do_request_inner(Req, UrlInfo, Opts),
    case nhttpc_retry:should_retry(Result, Attempt, RetryOpts) of
        {true, Delay} ->
            timer:sleep(Delay),
            do_request_with_retry(Req, UrlInfo, Opts, RetryOpts, Attempt + 1);
        false ->
            maybe_retries_exhausted(Result, Attempt, RetryOpts)
    end.

-spec maybe_retries_exhausted(
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()},
    pos_integer(),
    false | nhttpc_retry:retry_opts()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
maybe_retries_exhausted({ok, _} = Ok, _Attempt, _RetryOpts) ->
    Ok;
maybe_retries_exhausted({error, _} = Error, Attempt, #{max_attempts := Max}) when
    Attempt >= Max, Attempt > 1
->
    {error, Canonical} = nhttpc_error:wrap(Error),
    nhttpc_error:retries_exhausted(Attempt, Canonical);
maybe_retries_exhausted(Result, _Attempt, _RetryOpts) ->
    Result.

%%%-----------------------------------------------------------------------------
%% INTERNAL: REDIRECT HANDLING
%%%-----------------------------------------------------------------------------
-doc "Add Host header value.".
-spec add_host_value(nhttp_lib:headers(), binary(), inet:port_number(), tcp | ssl) ->
    nhttp_lib:headers().
add_host_value(Headers, Host, Port, Transport) ->
    DefaultPort =
        case Transport of
            ssl -> 443;
            tcp -> 80
        end,
    HostValue =
        case Port of
            DefaultPort -> Host;
            _ -> <<Host/binary, ":", (integer_to_binary(Port))/binary>>
        end,
    [{<<"host">>, HostValue} | Headers].

-doc """
Adjust request for redirect based on status code.
RFC 9110 Section 15.4:

- 301/302: Method may change to GET (historically), except GET, HEAD and
  QUERY (RFC 10008 Section 2.5)
- 303: Always changes to GET
- 307/308: Method and body preserved
""".
-spec adjust_request_for_redirect(
    nhttp_lib:request(), nhttp_lib:status(), nhttpc_url:http_url_info(), nhttpc_url:http_url_info()
) -> nhttp_lib:request().
adjust_request_for_redirect(#{method := Method} = Req, Status, OldUrlInfo, NewUrlInfo) ->
    Req1 = apply_cross_origin_policy(Req, OldUrlInfo, NewUrlInfo),
    case Status of
        303 ->
            Req2 = Req1#{method => get, body => <<>>},
            update_request_path(Req2, NewUrlInfo);
        Status when Status =:= 301; Status =:= 302 ->
            case Method of
                get ->
                    update_request_path(Req1, NewUrlInfo);
                head ->
                    update_request_path(Req1, NewUrlInfo);
                query ->
                    update_request_path(Req1, NewUrlInfo);
                _ ->
                    Req2 = Req1#{method => get, body => <<>>},
                    update_request_path(Req2, NewUrlInfo)
            end;
        Status when Status =:= 307; Status =:= 308 ->
            update_request_path(Req1, NewUrlInfo);
        _ ->
            update_request_path(Req1, NewUrlInfo)
    end.

-doc """
Apply cross-origin security policy.
Strip sensitive headers when redirecting to a different origin.
""".
-spec apply_cross_origin_policy(
    nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc_url:http_url_info()
) -> nhttp_lib:request().
apply_cross_origin_policy(#{headers := Headers} = Req, FromUrl, ToUrl) ->
    case is_same_origin(FromUrl, ToUrl) of
        true ->
            Req;
        false ->
            FilteredHeaders = nhttp_headers:filter(
                fun(K, _V) -> not is_sensitive_header(K) end, Headers
            ),
            Req#{headers => FilteredHeaders}
    end.

-doc "Get base path (directory part).".
-spec base_path(binary()) -> binary().
base_path(Path) ->
    case binary:split(Path, <<"/">>, [global, trim_all]) of
        [] ->
            <<"/">>;
        Parts ->
            Init = lists:droplast(Parts),
            case Init of
                [] -> <<"/">>;
                _ -> iolist_to_binary([<<"/">>, lists:join(<<"/">>, Init), <<"/">>])
            end
    end.

-doc "Check custom redirect policy callback.".
-spec check_redirect_policy(
    nhttp_lib:status(), binary(), nhttp_lib:request(), nhttpc:opts()
) ->
    continue | stop | {continue, nhttpc:opts()}.
check_redirect_policy(Status, Location, Req, Opts) ->
    case maps:get(redirect_policy, Opts, undefined) of
        undefined ->
            continue;
        PolicyFun when is_function(PolicyFun, 4) ->
            PolicyFun(Status, Location, Req, Opts)
    end.

-doc "Execute request with redirect handling (called by retry wrapper).".
-spec do_request_inner(nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_request_inner(Req, UrlInfo, Opts) ->
    FollowRedirects = maps:get(follow_redirects, Opts, true),
    case FollowRedirects of
        true ->
            do_request_with_redirects(Req, UrlInfo, Opts, [], 0);
        false ->
            do_single_request(Req, UrlInfo, Opts)
    end.

-doc "Execute request with redirect following.".
-spec do_request_with_redirects(
    nhttp_lib:request(),
    nhttpc_url:http_url_info(),
    nhttpc:opts(),
    [binary()],
    non_neg_integer()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_request_with_redirects(Req, UrlInfo, Opts, VisitedUrls, RedirectCount) ->
    MaxRedirects = maps:get(max_redirects, Opts, 5),
    case RedirectCount >= MaxRedirects of
        true ->
            nhttp_error:max_redirects(MaxRedirects);
        false ->
            case do_single_request(Req, UrlInfo, Opts) of
                {ok, Resp} ->
                    case is_redirect(Resp) of
                        true ->
                            handle_redirect(Req, UrlInfo, Opts, Resp, VisitedUrls, RedirectCount);
                        false ->
                            {ok, Resp}
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

-doc "Follow redirect to new location.".
-spec follow_redirect(
    nhttp_lib:request(),
    nhttpc_url:http_url_info(),
    nhttpc:opts(),
    nhttp_lib:response(),
    binary(),
    [binary()],
    non_neg_integer()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
follow_redirect(
    Req, UrlInfo, Opts, #{status := Status}, Location, VisitedUrls, RedirectCount
) ->
    case resolve_redirect_url(Location, UrlInfo) of
        {ok, NewUrlInfo} ->
            NewUrl = url_info_to_url(NewUrlInfo),
            case lists:member(NewUrl, VisitedUrls) of
                true ->
                    nhttp_error:redirect_loop(NewUrl);
                false ->
                    NewReq = adjust_request_for_redirect(Req, Status, UrlInfo, NewUrlInfo),
                    do_request_with_redirects(
                        NewReq,
                        NewUrlInfo,
                        Opts,
                        [NewUrl | VisitedUrls],
                        RedirectCount + 1
                    )
            end;
        {error, _} = Error ->
            Error
    end.

-doc "Get Location header from response.".
-spec get_location_header(nhttp_lib:headers()) ->
    {ok, binary()} | {error, nhttpc_error:any_reason()}.
get_location_header(Headers) ->
    case get_header_value(<<"location">>, Headers) of
        undefined ->
            {error, {invalid_redirect_location, <<"missing">>}};
        Location when is_binary(Location), byte_size(Location) > 0 ->
            {ok, Location};
        Location ->
            {error, {invalid_redirect_location, Location}}
    end.

-doc "Handle redirect response.".
-spec handle_redirect(
    nhttp_lib:request(),
    nhttpc_url:http_url_info(),
    nhttpc:opts(),
    nhttp_lib:response(),
    [binary()],
    non_neg_integer()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
handle_redirect(
    Req,
    UrlInfo,
    Opts,
    #{status := Status, headers := Headers} = Resp,
    VisitedUrls,
    RedirectCount
) ->
    case get_location_header(Headers) of
        {ok, Location} ->
            case check_redirect_policy(Status, Location, Req, Opts) of
                continue ->
                    follow_redirect(Req, UrlInfo, Opts, Resp, Location, VisitedUrls, RedirectCount);
                stop ->
                    {ok, Resp};
                {continue, NewOpts} ->
                    follow_redirect(
                        Req, UrlInfo, NewOpts, Resp, Location, VisitedUrls, RedirectCount
                    )
            end;
        {error, _} = Error ->
            Error
    end.

-doc "Check if response is a redirect.".
-spec is_redirect(nhttp_lib:response()) -> boolean().
is_redirect(#{status := 301}) -> true;
is_redirect(#{status := 302}) -> true;
is_redirect(#{status := 303}) -> true;
is_redirect(#{status := 307}) -> true;
is_redirect(#{status := 308}) -> true;
is_redirect(_) -> false.

-doc "Check if two URLs have the same origin.".
-spec is_same_origin(nhttpc_url:http_url_info(), nhttpc_url:http_url_info()) -> boolean().
is_same_origin(
    #{scheme := Scheme, host := Host, port := Port},
    #{scheme := Scheme, host := Host, port := Port}
) ->
    true;
is_same_origin(_, _) ->
    false.

-doc """
Check if header is sensitive and should be stripped on cross-origin redirect.
Headers should be lowercase (normalized at parse time).
""".
-spec is_sensitive_header(binary()) -> boolean().
is_sensitive_header(<<"authorization">>) -> true;
is_sensitive_header(<<"cookie">>) -> true;
is_sensitive_header(_) -> false.

-doc "Normalize path by resolving . and .. segments.".
-spec normalize_path(binary()) -> binary().
normalize_path(Path) ->
    Parts = binary:split(Path, <<"/">>, [global]),
    Normalized = normalize_path_parts(Parts, []),
    case Normalized of
        [] -> <<"/">>;
        _ -> iolist_to_binary([<<"/">>, lists:join(<<"/">>, Normalized)])
    end.

-spec normalize_path_parts([binary()], [binary()]) -> [binary()].
normalize_path_parts([], Acc) ->
    lists:reverse(Acc);
normalize_path_parts([<<>> | Rest], Acc) ->
    normalize_path_parts(Rest, Acc);
normalize_path_parts([<<".">> | Rest], Acc) ->
    normalize_path_parts(Rest, Acc);
normalize_path_parts([<<"..">> | Rest], []) ->
    normalize_path_parts(Rest, []);
normalize_path_parts([<<"..">> | Rest], [_ | AccRest]) ->
    normalize_path_parts(Rest, AccRest);
normalize_path_parts([Segment | Rest], Acc) ->
    normalize_path_parts(Rest, [Segment | Acc]).

-doc "Resolve relative path against base.".
-spec resolve_path(binary(), binary()) -> binary().
resolve_path(BasePath, RelPath) ->
    Combined = <<BasePath/binary, RelPath/binary>>,
    normalize_path(Combined).

-doc "Resolve redirect URL (absolute or relative).".
-spec resolve_redirect_url(binary(), nhttpc_url:http_url_info()) ->
    {ok, nhttpc_url:http_url_info()} | {error, nhttpc_error:any_reason()}.
resolve_redirect_url(Location, OriginalUrlInfo) ->
    case Location of
        <<"http://", _/binary>> ->
            nhttpc_url:parse_http_url(Location);
        <<"https://", _/binary>> ->
            nhttpc_url:parse_http_url(Location);
        <<"/", _/binary>> ->
            #{scheme := Scheme, host := Host, port := Port} = OriginalUrlInfo,
            {Path, Query} = split_path_query_location(Location),
            {ok, #{scheme => Scheme, host => Host, port => Port, path => Path, query => Query}};
        _ ->
            #{scheme := Scheme, host := Host, port := Port, path := OrigPath} = OriginalUrlInfo,
            BasePath = base_path(OrigPath),
            {Path, Query} = split_path_query_location(Location),
            ResolvedPath = resolve_path(BasePath, Path),
            {ok, #{
                scheme => Scheme, host => Host, port => Port, path => ResolvedPath, query => Query
            }}
    end.

-doc "Split path and query from location header.".
-spec split_path_query_location(binary()) -> {binary(), binary()}.
split_path_query_location(Location) ->
    case binary:split(Location, <<"?">>) of
        [Path] -> {Path, <<>>};
        [Path, Query] -> {Path, Query}
    end.

-doc "Update request path for redirect target.".
-spec update_request_path(nhttp_lib:request(), nhttpc_url:http_url_info()) -> nhttp_lib:request().
update_request_path(Req, #{path := Path} = UrlInfo) ->
    Query = maps:get(query, UrlInfo, <<>>),
    FullPath =
        case Query of
            <<>> -> Path;
            _ -> <<Path/binary, "?", Query/binary>>
        end,
    #{host := Host, port := Port, scheme := Scheme} = UrlInfo,
    Transport =
        case Scheme of
            https -> ssl;
            http -> tcp
        end,
    #{headers := Headers} = Req,
    HeadersWithoutHost = nhttp_headers:delete(<<"host">>, Headers),
    Req#{
        path => FullPath,
        headers => add_host_value(HeadersWithoutHost, Host, Port, Transport)
    }.

-doc "Convert url_info back to URL string.".
-spec url_info_to_url(nhttpc_url:http_url_info()) -> binary().
url_info_to_url(#{scheme := Scheme, host := Host, port := Port, path := Path} = UrlInfo) ->
    SchemeStr = atom_to_binary(Scheme),
    DefaultPort =
        case Scheme of
            http -> 80;
            https -> 443
        end,
    PortStr =
        case Port of
            DefaultPort -> <<>>;
            _ -> <<":", (integer_to_binary(Port))/binary>>
        end,
    Query = maps:get(query, UrlInfo, <<>>),
    QueryStr =
        case Query of
            <<>> -> <<>>;
            _ -> <<"?", Query/binary>>
        end,
    <<SchemeStr/binary, "://", Host/binary, PortStr/binary, Path/binary, QueryStr/binary>>.

%%%-----------------------------------------------------------------------------
%% INTERNAL: SINGLE REQUEST EXECUTION
%%%-----------------------------------------------------------------------------
-spec add_host_header(nhttp_lib:request(), binary(), inet:port_number(), tcp | ssl) ->
    nhttp_lib:request().
add_host_header(#{headers := Headers} = Req, Host, Port, Transport) ->
    case nhttp_headers:has(<<"host">>, Headers) of
        true ->
            Req;
        false ->
            DefaultPort =
                case Transport of
                    ssl -> 443;
                    tcp -> 80
                end,
            HostValue =
                case Port of
                    DefaultPort -> Host;
                    _ -> <<Host/binary, ":", (integer_to_binary(Port))/binary>>
                end,
            Req#{headers => [{<<"host">>, HostValue} | Headers]}
    end.

-doc "Apply secure TLS defaults when transport is ssl.".
-spec apply_tls_defaults(tcp | ssl, nhttp_sock:connect_opts()) -> nhttp_sock:connect_opts().
apply_tls_defaults(tcp, Opts) ->
    Opts;
apply_tls_defaults(ssl, Opts) ->
    Opts1 =
        case maps:is_key(verify, Opts) of
            true -> Opts;
            false -> Opts#{verify => verify_peer}
        end,
    case maps:is_key(cacertfile, Opts1) orelse maps:is_key(cacerts, Opts1) of
        true -> Opts1;
        false -> Opts1#{cacerts => public_key:cacerts_get()}
    end.

-doc """
Build connection options including SSL settings from request options.
Applies secure TLS defaults: verify_peer with system CA certificates.
""".
-spec build_connect_opts(tcp | ssl, map()) -> nhttp_sock:connect_opts().
build_connect_opts(Transport, Opts) ->
    BaseOpts = #{transport => Transport},
    UserOpts =
        case nhttpc_opts:tls(Opts) of
            undefined ->
                BaseOpts;
            TlsMap ->
                maps:merge(BaseOpts, normalize_tls_map(TlsMap))
        end,
    apply_tls_defaults(Transport, UserOpts).

-spec detect_protocol(nhttp_sock:t()) -> http1_1 | http2.
detect_protocol(Socket) ->
    case nhttp_sock:negotiated_protocol(Socket) of
        {ok, <<"h2">>} -> http2;
        {ok, <<"http/1.1">>} -> http1_1;
        {ok, <<"http/1.0">>} -> http1_1;
        {error, no_alpn} -> http1_1;
        _ -> http1_1
    end.

-spec do_pooled_request(nhttp_lib:request(), nhttpc_url:http_url_info(), term(), nhttpc:opts()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_pooled_request(Req, UrlInfo, PoolRef, Opts) when is_map(UrlInfo) ->
    Timeout = nhttpc_opts:timeout(request, Opts),
    MaxRespSize = maps:get(max_response_size, Opts, ?DEFAULT_MAX_RESPONSE_SIZE),
    MaxHeaderSize = maps:get(max_header_size, Opts, ?DEFAULT_MAX_HEADER_SIZE),
    MaxHeadersCount = maps:get(max_headers_count, Opts, ?DEFAULT_MAX_HEADERS_COUNT),
    nhttpc_pool:request(PoolRef, Req, Timeout, #{
        max_response_size => MaxRespSize,
        max_header_size => MaxHeaderSize,
        max_headers_count => MaxHeadersCount
    }).

-spec do_poolless_h3_exchange(
    nhttp_lib:request(),
    binary(),
    inet:port_number(),
    nhttpc:opts(),
    timeout(),
    pos_integer() | infinity,
    body_sink(),
    boolean()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_poolless_h3_exchange(Req, Host, Port, Opts, Timeout, MaxResponseSize, Sink, IsStreamingBody) ->
    ConnectTimeout = min(nhttpc_opts:timeout(connect, Opts), Timeout),
    HostBin = normalize_h3_host(Host),
    HostStr = binary_to_list(HostBin),
    ConnectOpts = #{
        tls => #{alpn => [<<"h3">>], verify => verify_none},
        idle_timeout => Timeout
    },
    case nquic:connect(HostStr, Port, ConnectOpts#{timeout => ConnectTimeout}) of
        {ok, Ctx1} ->
            Result =
                case IsStreamingBody of
                    true ->
                        nhttpc_stream_send:do_h3_streaming_upload(
                            Ctx1, Req, Opts, Timeout
                        );
                    false ->
                        do_h3_request(Ctx1, Req, Timeout, MaxResponseSize)
                end,
            nquic_lib:close(Ctx1),
            apply_sink_to_response(Result, Sink);
        {error, Reason} ->
            {error, {connect_error, Reason}}
    end.

-spec do_poolless_request(
    nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts(), body_sink()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_poolless_request(Req, UrlInfo, EffectiveOpts, Sink) ->
    #{host := Host, port := Port} = UrlInfo,
    WireTransport = nhttpc_opts:request_transport(UrlInfo, EffectiveOpts),
    Timeout = nhttpc_opts:timeout(request, EffectiveOpts),
    Transport = tcp_transport(WireTransport),
    ReqWithHost = add_host_header(Req, Host, Port, Transport),
    ReqWithCookies = maybe_add_cookies(ReqWithHost, UrlInfo, EffectiveOpts),
    MaxResponseSize = maps:get(
        max_response_size, EffectiveOpts, ?DEFAULT_MAX_RESPONSE_SIZE
    ),
    IsStreamingBody = is_streaming_body(maps:get(body, ReqWithCookies, <<>>)),
    Result =
        case WireTransport of
            quic ->
                do_poolless_h3_exchange(
                    ReqWithCookies,
                    Host,
                    Port,
                    EffectiveOpts,
                    Timeout,
                    MaxResponseSize,
                    Sink,
                    IsStreamingBody
                );
            _ ->
                do_poolless_tcp_exchange(
                    ReqWithCookies,
                    Host,
                    Port,
                    Transport,
                    EffectiveOpts,
                    Timeout,
                    MaxResponseSize,
                    Sink,
                    IsStreamingBody
                )
        end,
    Result1 = maybe_store_cookies(Result, UrlInfo, EffectiveOpts),
    decompress_unless_sink(Result1, Sink, EffectiveOpts).

-spec normalize_tls_map(nhttpc:tls_opts()) -> map().
normalize_tls_map(Tls) ->
    Extra = maps:get(extra, Tls, []),
    Base = maps:remove(extra, Tls),
    case Extra of
        [] ->
            Base;
        _ ->
            lists:foldl(
                fun
                    ({K, V}, Acc) -> Acc#{K => V};
                    (_, Acc) -> Acc
                end,
                Base,
                Extra
            )
    end.

-spec tcp_transport(tcp | ssl | quic) -> tcp | ssl.
tcp_transport(ssl) -> ssl;
tcp_transport(_) -> tcp.

-type body_sink() ::
    none
    | {file, file:filename()}
    | {fold, nhttpc:on_data_fun(), term()}.
-spec await_conn_response(pid(), reference(), timeout()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
await_conn_response(ConnPid, StreamRef, Timeout) ->
    MonRef = monitor(process, ConnPid),
    try
        receive
            {nhttpc_response, ConnPid, StreamRef, Response} ->
                Response;
            {nhttpc_error, ConnPid, StreamRef, Reason} ->
                {error, Reason};
            {'DOWN', MonRef, process, ConnPid, Reason} ->
                {error, {connection_down, Reason}}
        after Timeout ->
            nhttp_error:request_timeout(Timeout)
        end
    after
        demonitor(MonRef, [flush])
    end.

-spec body_sink(nhttpc:opts()) -> body_sink().
body_sink(#{body_to := {file, Path}}) ->
    {file, Path};
body_sink(#{body_to := {fold, F}} = Opts) when is_function(F, 2) ->
    {fold, F, maps:get(fold_init, Opts, [])};
body_sink(_) ->
    none.

-spec do_conn_request(nhttp_lib:request(), nhttpc:connection(), nhttpc:opts()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_conn_request(Req, Conn, Opts) ->
    Timeout = nhttpc_opts:timeout(request, Opts),
    ConnPid = nhttpc:conn_pid(Conn),
    {ok, StreamRef} = nhttpc_conn:send_async(ConnPid, Req, self()),
    await_conn_response(ConnPid, StreamRef, Timeout).

-spec do_poolless_tcp_exchange(
    nhttp_lib:request(),
    binary(),
    inet:port_number(),
    tcp | ssl,
    nhttpc:opts(),
    timeout(),
    pos_integer() | infinity,
    body_sink(),
    boolean()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_poolless_tcp_exchange(
    Req, Host, Port, Transport, Opts, Timeout, MaxResponseSize, Sink, IsStreamingBody
) ->
    ConnectTimeout = min(5000, Timeout),
    ConnectOpts = build_connect_opts(Transport, Opts),
    case nhttp_sock:connect(Host, Port, ConnectOpts, ConnectTimeout) of
        {ok, Socket} ->
            Protocol = detect_protocol(Socket),
            Result =
                case {Sink, IsStreamingBody} of
                    {none, true} ->
                        nhttpc_stream_send:do_streaming_upload(
                            Socket, Protocol, Req, Opts, Timeout
                        );
                    {none, false} ->
                        do_request_on_connection(
                            Socket, Protocol, Req, Timeout, MaxResponseSize, Opts
                        );
                    {Sink1, false} ->
                        do_poolless_stream_with_sink(
                            Socket, Protocol, Req, Timeout, Sink1, Opts
                        );
                    {Sink1, true} ->
                        UploadResult = nhttpc_stream_send:do_streaming_upload(
                            Socket, Protocol, Req, Opts, Timeout
                        ),
                        apply_sink_to_response(UploadResult, Sink1)
                end,
            nhttp_sock:close(Socket),
            Result;
        {error, _} = Error ->
            Error
    end.

-spec do_request_on_connection(
    nhttp_sock:t(),
    nhttp_lib:version(),
    nhttp_lib:request(),
    timeout(),
    pos_integer() | infinity,
    nhttpc:opts()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_request_on_connection(Socket, http1_1, Req, Timeout, MaxResponseSize, Opts) ->
    do_h1_request(Socket, Req, Timeout, MaxResponseSize, Opts);
do_request_on_connection(Socket, http2, Req, Timeout, MaxResponseSize, _Opts) ->
    do_h2_request(Socket, Req, Timeout, MaxResponseSize).

-doc "Execute a single request without redirect following.".
-spec do_single_request(nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_single_request(Req, UrlInfo, Opts) ->
    Sink = body_sink(Opts),
    EffectiveOpts = compress_opts(Opts, Sink),
    ReqWithEncoding = maybe_add_accept_encoding(Req, EffectiveOpts),
    case maps:get(via, Opts, oneshot) of
        oneshot ->
            case oneshot_target_valid(ReqWithEncoding, UrlInfo) of
                true ->
                    do_poolless_request(ReqWithEncoding, UrlInfo, EffectiveOpts, Sink);
                false ->
                    {error, {invalid_request, missing_scheme_or_authority}}
            end;
        {pool, PoolRef} ->
            Result = do_pooled_request(ReqWithEncoding, UrlInfo, PoolRef, EffectiveOpts),
            sink_response(Result, Sink, EffectiveOpts);
        {conn, Conn} ->
            Result = do_conn_request(ReqWithEncoding, Conn, EffectiveOpts),
            sink_response(Result, Sink, EffectiveOpts)
    end.

-doc "Check if body is a streaming source.".
-spec is_streaming_body(term()) -> boolean().
is_streaming_body({stream, _Source}) -> true;
is_streaming_body({stream, _Source, _Opts}) -> true;
is_streaming_body(_) -> false.

-spec oneshot_target_valid(nhttp_lib:request(), nhttpc_url:http_url_info()) -> boolean().
oneshot_target_valid(Req, #{host := Host}) ->
    HasHost = is_binary(Host) andalso byte_size(Host) > 0,
    HasAuthority =
        case maps:get(authority, Req, undefined) of
            undefined -> false;
            A when is_binary(A) -> byte_size(A) > 0;
            _ -> false
        end,
    HasHost orelse HasAuthority.

-spec telemetry_pool_ref(nhttpc:opts()) -> term() | undefined.
telemetry_pool_ref(#{via := {pool, Ref}}) -> Ref;
telemetry_pool_ref(_) -> undefined.

%%%-----------------------------------------------------------------------------
%% INTERNAL: BODY SINK
%%%-----------------------------------------------------------------------------
-spec apply_sink_to_response(
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()},
    body_sink()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
apply_sink_to_response({ok, Resp}, none) ->
    {ok, Resp};
apply_sink_to_response({ok, #{body := Body} = Resp}, {file, FilePath}) ->
    case file:write_file(FilePath, Body, [raw]) of
        ok ->
            {ok, Resp#{body => {file, FilePath, byte_size_iodata(Body)}}};
        {error, Reason} ->
            {error, {file_error, Reason}}
    end;
apply_sink_to_response({ok, #{body := Body} = Resp}, {fold, F, Init}) ->
    FinalAcc = fold_body(Body, F, Init),
    {ok, Resp#{body => {fold, FinalAcc}}};
apply_sink_to_response({error, _} = Error, _Sink) ->
    Error.

-spec build_file_stream_opts(file:fd(), nhttpc:opts()) -> nhttpc_stream_recv:stream_opts().
build_file_stream_opts(Fd, Opts) ->
    Base = #{
        initial_acc => 0,
        on_headers => fun(Headers, Status) ->
            put(nhttpc_stream_status, Status),
            put(nhttpc_stream_headers, Headers),
            continue
        end,
        on_data => fun(Chunk, BytesWritten) ->
            case file:write(Fd, Chunk) of
                ok -> {continue, BytesWritten + byte_size(Chunk)};
                {error, Reason} -> {stop, {file_error, Reason}, BytesWritten}
            end
        end,
        on_complete => fun(BytesWritten) ->
            Status = erase(nhttpc_stream_status),
            Headers = erase(nhttpc_stream_headers),
            {ok, {Status, Headers, BytesWritten}}
        end
    },
    maybe_add_download_progress(Base, Opts).

-spec build_fold_stream_opts(nhttpc:on_data_fun(), term(), nhttpc:opts()) ->
    nhttpc_stream_recv:stream_opts().
build_fold_stream_opts(F, Init, Opts) ->
    Base = #{
        initial_acc => {fold, Init, false},
        on_headers => fun(Headers, Status) ->
            put(nhttpc_stream_status, Status),
            put(nhttpc_stream_headers, Headers),
            continue
        end,
        on_data => fun(Chunk, {fold, Acc, Stopped}) ->
            case Stopped of
                true ->
                    {continue, {fold, Acc, true}};
                false ->
                    case F(Chunk, Acc) of
                        {continue, NewAcc} ->
                            {continue, {fold, NewAcc, false}};
                        {stop, Result, _NewAcc} ->
                            {continue, {fold, Result, true}}
                    end
            end
        end,
        on_complete => fun({fold, Acc, _}) ->
            Status = erase(nhttpc_stream_status),
            Headers = erase(nhttpc_stream_headers),
            {ok, {Status, Headers, Acc}}
        end
    },
    maybe_add_download_progress(Base, Opts).

-spec byte_size_iodata(iodata()) -> non_neg_integer().
byte_size_iodata(B) when is_binary(B) -> byte_size(B);
byte_size_iodata(L) when is_list(L) -> iolist_size(L).

-spec copy_response_limits(
    nhttpc_stream_recv:stream_opts(), nhttpc:opts()
) ->
    nhttpc_stream_recv:stream_opts().
copy_response_limits(StreamOpts, Opts) ->
    lists:foldl(
        fun(Key, Acc) ->
            case maps:get(Key, Opts, undefined) of
                undefined -> Acc;
                Value -> Acc#{Key => Value}
            end
        end,
        StreamOpts,
        [max_response_size, max_header_size, max_headers_count]
    ).

-spec do_poolless_stream_with_sink(
    nhttp_sock:t(),
    nhttp_lib:version(),
    nhttp_lib:request(),
    timeout(),
    {file, file:filename()} | {fold, nhttpc:on_data_fun(), term()},
    nhttpc:opts()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_poolless_stream_with_sink(Socket, Protocol, Req, Timeout, {file, FilePath}, Opts) ->
    case file:open(FilePath, [write, binary, raw]) of
        {ok, Fd} ->
            StreamOpts = build_file_stream_opts(Fd, Opts),
            Result = nhttpc_stream_recv:do_stream_on_connection(
                Socket, Protocol, Req, StreamOpts, Timeout
            ),
            file:close(Fd),
            case Result of
                {ok, {Status, Headers, Bytes}} ->
                    {ok, #{
                        status => Status,
                        headers => Headers,
                        body => {file, FilePath, Bytes}
                    }};
                {error, _} = Error ->
                    Error
            end;
        {error, Reason} ->
            {error, {file_error, Reason}}
    end;
do_poolless_stream_with_sink(Socket, Protocol, Req, Timeout, {fold, F, Init}, Opts) ->
    StreamOpts = build_fold_stream_opts(F, Init, Opts),
    case nhttpc_stream_recv:do_stream_on_connection(Socket, Protocol, Req, StreamOpts, Timeout) of
        {ok, {Status, Headers, Acc}} ->
            {ok, #{
                status => Status,
                headers => Headers,
                body => {fold, Acc}
            }};
        {error, _} = Error ->
            Error
    end.

-spec fold_body(iodata(), nhttpc:on_data_fun(), term()) -> term().
fold_body(Body, F, Init) ->
    Chunk = iolist_to_binary(Body),
    case F(Chunk, Init) of
        {continue, Acc} -> Acc;
        {stop, Result, _Acc} -> Result
    end.

-spec maybe_add_download_progress(
    nhttpc_stream_recv:stream_opts(), nhttpc:opts()
) ->
    nhttpc_stream_recv:stream_opts().
maybe_add_download_progress(StreamOpts, Opts) ->
    StreamOpts1 =
        case maps:get(on_download_progress, Opts, undefined) of
            undefined -> StreamOpts;
            Fun when is_function(Fun, 2) -> StreamOpts#{on_progress => Fun}
        end,
    copy_response_limits(StreamOpts1, Opts).

-doc "Hand a whole response to the sink, then undo its Content-Encoding.".
-spec sink_response(
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()},
    body_sink(),
    nhttpc:opts()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
sink_response(Result, Sink, Opts) ->
    decompress_unless_sink(apply_sink_to_response(Result, Sink), Sink, Opts).

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/1.1 REQUEST
%%%-----------------------------------------------------------------------------
-spec do_h1_request(
    nhttp_sock:t(),
    nhttp_lib:request(),
    timeout(),
    pos_integer() | infinity,
    nhttpc:opts()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_h1_request(Socket, Req, Timeout, MaxResponseSize, Opts) ->
    case nhttp_h1:encode_request(Req) of
        {ok, IOList} ->
            case nhttp_sock:send(Socket, IOList) of
                ok ->
                    recv_h1_response(
                        Socket, <<>>, Timeout, MaxResponseSize, h1_parse_opts(Opts)
                    );
                {error, Reason} ->
                    nhttp_error:send_error(Reason)
            end;
        {error, EncodeError} ->
            nhttpc_error:invalid_request(element(1, EncodeError))
    end.

-spec recv_h1_response(
    nhttp_sock:t(),
    binary(),
    timeout(),
    pos_integer() | infinity,
    nhttp_h1:opts()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
recv_h1_response(Socket, Buffer, Timeout, MaxResponseSize, ParseOpts) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            Combined = <<Buffer/binary, Data/binary>>,
            case nhttp_h1:parse_response(Combined, ParseOpts) of
                {ok, #{body := Body} = Resp, _Consumed} ->
                    BodySize = byte_size(Body),
                    case MaxResponseSize of
                        infinity ->
                            {ok, Resp};
                        MaxSize when BodySize > MaxSize ->
                            nhttp_error:response_too_large(BodySize, MaxSize);
                        _ ->
                            {ok, Resp}
                    end;
                {more, _} ->
                    BufferSize = byte_size(Combined),
                    case MaxResponseSize of
                        infinity ->
                            recv_h1_response(Socket, Combined, Timeout, MaxResponseSize, ParseOpts);
                        MaxSize when BufferSize > MaxSize ->
                            nhttp_error:response_too_large(BufferSize, MaxSize);
                        _ ->
                            recv_h1_response(Socket, Combined, Timeout, MaxResponseSize, ParseOpts)
                    end;
                {error, Reason} ->
                    nhttp_error:malformed_response(Reason, Combined)
            end;
        {error, timeout} ->
            nhttp_error:request_timeout(Timeout);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/2 REQUEST
%%%-----------------------------------------------------------------------------
-doc """
Hand the peer the credit due after the DATA frames of `StreamId` in
`Events`. `nhttpc_h2_window` decides which WINDOW_UPDATE frames are due,
the same policy the pooled connection and the streaming path apply, so
one peer sees one client.
""".
-spec credit_peer(
    nhttp_sock:t(),
    nhttp_h2:conn(),
    nhttp_lib:stream_id(),
    nhttpc_h2_window:t(),
    [nhttp_h2:event()]
) ->
    {ok, nhttp_h2:conn(), nhttpc_h2_window:t()} | {error, nhttpc_error:any_reason()}.
credit_peer(Socket, H2Conn, StreamId, Window, Events) ->
    {Credits, Window1} = lists:foldl(
        fun
            ({data, Id, Data, Fin}, {Due, W}) when Id =:= StreamId ->
                {NewDue, W1} = nhttpc_h2_window:consume(Id, byte_size(Data), Fin, W),
                {Due ++ NewDue, W1};
            (_Event, Acc) ->
                Acc
        end,
        {[], Window},
        Events
    ),
    maybe
        {ok, H2Conn1, Frames} ?= nhttpc_h2:window_update_frames(Credits, H2Conn),
        ok ?= send_frames(Socket, Frames),
        {ok, H2Conn1, Window1}
    end.

-spec do_h2_request(nhttp_sock:t(), nhttp_lib:request(), timeout(), pos_integer() | infinity) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_h2_request(Socket, Req, Timeout, MaxResponseSize) ->
    H2Conn0 = nhttp_h2:new(client, nhttpc_h2:codec_settings()),
    Preface = nhttp_h2:preface(H2Conn0),
    Headers = nhttpc_h2:request_to_h2_headers(Req),
    Body = maps:get(body, Req, <<>>),
    EndStream = end_stream_for_body(Body),
    maybe
        ok ?= nhttpc_h2:send_with_error(Socket, Preface),
        {ok, H2Conn1} ?= recv_h2_settings(Socket, H2Conn0, Timeout),
        {ok, StreamId, H2Conn2} = nhttp_h2:open_stream(H2Conn1),
        {ok, H2Conn3, HeaderFrame} ?=
            nhttpc_h2:wrap_h2_send_result(
                nhttp_h2:send_headers(H2Conn2, StreamId, Headers, EndStream)
            ),
        ok ?= nhttpc_h2:send_with_error(Socket, HeaderFrame),
        {ok, H2Conn4, Events} ?=
            nhttpc_stream_send:send_h2_body(Socket, H2Conn3, StreamId, Body, Timeout),
        recv_h2_response(Socket, H2Conn4, StreamId, Timeout, MaxResponseSize, Events)
    end.

-doc "END_STREAM rides the HEADERS frame of a request without a body.".
-spec end_stream_for_body(iodata()) -> nhttp_h2:fin().
end_stream_for_body(Body) ->
    case iolist_size(Body) of
        0 -> fin;
        _Size -> nofin
    end.

-doc """
Credit the peer for the DATA frames in `Events`, then fold them into the
response. The credit goes out first, so that the peer can send the rest
of a body above the initial window while the client waits for it.
""".
-spec handle_h2_response_events(
    nhttp_sock:t(),
    nhttp_h2:conn(),
    nhttp_lib:stream_id(),
    timeout(),
    pos_integer() | infinity,
    nhttpc_h2:h2_response_acc(),
    nhttpc_h2_window:t(),
    [nhttp_h2:event()]
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
handle_h2_response_events(Socket, H2Conn, StreamId, Timeout, MaxResponseSize, Acc, Window, Events) ->
    maybe
        {ok, H2Conn1, Window1} ?= credit_peer(Socket, H2Conn, StreamId, Window, Events),
        case nhttpc_h2:process_h2_response_events(Events, StreamId, Acc, MaxResponseSize) of
            {continue, NewAcc} ->
                recv_h2_response_loop(
                    Socket, H2Conn1, StreamId, Timeout, MaxResponseSize, NewAcc, Window1
                );
            {done, Resp} ->
                {ok, Resp};
            {error, _} = Error ->
                Error
        end
    end.

-spec handle_settings_events(nhttp_sock:t(), nhttp_h2:conn(), [nhttp_h2:event()], timeout()) ->
    {ok, nhttp_h2:conn()} | {error, nhttpc_error:any_reason()}.
handle_settings_events(_Socket, H2Conn, [], _Timeout) ->
    {ok, H2Conn};
handle_settings_events(Socket, H2Conn, [Event | Rest], Timeout) ->
    case Event of
        {settings, _Settings} ->
            handle_settings_events(Socket, H2Conn, Rest, Timeout);
        settings_ack ->
            {ok, H2Conn};
        _ ->
            handle_settings_events(Socket, H2Conn, Rest, Timeout)
    end.

-doc """
Receive the response. `Events` are the response events that reached
the codec while the body was going out.
""".
-spec recv_h2_response(
    nhttp_sock:t(),
    nhttp_h2:conn(),
    nhttp_lib:stream_id(),
    timeout(),
    pos_integer() | infinity,
    [nhttp_h2:event()]
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
recv_h2_response(Socket, H2Conn, StreamId, Timeout, MaxResponseSize, Events) ->
    Acc = nhttpc_h2:new_response_acc(),
    Window = nhttpc_h2_window:new(?H2_LOCAL_INITIAL_WINDOW),
    handle_h2_response_events(
        Socket, H2Conn, StreamId, Timeout, MaxResponseSize, Acc, Window, Events
    ).

-spec recv_h2_response_loop(
    nhttp_sock:t(),
    nhttp_h2:conn(),
    nhttp_lib:stream_id(),
    timeout(),
    pos_integer() | infinity,
    nhttpc_h2:h2_response_acc(),
    nhttpc_h2_window:t()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
recv_h2_response_loop(Socket, H2Conn, StreamId, Timeout, MaxResponseSize, Acc, Window) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            case nhttp_h2:recv(H2Conn, Data) of
                {ok, Events, NewH2Conn} ->
                    handle_h2_response_events(
                        Socket, NewH2Conn, StreamId, Timeout, MaxResponseSize, Acc, Window, Events
                    );
                {ok, Events, NewH2Conn, ToSend} ->
                    case nhttp_sock:send(Socket, ToSend) of
                        ok ->
                            handle_h2_response_events(
                                Socket,
                                NewH2Conn,
                                StreamId,
                                Timeout,
                                MaxResponseSize,
                                Acc,
                                Window,
                                Events
                            );
                        {error, Reason} ->
                            nhttp_error:send_error(Reason)
                    end;
                {error, Reason} ->
                    {error, {http2_error, Reason}}
            end;
        {error, timeout} ->
            nhttp_error:request_timeout(Timeout);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

-spec recv_h2_settings(nhttp_sock:t(), nhttp_h2:conn(), timeout()) ->
    {ok, nhttp_h2:conn()} | {error, nhttpc_error:any_reason()}.
recv_h2_settings(Socket, H2Conn, Timeout) ->
    case nhttp_sock:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            case nhttp_h2:recv(H2Conn, Data) of
                {ok, Events, NewH2Conn} ->
                    handle_settings_events(Socket, NewH2Conn, Events, Timeout);
                {ok, Events, NewH2Conn, ToSend} ->
                    case nhttp_sock:send(Socket, ToSend) of
                        ok -> handle_settings_events(Socket, NewH2Conn, Events, Timeout);
                        {error, Reason} -> nhttp_error:send_error(Reason)
                    end;
                {error, Reason} ->
                    {error, {http2_error, Reason}}
            end;
        {error, timeout} ->
            nhttp_error:connect_timeout(Timeout);
        {error, Reason} ->
            nhttp_error:recv_error(Reason)
    end.

-spec send_frames(nhttp_sock:t(), [iodata()]) -> ok | {error, nhttpc_error:any_reason()}.
send_frames(_Socket, []) ->
    ok;
send_frames(Socket, Frames) ->
    nhttpc_h2:send_with_error(Socket, Frames).

%%%-----------------------------------------------------------------------------
%% INTERNAL: HTTP/3 REQUEST
%%%-----------------------------------------------------------------------------
-spec do_h3_request(nquic:ctx(), nhttp_lib:request(), timeout(), pos_integer() | infinity) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
do_h3_request(Ctx0, Req, Timeout, MaxResponseSize) ->
    maybe
        {ok, H3Conn0, Ctx1, InitActions} ?= init_h3_streams(Ctx0),
        {ok, Ctx2} ?= execute_h3_actions(Ctx1, InitActions),
        {ok, StreamId, Ctx3} ?= nquic_lib:open_stream(Ctx2, #{type => bidi}),
        Headers = nhttpc_h3:request_to_h3_headers(Req),
        Body = maps:get(body, Req, <<>>),
        HasBody = iolist_size(Body) > 0,
        EndStream =
            case HasBody of
                true -> nofin;
                false -> fin
            end,
        {ok, H3Conn1, HeaderActions} ?=
            nhttpc_h3:wrap_h3_send_result(
                nhttp_h3:send_headers(H3Conn0, StreamId, Headers, EndStream)
            ),
        {ok, H3ConnFinal, Ctx4} ?=
            maybe_send_h3_body(Ctx3, H3Conn1, StreamId, Body, HasBody, HeaderActions),
        {ok, Ctx5} = nquic_lib:flush_notimers(Ctx4),
        recv_h3_response(Ctx5, H3ConnFinal, StreamId, Timeout, MaxResponseSize)
    end.

-spec execute_h3_actions(nquic:ctx(), [nhttp_h3:action()]) ->
    {ok, nquic:ctx()} | {error, nhttpc_error:any_reason()}.
execute_h3_actions(Ctx, Actions) ->
    case nhttpc_h3:execute_actions_noflush(Ctx, Actions) of
        {ok, _} = Ok -> Ok;
        {error, _} = Error -> Error;
        {close, Code} -> {error, {http3_error, {connection_close, Code}}}
    end.

-spec handle_h3_recv_events(
    nhttp_lib:stream_id(),
    pos_integer() | infinity,
    nhttpc_h3:h3_response_acc(),
    [nhttp_h3:event()]
) ->
    {continue, nhttpc_h3:h3_response_acc()}
    | {done, nhttp_lib:response()}
    | {error, nhttpc_error:any_reason()}.
handle_h3_recv_events(StreamId, MaxResponseSize, Acc, Events) ->
    case nhttpc_h3:process_h3_response_events(Events, StreamId, Acc) of
        {continue, #{acc_size := AccSize}} when
            MaxResponseSize =/= infinity, AccSize > MaxResponseSize
        ->
            nhttp_error:response_too_large(AccSize, MaxResponseSize);
        Other ->
            Other
    end.

-spec init_h3_streams(nquic:ctx()) ->
    {ok, nhttp_h3:conn(), nquic:ctx(), [nhttp_h3:action()]} | {error, nhttpc_error:any_reason()}.
init_h3_streams(Ctx0) ->
    H3Conn = nhttp_h3:new(client, #{}),
    maybe
        {ok, CtrlId, Ctx1} ?= nquic_lib:open_stream(Ctx0, #{type => uni}),
        {ok, EncId, Ctx2} ?= nquic_lib:open_stream(Ctx1, #{type => uni}),
        {ok, DecId, Ctx3} ?= nquic_lib:open_stream(Ctx2, #{type => uni}),
        {ok, H3Conn1, Actions} ?=
            nhttp_h3:init_local_streams(H3Conn, #{
                control => CtrlId,
                encoder => EncId,
                decoder => DecId
            }),
        {ok, H3Conn1, Ctx3, Actions}
    end.

-spec maybe_send_h3_body(
    nquic:ctx(),
    nhttp_h3:conn(),
    nhttp_lib:stream_id(),
    iodata(),
    boolean(),
    [nhttp_h3:action()]
) ->
    {ok, nhttp_h3:conn(), nquic:ctx()} | {error, nhttpc_error:any_reason()}.
maybe_send_h3_body(Ctx, H3Conn, _StreamId, _Body, false, HeaderActions) ->
    case execute_h3_actions(Ctx, HeaderActions) of
        {ok, Ctx1} -> {ok, H3Conn, Ctx1};
        {error, _} = Error -> Error
    end;
maybe_send_h3_body(Ctx, H3Conn, StreamId, Body, true, HeaderActions) ->
    case nhttpc_h3:wrap_h3_send_result(nhttp_h3:send_data(H3Conn, StreamId, Body, fin)) of
        {ok, NewH3Conn, BodyActions} ->
            case execute_h3_actions(Ctx, HeaderActions ++ BodyActions) of
                {ok, Ctx1} -> {ok, NewH3Conn, Ctx1};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

-spec normalize_h3_host(binary()) -> binary().
normalize_h3_host(Host) when is_binary(Host) -> Host.

-spec process_h3_quic_events(
    nquic:ctx(),
    nhttp_h3:conn(),
    nhttp_lib:stream_id(),
    timeout(),
    pos_integer() | infinity,
    nhttpc_h3:h3_response_acc(),
    [nquic_protocol:event()]
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
process_h3_quic_events(Ctx, H3Conn, StreamId, Timeout, MaxResponseSize, Acc, []) ->
    recv_h3_response_loop(Ctx, H3Conn, StreamId, Timeout, MaxResponseSize, Acc);
process_h3_quic_events(Ctx, H3Conn, StreamId, Timeout, MaxResponseSize, Acc, [Event | Rest]) ->
    case Event of
        {stream_data, SId} ->
            case nquic_lib:recv(Ctx, SId) of
                {ok, Data, IsFin, Ctx1} ->
                    Fin =
                        case IsFin of
                            true -> fin;
                            false -> nofin
                        end,
                    case nhttp_h3:recv(H3Conn, SId, Data, Fin) of
                        {ok, H3Events, NewH3Conn, Actions} ->
                            case execute_h3_actions(Ctx1, Actions) of
                                {ok, Ctx2} ->
                                    case
                                        handle_h3_recv_events(
                                            StreamId, MaxResponseSize, Acc, H3Events
                                        )
                                    of
                                        {continue, NewAcc} ->
                                            process_h3_quic_events(
                                                Ctx2,
                                                NewH3Conn,
                                                StreamId,
                                                Timeout,
                                                MaxResponseSize,
                                                NewAcc,
                                                Rest
                                            );
                                        {done, Resp} ->
                                            {ok, Resp};
                                        {error, _} = Error ->
                                            Error
                                    end;
                                {error, _} = Error ->
                                    Error
                            end;
                        {error, Reason} ->
                            {error, {http3_error, Reason}}
                    end;
                {error, Reason} ->
                    {error, {quic_error, Reason}}
            end;
        {stream_opened, SId} ->
            Type = stream_id_type(SId),
            case nhttp_h3:stream_opened(H3Conn, SId, Type) of
                {ok, NewH3Conn} ->
                    process_h3_quic_events(
                        Ctx, NewH3Conn, StreamId, Timeout, MaxResponseSize, Acc, Rest
                    );
                {error, Reason} ->
                    {error, {http3_error, Reason}}
            end;
        {stream_reset, SId, AppErrorCode} ->
            {ok, H3Events, NewH3Conn} = nhttp_h3:stream_reset(H3Conn, SId, AppErrorCode),
            case handle_h3_recv_events(StreamId, MaxResponseSize, Acc, H3Events) of
                {continue, NewAcc} ->
                    process_h3_quic_events(
                        Ctx, NewH3Conn, StreamId, Timeout, MaxResponseSize, NewAcc, Rest
                    );
                {done, Resp} ->
                    {ok, Resp};
                {error, _} = Error ->
                    Error
            end;
        connection_closed ->
            {error, {quic_closed, connection_closed}};
        _ ->
            process_h3_quic_events(
                Ctx, H3Conn, StreamId, Timeout, MaxResponseSize, Acc, Rest
            )
    end.

-spec recv_h3_response(
    nquic:ctx(),
    nhttp_h3:conn(),
    nhttp_lib:stream_id(),
    timeout(),
    pos_integer() | infinity
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
recv_h3_response(Ctx, H3Conn, StreamId, Timeout, MaxResponseSize) ->
    Acc = nhttpc_h3:new_response_acc(),
    recv_h3_response_loop(Ctx, H3Conn, StreamId, Timeout, MaxResponseSize, Acc).

-spec recv_h3_response_loop(
    nquic:ctx(),
    nhttp_h3:conn(),
    nhttp_lib:stream_id(),
    timeout(),
    pos_integer() | infinity,
    nhttpc_h3:h3_response_acc()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
recv_h3_response_loop(Ctx, H3Conn, StreamId, Timeout, MaxResponseSize, Acc) ->
    case nquic_lib:recv_direct(Ctx, Timeout) of
        {ok, QuicEvents, Ctx1} ->
            process_h3_quic_events(
                Ctx1, H3Conn, StreamId, Timeout, MaxResponseSize, Acc, QuicEvents
            );
        {error, timeout, _Ctx1} ->
            nhttp_error:request_timeout(Timeout);
        {error, Reason, _Ctx1} ->
            {error, {quic_error, Reason}}
    end.

-spec stream_id_type(non_neg_integer()) -> bidi | uni.
stream_id_type(StreamId) ->
    case StreamId band 2#10 of
        0 -> bidi;
        _ -> uni
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL: COMPRESSION
%%%-----------------------------------------------------------------------------
-doc "Turn compression off when a body sink takes the bytes as they arrive.".
-spec compress_opts(nhttpc:opts(), body_sink()) -> nhttpc:opts().
compress_opts(Opts, none) ->
    Opts;
compress_opts(Opts, _Sink) ->
    Opts#{compress => false}.

-doc "Decompress response body based on Content-Encoding header.".
-spec decompress_response_body(nhttp_lib:response()) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
decompress_response_body(#{headers := Headers, body := Body} = Resp) ->
    case get_content_encoding(Headers) of
        identity ->
            {ok, Resp};
        gzip ->
            case nhttp_compress:decompress(Body, gzip) of
                {ok, Decompressed} ->
                    NewHeaders = remove_content_encoding(Headers),
                    {ok, Resp#{headers => NewHeaders, body => Decompressed}};
                {error, Reason} ->
                    {error, {decompress_error, gzip, Reason}}
            end;
        deflate ->
            case nhttp_compress:decompress(Body, deflate) of
                {ok, Decompressed} ->
                    NewHeaders = remove_content_encoding(Headers),
                    {ok, Resp#{headers => NewHeaders, body => Decompressed}};
                {error, Reason} ->
                    {error, {decompress_error, deflate, Reason}}
            end;
        {unknown, _Encoding} ->
            {ok, Resp}
    end.

-doc "Undo the Content-Encoding of a response that no sink took.".
-spec decompress_unless_sink(
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()},
    body_sink(),
    nhttpc:opts()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
decompress_unless_sink(Result, none, Opts) ->
    maybe_decompress_response(Result, Opts);
decompress_unless_sink(Result, _Sink, _Opts) ->
    Result.

-doc "Extract Content-Encoding from response headers.".
-spec get_content_encoding(nhttp_lib:headers()) -> gzip | deflate | identity | {unknown, binary()}.
get_content_encoding(Headers) ->
    case get_header_value(<<"content-encoding">>, Headers) of
        undefined ->
            identity;
        Encoding ->
            case nhttp_headers:to_lower(Encoding) of
                <<"gzip">> -> gzip;
                <<"x-gzip">> -> gzip;
                <<"deflate">> -> deflate;
                <<"identity">> -> identity;
                _ -> {unknown, Encoding}
            end
    end.

-spec get_header_value(binary(), nhttp_lib:headers()) -> binary() | undefined.
get_header_value(Name, Headers) ->
    nhttp_headers:get(Name, Headers).

-doc """
Build the `t:nhttp_h1:opts/0` map used to parse responses.
Caps response header *size* and *count* with safe defaults. Callers can
override either bound via `max_header_size` / `max_headers_count` in
`t:nhttpc:opts/0` or `t:nhttpc_stream_recv:stream_opts/0`. Use `infinity`
to disable a bound.
""".
-spec h1_parse_opts(nhttpc:opts() | nhttpc_stream_recv:stream_opts()) -> nhttp_h1:opts().
h1_parse_opts(Opts) ->
    Size = maps:get(max_header_size, Opts, ?DEFAULT_MAX_HEADER_SIZE),
    Count = maps:get(max_headers_count, Opts, ?DEFAULT_MAX_HEADERS_COUNT),
    maps:filter(
        fun(_, V) -> V =/= infinity end,
        #{max_header_size => Size, max_headers_count => Count}
    ).

-spec maybe_add_accept_encoding(nhttp_lib:request(), nhttpc:opts()) -> nhttp_lib:request().
maybe_add_accept_encoding(#{headers := Headers} = Req, Opts) ->
    Compress = maps:get(compress, Opts, true),
    case Compress andalso not nhttp_headers:has(<<"accept-encoding">>, Headers) of
        true ->
            NewHeaders = [{<<"accept-encoding">>, <<"gzip, deflate">>} | Headers],
            Req#{headers => NewHeaders};
        false ->
            Req
    end.

-doc """
Decompress response body if Content-Encoding indicates compression.
decompress option defaults to true.
""".
-spec maybe_decompress_response(
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}, nhttpc:opts()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
maybe_decompress_response({error, _} = Error, _Opts) ->
    Error;
maybe_decompress_response({ok, Resp}, Opts) ->
    Decompress = maps:get(decompress, Opts, true),
    case Decompress of
        true ->
            decompress_response_body(Resp);
        false ->
            {ok, Resp}
    end.

-doc "Remove Content-Encoding header after decompression.".
-spec remove_content_encoding(nhttp_lib:headers()) -> nhttp_lib:headers().
remove_content_encoding(Headers) ->
    nhttp_headers:delete(<<"content-encoding">>, Headers).

%%%-----------------------------------------------------------------------------
%% INTERNAL: COOKIE JAR
%%%-----------------------------------------------------------------------------
-spec handle_cookie_jar_set(ok | {error, nhttpc:error()}) -> ok.
handle_cookie_jar_set(ok) ->
    ok;
handle_cookie_jar_set({error, {cookie_jar_error, stopped}}) ->
    ok.

-doc "Add Cookie header from cookie jar if available.".
-spec maybe_add_cookies(nhttp_lib:request(), nhttpc_url:http_url_info(), nhttpc:opts()) ->
    nhttp_lib:request().
maybe_add_cookies(#{headers := Headers} = Req, UrlInfo, Opts) ->
    case maps:get(cookie_jar, Opts, undefined) of
        undefined ->
            Req;
        Jar ->
            Uri = url_info_to_uri(UrlInfo),
            case nhttpc_cookie_jar:get_header(Jar, Uri) of
                {ok, undefined} ->
                    Req;
                {ok, CookieValue} ->
                    case nhttp_headers:has(<<"cookie">>, Headers) of
                        true ->
                            Req;
                        false ->
                            Req#{headers => [{<<"cookie">>, CookieValue} | Headers]}
                    end;
                {error, _} ->
                    Req
            end
    end.

-doc "Store Set-Cookie headers in cookie jar if available.".
-spec maybe_store_cookies(
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()},
    nhttpc_url:http_url_info(),
    nhttpc:opts()
) ->
    {ok, nhttp_lib:response()} | {error, nhttpc_error:any_reason()}.
maybe_store_cookies({error, _} = Error, _UrlInfo, _Opts) ->
    Error;
maybe_store_cookies({ok, #{headers := Headers} = Resp}, UrlInfo, Opts) ->
    case maps:get(cookie_jar, Opts, undefined) of
        undefined ->
            {ok, Resp};
        Jar ->
            Uri = url_info_to_uri(UrlInfo),
            ok = handle_cookie_jar_set(nhttpc_cookie_jar:set_from_headers(Jar, Uri, Headers)),
            {ok, Resp}
    end.

-doc "Convert url_info to URI format expected by cookie jar.".
-spec url_info_to_uri(nhttpc_url:http_url_info()) -> nhttpc_cookie_jar:uri().
url_info_to_uri(#{scheme := Scheme, host := Host, path := Path} = UrlInfo) ->
    #{
        scheme => Scheme,
        host => Host,
        path => Path,
        port => maps:get(port, UrlInfo, 80)
    }.
