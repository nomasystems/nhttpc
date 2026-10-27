-module(nhttpc_url).

-moduledoc """
URL parsing and request canonicalisation for the nhttpc client.

Two responsibilities:

  * `parse_url/1` — decode an absolute URL into its components
    (`scheme`, `host`, `port`, `path`, `query`). Supports the four
    schemes the rest of the stack understands: `http`, `https`, `ws`,
    `wss`.
  * `to_request/1` — validate a canonical `nhttp_lib:request()`
    (`scheme` + `authority` + `path`) or a partial path-only request
    (used for pool / on-connection dispatch where the connection
    carries the authority). Accepts the `http` and `https` schemes.
    Returns the request together with a derived `http_url_info/0`
    for the transport layer.

Callers wanting URL input use `nhttpc:url/2` to build the canonical
map; only `scheme` / `authority` / `path` are accepted on the request.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    parse_http_url/1,
    parse_url/1
]).

-export([
    normalize_method/1,

    split_path_query/1,
    to_request/1
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    http_url_info/0,
    parse_error/0,
    url_info/0
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-type url_info() :: #{
    scheme := nhttp_lib:scheme(),
    host := binary(),
    port := inet:port_number(),
    path := binary(),
    query => binary()
}.

-type http_url_info() :: #{
    scheme := http | https,
    host := binary(),
    port := inet:port_number(),
    path := binary(),
    query => binary()
}.

-type parse_error() ::
    invalid_url_scheme
    | empty_host
    | invalid_port
    | invalid_authority
    | invalid_ipv6_address.

%%%-----------------------------------------------------------------------------
%% URL PARSING
%%%-----------------------------------------------------------------------------
-doc """
Parse an absolute `http` or `https` URL into components.

Returns `{error, invalid_url_scheme}` for the `ws` and `wss` schemes and
for any other scheme.
""".
-spec parse_http_url(binary()) -> {ok, http_url_info()} | {error, parse_error()}.
parse_http_url(Url) ->
    case parse_url(Url) of
        {ok, #{scheme := http} = UrlInfo} -> {ok, UrlInfo};
        {ok, #{scheme := https} = UrlInfo} -> {ok, UrlInfo};
        {ok, #{scheme := ws}} -> {error, invalid_url_scheme};
        {ok, #{scheme := wss}} -> {error, invalid_url_scheme};
        {error, _} = Error -> Error
    end.

-doc """
Parse an absolute URL into components.
Supports the `http`, `https`, `ws`, and `wss` schemes. Returns
`{error, invalid_url_scheme}` for anything else.
""".
-spec parse_url(binary()) -> {ok, url_info()} | {error, parse_error()}.
parse_url(Url) when is_binary(Url) ->
    parse_url_scheme(Url).

%%%-----------------------------------------------------------------------------
%% REQUEST CANONICALISATION
%%%-----------------------------------------------------------------------------
-spec default_port(http | https) -> inet:port_number().
default_port(http) -> 80;
default_port(https) -> 443.

-spec normalize_method(nhttp_lib:method() | binary()) -> nhttp_lib:method().
normalize_method(get) -> get;
normalize_method(head) -> head;
normalize_method(post) -> post;
normalize_method(put) -> put;
normalize_method(delete) -> delete;
normalize_method(connect) -> connect;
normalize_method(options) -> options;
normalize_method(trace) -> trace;
normalize_method(patch) -> patch;
normalize_method(query) -> query;
normalize_method(Method) when is_binary(Method) -> nhttp_lib:decode_method(Method).

-spec partial_url_info(binary()) -> http_url_info().
partial_url_info(Path) ->
    #{scheme => http, host => <<>>, port => 80, path => Path}.

-spec split_authority_host_port(binary(), inet:port_number()) ->
    {binary(), inet:port_number()}.
split_authority_host_port(<<"[", Rest/binary>>, DefaultPort) ->
    case binary:split(Rest, <<"]">>) of
        [IPv6, <<>>] ->
            {<<"[", IPv6/binary, "]">>, DefaultPort};
        [IPv6, <<":", PortBin/binary>>] ->
            case parse_port(PortBin) of
                {ok, Port} -> {<<"[", IPv6/binary, "]">>, Port};
                error -> {<<"[", IPv6/binary, "]">>, DefaultPort}
            end;
        _ ->
            {Rest, DefaultPort}
    end;
split_authority_host_port(Authority, DefaultPort) ->
    case binary:split(Authority, <<":">>, [global]) of
        [Host] ->
            {Host, DefaultPort};
        [Host, PortBin] ->
            case parse_port(PortBin) of
                {ok, Port} -> {Host, Port};
                error -> {Host, DefaultPort}
            end;
        _ ->
            {Authority, DefaultPort}
    end.

-doc """
Validate and canonicalise a user-supplied request map and return the
matching `url_info/0` for the transport layer.
Accepted shapes:
  * Canonical — `#{method, scheme, authority, path, headers?, body?}`.
    Used for `oneshot` dispatch.
  * Partial — `#{method, path, headers?, body?}` (path-only). Used
    for pool / on-connection dispatch where the authority lives on
    the connection. A dummy `http_url_info/0` is returned; the oneshot
    path rejects partial requests downstream.
A canonical request with another scheme returns
`{error, {invalid_request, invalid_url_scheme}}`. Anything else returns
`{error, {invalid_request, bad_shape}}`.
""".
-spec to_request(map()) ->
    {ok, nhttp_lib:request(), http_url_info()} | {error, {invalid_request, atom()}}.
to_request(
    #{method := Method, scheme := Scheme, authority := Authority, path := Path} = Req0
) when is_binary(Path), (Scheme =:= http orelse Scheme =:= https) ->
    Req1 = Req0#{
        method => normalize_method(Method),
        scheme => Scheme,
        authority => Authority,
        path => Path,
        headers => maps:get(headers, Req0, []),
        body => maps:get(body, Req0, <<>>)
    },
    UrlInfo = url_info_from_canonical(Scheme, Authority, Path),
    {ok, Req1, UrlInfo};
to_request(#{method := _, scheme := _, authority := _, path := Path}) when is_binary(Path) ->
    {error, {invalid_request, invalid_url_scheme}};
to_request(#{method := Method, path := Path} = Req0) when is_binary(Path) ->
    Req1 = Req0#{
        method => normalize_method(Method),
        headers => maps:get(headers, Req0, []),
        body => maps:get(body, Req0, <<>>)
    },
    {ok, Req1, partial_url_info(Path)};
to_request(_) ->
    {error, {invalid_request, bad_shape}}.

-spec url_info_from_canonical(http | https, binary(), binary()) -> http_url_info().
url_info_from_canonical(Scheme, Authority, FullPath) ->
    {Host, Port} = split_authority_host_port(Authority, default_port(Scheme)),
    {Path, Query} = split_path_query(FullPath),
    NormPath =
        case Path of
            <<>> -> <<"/">>;
            _ -> Path
        end,
    #{scheme => Scheme, host => Host, port => Port, path => NormPath, query => Query}.

%%%-----------------------------------------------------------------------------
%% URL PARSING - INTERNAL
%%%-----------------------------------------------------------------------------
-spec ensure_path(binary()) -> binary().
ensure_path(<<>>) -> <<"/">>;
ensure_path(Path) -> <<"/", Path/binary>>.

-spec find_authority_end(binary()) -> {slash | query, non_neg_integer()} | none.
find_authority_end(Bin) ->
    SlashPos = binary:match(Bin, <<"/">>),
    QueryPos = binary:match(Bin, <<"?">>),
    case {SlashPos, QueryPos} of
        {nomatch, nomatch} -> none;
        {{Pos, _}, nomatch} -> {slash, Pos};
        {nomatch, {Pos, _}} -> {query, Pos};
        {{SlashP, _}, {QueryP, _}} when SlashP < QueryP -> {slash, SlashP};
        {_, {QueryP, _}} -> {query, QueryP}
    end.

-spec parse_host_port(binary(), inet:port_number()) ->
    {ok, binary(), inet:port_number()} | {error, parse_error()}.
parse_host_port(<<>>, _DefaultPort) ->
    {error, empty_host};
parse_host_port(Authority, DefaultPort) ->
    case Authority of
        <<"[", Rest/binary>> -> parse_ipv6_host_port(Rest, DefaultPort);
        _ -> parse_regular_host_port(Authority, DefaultPort)
    end.

-spec parse_ipv6_host_port(binary(), inet:port_number()) ->
    {ok, binary(), inet:port_number()} | {error, parse_error()}.
parse_ipv6_host_port(Bin, DefaultPort) ->
    case binary:split(Bin, <<"]">>) of
        [IPv6, <<>>] ->
            {ok, <<"[", IPv6/binary, "]">>, DefaultPort};
        [IPv6, <<":", PortBin/binary>>] ->
            case parse_port(PortBin) of
                {ok, Port} -> {ok, <<"[", IPv6/binary, "]">>, Port};
                error -> {error, invalid_port}
            end;
        _ ->
            {error, invalid_ipv6_address}
    end.

-spec parse_port(binary()) -> {ok, inet:port_number()} | error.
parse_port(Bin) ->
    try
        case binary_to_integer(Bin) of
            Port when Port >= 1, Port =< 65535 -> {ok, Port};
            _ -> error
        end
    catch
        error:badarg -> error
    end.

-spec parse_regular_host_port(binary(), inet:port_number()) ->
    {ok, binary(), inet:port_number()} | {error, parse_error()}.
parse_regular_host_port(Authority, DefaultPort) ->
    case binary:split(Authority, <<":">>, [global]) of
        [Host] ->
            {ok, Host, DefaultPort};
        [Host, PortBin] ->
            case parse_port(PortBin) of
                {ok, Port} -> {ok, Host, Port};
                error -> {error, invalid_port}
            end;
        _ ->
            {error, invalid_authority}
    end.

-spec parse_url_authority(binary(), nhttp_lib:scheme(), inet:port_number()) ->
    {ok, url_info()} | {error, parse_error()}.
parse_url_authority(Rest, Scheme, DefaultPort) ->
    {Authority, PathAndQuery} = split_authority(Rest),
    case parse_host_port(Authority, DefaultPort) of
        {ok, Host, Port} ->
            {Path, Query} = split_path_query(PathAndQuery),
            UrlInfo = #{
                scheme => Scheme,
                host => Host,
                port => Port,
                path => ensure_path(Path),
                query => Query
            },
            {ok, UrlInfo};
        {error, _} = Error ->
            Error
    end.

-spec parse_url_scheme(binary()) -> {ok, url_info()} | {error, parse_error()}.
parse_url_scheme(<<"https://", Rest/binary>>) ->
    parse_url_authority(Rest, https, 443);
parse_url_scheme(<<"http://", Rest/binary>>) ->
    parse_url_authority(Rest, http, 80);
parse_url_scheme(<<"wss://", Rest/binary>>) ->
    parse_url_authority(Rest, wss, 443);
parse_url_scheme(<<"ws://", Rest/binary>>) ->
    parse_url_authority(Rest, ws, 80);
parse_url_scheme(_) ->
    {error, invalid_url_scheme}.

-spec split_authority(binary()) -> {binary(), binary()}.
split_authority(Bin) ->
    case find_authority_end(Bin) of
        {slash, Pos} ->
            Authority = binary:part(Bin, 0, Pos),
            Rest = binary:part(Bin, Pos + 1, byte_size(Bin) - Pos - 1),
            {Authority, Rest};
        {query, Pos} ->
            Authority = binary:part(Bin, 0, Pos),
            Rest = binary:part(Bin, Pos, byte_size(Bin) - Pos),
            {Authority, Rest};
        none ->
            {Bin, <<>>}
    end.

-spec split_path_query(binary()) -> {binary(), binary()}.
split_path_query(<<"?", Query/binary>>) ->
    {<<>>, Query};
split_path_query(Bin) ->
    case binary:split(Bin, <<"?">>) of
        [Path] -> {Path, <<>>};
        [Path, Query] -> {Path, Query}
    end.
