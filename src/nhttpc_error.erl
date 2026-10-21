-module(nhttpc_error).

-moduledoc """
Canonical error taxonomy for nhttpc.

Every error that crosses the public surface of `nhttpc`, `nhttpc_cookie_jar`,
or `nhttpc_url` is shaped by this module so callers can pattern-match
on `nhttpc:error/0` without destructuring upstream shapes.

Two responsibilities:

  * Constructors — `connect_error/1`, `tls_error/1`, `timeout/1`, …
    return canonical `{error, nhttpc:error()}` tuples. Use them at the
    sites that currently call `nhttp_error:foo()`.
  * Mappers — `from_h1/1`, `from_h2/1`, `from_h3/1`, `from_ssl/1`,
    `from_file/1`, and the generic `wrap/1` translate any internal
    map-style or upstream error shape into the canonical sum.

`wrap/1` is the single funnel for unknown error terms; it inspects the
shape and dispatches to the right specific mapper. Already-canonical
values pass through unchanged.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS - CONSTRUCTORS
%%%-----------------------------------------------------------------------------
-export([
    body_error/1,
    connect_error/1,
    connection_closed/0,
    cookie_jar_error/1,
    file_error/1,
    http_error/2,
    invalid_opts/1,
    invalid_request/1,
    pool_closed/0,
    pool_full/0,
    protocol_error/2,
    redirect_error/1,
    retries_exhausted/2,
    stream_error/1,
    timeout/1,
    tls_error/1
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - MAPPERS
%%%-----------------------------------------------------------------------------
-export([
    from_file/1,
    from_h1/1,
    from_h2/1,
    from_h3/1,
    from_ssl/1,
    wrap/1,
    wrap_result/1
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - TYPES
%%%-----------------------------------------------------------------------------
-export_type([
    any_reason/0
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-doc """
Wide error type for internal modules that emit raw `nhttp_error`-style
shapes. Always wrapped by `wrap/1` at the public boundary.
""".
-type any_reason() ::
    nhttpc:error()
    | {nhttp_error:category(), nhttp_error:reason()}
    | atom()
    | tuple()
    | binary()
    | map()
    | list()
    | integer().

%%%-----------------------------------------------------------------------------
%% CONSTRUCTORS
%%%-----------------------------------------------------------------------------
-doc "Body / upload iterator error.".
-spec body_error(nhttpc:body_reason()) -> {error, nhttpc:error()}.
body_error(Reason) ->
    {error, {body_error, Reason}}.

-doc "Connection establishment error.".
-spec connect_error(inet:posix() | term()) -> {error, nhttpc:error()}.
connect_error(Posix) ->
    {error, {connect_error, Posix}}.

-doc "Connection closed by peer mid-request.".
-spec connection_closed() -> {error, nhttpc:error()}.
connection_closed() ->
    {error, {connect_error, closed}}.

-doc "Cookie jar error.".
-spec cookie_jar_error(nhttpc:cookie_jar_reason()) -> {error, nhttpc:error()}.
cookie_jar_error(Reason) ->
    {error, {cookie_jar_error, Reason}}.

-doc "File operation error.".
-spec file_error(nhttpc:file_reason()) ->
    {error, nhttpc:error()}.
file_error(Reason) ->
    {error, {file_error, Reason}}.

-doc "Response carries an HTTP error status (4xx / 5xx) when surfaced.".
-spec http_error(nhttp_lib:status(), nhttp_lib:headers()) -> {error, nhttpc:error()}.
http_error(Status, Headers) ->
    {error, {http_error, Status, Headers}}.

-doc "Invalid options passed to a request or pool.".
-spec invalid_opts(atom()) -> {error, nhttpc:error()}.
invalid_opts(Kind) ->
    {error, {invalid_opts, Kind}}.

-doc "Invalid request map passed to the API.".
-spec invalid_request(atom()) -> {error, nhttpc:error()}.
invalid_request(Kind) ->
    {error, {invalid_request, Kind}}.

-doc "Pool is shutting down / draining.".
-spec pool_closed() -> {error, nhttpc:error()}.
pool_closed() ->
    {error, pool_closed}.

-doc "Pool is full and the caller may not block further.".
-spec pool_full() -> {error, nhttpc:error()}.
pool_full() ->
    {error, pool_full}.

-doc "Protocol-level error during framing or parsing.".
-spec protocol_error(nhttp_lib:version(), nhttpc:protocol_detail()) ->
    {error, nhttpc:error()}.
protocol_error(Version, Detail) ->
    {error, {protocol_error, Version, Detail}}.

-doc "Redirect handling refused or exhausted.".
-spec redirect_error(nhttpc:redirect_reason()) -> {error, nhttpc:error()}.
redirect_error(Reason) ->
    {error, {redirect_error, Reason}}.

-doc "Retry budget exhausted, carrying the last typed error.".
-spec retries_exhausted(pos_integer(), nhttpc:error()) -> {error, nhttpc:error()}.
retries_exhausted(Attempts, LastError) ->
    {error, {retries_exhausted, Attempts, LastError}}.

-doc "Stream-level error (HTTP/2, HTTP/3, or size cap).".
-spec stream_error(nhttpc:stream_reason()) -> {error, nhttpc:error()}.
stream_error(Reason) ->
    {error, {stream_error, Reason}}.

-doc "Timeout for one of the documented phases.".
-spec timeout(connect | request | checkout | idle | body) -> {error, nhttpc:error()}.
timeout(Phase) ->
    {error, {timeout, Phase}}.

-doc "TLS handshake or option failure.".
-spec tls_error(nhttpc:tls_reason()) -> {error, nhttpc:error()}.
tls_error(Reason) ->
    {error, {tls_error, Reason}}.

%%%-----------------------------------------------------------------------------
%% MAPPERS
%%%-----------------------------------------------------------------------------
-doc """
Map a `file:posix()` / `file:*` error into a canonical `file_error/0`.
""".
-spec from_file(nhttpc:file_reason()) ->
    {error, nhttpc:error()}.
from_file(Reason) ->
    {error, {file_error, Reason}}.

-doc """
Map an HTTP/1.1 parse error into a canonical protocol error.
""".
-spec from_h1(nhttp_h1:parse_error()) -> {error, nhttpc:error()}.
from_h1(ParseError) ->
    {error, {protocol_error, http1_1, ParseError}}.

-doc """
Map an HTTP/2 error code or upstream library error into a canonical
protocol or stream error.
""".
-spec from_h2(nhttp_lib:error_code() | nhttp_lib:error()) ->
    {error, nhttpc:error()}.
from_h2({connection_error, ErrorCode, _Reason}) ->
    {error, {protocol_error, http2, {h2, ErrorCode}}};
from_h2({stream_error, _StreamId, ErrorCode, _Reason}) ->
    {error, {stream_error, {protocol, {h2, ErrorCode}}}};
from_h2({protocol_violation, _Atom, _Bin} = Violation) ->
    {error, {protocol_error, http2, Violation}};
from_h2(ErrorCode) when is_atom(ErrorCode); is_integer(ErrorCode) ->
    {error, {protocol_error, http2, {h2, ErrorCode}}}.

-doc """
Map an HTTP/3 error code or upstream library error into a canonical
protocol or stream error.
""".
-spec from_h3(nhttp_lib:error_code() | nhttp_lib:error()) ->
    {error, nhttpc:error()}.
from_h3({connection_error, ErrorCode, _Reason}) ->
    {error, {protocol_error, http3, {h3, ErrorCode}}};
from_h3({stream_error, _StreamId, ErrorCode, _Reason}) ->
    {error, {stream_error, {protocol, {h3, ErrorCode}}}};
from_h3({protocol_violation, _Atom, _Bin} = Violation) ->
    {error, {protocol_error, http3, Violation}};
from_h3(ErrorCode) when is_atom(ErrorCode); is_integer(ErrorCode) ->
    {error, {protocol_error, http3, {h3, ErrorCode}}}.

-doc """
Map a TLS / `ssl` error into a canonical `tls_error/0`.
Handles the common shapes returned by `ssl:connect/4`,
`ssl:setopts/2`, the `{tls_alert, _}` record from a peer-sent alert,
and bare atoms for hostname / option failures.
""".
-spec from_ssl(term()) -> {error, nhttpc:error()}.
from_ssl({tls_alert, _} = Alert) ->
    {error, {tls_error, Alert}};
from_ssl({options, _} = Options) ->
    {error, {tls_error, Options}};
from_ssl({handshake_failure, Reason}) ->
    {error, {tls_error, {handshake, Reason}}};
from_ssl({hostname_check_failed, Hostname}) when is_binary(Hostname) ->
    {error, {tls_error, {hostname_mismatch, Hostname}}};
from_ssl({hostname_check_failed, Hostname}) when is_list(Hostname) ->
    {error, {tls_error, {hostname_mismatch, list_to_binary(Hostname)}}};
from_ssl(closed) ->
    {error, {connect_error, closed}};
from_ssl(Reason) ->
    {error, {tls_error, {handshake, Reason}}}.

-doc """
Generic dispatcher used at the public boundary. Pass-through for values
already in the canonical taxonomy; otherwise normalises the internal
`nhttp_error`-style map shapes, bare posix atoms, and the tuple forms
produced by the connection / pool / stream modules.
Always returns an `{error, nhttpc:error()}` tuple.
""".
-spec wrap(term()) -> {error, nhttpc:error()}.
wrap({error, Reason}) ->
    wrap_reason(Reason);
wrap(Reason) ->
    wrap_reason(Reason).

-doc """
Wrap an `{ok, _} | {error, _}` result so the error arm is canonical.
The `ok` and `{ok, _}` arms pass through unchanged.
""".
-spec wrap_result(ok | {ok, T} | {error, term()}) ->
    ok | {ok, T} | {error, nhttpc:error()}.
wrap_result(ok) ->
    ok;
wrap_result({ok, _} = Ok) ->
    Ok;
wrap_result({error, _} = Error) ->
    wrap(Error).

%%%-----------------------------------------------------------------------------
%% INTERNAL
%%%-----------------------------------------------------------------------------
-spec classify_stream_stopped(term()) -> {error, nhttpc:error()}.
classify_stream_stopped({file_error, R}) ->
    case file_reason(R) of
        {ok, Reason} -> {error, {body_error, {file_read, Reason}}};
        error -> {error, {stream_error, cancelled}}
    end;
classify_stream_stopped({response_too_large, _, _}) ->
    {error, {stream_error, exceeded_max_response_size}};
classify_stream_stopped(Reason) when
    Reason =:= cancel_requested;
    Reason =:= user_requested_stop;
    Reason =:= cancelled
->
    {error, {stream_error, cancelled}};
classify_stream_stopped(_Reason) ->
    {error, {stream_error, cancelled}}.

-spec classify_tls(term()) -> nhttpc:tls_reason().
classify_tls({tls_alert, _} = Alert) ->
    Alert;
classify_tls({options, _} = Options) ->
    Options;
classify_tls({handshake_failure, R}) ->
    {handshake, R};
classify_tls({hostname_check_failed, H}) when is_binary(H) ->
    {hostname_mismatch, H};
classify_tls({hostname_check_failed, H}) when is_list(H) ->
    {hostname_mismatch, list_to_binary(H)};
classify_tls(Reason) ->
    {handshake, Reason}.

-spec file_reason(dynamic()) -> {ok, nhttpc:file_reason()} | error.
file_reason(R) when
    R =:= eacces;
    R =:= eagain;
    R =:= ebadf;
    R =:= ebadmsg;
    R =:= ebusy;
    R =:= edeadlk;
    R =:= edeadlock;
    R =:= edquot;
    R =:= eexist;
    R =:= efault;
    R =:= efbig;
    R =:= eftype;
    R =:= eintr;
    R =:= einval;
    R =:= eio;
    R =:= eisdir;
    R =:= eloop;
    R =:= emfile;
    R =:= emlink;
    R =:= emultihop;
    R =:= enametoolong;
    R =:= enfile;
    R =:= enobufs;
    R =:= enodev;
    R =:= enolck;
    R =:= enolink;
    R =:= enoent;
    R =:= enomem;
    R =:= enospc;
    R =:= enosr;
    R =:= enostr;
    R =:= enosys;
    R =:= enotblk;
    R =:= enotdir;
    R =:= enotsup;
    R =:= enxio;
    R =:= eopnotsupp;
    R =:= eoverflow;
    R =:= eperm;
    R =:= epipe;
    R =:= erange;
    R =:= erofs;
    R =:= espipe;
    R =:= esrch;
    R =:= estale;
    R =:= etxtbsy;
    R =:= exdev;
    R =:= badarg;
    R =:= terminated;
    R =:= system_limit
->
    {ok, R};
file_reason(_) ->
    error.

-spec wrap_connection_map(map()) -> {error, nhttpc:error()}.
wrap_connection_map(#{type := connect_failed, posix := P}) ->
    {error, {connect_error, P}};
wrap_connection_map(#{type := connect_refused, posix := P}) ->
    {error, {connect_error, P}};
wrap_connection_map(#{type := connect_timeout}) ->
    {error, {timeout, connect}};
wrap_connection_map(#{type := tls_error, reason := R}) ->
    {_, _} = E = classify_tls(R),
    {error, {tls_error, E}};
wrap_connection_map(#{type := alpn_error, reason := R}) ->
    {error, {tls_error, {handshake, {alpn, R}}}};
wrap_connection_map(#{type := not_ready}) ->
    {error, {connect_error, not_ready}};
wrap_connection_map(Reason) ->
    {error, {connect_error, Reason}}.

-spec wrap_http2_map(map()) -> {error, nhttpc:error()}.
wrap_http2_map(#{type := goaway, error_code := EC, debug_data := DebugData}) when
    is_binary(DebugData), DebugData =/= <<>>
->
    {error, {protocol_error, http2, {h2, EC, DebugData}}};
wrap_http2_map(#{type := goaway, error_code := EC}) ->
    {error, {protocol_error, http2, {h2, EC}}};
wrap_http2_map(#{type := stream_reset, error_code := EC}) ->
    {error, {stream_error, {protocol, {h2, EC}}}};
wrap_http2_map(#{type := cancelled}) ->
    {error, {stream_error, cancelled}};
wrap_http2_map(#{type := stream_closed, reason := R}) ->
    {error, {stream_error, {protocol, R}}};
wrap_http2_map(#{type := refused}) ->
    {error, {stream_error, {protocol, refused_stream}}};
wrap_http2_map(#{type := rate_limited}) ->
    {error, {protocol_error, http2, {h2, enhance_your_calm}}};
wrap_http2_map(#{type := flow_control_error, reason := R}) ->
    {error, {stream_error, {flow_control, R}}};
wrap_http2_map(Reason) ->
    {error, {protocol_error, http2, Reason}}.

-spec wrap_pool_map(map()) -> {error, nhttpc:error()}.
wrap_pool_map(#{type := checkout_timeout}) ->
    {error, {timeout, checkout}};
wrap_pool_map(#{type := draining}) ->
    {error, pool_closed};
wrap_pool_map(#{type := exhausted}) ->
    {error, pool_full};
wrap_pool_map(#{type := no_connections_available}) ->
    {error, pool_full};
wrap_pool_map(Reason) ->
    {error, {connect_error, {pool, Reason}}}.

-spec wrap_reason(term()) -> {error, nhttpc:error()}.
wrap_reason({invalid_request, _} = E) ->
    {error, E};
wrap_reason({invalid_opts, _} = E) ->
    {error, E};
wrap_reason({connect_error, _} = E) ->
    {error, E};
wrap_reason({tls_error, _} = E) ->
    {error, E};
wrap_reason({timeout, Phase} = E) when
    Phase =:= connect;
    Phase =:= request;
    Phase =:= checkout;
    Phase =:= idle;
    Phase =:= body
->
    {error, E};
wrap_reason({protocol_error, _, _} = E) ->
    {error, E};
wrap_reason({http_error, _, _} = E) ->
    {error, E};
wrap_reason({redirect_error, _} = E) ->
    {error, E};
wrap_reason({retries_exhausted, _, _} = E) ->
    {error, E};
wrap_reason({stream_error, _} = E) ->
    {error, E};
wrap_reason({body_error, _} = E) ->
    {error, E};
wrap_reason({cookie_jar_error, _} = E) ->
    {error, E};
wrap_reason({file_error, _} = E) ->
    {error, E};
wrap_reason(pool_full) ->
    {error, pool_full};
wrap_reason(pool_closed) ->
    {error, pool_closed};
wrap_reason({connection, Reason}) ->
    wrap_connection_map(Reason);
wrap_reason({request, Reason}) ->
    wrap_request_map(Reason);
wrap_reason({http2, Reason}) ->
    wrap_http2_map(Reason);
wrap_reason({pool, Reason}) ->
    wrap_pool_map(Reason);
wrap_reason({redirect_loop, Url}) when is_binary(Url) ->
    {error, {redirect_error, {redirect_loop, Url}}};
wrap_reason({invalid_redirect_location, _} = Reason) ->
    {error, {redirect_error, Reason}};
wrap_reason({connection_down, _}) ->
    {error, {connect_error, closed}};
wrap_reason(closed) ->
    {error, {connect_error, closed}};
wrap_reason(timeout) ->
    {error, {timeout, request}};
wrap_reason(Reason) ->
    {error, {connect_error, Reason}}.

-spec wrap_request_map(map()) -> {error, nhttpc:error()}.
wrap_request_map(#{type := request_timeout}) ->
    {error, {timeout, request}};
wrap_request_map(#{type := body_timeout}) ->
    {error, {timeout, body}};
wrap_request_map(#{type := connection_closed}) ->
    {error, {connect_error, closed}};
wrap_request_map(#{type := malformed_response, parse_error := PE}) ->
    {error, {protocol_error, http1_1, PE}};
wrap_request_map(#{type := max_redirects_exceeded}) ->
    {error, {redirect_error, max_redirects_exceeded}};
wrap_request_map(#{type := redirect_loop, url := U}) ->
    {error, {redirect_error, {redirect_loop, U}}};
wrap_request_map(#{type := recv_error, posix := P}) ->
    {error, {connect_error, P}};
wrap_request_map(#{type := send_error, posix := P}) ->
    {error, {connect_error, P}};
wrap_request_map(#{type := response_too_large}) ->
    {error, {stream_error, exceeded_max_response_size}};
wrap_request_map(#{type := file_error, operation := open, reason := R}) ->
    {error, {body_error, {file_open, R}}};
wrap_request_map(#{type := file_error, operation := read, reason := R}) ->
    {error, {body_error, {file_read, R}}};
wrap_request_map(#{type := file_error, reason := R}) ->
    {error, {body_error, {file_read, R}}};
wrap_request_map(#{type := upload_error, reason := R}) ->
    {error, {body_error, {iterator, R}}};
wrap_request_map(#{type := stream_stopped, reason := R}) ->
    classify_stream_stopped(R);
wrap_request_map(Reason) ->
    {error, {stream_error, {protocol, Reason}}}.
