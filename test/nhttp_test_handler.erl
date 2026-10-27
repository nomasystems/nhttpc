%%%-----------------------------------------------------------------------------
%%% @doc Simple test handler for client tests.
%%%
%%% Endpoints:
%%%   - GET /echo - Echo request info
%%%   - POST /echo - Echo request body
%%%   - GET /gzip - Return gzip compressed response
%%%   - GET /deflate - Return deflate compressed response
%%%   - GET /identity - Return uncompressed response
%%%   - GET /set-cookie - Set a simple cookie
%%%   - GET /set-cookie-options - Set cookie with options
%%%   - GET /check-cookie - Check if Cookie header was sent
%%%   - GET /multi-cookie - Set multiple cookies
%%%   - GET /redirect/{301,302,303,307,308} - Redirect with status code
%%%   - GET /redirect-chain - Chain of redirects
%%%   - GET /redirect-loop - Infinite redirect loop
%%%   - GET /redirect-relative - Relative path redirect
%%%   - POST /redirect-307 - POST redirect that preserves method
%%%   - GET /final - Final destination for redirects
%%%   - GET /check-auth - Check if Authorization header was sent
%%%   - GET /retry-503 - Always return 503 Service Unavailable
%%%   - GET /retry-502 - Always return 502 Bad Gateway
%%%   - GET /retry-504 - Always return 504 Gateway Timeout
%%%   - GET /retry-500 - Always return 500 (not retryable by default)
%%%   - GET /retry-flaky/ID/FAIL - Fail FAIL times then succeed for request ID
%%%   - GET /retry-redirect/ID - First returns 503, then redirect to /final
%%%
%%% Streaming Endpoints (Stage E):
%%%   - GET /stream/small - Small (100 byte) response for streaming
%%%   - GET /stream/large - Large (100KB) response for streaming
%%%   - GET /stream/chunked - Chunked transfer encoding response
%%%   - GET /stream/progress - Response with Content-Length for progress
%%%   - GET /stream/gzip - Gzip compressed streamed response
%%%
%%% Upload Endpoints (Stage F):
%%%   - POST /upload/echo - Echo back the uploaded body and size
%%%   - PUT /upload/echo - Echo back the uploaded body and size
%%%   - POST /upload/hash - Return MD5 hash and size of uploaded body
%%%   - POST /upload/verify - Upload data and verify expected checksum
%%%   - POST /upload/large - Accept large upload and return size
%%%   - POST /upload/chunked - Accept chunked upload and return size
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttp_test_handler).

-behaviour(nhttp_handler).

-export([init/1, handle_request/2, handle_request_body/3]).

-spec init(term()) -> {ok, #{}}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp_lib:request(), State) ->
    {reply, nhttp_lib:response(), State}
    | {accept_body, {nhttp_lib:request(), [binary()]}, State}
    | {stream, nhttp_stream:spec(), State}
when
    State :: #{}.
%% When the body is streaming, accept it; once fully buffered, dispatch
%% via handle_request_body/3 below.
handle_request(#{body := streaming} = Req, State) ->
    {accept_body, {Req, []}, State};
%% GET /empty - Return empty body with Content-Length: 0
handle_request(#{method := get, path := <<"/empty", _/binary>>}, State) ->
    Resp = #{
        status => 200,
        headers => [
            {<<"content-type">>, <<"text/plain">>},
            {<<"content-length">>, <<"0">>}
        ],
        body => <<>>
    },
    {reply, Resp, State};
%% GET /chunked - Return chunked transfer encoded response
handle_request(#{method := get, path := <<"/chunked", _/binary>>}, State) ->
    %% Note: Actual chunked encoding is done by the server layer
    %% This just returns content without explicit Content-Length
    Body = <<"chunk1\nchunk2\nchunk3\n">>,
    Resp = nhttp_resp:ok(
        [{<<"content-type">>, <<"text/plain">>}],
        Body
    ),
    {reply, Resp, State};
%% GET /compressed/gzip - Gzip compressed response for streaming tests
handle_request(#{method := get, path := <<"/compressed/gzip", _/binary>>}, State) ->
    PlainBody = <<"Compressed response body for streaming tests">>,
    case nhttp_compress:compress(PlainBody, gzip, 6) of
        {ok, CompressedBody} ->
            Resp = nhttp_resp:ok(
                [{<<"content-type">>, <<"text/plain">>}, {<<"content-encoding">>, <<"gzip">>}],
                CompressedBody
            ),
            {reply, Resp, State};
        {error, _Reason} ->
            Resp = nhttp_resp:ok([{<<"content-type">>, <<"text/plain">>}], PlainBody),
            {reply, Resp, State}
    end;
%% GET /compressed/deflate - Deflate compressed response for streaming tests
handle_request(#{method := get, path := <<"/compressed/deflate", _/binary>>}, State) ->
    PlainBody = <<"Compressed response body for streaming tests">>,
    case nhttp_compress:compress(PlainBody, deflate, 6) of
        {ok, CompressedBody} ->
            Resp = nhttp_resp:ok(
                [{<<"content-type">>, <<"text/plain">>}, {<<"content-encoding">>, <<"deflate">>}],
                CompressedBody
            ),
            {reply, Resp, State};
        {error, _Reason} ->
            Resp = nhttp_resp:ok([{<<"content-type">>, <<"text/plain">>}], PlainBody),
            {reply, Resp, State}
    end;
%% GET /gzip - Return gzip compressed response
handle_request(#{method := get, path := <<"/gzip", _/binary>>}, State) ->
    PlainBody = <<"Hello, compressed world! This is a gzip response.">>,
    case nhttp_compress:compress(PlainBody, gzip, 6) of
        {ok, CompressedBody} ->
            Resp = nhttp_resp:ok(
                [{<<"content-type">>, <<"text/plain">>}, {<<"content-encoding">>, <<"gzip">>}],
                CompressedBody
            ),
            {reply, Resp, State};
        {error, _Reason} ->
            %% Fallback to uncompressed
            Resp = nhttp_resp:ok([{<<"content-type">>, <<"text/plain">>}], PlainBody),
            {reply, Resp, State}
    end;
%% GET /deflate - Return deflate compressed response
handle_request(#{method := get, path := <<"/deflate", _/binary>>}, State) ->
    PlainBody = <<"Hello, compressed world! This is a deflate response.">>,
    case nhttp_compress:compress(PlainBody, deflate, 6) of
        {ok, CompressedBody} ->
            Resp = nhttp_resp:ok(
                [{<<"content-type">>, <<"text/plain">>}, {<<"content-encoding">>, <<"deflate">>}],
                CompressedBody
            ),
            {reply, Resp, State};
        {error, _Reason} ->
            %% Fallback to uncompressed
            Resp = nhttp_resp:ok([{<<"content-type">>, <<"text/plain">>}], PlainBody),
            {reply, Resp, State}
    end;
%% GET /identity - Return uncompressed response explicitly
handle_request(#{method := get, path := <<"/identity", _/binary>>}, State) ->
    Body = <<"Hello, uncompressed world!">>,
    Resp = nhttp_resp:ok(
        [{<<"content-type">>, <<"text/plain">>}, {<<"content-encoding">>, <<"identity">>}],
        Body
    ),
    {reply, Resp, State};
%% GET /set-cookie - Set a simple session cookie
handle_request(#{method := get, path := <<"/set-cookie", _/binary>>}, State) ->
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"text/plain">>},
            {<<"set-cookie">>, <<"session=abc123">>}
        ],
        <<"Cookie set">>
    ),
    {reply, Resp, State};
%% GET /set-cookie-options - Set cookie with various options
handle_request(#{method := get, path := <<"/set-cookie-options", _/binary>>}, State) ->
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"text/plain">>},
            {<<"set-cookie">>, <<"auth=token123; Path=/; Max-Age=3600; HttpOnly; Secure">>}
        ],
        <<"Cookie with options set">>
    ),
    {reply, Resp, State};
%% GET /check-cookie - Check if Cookie header was sent and echo it
handle_request(
    #{method := get, path := <<"/check-cookie", _/binary>>, headers := Headers}, State
) ->
    CookieValue = proplists:get_value(<<"cookie">>, Headers, <<"none">>),
    Body = iolist_to_binary([<<"Cookie: ">>, CookieValue]),
    Resp = nhttp_resp:ok([{<<"content-type">>, <<"text/plain">>}], Body),
    {reply, Resp, State};
%% GET /multi-cookie - Set multiple cookies
handle_request(#{method := get, path := <<"/multi-cookie", _/binary>>}, State) ->
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"text/plain">>},
            {<<"set-cookie">>, <<"cookie1=value1; Path=/">>},
            {<<"set-cookie">>, <<"cookie2=value2; Path=/">>},
            {<<"set-cookie">>, <<"cookie3=value3; Path=/api">>}
        ],
        <<"Multiple cookies set">>
    ),
    {reply, Resp, State};
%% ANY /redirect-no-location - 301 without Location header
handle_request(#{path := <<"/redirect-no-location", _/binary>>}, State) ->
    Resp = #{
        status => 301,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"Moved but no location">>
    },
    {reply, Resp, State};
%% POST /redirect-301-post - 301 for POST (becomes GET)
handle_request(#{method := post, path := <<"/redirect-301-post", _/binary>>}, State) ->
    Resp = #{
        status => 301,
        headers => [
            {<<"location">>, <<"/check-method">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Moved Permanently">>
    },
    {reply, Resp, State};
%% QUERY /redirect-query/{301,302} - 301 or 302 for QUERY (preserved, RFC 10008)
handle_request(
    #{method := query, path := <<"/redirect-query/", Status:3/binary, _/binary>>}, State
) ->
    Resp = #{
        status => binary_to_integer(Status),
        headers => [
            {<<"location">>, <<"/check-method">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Redirect">>
    },
    {reply, Resp, State};
%% HEAD /redirect-301-head - 301 for HEAD (preserved)
handle_request(#{method := head, path := <<"/redirect-301-head", _/binary>>}, State) ->
    Resp = #{
        status => 301,
        headers => [
            {<<"location">>, <<"/echo">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<>>
    },
    {reply, Resp, State};
%% GET /redirect-with-query - 302 redirect with query string in location
handle_request(#{method := get, path := <<"/redirect-with-query", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [
            {<<"location">>, <<"/echo?redirected=true&from=query">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Redirect with query">>
    },
    {reply, Resp, State};
%% GET /redirect-301 - Permanent redirect
handle_request(#{method := get, path := <<"/redirect-301", _/binary>>}, State) ->
    Resp = #{
        status => 301,
        headers => [
            {<<"location">>, <<"/final">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Moved Permanently">>
    },
    {reply, Resp, State};
%% GET /redirect-302 - Temporary redirect
handle_request(#{method := get, path := <<"/redirect-302", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [
            {<<"location">>, <<"/final">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Found">>
    },
    {reply, Resp, State};
%% GET /redirect-303 - See Other (always GET)
handle_request(#{method := _, path := <<"/redirect-303", _/binary>>}, State) ->
    Resp = #{
        status => 303,
        headers => [
            {<<"location">>, <<"/final">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"See Other">>
    },
    {reply, Resp, State};
%% GET/POST /redirect-307 - Temporary redirect preserving method
handle_request(#{method := _, path := <<"/redirect-307", _/binary>>}, State) ->
    Resp = #{
        status => 307,
        headers => [
            {<<"location">>, <<"/check-method">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Temporary Redirect">>
    },
    {reply, Resp, State};
%% GET/POST /redirect-308 - Permanent redirect preserving method
handle_request(#{method := _, path := <<"/redirect-308", _/binary>>}, State) ->
    Resp = #{
        status => 308,
        headers => [
            {<<"location">>, <<"/check-method">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Permanent Redirect">>
    },
    {reply, Resp, State};
%% GET /redirect-chain-3 - Third step in redirect chain (match before /redirect-chain-2 and /redirect-chain)
handle_request(#{method := get, path := <<"/redirect-chain-3", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [
            {<<"location">>, <<"/final">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Step 3">>
    },
    {reply, Resp, State};
%% GET /redirect-chain-2 - Second step in redirect chain (match before /redirect-chain)
handle_request(#{method := get, path := <<"/redirect-chain-2", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [
            {<<"location">>, <<"/redirect-chain-3">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Step 2">>
    },
    {reply, Resp, State};
%% GET /redirect-chain - First step in redirect chain
handle_request(#{method := get, path := <<"/redirect-chain", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [
            {<<"location">>, <<"/redirect-chain-2">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Step 1">>
    },
    {reply, Resp, State};
%% GET /redirect-loop-2 - Second step in loop (match before /redirect-loop)
handle_request(#{method := get, path := <<"/redirect-loop-2", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [
            {<<"location">>, <<"/redirect-loop">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Loop 2">>
    },
    {reply, Resp, State};
%% GET /redirect-loop - Infinite redirect loop
handle_request(#{method := get, path := <<"/redirect-loop", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [
            {<<"location">>, <<"/redirect-loop-2">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Loop 1">>
    },
    {reply, Resp, State};
%% GET /redirect-absolute/PORT - Redirect with absolute URL to a different port
handle_request(#{method := get, path := <<"/redirect-absolute/", PortBin/binary>>}, State) ->
    CleanPort =
        case binary:split(PortBin, <<"?">>) of
            [P, _] -> P;
            [P] -> P
        end,
    Location = <<"http://127.0.0.1:", CleanPort/binary, "/final">>,
    Resp = #{
        status => 301,
        headers => [
            {<<"location">>, Location},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Redirecting to absolute URL">>
    },
    {reply, Resp, State};
%% GET /redirect-cross-origin/PORT - Cross-origin redirect (strips auth headers)
handle_request(
    #{method := get, path := <<"/redirect-cross-origin/", PortBin/binary>>}, State
) ->
    CleanPort =
        case binary:split(PortBin, <<"?">>) of
            [P, _] -> P;
            [P] -> P
        end,
    Location = <<"http://127.0.0.1:", CleanPort/binary, "/check-auth">>,
    Resp = #{
        status => 302,
        headers => [
            {<<"location">>, Location},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Cross-origin redirect">>
    },
    {reply, Resp, State};
%% GET /redirect-relative - Relative path redirect
handle_request(#{method := get, path := <<"/api/redirect-relative", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [
            {<<"location">>, <<"../final">>},
            {<<"content-type">>, <<"text/plain">>}
        ],
        body => <<"Relative Redirect">>
    },
    {reply, Resp, State};
%% GET /final - Final destination for redirects
handle_request(#{method := get, path := <<"/final", _/binary>>}, State) ->
    Resp = nhttp_resp:ok(
        [{<<"content-type">>, <<"text/plain">>}],
        <<"Final destination reached">>
    ),
    {reply, Resp, State};
%% ANY /check-method - Echo the request method (for 307/308 tests)
handle_request(
    #{method := Method, path := <<"/check-method", _/binary>>, body := Body}, State
) ->
    MethodBin = nhttp_lib:encode_method(Method),
    RespBody = iolist_to_binary([<<"Method: ">>, MethodBin, <<"\nBody: ">>, Body]),
    Resp = nhttp_resp:ok(
        [{<<"content-type">>, <<"text/plain">>}],
        RespBody
    ),
    {reply, Resp, State};
%% GET /check-auth - Check if Authorization header was sent
handle_request(
    #{method := get, path := <<"/check-auth", _/binary>>, headers := Headers}, State
) ->
    AuthValue = proplists:get_value(<<"authorization">>, Headers, <<"none">>),
    Body = iolist_to_binary([<<"Authorization: ">>, AuthValue]),
    Resp = nhttp_resp:ok([{<<"content-type">>, <<"text/plain">>}], Body),
    {reply, Resp, State};
%% GET /retry-503 - Always return 503 Service Unavailable
handle_request(#{method := get, path := <<"/retry-503", _/binary>>}, State) ->
    Resp = #{
        status => 503,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"Service Unavailable">>
    },
    {reply, Resp, State};
%% GET /retry-502 - Always return 502 Bad Gateway
handle_request(#{method := get, path := <<"/retry-502", _/binary>>}, State) ->
    Resp = #{
        status => 502,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"Bad Gateway">>
    },
    {reply, Resp, State};
%% GET /retry-504 - Always return 504 Gateway Timeout
handle_request(#{method := get, path := <<"/retry-504", _/binary>>}, State) ->
    Resp = #{
        status => 504,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"Gateway Timeout">>
    },
    {reply, Resp, State};
%% GET /retry-500 - Always return 500 (not retryable by default)
handle_request(#{method := get, path := <<"/retry-500", _/binary>>}, State) ->
    Resp = #{
        status => 500,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"Internal Server Error">>
    },
    {reply, Resp, State};
%% GET /retry-flaky/ID/FAIL - Fail FAIL times then succeed for request ID
%% Uses ETS to track attempt count per request id
handle_request(#{method := get, path := <<"/retry-flaky/", Rest/binary>>}, State) ->
    %% Parse path segments: ID/FAIL
    {Id, FailCount} =
        case binary:split(Rest, <<"/">>) of
            [IdPart, FailPart] ->
                %% Strip query string if present
                CleanFail =
                    case binary:split(FailPart, <<"?">>) of
                        [F, _] -> F;
                        [F] -> F
                    end,
                {IdPart, binary_to_integer(CleanFail)};
            [IdPart] ->
                %% Strip query string if present
                CleanId =
                    case binary:split(IdPart, <<"?">>) of
                        [I, _] -> I;
                        [I] -> I
                    end,
                {CleanId, 2}
        end,

    %% Get ETS table for tracking attempts
    Tab = ensure_retry_table(),
    Key = {retry_flaky, Id},
    Attempts = ets:update_counter(Tab, Key, 1, {Key, 0}),

    case Attempts =< FailCount of
        true ->
            %% Still failing
            Resp = #{
                status => 503,
                headers => [{<<"content-type">>, <<"text/plain">>}],
                body => iolist_to_binary([
                    <<"Attempt ">>, integer_to_binary(Attempts), <<" failed">>
                ])
            },
            {reply, Resp, State};
        false ->
            %% Success - reset counter for next test
            ets:delete(Tab, Key),
            Resp = nhttp_resp:ok(
                [{<<"content-type">>, <<"text/plain">>}],
                iolist_to_binary([
                    <<"Success after ">>, integer_to_binary(Attempts), <<" attempts">>
                ])
            ),
            {reply, Resp, State}
    end;
%%%-----------------------------------------------------------------------------
%%% STREAMING ENDPOINTS (Stage E)
%%%-----------------------------------------------------------------------------

%% GET /stream/small - Small (100 byte) response for streaming
handle_request(#{method := get, path := <<"/stream/small", _/binary>>}, State) ->
    %% Generate 100 bytes of content
    Body = binary:copy(<<"x">>, 100),
    Resp = nhttp_resp:ok(
        [{<<"content-type">>, <<"application/octet-stream">>}],
        Body
    ),
    {reply, Resp, State};
%% GET /stream/large - Large (100KB) response for streaming
handle_request(#{method := get, path := <<"/stream/large", _/binary>>}, State) ->
    %% Generate 100KB of content with pattern for verification

    % 16 bytes
    Pattern = <<"0123456789ABCDEF">>,
    % 102400 bytes = 100KB
    Body = binary:copy(Pattern, 6400),
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"application/octet-stream">>},
            {<<"content-length">>, integer_to_binary(byte_size(Body))}
        ],
        Body
    ),
    {reply, Resp, State};
%% GET /stream/chunked - Response without Content-Length (server decides encoding)
handle_request(#{method := get, path := <<"/stream/chunked", _/binary>>}, State) ->
    %% Don't set Content-Length so server may use connection: close or chunked
    Body = <<"chunk1-data\nchunk2-data\nchunk3-data\n">>,
    Resp = nhttp_resp:ok(
        [{<<"content-type">>, <<"text/plain">>}],
        Body
    ),
    {reply, Resp, State};
%% GET /stream/progress - Response with Content-Length for progress tracking
handle_request(#{method := get, path := <<"/stream/progress", _/binary>>}, State) ->
    %% Generate 10KB of content with explicit content-length

    % 10000 bytes
    Body = binary:copy(<<"progress">>, 1250),
    Size = byte_size(Body),
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"application/octet-stream">>},
            {<<"content-length">>, integer_to_binary(Size)}
        ],
        Body
    ),
    {reply, Resp, State};
%% GET /stream/gzip - Gzip compressed streamed response
handle_request(#{method := get, path := <<"/stream/gzip", _/binary>>}, State) ->
    % ~2300 bytes
    PlainBody = binary:copy(<<"compressed-stream-data\n">>, 100),
    case nhttp_compress:compress(PlainBody, gzip, 6) of
        {ok, CompressedBody} ->
            Resp = nhttp_resp:ok(
                [
                    {<<"content-type">>, <<"application/octet-stream">>},
                    {<<"content-encoding">>, <<"gzip">>}
                ],
                CompressedBody
            ),
            {reply, Resp, State};
        {error, _Reason} ->
            Resp = nhttp_resp:ok(
                [{<<"content-type">>, <<"application/octet-stream">>}],
                PlainBody
            ),
            {reply, Resp, State}
    end;
%%%-----------------------------------------------------------------------------
%%% RETRY ENDPOINTS (Stage D)
%%%-----------------------------------------------------------------------------

%% GET /retry-redirect/ID - First returns 503, then redirect, then success (complex scenario)
handle_request(
    #{method := get, path := <<"/retry-redirect/", Rest/binary>>}, State
) ->
    %% Extract ID from path
    Id =
        case binary:split(Rest, <<"?">>) of
            [IdPart, _] -> IdPart;
            [IdPart] -> IdPart
        end,

    Tab = ensure_retry_table(),
    Key = {retry_redirect, Id},
    Attempts = ets:update_counter(Tab, Key, 1, {Key, 0}),

    case Attempts of
        1 ->
            %% First attempt: 503
            Resp = #{
                status => 503,
                headers => [{<<"content-type">>, <<"text/plain">>}],
                body => <<"Service temporarily unavailable">>
            },
            {reply, Resp, State};
        _ ->
            %% Second attempt: redirect to final
            ets:delete(Tab, Key),
            Resp = #{
                status => 302,
                headers => [
                    {<<"location">>, <<"/final">>},
                    {<<"content-type">>, <<"text/plain">>}
                ],
                body => <<"Redirecting">>
            },
            {reply, Resp, State}
    end;
%% GET /redirect-max - Redirect chain that exceeds max limit
handle_request(#{method := get, path := <<"/redirect-max-1", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [{<<"location">>, <<"/redirect-max-2">>}],
        body => <<>>
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/redirect-max-2", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [{<<"location">>, <<"/redirect-max-3">>}],
        body => <<>>
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/redirect-max-3", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [{<<"location">>, <<"/redirect-max-4">>}],
        body => <<>>
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/redirect-max-4", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [{<<"location">>, <<"/redirect-max-5">>}],
        body => <<>>
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/redirect-max-5", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [{<<"location">>, <<"/redirect-max-6">>}],
        body => <<>>
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/redirect-max-6", _/binary>>}, State) ->
    Resp = #{
        status => 302,
        headers => [{<<"location">>, <<"/final">>}],
        body => <<>>
    },
    {reply, Resp, State};
%%%-----------------------------------------------------------------------------
%%% UPLOAD ENDPOINTS (Stage F)
%%%-----------------------------------------------------------------------------

%% POST /upload/echo - Echo back the uploaded body and size
handle_request(
    #{method := post, path := <<"/upload/echo", _/binary>>, body := Body}, State
) ->
    Size = byte_size(Body),
    RespBody = iolist_to_binary([
        <<"Received: ">>,
        integer_to_binary(Size),
        <<" bytes\n">>,
        <<"Body: ">>,
        Body
    ]),
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"text/plain">>},
            {<<"x-received-size">>, integer_to_binary(Size)}
        ],
        RespBody
    ),
    {reply, Resp, State};
%% PUT /upload/echo - Echo back the uploaded body and size
handle_request(#{method := put, path := <<"/upload/echo", _/binary>>, body := Body}, State) ->
    Size = byte_size(Body),
    RespBody = iolist_to_binary([
        <<"Received: ">>,
        integer_to_binary(Size),
        <<" bytes\n">>,
        <<"Body: ">>,
        Body
    ]),
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"text/plain">>},
            {<<"x-received-size">>, integer_to_binary(Size)}
        ],
        RespBody
    ),
    {reply, Resp, State};
%% POST /upload/hash - Return MD5 hash and size of uploaded body
handle_request(
    #{method := post, path := <<"/upload/hash", _/binary>>, body := Body}, State
) ->
    Size = byte_size(Body),
    Hash = crypto:hash(md5, Body),
    HexHash = binary:encode_hex(Hash, lowercase),
    RespBody = iolist_to_binary([
        <<"Size: ">>,
        integer_to_binary(Size),
        <<"\n">>,
        <<"MD5: ">>,
        HexHash
    ]),
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"text/plain">>},
            {<<"x-received-size">>, integer_to_binary(Size)},
            {<<"x-md5-hash">>, HexHash}
        ],
        RespBody
    ),
    {reply, Resp, State};
%% POST /upload/verify - Upload data and verify expected checksum (in query string)
handle_request(
    #{method := post, path := <<"/upload/verify", QueryPart/binary>>, body := Body}, State
) ->
    Size = byte_size(Body),
    Hash = crypto:hash(md5, Body),
    HexHash = binary:encode_hex(Hash, lowercase),
    %% Parse expected hash from query string
    ExpectedHash =
        case QueryPart of
            <<"?expected=", Expected/binary>> -> Expected;
            _ -> <<>>
        end,
    case ExpectedHash =:= HexHash of
        true ->
            RespBody = iolist_to_binary([
                <<"OK: ">>, integer_to_binary(Size), <<" bytes, hash matches">>
            ]),
            Resp = nhttp_resp:ok(
                [{<<"content-type">>, <<"text/plain">>}],
                RespBody
            ),
            {reply, Resp, State};
        false ->
            RespBody = iolist_to_binary([
                <<"MISMATCH: expected ">>, ExpectedHash, <<", got ">>, HexHash
            ]),
            Resp = #{
                status => 400,
                headers => [{<<"content-type">>, <<"text/plain">>}],
                body => RespBody
            },
            {reply, Resp, State}
    end;
%% POST /upload/large - Accept large upload and return size only (for memory testing)
handle_request(
    #{method := post, path := <<"/upload/large", _/binary>>, body := Body}, State
) ->
    Size = byte_size(Body),
    RespBody = integer_to_binary(Size),
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"text/plain">>},
            {<<"x-received-size">>, integer_to_binary(Size)}
        ],
        RespBody
    ),
    {reply, Resp, State};
%% POST /upload/chunked - Accept chunked upload, return size and info
handle_request(
    #{
        method := post, path := <<"/upload/chunked", _/binary>>, headers := Headers, body := Body
    },
    State
) ->
    Size = byte_size(Body),
    TransferEncoding = proplists:get_value(<<"transfer-encoding">>, Headers, <<"none">>),
    ContentLength = proplists:get_value(<<"content-length">>, Headers, <<"none">>),
    RespBody = iolist_to_binary([
        <<"Size: ">>,
        integer_to_binary(Size),
        <<"\n">>,
        <<"Transfer-Encoding: ">>,
        TransferEncoding,
        <<"\n">>,
        <<"Content-Length: ">>,
        ContentLength
    ]),
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"text/plain">>},
            {<<"x-received-size">>, integer_to_binary(Size)}
        ],
        RespBody
    ),
    {reply, Resp, State};
%%%-----------------------------------------------------------------------------
%%% SLOW ENDPOINTS (for timeout testing)
%%%-----------------------------------------------------------------------------

%% GET /slow/N - Wait N milliseconds before responding
handle_request(#{method := get, path := <<"/slow/", Rest/binary>>}, State) ->
    %% Parse delay from path
    DelayMs =
        case binary:split(Rest, <<"?">>) of
            [DelayPart, _] -> binary_to_integer(DelayPart);
            [DelayPart] -> binary_to_integer(DelayPart)
        end,
    timer:sleep(DelayMs),
    Resp = nhttp_resp:ok(
        [{<<"content-type">>, <<"text/plain">>}],
        <<"Slow response after delay">>
    ),
    {reply, Resp, State};
%% GET /close-connection - Respond and signal connection close
handle_request(#{method := get, path := <<"/close-connection", _/binary>>}, State) ->
    Resp = nhttp_resp:ok(
        [{<<"content-type">>, <<"text/plain">>}, {<<"connection">>, <<"close">>}],
        <<"Connection will close">>
    ),
    {reply, Resp, State};
%%%-----------------------------------------------------------------------------
%%% DEFAULT ENDPOINTS
%%%-----------------------------------------------------------------------------

%% GET /trailers - Streaming response that ends with HTTP/2 trailers
handle_request(#{method := get, path := <<"/trailers">>}, State) ->
    Headers = [{<<"content-type">>, <<"application/grpc">>}],
    Producer = fun(SendChunk) ->
        ok = SendChunk(<<"grpc-response-body">>),
        {trailers, [{<<"grpc-status">>, <<"0">>}, {<<"grpc-message">>, <<>>}]}
    end,
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
%% GET /trailers-only - Headers-only response (no body, no trailers; Trailers-Only gRPC pattern)
handle_request(#{method := get, path := <<"/trailers-only">>}, State) ->
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"application/grpc">>},
            {<<"grpc-status">>, <<"0">>},
            {<<"grpc-message">>, <<>>}
        ],
        <<>>
    ),
    {reply, Resp, State};
%% GET /echo - Echo request info as JSON
handle_request(#{method := get, path := Path, headers := Headers}, State) ->
    %% Include Accept-Encoding header in response for testing
    AcceptEncoding = proplists:get_value(<<"accept-encoding">>, Headers, <<"none">>),
    Body = iolist_to_binary([
        <<"GET request to ">>,
        Path,
        <<"\nAccept-Encoding: ">>,
        AcceptEncoding
    ]),
    Resp = nhttp_resp:ok([{<<"content-type">>, <<"text/plain">>}], Body),
    {reply, Resp, State};
%% POST /echo - Echo request body
handle_request(#{method := post, path := _Path, body := Body}, State) ->
    Resp = nhttp_resp:ok([{<<"content-type">>, <<"application/octet-stream">>}], Body),
    {reply, Resp, State};
%% HEAD - Return headers only, no body (RFC compliant)
handle_request(#{method := head, path := _Path}, State) ->
    Resp = nhttp_resp:ok([{<<"content-type">>, <<"text/plain">>}], <<>>),
    {reply, Resp, State};
%% OPTIONS - Return allowed methods
handle_request(#{method := options, path := _Path}, State) ->
    Resp = nhttp_resp:ok(
        [
            {<<"content-type">>, <<"text/plain">>},
            {<<"allow">>, <<"GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH">>}
        ],
        <<>>
    ),
    {reply, Resp, State};
%% Other methods
handle_request(#{method := Method, path := Path}, State) ->
    Body = iolist_to_binary([nhttp_lib:encode_method(Method), <<" request to ">>, Path]),
    Resp = nhttp_resp:ok([{<<"content-type">>, <<"text/plain">>}], Body),
    {reply, Resp, State}.

%%%-----------------------------------------------------------------------------
%%% STREAMING REQUEST BODY HANDLER
%%%-----------------------------------------------------------------------------

-spec handle_request_body(
    nhttp_handler:body_event(), {nhttp_lib:request(), [binary()]}, State
) ->
    nhttp_handler:request_result(State)
when
    State :: #{}.
handle_request_body({data, Chunk}, {Req, Acc}, State) ->
    {accept_body, {Req, [Chunk | Acc]}, State};
handle_request_body({fin, Trailers}, {Req, Acc}, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    BufferedReq = Req#{body => Body, trailers => Trailers},
    handle_request(BufferedReq, State);
handle_request_body({abort, _Reason}, _BodyState, State) ->
    Resp = #{
        status => 400,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"body aborted">>
    },
    {reply, Resp, State}.

%%%-----------------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------

%% @doc Get the ETS table for tracking retry attempts.
%% The table is created by the test suite init_per_group.
%% Falls back to creating it if running outside of test suite.
-spec ensure_retry_table() -> ets:tid() | atom().
ensure_retry_table() ->
    case ets:whereis(nhttp_test_retry) of
        undefined ->
            %% Fallback for running outside test suite
            ets:new(nhttp_test_retry, [public, named_table, {write_concurrency, true}]);
        Tid ->
            Tid
    end.
