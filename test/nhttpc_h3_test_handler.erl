%%%-----------------------------------------------------------------------------
%%% @doc Minimal HTTP/3 test handler for nhttpc_h3_conn_SUITE.
%%%
%%% Endpoints:
%%%   - GET /hello - Simple greeting response
%%%   - POST /echo - Echo request body back
%%%   - GET /empty - Empty body response
%%%   - GET /large - 1300-byte position-dependent body (partial-frame carry-over guard)
%%%   - GET /many-headers - Three response headers (header-count limit guard)
%%%   - GET /slow - 2 second delayed response
%%%   - GET /stream-multi - Multi-frame streamed body
%%%   - GET /stream-trailers - Streamed body followed by trailers
%%%   - GET /abort - Abort the request (server resets the stream)
%%%   - GET /crash - Handler raises (server resets the stream)
%%%   - * - 404 Not Found
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_h3_test_handler).

-behaviour(nhttp_handler).

%%%-----------------------------------------------------------------------------
%%% EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    init/1,
    handle_request/2,
    handle_request_body/3
]).

%%%-----------------------------------------------------------------------------
%%% HANDLER CALLBACKS
%%%-----------------------------------------------------------------------------

-spec init(term()) -> {ok, #{}}.
init(_Args) ->
    {ok, #{}}.

-spec handle_request(nhttp_lib:request(), State) ->
    {reply, nhttp_lib:response(), State}
    | {accept_body, {nhttp_lib:request(), [binary()]}, State}
when
    State :: #{}.
handle_request(#{body := streaming} = Req, State) ->
    {accept_body, {Req, []}, State};
handle_request(#{method := get, path := <<"/hello", _/binary>>}, State) ->
    Resp = #{
        status => 200,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"Hello!">>
    },
    {reply, Resp, State};
handle_request(#{method := post, path := <<"/echo", _/binary>>, body := Body}, State) ->
    Resp = #{
        status => 200,
        headers => [{<<"content-type">>, <<"application/octet-stream">>}],
        body => Body
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/empty", _/binary>>}, State) ->
    Resp = #{
        status => 200,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<>>
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/large", _/binary>>}, State) ->
    Body = <<<<(I rem 256)>> || I <- lists:seq(1, 1300)>>,
    Resp = #{
        status => 200,
        headers => [{<<"content-type">>, <<"application/octet-stream">>}],
        body => Body
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/many-headers", _/binary>>}, State) ->
    Resp = #{
        status => 200,
        headers => [
            {<<"content-type">>, <<"text/plain">>},
            {<<"x-one">>, <<"1">>},
            {<<"x-two">>, <<"2">>}
        ],
        body => <<"many">>
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/slow", _/binary>>}, State) ->
    timer:sleep(2000),
    Resp = #{
        status => 200,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"slow">>
    },
    {reply, Resp, State};
handle_request(#{method := get, path := <<"/stream-multi", _/binary>>}, State) ->
    Producer = fun(SendChunk) ->
        ok = SendChunk(<<"part-one ">>),
        ok = SendChunk(<<"part-two ">>),
        ok = SendChunk(<<"part-three">>)
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{method := get, path := <<"/stream-trailers", _/binary>>}, State) ->
    Producer = fun(SendChunk) ->
        ok = SendChunk(<<"with-trailers">>),
        {trailers, [{<<"x-checksum">>, <<"abc123">>}]}
    end,
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    {stream, nhttp_stream:producer(200, Headers, Producer), State};
handle_request(#{method := get, path := <<"/abort", _/binary>>}, State) ->
    {abort, h3_test_abort, State};
handle_request(#{method := get, path := <<"/crash", _/binary>>}, _State) ->
    erlang:error(h3_test_crash);
handle_request(_Req, State) ->
    Resp = #{
        status => 404,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"Not Found">>
    },
    {reply, Resp, State}.

-spec handle_request_body(
    nhttp_handler:body_event(), {nhttp_lib:request(), [binary()]}, State
) ->
    nhttp_handler:request_result(State)
when
    State :: #{}.
handle_request_body({data, Chunk}, {Req, Acc}, State) ->
    {accept_body, {Req, [Chunk | Acc]}, State};
handle_request_body({fin, _Trailers}, {Req, Acc}, State) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    handle_request(Req#{body => Body}, State);
handle_request_body({abort, _Reason}, _BodyState, State) ->
    Resp = #{
        status => 400,
        headers => [{<<"content-type">>, <<"text/plain">>}],
        body => <<"aborted">>
    },
    {reply, Resp, State}.
