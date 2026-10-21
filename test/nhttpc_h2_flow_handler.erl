-module(nhttpc_h2_flow_handler).

-moduledoc """
The `nhttp` handler of the flow-control conformance suite. It counts the
connections it was started on, the requests it saw and the request body
octets it read, in an `atomics` array the suite hands it through
`handler_args`, and answers every request with a 200. A `GET /body/N`
answers with a body of `N` octets; every other path answers with none.
""".

-behaviour(nhttp_handler).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - HANDLER
%%%-----------------------------------------------------------------------------
-export([
    handle_request/2,
    handle_request_body/3,
    init/1,
    terminate/2
]).

%%%-----------------------------------------------------------------------------
%%% EXPORTS - COUNTERS
%%%-----------------------------------------------------------------------------
-export([
    new_counters/0,
    read/1
]).

%%%-----------------------------------------------------------------------------
%%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([counters/0]).

-type counters() :: atomics:atomics_ref().

-type body_state() :: {nhttp_lib:request(), [binary()]}.

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(C_CONNECTIONS, 1).
-define(C_REQUESTS, 2).
-define(C_BODY_OCTETS, 3).
-define(C_CLOSED, 4).
-define(C_SIZE, 4).
-define(BODY_PREFIX, "/body/").

%%%-----------------------------------------------------------------------------
%%% COUNTERS
%%%-----------------------------------------------------------------------------

-doc "A zeroed counter array for one server.".
-spec new_counters() -> counters().
new_counters() ->
    atomics:new(?C_SIZE, [{signed, false}]).

-doc "The counters by name.".
-spec read(counters()) ->
    #{
        connections := non_neg_integer(),
        requests := non_neg_integer(),
        body_octets := non_neg_integer(),
        closed := non_neg_integer()
    }.
read(Counters) ->
    #{
        connections => atomics:get(Counters, ?C_CONNECTIONS),
        requests => atomics:get(Counters, ?C_REQUESTS),
        body_octets => atomics:get(Counters, ?C_BODY_OCTETS),
        closed => atomics:get(Counters, ?C_CLOSED)
    }.

%%%-----------------------------------------------------------------------------
%%% HANDLER
%%%-----------------------------------------------------------------------------

-spec init(counters()) -> {ok, counters()}.
init(Counters) ->
    atomics:add(Counters, ?C_CONNECTIONS, 1),
    {ok, Counters}.

-spec handle_request(nhttp_lib:request(), counters()) ->
    nhttp_handler:request_result(counters()).
handle_request(#{body := streaming} = Req, Counters) ->
    {accept_body, {Req, []}, Counters};
handle_request(#{path := Path} = Req, Counters) ->
    atomics:add(Counters, ?C_REQUESTS, 1),
    atomics:add(Counters, ?C_BODY_OCTETS, byte_size(maps:get(body, Req, <<>>))),
    Body = response_body(Path),
    Resp = #{
        status => 200,
        headers => [
            {<<"content-type">>, <<"application/octet-stream">>},
            {<<"content-length">>, integer_to_binary(byte_size(Body))}
        ],
        body => Body
    },
    {reply, Resp, Counters}.

-spec handle_request_body(nhttp_handler:body_event(), body_state(), counters()) ->
    nhttp_handler:request_result(counters()).
handle_request_body({data, Chunk}, {Req, Acc}, Counters) ->
    {accept_body, {Req, [Chunk | Acc]}, Counters};
handle_request_body({fin, Trailers}, {Req, Acc}, Counters) ->
    Body = iolist_to_binary(lists:reverse(Acc)),
    handle_request(Req#{body => Body, trailers => Trailers}, Counters);
handle_request_body({abort, _Reason}, _BodyState, Counters) ->
    Resp = #{status => 400, headers => [], body => <<>>},
    {reply, Resp, Counters}.

-spec terminate(term(), counters()) -> ok.
terminate(_Reason, Counters) ->
    atomics:add(Counters, ?C_CLOSED, 1),
    ok.

-spec response_body(binary()) -> binary().
response_body(<<?BODY_PREFIX, Size/binary>>) ->
    binary:copy(<<$r>>, binary_to_integer(Size));
response_body(_Path) ->
    <<>>.
