%%%-----------------------------------------------------------------------------
%%% @doc WebSocket test handler for nhttpc client integration tests.
%%%
%%% Endpoints (all complete the RFC 6455 handshake via nhttp's WS layer):
%%%   - GET /ws/echo          - Echo text and binary frames back
%%%   - GET /ws/sub           - Negotiate a subprotocol (prefers "v2")
%%%   - GET /ws/server-close  - Close with 1011 on the first text frame
%%%   - GET /ws/idle          - Upgrade then stay silent (client idle test)
%%%
%%% PING/PONG frames are handled automatically by the nhttp WS runtime.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_ws_test_handler).

-behaviour(nhttp_handler).

-export([init/1, handle_request/2, handle_ws_frame/3]).

init(_Args) ->
    {ok, #{}}.

handle_request(#{path := <<"/ws/echo">>}, State) ->
    {upgrade, websocket, State};
handle_request(#{path := <<"/ws/sub">>, headers := Headers}, State) ->
    Offered = offered_subprotocols(Headers),
    {upgrade, websocket, #{subprotocol => select_subprotocol(Offered)}, State};
handle_request(#{path := <<"/ws/server-close">>}, State) ->
    {upgrade, websocket, State#{action => server_close}};
handle_request(#{path := <<"/ws/idle">>}, State) ->
    {upgrade, websocket, State};
handle_request(_Req, State) ->
    {reply, nhttp_resp:not_found(), State}.

handle_ws_frame({text, _Data}, _Session, #{action := server_close} = State) ->
    {close, 1011, <<"server initiated">>, State};
handle_ws_frame({text, Data}, _Session, State) ->
    {reply, {text, Data}, State};
handle_ws_frame({binary, Data}, _Session, State) ->
    {reply, {binary, Data}, State}.

offered_subprotocols(Headers) ->
    case lists:keyfind(<<"sec-websocket-protocol">>, 1, Headers) of
        {_, Value} ->
            [string:trim(P) || P <- binary:split(Value, <<",">>, [global, trim_all])];
        false ->
            []
    end.

select_subprotocol(Offered) ->
    case lists:member(<<"v2">>, Offered) of
        true ->
            <<"v2">>;
        false ->
            case Offered of
                [First | _] -> First;
                [] -> undefined
            end
    end.
