-module(h3_debug_test).
-export([run/0]).

run() ->
    application:ensure_all_started(ssl),
    application:ensure_all_started(crypto),
    CertFile = "test/conf/server.pem",
    KeyFile = "test/conf/server.key",
    ListenOpts = #{tls => #{certfile => CertFile, keyfile => KeyFile, alpn => [<<"h3">>]}},
    {ok, Listener} = nquic:listen(0, ListenOpts),
    unlink(Listener),
    {ok, Port} = nquic_listener:get_port(Listener),

    HandlerOpts = #{handler => nhttpc_h3_test_handler},
    Self = self(),
    spawn(fun() -> acc_loop(Listener, HandlerOpts, Self) end),

    Opts = #{
        host => <<"127.0.0.1">>,
        port => Port,
        connect_timeout => 5000,
        request_timeout => 30000,
        idle_timeout => 60000
    },
    {ok, ConnPid} = nhttpc_h3_conn:start_link(Opts),
    ok = nhttpc_h3_conn:wait_ready(ConnPid, 10000),
    io:format("Client Ready!~n"),

    receive
        {server_h3_conn, _} -> ok
    after 5000 -> ok
    end,

    Req = #{method => get, path => <<"/hello">>, headers => [], body => <<>>},
    {ok, Ref} = nhttpc_h3_conn:send_async(ConnPid, Req, self()),
    io:format("Request sent, ref=~p~n", [Ref]),

    receive
        {nhttpc_response, ConnPid, Ref, {ok, Resp}} ->
            io:format("Response: status=~p body=~p~n", [
                maps:get(status, Resp), maps:get(body, Resp, <<>>)
            ]);
        {nhttpc_error, ConnPid, Ref, Reason} ->
            io:format("Error: ~p~n", [Reason])
    after 10000 ->
        io:format("Timeout waiting for response~n"),
        io:format("Client alive: ~p~n", [is_process_alive(ConnPid)]),
        case process_info(ConnPid, messages) of
            {messages, Msgs} -> io:format("Client msgs: ~p~n", [Msgs]);
            undefined -> io:format("Client dead~n")
        end
    end,

    try
        nhttpc_h3_conn:stop(ConnPid)
    catch
        _:_ -> ok
    end,
    try
        exit(Listener, kill)
    catch
        _:_ -> ok
    end,
    ok.

acc_loop(Listener, HandlerOpts, Notify) ->
    process_flag(trap_exit, true),
    case nquic:accept(Listener, #{timeout => 5000}) of
        {ok, QConn} ->
            {ok, Pid} = nhttp_h3_conn:start_link(h3_test, QConn, HandlerOpts, self()),
            io:format("Server h3 conn started: ~p~n", [Pid]),
            Notify ! {server_h3_conn, Pid},
            acc_loop(Listener, HandlerOpts, Notify);
        {error, timeout} ->
            acc_loop(Listener, HandlerOpts, Notify);
        {error, _R} ->
            ok
    end.
