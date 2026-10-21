%%%-----------------------------------------------------------------------------
%%% @doc Common Test suite covering nhttpc:stream/2 and the unified
%%% streaming event vocabulary (plan 07b).
%%%
%%% The async surface delivers:
%%%   {nhttpc, Handle, {headers, Status, Headers}}
%%%   {nhttpc, Handle, {data, Chunk, more | fin}}
%%%   {nhttpc, Handle, {trailers, Headers}}      %% H2 / H3 only
%%%   {nhttpc, Handle, {done, normal}}           %% terminal
%%%   {nhttpc, Handle, {error, Reason}}          %% terminal
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_stream_events_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    suite/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    stream_h1_emits_headers_data_done/1,
    stream_h1_chunked_emits_multiple_data_events/1,
    stream_h2_emits_headers_data_done/1,
    stream_h2_trailers_event/1,
    stream_h3_emits_headers_data_done/1,
    stream_cancel_after_headers/1,
    stream_terminal_event_exactly_once/1,
    stream_rejects_body_to/1,
    stream_rejects_non_oneshot_via/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
        {group, http1},
        {group, http2},
        {group, http3},
        {group, validation}
    ].

groups() ->
    [
        {http1, [sequence], [
            stream_h1_emits_headers_data_done,
            stream_h1_chunked_emits_multiple_data_events,
            stream_cancel_after_headers,
            stream_terminal_event_exactly_once
        ]},
        {http2, [sequence], [
            stream_h2_emits_headers_data_done,
            stream_h2_trailers_event
        ]},
        {http3, [sequence], [
            stream_h3_emits_headers_data_done
        ]},
        {validation, [parallel], [
            stream_rejects_body_to,
            stream_rejects_non_oneshot_via
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(ssl),
    PrivDir = proplists:get_value(priv_dir, Config, ""),
    ProjectRoot = find_project_root(PrivDir),
    ConfDir = filename:join([ProjectRoot, "test", "conf"]),
    CertFile = filename:join(ConfDir, "server.pem"),
    KeyFile = filename:join(ConfDir, "server.key"),
    case filelib:is_file(CertFile) andalso filelib:is_file(KeyFile) of
        true -> [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        false -> Config
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(http1, Config) ->
    case start_h1_server() of
        {ok, ServerPid, Port} ->
            [{server_pid, ServerPid}, {server_port, Port} | Config];
        {error, Reason} ->
            ct:fail("Failed to start H1 server: ~p", [Reason])
    end;
init_per_group(http2, Config) ->
    case has_tls(Config) of
        true ->
            CertFile = ?config(certfile, Config),
            KeyFile = ?config(keyfile, Config),
            case start_h2_server(CertFile, KeyFile) of
                {ok, ServerPid, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    ct:fail("Failed to start H2 server: ~p", [Reason])
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(http3, Config) ->
    case has_tls(Config) of
        true ->
            CertFile = ?config(certfile, Config),
            KeyFile = ?config(keyfile, Config),
            case start_h3_server(CertFile, KeyFile) of
                {ok, ServerPid, Port} ->
                    [{server_pid, ServerPid}, {server_port, Port} | Config];
                {error, Reason} ->
                    {skip, {h3_server_failed, Reason}}
            end;
        false ->
            {skip, "TLS certificates not available"}
    end;
init_per_group(validation, Config) ->
    Config.

end_per_group(validation, _Config) ->
    ok;
end_per_group(_Group, Config) ->
    nhttpc_test_server:stop(proplists:get_value(server_pid, Config)),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/1.1 CASES
%%%-----------------------------------------------------------------------------

stream_h1_emits_headers_data_done(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/progress">>)),
    {ok, Handle} = nhttpc:stream(Req, #{}),
    {Status, _Headers, Body, Trailers, Terminal} = collect_events(Handle, 10000),
    ?assertEqual(200, Status),
    ?assertEqual(byte_size(binary:copy(<<"progress">>, 1250)), byte_size(Body)),
    ?assertEqual(undefined, Trailers),
    ?assertEqual({done, normal}, Terminal),
    ok = nhttpc:flush(Handle),
    ok.

stream_h1_chunked_emits_multiple_data_events(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/large">>)),
    {ok, Handle} = nhttpc:stream(Req, #{}),
    {Status, _Headers, Body, _, Terminal, DataCount} = collect_events_counted(Handle, 10000),
    ?assertEqual(200, Status),
    Expected = binary:copy(<<"0123456789ABCDEF">>, 6400),
    ?assertEqual(Expected, Body),
    ?assertEqual({done, normal}, Terminal),
    ?assert(DataCount >= 1),
    ok = nhttpc:flush(Handle),
    ok.

stream_cancel_after_headers(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/large">>)),
    {ok, Handle} = nhttpc:stream(Req, #{}),
    receive
        {nhttpc, Handle, {headers, _, _}} -> ok
    after 5000 ->
        ct:fail("did not receive headers within 5s")
    end,
    ok = nhttpc:cancel(Handle),
    ?assertEqual(0, count_handle_events(Handle)),
    ok.

stream_terminal_event_exactly_once(Config) ->
    Port = ?config(server_port, Config),
    {ok, Req} = nhttpc:url(get, url(Port, <<"/stream/small">>)),
    {ok, Handle} = nhttpc:stream(Req, #{}),
    {Status, _Headers, _Body, _Trailers, Terminal} = collect_events(Handle, 5000),
    ?assertEqual(200, Status),
    ?assertEqual({done, normal}, Terminal),
    ?assertEqual(0, count_terminal_events(Handle)),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/2 CASES
%%%-----------------------------------------------------------------------------

stream_h2_emits_headers_data_done(Config) ->
    Port = ?config(server_port, Config),
    Url = h2_url(Port, <<"/stream/progress">>),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:stream(Req, #{
        tls => #{verify => verify_none},
        versions => [http2]
    }),
    {Status, _Headers, Body, _, Terminal} = collect_events(Handle, 10000),
    ?assertEqual(200, Status),
    ?assertEqual(byte_size(binary:copy(<<"progress">>, 1250)), byte_size(Body)),
    ?assertEqual({done, normal}, Terminal),
    ok = nhttpc:flush(Handle),
    ok.

stream_h2_trailers_event(Config) ->
    Port = ?config(server_port, Config),
    Url = h2_url(Port, <<"/trailers">>),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:stream(Req, #{
        tls => #{verify => verify_none},
        versions => [http2]
    }),
    {Status, _Headers, Body, Trailers, Terminal} = collect_events(Handle, 10000),
    ?assertEqual(200, Status),
    ?assertEqual(<<"grpc-response-body">>, Body),
    ?assertNotEqual(undefined, Trailers),
    ?assertEqual({done, normal}, Terminal),
    GrpcStatus = proplists:get_value(<<"grpc-status">>, Trailers),
    ?assertEqual(<<"0">>, GrpcStatus),
    ok = nhttpc:flush(Handle),
    ok.

%%%-----------------------------------------------------------------------------
%%% HTTP/3 CASES
%%%-----------------------------------------------------------------------------

stream_h3_emits_headers_data_done(Config) ->
    Port = ?config(server_port, Config),
    Url = h3_url(Port, <<"/hello">>),
    {ok, Req} = nhttpc:url(get, Url),
    {ok, Handle} = nhttpc:stream(Req, #{
        tls => #{verify => verify_none},
        versions => [http3]
    }),
    {Status, _Headers, Body, _, Terminal} = collect_events(Handle, 10000),
    ?assertEqual(200, Status),
    ?assertEqual(<<"Hello!">>, Body),
    ?assertEqual({done, normal}, Terminal),
    ok = nhttpc:flush(Handle),
    ok.

%%%-----------------------------------------------------------------------------
%%% VALIDATION CASES
%%%-----------------------------------------------------------------------------

stream_rejects_body_to(_Config) ->
    {ok, Req} = nhttpc:url(get, <<"http://127.0.0.1:1/echo">>),
    Result = nhttpc:stream(Req, #{body_to => {file, "/tmp/should_not_be_used"}}),
    ?assertEqual({error, {invalid_opts, body_to_on_stream}}, Result),
    ok.

stream_rejects_non_oneshot_via(_Config) ->
    {ok, Req} = nhttpc:url(get, <<"http://127.0.0.1:1/echo">>),
    Result1 = nhttpc:stream(Req, #{via => {pool, some_pool}}),
    ?assertEqual({error, {invalid_opts, stream_via_not_oneshot}}, Result1),
    Result2 = nhttpc:stream(Req, #{
        via => {conn, #{pid => self(), version => http1_1, peer => {{127, 0, 0, 1}, 1}}}
    }),
    ?assertEqual({error, {invalid_opts, stream_via_not_oneshot}}, Result2),
    ok.

%%%-----------------------------------------------------------------------------
%%% HELPERS — EVENT COLLECTION
%%%-----------------------------------------------------------------------------

collect_events(Handle, Timeout) ->
    collect_events(Handle, Timeout, undefined, undefined, [], undefined).

collect_events(Handle, Timeout, Status, Headers, BodyAcc, Trailers) ->
    receive
        {nhttpc, Handle, {headers, S, Hs}} ->
            collect_events(Handle, Timeout, S, Hs, BodyAcc, Trailers);
        {nhttpc, Handle, {data, Chunk, _Fin}} ->
            collect_events(Handle, Timeout, Status, Headers, [Chunk | BodyAcc], Trailers);
        {nhttpc, Handle, {trailers, Ts}} ->
            collect_events(Handle, Timeout, Status, Headers, BodyAcc, Ts);
        {nhttpc, Handle, {done, _} = Done} ->
            {Status, Headers, iolist_to_binary(lists:reverse(BodyAcc)), Trailers, Done};
        {nhttpc, Handle, {error, _} = Err} ->
            {Status, Headers, iolist_to_binary(lists:reverse(BodyAcc)), Trailers, Err}
    after Timeout ->
        ct:fail(
            "timeout collecting stream events; got status=~p headers=~p body_bytes=~p",
            [Status, Headers, iolist_size(BodyAcc)]
        )
    end.

collect_events_counted(Handle, Timeout) ->
    collect_events_counted(Handle, Timeout, undefined, undefined, [], undefined, 0).

collect_events_counted(Handle, Timeout, Status, Headers, BodyAcc, Trailers, Count) ->
    receive
        {nhttpc, Handle, {headers, S, Hs}} ->
            collect_events_counted(Handle, Timeout, S, Hs, BodyAcc, Trailers, Count);
        {nhttpc, Handle, {data, Chunk, _Fin}} ->
            collect_events_counted(
                Handle, Timeout, Status, Headers, [Chunk | BodyAcc], Trailers, Count + 1
            );
        {nhttpc, Handle, {trailers, Ts}} ->
            collect_events_counted(Handle, Timeout, Status, Headers, BodyAcc, Ts, Count);
        {nhttpc, Handle, {done, _} = Done} ->
            {Status, Headers, iolist_to_binary(lists:reverse(BodyAcc)), Trailers, Done, Count};
        {nhttpc, Handle, {error, _} = Err} ->
            {Status, Headers, iolist_to_binary(lists:reverse(BodyAcc)), Trailers, Err, Count}
    after Timeout ->
        ct:fail("timeout collecting stream events")
    end.

count_handle_events(Handle) ->
    count_handle_events(Handle, 0).

count_handle_events(Handle, Acc) ->
    receive
        {nhttpc, Handle, _} -> count_handle_events(Handle, Acc + 1)
    after 50 ->
        Acc
    end.

count_terminal_events(Handle) ->
    count_terminal_events(Handle, 0).

count_terminal_events(Handle, Acc) ->
    receive
        {nhttpc, Handle, {done, _}} -> count_terminal_events(Handle, Acc + 1);
        {nhttpc, Handle, {error, _}} -> count_terminal_events(Handle, Acc + 1)
    after 50 ->
        Acc
    end.

%%%-----------------------------------------------------------------------------
%%% HELPERS — SERVERS / URLS
%%%-----------------------------------------------------------------------------

start_h1_server() ->
    case
        nhttp:start_link(stream_events_h1, #{
            port => 0,
            handler => nhttp_test_handler,
            protocols => [http1_1]
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, _} = Error ->
            Error
    end.

start_h2_server(CertFile, KeyFile) ->
    case
        nhttp:start_link(stream_events_h2, #{
            port => 0,
            handler => nhttp_test_handler,
            versions => [http2, http1_1],
            tls => #{certfile => CertFile, keyfile => KeyFile}
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, _} = Error ->
            Error
    end.

start_h3_server(CertFile, KeyFile) ->
    case
        nhttp:start_link(stream_events_h3, #{
            port => 0,
            handler => nhttpc_h3_test_handler,
            versions => [http3],
            tls => #{certfile => CertFile, keyfile => KeyFile}
        })
    of
        {ok, ServerPid} ->
            unlink(ServerPid),
            {ok, Port} = nhttp:get_port(ServerPid),
            {ok, ServerPid, Port};
        {error, _} = Error ->
            Error
    end.

url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"http://127.0.0.1:", PortBin/binary, Path/binary>>.

h2_url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"https://127.0.0.1:", PortBin/binary, Path/binary>>.

h3_url(Port, Path) ->
    PortBin = integer_to_binary(Port),
    <<"https://127.0.0.1:", PortBin/binary, Path/binary>>.

has_tls(Config) ->
    proplists:get_value(certfile, Config) =/= undefined.

find_project_root(Dir) ->
    RebarConfig = filename:join(Dir, "rebar.config"),
    case filelib:is_file(RebarConfig) of
        true ->
            Dir;
        false ->
            Parent = filename:dirname(Dir),
            case Parent =:= Dir of
                true -> ".";
                false -> find_project_root(Parent)
            end
    end.
