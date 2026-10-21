-module(nhttpc_test_server).

-moduledoc """
Test server lifecycle helpers.

`nhttp:stop/1` sends `exit(Pid, shutdown)` and waits 10 seconds for the
listener to answer. A supervisor acts on that signal only from the process
that started it, and Common Test runs each configuration function in a fresh
process, so the signal is dropped and the wait always expires. `stop/1` here
uses `proc_lib:stop/3`, which reaches a supervisor from any process.
""".

%%%-----------------------------------------------------------------------------
%%% EXPORTS - API
%%%-----------------------------------------------------------------------------
-export([
    stop/1
]).

%%%-----------------------------------------------------------------------------
%%% MACROS
%%%-----------------------------------------------------------------------------
-define(FORCE_KILL_TIMEOUT, 1000).
-define(STOP_TIMEOUT, 5000).

%%%-----------------------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------------------

-doc """
Stop an `nhttp` listener and wait for it to terminate.

Accepts `undefined` so that a caller which reads a pid out of the test
configuration does not need a guard of its own.
""".
-spec stop(pid() | undefined) -> ok.
stop(undefined) ->
    ok;
stop(Pid) when is_pid(Pid) ->
    case is_process_alive(Pid) of
        false ->
            ok;
        true ->
            try proc_lib:stop(Pid, shutdown, ?STOP_TIMEOUT) of
                _ -> ok
            catch
                exit:noproc -> ok;
                exit:{noproc, _} -> ok;
                exit:timeout -> force_kill(Pid);
                exit:{timeout, _} -> force_kill(Pid)
            end
    end.

%%%-----------------------------------------------------------------------------
%%% INTERNAL
%%%-----------------------------------------------------------------------------

-spec force_kill(pid()) -> ok.
force_kill(Pid) ->
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after ?FORCE_KILL_TIMEOUT ->
        demonitor(Ref, [flush]),
        ok
    end.
