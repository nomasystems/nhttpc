-module(nhttpc_handle).

-moduledoc """
Opaque handle constructor for the nhttpc 1.0 unified async protocol.

Every async surface (`nhttpc:send/2`, the future `nhttpc:stream/2` and
`nhttpc:ws_open/2`) returns an `nhttpc:handle/0`. The handle pairs a
fresh `reference/0` with the worker `pid/0` that owns the request and a
`kind/0` tag for protocol-aware consumers.

The struct is intentionally minimal so the same accessor module is
shared between the request worker (high-level `send/2`) and the
connection processes that emit unified events directly.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    is_handle/1,
    kind/1,
    new/3,
    pid/1,
    ref/1
]).

-export_type([handle/0, kind/0]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-type kind() :: request | stream | ws.

-opaque handle() :: #{pid := pid(), ref := reference(), kind := kind()}.

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-spec is_handle(term()) -> boolean().
is_handle(#{pid := P, ref := R, kind := K}) when
    is_pid(P), is_reference(R), (K =:= request orelse K =:= stream orelse K =:= ws)
->
    true;
is_handle(_) ->
    false.

-spec kind(handle()) -> kind().
kind(#{kind := Kind}) -> Kind.

-spec new(pid(), reference(), kind()) -> handle().
new(Pid, Ref, Kind) when is_pid(Pid), is_reference(Ref) ->
    #{pid => Pid, ref => Ref, kind => Kind}.

-spec pid(handle()) -> pid().
pid(#{pid := Pid}) -> Pid.

-spec ref(handle()) -> reference().
ref(#{ref := Ref}) -> Ref.
