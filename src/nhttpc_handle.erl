-module(nhttpc_handle).

-moduledoc """
Opaque handle constructor for the nhttpc 1.0 unified async protocol.

Every async surface (`nhttpc:send/2`, `nhttpc:stream/2` and
`nhttpc:ws_open/2`) returns an `nhttpc:handle/0`. The handle pairs a
fresh `reference/0` with the `pid/0` of the process that sends the
events and a `kind/0` tag for protocol-aware consumers.

The process of a handle from `new/3` serves that handle alone. The
process of a handle from `new_shared/3` is a connection that carries
other streams too, so `nhttpc:cancel/1` never stops it.
""".

%%%-----------------------------------------------------------------------------
%% EXPORTS
%%%-----------------------------------------------------------------------------
-export([
    is_handle/1,
    is_shared/1,
    kind/1,
    new/3,
    new_shared/3,
    pid/1,
    ref/1
]).

-export_type([handle/0, kind/0]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-type kind() :: request | stream | ws.

-opaque handle() :: #{pid := pid(), ref := reference(), kind := kind(), shared => true}.

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

-doc "Whether the process of the handle serves other handles too.".
-spec is_shared(handle()) -> boolean().
is_shared(Handle) ->
    is_map_key(shared, Handle).

-spec kind(handle()) -> kind().
kind(#{kind := Kind}) -> Kind.

-spec new(pid(), reference(), kind()) -> handle().
new(Pid, Ref, Kind) when is_pid(Pid), is_reference(Ref) ->
    #{pid => Pid, ref => Ref, kind => Kind}.

-doc "A handle whose process is a connection that carries other streams.".
-spec new_shared(pid(), reference(), kind()) -> handle().
new_shared(Pid, Ref, Kind) when is_pid(Pid), is_reference(Ref) ->
    #{pid => Pid, ref => Ref, kind => Kind, shared => true}.

-spec pid(handle()) -> pid().
pid(#{pid := Pid}) -> Pid.

-spec ref(handle()) -> reference().
ref(#{ref := Ref}) -> Ref.
