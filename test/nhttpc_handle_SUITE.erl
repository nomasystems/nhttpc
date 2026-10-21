%%%-----------------------------------------------------------------------------
%%% @doc Tests for the nhttpc:handle/0 opaque constructor.
%%%
%%% Covers `nhttpc_handle:new/3` and the accessor functions
%%% (`ref/1`, `pid/1`, `kind/1`) as well as the predicate `is_handle/1`
%%% with each shape it must accept and reject.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_handle_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% EXPORTS
%%%-----------------------------------------------------------------------------

-export([
    all/0,
    suite/0,
    groups/0
]).

-export([
    new_returns_map_with_fields/1,
    accessors_round_trip_each_kind/1,
    is_handle_accepts_each_kind/1,
    is_handle_rejects_non_map/1,
    is_handle_rejects_missing_fields/1,
    is_handle_rejects_invalid_kind/1,
    is_handle_rejects_wrong_field_types/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [{group, handle}].

groups() ->
    [
        {handle, [parallel], [
            new_returns_map_with_fields,
            accessors_round_trip_each_kind,
            is_handle_accepts_each_kind,
            is_handle_rejects_non_map,
            is_handle_rejects_missing_fields,
            is_handle_rejects_invalid_kind,
            is_handle_rejects_wrong_field_types
        ]}
    ].

%%%-----------------------------------------------------------------------------
%%% TESTS
%%%-----------------------------------------------------------------------------

new_returns_map_with_fields(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    Handle = nhttpc_handle:new(Pid, Ref, request),
    ?assertEqual(Pid, maps:get(pid, Handle)),
    ?assertEqual(Ref, maps:get(ref, Handle)),
    ?assertEqual(request, maps:get(kind, Handle)),
    ok.

accessors_round_trip_each_kind(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    lists:foreach(
        fun(Kind) ->
            H = nhttpc_handle:new(Pid, Ref, Kind),
            ?assertEqual(Pid, nhttpc_handle:pid(H)),
            ?assertEqual(Ref, nhttpc_handle:ref(H)),
            ?assertEqual(Kind, nhttpc_handle:kind(H))
        end,
        [request, stream, ws]
    ),
    ok.

is_handle_accepts_each_kind(_Config) ->
    Pid = self(),
    Ref = make_ref(),
    ?assert(nhttpc_handle:is_handle(nhttpc_handle:new(Pid, Ref, request))),
    ?assert(nhttpc_handle:is_handle(nhttpc_handle:new(Pid, Ref, stream))),
    ?assert(nhttpc_handle:is_handle(nhttpc_handle:new(Pid, Ref, ws))),
    ok.

is_handle_rejects_non_map(_Config) ->
    %% Non-map terms hit the catch-all `is_handle(_) -> false.` clause.
    ?assertNot(nhttpc_handle:is_handle(undefined)),
    ?assertNot(nhttpc_handle:is_handle(<<"binary">>)),
    ?assertNot(nhttpc_handle:is_handle([])),
    ?assertNot(nhttpc_handle:is_handle({pid, ref, request})),
    ?assertNot(nhttpc_handle:is_handle(42)),
    ok.

is_handle_rejects_missing_fields(_Config) ->
    ?assertNot(nhttpc_handle:is_handle(#{})),
    ?assertNot(nhttpc_handle:is_handle(#{pid => self()})),
    ?assertNot(nhttpc_handle:is_handle(#{pid => self(), ref => make_ref()})),
    ?assertNot(nhttpc_handle:is_handle(#{ref => make_ref(), kind => request})),
    ok.

is_handle_rejects_invalid_kind(_Config) ->
    %% A map with the three fields but a kind outside {request, stream, ws}
    %% fails the guard and falls through to the `false` clause.
    Pid = self(),
    Ref = make_ref(),
    ?assertNot(nhttpc_handle:is_handle(#{pid => Pid, ref => Ref, kind => bogus})),
    ?assertNot(nhttpc_handle:is_handle(#{pid => Pid, ref => Ref, kind => undefined})),
    ok.

is_handle_rejects_wrong_field_types(_Config) ->
    Ref = make_ref(),
    ?assertNot(nhttpc_handle:is_handle(#{pid => not_a_pid, ref => Ref, kind => request})),
    ?assertNot(nhttpc_handle:is_handle(#{pid => self(), ref => not_a_ref, kind => request})),
    ok.
