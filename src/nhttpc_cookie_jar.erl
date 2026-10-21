-module(nhttpc_cookie_jar).

-moduledoc """
Cookie jar with ETS-based storage (RFC 6265).

An ETS-based cookie jar designed for high-performance concurrent access.
All read/write operations go directly to ETS — no gen_server bottleneck.
A minimal owner process handles table ownership, periodic cleanup, and
optional file persistence.

## Opacity (1.0)

`jar()` is an opaque `reference()`. The owner pid and ETS table are
held under `persistent_term` keyed on the reference. Internal state is
not destructurable from outside this module; users handle only the
reference returned by `start_link/0,1`.

## Quick Start

```erlang
%% Start a cookie jar
{ok, Jar} = nhttpc_cookie_jar:start_link().

%% Use with requests
{ok, Resp} = nhttpc:get(Url, #{cookie_jar => Jar}).

%% Manual cookie operations
ok = nhttpc_cookie_jar:set_from_headers(Jar, Uri, ResponseHeaders).
{ok, Cookies} = nhttpc_cookie_jar:get(Jar, Uri).
```
""".

-include_lib("stdlib/include/ms_transform.hrl").

%%%-----------------------------------------------------------------------------
%% EXPORTS - API
%%%-----------------------------------------------------------------------------
-export([
    all/1,
    clear/1,
    clear/2,
    get/2,
    get_header/2,
    persist/1,
    set/3,
    set_from_headers/3,
    start_link/0,
    start_link/1,
    stop/1
]).

%%%-----------------------------------------------------------------------------
%% EXPORTS - INTERNAL
%%%-----------------------------------------------------------------------------
-export([
    resolve/1
]).

%%%-----------------------------------------------------------------------------
%% TYPES
%%%-----------------------------------------------------------------------------
-export_type([jar/0, uri/0]).

-opaque jar() :: reference().

-type uri() :: #{
    scheme := http | https | binary(),
    host := binary(),
    path := binary(),
    port => inet:port_number()
}.

-type state() :: #{pid := pid(), tab := ets:tid()}.

%%%-----------------------------------------------------------------------------
%% MACROS
%%%-----------------------------------------------------------------------------
-define(PT_KEY(Ref), {?MODULE, Ref}).
-define(START_TIMEOUT, 5000).
-define(PERSIST_TIMEOUT, 5000).
-define(STOP_TIMEOUT, 5000).

%%%-----------------------------------------------------------------------------
%% RECORDS
%%%-----------------------------------------------------------------------------
-record(cookie, {
    key :: {binary(), binary(), binary()},
    value :: binary(),
    expires :: integer() | session,
    secure :: boolean(),
    http_only :: boolean(),
    same_site :: strict | lax | none | undefined,
    creation_time :: integer(),
    last_access_time :: integer()
}).

-record(owner_state, {
    tab :: ets:tid(),
    persist :: memory | file,
    file :: file:filename() | undefined,
    cleanup_interval :: pos_integer()
}).

%%%-----------------------------------------------------------------------------
%% API
%%%-----------------------------------------------------------------------------
-doc "Get all cookies in the jar as a list.".
-spec all(jar()) -> {ok, [map()]} | {error, nhttpc:error()}.
all(Jar) ->
    case resolve(Jar) of
        {ok, #{tab := Tab}} ->
            Cookies = ets:tab2list(Tab),
            {ok, lists:map(fun record_to_map/1, Cookies)};
        {error, _} = E ->
            E
    end.

-doc "Clear all cookies from the jar.".
-spec clear(jar()) -> ok | {error, nhttpc:error()}.
clear(Jar) ->
    case resolve(Jar) of
        {ok, #{tab := Tab}} ->
            true = ets:delete_all_objects(Tab),
            ok;
        {error, _} = E ->
            E
    end.

-doc "Clear all cookies for a specific domain.".
-spec clear(jar(), binary()) -> ok | {error, nhttpc:error()}.
clear(Jar, Domain) ->
    case resolve(Jar) of
        {ok, #{tab := Tab}} ->
            _ = ets:select_delete(Tab, domain_spec(Domain, true)),
            ok;
        {error, _} = E ->
            E
    end.

-doc """
Get cookies matching the given URI.
Returns `{ok, [{Name, Value}]}` sorted by path length (longest first).
""".
-spec get(jar(), uri()) -> {ok, [{binary(), binary()}]} | {error, nhttpc:error()}.
get(Jar, Uri) ->
    case resolve(Jar) of
        {ok, #{tab := Tab}} ->
            {ok, do_get(Tab, Uri)};
        {error, _} = E ->
            E
    end.

-doc """
Get the `Cookie` header value for a URI.
Returns `{ok, binary()}` when at least one cookie matches, or
`{ok, undefined}` when none match.
""".
-spec get_header(jar(), uri()) ->
    {ok, binary() | undefined} | {error, nhttpc:error()}.
get_header(Jar, Uri) ->
    case get(Jar, Uri) of
        {ok, []} ->
            {ok, undefined};
        {ok, Cookies} ->
            Pairs = [[Name, <<"=">>, Value] || {Name, Value} <- Cookies],
            {ok, iolist_to_binary(lists:join(<<"; ">>, Pairs))};
        {error, _} = E ->
            E
    end.

-doc "Trigger persistence (if configured).".
-spec persist(jar()) -> ok | {error, nhttpc:error()}.
persist(Jar) ->
    case resolve(Jar) of
        {ok, #{pid := Pid}} ->
            Pid ! {persist, self()},
            receive
                {Pid, Result} -> Result
            after ?PERSIST_TIMEOUT ->
                {error, {cookie_jar_error, persist_timeout}}
            end;
        {error, _} = E ->
            E
    end.

-doc """
Set a cookie from a parsed cookie map.
Cookie map should have: name, value, and optionally domain, path,
expires, max_age, secure, http_only, same_site.
""".
-spec set(jar(), uri(), map()) -> ok | {error, nhttpc:error()}.
set(Jar, Uri, Cookie) ->
    case resolve(Jar) of
        {ok, #{tab := Tab}} ->
            Record = cookie_to_record(Cookie, Uri),
            true = ets:insert(Tab, Record),
            ok;
        {error, _} = E ->
            E
    end.

-doc """
Process Set-Cookie headers from a response.
Parses each Set-Cookie header and stores valid cookies.
""".
-spec set_from_headers(jar(), uri(), [{binary(), binary()}]) ->
    ok | {error, nhttpc:error()}.
set_from_headers(Jar, Uri, Headers) ->
    case resolve(Jar) of
        {ok, #{tab := Tab}} ->
            SetCookies = extract_set_cookies(Headers),
            lists:foreach(
                fun(SetCookieValue) ->
                    case nhttp_cookie:decode_set_cookie(SetCookieValue) of
                        {ok, Cookie} ->
                            Record = cookie_to_record(Cookie, Uri),
                            ets:insert(Tab, Record);
                        {error, _} ->
                            ok
                    end
                end,
                SetCookies
            ),
            ok;
        {error, _} = E ->
            E
    end.

-doc "Start a cookie jar with default options.".
-spec start_link() -> {ok, jar()}.
start_link() ->
    start_link(#{}).

-doc """
Start a cookie jar with options.
Options:
  - `persist` => `memory` | `file` (default: `memory`)
  - `file` => Path (required if persist => file)
  - `cleanup_interval` => Ms (default: 60000)
""".
-spec start_link(map()) -> {ok, jar()}.
start_link(Opts) ->
    Caller = self(),
    Owner = spawn_link(fun() -> owner_init(Opts, Caller) end),
    receive
        {Owner, started, Tab} ->
            Ref = make_ref(),
            persistent_term:put(?PT_KEY(Ref), #{pid => Owner, tab => Tab}),
            _ = spawn(fun() -> reaper(Owner, Ref) end),
            {ok, Ref}
    after ?START_TIMEOUT ->
        unlink(Owner),
        exit(Owner, kill),
        error(cookie_jar_start_timeout)
    end.

-doc "Stop the cookie jar and cleanup resources.".
-spec stop(jar()) -> ok | {error, nhttpc:error()}.
stop(Jar) ->
    case resolve(Jar) of
        {ok, #{pid := Pid}} ->
            persistent_term:erase(?PT_KEY(Jar)),
            MRef = erlang:monitor(process, Pid),
            Pid ! {stop, self()},
            receive
                {Pid, _PersistResult} -> ok
            after ?STOP_TIMEOUT ->
                exit(Pid, kill)
            end,
            receive
                {'DOWN', MRef, process, Pid, _} -> ok
            after ?STOP_TIMEOUT ->
                ok
            end;
        {error, _} = E ->
            E
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL — RESOLUTION
%%%-----------------------------------------------------------------------------
-doc false.
-spec resolve(jar()) -> {ok, state()} | {error, nhttpc:error()}.
resolve(Ref) when is_reference(Ref) ->
    case persistent_term:get(?PT_KEY(Ref), undefined) of
        undefined ->
            {error, {cookie_jar_error, stopped}};
        State ->
            {ok, State}
    end;
resolve(_Other) ->
    {error, {cookie_jar_error, stopped}}.

%%%-----------------------------------------------------------------------------
%% INTERNAL — GET
%%%-----------------------------------------------------------------------------
-spec do_get(ets:tid(), uri()) -> [{binary(), binary()}].
do_get(Tab, Uri) ->
    #{scheme := Scheme, host := Host, path := Path} = Uri,
    Now = erlang:system_time(second),
    IsSecure = is_secure_scheme(Scheme),
    Matching = lists:flatmap(
        fun(Domain) ->
            Cookies = ets:select(Tab, domain_spec(Domain, '$_')),
            lists:filter(
                fun(Cookie) -> cookie_matches(Cookie, Host, Path, IsSecure, Now) end,
                Cookies
            )
        end,
        domain_candidates(Host)
    ),
    Sorted = lists:sort(fun cookie_sort/2, Matching),
    [{Name, Value} || #cookie{key = {_, _, Name}, value = Value} <- Sorted].

-spec domain_candidates(binary()) -> [binary()].
domain_candidates(Host) ->
    [Host | parent_domains(Host)].

-spec domain_spec(binary(), '$_' | true) -> ets:match_spec().
domain_spec(Domain, Body) ->
    Head = erlang:make_tuple(
        record_info(size, cookie),
        '_',
        [{1, cookie}, {#cookie.key, {Domain, '_', '_'}}]
    ),
    [{Head, [], [Body]}].

-spec parent_domains(binary()) -> [binary()].
parent_domains(Host) ->
    case binary:match(Host, <<".">>) of
        nomatch ->
            [];
        {Pos, 1} ->
            Rest = binary:part(Host, Pos + 1, byte_size(Host) - Pos - 1),
            [Rest | parent_domains(Rest)]
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL — REAPER (OWNER-DOWN MONITOR)
%%%-----------------------------------------------------------------------------
-spec reaper(pid(), reference()) -> ok.
reaper(Owner, Ref) ->
    MRef = erlang:monitor(process, Owner),
    receive
        {'DOWN', MRef, process, Owner, _Reason} ->
            persistent_term:erase(?PT_KEY(Ref)),
            ok
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL — OWNER PROCESS
%%%-----------------------------------------------------------------------------
-spec cleanup_expired(ets:tid()) -> non_neg_integer().
cleanup_expired(Tab) ->
    Now = erlang:system_time(second),
    ets:select_delete(
        Tab,
        ets:fun2ms(fun(#cookie{expires = E}) when E =/= session, E < Now -> true end)
    ).

-spec maybe_load_from_file(#owner_state{}) -> ok.
maybe_load_from_file(#owner_state{persist = file, file = File, tab = Tab}) when
    File =/= undefined
->
    case file:read_file(File) of
        {ok, Binary} ->
            case binary_to_term(Binary) of
                Cookies when is_list(Cookies) ->
                    ets:insert(Tab, Cookies),
                    ok;
                _ ->
                    ok
            end;
        {error, _} ->
            ok
    end;
maybe_load_from_file(_) ->
    ok.

-spec owner_init(map(), pid()) -> no_return().
owner_init(Opts, Parent) ->
    process_flag(trap_exit, true),
    Tab = ets:new(?MODULE, [
        ordered_set,
        public,
        {keypos, #cookie.key},
        {read_concurrency, true}
    ]),
    State = #owner_state{
        tab = Tab,
        persist = maps:get(persist, Opts, memory),
        file = maps:get(file, Opts, undefined),
        cleanup_interval = maps:get(cleanup_interval, Opts, 60000)
    },
    maybe_load_from_file(State),
    Parent ! {self(), started, Tab},
    owner_loop(State).

-spec owner_loop(#owner_state{}) -> no_return().
owner_loop(#owner_state{tab = Tab, cleanup_interval = Interval} = State) ->
    receive
        {persist, From} ->
            Result = persist_to_file(State),
            From ! {self(), Result},
            owner_loop(State);
        {stop, From} ->
            Result = persist_to_file(State),
            ets:delete(Tab),
            From ! {self(), Result},
            exit(normal);
        {'EXIT', _From, Reason} ->
            _ = persist_to_file(State),
            ets:delete(Tab),
            exit(Reason)
    after Interval ->
        cleanup_expired(Tab),
        owner_loop(State)
    end.

-spec persist_to_file(#owner_state{}) -> ok | {error, nhttpc:error()}.
persist_to_file(#owner_state{persist = file, file = File, tab = Tab}) when
    File =/= undefined
->
    Cookies = ets:tab2list(Tab),
    Persistent = [C || C = #cookie{expires = Exp} <- Cookies, Exp =/= session],
    Binary = term_to_binary(Persistent),
    case file:write_file(File, Binary) of
        ok -> ok;
        {error, Reason} -> {error, {cookie_jar_error, {file, Reason}}}
    end;
persist_to_file(_) ->
    ok.

%%%-----------------------------------------------------------------------------
%% INTERNAL — COOKIE MATCHING
%%%-----------------------------------------------------------------------------
-spec cookie_matches(#cookie{}, binary(), binary(), boolean(), integer()) -> boolean().
cookie_matches(
    #cookie{key = {Domain, CookiePath, _}, expires = Expires, secure = Secure},
    Host,
    Path,
    IsSecure,
    Now
) ->
    NotExpired =
        case Expires of
            session -> true;
            ExpTime -> ExpTime > Now
        end,
    NotExpired andalso
        (not Secure orelse IsSecure) andalso
        domain_matches(Host, Domain) andalso
        path_matches(Path, CookiePath).

-spec cookie_sort(#cookie{}, #cookie{}) -> boolean().
cookie_sort(
    #cookie{key = {_, PathA, _}, creation_time = TimeA},
    #cookie{key = {_, PathB, _}, creation_time = TimeB}
) ->
    case byte_size(PathA) =:= byte_size(PathB) of
        true -> TimeA =< TimeB;
        false -> byte_size(PathA) > byte_size(PathB)
    end.

-spec domain_matches(binary(), binary()) -> boolean().
domain_matches(Host, Domain) ->
    Host =:= Domain orelse
        case Domain of
            <<$., Rest/binary>> ->
                suffix_match(Host, Rest);
            _ ->
                suffix_match(Host, Domain)
        end.

-spec is_secure_scheme(http | https | binary()) -> boolean().
is_secure_scheme(https) -> true;
is_secure_scheme(<<"https">>) -> true;
is_secure_scheme(_) -> false.

-spec path_matches(binary(), binary()) -> boolean().
path_matches(Path, CookiePath) ->
    Path =:= CookiePath orelse
        begin
            CookieLen = byte_size(CookiePath),
            PathLen = byte_size(Path),
            PathLen > CookieLen andalso
                binary:part(Path, 0, CookieLen) =:= CookiePath andalso
                (binary:last(CookiePath) =:= $/ orelse
                    binary:at(Path, CookieLen) =:= $/)
        end.

-spec suffix_match(binary(), binary()) -> boolean().
suffix_match(Host, Domain) ->
    HostLen = byte_size(Host),
    DomainLen = byte_size(Domain),
    case HostLen > DomainLen of
        true ->
            Suffix = binary:part(Host, HostLen - DomainLen, DomainLen),
            Prefix = binary:part(Host, HostLen - DomainLen - 1, 1),
            Suffix =:= Domain andalso Prefix =:= <<".">>;
        false ->
            false
    end.

%%%-----------------------------------------------------------------------------
%% INTERNAL — COOKIE CONVERSION
%%%-----------------------------------------------------------------------------
-spec cookie_to_record(map(), uri()) -> #cookie{}.
cookie_to_record(Cookie, Uri) ->
    #{name := Name, value := Value} = Cookie,
    Now = erlang:system_time(second),

    Domain =
        case maps:get(domain, Cookie, undefined) of
            undefined -> maps:get(host, Uri);
            D -> normalize_domain(D)
        end,

    Path =
        case maps:get(path, Cookie, undefined) of
            undefined -> default_path(maps:get(path, Uri, <<"/">>));
            P -> P
        end,

    Expires =
        case maps:get(max_age, Cookie, undefined) of
            undefined ->
                case maps:get(expires, Cookie, undefined) of
                    undefined -> session;
                    Exp -> datetime_to_timestamp(Exp)
                end;
            MaxAge when is_integer(MaxAge) ->
                Now + MaxAge
        end,

    #cookie{
        key = {Domain, Path, Name},
        value = Value,
        expires = Expires,
        secure = maps:get(secure, Cookie, false),
        http_only = maps:get(http_only, Cookie, false),
        same_site = maps:get(same_site, Cookie, undefined),
        creation_time = Now,
        last_access_time = Now
    }.

-spec datetime_to_timestamp(calendar:datetime()) -> integer().
datetime_to_timestamp(DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime) - 62167219200.

-spec default_path(binary()) -> binary().
default_path(<<"/">>) ->
    <<"/">>;
default_path(Path) ->
    case binary:last(Path) of
        $/ ->
            Path;
        _ ->
            case binary:split(Path, <<"/">>, [global, trim_all]) of
                [] ->
                    <<"/">>;
                Parts ->
                    case lists:droplast(Parts) of
                        [] -> <<"/">>;
                        Prefix -> iolist_to_binary([<<"/">>, lists:join(<<"/">>, Prefix)])
                    end
            end
    end.

-spec extract_set_cookies([{binary(), binary()}]) -> [binary()].
extract_set_cookies(Headers) ->
    [V || {K, V} <- Headers, K =:= <<"set-cookie">>].

-spec normalize_domain(binary()) -> binary().
normalize_domain(<<$., Rest/binary>>) -> Rest;
normalize_domain(Domain) -> Domain.

-spec record_to_map(#cookie{}) -> map().
record_to_map(#cookie{
    key = {Domain, Path, Name},
    value = Value,
    expires = Expires,
    secure = Secure,
    http_only = HttpOnly,
    same_site = SameSite,
    creation_time = CreationTime
}) ->
    #{
        name => Name,
        value => Value,
        domain => Domain,
        path => Path,
        expires => Expires,
        secure => Secure,
        http_only => HttpOnly,
        same_site => SameSite,
        creation_time => CreationTime
    }.
