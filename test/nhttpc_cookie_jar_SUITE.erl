%%%-----------------------------------------------------------------------------
%%% @doc Cookie jar test suite.
%%%
%%% Tests the nhttpc_cookie_jar module for cookie storage, retrieval,
%%% persistence, and cleanup functionality.
%%% @end
%%%-----------------------------------------------------------------------------
-module(nhttpc_cookie_jar_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%-----------------------------------------------------------------------------
%%% EXPORTS
%%%-----------------------------------------------------------------------------

%% CT callbacks
-export([
    all/0,
    suite/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Basic tests
-export([
    cookie_jar_new_stop/1,
    cookie_jar_set_get/1,
    cookie_jar_get_header/1,
    cookie_jar_clear/1,
    cookie_jar_all/1
]).

%% Persistence tests
-export([
    cookie_jar_persist_memory/1,
    cookie_jar_persist_file/1,
    cookie_jar_load_from_file/1
]).

%% Expiration tests
-export([
    cookie_jar_expired_cleanup/1,
    cookie_jar_session_cookies/1
]).

%% Edge cases
-export([
    cookie_jar_secure_cookie/1,
    cookie_jar_path_matching/1,
    cookie_jar_domain_matching/1
]).

%% Opacity / uniform returns (plan 08)
-export([
    start_link_returns_opaque_reference/1,
    get_header_returns_ok_undefined_when_no_cookies/1,
    persist_timeout_returns_typed_error/1,
    operations_on_stopped_jar_return_stopped_error/1,
    pid_in_cookie_jar_opt_returns_invalid_opts/1,
    owner_down_cleans_persistent_term/1
]).

%% Domain-bucket index (plan 27)
-export([
    index_exact_domain_isolation/1,
    index_subdomain_inheritance/1,
    index_attributes_preserved/1,
    index_clear_by_domain_scoped/1,
    index_expired_cleanup_scoped/1,
    index_get_scales_flat/1
]).

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
        {group, basic},
        {group, persistence},
        {group, expiration},
        {group, edge_cases},
        {group, opacity},
        {group, index}
    ].

groups() ->
    [
        {basic, [sequence], [
            cookie_jar_new_stop,
            cookie_jar_set_get,
            cookie_jar_get_header,
            cookie_jar_clear,
            cookie_jar_all
        ]},
        {persistence, [sequence], [
            cookie_jar_persist_memory,
            cookie_jar_persist_file,
            cookie_jar_load_from_file
        ]},
        {expiration, [sequence], [
            cookie_jar_expired_cleanup,
            cookie_jar_session_cookies
        ]},
        {edge_cases, [sequence], [
            cookie_jar_secure_cookie,
            cookie_jar_path_matching,
            cookie_jar_domain_matching
        ]},
        {opacity, [sequence], [
            start_link_returns_opaque_reference,
            get_header_returns_ok_undefined_when_no_cookies,
            persist_timeout_returns_typed_error,
            operations_on_stopped_jar_return_stopped_error,
            pid_in_cookie_jar_opt_returns_invalid_opts,
            owner_down_cleans_persistent_term
        ]},
        {index, [sequence], [
            index_exact_domain_isolation,
            index_subdomain_inheritance,
            index_attributes_preserved,
            index_clear_by_domain_scoped,
            index_expired_cleanup_scoped,
            index_get_scales_flat
        ]}
    ].

init_per_suite(Config) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    [{priv_dir, PrivDir} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% BASIC TESTS
%%%-----------------------------------------------------------------------------

cookie_jar_new_stop(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

cookie_jar_set_get(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    Cookie = #{name => <<"session">>, value => <<"abc123">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),
    {ok, Cookies} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([{<<"session">>, <<"abc123">>}], Cookies),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

cookie_jar_get_header(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    Cookie1 = #{name => <<"name1">>, value => <<"value1">>},
    Cookie2 = #{name => <<"name2">>, value => <<"value2">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie1),
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie2),
    {ok, Header} = nhttpc_cookie_jar:get_header(Jar, Uri),
    %% Header should contain both cookies joined by "; "
    ?assert(binary:match(Header, <<"name1=value1">>) =/= nomatch),
    ?assert(binary:match(Header, <<"name2=value2">>) =/= nomatch),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

cookie_jar_clear(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    Cookie = #{name => <<"test">>, value => <<"value">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),
    {ok, [_]} = nhttpc_cookie_jar:get(Jar, Uri),
    ok = nhttpc_cookie_jar:clear(Jar),
    {ok, []} = nhttpc_cookie_jar:get(Jar, Uri),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

cookie_jar_all(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri1 = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    Uri2 = #{scheme => http, host => <<"other.com">>, path => <<"/">>},
    Cookie1 = #{name => <<"c1">>, value => <<"v1">>},
    Cookie2 = #{name => <<"c2">>, value => <<"v2">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri1, Cookie1),
    ok = nhttpc_cookie_jar:set(Jar, Uri2, Cookie2),
    {ok, All} = nhttpc_cookie_jar:all(Jar),
    ?assertEqual(2, length(All)),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%%%-----------------------------------------------------------------------------
%%% PERSISTENCE TESTS
%%%-----------------------------------------------------------------------------

cookie_jar_persist_memory(_Config) ->
    %% Default is memory persistence (no file)
    {ok, Jar} = nhttpc_cookie_jar:start_link(#{persist => memory}),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    Cookie = #{name => <<"test">>, value => <<"value">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),
    ok = nhttpc_cookie_jar:persist(Jar),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

cookie_jar_persist_file(Config) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    File = filename:join(PrivDir, "test_cookies.dat"),
    {ok, Jar} = nhttpc_cookie_jar:start_link(#{persist => file, file => File}),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    Cookie = #{name => <<"persistent">>, value => <<"cookie">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),
    ok = nhttpc_cookie_jar:persist(Jar),
    ok = nhttpc_cookie_jar:stop(Jar),
    %% Verify file was created
    ?assert(filelib:is_file(File)),
    ok.

cookie_jar_load_from_file(Config) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    File = filename:join(PrivDir, "test_load_cookies.dat"),
    %% Create a jar and save a cookie
    {ok, Jar1} = nhttpc_cookie_jar:start_link(#{persist => file, file => File}),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    %% Use max_age to make it a persistent cookie (not session)
    Cookie = #{name => <<"loaded">>, value => <<"fromfile">>, max_age => 3600},
    ok = nhttpc_cookie_jar:set(Jar1, Uri, Cookie),
    ok = nhttpc_cookie_jar:stop(Jar1),
    %% Create a new jar with the same file and verify cookie is loaded
    {ok, Jar2} = nhttpc_cookie_jar:start_link(#{persist => file, file => File}),
    {ok, Cookies} = nhttpc_cookie_jar:get(Jar2, Uri),
    ?assertEqual([{<<"loaded">>, <<"fromfile">>}], Cookies),
    ok = nhttpc_cookie_jar:stop(Jar2),
    ok.

%%%-----------------------------------------------------------------------------
%%% EXPIRATION TESTS
%%%-----------------------------------------------------------------------------

cookie_jar_expired_cleanup(_Config) ->
    %% Use short cleanup interval to test expiration
    {ok, Jar} = nhttpc_cookie_jar:start_link(#{cleanup_interval => 100}),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    %% Set a cookie that expires in 1 second using max_age
    Cookie = #{name => <<"expiring">>, value => <<"soon">>, max_age => 1},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),
    %% Cookie should be returned immediately
    {ok, CookiesBefore} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([{<<"expiring">>, <<"soon">>}], CookiesBefore),
    %% Wait for it to expire and cleanup to run
    timer:sleep(1200),
    %% Expired cookie shouldn't be returned
    {ok, CookiesAfter} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([], CookiesAfter),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

cookie_jar_session_cookies(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    %% Session cookie (no expires)
    Cookie = #{name => <<"session">>, value => <<"data">>},
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),
    {ok, Cookies} = nhttpc_cookie_jar:get(Jar, Uri),
    ?assertEqual([{<<"session">>, <<"data">>}], Cookies),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%%%-----------------------------------------------------------------------------
%%% EDGE CASES
%%%-----------------------------------------------------------------------------

cookie_jar_secure_cookie(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    %% Set secure cookie
    HttpsUri = #{scheme => https, host => <<"example.com">>, path => <<"/">>},
    SecureCookie = #{name => <<"secure">>, value => <<"secret">>, secure => true},
    ok = nhttpc_cookie_jar:set(Jar, HttpsUri, SecureCookie),
    %% Should be returned for HTTPS
    {ok, HttpsCookies} = nhttpc_cookie_jar:get(Jar, HttpsUri),
    ?assertEqual([{<<"secure">>, <<"secret">>}], HttpsCookies),
    %% Should NOT be returned for HTTP
    HttpUri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    {ok, HttpCookies} = nhttpc_cookie_jar:get(Jar, HttpUri),
    ?assertEqual([], HttpCookies),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

cookie_jar_path_matching(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    BaseUri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    ApiUri = #{scheme => http, host => <<"example.com">>, path => <<"/api">>},
    %% Set cookie for /api path
    Cookie = #{name => <<"api_token">>, value => <<"xyz">>, path => <<"/api">>},
    ok = nhttpc_cookie_jar:set(Jar, ApiUri, Cookie),
    %% Should match /api path
    {ok, ApiCookies} = nhttpc_cookie_jar:get(Jar, ApiUri),
    ?assertEqual([{<<"api_token">>, <<"xyz">>}], ApiCookies),
    %% Should not match root path
    {ok, RootCookies} = nhttpc_cookie_jar:get(Jar, BaseUri),
    ?assertEqual([], RootCookies),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

cookie_jar_domain_matching(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    MainUri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    SubUri = #{scheme => http, host => <<"sub.example.com">>, path => <<"/">>},
    %% Set cookie with domain attribute
    Cookie = #{name => <<"domain_cookie">>, value => <<"shared">>, domain => <<".example.com">>},
    ok = nhttpc_cookie_jar:set(Jar, MainUri, Cookie),
    %% Should match main domain
    {ok, MainCookies} = nhttpc_cookie_jar:get(Jar, MainUri),
    ?assert(length(MainCookies) > 0),
    %% Should match subdomain
    {ok, SubCookies} = nhttpc_cookie_jar:get(Jar, SubUri),
    ?assert(length(SubCookies) > 0),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

%%%-----------------------------------------------------------------------------
%%% OPACITY / UNIFORM RETURNS (plan 08)
%%%-----------------------------------------------------------------------------

start_link_returns_opaque_reference(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    ?assert(is_reference(Jar)),
    ?assertNot(is_pid(Jar)),
    ?assertNot(is_tuple(Jar)),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

get_header_returns_ok_undefined_when_no_cookies(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    ?assertEqual({ok, undefined}, nhttpc_cookie_jar:get_header(Jar, Uri)),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

persist_timeout_returns_typed_error(_Config) ->
    %% Drive a persist_timeout by suspending the owner so the persist
    %% reply never arrives within the receive window.
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    #{pid := Pid} = persistent_term:get({nhttpc_cookie_jar, Jar}),
    true = erlang:suspend_process(Pid),
    try
        ?assertEqual(
            {error, {cookie_jar_error, persist_timeout}},
            nhttpc_cookie_jar:persist(Jar)
        )
    after
        true = erlang:resume_process(Pid),
        %% Drain the late persist reply if it shows up.
        receive
            {Pid, _} -> ok
        after 50 ->
            ok
        end,
        ok = nhttpc_cookie_jar:stop(Jar)
    end,
    ok.

operations_on_stopped_jar_return_stopped_error(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    ok = nhttpc_cookie_jar:stop(Jar),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    Err = {error, {cookie_jar_error, stopped}},
    ?assertEqual(Err, nhttpc_cookie_jar:get(Jar, Uri)),
    ?assertEqual(Err, nhttpc_cookie_jar:get_header(Jar, Uri)),
    ?assertEqual(
        Err,
        nhttpc_cookie_jar:set(Jar, Uri, #{name => <<"n">>, value => <<"v">>})
    ),
    ?assertEqual(Err, nhttpc_cookie_jar:set_from_headers(Jar, Uri, [])),
    ?assertEqual(Err, nhttpc_cookie_jar:clear(Jar)),
    ?assertEqual(Err, nhttpc_cookie_jar:clear(Jar, <<"example.com">>)),
    ?assertEqual(Err, nhttpc_cookie_jar:all(Jar)),
    ?assertEqual(Err, nhttpc_cookie_jar:persist(Jar)),
    ?assertEqual(Err, nhttpc_cookie_jar:stop(Jar)),
    ok.

pid_in_cookie_jar_opt_returns_invalid_opts(_Config) ->
    PidShape = self(),
    ?assertMatch(
        {error, {invalid_opts, {bad_cookie_jar, _}}},
        nhttpc_opts:normalize(#{cookie_jar => PidShape})
    ),
    ?assertMatch(
        {error, {invalid_opts, {bad_cookie_jar, _}}},
        nhttpc_opts:normalize(#{cookie_jar => some_atom})
    ),
    ?assertMatch(
        {error, {invalid_opts, {bad_cookie_jar, _}}},
        nhttpc_opts:normalize(#{cookie_jar => {self(), make_ref()}})
    ),
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    ?assertMatch(
        {ok, _},
        nhttpc_opts:normalize(#{cookie_jar => Jar})
    ),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

owner_down_cleans_persistent_term(_Config) ->
    %% Start the jar in an unlinked process so killing its owner
    %% doesn't propagate to the test runner.
    Parent = self(),
    Helper = spawn(fun() ->
        {ok, J} = nhttpc_cookie_jar:start_link(),
        Parent ! {jar, J},
        receive
            done -> ok
        end
    end),
    Jar =
        receive
            {jar, J} -> J
        after 1000 ->
            ct:fail(jar_not_started)
        end,
    Key = {nhttpc_cookie_jar, Jar},
    #{pid := Pid} = persistent_term:get(Key),
    MRef = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 1000 ->
        ct:fail(owner_did_not_die)
    end,
    %% Reaper races with this; poll for cleanup.
    ok = wait_until_erased(Key, 50),
    ?assertEqual(
        {error, {cookie_jar_error, stopped}},
        nhttpc_cookie_jar:get(Jar, #{
            scheme => http,
            host => <<"example.com">>,
            path => <<"/">>
        })
    ),
    Helper ! done,
    ok.

-spec wait_until_erased({atom(), reference()}, non_neg_integer()) -> ok.
wait_until_erased(_Key, 0) ->
    ct:fail(persistent_term_not_erased);
wait_until_erased(Key, N) ->
    case persistent_term:get(Key, undefined) of
        undefined ->
            ok;
        _ ->
            timer:sleep(20),
            wait_until_erased(Key, N - 1)
    end.

%%%-----------------------------------------------------------------------------
%%% DOMAIN-BUCKET INDEX (plan 27)
%%%-----------------------------------------------------------------------------

index_exact_domain_isolation(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Set = fun(Host, Name) ->
        Uri = #{scheme => http, host => Host, path => <<"/">>},
        ok = nhttpc_cookie_jar:set(Jar, Uri, #{name => Name, value => Host})
    end,
    Set(<<"a.com">>, <<"ca">>),
    Set(<<"b.com">>, <<"cb">>),
    Set(<<"xa.com">>, <<"cxa">>),
    Get = fun(Host) ->
        {ok, Cs} = nhttpc_cookie_jar:get(Jar, #{
            scheme => http, host => Host, path => <<"/">>
        }),
        Cs
    end,
    ?assertEqual([{<<"ca">>, <<"a.com">>}], Get(<<"a.com">>)),
    ?assertEqual([{<<"cb">>, <<"b.com">>}], Get(<<"b.com">>)),
    ?assertEqual([{<<"cxa">>, <<"xa.com">>}], Get(<<"xa.com">>)),
    ?assertEqual([], Get(<<"c.com">>)),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

index_subdomain_inheritance(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Uri = #{scheme => http, host => <<"example.com">>, path => <<"/">>},
    Cookie = #{
        name => <<"shared">>, value => <<"v">>, domain => <<".example.com">>
    },
    ok = nhttpc_cookie_jar:set(Jar, Uri, Cookie),
    Get = fun(Host) ->
        {ok, Cs} = nhttpc_cookie_jar:get(Jar, #{
            scheme => http, host => Host, path => <<"/">>
        }),
        Cs
    end,
    ?assertEqual([{<<"shared">>, <<"v">>}], Get(<<"example.com">>)),
    ?assertEqual([{<<"shared">>, <<"v">>}], Get(<<"deep.sub.example.com">>)),
    ?assertEqual([], Get(<<"notexample.com">>)),
    ?assertEqual([], Get(<<"example.com.evil.com">>)),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

index_attributes_preserved(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    HttpsUri = #{scheme => https, host => <<"secure.test">>, path => <<"/app">>},
    Cookie = #{
        name => <<"sid">>,
        value => <<"deadbeef">>,
        path => <<"/app">>,
        secure => true,
        http_only => true,
        same_site => strict,
        max_age => 3600
    },
    ok = nhttpc_cookie_jar:set(Jar, HttpsUri, Cookie),
    {ok, [Map]} = nhttpc_cookie_jar:all(Jar),
    ?assertEqual(<<"sid">>, maps:get(name, Map)),
    ?assertEqual(<<"secure.test">>, maps:get(domain, Map)),
    ?assertEqual(<<"/app">>, maps:get(path, Map)),
    ?assertEqual(true, maps:get(secure, Map)),
    ?assertEqual(true, maps:get(http_only, Map)),
    ?assertEqual(strict, maps:get(same_site, Map)),
    {ok, Secure} = nhttpc_cookie_jar:get(Jar, HttpsUri),
    ?assertEqual([{<<"sid">>, <<"deadbeef">>}], Secure),
    {ok, Plain} = nhttpc_cookie_jar:get(Jar, HttpsUri#{scheme := http}),
    ?assertEqual([], Plain),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

index_clear_by_domain_scoped(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Set = fun(Host) ->
        Uri = #{scheme => http, host => Host, path => <<"/">>},
        ok = nhttpc_cookie_jar:set(Jar, Uri, #{name => <<"n">>, value => Host})
    end,
    Set(<<"example.com">>),
    Set(<<"sub.example.com">>),
    Set(<<"other.com">>),
    ok = nhttpc_cookie_jar:clear(Jar, <<"example.com">>),
    Get = fun(Host) ->
        {ok, Cs} = nhttpc_cookie_jar:get(Jar, #{
            scheme => http, host => Host, path => <<"/">>
        }),
        Cs
    end,
    ?assertEqual([], Get(<<"example.com">>)),
    ?assertEqual([{<<"n">>, <<"sub.example.com">>}], Get(<<"sub.example.com">>)),
    ?assertEqual([{<<"n">>, <<"other.com">>}], Get(<<"other.com">>)),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

index_expired_cleanup_scoped(_Config) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(#{cleanup_interval => 30}),
    UriA = #{scheme => http, host => <<"keep.test">>, path => <<"/">>},
    UriB = #{scheme => http, host => <<"drop.test">>, path => <<"/">>},
    Fresh = #{name => <<"fresh">>, value => <<"v">>, max_age => 3600},
    Expired = #{
        name => <<"old">>, value => <<"v">>, expires => {{1971, 1, 1}, {0, 0, 0}}
    },
    ok = nhttpc_cookie_jar:set(Jar, UriA, Fresh),
    ok = nhttpc_cookie_jar:set(Jar, UriB, Expired),
    OnlyFresh = fun() ->
        case nhttpc_cookie_jar:all(Jar) of
            {ok, [#{name := <<"fresh">>, domain := <<"keep.test">>}]} -> true;
            _ -> false
        end
    end,
    ok = poll_until(OnlyFresh, 100),
    {ok, Kept} = nhttpc_cookie_jar:get(Jar, UriA),
    ?assertEqual([{<<"fresh">>, <<"v">>}], Kept),
    ok = nhttpc_cookie_jar:stop(Jar),
    ok.

index_get_scales_flat(_Config) ->
    SmallPer = bench_get(20),
    LargePer = bench_get(2000),
    ct:log("get/2 per-call us: small(20)=~p large(2000)=~p", [SmallPer, LargePer]),
    %% O(matches) keeps the curve flat; an O(jar-size) regression at a
    %% 100x larger jar would blow well past the 6x + 30us slack.
    ?assert(LargePer =< SmallPer * 6 + 30),
    ok.

-spec bench_get(pos_integer()) -> float().
bench_get(NDomains) ->
    {ok, Jar} = nhttpc_cookie_jar:start_link(),
    Target = #{scheme => http, host => <<"target.bench">>, path => <<"/">>},
    lists:foreach(
        fun(I) ->
            Host = iolist_to_binary([
                <<"d">>, integer_to_binary(I), <<".bench">>
            ]),
            Uri = #{scheme => http, host => Host, path => <<"/">>},
            lists:foreach(
                fun(J) ->
                    Name = iolist_to_binary([<<"c">>, integer_to_binary(J)]),
                    ok = nhttpc_cookie_jar:set(Jar, Uri, #{
                        name => Name, value => <<"v">>
                    })
                end,
                lists:seq(1, 5)
            )
        end,
        lists:seq(1, NDomains)
    ),
    lists:foreach(
        fun(J) ->
            Name = iolist_to_binary([<<"t">>, integer_to_binary(J)]),
            ok = nhttpc_cookie_jar:set(Jar, Target, #{
                name => Name, value => <<"v">>
            })
        end,
        lists:seq(1, 5)
    ),
    _ = time_gets(Jar, Target, 200),
    Reps = [time_gets(Jar, Target, 1000) || _ <- lists:seq(1, 3)],
    ok = nhttpc_cookie_jar:stop(Jar),
    lists:min(Reps).

-spec time_gets(nhttpc_cookie_jar:jar(), map(), pos_integer()) -> float().
time_gets(Jar, Uri, K) ->
    T0 = erlang:monotonic_time(microsecond),
    do_gets(Jar, Uri, K),
    T1 = erlang:monotonic_time(microsecond),
    (T1 - T0) / K.

-spec do_gets(nhttpc_cookie_jar:jar(), map(), non_neg_integer()) -> ok.
do_gets(_Jar, _Uri, 0) ->
    ok;
do_gets(Jar, Uri, K) ->
    {ok, [_, _, _, _, _]} = nhttpc_cookie_jar:get(Jar, Uri),
    do_gets(Jar, Uri, K - 1).

-spec poll_until(fun(() -> boolean()), non_neg_integer()) -> ok.
poll_until(_Pred, 0) ->
    ct:fail(poll_condition_not_met);
poll_until(Pred, N) ->
    case Pred() of
        true ->
            ok;
        false ->
            timer:sleep(20),
            poll_until(Pred, N - 1)
    end.
