-module(nhttpc_prop_SUITE).

-moduledoc """
Property-based tests for nhttpc using triq.

Tests properties of:
- URL parsing (nhttpc_url)
- Retry logic (nhttpc_retry)
- Request outcomes against dropped connections (nhttpc_chaos_server)
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("triq/include/triq.hrl").

%%%-----------------------------------------------------------------------------
%%% CT CALLBACKS
%%%-----------------------------------------------------------------------------

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

%% URL property tests
-export([
    prop_url_parse_valid_urls/1,
    prop_url_path_starts_with_slash/1,
    prop_url_default_ports/1,
    prop_url_scheme_extraction/1,
    prop_url_host_extraction/1,
    prop_url_explicit_port/1
]).

%% Retry property tests
-export([
    prop_retry_delay_non_negative/1,
    prop_retry_delay_bounded/1,
    prop_retry_exhausted/1,
    prop_retry_normalize_opts/1,
    prop_retry_jitter_bounds/1
]).

%% Chaos property tests
-export([
    prop_chaos_failure_sequence/1
]).

all() ->
    [{group, url_properties}, {group, retry_properties}, {group, chaos_properties}].

groups() ->
    [
        {url_properties, [parallel], [
            prop_url_parse_valid_urls,
            prop_url_path_starts_with_slash,
            prop_url_default_ports,
            prop_url_scheme_extraction,
            prop_url_host_extraction,
            prop_url_explicit_port
        ]},
        {retry_properties, [parallel], [
            prop_retry_delay_non_negative,
            prop_retry_delay_bounded,
            prop_retry_exhausted,
            prop_retry_normalize_opts,
            prop_retry_jitter_bounds
        ]},
        {chaos_properties, [], [
            prop_chaos_failure_sequence
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(chaos_properties, Config) ->
    {ok, Server} = nhttpc_chaos_server:start(tcp),
    [{chaos_server, Server} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(chaos_properties, Config) ->
    nhttpc_chaos_server:stop(proplists:get_value(chaos_server, Config));
end_per_group(_Group, _Config) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% GENERATORS
%%%-----------------------------------------------------------------------------

%% Generate a valid scheme
gen_scheme() ->
    oneof([<<"http">>, <<"https">>]).

%% Generate a valid hostname (simplified)
gen_hostname() ->
    ?LET(
        Parts,
        non_empty(list(gen_hostname_part())),
        iolist_to_binary(lists:join(<<".">>, Parts))
    ).

gen_hostname_part() ->
    ?LET(
        Chars,
        non_empty(list(gen_hostname_char())),
        list_to_binary(Chars)
    ).

gen_hostname_char() ->
    oneof([
        choose($a, $z),
        choose($0, $9)
    ]).

%% Generate a valid port number
gen_port() ->
    choose(1, 65535).

%% Generate a valid path segment
gen_path_segment() ->
    ?LET(
        Chars,
        list(gen_path_char()),
        list_to_binary(Chars)
    ).

gen_path_char() ->
    oneof([
        choose($a, $z),
        choose($A, $Z),
        choose($0, $9),
        oneof([$-, $_, $., $~])
    ]).

%% Generate a path (may be empty)
gen_path() ->
    ?LET(
        Segments,
        list(gen_path_segment()),
        case Segments of
            [] -> <<>>;
            _ -> iolist_to_binary([<<"/">>, lists:join(<<"/">>, Segments)])
        end
    ).

%% Generate a query string (simplified, may be empty)
gen_query() ->
    oneof([
        <<>>,
        ?LET(
            {K, V},
            {gen_query_key(), gen_query_value()},
            <<K/binary, "=", V/binary>>
        )
    ]).

gen_query_key() ->
    ?LET(
        Chars,
        non_empty(list(choose($a, $z))),
        list_to_binary(Chars)
    ).

gen_query_value() ->
    ?LET(
        Chars,
        list(oneof([choose($a, $z), choose($0, $9)])),
        list_to_binary(Chars)
    ).

%% Generate a complete valid URL
gen_url() ->
    ?LET(
        {Scheme, Host, MaybePort, Path, Query},
        {gen_scheme(), gen_hostname(), oneof([default, gen_port()]), gen_path(), gen_query()},
        begin
            PortPart =
                case MaybePort of
                    default -> <<>>;
                    Port -> <<":", (integer_to_binary(Port))/binary>>
                end,
            QueryPart =
                case Query of
                    <<>> -> <<>>;
                    Q -> <<"?", Q/binary>>
                end,
            PathPart =
                case Path of
                    <<>> -> <<>>;
                    P -> P
                end,
            <<Scheme/binary, "://", Host/binary, PortPart/binary, PathPart/binary,
                QueryPart/binary>>
        end
    ).

%% Generate retry options
gen_retry_opts() ->
    ?LET(
        {MaxAttempts, BaseDelay, MaxDelay, Backoff, Jitter},
        {
            choose(1, 10),
            choose(10, 1000),
            choose(1000, 30000),
            oneof([exponential, linear, constant]),
            bool()
        },
        #{
            max_attempts => MaxAttempts,
            base_delay => BaseDelay,
            max_delay => max(BaseDelay, MaxDelay),
            backoff => Backoff,
            jitter => Jitter,
            retry_on => [timeout, connection_error]
        }
    ).

%% Generate attempt number
gen_attempt() ->
    choose(1, 20).

%%%-----------------------------------------------------------------------------
%%% URL PROPERTY TESTS
%%%-----------------------------------------------------------------------------

%% Property: Valid URLs should parse successfully
prop_url_parse_valid_urls(_Config) ->
    Prop = ?FORALL(
        Url,
        gen_url(),
        begin
            Result = nhttpc_url:parse_url(Url),
            case Result of
                {ok, UrlInfo} when is_map(UrlInfo) ->
                    maps:is_key(scheme, UrlInfo) andalso
                        maps:is_key(host, UrlInfo) andalso
                        maps:is_key(port, UrlInfo) andalso
                        maps:is_key(path, UrlInfo);
                _ ->
                    ct:log("URL parse failed: ~p -> ~p", [Url, Result]),
                    false
            end
        end
    ),
    run_property(Prop, 200).

%% Property: Parsed path always starts with "/"
prop_url_path_starts_with_slash(_Config) ->
    Prop = ?FORALL(
        Url,
        gen_url(),
        begin
            case nhttpc_url:parse_url(Url) of
                {ok, #{path := Path}} ->
                    case Path of
                        <<"/", _/binary>> ->
                            true;
                        _ ->
                            ct:log("Path doesn't start with /: ~p from ~p", [Path, Url]),
                            false
                    end;
                _ ->
                    % Invalid URLs don't count
                    true
            end
        end
    ),
    run_property(Prop, 200).

%% Property: Default port based on scheme
prop_url_default_ports(_Config) ->
    Prop = ?FORALL(
        {Scheme, Host},
        {gen_scheme(), gen_hostname()},
        begin
            Url = <<Scheme/binary, "://", Host/binary>>,
            case nhttpc_url:parse_url(Url) of
                {ok, #{scheme := ParsedScheme, port := Port}} ->
                    ExpectedPort =
                        case ParsedScheme of
                            http -> 80;
                            https -> 443
                        end,
                    Port =:= ExpectedPort;
                _ ->
                    true
            end
        end
    ),
    run_property(Prop, 100).

%% Property: Scheme is correctly extracted
prop_url_scheme_extraction(_Config) ->
    Prop = ?FORALL(
        {Scheme, Host},
        {gen_scheme(), gen_hostname()},
        begin
            Url = <<Scheme/binary, "://", Host/binary>>,
            ExpectedScheme =
                case Scheme of
                    <<"http">> -> http;
                    <<"https">> -> https
                end,
            case nhttpc_url:parse_url(Url) of
                {ok, #{scheme := ParsedScheme}} ->
                    ParsedScheme =:= ExpectedScheme;
                _ ->
                    true
            end
        end
    ),
    run_property(Prop, 100).

%% Property: Host is correctly extracted
prop_url_host_extraction(_Config) ->
    Prop = ?FORALL(
        {Scheme, Host},
        {gen_scheme(), gen_hostname()},
        begin
            Url = <<Scheme/binary, "://", Host/binary>>,
            case nhttpc_url:parse_url(Url) of
                {ok, #{host := ParsedHost}} ->
                    ParsedHost =:= Host;
                _ ->
                    true
            end
        end
    ),
    run_property(Prop, 100).

%% Property: Explicit port is correctly parsed
prop_url_explicit_port(_Config) ->
    Prop = ?FORALL(
        {Scheme, Host, Port},
        {gen_scheme(), gen_hostname(), gen_port()},
        begin
            Url = <<Scheme/binary, "://", Host/binary, ":", (integer_to_binary(Port))/binary>>,
            case nhttpc_url:parse_url(Url) of
                {ok, #{port := ParsedPort}} ->
                    ParsedPort =:= Port;
                _ ->
                    true
            end
        end
    ),
    run_property(Prop, 100).

%%%-----------------------------------------------------------------------------
%%% RETRY PROPERTY TESTS
%%%-----------------------------------------------------------------------------

%% Property: compute_delay always returns non-negative integer
prop_retry_delay_non_negative(_Config) ->
    Prop = ?FORALL(
        {Attempt, Opts},
        {gen_attempt(), gen_retry_opts()},
        begin
            Delay = nhttpc_retry:compute_delay(Attempt, Opts),
            is_integer(Delay) andalso Delay >= 0
        end
    ),
    run_property(Prop, 300).

%% Property: Delay is bounded by max_delay (allowing for jitter)
prop_retry_delay_bounded(_Config) ->
    Prop = ?FORALL(
        {Attempt, Opts},
        {gen_attempt(), gen_retry_opts()},
        begin
            #{max_delay := MaxDelay, jitter := Jitter} = Opts,
            Delay = nhttpc_retry:compute_delay(Attempt, Opts),
            %% With jitter, delay can be up to 25% more than max
            MaxAllowed =
                case Jitter of
                    true -> MaxDelay + (MaxDelay div 4) + 1;
                    false -> MaxDelay
                end,
            Delay =< MaxAllowed
        end
    ),
    run_property(Prop, 300).

%% Property: should_retry returns false when attempts exhausted
prop_retry_exhausted(_Config) ->
    Prop = ?FORALL(
        Opts,
        gen_retry_opts(),
        begin
            #{max_attempts := MaxAttempts} = Opts,
            %% At max attempts, should_retry returns false
            Result = nhttpc_retry:should_retry({error, timeout}, MaxAttempts, Opts),
            Result =:= false
        end
    ),
    run_property(Prop, 100).

%% Property: normalize_opts produces valid options
prop_retry_normalize_opts(_Config) ->
    Prop = ?FORALL(
        Input,
        oneof([
            false,
            choose(1, 10),
            gen_retry_opts()
        ]),
        begin
            Result = nhttpc_retry:normalize_opts(Input),
            case Result of
                false ->
                    true;
                Opts when is_map(Opts) ->
                    maps:is_key(max_attempts, Opts) andalso
                        maps:is_key(base_delay, Opts) andalso
                        maps:is_key(max_delay, Opts) andalso
                        maps:is_key(backoff, Opts) andalso
                        maps:is_key(jitter, Opts) andalso
                        maps:is_key(retry_on, Opts)
            end
        end
    ),
    run_property(Prop, 100).

%% Property: Jitter stays within expected bounds
prop_retry_jitter_bounds(_Config) ->
    Prop = ?FORALL(
        {BaseDelay, MaxDelay},
        {choose(100, 1000), choose(1000, 10000)},
        begin
            Opts = #{
                max_attempts => 3,
                base_delay => BaseDelay,
                max_delay => MaxDelay,
                backoff => constant,
                jitter => true,
                retry_on => [timeout]
            },
            %% With constant backoff, delay should be around base_delay +/- 25%
            Delays = [nhttpc_retry:compute_delay(1, Opts) || _ <- lists:seq(1, 50)],
            MinExpected = BaseDelay - (BaseDelay div 4) - 1,
            MaxExpected = BaseDelay + (BaseDelay div 4) + 1,
            lists:all(fun(D) -> D >= MinExpected andalso D =< MaxExpected end, Delays)
        end
    ),
    run_property(Prop, 50).

%% Property: For any sequence of served and dropped requests, each request
%% gets the outcome of its own step: a dropped one returns the
%% closed-connection error, and a served one returns its own response, also
%% right after a drop.
prop_chaos_failure_sequence(Config) ->
    Server = proplists:get_value(chaos_server, Config),
    Port = nhttpc_chaos_server:get_port(Server),
    Prop = ?FORALL(
        Steps,
        non_empty(list(oneof([serve, drop]))),
        run_failure_steps(Server, Port, 1, Steps)
    ),
    run_property(Prop, 50).

%%%-----------------------------------------------------------------------------
%%% HELPERS
%%%-----------------------------------------------------------------------------

%% Run a triq property and convert result to CT format
run_property(Prop, NumTests) ->
    case triq:check(Prop, NumTests) of
        true ->
            ok;
        false ->
            ct:fail(property_failed);
        {error, Reason} ->
            ct:fail({property_error, Reason})
    end.

-spec run_failure_steps(
    nhttpc_chaos_server:server_ref(), inet:port_number(), pos_integer(), [serve | drop]
) -> boolean().
run_failure_steps(_Server, _Port, _N, []) ->
    true;
run_failure_steps(Server, Port, N, [Step | Rest]) ->
    ok = nhttpc_chaos_server:set_failure_mode(Server, failure_mode(Step)),
    Path = <<"/chaos-", (integer_to_binary(N))/binary>>,
    Url = iolist_to_binary([<<"http://127.0.0.1:">>, integer_to_binary(Port), Path]),
    Result = nhttpc:get(Url, #{timeouts => #{request => 2000}}),
    case step_outcome_matches(Step, Path, Result) of
        true ->
            run_failure_steps(Server, Port, N + 1, Rest);
        false ->
            ct:log("step ~p (~p) on ~s returned ~p", [N, Step, Path, Result]),
            false
    end.

-spec failure_mode(serve | drop) -> nhttpc_chaos_server:failure_mode().
failure_mode(serve) -> none;
failure_mode(drop) -> close_before_response.

-spec step_outcome_matches(serve | drop, binary(), {ok, map()} | {error, term()}) -> boolean().
step_outcome_matches(serve, Path, {ok, #{status := 200, body := Body}}) ->
    string:prefix(Body, <<"OK: get ", Path/binary, "\n">>) =/= nomatch;
step_outcome_matches(drop, _Path, {error, {connect_error, closed}}) ->
    true;
step_outcome_matches(_Step, _Path, _Result) ->
    false.
