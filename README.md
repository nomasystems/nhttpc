# nhttpc

[![Hex.pm](https://img.shields.io/hexpm/v/nhttpc.svg)](https://hex.pm/packages/nhttpc)
[![CI](https://github.com/nomasystems/nhttpc/actions/workflows/ci.yml/badge.svg)](https://github.com/nomasystems/nhttpc/actions/workflows/ci.yml)

HTTP/1.1, HTTP/2, HTTP/3 and WebSocket client for Erlang/OTP 27+.

## Getting started

```erlang
%% rebar.config
{deps, [nhttpc]}.
```

### One-shot requests

```erlang
{ok, #{status := 200, body := Body}} =
    nhttpc:get(<<"https://api.example.com/users/42">>).

{ok, Resp} = nhttpc:post(
    <<"https://api.example.com/users">>,
    <<"{\"name\":\"alice\"}">>,
    #{headers => [{<<"content-type">>, <<"application/json">>}]}).
```

### Connection pools

```erlang
{ok, _} = nhttpc:start_link(api_pool, #{
    host => <<"api.example.com">>,
    port => 443,
    versions => [http2, http1_1]
}).

{ok, Resp} = nhttpc:get(<<"https://api.example.com/users">>,
                        #{via => {pool, api_pool}}).
```

### Async requests

Every async surface delivers `{nhttpc, Handle, Event}` to its owner.
`cancel/1` returns only after draining those events from the mailbox.

```erlang
{ok, Req} = nhttpc:url(get, <<"https://api.example.com/users/42">>),
{ok, H} = nhttpc:send(Req),
{ok, Resp} = nhttpc:await(H, 5_000).
```

## Features

- **HTTP/1.1** ([RFC 9112](https://www.rfc-editor.org/rfc/rfc9112)) and **HTTP/2** ([RFC 9113](https://www.rfc-editor.org/rfc/rfc9113)) with protocol selection via TLS ALPN
- **HTTP/3** over QUIC ([RFC 9114](https://www.rfc-editor.org/rfc/rfc9114))
- **WebSocket** ([RFC 6455](https://www.rfc-editor.org/rfc/rfc6455)) on the same handle protocol as async requests
- **Three dispatch modes**: one-shot, pooled, or on a caller-owned connection, selected by `via`
- **Connection pools** that pack HTTP/2 and HTTP/3 streams onto warm connections and grow on stream slots and send credit
- **Streaming** downloads and uploads with explicit consumer backpressure
- **Retries** with backoff, method-aware and off by default
- **Redirects** with a hop cap and credential stripping across origins
- **Cookie jar** ([RFC 6265](https://www.rfc-editor.org/rfc/rfc6265)) on ETS
- **Secure defaults**: TLS peer verification against the system CAs
- **Sealed error taxonomy**: every public spec returns `{error, nhttpc:error()}`
- **Telemetry** events and OpenTelemetry metrics (opt-in)

## Documentation

[nhttpc on HexDocs](https://hexdocs.pm/nhttpc)

## License

Apache License 2.0
