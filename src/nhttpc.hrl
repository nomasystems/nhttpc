%%% @doc Internal macros for nhttpc client.
%%%
%%% Public types live on the relevant module (`nhttpc`,
%%% `nhttpc_stream_recv`, `nhttpc_stream_send`, ...) via `-export_type`.
%%% This header retains only the default-value macros consumed by
%%% multiple implementation modules.
%%% @end
-ifndef(NHTTPC_HRL).
-define(NHTTPC_HRL, true).

%%%-----------------------------------------------------------------------------
%%% DEFAULT VALUES
%%%-----------------------------------------------------------------------------
%% Default max response size (100MB)
-define(DEFAULT_MAX_RESPONSE_SIZE, 104857600).

%% Default response header size budget (64 KiB) — RFC 9110 doesn't fix a
%% number, but 64 KiB is the common ceiling enforced by major HTTP/1.1
%% servers and proxies.
-define(DEFAULT_MAX_HEADER_SIZE, 65536).

%% Default response header count cap (100) — protects against header-bomb
%% style abuse without rejecting normal traffic.
-define(DEFAULT_MAX_HEADERS_COUNT, 100).

-endif.
