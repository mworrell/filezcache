%% @author Marc Worrell
%% @copyright 2013-2014 Marc Worrell

%% Copyright 2013-2014 Marc Worrell
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(filezcache).

-export([
    insert/2, 
    insert/3, 
    insert_file/2,
    insert_file/3,
    insert_tmpfile/2,
    insert_tmpfile/3,
    insert_wait/1,
    insert_stream/1,
    insert_stream/3,
    insert_stream/4,
    append_stream/2,
    finish_stream/1,
    locate_monitor/1,
    access/1,
    lookup/1, 
    lookup_file/1, 
    lookup/2, 
    lookup_file/2, 
    delete/1,
    where/1,

    stats/0,

    data_dir/0,
    journal_dir/0,
    checksum/1
    ]).

-define(BLOCK_SIZE, 65536).


%%% API

insert(Key, Bin) ->
    insert(Key, Bin, []).

insert(Key, Bin, Opts) when is_binary(Bin) ->
    insert_1(Key, {data, Bin}, Opts).

insert_file(Key, FilePath) ->
    insert_file(Key, FilePath, []).

insert_file(Key, FilePath, Opts) ->
    insert_1(Key, {file, FilePath}, Opts).

insert_tmpfile(Key, FilePath) ->
    insert_tmpfile(Key, FilePath, []).

insert_tmpfile(Key, FilePath, Opts) ->
    insert_1(Key, {tmpfile, FilePath}, Opts).

insert_wait(Key) ->
    insert_1(Key, none, []).

insert_stream(Key) ->
    insert_stream(Key, undefined, []).

insert_stream(Key, FinalSize, Opts) ->
    insert_1(Key, {stream_start, self(), FinalSize}, Opts).

insert_stream(Key, FinalSize, StreamFun, Opts) when is_function(StreamFun, 1) ->
    insert_1(Key, {stream_fun, self(), FinalSize, StreamFun}, Opts).

append_stream(Pid, Bin) ->
    filezcache_entry:append_stream(Pid, Bin).

finish_stream(Pid) ->
    filezcache_entry:finish_stream(Pid).

-spec locate_monitor(term()) -> {ok, {file, integer(), string()}} | {ok, {pid, pid()}} | {error, term()}.
locate_monitor(Key) ->
    case filezcache_entry_manager:lookup(Key, self()) of
        {ok, _Found} = OK ->
            OK;
        {error, enoent} ->
            case where(Key) of
                undefined ->
                    {error, enoent};
                Pid ->
                    filezcache_entry_manager:log_access(Key, self()),
                    {ok, {pid, Pid}}
            end
    end.

-spec access(term()) -> ok.
access(Key) ->
    filezcache_entry_manager:log_access(Key). 

-spec lookup(term()) -> {ok, {file, integer(), string()}} | {ok, {device, pid()}} | {error, term()}.
lookup(Key) ->
    lookup(Key, []).

lookup(Pid, Opts) when is_pid(Pid) ->
    filezcache_entry:fetch(Pid, Opts);
lookup(Key, Opts) ->
    filezcache_event:lookup(Key),
    case filezcache_entry_manager:lookup(Key) of
        {ok, _Found} = OK ->
            OK;
        {error, enoent} ->
            case filezcache_store:lookup(Key) of
                {ok, Pid} ->
                    filezcache_entry:fetch(Pid, Opts);
                {error, _} = Error ->
                    Error
            end
    end.

-spec lookup_file(term()) -> {ok, {file, integer(), string()}} | {error, term()}.
lookup_file(Key) ->
    lookup_file(Key, []).

lookup_file(Pid, Opts) when is_pid(Pid) ->
    try
        filezcache_entry:fetch_file(Pid, Opts)
    catch
        exit:{noproc, _} ->
            {error, enoent}
    end;
lookup_file(Key, Opts) ->
    filezcache_event:lookup(Key),
    case filezcache_entry_manager:lookup(Key) of
        {ok, Found} ->
            Found;
        {error, enoent} ->
            case filezcache_store:lookup(Key) of
                {ok, Pid} ->
                    try
                        filezcache_entry:fetch_file(Pid, Opts)
                    catch
                        exit:{noproc, _} ->
                            {error, enoent}
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

-spec delete(term()) -> ok | {error, lockedlog_a}.
delete(Key) ->
    {ok,_} = filezcache_entry_manager:delete(Key),
    case filezcache_store:lookup(Key) of
        {ok, Pid} ->
            filezcache_entry:delete(Pid);
        {error, enoent} ->
            ok
    end.

-spec where(term()) -> pid() | undefined.
where(Key) ->
    case filezcache_store:lookup(Key) of
        {ok, Pid} -> Pid;
        {error, enoent} -> undefined
    end.

-spec stats() -> list().
stats() ->
    filezcache_entry_manager:stats().

%%% Support functions

insert_1(Key, DataSource, Opts) ->
    insert_or_error(filezcache_store:lookup(Key), Key, DataSource, Opts).

insert_or_error({ok, Pid}, _Key, _DataSource, _Opts) ->
    {error, {already_started, Pid}};
insert_or_error({error, enoent}, Key, DataSource, Opts) ->
    case filezcache_entry_manager:insert(Key, Opts) of
        {ok, Pid} ->
            case DataSource of
                none -> ok;
                _Data -> ok = filezcache_entry:store(Pid, DataSource)
            end,
            {ok, Pid};
        {error, _} = Error ->
            Error
    end.


%% @doc Return the directory for the storage of the cached files
-spec data_dir() -> file:filename().
data_dir() ->
    case application:get_env(filezcache, data_dir) of
        undefined -> filename:join([priv_dir(), "data"]);
        {ok, Dir} -> Dir
    end.

%% @doc Return the directory for the storage of the log/journal files
-spec journal_dir() -> file:filename().
journal_dir() ->
    case application:get_env(filezcache, journal_dir) of
        undefined -> filename:join([priv_dir(), "journal"]);
        {ok, Dir} -> Dir
    end.

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} -> "priv";
        PrivDir -> PrivDir
    end.

-spec checksum(file:filename()) -> binary().
checksum(Filename) ->
    Ctx = crypto:hash_init(sha),
    {ok, FD} = file:open(Filename, [read,binary]),
    Ctx1 = checksum1(Ctx, FD),
    file:close(FD),
    crypto:hash_final(Ctx1).

checksum1(Ctx, FD) ->
    case file:read(FD, ?BLOCK_SIZE) of
        eof ->
            Ctx;
        {ok, Data} ->
            checksum1(crypto:hash_update(Ctx, Data), FD)
    end.
