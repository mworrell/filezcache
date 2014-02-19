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
    insert_stream/1,
    insert_stream/2,
    append_stream/2,
    finish_stream/1,
    lookup/1, 
    lookup_file/1, 
    lookup/2, 
    lookup_file/2, 
    delete/1,

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

insert_stream(Key) ->
    insert_stream(Key, []).

insert_stream(Key, Opts) ->
    insert_1(Key, {stream_start, self()}, Opts).

append_stream(Pid, Bin) ->
    filezcache_entry:append_stream(Pid, Bin).

finish_stream(Pid) ->
    filezcache_entry:finish_stream(Pid).

-spec lookup(term()) -> {ok, {file, integer(), string()}} | {ok, {stream, function()}} | {error, term()}.
lookup(Key) ->
    lookup(Key, []).

lookup(Key, Opts) ->
    filezcache_event:lookup(Key),
    case filezcache_store:lookup(Key) of
        {ok, Pid} ->
            filezcache_entry:fetch(Pid, Opts);
        {error, _} = Error ->
            Error
    end.

-spec lookup_file(term()) -> {ok, {file, integer(), string()}} | {error, term()}.
lookup_file(Key) ->
    lookup_file(Key, []).

lookup_file(Key, Opts) ->
    filezcache_event:lookup(Key),
    case filezcache_store:lookup(Key) of
        {ok, Pid} ->
            try
                filezcache_entry:fetch_file(Pid, Opts)
            catch
                exit:{noproc, _} ->
                    {error, not_found}
            end;
        {error, _} = Error ->
            Error
    end.

-spec delete(term()) -> ok | {error, locked|not_found}.
delete(Key) ->
    case filezcache_store:lookup(Key) of
        {ok, Pid} ->
            filezcache_entry:delete(Pid);
        {error, _Reason} = Error ->
            Error
    end.

stats() ->
    filezcache_entry_manager:stats().

%%% Support functions

insert_1(Key, What, Opts) ->
    insert_or_error(filezcache_store:lookup(Key), Key, What, Opts).

insert_or_error({ok, Pid}, _Key, _What, _Opts) ->
    {error, {already_started, Pid}};
insert_or_error({error, not_found}, Key, What, Opts) ->
    case filezcache_entry_manager:insert(Key, Opts) of
        {ok, Pid} ->
            filezcache_entry:store(Pid, What),
            {ok, Pid};
        {error, _} = Error ->
            Error
    end.


%% @doc Return the directory for the storage of the cached files
-spec data_dir() -> file:filename().
data_dir() ->
    case application:get_env(data_dir) of
        undefined -> filename:join([priv_dir(), "data"]);
        DataDir -> DataDir
    end.

%% @doc Return the directory for the storage of the log/journal files
-spec journal_dir() -> file:filename().
journal_dir() ->
    case application:get_env(journal_dir) of
        undefined -> filename:join([priv_dir(), "journal"]);
        DataDir -> DataDir
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
