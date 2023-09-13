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

%% @doc Insert binary value.
%% @equiv insert(Key, Bin, [])

-spec insert(Key, Bin) -> Result when
	Key :: term(),
	Bin :: binary(),
	Result :: {ok, Pid} | {error, Reason},
	Pid :: pid(),
	Reason :: term().
insert(Key, Bin) ->
    insert(Key, Bin, []).

%% @doc Insert binary value and options.

-spec insert(Key, Bin, Opts) -> Result when
	Key :: term(),
	Bin :: binary(),
	Opts :: list(),
	Result :: {ok, Pid} | {error, Reason},
	Pid :: pid(),
	Reason :: term().
insert(Key, Bin, Opts) when is_binary(Bin) ->
    insert_1(Key, {data, Bin}, Opts).

%% @doc Insert file.
%% @equiv insert_file(Key, FilePath, [])

-spec insert_file(Key, FilePath) -> Result when
	Key :: term(),
	FilePath :: file:filename_all(),
	Result :: {ok, Pid} | {error, Reason},
    Pid :: pid(),
    Reason :: term().
insert_file(Key, FilePath) ->
    insert_file(Key, FilePath, []).

%% @doc Insert file and options.

-spec insert_file(Key, FilePath, Opts) -> Result when
	Key :: term(),
	FilePath :: file:filename_all(),
	Opts :: list(),
	Result :: {ok, Pid} | {error, Reason},
    Pid :: pid(),
    Reason :: term().
insert_file(Key, FilePath, Opts) ->
    insert_1(Key, {file, FilePath}, Opts).

%% @doc Insert temporary file.
%% @equiv insert_tmpfile(Key, FilePath, [])

-spec insert_tmpfile(Key, FilePath) -> Result when
	Key :: term(),
	FilePath :: file:filename_all(),
	Result :: {ok, Pid} | {error, Reason},
    Pid :: pid(),
    Reason :: term().
insert_tmpfile(Key, FilePath) ->
    insert_tmpfile(Key, FilePath, []).

%% @doc Insert temporary file and options.

-spec insert_tmpfile(Key, FilePath, Opts) -> Result when
	Key :: term(),
	FilePath :: file:filename_all(),
	Opts :: list(),
	Result :: {ok, Pid} | {error, Reason},
    Pid :: pid(),
    Reason :: term().
insert_tmpfile(Key, FilePath, Opts) ->
    insert_1(Key, {tmpfile, FilePath}, Opts).

insert_wait(Key) ->
    insert_1(Key, none, []).

%% @doc Insert stream.
%% @equiv insert_stream(Key, undefined, [])

-spec insert_stream(Key) -> Result when
	Key :: term(),
	Result :: {ok, Pid} | {error, Reason},
	Pid :: pid(),
	Reason :: term().
insert_stream(Key) ->
    insert_stream(Key, undefined, []).

%% @doc Insert stream.

-spec insert_stream(Key, FinalSize, Opts) -> Result when
	Key :: term(),
	FinalSize :: non_neg_integer() | undefined,
	Opts :: list(),
	Result :: {ok, Pid} | {error, Reason},
	Pid :: pid(),
	Reason :: term().
insert_stream(Key, FinalSize, Opts) ->
    insert_1(Key, {stream_start, self(), FinalSize}, Opts).

%% @doc Insert stream.

-spec insert_stream(Key, FinalSize, StreamFun, Opts) -> Result when
	Key :: term(),
	FinalSize :: non_neg_integer() | undefined,
    StreamFun :: function(),
	Opts :: list(),
	Result :: {ok, Pid} | {error, Reason},
	Pid :: pid(),
	Reason :: term().
insert_stream(Key, FinalSize, StreamFun, Opts) when is_function(StreamFun, 1) ->
    insert_1(Key, {stream_fun, self(), FinalSize, StreamFun}, Opts).

%% @doc Append to existed stream.

-spec append_stream(Pid, Bin) -> Result when
	Pid :: pid(),
	Bin :: binary(),
	Result :: ok.
append_stream(Pid, Bin) ->
    filezcache_entry:append_stream(Pid, Bin).

%% @doc Finish existed stream.

-spec finish_stream(Pid) -> Result when
	Pid :: pid(),
	Result :: ok.
finish_stream(Pid) ->
    filezcache_entry:finish_stream(Pid).

%% @doc Try to find monitor by Key.

-spec locate_monitor(Key) -> Result when
	Key :: term(),
	Result :: {ok, {file, FileSize, Filename}}
            | {ok, {pid, pid()}}
            | {error, term()},
    FileSize :: non_neg_integer(),
    Filename :: file:filename().
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

%% @doc Save access time as a information in the datastore.

-spec access(Key) -> Result when
	Key :: term(),
	Result :: ok.
access(Key) ->
    filezcache_entry_manager:log_access(Key).

%% @doc Lookup datastore element by Key.

-spec lookup(Key) -> Result when
	Key :: term(),
	Result :: {ok, {file, FileSize, Filename}}
            | {ok, {device, Pid}}
            | {error, term()},
    FileSize :: non_neg_integer(),
    Filename :: file:filename_all(),
    Pid :: pid().
lookup(Key) ->
    lookup(Key, []).

%% @doc Lookup datastore element by Pid or Key and options.

-spec lookup(PidOrKey, Opts) -> Result when
	PidOrKey :: pid() | term(),
	Opts :: list(),
	Result :: term().
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

%% @doc Lookup File by Key.

-spec lookup_file(Key) -> Result when
	Key :: term(),
	Result :: {ok, {file, FileSize, Filename}} | {error, term()},
    FileSize :: non_neg_integer(),
    Filename :: file:filename_all().
lookup_file(Key) ->
    lookup_file(Key, []).

%% @doc Lookup Pid and options.

-spec lookup_file(PidOrKey, Opts) -> Result when
	PidOrKey :: pid() | term(),
	Opts :: list(),
	Result :: term() | {error, enoent}.
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

%% @doc Delete data item for the storage.

-spec delete(Key) -> Result when
	Key :: term(),
	Result :: ok | {error, lockedlog_a}.
delete(Key) ->
    {ok,_} = filezcache_entry_manager:delete(Key),
    case filezcache_store:lookup(Key) of
        {ok, Pid} ->
            filezcache_entry:delete(Pid);
        {error, enoent} ->
            ok
    end.

%% @doc Check existence of Key in the data storage.

-spec where(Key) -> Result when
	Key :: term(),
	Result :: Pid | undefined,
	Pid :: pid().
where(Key) ->
    case filezcache_store:lookup(Key) of
        {ok, Pid} -> Pid;
        {error, enoent} -> undefined
    end.

%% @doc Return data storade information.

-spec stats() -> Result when
	Result :: [tuple()].
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


%% @doc Return the directory for the storage of the cached files.

-spec data_dir() -> Result when
	Result :: file:filename().
data_dir() ->
    case application:get_env(filezcache, data_dir) of
        undefined -> filename:join([priv_dir(), "data"]);
        {ok, Dir} -> Dir
    end.

%% @doc Return the directory for the storage of the `log/journal' files.

-spec journal_dir() -> Result when
	Result :: file:filename().
journal_dir() ->
    case application:get_env(filezcache, journal_dir) of
        undefined -> filename:join([priv_dir(), "journal"]);
        {ok, Dir} -> Dir
    end.

%% @doc Return priv directory location.

-spec priv_dir() -> Result when
	Result :: file:filename() | list().
priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} -> "priv";
        PrivDir -> PrivDir
    end.

%% @doc Return checksum.

-spec checksum(Filename) -> Result when
	Filename :: file:filename(),
	Result :: binary().
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
