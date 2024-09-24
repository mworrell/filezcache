%% @private
%% @author Marc Worrell
%% @copyright 2013-2024 Marc Worrell
%% @doc Manage all files in the filezcache directory. Handle insertion,
%% lookup, deletion and streaming files.
%% @end

%% Copyright 2013-2024 Marc Worrell
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

-module(filezcache_entry_manager).

-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
    start_link/0,
    record_stat/1,
    insert/2,
    lookup/1,
    lookup/2,
    delete/1,
    gc/1,
    stats/0,
    log_access/1,
    log_access/2,
    log_ready/5
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


-define(TIMEOUT, infinity).

% Max cache size and garbage collection setings
-define(GC_MAX_BYTES, 10_737_418_240). % 10G
-define(GC_INTERVAL, 1000).

-define(GC_BATCH_PERIODIC, 500).
-define(GC_BATCH_INSERT, 100).

-define(TICK_INTERVAL, 1000).
-define(TICKS_RECENT, 1).

% Periodically write the log to disk (10 min)
-define(WRITE_LOG_INTERVAL, 600_000).

% Names of ETS tables
-define(FILE_ENTRY_TAB, filezcache_entries_tab).
-define(FILENAME_TAB, filezcache_filename_tab).

% Name of the stored log file in the journal dir
-define(LOG_FILENAME, "filezcache-entries.dat").

-record(filezcache_entry, {
    key :: term(),
    filename :: binary(),
    size = undefined :: undefined | non_neg_integer(),
    checksum = undefined :: undefined | binary(),
    lru_tick = 0 :: non_neg_integer()
}).

-record(state, {
    % Monitors for cache write entries
    monitors = #{} :: map(),

    % Monitors for key entry processes referring to cache keys
    key2referrers = #{} :: map(),
    referrer2keys = #{} :: map(),

    lru :: filezcache_lru:lru(),
    tick = ?TICKS_RECENT :: non_neg_integer(),
    bytes = 0 :: non_neg_integer(),

    insert_count = 0 :: non_neg_integer(),
    delete_count = 0 :: non_neg_integer(),
    hit_count = 0 :: non_neg_integer(),
    miss_count = 0  :: non_neg_integer(),
    evict_count = 0 :: non_neg_integer()
}).


%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stats() ->
    gen_server:call(?MODULE, stats).

insert(Key, Opts) ->
    gen_server:call(?MODULE, {insert, Key, self(), Opts}, ?TIMEOUT).

lookup(Key) ->
    lookup(Key, undefined).

lookup(Key, MonitorPid) ->
    case ets:lookup(?FILE_ENTRY_TAB, Key) of
        [] ->
            {error, enoent};
        [ #filezcache_entry{ size = undefined } ] ->
            {error, enoent};
        [ #filezcache_entry{ size = Size, filename = Filename } ] ->
            log_access(Key, MonitorPid),
            case filelib:is_regular(Filename) of
                true ->
                    {ok, {file, Size, Filename}};
                false ->
                    delete_file(Key, Filename),
                    {error, enoent}
            end
    end.

%% @doc Delete a key if it is inactive, ie. not being used by another process.
delete(Key) ->
    gen_server:call(?MODULE, {delete, Key}).

-spec record_stat(What) -> ok when
    What :: hit | miss | evict | insert.
record_stat(What) ->
    gen_server:cast(?MODULE, {stat, What}).

gc(Key) ->
    gen_server:cast(?MODULE, {gc, Key}).

log_access(Key) ->
    log_access(Key, undefined).

log_access(Key, MonitorPid) ->
    gen_server:cast(?MODULE, {log_access, Key, MonitorPid}).

%% @doc Called by the filezcache_entry, with undefined Size when starting and a defined
%% size when the file is completely written to the cache.
log_ready(EntryPid, Key, Filename, Size, Checksum) ->
    gen_server:cast(?MODULE, {log_ready, EntryPid, Key, Filename, Size, Checksum}).


%% gen_server callbacks

init([]) ->
    ok = ensure_tables(),
    filezcache_store:init(),
    gen_server:cast(self(), log_init),
    timer:send_after(?TICK_INTERVAL, tick),
    timer:send_after(?GC_INTERVAL, gc),
    timer:send_after(?WRITE_LOG_INTERVAL, write_log),
    {ok, #state{
        lru = filezcache_lru:new()
    }}.

handle_call({insert, Key, WriterPid, Opts}, _From, #state{ monitors = Monitors } = State) ->
    case filezcache_store:lookup(Key) of
        {ok, Pid} ->
            {reply, {error, {already_started, Pid}}, State};
        {error, enoent} ->
            {ok, Pid} = filezcache_entry_sup:start_child(Key, WriterPid, Opts),
            filezcache_store:insert(Key, Pid),
            filezcache_event:insert(Key),
            Mon = erlang:monitor(process, Pid),
            State1 = State#state{
                monitors = Monitors#{ Mon => {Key, Pid} }
            },
            State2 = case proplists:get_value(monitor, Opts) of
                true ->
                    maybe_add_key_referrer(Key, WriterPid, State1);
                _ ->
                    State1
            end,
            {reply, {ok, Pid}, State2}
    end;
handle_call({delete, Key}, _From, State) ->
    % Delete a key if it isn't "hot" and is not referred to by any processes.
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            {reply, {error, writing}, State};
        {error, enoent} ->
            case is_referred(Key, State) of
                true ->
                    {reply, {error, locked}, State};
                false ->
                    State1 = delete_key(Key, State),
                    {reply, ok, State1}
            end
    end;
handle_call(stats, _From, State) ->
    Stats = #{
        bytes => State#state.bytes,
        max_bytes => max_bytes(),
        processes => maps:size(State#state.monitors),
        entries => ets:info(?FILE_ENTRY_TAB, size),
        referrers => maps:size(State#state.referrer2keys),
        insert_count => State#state.insert_count,
        delete_count => State#state.delete_count,
        hit_count => State#state.hit_count,
        miss_count => State#state.miss_count,
        evict_count => State#state.evict_count
    },
    {reply, Stats, State}.

handle_cast({repop, Key, Filename, undefined, _Checksum}, State) ->
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            % Some process is inserting this entry - ignore.
            ok;
        {error, enoent} ->
            % Incomplete entry - delete
            delete_file(Key, Filename)
    end,
    {noreply, State};
handle_cast({repop, Key, _Filename, Size, _Checksum}, #state{ lru = LRU } = State) ->
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            % Still writing
            {noreply, State};
        {error, enoent} ->
            % Present - register in LRU
            LRU1 = filezcache_lru:push(Key, State#state.tick, LRU),
            {noreply, State#state{
                bytes = State#state.bytes + Size,
                lru = LRU1,
                insert_count = State#state.insert_count + 1
            }}
    end;
handle_cast({delete_if_inactive, Key}, State) ->
    % Delete a key if it isn't "hot" and is not referred to by any processes.
    State1 = case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            State;
        {error, enoent} ->
            case is_recently_used(Key, State) orelse is_referred(Key, State) of
                true ->
                    State;
                false ->
                    delete_key(Key, State)
            end
    end,
    {noreply, State1};
handle_cast({gc, Key}, State) ->
    State1 = case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            State;
        {error, enoent} ->
            case is_recently_used(Key, State) orelse is_referred(Key, State) of
                true ->
                    State;
                false ->
                    delete_key(Key, State)
            end
    end,
    {noreply, State1};

handle_cast(log_init, State) ->
    proc_lib:spawn_link(fun() -> repopulate() end),
    {noreply, State};

handle_cast({log_ready, Pid, Key, Filename, Size, Checksum}, #state{ bytes = Bytes, lru = LRU } = State) ->
    % File is succesfully (being) inserted into the cache.
    Entry = #filezcache_entry{
        key = Key,
        filename = Filename,
        size = Size,
        checksum = Checksum,
        lru_tick = State#state.tick
    },
    ets:insert(?FILE_ENTRY_TAB, Entry),
    ets:insert(?FILENAME_TAB, {Filename, Key}),
    filezcache_event:insert_ready(Key, Size, Filename),
    State1 = case Size of
        undefined ->
            State;
        _ ->
            % Close the entry process - the file is safely recorded in the ets table.
            filezcache_entry:logged(Pid),
            State#state{ bytes = Bytes + Size }
    end,
    LRU1 = filezcache_lru:push(Key, State#state.tick, LRU),
    State2 = State1#state{
        lru = LRU1,
        insert_count = State#state.insert_count + 1
    },
    State3 = do_gc(State2, ?GC_BATCH_INSERT),
    {noreply, State3};

handle_cast({log_access, Key, MonitorPid}, #state{ lru = LRU } = State) ->
    % Log recent access to this key, and optionally add the Pid to the list of processes
    % handling this key.
    LRU1 = filezcache_lru:push(Key, State#state.tick, LRU),
    State1 = State#state{
        lru = LRU1,
        hit_count = State#state.hit_count + 1
    },
    {noreply, maybe_add_key_referrer(Key, MonitorPid, State1)};

handle_cast({stat, What}, State) ->
    State1 = case What of
        insert -> State#state{ insert_count = State#state.insert_count + 1 };
        delete -> State#state{ delete_count = State#state.delete_count + 1 };
        hit -> State#state{ hit_count = State#state.hit_count + 1 };
        miss -> State#state{ miss_count = State#state.miss_count + 1 };
        evict -> State#state{ evict_count = State#state.evict_count + 1 }
    end,
    {noreply, State1};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, #state{ monitors = Monitors } = State) ->
    State2 = case maps:get(MRef, Monitors, error) of
        {Key, MPid} when MPid =:= Pid ->
            filezcache_store:delete(Key),
            State1 = case is_logged(Key) of
                true ->
                    State;
                false ->
                    S1 = delete_key(Key, State),
                    filezcache_event:delete(Key),
                    S1
            end,
            State1#state{
                monitors = maps:remove(MRef, Monitors)
            };
        error ->
            State
    end,
    State3 = case maps:get(Pid, State2#state.referrer2keys, error) of
        error ->
            State2;
        RefKeys ->
            State2#state{
                referrer2keys = maps:remove(Pid, State2#state.referrer2keys),
                key2referrers = remove_referrer(Pid, RefKeys, State2#state.key2referrers)
            }
    end,
    {noreply, State3};
handle_info(gc, State) ->
    State1 = do_gc(State),
    timer:send_after(?GC_INTERVAL, gc),
    {noreply, State1};
handle_info(tick, #state{ tick = Tick } = State) ->
    timer:send_after(?TICK_INTERVAL, tick),
    {noreply, State#state{ tick = Tick + 1 }};
handle_info(write_log, #state{} = State) ->
    write_log(),
    timer:send_after(?WRITE_LOG_INTERVAL, write_log),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @doc Check if the key is in the list of recently used entries
is_recently_used(Key, #state{ tick = Tick }) ->
    case ets:lookup(?FILE_ENTRY_TAB, Key) of
        [ #filezcache_entry{ lru_tick = LRUTick } ] ->
            LRUTick >= Tick - ?TICKS_RECENT;
        [] ->
            false
    end.

is_logged(Key) ->
    case ets:lookup(?FILE_ENTRY_TAB, Key) of
        [] ->
            false;
        [ #filezcache_entry{ size = undefined } ] ->
            false;
        [ #filezcache_entry{} ] ->
            true
    end.

%% @doc Add a monitor to a process that caches a key - this prevents garbage collection of the entry
maybe_add_key_referrer(_Key, undefined, State) ->
    State;
maybe_add_key_referrer(Key, Pid, State) ->
    case maps:get(Pid, State#state.referrer2keys, error) of
        error ->
            do_add_key_referrer(Key, Pid, State);
        RefKeys ->
            case lists:member(Key, RefKeys) of
                true ->
                    State;
                false ->
                    do_add_key_referrer(Key, Pid, State)
            end
    end.

do_add_key_referrer(Key, Pid, #state{ key2referrers = KeyRef, referrer2keys = RefKey } = State) ->
    _ = erlang:monitor(process, Pid),
    State#state{
        key2referrers = KeyRef#{ Key => [ Pid | maps:get(Key, KeyRef, []) ] },
        referrer2keys = RefKey#{ Pid => [ Key | maps:get(Pid, RefKey, []) ] }
    }.

is_referred(Key, State) ->
    maps:is_key(Key, State#state.key2referrers).

remove_referrer(_Pid, [], Key2Pids) ->
    Key2Pids;
remove_referrer(Pid, [Key|Keys], Key2Pids) ->
    case maps:get(Key, Key2Pids, error) of
        error ->
            remove_referrer(Pid, Keys, Key2Pids);
        Pids ->
            case lists:delete(Pid, Pids) of
                [] ->
                    remove_referrer(Pid, Keys, maps:remove(Key, Key2Pids));
                Pids1 ->
                    remove_referrer(Pid, Keys, Key2Pids#{ Key =>  Pids1 })
            end
    end.

%% @doc Ensure that the proper filezcache_log and LRU tables have been created.
ensure_tables() ->
    % Key to filename, #filezcache_entry{}
    ets:new(?FILE_ENTRY_TAB, [
            set,
            protected,
            named_table,
            {keypos, #filezcache_entry.key}
        ]),
    % Filename to key {filename, key}
    ets:new(?FILENAME_TAB, [
            set,
            protected,
            named_table,
            {keypos, 1}
        ]),
    ok.

%% @doc Repopulates the cache using the log
repopulate() ->
    % Read the FILE_ENTRY_TAB from disk
    SavedEntries = read_log(),
    lists:foreach(
        fun
            (#filezcache_entry{ size = undefined, filename = Filename }) ->
                file:delete(Filename);
            (E) ->
                ets:insert(?FILE_ENTRY_TAB, E)
        end,
        SavedEntries),
    % Check the file entry tab with the journal dir
    Entries = ets:tab2list(?FILE_ENTRY_TAB),
    ?LOG_INFO(#{
        in => filezcache,
        text => <<"Populating filezcache with existing keys from the log">>,
        result => ok,
        count => length(Entries)
    }),
    repopulate(Entries),
    scan_cache().

repopulate([]) ->
    ok;
repopulate([ #filezcache_entry{ key = Key, filename = Filename, size = Size, checksum = Checksum } | Entries ]) ->
    ets:insert(?FILENAME_TAB, {Filename, Key}),
    case filezcache_store:lookup(Key) of
        {error, enoent} ->
            case file:read_file_info(Filename) of
                {ok, #file_info{ type = regular, size = Size }} ->
                    gen_server:cast(?MODULE, {repop, Key, Filename, Size, Checksum});
                {ok, #file_info{ type = regular, size = _OtherSize }} ->
                    gen_server:cast(?MODULE, {delete_if_inactive, Key});
                _Other ->
                    gen_server:cast(?MODULE, {delete_if_inactive, Key})
            end;
        {ok, _Pid} ->
            nop
    end,
    repopulate(Entries).

%% @doc Scan the cache directories, remove all files not in the log
scan_cache() ->
    Dir = filezcache:data_dir(),
    ?LOG_INFO(#{
        in => filezcache,
        text => <<"Removing unregistered files from the filezcache directory">>,
        result => ok,
        what => remove_unregistered,
        directory => iolist_to_binary(Dir)
    }),
    scan_dir(Dir).

scan_dir(Dir) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            scan_files(Dir, Files);
        {error, _} ->
            ok
    end.

scan_files(_Dir, []) ->
    ok;
scan_files(Dir, ["."++_|Fs]) ->
    scan_files(Dir, Fs);
scan_files(Dir, [F|Fs]) ->
    Path = filename:join(Dir, F),
    case filelib:is_regular(Path) of
        true ->
            case find_by_filename(Path) of
                [] -> file:delete(Path);
                [_|_] -> ok
            end;
        false ->
            scan_dir(Path)
    end,
    scan_files(Dir, Fs).


%% @doc Delete an entry by key.
delete_key(Key, #state{ lru = LRU } = State) ->
    {_, LRU1} = filezcache_lru:take(Key, LRU),
    Delta = case ets:lookup(?FILE_ENTRY_TAB, Key) of
        [] ->
            0;
        [ #filezcache_entry{ filename = Filename, size = Size } ] ->
            ok = delete_file(Key, Filename),
            case Size of
                undefined -> 0;
                _ -> Size
            end
    end,
    State#state{
        bytes = State#state.bytes - Delta,
        lru = LRU1,
        delete_count = State#state.delete_count + 1
    }.

%% @doc Delete an entry and its associated cache file.
delete_file(Key, Filename) ->
    _ = file:delete(Filename),
    ets:delete(?FILENAME_TAB, Filename),
    ets:delete(?FILE_ENTRY_TAB, Key),
    ok.

%% @doc Find an entry by the cached file
find_by_filename(Path) when is_list(Path) ->
    find_by_filename(unicode:characters_to_binary(Path, utf8));
find_by_filename(Path) when is_binary(Path) ->
    Ks = ets:lookup(?FILENAME_TAB, Path),
    L = lists:map(
        fun({_Filename, K}) ->
            ets:lookup(?FILE_ENTRY_TAB, K)
        end,
        Ks),
    lists:flatten(L).


%% @doc Perform a gc step, drop least recently used entries till we get to the
%% desired cache size. We delete max GC_BATCH files per garbage collect step.
do_gc(State) ->
    do_gc(State, ?GC_BATCH_PERIODIC).

do_gc(State, 0) ->
    State;
do_gc(#state{ bytes = Bytes, lru = LRU } = State, N) ->
    Max = max_bytes(),
    if
        Max >= Bytes ->
            State;
        true ->
            case filezcache_lru:empty(LRU) of
                true ->
                    State;
                false ->
                    State1 = drop_oldest_key(State),
                    do_gc(State1, N - 1)
            end
    end.

drop_oldest_key(#state{ lru = LRU } = State) ->
    {Key, _Value, LRU1} = filezcache_lru:pop(LRU),
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            LRU2 = filezcache_lru:push(Key, State#state.tick, LRU1),
            State#state{ lru = LRU2 };
        {error, enoent} ->
            case is_referred(Key, State) of
                true ->
                    LRU2 = filezcache_lru:push(Key, State#state.tick, LRU1),
                    State#state{ lru = LRU2 };
                false ->
                    Delta = case ets:lookup(?FILE_ENTRY_TAB, Key) of
                        [] ->
                            0;
                        [ #filezcache_entry{ filename = Filename, size = Size } ] ->
                            ok = delete_file(Key, Filename),
                            case Size of
                                undefined -> 0;
                                _ -> Size
                            end
                    end,
                    State#state{
                        bytes = State#state.bytes - Delta,
                        lru = LRU1,
                        evict_count = State#state.evict_count + 1
                    }
            end
    end.


%% @doc Return the max cache size from the application config.
max_bytes() ->
    case application:get_env(filezcache, max_bytes) of
        undefined -> ?GC_MAX_BYTES;
        {ok, N} when is_integer(N) -> N
    end.

%% @doc Read the saved log from disk.
read_log() ->
    case file:read_file(filename_log()) of
        {ok, Data} ->
            try
                Es = binary_to_term(Data),
                lists:filtermap(
                    fun
                        (#filezcache_entry{} = E) ->
                            {true, E#filezcache_entry{ lru_tick = 0 }};
                        (_) ->
                            false
                    end,
                    Es)
            catch _:_ ->
                []
            end;
        {error, _} ->
            []
    end.

%% @doc Save the log to disk
write_log() ->
    Es = ets:tab2list(?FILE_ENTRY_TAB),
    TmpFilename = iolist_to_binary([ filename_log(), ".tmp" ]),
    case file:write_file(TmpFilename, erlang:term_to_binary(Es, [ compressed ])) of
        ok ->
            case file:rename(TmpFilename, filename_log()) of
                ok -> ok;
                {error, _} = Error ->
                    file:delete(TmpFilename),
                    Error
            end;
        {error, _} = Error ->
            file:delete(TmpFilename),
            Error
    end.

filename_log() ->
    filename:join([filezcache:journal_dir(), ?LOG_FILENAME]).
