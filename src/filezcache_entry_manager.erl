%% @private
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

%%% @doc Manage all entries.

-module(filezcache_entry_manager).

-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").

-export([
        start_link/0,
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

-export([
    ensure_tables/0
    ]).

-record(filezcache_log_entry, {
        key,
        filename,
        size,
        checksum
    }).


-record(state, {
    % Monitors for cache write entries
    monitors,
    sizes,

    % Monitors for key entries referring to cache keys
    key2referrers,
    referrer2keys,

    iterator = start,
    recent :: list(),
    gc_candidate_pool = [] :: list(),
    bytes = 0 :: integer(),
    max_bytes :: integer() 
    }).


-define(TIMEOUT, infinity).

% Garbage collection setings
-define(GC_INTERVAL, 1000). 
-define(GC_MAX_BYTES, 10737418240).
-define(GC_POOL_SIZE, 100).
-define(GC_CHANCE_1_IN_N, 20).

% Every 10 minutes we empty the oldest table with recently used items
-define(RECENT_INTERVAL, 60000).


%% API

start_link() ->
    ok = ensure_tables(),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stats() ->
    gen_server:call(?MODULE, stats).

insert(Key, Opts) ->
    gen_server:call(?MODULE, {insert, Key, self(), Opts}, ?TIMEOUT).

lookup(Key) ->
    lookup(Key, undefined).

lookup(Key, MonitorPid) ->
    case mnesia:dirty_read(filezcache_log_entry, Key) of
        [] ->
            {error, enoent};
        [#filezcache_log_entry{size=undefined}] ->
            {error, enoent};
        [#filezcache_log_entry{size=Size, filename=Filename}] ->
            case filelib:is_regular(Filename) of
                true ->
                    log_access(Key, MonitorPid),
                    {ok, {file, Size, Filename}};
                false ->
                    delete(Key, Filename),
                    {error, enoent}
            end 
    end.

gc(Key) ->
    gen_server:cast(?MODULE, {gc, Key}).

log_access(Key) ->
    log_access(Key, undefined).

log_access(Key, MonitorPid) ->
    gen_server:cast(?MODULE, {log_access, Key, MonitorPid}).

log_ready(EntryPid, Key, Filename, Size, Checksum) ->
    gen_server:cast(?MODULE, {log_ready, EntryPid, Key, Filename, Size, Checksum}).


%% gen_server callbacks

init([]) ->
    filezcache_store:init(),
    gen_server:cast(self(), log_init),
    timer:send_after(?GC_INTERVAL, gc),
    timer:send_after(?RECENT_INTERVAL, recent_rotate),
    {ok, #state{
            sizes = ets:new(?MODULE, [set, private]),
            monitors = gb_trees:empty(),
            recent = [ 
                ets:new(filezcache_recent_1, [set]), 
                ets:new(filezcache_recent_2, [set]),
                ets:new(filezcache_recent_3, [set])
            ],
            key2referrers = dict:new(),
            referrer2keys = dict:new(),
            max_bytes = max_bytes()}}.

handle_call({insert, Key, WriterPid, Opts}, _From, State) ->
    case filezcache_store:lookup(Key) of
        {ok, Pid} ->
            {reply, {error, {already_started, Pid}}, State};
        {error, enoent} ->
            {ok, Pid} = filezcache_entry_sup:start_child(Key, WriterPid, Opts),
            filezcache_store:insert(Key, Pid),
            filezcache_event:insert(Key),
            Mon = erlang:monitor(process, Pid),
            State1 = State#state{monitors=gb_trees:enter(Mon, {Key, Pid}, State#state.monitors)},
            State2 = case proplists:get_value(monitor, Opts) of
                true -> 
                    maybe_add_key_referrer(Key, WriterPid, State1);
                _ ->
                    State1
            end,
            {reply, {ok, Pid}, State2}
    end;
handle_call(stats, _From, State) ->
    Stats = #{
        bytes => State#state.bytes,
        max_bytes => max_bytes(),
        processes => gb_trees:size(State#state.monitors),
        entries => mnesia:table_info(filezcache_log_entry, size),
        referrers => dict:size(State#state.referrer2keys),
        gc_candidate_pool => State#state.gc_candidate_pool
    },
    {reply, Stats, State}.

handle_cast({repop, Key, Filename, undefined, _Checksum}, State) ->
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            ok;
        {error, enoent} ->
            delete(Key, Filename)
    end,
    {noreply, State};
handle_cast({repop, Key, _Filename, Size, _Checksum}, State) ->
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            {noreply, State};
        {error, enoent} ->
            {noreply, State#state{bytes=State#state.bytes+Size}}
    end;
handle_cast({delete_if_inactive, Key, Filename}, State) ->
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            nop;
        {error, enoent} ->
            case is_recently_used(Key, State) orelse is_referred(Key, State) of
                true ->
                    nop;
                false ->
                    delete(Key, Filename)
            end
    end,
    {noreply, State};
handle_cast({gc, Key}, State) ->
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            {noreply, State};
        {error, enoent} ->
            case is_recently_used(Key, State) orelse is_referred(Key, State) of
                true ->
                    {noreply, State};
                false ->
                    {ok, Size} = delete(Key),
                    {noreply, State#state{bytes=State#state.bytes - Size}}
            end
    end;

handle_cast(log_init, State) ->
    proc_lib:spawn_link(fun() -> repopulate() end),
    {noreply, State};

%% Log a cache entry to the disk log
handle_cast({log_ready, Pid, Key, Filename, Size, Checksum}, #state{bytes=Bytes} = State) ->
    F = fun() ->
            mnesia:write(#filezcache_log_entry{key=Key, filename=Filename, size=Size, checksum=Checksum})
        end,
    mnesia:activity(transaction, F),
    filezcache_event:insert_ready(Key, Size, Filename),
    case Size of
        undefined -> ok;
        _ -> filezcache_entry:logged(Pid)
    end,
    {noreply, State#state{bytes=Bytes + case Size of undefined -> 0; _ -> Size end}};

%% Remove recently used keys from the eviction pool
handle_cast({log_access, Key, MonitorPid}, #state{gc_candidate_pool=Pool, recent=[Table|_]} = State) ->
    ets:insert(Table, {Key, os:timestamp()}),
    State1 = State#state{gc_candidate_pool=lists:delete(Key, Pool)},
    {noreply, maybe_add_key_referrer(Key, MonitorPid, State1)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, #state{monitors=Monitors, bytes=Bytes} = State) ->
    State1 = case gb_trees:lookup(MRef, State#state.monitors) of
        {value, {Key, Pid}} ->
            filezcache_store:delete(Key),
            Bytes1 = case is_logged(Key) of
                true ->
                    Bytes;
                false ->
                    {ok, FileSize} = delete(Key),
                    filezcache_event:delete(Key),
                    Bytes - FileSize
            end,
            State#state{monitors = gb_trees:delete(MRef, Monitors), bytes=Bytes1};
        none ->
            State
    end,
    State2 = case dict:find(Pid, State1#state.referrer2keys) of
        {ok, RefKeys} ->
            State1#state{
                referrer2keys = dict:erase(Pid, State#state.referrer2keys), 
                key2referrers = remove_referrer(Pid, RefKeys, State#state.key2referrers)
            };
        error -> 
            State1
    end,
    {noreply, State2};
handle_info(gc, State) ->
    State1 = do_gc(State),
    timer:send_after(?GC_INTERVAL, gc),
    {noreply, State1};
handle_info(recent_rotate, #state{recent=Tables} = State) ->
    timer:send_after(?RECENT_INTERVAL, recent_rotate),
    ets:delete_all_objects(lists:last(Tables)),
    Tables1 = [ lists:last(Tables) | lists:sublist(Tables, length(Tables)-1) ],
    {noreply, State#state{recent=Tables1}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @doc Check if the key is in the list of recently used entries
is_recently_used(Key, #state{recent=Tables}) ->
    lists:any(fun(Table) ->
                 ets:lookup(Table, Key) =/= []
              end,
              Tables).

is_logged(Key) ->
    F = fun() ->
            mnesia:read(filezcache_log_entry, Key)
        end,
    case mnesia:activity(transaction, F) of
        [] ->
            false;
        [#filezcache_log_entry{size=undefined}] ->
            false;
        [#filezcache_log_entry{}] ->
            true
    end.

%% @doc Add a monitor to a process that caches a key - this prevents garbage collection of the entry
maybe_add_key_referrer(_Key, undefined, State) ->
    State;
maybe_add_key_referrer(Key, Pid, State) ->
    case dict:find(Pid, State#state.referrer2keys) of
        {ok, RefKeys} ->
            case lists:member(Key, RefKeys) of
                true ->
                    State;
                false ->
                    do_add_key_referrer(Key, Pid, State)
            end;
        error ->
            do_add_key_referrer(Key, Pid, State)
    end.

do_add_key_referrer(Key, Pid, State) ->
    _ = erlang:monitor(process, Pid),
    State#state{
        key2referrers = dict:append(Key, Pid, State#state.key2referrers), 
        referrer2keys = dict:append(Pid, Key, State#state.referrer2keys)
    }.

is_referred(Key, State) ->
    dict:is_key(Key, State#state.key2referrers).

remove_referrer(_Pid, [], Key2Pids) ->
    Key2Pids;
remove_referrer(Pid, [Key|Keys], Key2Pids) ->
    case dict:find(Key, Key2Pids) of
        {ok, Pids} ->
            case lists:delete(Pid, Pids) of
                [] ->
                    remove_referrer(Pid, Keys, dict:erase(Key, Key2Pids));
                Pids1 ->
                    remove_referrer(Pid, Keys, dict:store(Key, Pids1, Key2Pids))
            end;
        error ->
            remove_referrer(Pid, Keys, Key2Pids)
    end. 

%% @doc Ensure that the proper filezcache_log table has been created
ensure_tables() ->
    TabDef = [
        {type, set},
        {record_name, filezcache_log_entry},
        {index, [#filezcache_log_entry.filename]},
        {attributes, record_info(fields, filezcache_log_entry)}
        | case application:get_env(mnesia, dir) of
             {ok, _} -> [ {disc_copies, [node()]} ];
             undefined -> []
          end
    ],
    case mnesia:create_table(filezcache_log_entry, TabDef) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, filezcache_log_entry}} -> ok
    end.


%% @doc Repopulates the cache using the log
repopulate() ->
    Keys = mnesia:dirty_all_keys(filezcache_log_entry), 
    error_logger:info_msg("filezcache: repopulating cache with ~p keys", [length(Keys)]),
    repopulate(Keys),
    error_logger:info_msg("filezcache: scanning cache directory for unknown files."),
    scan_cache().

repopulate([]) ->
    ok;
repopulate([Key|Keys]) ->
    F = fun() ->
            mnesia:read(filezcache_log_entry, Key)
        end,
    Rs = mnesia:activity(transaction, F),
    lists:foreach(fun repop_term/1, Rs),
    repopulate(Keys).

repop_term(#filezcache_log_entry{key=Key, filename=Filename, size=Size, checksum=Checksum}) ->
    case filezcache_store:lookup(Key) of
        {error, enoent} ->
            case file:read_file_info(Filename) of
                {ok, #file_info{type=regular, size=Size}} ->
                    gen_server:cast(?MODULE, {repop, Key, Filename, Size, Checksum});
                {ok, #file_info{type=regular, size=_OtherSize}} ->
                    gen_server:cast(?MODULE, {delete_if_inactive, Key});
                _Other ->
                    nop
            end;
        {ok, _Pid} ->
            nop
    end;
repop_term(_Term) ->
    ok.

%% @doc Scan the cache directories, remove all files not in the log
scan_cache() ->
    scan_dir(filezcache:data_dir()).

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


%% @doc Delete an entry by key
delete(Key) ->
    F = fun() ->
            mnesia:read(filezcache_log_entry, Key)
        end,
    case mnesia:activity(transaction, F) of
        [] -> 
            {ok, 0};
        [#filezcache_log_entry{key=Key,filename=Filename, size=Size}] ->
            ok = delete(Key,Filename),
            case Size of
                undefined -> {ok, 0};
                _ -> {ok, Size}
            end
    end.

%% @doc Delete an entry and its associated cache file.
delete(Key, Filename) ->
    F = fun() ->
            mnesia:delete({filezcache_log_entry, Key})
        end,
    mnesia:activity(transaction, F),
    _ = file:delete(Filename),
    ok.

%% @doc Find an entry by the cached file
find_by_filename(Path) ->
    F = fun() ->
            mnesia:index_read(filezcache_log_entry, Path, #filezcache_log_entry.filename)
        end,
    mnesia:activity(transaction, F).


%% @doc Perform a gc step, keep eviction pool with gc-candidates populated
do_gc(State) ->
    State1 = maybe_evict(State),
    fill_pool(State1, normal, 1).

maybe_evict(#state{gc_candidate_pool=[], bytes=Bytes} = State) ->
    case Bytes > max_bytes() of
        true -> do_gc(fill_pool(State, eager, 1));
        false -> State
    end;
maybe_evict(#state{gc_candidate_pool=Pool, bytes=Bytes} = State) ->
    case Bytes > max_bytes() of
        true -> State#state{gc_candidate_pool=random_evict(Pool)};
        false -> State
    end;
maybe_evict(State) ->
    State.


fill_pool(State, normal, N) when N >= 20 ->
    State;
fill_pool(State, eager, N) when N >= 100 ->
    State;
fill_pool(#state{gc_candidate_pool=Pool, iterator=Iterator} = State, Method, N) ->
    case length(Pool) < ?GC_POOL_SIZE of
        true ->
            {Candidates, Iterator1} = iterate(Iterator), 
            Pool1 = fill_pool_1(Pool, Candidates, Method),
            fill_pool(State#state{gc_candidate_pool=Pool1, iterator=Iterator1}, Method, N+1);
        false ->
            State
    end.

fill_pool_1(Pool, [], _Method) ->
    Pool;
fill_pool_1(Pool, [#filezcache_log_entry{key=Key, filename=Filename}|Cs], Method) ->
    case not lists:member(Key, Pool) andalso do_select(Method) of
        true ->
            case filelib:is_regular(Filename) of
                true ->
                    fill_pool_1([Key|Pool], Cs, Method);
                false ->
                    delete(Key),
                    fill_pool_1(Pool, Cs, Method)
            end;
        false ->
            fill_pool_1(Pool, Cs, Method)
    end.

do_select(eager) ->
    true;
do_select(normal) ->
    rand_uniform(?GC_CHANCE_1_IN_N) =:= 1.

random_evict([]) -> 
    [];
random_evict(Pool) ->
    Key = lists:nth(rand_uniform(length(Pool)), Pool),
    gc(Key),
    lists:delete(Key, Pool).

max_bytes() ->
    case application:get_env(filezcache, max_bytes) of
        undefined -> ?GC_MAX_BYTES;
        {ok, N} when is_integer(N) -> N
    end.


%% @doc Iterate over mnesia
iterate(start) ->
    iterate(0);
iterate(SlotNr) ->
    case get_slot(SlotNr) of
        '$end_of_table' -> 
            {[], start};
        Entries -> 
            {Entries, SlotNr+1}
    end.

get_slot(SlotNr) ->
    try 
        mnesia:dirty_slot(filezcache_log_entry, SlotNr)
    catch
        error:badarg -> '$end_of_table'
    end.

-spec rand_uniform( pos_integer() ) -> pos_integer().
rand_uniform(N) ->
    rand:uniform(N).
