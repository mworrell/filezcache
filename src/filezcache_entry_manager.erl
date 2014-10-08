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
        stats/0,
        log_access/1,
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
    monitors,
    sizes,
    iterator = start,
    gc_candidate_pool = [] :: list(pid()),
    bytes = 0 :: integer(),
    max_bytes :: integer() 
    }).


-define(TIMEOUT, infinity).

% Garbage collection setings
-define(GC_INTERVAL, 1000). 
-define(GC_MAX_BYTES, 10737418240).
-define(GC_POOL_SIZE, 100).
-define(GC_CHANCE_1_IN_N, 20).

%% API

start_link() ->
    ok = ensure_tables(),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stats() ->
    gen_server:call(?MODULE, stats).

insert(Key, Opts) ->
    gen_server:call(?MODULE, {insert, Key, self(), Opts}, ?TIMEOUT).

log_access(Key) ->
    gen_server:cast(?MODULE, {log_access, Key}).

log_ready(EntryPid, Key, Filename, Size, Checksum) ->
    gen_server:cast(?MODULE, {log_ready, EntryPid, Key, Filename, Size, Checksum}).


%% gen_server callbacks

init([]) ->
    {A1,A2,A3} = os:timestamp(),
    random:seed(A1, A2, A3),
    filezcache_store:init(),
    gen_server:cast(self(), log_init),
    timer:send_after(?GC_INTERVAL, gc),
    {ok, #state{
            sizes = ets:new(?MODULE, [set, private]),
            monitors = gb_trees:empty(), 
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
            {reply, {ok, Pid}, State1}
    end;
handle_call(stats, _From, State) ->
    Stats = [
        {bytes, State#state.bytes},
        {max_bytes, max_bytes()},
        {count, gb_trees:size(State#state.monitors)},
        {gc_candidate_pool, State#state.gc_candidate_pool}
    ],
    {reply, Stats, State}.

handle_cast({repop, Key, Filename, undefined, _Checksum}, State) ->
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            ok;
        {error, enoent} ->
            delete(Key, Filename)
    end,
    {noreply, State};
handle_cast({repop, Key, Filename, Size, Checksum}, State) ->
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            {noreply, State};
        {error, enoent} ->
            {ok, Pid} = filezcache_entry_sup:start_child(Key, self(), []),
            filezcache_store:insert(Key, Pid),
            Mon = erlang:monitor(process, Pid),
            filezcache_entry:repop(Pid, Key, Filename, Size, Checksum),
            State1 = State#state{monitors=gb_trees:enter(Mon, {Key, Pid}, State#state.monitors)},
            {noreply, State1}
    end;
handle_cast({delete_if_unused, Key, Filename}, State) ->
    case filezcache_store:lookup(Key) of
        {ok, _Pid} ->
            nop;
        {error, enoent} ->
            delete(Key, Filename)
    end,
    {noreply, State};


handle_cast(log_init, State) ->
    proc_lib:spawn_link(fun() -> repopulate() end),
    {noreply, State};

%% Log a cache entry to the disk log
handle_cast({log_ready, Pid, Key, Filename, Size, Checksum}, #state{sizes=SizeTab, bytes=Bytes} = State) ->
    F = fun() ->
            mnesia:write(#filezcache_log_entry{key=Key, filename=Filename, size=Size, checksum=Checksum})
        end,
    mnesia:activity(transaction, F), 
    ets:insert(SizeTab, {Pid,Size}),
    filezcache_event:insert_ready(Key, Size, Filename),
    {noreply, State#state{bytes=Bytes + case Size of undefined -> 0; _ -> Size end}};

%% Remove recently used keys from the eviction pool
handle_cast({log_access, Pid}, #state{gc_candidate_pool=Pool} = State) ->
    case lists:member(Pid, Pool) of
        false ->
            {noreply, State};
        true ->
            Pool1 = [ P || P <- Pool, P =/= Pid ],
            {noreply, State#state{gc_candidate_pool=Pool1}}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, #state{monitors=Monitors, bytes=Bytes, sizes=SizeTab} = State) ->
    case gb_trees:lookup(MRef, State#state.monitors) of
        {value, {Key, Pid}} ->
            Size = case ets:lookup(SizeTab, Pid) of
                        [{_Pid, N}] -> 
                            ets:delete(SizeTab, Pid),
                            N;
                        [] -> 
                            0
                   end,
            delete(Key),
            filezcache_store:delete(Key),
            filezcache_event:delete(Key),
            {noreply, State#state{monitors = gb_trees:delete(MRef, Monitors), bytes=erlang:max(0,Bytes-Size)}};
        none ->
            {noreply, State} 
    end; 
handle_info(gc, State) ->
    State1 = do_gc(State),
    timer:send_after(?GC_INTERVAL, gc),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


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
                    gen_server:cast(?MODULE, {delete_if_unused, Key, Filename});
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
            ok;
        [#filezcache_log_entry{key=Key,filename=Filename}] ->
            delete(Key,Filename)
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


fill_pool(State, _Method, 20) ->
    State;
fill_pool(State, normal, 2) ->
    State;
fill_pool(#state{gc_candidate_pool=Pool, iterator=Iterator} = State, Method, N) ->
    case length(Pool) < ?GC_POOL_SIZE of
        true ->
            {Candidates, Iterator1} = filezcache_store:iterate(Iterator), 
            Pool1 = fill_pool_1(Pool, Candidates, Method),
            fill_pool(State#state{gc_candidate_pool=Pool1, iterator=Iterator1}, Method, N+1);
        false ->
            State
    end.

fill_pool_1(Pool, [], _Method) ->
    Pool;
fill_pool_1(Pool, [C|Cs], Method) ->
    case not lists:member(C, Pool) andalso do_select(Method) of
        true -> fill_pool_1([C|Pool], Cs, Method);
        false -> fill_pool_1(Pool, Cs, Method)
    end.

do_select(eager) ->
    true;
do_select(normal) ->
    random:uniform(?GC_CHANCE_1_IN_N) =:= 1.

random_evict([]) -> 
    [];
random_evict(Pool) ->
    Victim = lists:nth(random:uniform(length(Pool)), Pool),
    filezcache_entry:delete(Victim),
    [ Pid || Pid <- Pool, Pid =/= Victim ].

max_bytes() ->
    case application:get_env(filezcache, max_bytes) of
        undefined -> ?GC_MAX_BYTES;
        {ok, N} when is_integer(N) -> N
    end.
