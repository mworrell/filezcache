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

-module(filecache_entry_manager).

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


-record(state, {
    monitors,
    sizes,
    log,
    iterator = start,
    gc_candidate_pool = [] :: list(pid()),
    bytes = 0 :: integer(),
    max_bytes :: integer() 
    }).

-define(LOG_SIZE, 1000000).
-define(LOG_FILES, 5).

% Garbage collection setings
-define(GC_INTERVAL, 1000). 
-define(GC_MAX_BYTES, 10737418240).
-define(GC_POOL_SIZE, 100).
-define(GC_CHANCE_1_IN_N, 20).

%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stats() ->
    gen_server:call(?MODULE, stats).

insert(Key, Opts) ->
    gen_server:call(?MODULE, {insert, Key, self(), Opts}).

log_access(Key) ->
    gen_server:cast(?MODULE, {log_access, Key}).

log_ready(EntryPid, Key, Filename, Size, Checksum) ->
    gen_server:cast(?MODULE, {log_ready, EntryPid, Key, Filename, Size, Checksum}).


%% gen_server callbacks

init([]) ->
    {A1,A2,A3} = os:timestamp(),
    random:seed(A1, A2, A3),
    filecache_store:init(),
    gen_server:cast(self(), log_init),
    timer:send_after(?GC_INTERVAL, gc),
    {ok, #state{
            sizes = ets:new(?MODULE, [set, private]),
            monitors = gb_trees:empty(), 
            max_bytes = max_bytes()}}.

handle_call({insert, Key, WriterPid, Opts}, _From, State) ->
    case filecache_store:lookup(Key) of
        {ok, Pid} ->
            {reply, {error, {already_started, Pid}}, State};
        {error, not_found} ->
            {ok, Pid} = filecache_entry_sup:start_child(Key, WriterPid, Opts),
            filecache_store:insert(Key, Pid),
            filecache_event:insert(Key),
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

handle_cast({repop, Key, Filename, Size, Checksum}, State) ->
    case filecache_store:lookup(Key) of
        {ok, _Pid} ->
            {noreply, State};
        {error, not_found} ->
            {ok, Pid} = filecache_entry_sup:start_child(Key, self(), []),
            filecache_store:insert(Key, Pid),
            Mon = erlang:monitor(process, Pid),
            filecache_entry:repop(Pid, Key, Filename, Size, Checksum),
            State1 = State#state{monitors=gb_trees:enter(Mon, {Key, Pid}, State#state.monitors)},
            {noreply, State1}
    end;
handle_cast({delete_if_unused, Key, Filename}, State) ->
    case filecache_store:lookup(Key) of
        {ok, _Pid} ->
            nop;
        {error, not_found} ->
            _ = file:delete(Filename)
    end,
    {noreply, State};


handle_cast(log_init, State) ->
    {ok, Log} = open_log(),
    proc_lib:spawn_link(fun() -> repopulate(Log) end),
    {noreply, State#state{log=Log}};

%% Log a valid cache entry to the disk log
handle_cast({log_ready, Pid, Key, Filename, Size, Checksum}, #state{log=Log, sizes=SizeTab, bytes=Bytes} = State) ->
    ok = disk_log:log(Log, {log_ready, Key, Filename, Size, Checksum}),
    ets:insert(SizeTab, {Pid,Size}),
    filecache_event:insert_ready(Key, Size, Filename),
    {noreply, State#state{bytes=Size+Bytes}};

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
            filecache_store:delete(Key),
            filecache_event:delete(Key),
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


open_log() ->
    LogFile = filename:join([filecache:journal_dir(), "filecache.log"]),
    LogArgs = [
        {name, filecache},
        {file, LogFile},
        {size, {?LOG_SIZE, ?LOG_FILES}},
        {type, wrap},
        {mode, read_write}
    ],
    case disk_log:open(LogArgs) of
        {ok, Log} ->
            {ok, Log};
        {repaired, Log, {recovered, _Rec}, {badbytes, _Bad}} ->
            {ok, Log}
    end.


%% @doc Repopulates the cache using the disk-log and the data files
repopulate(Log) ->
    disk_log:block(Log),
    repop_loop(Log, disk_log:chunk(Log, start)).

repop_loop(Log, eof) ->
    disk_log:unblock(Log),
    ok;
repop_loop(Log, {error, _Error}) ->
    disk_log:unblock(Log),
    ok;
repop_loop(Log, {Cont, Terms}) ->
    repop_terms(Terms),
    repop_loop(Log, disk_log:chunk(Log, Cont));
repop_loop(Log, {Cont, Terms, _BadBytes}) ->
    repop_terms(Terms),
    repop_loop(Log, disk_log:chunk(Log, Cont)).

repop_terms(Terms) ->
    lists:foreach(fun(T) -> repop_term(T) end, Terms).

repop_term({log_ready, Key, Filename, Size, Checksum}) ->
    case filecache_store:lookup(Key) of
        {error, not_found} ->
            case file:read_file_info(Filename) of
                {ok, #file_info{type=regular, size=Size}} ->
                    case filecache:checksum(Filename) of
                        Checksum ->
                            gen_server:cast(?MODULE, {repop, Key, Filename, Size, Checksum});
                        _Other ->
                            gen_server:cast(?MODULE, {delete_if_unused, Key, Filename})
                    end;
                {ok, #file_info{type=regular}} ->
                    gen_server:cast(?MODULE, {delete_if_unused, Key, Filename});
                _Other ->
                    nop
            end;
        {ok, _Pid} ->
            nop
    end;
repop_term(_Term) ->
    ok.


%% @doc Perform a gc step, keep eviction pool with gc-candidates populated
do_gc(State) ->
    State1 = maybe_evict(State),
    fill_pool(State1, normal).

maybe_evict(#state{gc_candidate_pool=[], bytes=Bytes} = State) ->
    case Bytes > max_bytes() of
        true -> do_gc(fill_pool(State, eager));
        false -> State
    end;
maybe_evict(#state{gc_candidate_pool=Pool, bytes=Bytes} = State) ->
    case Bytes > max_bytes() of
        true -> State#state{gc_candidate_pool=random_evict(Pool)};
        false -> State
    end;
maybe_evict(State) ->
    State.

fill_pool(#state{gc_candidate_pool=Pool, iterator=Iterator} = State, Method) ->
    case length(Pool) < ?GC_POOL_SIZE of
        true ->
            {Candidates, Iterator1} = filecache_store:iterate(Iterator), 
            Pool1 = fill_pool_1(Pool, Candidates, Method),
            State#state{gc_candidate_pool=Pool1, iterator=Iterator1};
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
    filecache_entry:delete(Victim),
    [ Pid || Pid <- Pool, Pid =/= Victim ].

max_bytes() ->
    case application:get_env(filecache, max_bytes) of
        undefined -> ?GC_MAX_BYTES;
        {ok, N} when is_integer(N) -> N
    end.
