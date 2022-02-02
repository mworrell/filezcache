%% @author Marc Worrell
%% @copyright 2013-2022 Marc Worrell
%% @doc Writes a file to the filezcache, streams the file while writing

%% Copyright 2013-2022 Marc Worrell
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

-module(filezcache_entry).

-include_lib("kernel/include/logger.hrl").

-behaviour(gen_fsm).

% api
-export([
        start_link/3,
        fetch/2,
        fetch_file/2,
        store/2,
        repop/5,
        append_stream/2,
        finish_stream/1,
        delete/1,
        logged/1,
        gc/1
        ]).

% gen_fsm
-export([init/1, handle_sync_event/4, handle_event/3, handle_info/3, terminate/3, code_change/4]).

% states
-export([
    wait_for_data/2,
    wait_for_data/3,
    streaming/2,
    streaming/3,
    idle/3,
    idle/2,
    closing/2
    ]).

% For testing
-export([ 
    filename/1
    ]).

-record(state, {
        key,
        filename,
        fd,
        size,
        final_size,
        checksum,
        checksum_context,
        writer_pid,
        writer_mon,
        lockers = [],
        devices = [],
        waiters = [],
        last_access,
        last_check}).

% Max every 10secs, we check if our file still exists (usec)
-define(CHECK_PERIOD, 10000000).

% After 30 minutes in the idle look we assume something went wrong with logging and will close down.
-define(IDLE_TIMEOUT, 1800000).

%% API

start_link(Key, Writer, Opts) ->
    gen_fsm:start_link(?MODULE, [Key, Writer, Opts], []).

fetch(Pid, Opts) ->
    try
        gen_fsm:sync_send_event(Pid, {fetch, self(), Opts}, infinity)
    catch
        exit:{noproc, _} ->
            {error, enoent}
    end.

fetch_file(Pid, Opts) ->
    try
        gen_fsm:sync_send_event(Pid, {fetch_file, self(), Opts}, infinity)
    catch
        exit:{noproc, _} ->
            {error, enoent}
    end.

-spec store(pid(),
             {stream_start, pid(), integer()|undefined}
            |{stream_fun, pid(), function(), integer()|undefined}
            |{data, binary()}
            |{file, file:filename_all()}
            |{tmpfile, file:filename_all()}) -> ok.
store(Pid, Value) ->
    gen_fsm:send_event(Pid, Value).

repop(Pid, Key, Filename, Size, Checksum) ->
    gen_fsm:send_event(Pid, {repop, Key, Filename, Size, Checksum}).

append_stream(Pid, Data) ->
    gen_fsm:send_event(Pid, {stream_append, self(), Data}).

finish_stream(Pid) ->
    gen_fsm:send_event(Pid, {stream_finish, self()}).

delete(Pid) ->
    try
        case gen_fsm:sync_send_all_state_event(Pid, delete, infinity) of
            ok ->
                MRef = erlang:monitor(process, Pid),
                receive
                    {'DOWN', MRef, process, Pid, _Reason} ->
                        ok
                end;
            Other ->
                Other
        end
    catch
        exit:{noproc, _} ->
            {error, noproc}
    end.

logged(Pid) ->
    gen_fsm:send_event(Pid, logged).

gc(Pid) ->
    gen_fsm:send_all_state_event(Pid, gc).

%% gen_server callbacks

init([Key, WriterPid, Opts]) ->
    process_flag(trap_exit, true),
    Filename = filename(Key),
    Now = os:timestamp(),
    State =  #state{key = Key,
                    filename = Filename,
                    fd = undefined,
                    size = 0,
                    final_size = undefined,
                    checksum = 0,
                    checksum_context = undefined, 
                    writer_pid = WriterPid,
                    writer_mon = erlang:monitor(process, WriterPid), 
                    devices = [],
                    waiters = [],
                    last_access = Now,
                    last_check = Now},
    % Ensure that a log entry has been made, but with 'undefined' size
    log_ready(State),
    {ok, wait_for_data, opt_locker(State, WriterPid, Opts)}.

%% Wait till all data is received and stored in the cache file.

wait_for_data({data, Data}, #state{devices=Devices, waiters=Waiters, filename=Filename} = State) when is_binary(Data) ->
    Size = size(Data),
    ok = file:write_file(Filename, Data),
    send_devices(Devices, {final, Size}),
    send_waiters(Waiters, Size, Filename),
    State1 = State#state{checksum=crypto:hash(sha, Data), 
                         size=Size,
                         final_size=Size,
                         devices=[]},
    log_ready(State1),
    {next_state, idle, demonitor_writer(State1), ?IDLE_TIMEOUT};
wait_for_data({file, Filename}, #state{filename=Filename} = State) ->
    State1 = send_file_state(set_file_state(Filename, State)),
    log_ready(State1),
    {next_state, idle, demonitor_writer(State1), ?IDLE_TIMEOUT};
wait_for_data({file, File}, #state{filename=Filename} = State) ->
    {ok, _BytesCopied} = file:copy(File, Filename),
    State1 = send_file_state(set_file_state(Filename, State)),
    log_ready(State1),
    {next_state, idle, demonitor_writer(State1), ?IDLE_TIMEOUT};
wait_for_data({tmpfile, TmpFile}, #state{filename=Filename} = State) ->
    ok = rename(TmpFile, Filename),
    State1 = send_file_state(set_file_state(Filename, State)),
    log_ready(State1),
    {next_state, idle, demonitor_writer(State1), ?IDLE_TIMEOUT};
wait_for_data({stream_fun, Streamer, FinalSize, Fun}, #state{writer_pid=Streamer, filename=Filename} = State) ->
    Self = self(),
    Pid = spawn_link(fun() -> Fun(Self) end),
    {ok, FD} = file:open(Filename, [write,binary]),
    State1 = (demonitor_writer(State))#state{
        fd=FD,
        size=0,
        writer_pid=Pid,
        writer_mon=erlang:monitor(process,Pid),
        final_size=FinalSize,
        checksum_context=crypto:hash_init(sha)
    },
    {next_state, streaming, State1};
wait_for_data({stream_start, Streamer, FinalSize}, #state{writer_pid=Streamer, filename=Filename} = State) ->
    {ok, FD} = file:open(Filename, [write,binary]),
    State1 = State#state{
        fd=FD,
        size=0,
        final_size=FinalSize,
        checksum_context=crypto:hash_init(sha)
    },
    {next_state, streaming, State1};
wait_for_data({repop, Key, Filename, Size, Checksum}, State) ->
    State1 = State#state{
        key=Key,
        filename=Filename,
        size=Size,
        final_size=Size,
        checksum=Checksum
    },
    State2 = send_file_state(State1),
    log_ready(State2),
    {next_state, idle, demonitor_writer(State2), ?IDLE_TIMEOUT}.

wait_for_data({fetch, _Pid, _Opts} = Fetch, From, State) ->
    handle_reply_partial_data(Fetch, From, wait_for_data, State);
wait_for_data({fetch_file, Pid, Opts}, From, #state{waiters=Waiters, key=Key} = State) ->
    filezcache_entry_manager:log_access(Key, Pid),
    {next_state, wait_for_data, opt_locker(State#state{waiters=[From|Waiters]}, Pid, Opts)}.

%% Receive data from a stream

streaming({stream_append, Streamer, Data}, #state{writer_pid=Streamer, fd=FD, devices=Devices, size=Size, checksum_context=Ctx} = State) ->
    ok = file:write(FD, Data),
    Ctx1 = crypto:hash_update(Ctx, Data),
    NewSize = Size+size(Data),
    send_devices(Devices, {stream, NewSize}),
    {next_state, streaming, State#state{checksum_context=Ctx1, size=NewSize}};
streaming({stream_finish, Streamer}, #state{writer_pid=Streamer, fd=FD, size=Size, devices=Devices, waiters=Waiters, checksum_context=Ctx} = State) ->
    ok = file:close(FD),
    send_devices(Devices, {final, Size}),
    send_waiters(Waiters, Size, State#state.filename),
    State1 = State#state{checksum_context=undefined, fd=undefined, final_size=Size, checksum=crypto:hash_final(Ctx), devices=[], waiters=[]},
    log_ready(State1),
    {next_state, idle, demonitor_writer(State1), ?IDLE_TIMEOUT}.

streaming({fetch, _Pid, _Opts} = Fetch, From, State) ->
    handle_reply_partial_data(Fetch, From, streaming, State);
streaming({fetch_file, Pid, Opts}, From, #state{waiters=Waiters, key=Key} = State) ->
    filezcache_entry_manager:log_access(Key, Pid),
    {next_state, streaming, opt_locker(State#state{waiters=[From|Waiters]}, Pid, Opts)}.

%% idle - handle all cache requests
idle({Fetch, Pid, Opts}, _From, #state{key=Key, filename=Filename, size=Size} = State) when Fetch =:= fetch; Fetch =:= fetch_file ->
    filezcache_entry_manager:log_access(Key),
    case maybe_check_filename(os:timestamp(), State) of
        {ok, State1} ->
            {reply, {ok, {file, Size, Filename}}, idle, opt_locker(State1, Pid, Opts), ?IDLE_TIMEOUT};
        {error, _} = Error ->
            ?LOG_WARNING("Filezcache file error ~p on ~p", [Error, Filename]),
            {reply, Error, closing, State, 0}
    end.

idle(logged, #state{fd=FD} = State) ->
    _ = file:close(FD),
    {stop, normal, State#state{fd=undefined, filename=undefined}};

idle(timeout, State) ->
    {next_state, closing, State}.


%% Closing down
closing(timeout, #state{fd=FD, filename=Filename} = State) when FD =/= undefined ->
    _ = file:close(FD),
    _ = file:delete(Filename),
    {stop, normal, State#state{fd=undefined, filename=undefined}};
closing(timeout, #state{filename=Filename} = State) ->
    _ = file:delete(Filename),
    {stop, normal, State#state{filename=undefined}}.

%% All state events: 'gc'
handle_event(gc, wait_for_data, State) ->
    {next_state, wait_for_data, State};
handle_event(gc, streaming, State) ->
    {next_state, streaming, State};
handle_event(gc, StateName, #state{lockers=Lockers} = State) when Lockers =/= [] ->
    {next_state, StateName, State};
handle_event(gc, _StateName, State) ->
    {next_state, closing, State, 0};

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% All sync state events: 'delete'
handle_sync_event(delete, _From, StateName, #state{lockers=Lockers} = State) when Lockers =/= [] ->
    {reply, {error, locked}, StateName, State};
handle_sync_event(delete, _From, _StateName, State) ->
    {reply, ok, closing, State, 0};

%% Note: DO NOT reply to unexpected calls. Let the call-maker crash!
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% Remove stopped processes that were locking this entry
handle_info({'DOWN', MRef, process, _Pid, _Reason}, _StateName, #state{writer_mon=MRef} = State) ->
    error_logger:warning_msg("Filezcache entry's writer is down (~p)", [State#state.key]),
    {stop, normal, State#state{writer_mon=undefined}};
handle_info({'DOWN', MRef, process, _Pid, _Reason}, StateName, State) ->
    State1 = State#state{lockers=[ M || M <- State#state.lockers, M =/= MRef ]},
    {next_state, StateName, State1};
handle_info(_Info, StateName, State) ->
    erlang:put(state_name, StateName), 
    {next_state, StateName, State}.

terminate(_Reason, _StateName, #state{fd=undefined}) ->
    % Regular shutdown, keep the cache entry
    ok;
terminate(_Reason, _StateName, #state{fd=FD, filename=Filename}) ->
    % Incomplete cache entry, cleanup
    _ = file:close(FD), 
    _ = file:delete(Filename),
    ok. 

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%% ------------------------------------------------------------------------------------------------------
%%% Support functions
%%% ------------------------------------------------------------------------------------------------------

demonitor_writer(#state{writer_mon=undefined} = State) ->
    State;
demonitor_writer(#state{writer_mon=MRef} = State) ->
    erlang:demonitor(MRef), 
    State#state{
        writer_pid=undefined,
        writer_mon=undefined
    }.

maybe_check_filename(Now, State) ->
    IsCheckTime = timer:now_diff(Now, State#state.last_check) >  ?CHECK_PERIOD,
    maybe_check_filename1(IsCheckTime, Now, State).

maybe_check_filename1(false, Now, #state{filename=Filename} = State) ->
    case filelib:is_regular(Filename) of
        false ->
            {error, enoent};
        true ->
            {ok, State#state{last_check = Now, last_access = Now}}
    end;
maybe_check_filename1(true, Now, State) ->
    {ok, State#state{last_access = Now}}.


%% @doc We didn't receive all data yet, reply with a io-device which can be used to read the data.
handle_reply_partial_data({fetch, Pid, Opts}, _From, StateName, 
                          #state{key=Key, size=Size, final_size=FinalSize, filename=Filename, devices=Devices} = State) ->
    {ok, DevicePid} = filezcache_device_sup:start_child(self(), Filename, Size, FinalSize),
    filezcache_entry_manager:log_access(Key, DevicePid),
    {reply, {ok, {device, DevicePid}}, StateName, opt_locker(State#state{devices=[DevicePid|Devices]}, Pid, Opts)}.


log_ready(#state{key=Key, filename=Filename, final_size=FinalSize, checksum=Checksum}) ->
    filezcache_entry_manager:log_ready(self(), Key, Filename, FinalSize, Checksum).


set_file_state(Filename, State) ->
    Size = filelib:file_size(Filename),
    State#state{filename=Filename, size=Size, final_size=Size, checksum=filezcache:checksum(Filename)}.

send_file_state(#state{devices=Devices, waiters=Waiters, filename=Filename, size=Size} = State) ->
    send_devices(Devices, {final, Size}),
    send_waiters(Waiters, Size, Filename),
    State#state{devices=[], waiters=[]}.


filename(Key) ->
    [ A1,A2,B1,B2 | HashS ] = encode(crypto:hash(sha256, term_to_binary(Key)), 36),
    Filename = filename:join([filezcache:data_dir(), [A1,A2], [B1,B2], HashS]),
    ok = filelib:ensure_dir(Filename),
    Filename. 

encode(Data, Base) when is_binary(Data) ->
    encode(binary_to_list(Data), Base);
encode(Data, Base) when is_list(Data) ->
    F = fun(C) ->
        case erlang:integer_to_list(C, Base) of
            [C1, C2] -> [C1, C2];
            [C1]     -> [$0, C1]
        end
    end,
    lists:flatten([F(I) || I <- Data]).


send_devices([], _Msg) ->
    ok;
send_devices(Pids, Msg) ->
    lists:map(fun(Pid) -> 
                  gen_server:cast(Pid, Msg)
              end,
              Pids).

send_waiters([], _Size, _Filename) ->
    ok;
send_waiters(Pids, Size, Filename) ->
    lists:map(fun(From) ->
                    gen_fsm:reply(From, {ok, {file, Size, Filename}})
              end,
              Pids).


opt_locker(State, _Pid, []) ->
    State;
opt_locker(State, Pid, Opts) ->
    case lists:member(lock, Opts) of
        true ->
            MRef = erlang:monitor(process, Pid),
            State#state{lockers=[MRef|State#state.lockers]};
        false ->
            State
    end.


rename(TmpFile, Filename) ->
    case file:rename(TmpFile, Filename) of
        %% cross-fs rename is not supported by erlang, so copy and delete the file
        {error, exdev} ->
            {ok, _BytesCopied} = file:copy(TmpFile, Filename),
            ok = file:delete(TmpFile);
        ok -> 
            ok
    end.
