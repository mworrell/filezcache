-module(filezcache_entry_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [delete_complete, delete_streaming, collect_complete, idle_timeout, missing_file].

init_per_suite(Config) ->
    %% Keep entries alive without the manager acknowledging their log writes.
    %% This exercises cleanup before the asynchronous 'logged' cast arrives.
    undefined = whereis(filezcache_entry_manager),
    Saved = application:get_env(filezcache, data_dir),
    ok = application:set_env(filezcache, data_dir, ?config(priv_dir, Config)),
    [{saved_data_dir, Saved} | Config].

end_per_suite(Config) ->
    case ?config(saved_data_dir, Config) of
        undefined -> application:unset_env(filezcache, data_dir);
        {ok, Dir} -> application:set_env(filezcache, data_dir, Dir)
    end.

delete_complete(_Config) ->
    with_entry(complete, fun(Pid, _File) ->
        ok = filezcache_entry:delete(Pid)
    end).

delete_streaming(_Config) ->
    with_entry(streaming, fun(Pid, _File) ->
        ok = filezcache_entry:delete(Pid)
    end).

collect_complete(_Config) ->
    with_entry(complete, fun(Pid, _File) ->
        gen_statem:cast(Pid, gc)
    end).

idle_timeout(_Config) ->
    with_entry(complete, fun(Pid, File) ->
        {idle, State} = sys:get_state(Pid),
        ok = gen_statem:stop(Pid),
        %% Reuse the real entry state, but expire its timer immediately.
        {ok, TimedPid} = gen_statem:start(?MODULE, State, []),
        Ref = monitor(process, TimedPid),
        ok = gen_statem:call(TimedPid, expire),
        receive
            {'DOWN', Ref, process, TimedPid, normal} -> ok
        after 1000 -> ct:fail(idle_timeout_did_not_close)
        end,
        false = filelib:is_file(File)
    end).

missing_file(_Config) ->
    with_entry(complete, fun(Pid, File) ->
        ok = file:delete(File),
        {error, enoent} = filezcache_entry:fetch_file(Pid, [])
    end).

with_entry(Kind, Fun) ->
    Key = {?MODULE, make_ref()},
    {ok, Pid} = gen_statem:start(filezcache_entry, [Key, self(), []], []),
    Ref = monitor(process, Pid),
    File = filezcache_entry:filename(Key),
    try
        case Kind of
            complete -> filezcache_entry:store(Pid, {data, <<"cache data">>});
            streaming ->
                filezcache_entry:store(Pid, {stream_start, self(), 100}),
                filezcache_entry:append_stream(Pid, <<"partial data">>)
        end,
        %% Synchronize with the casts before triggering cleanup.
        {_, _} = sys:get_state(Pid),
        true = filelib:is_file(File),
        Fun(Pid, File),
        receive
            {'DOWN', Ref, process, Pid, normal} -> ok;
            {'DOWN', Ref, process, Pid, Reason} -> ct:fail({entry_crashed, Reason})
        after 1000 -> ct:fail(entry_did_not_close)
        end,
        false = filelib:is_file(File)
    after
        demonitor(Ref, [flush]),
        case is_process_alive(Pid) of
            true -> gen_statem:stop(Pid);
            false -> ok
        end,
        file:delete(File)
    end.

%% Test callback delegates every event to the production state functions.
init(State) ->
    {ok, idle, State}.

callback_mode() ->
    state_functions.

idle({call, From}, expire, State) ->
    {keep_state, State, [{reply, From, ok}, 0]};
idle(Type, Event, State) ->
    filezcache_entry:idle(Type, Event, State).

closing(Type, Event, State) ->
    filezcache_entry:closing(Type, Event, State).

terminate(Reason, StateName, State) ->
    filezcache_entry:terminate(Reason, StateName, State).
