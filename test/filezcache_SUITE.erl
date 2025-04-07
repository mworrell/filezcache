-module(filezcache_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").


%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(filezcache),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(filezcache),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

all() ->
    [
        remove_unknown_key_test,
        cache_binary_test,
        cache_delete_test,
        cache_tmpfile_test,
        cache_file_test,
        cache_stream_test,
        eviction_test
    ].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

remove_unknown_key_test(_Config) ->
    ok = filezcache:delete(unknown_key),
    ok.

cache_binary_test(_Config) ->
    #{
        insert_count := 0,
        hit_count := 0
    } = filezcache:stats(),
    {ok, Pid} = filezcache:insert(a, <<"a">>),
    true = is_pid(Pid),
    {ok, {file, 1, File}} = filezcache:lookup(a),
    #{
        hit_count := 1,
        miss_count := 0
    } = filezcache:stats(),
    true = filelib:is_file(File),
    {ok, <<"a">>} = file:read_file(File),
    ok.

cache_delete_test(_Config) ->
    % Delete the key from the previous test
    #{
        delete_count := 0
    } = filezcache:stats(),
    ok = filezcache:delete(a),
    #{
        delete_count := 1,
        miss_count := 0
    } = filezcache:stats(),
    {error, enoent} = filezcache:lookup(a),
    #{
        miss_count := 1
    } = filezcache:stats(),
    ok.

cache_tmpfile_test(_Config) ->
    Data = <<"test-tmp">>,
    DataSize = size(Data),
    TmpFile = tempfile(),
    ok = file:write_file(TmpFile, Data),
    true = filelib:is_file(TmpFile),
    {ok, Pid} = filezcache:insert_tmpfile(tmpfile, TmpFile),
    true = is_pid(Pid),
    % New file in the cache
    {ok, {file, DataSize, File}} = filezcache:lookup(tmpfile),
    true = filelib:is_file(File),
    {ok, Data} = file:read_file(File),
    % Tmpfile should be gone
    false = filelib:is_file(TmpFile),
    % And delete the cache entry again
    ok = filezcache:delete(tmpfile),
    {error, enoent} = filezcache:lookup(tmpfile),
    ok.

cache_file_test(_Config) ->
    Data = <<"test-file">>,
    DataSize = size(Data),
    TmpFile = tempfile(),
    ok = file:write_file(TmpFile, Data),
    true = filelib:is_file(TmpFile),
    {ok, Pid} = filezcache:insert_file(testfile, TmpFile),
    true = is_pid(Pid),
    % New file in the cache
    {ok, {file, DataSize, File}} = filezcache:lookup(testfile),
    true = filelib:is_file(File),
    {ok, Data} = file:read_file(File),
    % File should still be there
    true = filelib:is_file(TmpFile),
    ok = file:delete(TmpFile),
    % And delete the cache entry again
    ok = filezcache:delete(testfile),
    {error, enoent} = filezcache:lookup(testfile),
    ok.

cache_stream_test(_Config) ->
    {ok, WriterPid} = filezcache:insert_stream(stream, 20, []),
    true = is_pid(WriterPid),
    {ok, {device, IO}} = filezcache:lookup(stream),
    true = is_pid(IO),
    ok = filezcache:append_stream(WriterPid, <<"12345">>),
    {ok, <<"1234">>} = file:read(IO, 4),
    ok = filezcache:append_stream(WriterPid, <<"67890">>),
    {ok, <<"567890">>} = file:read(IO, 6),
    ok = filezcache:append_stream(WriterPid, <<"0987654321">>),
    true = erlang:is_process_alive(WriterPid),
    ok = filezcache:finish_stream(WriterPid),
    {ok, <<"0987654321">>} = file:read(IO, 10),
    {error, eof} = file:read(IO, 1),
    true = erlang:is_process_alive(IO),
    ok = file:close(IO),
    timer:sleep(100),
    false = erlang:is_process_alive(IO),
    false = erlang:is_process_alive(WriterPid),
    % Now the entry should be available as a file
    {ok, {file, 20, File}} = filezcache:lookup(stream),
    {ok, <<"12345678900987654321">>} = file:read_file(File),
    ok.

eviction_test(_Config) ->
    Data = <<"01234567899876543210">>,
    application:set_env(filezcache, max_bytes, 1000),
    % Insert 1000 entries of 20 bytes each
    Stats0 = filezcache:stats(),
    lists:foreach(
        fun(K) ->
            filezcache:insert(K, Data),
            % Bump key 1 to the back of the LRU
            {ok, _} = filezcache:lookup_file(1)
        end,
        lists:seq(1,1000)),
    timer:sleep(2000),
    Stats1 = filezcache:stats(),
    % Eviction should have started now with dropping the
    % first batch of files from the cache.
    #{ bytes := Size0, evict_count := Evict0 } = Stats0,
    #{ bytes := Size1, entries := Entries1, evict_count := Evict1 } = Stats1,
    true = (Size1 > Size0),
    true = (Size1 =< 1000),
    true = (Entries1 =< (1000 div size(Data))),
    true = (Evict1 > Evict0),
    % Oldest files should be gone
    lists:foreach(
        fun(K) ->
            {error, enoent} = filezcache:lookup_file(K)
        end,
        lists:seq(2,200)),
    % Key 1 should still be present
    {ok, _} = filezcache:lookup_file(1),
    % At least the newest 49 files should be present
    lists:foreach(
        fun(K) ->
            {ok, _} = filezcache:lookup_file(K)
        end,
        lists:seq(952,1000)).

%%--------------------------------------------------------------------
%% SUPPORT FUNCTIONS
%%--------------------------------------------------------------------

-spec tempfile() -> file:filename_all().
tempfile() ->
    A = rand:uniform(1000000000),
    B = rand:uniform(1000000000),
    Filename = filename:join(
        temppath(),
        iolist_to_binary( io_lib:format("ztmp-~s-~p.~p",[node(),A,B]) )
    ),
    case filelib:is_file(Filename) of
        true -> tempfile();
        false -> Filename
    end.

%% @doc Returns the path where to store temporary files.

-spec temppath() -> file:filename_all().
temppath() ->
    lists:foldl(
        fun
            (false, TmpPath) ->
                TmpPath;
            (Good, _) ->
                unicode:characters_to_binary(Good)
        end,
        <<"/tmp">>,
        [ os:getenv("TMP"), os:getenv("TEMP") ]).
