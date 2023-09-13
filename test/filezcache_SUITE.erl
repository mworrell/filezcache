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
        cache_binary_test,
        cache_delete_test,
        cache_tmpfile_test,
        cache_file_test,
        cache_stream_test
    ].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

cache_binary_test(_Config) ->
    {ok, Pid} = filezcache:insert(a, <<"a">>),
    true = is_pid(Pid),
    {ok, {file, 1, File}} = filezcache:lookup(a),
    true = filelib:is_file(File),
    {ok, <<"a">>} = file:read_file(File),
    ok.

cache_delete_test(_Config) ->
    % Delete the key from the previous test
    ok = filezcache:delete(a),
    {error, enoent} = filezcache:lookup(a),
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
    false = erlang:is_process_alive(IO),
    false = erlang:is_process_alive(WriterPid),
    ok.

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
