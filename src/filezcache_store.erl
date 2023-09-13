%% @private
%% @author Marc Worrell
%% @copyright 2013-2014 Marc Worrell
%% @doc Manages the mapping between filezache_entry processes and their keys.

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

-module(filezcache_store).

-export([init/0, insert/2, delete/1, lookup/1, iterate/1]).

-define(TABLE_ID, ?MODULE).

init() ->
    ets:new(?TABLE_ID, [set, protected, named_table]),
    ok.

insert(Key, Pid) ->
    ets:insert(?TABLE_ID, {Key, Pid}).

lookup(Key) ->
    case ets:lookup(?TABLE_ID, Key) of
        [{Key, Pid}] -> {ok, Pid};
        [] -> {error, enoent}
    end.

delete(Pid) when is_pid(Pid) ->
    ets:match_delete(?TABLE_ID, {'_', Pid});
delete(Key) ->
    ets:delete(?TABLE_ID, Key).

iterate(start) ->
    case ets:info(?TABLE_ID, size) of
        0 -> {[], start};
        N when is_integer(N) -> iterate(0)
    end;
iterate(SlotNr) ->
    case get_slot(SlotNr) of
        '$end_of_table' -> 
            {[], start};
        Entries -> 
            Pids = [ Pid || {_,Pid} <- Entries ],
            {Pids, SlotNr+1}
    end.

get_slot(SlotNr) ->
    try 
        ets:slot(?TABLE_ID, SlotNr)
    catch
        error:badarg -> '$end_of_table'
    end.
