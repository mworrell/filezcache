%% @private
%% @author Marc Worrell
%% @copyright 2013-2024 Marc Worrell
%% @doc Supervisor for filezcache application.
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

-module(filezcache_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    ElementSup = {filezcache_entry_sup, {filezcache_entry_sup, start_link, []},
                  permanent, 2000, supervisor, [filezcache_entry]},
    EventManager = {filezcache_event, {filezcache_event, start_link, []}, 
                    permanent, 2000, worker, [filezcache_event]},
    EntryManager = {filezcache_entry_manager, {filezcache_entry_manager, start_link, []},
                    permanent, 2000, worker, [filezcache_entry_manager]},
    DeviceSup = {filezcache_device_sup, {filezcache_device_sup, start_link, []},
                    permanent, 2000, worker, [filezcache_device]},
    Children = [ElementSup, EventManager, EntryManager, DeviceSup],
    RestartStrategy = {one_for_one, 4, 3600},
    {ok, {RestartStrategy, Children}}.
