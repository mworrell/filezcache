%% @author Marc Worrell
%% @copyright 2014 Marc Worrell

%% Copyright 2014 Marc Worrell
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

-module(filezcache_device_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/4]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(CachePid, Filename, Size, FinalSize) ->
    supervisor:start_child(?SERVER, [CachePid, Filename, Size, FinalSize]).

init([]) ->
    Element = {filezcache_device, {filezcache_device, start_link, []},
               temporary, brutal_kill, worker, [filezcache_device]},
    Children = [Element],
    RestartStrategy = {simple_one_for_one, 0, 1},
    {ok, {RestartStrategy, Children}}.
