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

-module(filezcache_event).

-export([start_link/0, add_handler/2, delete_handler/2, lookup/1, insert/1, insert_ready/3, delete/1]).

-define(SERVER, ?MODULE).

start_link() ->
    gen_event:start_link({local, ?SERVER}).

add_handler(Handler, Args) ->
    gen_event:add_handler(?SERVER, Handler, Args).

delete_handler(Handler, Args) ->
    gen_event:delete_handler(?SERVER, Handler, Args).

lookup(Key) ->
    gen_event:notify(?SERVER, {lookup, Key}).

insert(Key) ->
    gen_event:notify(?SERVER, {insert, Key}).

insert_ready(Key, Size, Filename) ->
    gen_event:notify(?SERVER, {insert_ready, {Key, Size, Filename}}).

delete(Key) ->
    gen_event:notify(?SERVER, {delete, Key}).
