%% @private
%% @author Marc Worrell
%% @copyright 2013-2024 Marc Worrell
%% @doc Log filecache events to logger.
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

-module(filezcache_event_logger).

-behaviour(gen_event).

-export([add_handler/0, delete_handler/0]).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, code_change/3, terminate/2]).

-record(state, {}).

-include_lib("kernel/include/logger.hrl").

%% API
add_handler() ->
    filezcache_event:add_handler(?MODULE, []).

delete_handler() ->
    filezcache_event:delete_handler(?MODULE, []).

%% gen_event callbacks
init([]) ->
    {ok, #state{}}.

handle_event({insert, Key}, State) ->
    ?LOG_INFO(#{
        in => filezcache,
        text => <<"Insert key start">>,
        result => ok,
        what => insert_key,
        key => Key
    }),
    {ok, State};
handle_event({lookup, Key}, State) ->
    ?LOG_INFO(#{
        in => filezcache,
        text => <<"Lookup key">>,
        result => ok,
        what => lookup_key,
        key => Key
    }),
    {ok, State};
handle_event({delete, Key}, State) ->
    ?LOG_INFO(#{
        in => filezcache,
        text => <<"Delete key">>,
        result => ok,
        what => delete_key,
        key => Key
    }),
    {ok, State};
handle_event({insert_ready, {Key, Size, Filename}}, State) ->
    ?LOG_INFO(#{
        in => filezcache,
        text => <<"Insert key done">>,
        result => ok,
        what => insert_ready,
        key => Key,
        size => Size,
        filename => Filename
    }),
    {ok, State}.

handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
