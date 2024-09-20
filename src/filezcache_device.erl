%% @private
%% @author Marc Worrell
%% @copyright 2024 Marc Worrell
%% @doc Simulates an IO device for reading files that are streamed
%% from the external storage system to the server. This enables
%% sending the files to clients before they are completely stored in
%% the caching system.
%% @end

%% Copyright 2024 Marc Worrell
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

-module(filezcache_device).

-behaviour(gen_server).

-export([
    start_link/4,

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
    ]).

-record(state, {
    size,
    final_size,
    filename,
    fd,
    position,
    reader_pid,
    cache_pid,
    reader_monitor,
    cache_monitor,
    waiting
    }).

% After 2 secs of inactivity this device will close the cache file and hibernate
-define(HIBERNATE_TIMEOUT, 2000).

start_link(CachePid, Filename, Size, FinalSize) ->
    gen_server:start_link(?MODULE, [CachePid, Filename, Size, FinalSize], []).

init([CachePid, Filename, Size, FinalSize]) ->
    {ok, #state{
            reader_pid = undefined,
            cache_pid = CachePid,
            size = Size,
            final_size = FinalSize,
            filename = Filename,
            fd = undefined,
            position = 0,
            reader_monitor = undefined, 
            cache_monitor = erlang:monitor(process, CachePid)
        }}.

handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_message}, State}.

handle_cast({final, FinalSize}, State) ->
    waiting_request(State#state{final_size=FinalSize, size=FinalSize});
handle_cast({stream, NewSize}, State) ->
    waiting_request(State#state{size=NewSize});
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

handle_info({file_request, From, ReplyAs, Request} = Req, State0) ->
    State = set_reader_pid(From, State0),
    case request(Request, State) of
        {Tag, Reply, NewState} when Tag =:= ok; Tag =:= error ->
            From ! {file_reply, ReplyAs, Reply},
            {noreply, NewState, ?HIBERNATE_TIMEOUT};
        {wait, NewState} ->
            {noreply, NewState#state{waiting=Req}, ?HIBERNATE_TIMEOUT};
        {stop, Reply, NewState} ->
            From ! {file_reply, ReplyAs, Reply},
            {stop, normal, NewState}
    end;
handle_info({io_request, From, ReplyAs, Request} = Req, State0) ->
    State = set_reader_pid(From, State0),
    case request(Request, State) of
        {Tag, Reply, NewState} when Tag =:= ok; Tag =:= error ->
            From ! {io_reply, ReplyAs, Reply},
            {noreply, NewState, ?HIBERNATE_TIMEOUT};
        {wait, NewState} ->
            {noreply, NewState#state{waiting=Req}, ?HIBERNATE_TIMEOUT};
        {stop, Reply, NewState} ->
            From ! {io_reply, ReplyAs, Reply},
            {stop, normal, NewState}
    end;
handle_info({'DOWN', _MRef, process, Object, _Info}, #state{cache_pid=Object, size=Size, final_size=Size} = State) ->
    {noreply, State#state{cache_pid=undefined}};
handle_info({'DOWN', _MRef, process, Object, _Info}, #state{cache_pid=Object} = State) ->
    {stop, normal, State};
handle_info({'DOWN', _MRef, process, Object, _Info}, #state{reader_pid=Object} = State) ->
    {stop, normal, State};
handle_info(timeout, #state{fd=undefined} = State) ->
    {noreply, State, hibernate};
handle_info(timeout, #state{fd=Fd} = State) ->
    _ = file:close(Fd),
    {noreply, State#state{fd=undefined}, hibernate};
handle_info(_Info, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% --------------------------------------------------------------------------------
%%% Internal functions
%%% --------------------------------------------------------------------------------

set_reader_pid(Pid, #state{reader_pid=undefined} = State) ->
    State#state{
        reader_pid = Pid,
        reader_monitor = erlang:monitor(process, Pid) 
    };
set_reader_pid(_Pid, State) ->
    State.

waiting_request(#state{waiting=undefined} = State) ->
    {noreply, State};
waiting_request(#state{waiting=Waiting} = State) ->
    handle_info(Waiting, State#state{waiting=undefined}).


request(close, State) ->
    {stop, ok, State};  
request({get_chars, _Encoding, _Prompt, N}, State) ->
    get_chars(N, State);
request({position,eof}, #state{final_size=FinalSize} = State) when is_integer(FinalSize) ->
    {ok, {ok, FinalSize}, State#state{position=FinalSize}};
request({position,eof}, #state{final_size=undefined} = State) ->
    {wait, State};
request({position,bof}, State) ->
    {ok, {ok, 0}, State#state{position=0}};
request({position,At}, State) ->
    {ok, {ok, At}, State#state{position=At}};
request({requests, Reqs}, State) ->
     multi_request(Reqs, {ok, ok, State});
request({setopts, Opts}, State) ->
    setopts(Opts, State);
request(getopts, State) ->
    getopts(State);
% Unsupported
request({put_chars, _Encoding, _Chars}, State) ->
    {error, {error,enotsup}, State};
request({get_line, _Encoding, _Prompt}, State) ->
    {error, {error,enotsup}, State};
request({get_geometry,_}, State) ->
    {error, {error,enotsup}, State};
% Old API
request({put_chars,Chars}, State) ->
    request({put_chars,latin1,Chars}, State);
request({put_chars,M,F,As}, State) ->
    request({put_chars,latin1,M,F,As}, State);
request({get_chars,Prompt,N}, State) ->
    request({get_chars,latin1,Prompt,N}, State);
request({get_line,Prompt}, State) ->
    request({get_line,latin1,Prompt}, State);
request({get_until, Prompt,M,F,As}, State) ->
    request({get_until,latin1,Prompt,M,F,As}, State);
request(_Other, State) ->
    {error, {error, request}, State}.

multi_request([R|Rs], {ok, _Res, State}) ->
    multi_request(Rs, request(R, State));
multi_request([_|_], Error) ->
    Error;
multi_request([], Result) ->
    Result.

setopts(_Opts, State) ->
    {error,{error,enotsup},State}.

getopts(State) ->
    {error,{error,enotsup},State}.


get_chars(N, State) ->
    case can_read(N, State) of
        ok ->
            do_get_chars(N, ensure_fd(State));
        eof ->
            {ok, {error, eof}, State};
        wait ->
            {wait, State}
    end.

can_read(N, #state{size=Size, position=Position}) when Position + N =< Size ->
    ok;
can_read(_N, #state{final_size=FinalSize, position=Position}) when is_integer(FinalSize), Position >= FinalSize ->
    eof;
can_read(_N, _State) ->
    wait.


do_get_chars(N, {ok, #state{fd=Fd, position=Position} = State}) ->
    {ok, _} = file:position(Fd, Position),
    {ok, Data} = file:read(Fd, N),
    {ok, Data, State#state{position=Position+size(Data)}};
do_get_chars(_N, {error, Error, State}) ->
    {error, Error, State}.

ensure_fd(#state{fd=undefined, filename=Filename} = State) ->
    case file:open(Filename, [raw,binary]) of
        {ok, Fd} ->
            {ok, State#state{fd=Fd}};
        {error, _} = Error ->
            {error, Error, State}
    end;
ensure_fd(State) ->
    {ok, State}.
