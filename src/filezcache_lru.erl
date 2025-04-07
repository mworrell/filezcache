%% @author Dmitry Poroh
%% @copyright Copyright (c) 2018 Dmitry Poroh
%% @doc Erlang LRU cache/Queue with deletion by key.
%%
%% Motivation: For many purposes we need FIFO of elements with
%% fast-enough deletion from the middle. This FIFO also can be used as
%% LRU cache (Least Recently Used Cache).
%%
%% @end

%% All rights reserved.
%% Distributed under the terms of the MIT License. See the LICENSE-MIT file.

-module(filezcache_lru).

%% API exports
-export([new/0,
         new/1,
         add/3,
         push/3,
         pop/1,
         empty/1,
         size/1,
         take/2,
         has_key/2,
         lookup_and_update/2
        ]).
-export_type([lru/0]).

%%====================================================================
%% Types
%%====================================================================

-type key() :: any().
-type value() :: any().

-record(lru, {max_size     = unlimited :: non_neg_integer() | unlimited,
              current_size = 0         :: non_neg_integer(),
              lookup_cache = #{}       :: #{key() => {non_neg_integer(), value()}},
              usage = gb_trees:empty() :: gb_trees:tree(non_neg_integer(), key()),
              last  = 0
             }).

-type lru() :: #lru{}.

%%====================================================================
%% API functions
%%====================================================================

%% @doc Create new unlimited cache.
%%
%% Complexity: O(1)
-spec new() -> lru().
new() ->
    #lru{}.

%% @doc Create new limited cache with defined max size
%%
%% Complexity: O(1)
-spec new(MaxSize :: pos_integer()) -> lru().
new(MaxSize) ->
    #lru{max_size = MaxSize}.

%% @doc Add new element to cache with defined key.
%%
%% Complexity: O(log(N))
-spec add(key(), value(), lru()) -> lru().
add(Key, Value, LRU) ->
    NewLRU = add_item(Key, Value, LRU),
    apply_limits(NewLRU).

%% @doc Push element to queue tail.
%%
%% Complexity: O(log(N))
-spec push(key(), value(), lru()) -> lru().
push(Key, Value, LRU) ->
    add(Key, Value, LRU).

%% @doc Pop element from head.
%%
%% Complexity: O(log(N))
-spec pop(lru()) -> {key(), value(), lru()} | error.
pop(LRU) ->
    pop_oldest(LRU).

%% @doc Take element from queue by key
%%
%% Complexity: O(log(N))
-spec take(key(), lru()) -> {value(), lru()} | error.
take(Key, LRU) ->
    #lru{lookup_cache = LookupCache, usage = Usage, last = Last, current_size = CurrentSize} = LRU,
    case maps:take(Key, LookupCache) of
        error ->
            error;
        {{Index, Value}, NewLookupCache} ->
            NewLast = case CurrentSize of
                          1 -> 0;
                          _ -> Last
                      end,
            NewUsage = gb_trees:delete(Index, Usage),
            {Value,
             LRU#lru{lookup_cache = NewLookupCache,
                     usage = NewUsage,
                     current_size = CurrentSize - 1,
                     last = NewLast
                    }}
    end.

%% @doc Check if queue is empty.
%%
%% Complexity: O(1)
-spec empty(lru()) -> boolean().
empty(#lru{current_size = 0}) ->
    true;
empty(#lru{}) ->
    false.

%% @doc Get number of elements in the LRU cache.
%% Complexity: O(1)
-spec size(lru()) -> non_neg_integer().
size(#lru{current_size = S}) ->
    S.

%% @doc Check if cache has element with key.
%%
%% Complexity: O(1)
-spec has_key(key(), lru()) -> boolean().
has_key(Key, #lru{lookup_cache = Cache}) ->
    maps:is_key(Key, Cache).

%% @doc Lookup and update cache (LRU function).
%%
%% Complexity: O(log(N))
-spec lookup_and_update(key(), lru()) -> {ok, value(), lru()} | error.
lookup_and_update(Key, #lru{lookup_cache = Cache} = LRU) ->
    case maps:find(Key, Cache) of
        {ok, {_, Value}} ->
            {ok, Value, update_key(Key, Value, LRU)};
        error ->
            error
    end.

%%====================================================================
%% Internal functions
%%====================================================================

-spec add_item(key(), value(), lru()) -> lru().
add_item(Key, Value, LRU) ->
    case has_key(Key, LRU) of
        true ->
            update_key(Key, Value, LRU);
        false ->
            add_new_item(Key, Value, LRU)
    end.


-spec add_new_item(key(), value(), lru()) -> lru().
add_new_item(Key, Value, LRU) ->
    #lru{current_size = CurrentSize, lookup_cache = LookupCache, usage = Usage, last = Last} = LRU,
    Index = Last + 1,
    LRU#lru{current_size = CurrentSize + 1,
            lookup_cache = LookupCache#{Key => {Index, Value}},
            usage        = gb_trees:enter(Index, Key, Usage),
            last         = Index
           }.

-spec update_key(key(), value(), lru()) -> lru().
update_key(Key, Value, LRU) ->
    #lru{lookup_cache = LookupCache, usage = Usage, last = Last} = LRU,
    #{Key := {CurrentIndex, _}} = LookupCache,
    UsageWithoutCurrent = gb_trees:delete(CurrentIndex, Usage),
    NewIndex = Last + 1,
    LRU#lru{lookup_cache = LookupCache#{Key => {NewIndex, Value}},
            usage        = gb_trees:enter(NewIndex, Key, UsageWithoutCurrent),
            last         = NewIndex
           }.

-spec pop_oldest(lru()) -> {key(), value(), lru()} | error.
pop_oldest(LRU) ->
    #lru{current_size = CurrentSize, usage = Usage, lookup_cache = LookupCache, last = Last } = LRU,
    case gb_trees:is_empty(Usage) of
        true ->
            error;
        false ->
            {_, OldestKey, NewUsage}  = gb_trees:take_smallest(Usage),
            {{_, OldestValue}, NewLookupCache} = maps:take(OldestKey, LookupCache),
            NewLast =
                case CurrentSize of
                    1 -> 0;
                    _ -> Last
                end,
            NewLRU = LRU#lru{current_size = CurrentSize - 1,
                             lookup_cache = NewLookupCache,
                             usage        = NewUsage,
                             last         = NewLast
                            },
            {OldestKey, OldestValue, NewLRU}
    end.

-spec apply_limits(lru()) ->  lru().
apply_limits(#lru{max_size = unlimited} = LRU) ->
    LRU;
apply_limits(#lru{current_size = CS, max_size = MaxSize} = LRU) when CS =< MaxSize ->
    LRU;
apply_limits(LRU) ->
    case pop_oldest(LRU) of
        error -> LRU;
        {_, _, NewLRU} -> apply_limits(NewLRU)
    end.
