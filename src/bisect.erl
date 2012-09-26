%% @doc: Space-efficient dictionary implemented using a binary
%%
%% This module implements a space-efficient dictionary with no
%% overhead per entry. Read and write access is O(log n).
%%
%% Keys and values are fixed size binaries stored ordered in a larger
%% binary which acts as a sparse array. All operations are implemented
%% using a binary search.
%%
%% As large binaries can be shared among processes, there can be
%% multiple concurrent readers of an instance of this structure.
%%
%% serialize/1 and deserialize/1
-module(bisect).
-author('Knut Nesheim <knutin@gmail.com>').

-export([new/2, insert/3, find/2, delete/2, compact/1]).
-export([serialize/1, deserialize/1, from_orddict/2, find_many/2]).
-export([expected_size/2, expected_size_mb/2, num_keys/1]).

-compile({no_auto_import, [size/1]}).
-compile(native).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%%
%% TYPES
%%

-type key_size()   :: pos_integer().
-type value_size() :: pos_integer().
-type block_size() :: pos_integer().

-type key()        :: binary().
-type value()      :: binary().

-type index()      :: pos_integer().

-record(bindict, {
          key_size   :: key_size(),
          value_size :: value_size(),
          block_size :: block_size(),
          b          :: binary()
}).
-type bindict() :: #bindict{}.


%%
%% API
%%

-spec new(key_size(), value_size()) -> bindict().
%% @doc: Returns a new empty dictionary where where the keys and
%% values will always be of the given size.
new(KeySize, ValueSize) when is_integer(KeySize) andalso is_integer(ValueSize) ->
    #bindict{key_size = KeySize,
             value_size = ValueSize,
             block_size = KeySize + ValueSize,
             b = <<>>}.


-spec insert(bindict(), key(), value()) -> bindict().
%% @doc: Inserts the key and value into the dictionary. If the size of
%% key and value is wrong, throws badarg. If the key is already in the
%% array, the value is updated.
insert(B, K, V) when byte_size(K) =/= B#bindict.key_size orelse
                     byte_size(V) =/= B#bindict.value_size ->
    erlang:error(badarg);

%% insert(#bindict{b = <<>>} = B, K, V) ->
%%     B#bindict{b = <<K/binary, V/binary>>};

insert(B, K, V) ->
    Index = index(B, K),
    LeftOffset = Index * B#bindict.block_size,
    RightOffset = byte_size(B#bindict.b) - LeftOffset,

    KeySize = B#bindict.key_size,
    ValueSize = B#bindict.value_size,

    %% case B#bindict.b of
    %%     <<Left:LeftOffset/binary, K:KeySize/binary, _:ValueSize/binary, Right/binary>> ->
    %%         B#bindict{b = <<Left/binary, K/binary, V/binary, Right/binary>>};

    %%     <<Left:LeftOffset/binary, Right:RightOffset/binary>> ->
    %%         B#bindict{b = <<Left/binary, K/binary, V/binary, Right/binary>>}
    %% end.
    B.


-spec find(bindict(), key()) -> value() | not_found.
%% @doc: Returns the value associated with the key or 'not_found' if
%% there is no such key.
find(B, K) ->
    Offset = index2offset(B, index(B, K)),
    KeySize = B#bindict.key_size,
    ValueSize = B#bindict.value_size,
    case B#bindict.b of
        <<_:Offset/binary, K:KeySize/binary, Value:ValueSize/binary, _/binary>> ->
            Value;
        _ ->
            not_found
    end.

-spec find_many(bindict(), [key()]) -> [value() | not_found].
find_many(B, Keys) ->
    lists:map(fun (K) -> find(B, K) end, Keys).

-spec delete(bindict(), key()) -> bindict().
delete(B, K) ->
    LeftOffset = index2offset(B, index(B, K)),
    KeySize = B#bindict.key_size,
    ValueSize = B#bindict.value_size,

    case B#bindict.b of
        <<Left:LeftOffset/binary, K:KeySize/binary, _:ValueSize/binary, Right/binary>> ->
            B#bindict{b = <<Left/binary, Right/binary>>};
        _ ->
            erlang:error(badarg)
    end.


%% @doc: Compacts the internal binary used for storage, by creating a
%% new copy where all the data is aligned in memory. Writes will cause
%% fragmentation.
compact(B) ->
    B#bindict{b = binary:copy(B#bindict.b)}.

%% @doc: Returns how many bytes would be used by the structure if it
%% was storing NumKeys.
expected_size(B, NumKeys) ->
    B#bindict.block_size * NumKeys.

expected_size_mb(B, NumKeys) ->
    expected_size(B, NumKeys) / 1024 / 1024.

-spec num_keys(bindict()) -> integer().
%% @doc: Returns the number of keys in the dictionary
num_keys(B) ->
    byte_size(B#bindict.b) div B#bindict.block_size.


-spec serialize(bindict()) -> binary().
%% @doc: Returns a binary representation of the dictionary which can
%% be deserialized later to recreate the same structure.
serialize(#bindict{} = B) ->
    term_to_binary(B).

-spec deserialize(binary()) -> bindict().
deserialize(Bin) ->
    case binary_to_term(Bin) of
        #bindict{} = B ->
            B;
        _ ->
            erlang:error(badarg)
    end.


%% @doc: Populates the dictionary with data from the orddict, taking
%% advantage of the fact that it is already ordered. The given bindict
%% must be empty, but contain size parameters.
from_orddict(#bindict{b = <<>>} = B, Orddict) ->
    KeySize = B#bindict.key_size,
    ValueSize = B#bindict.value_size,
    NewB = lists:foldl(fun ({K, V}, Bin) when byte_size(K) =:= B#bindict.key_size andalso
                                              byte_size(V) =:= B#bindict.value_size ->
                               <<Bin/binary, K:KeySize/binary, V:ValueSize/binary>>;
                           (_, _) ->
                               erlang:error(badarg)
                       end, B#bindict.b, orddict:to_list(Orddict)),
    B#bindict{b = NewB}.


%%
%% INTERNAL HELPERS
%%

index2offset(_, 0) -> 0;
index2offset(B, I) -> I * B#bindict.block_size.

%% @doc: Uses binary search to find the index of the given key. If the
%% key does not exist, the index where it should be inserted is
%% returned.
-spec index(bindict(), key()) -> index().
index(<<>>, _) ->
    0;
index(B, K) ->
    N = byte_size(B#bindict.b) div B#bindict.block_size,
    index(B, 0, N, K).

index(_B, Low, High, _K) when High =:= Low ->
    Low;

index(_B, Low, High, _K) when High < Low ->
    -1;

index(B, Low, High, K) ->
    Mid = (Low + High) div 2,
    MidOffset = index2offset(B, Mid),

    KeySize = B#bindict.key_size,
    case byte_size(B#bindict.b) > MidOffset of
        true ->
            <<_:MidOffset/binary, MidKey:KeySize/binary, _/binary>> = B#bindict.b,

            if
                MidKey > K ->
                    index(B, Low, Mid, K);
                MidKey < K ->
                    index(B, Mid + 1, High, K);
                MidKey =:= K ->
                    Mid
            end;
        false ->
            Mid
    end.


%%
%% TEST
%%
-ifdef(TEST).


-define(i2k(I), <<I:64/integer>>).
-define(i2v(I), <<I:8/integer>>).
-define(b2i(B), list_to_integer(binary_to_list(B))).

insert_test() ->
    insert_many(new(8, 1), [{2, 2}, {4, 4}, {1, 1}, {3, 3}]).

sorted_insert_test() ->
    B = insert_many(new(8, 1), [{1, 1}, {2, 2}, {3, 3}, {4, 4}]),
    ?assertEqual(<<1:64/integer, 1, 2:64/integer, 2,
                   3:64/integer, 3, 4:64/integer, 4>>, B#bindict.b).

index_test() ->
    B = #bindict{key_size = 8, value_size = 1, block_size = 9,
           b = <<0,0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,2,2>>},
    ?assertEqual(0, index(B, <<1:64/integer>>)),
    ?assertEqual(1, index(B, <<2:64/integer>>)),
    ?assertEqual(2, index(B, <<3:64/integer>>)),
    ?assertEqual(2, index(B, <<100:64/integer>>)).

find_test() ->
    B = insert_many(new(8, 1), [{2, 2}, {3, 3}, {1, 1}]),
    ?assertEqual(<<3:8/integer>>, find(B, <<3:64/integer>>)).

find_non_existing_test() ->
    B = insert_many(new(8, 1), [{2, 2}, {3, 3}, {1, 1}]),
    ?assertEqual(not_found, find(B, ?i2k(4))).

find_many_test() ->
    B = insert_many(new(8, 1), [{2, 2}, {3, 3}, {1, 1}]),
    find_many(B, [<<1:64/integer>>, <<2:64/integer>>, <<3:64/integer>>]).

insert_overwrite_test() ->
    B = insert_many(new(8, 1), [{2, 2}]),
    ?assertEqual(<<2>>, find(B, <<2:64/integer>>)),
    B2 = insert(B, <<2:64/integer>>, <<4>>),
    ?assertEqual(<<4>>, find(B2, <<2:64/integer>>)).


delete_test() ->
    B = insert_many(new(8, 1), [{2, 2}, {3, 3}, {1, 1}]),
    ?assertEqual(<<2:8/integer>>, find(B, ?i2k(2))),

    NewB = delete(B, ?i2k(2)),
    ?assertEqual(not_found, find(NewB, ?i2k(2))).

delete_non_existing_test() ->
    B = insert_many(new(8, 1), [{2, 2}, {3, 3}, {1, 1}]),
    ?assertError(badarg, delete(B, ?i2k(4))).


size_test() ->
    Start = 100000000000000,
    N = 1000,
    Spread = 1,
    KeyPairs = lists:map(fun (I) -> {I, 255} end,
                         lists:seq(Start, Start+(N*Spread), Spread)),

    B = insert_many(new(8, 1), KeyPairs),
    ?assertEqual(N+Spread, num_keys(B)).

serialize_test() ->
    KeyPairs = lists:map(fun (I) -> {I, 255} end, lists:seq(1, 100)),
    B = insert_many(new(8, 1), KeyPairs),
    ?assertEqual(B, deserialize(serialize(B))).

from_orddict_test() ->
    Orddict = orddict:from_list([{<<1:64/integer>>, <<255:8/integer>>}]),
    ?assertEqual(<<255>>, find(from_orddict(new(8, 1), Orddict), <<1:64/integer>>)).


speed_test_() ->
    {timeout, 600,
     fun() ->
             Start = 100000000000000,
             N = 10000,
             Keys = lists:seq(Start, Start+N),
             KeyValuePairs = lists:map(fun (I) -> {<<I:64/integer>>, <<255:8/integer>>} end,
                                       Keys),

             %% Will mostly be unique, if N is bigger than 10000
             ReadKeys = [lists:nth(random:uniform(N), Keys) || _ <- lists:seq(1, 1000)],
             B = from_orddict(new(8, 1), KeyValuePairs),
             time_reads(B, N, ReadKeys)
     end}.


insert_speed_test_() ->
    {timeout, 600,
     fun() ->
             Start = 100000000000000,
             N = 10000,
             Keys = lists:seq(Start, Start+N),
             KeyValuePairs = lists:map(fun (I) -> {<<I:64/integer>>, <<255:8/integer>>} end,
                                       Keys),
             ReadKeys = [lists:nth(random:uniform(N), Keys) || _ <- lists:seq(1, 1000)],

             StartTime = now(),
             B = lists:foldl(fun ({K, V}, B) ->
                                 insert(B, K, V)
                         end, new(8, 1), KeyValuePairs),
             ElapsedUs = timer:now_diff(now(), StartTime),
             error_logger:info_msg("insert in ~p ms, ~p us per key~n",
                                   [ElapsedUs / 1000,
                                    ElapsedUs / N
                                   ]),
             time_reads(B, N, ReadKeys)
     end}.


time_reads(B, Size, ReadKeys) ->
    Parent = self(),
    spawn(
      fun() ->
              Runs = 100,
              Timings =
                  lists:map(
                    fun (_) ->
                            StartTime = now(),
                            find_many(B, ReadKeys),
                            timer:now_diff(now(), StartTime)
                    end, lists:seq(1, Runs)),

              Rps = 1000000 / ((lists:sum(Timings) / length(Timings)) / length(ReadKeys)),
              error_logger:info_msg("Average over ~p runs, ~p keys in dict~n"
                                    "Average fetch ~p keys: ~p us, max: ~p us~n"
                                    "Average fetch 1 key: ~p us~n"
                                    "Theoretical sequential RPS: ~w~n",
                                    [Runs, Size, length(ReadKeys),
                                     lists:sum(Timings) / length(Timings),
                                     lists:max(Timings),
                                     (lists:sum(Timings) / length(Timings)) / length(ReadKeys),
                                     trunc(Rps)]),

              Parent ! done
      end),
    receive done -> ok after 1000 -> ok end.




insert_many(Bin, Pairs) ->
    lists:foldl(fun ({K, V}, B) when is_integer(K) andalso is_integer(V) ->
                        insert(B, ?i2k(K), ?i2v(V));
                    ({K, V}, B) ->
                        insert(B, K, V)
                end, Bin, Pairs).

-endif.
