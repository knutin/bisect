-module(bisect).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

-define(i2k(I), <<I:64/integer>>).
-define(i2v(I), <<I:8/integer>>).
-define(b2i(B), list_to_integer(binary_to_list(B))).



new() ->
    <<>>.
%%     new(8, 1, 10).

%% new(KeySize, ValueSize, NumKeys) ->
%%     binary:copy(<<0:((KeySize+ValueSize) * 8)>>, NumKeys).


index(<<>>, _) ->
    0;
index(B, K) ->
    index(B, 0, size(B) div 9, K).

index(_B, Low, High, _K) when High < Low ->
    0;

index(B, Low, High, K) ->
    Mid = (Low + High) div 2,
%%     io:format("k: ~p, low: ~p, mid: ~p, high: ~p, size: ~p~n",
%%               [K, Low, Mid, High, size(B) div 9]),
%%     io:format("b: ~p~n", [B]),

    MidIndex = Mid * 9,

    case size(B) div 9 > Mid of
        true ->
            <<_:MidIndex/binary, MidKey:8/binary, _/binary>> = B,

            if
                MidKey > K ->
                    index(B, Low, Mid - 1, K);
                MidKey < K ->
                    index(B, Mid + 1, High, K);
                true ->
                    Mid+1
            end;
        false ->
            Mid
    end.



insert(<<>>, K, V) ->
    <<K/binary, V/binary>>;

insert(B, K, V) ->
    Index = index(B, K) * 9,
    RightIndex = size(B),

    case B of
        <<Left:Index/binary>> ->
            <<Left/binary, K/binary, V/binary>>;
        <<Left:Index/binary, Right:RightIndex/binary>> ->
            <<Left/binary, K/binary, V/binary, Right/binary>>
    end.


find(B, K) ->
    Index = (index(B, K) * 9) - 9,
    case B of
        <<_:Index/binary, K:8/binary, Value:1/binary, _/binary>> ->
            Value;
        _ ->
            not_found
    end.


from_orddict(Orddict) ->
    lists:foldl(fun ({K, V}, B) ->
                        <<B/binary, K:64/integer, V:8/integer>>
                end, new(), orddict:to_list(Orddict)).



%%
%% TEST
%%

insert_test() ->
    insert_many(new(), [{2, 2}, {3, 3}, {1, 1}]).

find_test() ->
    B = insert_many(new(), [{2, 2}, {3, 3}, {1, 1}]),
    ?assertEqual(<<3:8/integer>>, find(B, <<3:64/integer>>)).


find_non_existing_test() ->
    B = insert_many(new(), [{2, 2}, {3, 3}, {1, 1}]),
    ?assertEqual(not_found, find(B, ?i2k(4))).

simple_index_test() ->
    B = <<12398:64/integer, 255>>,
    ?assertEqual(1, index(B, <<12398:64/integer>>)).

find_many_test() ->
    B = insert_many(new(), [{2, 2}, {3, 3}, {1, 1}]),
    find_many(B, [<<1:64/integer>>, <<2:64/integer>>, <<3:64/integer>>]).





size_test() ->
    Start = 100000000000000,
    N = 100000,
    Spread = 1,
    KeyPairs = lists:map(fun (I) -> {I, 255} end,
                         lists:seq(Start, Start+(N*Spread), Spread)),

    B = from_orddict(KeyPairs),

    error_logger:info_msg("~p keys in ~p mb~n",
                          [N, byte_size(B) / 1024 / 1024]).


from_file(File) ->
    {ok, Terms} = file:read_file(File),
    KeyPairs = lists:map(fun (K) -> {?b2i(K), 255} end,
                         binary_to_term(Terms)),
    {ReadKeys, _} = lists:unzip(lists:sublist(KeyPairs, 1000)),

    B = from_orddict(lists:sort(KeyPairs)),
    io:format("size: ~p mb~n", [byte_size(B) / 1024 / 1024]),
    time_reads(B, length(KeyPairs), ReadKeys).

speed_test() ->
    Start = 100000000000000,
    N = 1000000,
    Spread = 1,
    KeyPairs = lists:map(fun (I) -> {I, 255} end,
                         lists:seq(Start, Start+(N*Spread), Spread)),

    ReadKeys = [random:uniform(Start) + Start || _ <- lists:seq(1, 1000)],

    B = from_orddict(KeyPairs),
    time_reads(B, N, ReadKeys).


time_reads(B, Size, ReadKeys) ->
    Parent = self(),
    spawn(
      fun() ->
              Timings =
                  lists:map(
                    fun (_) ->
                            StartTime = now(),
                            find_many(B, ReadKeys),
                            timer:now_diff(now(), StartTime)
                    end, lists:seq(1, 20)),

              Rps = 1000000 / ((lists:sum(Timings) / length(Timings)) / 1000),
              error_logger:info_msg("20 runs, ~p keys, "
                                    "average: ~p us, "
                                    "max: ~p us, rps ~.2f~n",
                                    [Size,
                                     lists:sum(Timings) / length(Timings),
                                     lists:max(Timings), Rps]),

              Parent ! done
      end),
    receive done -> ok after 1000 -> ok end.




insert_many(Bin, Pairs) ->
    lists:foldl(fun ({K, V}, B) when is_integer(K) andalso is_integer(V) ->
                        insert(B, ?i2k(K), ?i2v(V));
                    ({K, V}, B) ->
                        insert(B, K, V)
                end, Bin, Pairs).

find_many(B, Keys) ->
    lists:map(fun (K) ->
                      {K, find(B, K)}
              end, Keys).
