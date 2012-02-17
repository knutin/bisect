-module(basho_bench_driver_bisect).

-export([new/1,
         run/4]).

new(_Id) ->
    case basho_bench_config:get(singleton) of
        true ->
            case whereis(bisect_server) of
                undefined ->
                    {ok, P} = bisect_server:start_link(bisect_server, 8, 1),
                    ok = bisect_server:inject(P, initial_b()),
                    {ok, P};
                Pid ->
                    {ok, Pid}
            end;
        false ->
            {ok, P} = bisect_server:start_link(8, 1),
            ok = bisect_server:inject(P, initial_b()),
            {ok, P}
    end.


initial_b() ->
    N = basho_bench_config:get(initial_keys),
    KeyValuePairs = lists:map(fun (I) -> {<<I:64/integer>>, <<255:16/integer>>} end,
                              lists:seq(1, N)),
    bisect:from_orddict(bisect:new(8, 2), KeyValuePairs).


run(mget, KeyGen, _ValueGen, P) ->
    NumKeys = basho_bench_config:get(mget_keys),
    StartKey = KeyGen(),
    Keys = [<<I:64/integer>> || I <- lists:seq(StartKey, StartKey + (NumKeys * 1000), 1000)],

    case catch(bisect_server:mget(P, Keys)) of
        {ok, _Value} ->
            {ok, P};
        {error, Reason} ->
            {error, Reason, P};
        {'EXIT', {timeout, _}} ->
            {error, timeout, P}
    end;

run(mget_serial, KeyGen, _ValueGen, P) ->
    NumKeys = basho_bench_config:get(mget_keys),
    StartKey = KeyGen(),
    Keys = [<<I:64/integer>> || I <- lists:seq(StartKey, StartKey + (NumKeys * 1000), 1000)],

    case catch(bisect_server:mget_serial(P, Keys)) of
        {ok, _Value} ->
            {ok, P};
        {error, Reason} ->
            {error, Reason, P};
        {'EXIT', {timeout, _}} ->
            {error, timeout, P}
    end;

run(put, KeyGen, ValueGen, P) ->
    case catch(bisect_server:insert(P, <<(KeyGen()):64/integer>>, ValueGen())) of
        ok ->
            {ok, P};
        {error, Reason} ->
            {error, Reason, P};
        {'EXIT', {timeout, _}} ->
            {error, timeout, P}
    end.
