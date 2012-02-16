-module(basho_bench_driver_bisect).

-export([new/1,
         run/4]).

new(_Id) ->
    case whereis(bisect_server) of
        undefined ->
            bisect_server:start_link(bisect_server, 8, 1);
        Pid ->
            {ok, Pid}
    end.

run(mget, KeyGen, _ValueGen, P) ->
    Start = KeyGen(),
    Keys = lists:seq(Start, Start+1000),

    case catch(bisect_server:mget(P, Keys)) of
        {ok, _Value} ->
            {ok, P};
        {error, Reason} ->
            {error, Reason, P};
        {'EXIT', {timeout, _}} ->
            {error, timeout, P}
    end;

run(put, KeyGen, ValueGen, Client) ->
    case catch(eredis:q(Client, ["SET", KeyGen(), ValueGen()], 100)) of
        {ok, <<"OK">>} ->
            {ok, Client};
        {error, Reason} ->
            {error, Reason, Client};
        {'EXIT', {timeout, _}} ->
            {error, timeout, Client}
    end.
