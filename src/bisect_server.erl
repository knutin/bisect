%% @doc: gen_server wrapping an instance of bisect, owns the bisect
%% structure, serializes writes, hands out the reference to the bisect
%% structure to concurrent readers.
-module(bisect_server).
-behaviour(gen_server).

%% API
-export([start_link/2, start_link/3, start_link_with_data/3, stop/1]).
-export([get/2, first/1, last/1, next/2, next_nth/3, mget/2, mget_serial/2,
         insert/3, append/2, cas/4, inject/2, num_keys/1, delete/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-record(state, {b}).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

start_link_with_data(KeySize, ValueSize, Data) ->
    gen_server:start_link(?MODULE, [KeySize, ValueSize, Data], []).

start_link(KeySize, ValueSize) ->
    gen_server:start_link(?MODULE, [KeySize, ValueSize], []).

start_link(Name, KeySize, ValueSize) ->
    gen_server:start_link({local, Name}, ?MODULE, [KeySize, ValueSize], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

get(Pid, K) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:find(B, K)}.

first(Pid) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:first(B)}.

last(Pid) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:last(B)}.

next(Pid, K) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:next(B, K)}.

next_nth(Pid, K, Steps) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:next_nth(B, K, Steps)}.

mget(Pid, Keys) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:find_many(B, Keys)}.

mget_serial(Pid, Keys) ->
    gen_server:call(Pid, {mget, Keys}).

num_keys(Pid) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:num_keys(B)}.

insert(Pid, K, V) ->
    gen_server:call(Pid, {insert, K, V}).

append(Pid, KV) ->
    gen_server:call(Pid, {append, KV}).

cas(Pid, K, OldV, V) ->
    gen_server:call(Pid, {cas, K, OldV, V}).

inject(Pid, B) ->
    gen_server:call(Pid, {inject, B}).

delete(Pid, K) ->
    gen_server:call(Pid, {delete, K}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([KeySize, ValueSize]) ->
    {ok, #state{b = bisect:new(KeySize, ValueSize)}};

init([KeySize, ValueSize, Data]) ->
    {ok, #state{b = bisect:new(KeySize, ValueSize, Data)}}.

handle_call(get_b, _From, State) ->
    {reply, {ok, State#state.b}, State};

handle_call({insert, K, V}, _From, #state{b = B} = State) ->
    {reply, ok, State#state{b = bisect:insert(B, K, V)}};

handle_call({append, KV}, _From, #state{b = B} = State) ->
    {reply, ok, State#state{b = bisect:append(B, KV)}};

handle_call({inject, B}, _From, State) ->
    {reply, ok, State#state{b = B}};

handle_call({mget, Keys}, _From, State) ->
    {reply, {ok, bisect:find_many(State#state.b, Keys)}, State};

handle_call({delete, K}, _From, #state{b = B} = State) ->
    case catch bisect:delete(B, K) of
        {'EXIT', {badarg, _}} ->
            {reply, {error, badarg}, State};
        NewB ->
            {reply, ok, State#state{b = NewB}}
    end;

handle_call({cas, K, OldV, V}, _From, #state{b = B} = State) ->
    case catch bisect:cas(B, K, OldV, V) of
        {'EXIT', {badarg, _}} ->
            {reply, {error, badarg}, State};
        NewB ->
            {reply, ok, State#state{b = NewB}}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%
%% TESTS
%%

-ifdef(TEST).


insert_test() ->
    {ok, S} = start_link(8, 1),
    ok = insert(S, <<1:64/integer>>, <<1>>),
    ok = insert(S, <<2:64/integer>>, <<2>>),
    ok = insert(S, <<3:64/integer>>, <<3>>),

    Keys = [<<1:64/integer>>, <<2:64/integer>>, <<3:64/integer>>],
    Values = [<<1>>, <<2>>, <<3>>],
    ?assertEqual({ok, Values}, mget(S, Keys)),
    ?assertEqual({ok, Values}, mget_serial(S, Keys)).


cas_test() ->
    {ok, S} = start_link(8, 1),
    ok = insert(S, <<1:64/integer>>, <<1>>),
    {error, badarg} = cas(S, <<2:64/integer>>, <<2>>, <<2>>),
    ?assertEqual({ok, <<1>>}, get(S, <<1:64/integer>>)),

    ok = cas(S, <<1:64/integer>>, <<1>>, <<2>>),
    ?assertEqual({ok, <<2>>}, get(S, <<1:64/integer>>)),

    ok = cas(S, <<2:64/integer>>, not_found, <<2>>),
    ?assertEqual({ok, <<2>>}, get(S, <<2:64/integer>>)).



inject_test() ->
    {ok, S} = start_link(8, 1),
    KeyPairs = lists:map(fun (I) -> {<<I:64/integer>>, <<255>>} end,
                         lists:seq(1, 100000)),

    B = bisect:from_orddict(bisect:new(8, 1), KeyPairs),

    Key = <<20:64/integer>>,
    ?assertEqual({ok, not_found}, get(S, Key)),
    ok = inject(S, B),
    ?assertEqual({ok, <<255>>}, get(S, Key)).


proper_test() ->
    ?assert(proper:quickcheck(?MODULE:prop_bisect())).


-record(prop__state, {keys = []}).

prop_bisect() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
               begin
                   {ok, S} = start_link(prop, 8, 1),

                   {History,State,Result} = run_commands(?MODULE, Cmds),
                   stop(S),

                   ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                       [History, State, Result]),
                             aggregate(command_names(Cmds), Result =:= ok))
                end)).

prop__key() ->
    elements(prop__keys()).

prop__value() ->
    elements(prop__values()).

prop__keys() ->
    [<<1:64/integer>>, <<2:64/integer>>, <<3:64/integer>>].

prop__values() ->
    [<<1:8/integer>>, <<2:8/integer>>, <<3:8/integer>>].


command(_S) ->
    oneof([{call, ?MODULE, insert, [prop, prop__key(), prop__value()]},
           {call, ?MODULE, get, [prop, prop__key()]},
           {call, ?MODULE, mget, [prop, prop__keys()]},
           {call, ?MODULE, delete, [prop, prop__key()]}
          ]).

initial_state() ->
    #prop__state{keys = []}.

precondition(_, _) ->
    true.

next_state(S, _, {call, _, insert, [_, Key, Value]}) ->
    S#prop__state{keys = lists:keystore(Key, 1, S#prop__state.keys, {Key, Value})};

next_state(S, _, {call, _, delete, [_, Key]}) ->
    S#prop__state{keys = lists:keydelete(Key, 1, S#prop__state.keys)};

next_state(S, _, _) ->
    S.


postcondition(S, {call, _, get, [_, Key]}, {ok, not_found}) ->
    not lists:keymember(Key, 1, S#prop__state.keys);

postcondition(S, {call, _, get, [_, Key]}, {ok, Value}) ->
    case lists:keyfind(Key, 1, S#prop__state.keys) of
        {Key, Value} ->
            true;
        _ ->
            false
    end;

postcondition(S, {call, _, mget, [_, Keys]}, {ok, Values}) ->
    lists:all(
      fun (V) -> V =:= true end,
      lists:map(
        fun ({Key, not_found}) ->
                not lists:keymember(Key, 1, S#prop__state.keys);
            ({Key, Value}) ->
                {Key, Value} =:= lists:keyfind(Key, 1, S#prop__state.keys)
        end, lists:zip(Keys, Values)));


postcondition(S, {call, _, delete, [_, Key]}, ok) ->
    lists:keymember(Key, 1, S#prop__state.keys);

postcondition(S, {call, _, delete, [_, Key]}, {error, badarg}) ->
    not lists:keymember(Key, 1, S#prop__state.keys);

postcondition(_S, {call, _, insert, _}, _) ->
    true.

-endif.
