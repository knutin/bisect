%% @doc: gen_server wrapping an instance of bisect, owns the bisect
%% structure, serializes writes, hands out the reference to the bisect
%% structure to concurrent readers.
-module(bisect_server).
-behaviour(gen_server).

%% API
-export([start_link/2, start_link/3, stop/1]).
-export([get/2, mget/2, mget_serial/2,
         insert/3, inject/2, num_keys/1, delete/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-record(state, {b}).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).


%%%===================================================================
%%% API
%%%===================================================================

start_link(KeySize, ValueSize) ->
    gen_server:start_link(?MODULE, [KeySize, ValueSize], []).

start_link(Name, KeySize, ValueSize) ->
    gen_server:start_link({local, Name}, ?MODULE, [KeySize, ValueSize], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

get(Pid, K) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:find(B, K)}.

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

inject(Pid, B) ->
    gen_server:call(Pid, {inject, B}).

delete(Pid, K) ->
    gen_server:call(Pid, {delete, K}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([KeySize, ValueSize]) ->
    {ok, #state{b = bisect:new(KeySize, ValueSize)}}.


handle_call(get_b, _From, State) ->
    {reply, {ok, State#state.b}, State};

handle_call({insert, K, V}, _From, #state{b = B} = State) ->
    {reply, ok, State#state{b = bisect:insert(B, K, V)}};

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

%% skeleton

-ifdef(false).

proper_test() ->
    ?assert(proper:quickcheck(?MODULE:prop_bisect())).

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

command(_S) ->
    [].

initial_state() ->
    [].

precondition(_, _) ->
    true.

next_state(S, _, _) ->
    S.

postcondition(_, _, _) ->
    true.

-endif.

%%
%% insert only
%%

-ifdef(false).

proper_test() ->
    ?assert(proper:quickcheck(?MODULE:prop_bisect())).


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
    oneof([{call, ?MODULE, insert, [prop, prop__key(), prop__value()]}]).


-record(prop__state, {keys = []}).


initial_state() ->
    #prop__state{keys = []}.

precondition(_, _) ->
    true.

next_state(S, _, {call, _, insert, [_, Key, Value]}) ->
    S#prop__state{
      keys = lists:keystore(Key, 1, S#prop__state.keys, {Key, Value})}.


postcondition(_S, _, _) ->
    true.

-endif.


%%
%% insert and get
%%

-ifdef(MODULE).


proper_test() ->
    ?assert(proper:quickcheck(?MODULE:prop_bisect())).

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
           {call, ?MODULE, get, [prop, prop__key()]}
          ]).

-record(prop__state, {keys = []}).

initial_state() ->
    #prop__state{keys = []}.

precondition(_, _) ->
    true.

next_state(S, _, {call, _, insert, [_, Key, Value]}) ->
    S#prop__state{
      keys = lists:keystore(Key, 1, S#prop__state.keys, {Key, Value})};
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
postcondition(_, {call, _, insert, _}, _) ->
    true.

-endif.


-ifdef(false).


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
