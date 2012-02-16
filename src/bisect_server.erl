%% @doc: Multiplexes requests to the write caches
-module(bisect_server).
-behaviour(gen_server).

%% API
-export([start_link/2, start_link/3, get/2, mget/2, insert/3, inject/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-record(state, {b}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(KeySize, ValueSize) ->
    gen_server:start_link(?MODULE, [KeySize, ValueSize], []).

start_link(Name, KeySize, ValueSize) ->
    gen_server:start_link({local, Name}, ?MODULE, [KeySize, ValueSize], []).

get(Pid, K) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:find(B, K)}.

mget(Pid, Keys) ->
    {ok, B} = gen_server:call(Pid, get_b),
    {ok, bisect:find_many(B, Keys)}.

insert(Pid, K, V) ->
    gen_server:call(Pid, {insert, K, V}).

inject(Pid, B) ->
    gen_server:call(Pid, {inject, B}).

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
    {reply, ok, State#state{b = B}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


insert_test() ->
    {ok, S} = start_link(8, 1),
    ok = insert(S, <<1:64/integer>>, <<1>>),
    ok = insert(S, <<2:64/integer>>, <<2>>),
    ok = insert(S, <<3:64/integer>>, <<3>>),

    Keys = [<<1:64/integer>>, <<2:64/integer>>, <<3:64/integer>>],
    Values = [<<1>>, <<2>>, <<3>>],
    ?assertEqual({ok, Values}, mget(S, Keys)).


inject_test() ->
    {ok, S} = start_link(8, 1),
    KeyPairs = lists:map(fun (I) -> {<<I:64/integer>>, <<255>>} end,
                         lists:seq(1, 100000)),

    B = bisect:from_orddict(bisect:new(8, 1), KeyPairs),

    Key = <<20:64/integer>>,
    ?assertEqual({ok, not_found}, get(S, Key)),
    ok = inject(S, B),
    ?assertEqual({ok, <<255>>}, get(S, Key)).

-endif.
