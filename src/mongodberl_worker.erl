%%%-------------------------------------------------------------------
%%% @author yunba
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jul 2015 7:00 PM
%%%-------------------------------------------------------------------
-module(mongodberl_worker).
-author("yunba").

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
	mongodb_pid
}).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Args :: term()) ->
	{ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(MongoInfo) ->
	gen_server:start_link(?MODULE, [MongoInfo], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
	{ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term()} | ignore).
init([MongoInfo]) ->
	[{Host, Port, Database}] = MongoInfo,
	process_flag(trap_exit, true),
	{ok, Connection} = mongo:connect(Host, Port, list_to_binary(Database)),
	io:format("mongob init: ~p~n", [Connection]),
	{ok, #state{mongodb_pid = Connection}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
	State :: #state{}) ->
	{reply, Reply :: term(), NewState :: #state{}} |
	{reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
	{noreply, NewState :: #state{}} |
	{noreply, NewState :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
	{stop, Reason :: term(), NewState :: #state{}}).

handle_call({get, Item, Key}, _From, #state{mongodb_pid = Conn}=State) ->
	{Doc} = mongo:find_one(Conn, <<"apps">>, {<<"_id">>, to_bin(binary_to_list(Key))}),
	Reply = case bson:lookup(Item, Doc) of
		         {Value} ->
			         io:format("worker: ~p~n ~p~n", [Value, State#state.mongodb_pid]),
			         {true, Value};
		         _Else ->
			         io:format("find seckey from mongodb failed ~p, ~p~n", [Key, _Else]),
			         {false, _Else}
	         end,
	{reply, Reply, State};

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
	{noreply, NewState :: #state{}} |
	{noreply, NewState :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
	{noreply, NewState :: #state{}} |
	{noreply, NewState :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
	io:format("handle info, ~p~n", [_Info]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
	State :: #state{}) -> term()).
terminate(_Reason, _State) ->
	io:format("mongo terminate~n"),
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
	Extra :: term()) ->
	{ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
to_bin(L) ->
	to_bin(L, []).
to_bin([], Acc) ->
	iolist_to_binary(lists:reverse(Acc));
to_bin([C1, C2 | Rest], Acc) ->
	to_bin(Rest, [(dehex(C1) bsl 4) bor dehex(C2) | Acc]).

dehex(C) when C >= $0, C =< $9 ->
	C - $0;
dehex(C) when C >= $a, C =< $f ->
	C - $a + 10;
dehex(C) when C >= $A, C =< $F ->
	C - $A + 10.