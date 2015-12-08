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
	mongodb_pid,
	mongodb_single_args,
	mongodb_replset,
	mongodb_rsConn,
	mongodb_singleConn,
	mongodb_dbName
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
init([[{Host,Port,MongoDatabase}]]=_MongoInfo) ->
	process_flag(trap_exit, true),
	{ok, #state{mongodb_single_args = {Host,Port},mongodb_dbName = MongoDatabase}};%%{ok,Pid}=mongoc:start_link(),
init([[{ReplSet,MongoDatabase}]]=_MongoInfo) ->
	process_flag(trap_exit, true),
	{ok, #state{mongodb_replset = ReplSet,mongodb_dbName = MongoDatabase}}.

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
%%----------------------------------------------------------
%%add
%%----------------------------------------------------------
handle_call(rs_connect,_From,State=#state{mongodb_rsConn = undefined,mongodb_pid = undefined,mongodb_replset = ReplSet,mongodb_dbName = MongoDatabase}) ->
	{ok,Pid}=mongoc:start_link(),
	case mongoc:rs_connect(Pid,ReplSet,[]) of
		 {ok,RsConn} ->
			 {reply, {ok, Pid, RsConn, MongoDatabase}, State#state{mongodb_rsConn = RsConn,mongodb_pid = Pid}}
	 end;
handle_call(rs_connect,_From,State=#state{mongodb_rsConn = RSConn,mongodb_pid = Pid,mongodb_dbName = MongoDatabase}) ->
	{reply, {ok, Pid,RSConn,MongoDatabase},State};
handle_call(connect, _From, State=#state{mongodb_pid = undefined}) ->
	{Host, Port} = State#state.mongodb_single_args,
	Database = State#state.mongodb_dbName,
	case mongoc:connect(Host, Port, list_to_binary(Database)) of
		{ok, Conn} ->
			{reply, {ok, Conn}, State#state{mongodb_singleConn = Conn}};
		{error, Error} ->
			{reply, {error, Error}, State}
	end;

handle_call(connect, _From, State=#state{mongodb_pid = Pid}) ->
	{reply, {ok, Pid}, State};

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
handle_info({'DOWN', _Ref, _Type, _Pid, _Info}, State) ->
	{noreply, State#state{mongodb_pid = undefined}};

handle_info({'EXIT', _Pid, _Info}, State) ->
	{noreply, State#state{mongodb_pid = undefined}};

handle_info(_Info, State) ->
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