%%%-------------------------------------------------------------------
%%% @author yunba
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jul 2015 5:58 PM
%%%-------------------------------------------------------------------
-module(mongodberl).
-author("yunba").

-behaviour(supervisor).

%% API
-export([start_link/1, get_value_from_mongo/3, make_sure_binary/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-compile({parse_transform, lager_transform}).
-include_lib("elog/include/elog.hrl").

%%%===================================================================
%%% API functions
%%%===================================================================
get_value_from_mongo(PoolPid, Item, Key) ->
    execute(PoolPid, {get, Item, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Args :: term()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Args) ->
    io:format("mongodberl:start_link arg:~p~n", [Args]),
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Args]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore |
    {error, Reason :: term()}).
%%-----------------------------------------------
%%change Args to contain a Replset
%%------------------------------------------------
%%Param :: {repl,["host1:port1","host2:port2", "host3:port3"]}
%%["abj-mongorep-2.yunba.io:27017","abj-mongorep-1.yunba.io:27017","abj-mongo-1.yunba.io:22227017"]
init([{replset, Param}] = [_Args]) ->
    {PoolName, ReplSet, MongoDbDatabase, MongodbConnNum} = Param,
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Pools = [
        {PoolName, [
            {size, MongodbConnNum},
            {max_overflow, 30}
        ],
            [{replset, ReplSet, MongoDbDatabase}]
        }
    ],

    PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
        PoolArgs = [{name, {local, Name}},
            {worker_module, mongodberl_worker}] ++ SizeArgs,
        poolboy:child_spec(Name, PoolArgs, WorkerArgs)
                          end, Pools),

    {ok, {SupFlags, PoolSpecs}};
init([{single, Param}] = [_Args]) ->
    {PoolName, MongodbArg, MongoDbDatabase, MongodbConnNum} = Param,
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Pools = [
        {PoolName, [
            {size, MongodbConnNum},
            {max_overflow, 30}
        ],
            [{single, MongodbArg, MongoDbDatabase}]
        }
    ],

    PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
        PoolArgs = [{name, {local, Name}},
            {worker_module, mongodberl_worker}] ++ SizeArgs,
        poolboy:child_spec(Name, PoolArgs, WorkerArgs)
                          end, Pools),

    {ok, {SupFlags, PoolSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

execute(PoolPid, Cmd) ->
    poolboy:transaction(PoolPid, fun(Worker) ->
        Req = case gen_server:call(Worker, get_connection_type) of
                  {ok, replset} ->
                      rs_connect;
                  {ok, single} ->
                      connect
              end,
        case gen_server:call(Worker, Req) of
            {ok, Mongo} ->
                case Cmd of
                    {get, Item, Key} ->
                        try
                            Mongo:set_encode_style(mochijson),
                            {ok, Doc} = Mongo:findOne("apps", [{"_id", {oid, make_sure_binary(Key)}}]),
                            case proplists:get_value(make_sure_binary(Item), Doc) of
                                Value when is_binary(Value) ->
                                    ?DEBUG("worker: ~p~n", [Value]),
                                    {true, Value};
                                _Else ->
                                    ?ERROR("find ~p from mongodb failed ~p, ~p~n", [Item, Key, _Else]),
                                    {false, _Else}
                            end
                        catch _:X ->
                            {false, <<"fail">>, X}
                        end;
                    _ ->
                        {false, <<"cmd_not_supported">>}
                end;
            Error ->
                Error
        end
                                 end).


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


make_sure_binary(Data) ->
    if
        is_list(Data) ->
            list_to_binary(Data);
        is_integer(Data) ->
            integer_to_binary(Data);
        is_atom(Data) ->
            atom_to_binary(Data, latin1);
        true ->
            Data
    end.

