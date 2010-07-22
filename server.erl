%% - Server module
%% - The server module creates a parallel registered process by spawning a process which
%% evaluates initialize().
%% The function initialize() does the following:
%%      1/ It makes the current process as a system process in order to trap exit.
%%      2/ It creates a process evaluating the store_loop() function.
%%      4/ It executes the server_loop() function.

-module(server).

-export([start/0]).

-record(db,
    {account,
        value = 0,
        writeTime = {0,0,0},
        readTime = {0,0,0}}).

-record(transactionData,
    {timeStamp,
        actions = [],
        dependencies = [], %[TimeStamp]
        oldValues = []}). %[{Account, Value, WriteTimeStamp}]

-record(transactionState,
    {transactions = [], %list of transactionData tuples
        storePid,
        serverPid}).

-record(storeState,
    {serverPid,
        transactionPid,
        database=[]}).%[db]

-record(serverState,
    {clientList = [],
        storePid,
        transactionPid}).

%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    register(transaction_server, spawn(fun() ->
                    process_flag(trap_exit, true),
                    Val= (catch initialize()),
                    io:format("Server terminated with:~p~n",[Val])
            end)).

initialize() ->
    process_flag(trap_exit, true),

    ServerPid = self(),

    %Prepare StoreState.
    StoreState = #storeState{serverPid = ServerPid,
        database=[#db{account=a},
            #db{account=b},
            #db{account=c},
            #db{account=d}]},
    StorePid = spawn_link(fun() -> store_init(StoreState) end),

    %Prepare TransactionState
    TransactionState = #transactionState{
        serverPid = ServerPid,
        storePid = StorePid},
    TransactionPid = spawn_link(fun() -> transaction_loop(TransactionState) end),

    %Tell the store who the transaction server is.
    StorePid ! {transactionPid, {TransactionPid, self()}},

    server_loop(#serverState{storePid = StorePid, transactionPid = TransactionPid}).
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store holding
%% the values of the global variable a, b, c and d
server_loop(State) ->
    receive
        {login, MM, Client} ->
            MM ! {ok, self()},
            io:format("New client has joined the server:~p.~n", [Client]),

            NewCL = add_client(Client, State#serverState.clientList),

            EndState = State#serverState{clientList = NewCL},
            server_loop(EndState);
        {close, Client} ->
            io:format("Client~p has left the server.~n", [Client]),
            State#serverState.storePid ! {print, self()},

            %get client timestamp
            TimeStamp = getTimeStamp(Client, State#serverState.clientList),

            %if we have a transaction going, we need to abort it!
            State#serverState.transactionPid ! {abort, {self(), TimeStamp}},

            NewCL = remove_client(Client, State#serverState.clientList),

            EndState = State#serverState{clientList = NewCL},
            server_loop(EndState);
        {request, Client} ->
            %Client is initializing a transaction here

            TimeStamp = now(),

            %update clientlist with timestamp
            NewCL = setTimeStamp(Client, TimeStamp, State#serverState.clientList),

            % tell the transaction server about a new transaction
            State#serverState.transactionPid ! {new, {self(), TimeStamp}},

            Client ! {proceed, self()},

            EndState = State#serverState{clientList = NewCL},
            server_loop(EndState);
        {confirm, Client} ->
            %get timestamp for client
            TimeStamp = getTimeStamp(Client, State#serverState.clientList),

            %Check with transaction server to se that the client is done.
            State#serverState.transactionPid ! {is_done, {self(), TimeStamp}},

            server_loop(State);
        {transaction_done, {Status, TimeStamp}} ->
            %Get correct client
            Client = getClient(TimeStamp, State#serverState.clientList),

            Client ! {Status, self()},
            server_loop(State);
        {action, Client, Act} ->
            io:format("Received~p from client~p~n", [Act, Client]),

            TimeStamp = getTimeStamp(Client, State#serverState.clientList),
            State#serverState.transactionPid ! {new_action, {self(), TimeStamp, Act}},

            server_loop(State)
    after 50000 ->
            case all_gone(State#serverState.clientList) of
                true -> exit(normal);
                false -> server_loop(State)
            end
    end.

%% - The values are maintained here
store_init(StoreState) ->
    ServerPid = StoreState#storeState.serverPid,
    receive
        {transactionPid, {TransactionPid, ServerPid}} ->
            %store TransactionPid in state
            NewStoreState = StoreState#storeState{transactionPid =
                TransactionPid},

            store_loop(NewStoreState)
    end.

store_loop(StoreState) ->
    ServerPid = StoreState#storeState.serverPid,
    TransactionPid = StoreState#storeState.transactionPid,
    receive
        {print, ServerPid} ->
            io:format("Database status:~n~p.~n",[StoreState#storeState.database]),
            store_loop(StoreState);
        {rollback, {TimeStamp, OldData}} ->
            % PERFORM COMPLETE ROLLBACK ATOMICLY
            NewDB = lists:foldl(
                fun({Account, Value, WriteTimeStamp}, DataBase) ->
                        case WriteTimeStamp == TimeStamp of
                            true ->
                                TempDB = updateDB(Account, Value, DataBase),
                                setAccountStamp(write, WriteTimeStamp, Account, TempDB);
                            false ->
                                DataBase
                        end
                end, StoreState#storeState.database, OldData),

            NewStoreState = StoreState#storeState{database=NewDB},
            store_loop(NewStoreState);
        {{write, Account, Value}, TimeStamp, TransactionPid} ->
            TempDB = updateDB(Account, Value, StoreState#storeState.database),
            DB = setAccountStamp(write, TimeStamp, Account, TempDB),

            NewState = StoreState#storeState{database=DB},
            store_loop(NewState);
        {{read, Account}, TimeStamp, TransactionPid} ->

            %when setting readtimestamp we need to do max of the old value and
            %the new values timestamps
            Data = lists:keyfind(Account, #db.account,
                StoreState#storeState.database),
            NewDB = setAccountStamp(read, erlang:max(Data#db.readTime,
                    TimeStamp), Account, StoreState#storeState.database),

            %respond with value, We do not do this since the client ignores it
            Data = lists:keyfind(Account, #db.account,
                StoreState#storeState.database),
            Value = Data#db.value,
            %StoreState#storeState.transactionPid ! {response, Value}

            io:format("Value of ~p is ~p~n", [Account, Value]),

            NewState = StoreState#storeState{database=NewDB},
            store_loop(NewState);
        {commited, {TimeStamp}} ->
            %if this is a "clean commit" we reset the writeStamp to avoid
            %getting our timestamps into a depenecy list, if we have already
            %been placed in a list the code inside of the transaction server
            %will solve that for us.
            NewDB = lists:map(
                fun(DB) ->
                        case DB#db.writeTime == TimeStamp of
                            true ->
                                DB#db{readTime = {0,0,0}};
                            false ->
                                DB
                        end
                end, StoreState#storeState.database),

            StoreState#storeState.transactionPid ! {response_commited, {ok}},

            NewState = StoreState#storeState{database=NewDB},
            io:format("Store loop: Transaction commited ~n~p~n", [NewState]),
            store_loop(NewState);
        {requestData, {Account}, TransactionPid} ->

            Data = lists:keyfind(Account, #db.account,
                StoreState#storeState.database),

            TransactionPid ! {response, {data, Data}},
            store_loop(StoreState);
        {requestTimeStamp, {Type, Account}, TransactionPid} ->
            TransactionPid ! {response, {timeStamp, getAccountStamp(Type, Account,
                        StoreState#storeState.database)}},
            store_loop(StoreState)
    end.

%% Process keeping track of the transactions.
transaction_loop(State) ->
    ServerPid = State#transactionState.serverPid,

    RequestStamp = fun (Type, Account) ->
            State#transactionState.storePid ! {requestTimeStamp, {Type, Account}, self()},
            receive
                {response, {timeStamp, Stamp}} -> Stamp
            end
    end,

    RequestData = fun (Account) ->
            State#transactionState.storePid ! {requestData, {Account}, self()},
            receive
                {response, {data, Data}} -> {Data#db.account, Data#db.value, Data#db.writeTime}
            end
    end,

    receive
        {new, {ServerPid, TimeStamp}} ->
            %create a new transaction and add it to the list of transactions.
            NewTransactionsData = #transactionData{timeStamp=TimeStamp},
            NewTransactions = [ NewTransactionsData | State#transactionState.transactions],
            EndState = State#transactionState{transactions = NewTransactions},
            transaction_loop(EndState);
        {is_done, {ServerPid, TimeStamp}} ->
            %get correct transactionData
            EndState =
            case getTransactionData(TimeStamp, State#transactionState.transactions) of
                aborted ->
                    %if we can not find TimeStamp it has been aborted! reply aborted
                    ServerPid ! {transaction_done, {abort, TimeStamp}},
                    State;
                TransactionData ->
                    %if actions and dependencies is empty then we are done
                    case TransactionData#transactionData.actions == [] andalso
                        TransactionData#transactionData.dependencies == [] of
                        true -> %Commit
                            %Tell the database that the we are commited
                            State#transactionState.storePid ! {commited, {TimeStamp}},
                            receive
                                {response_commited, {ok}} -> ok
                            end,

                            %Remove myself from all lists
                            TempTransactions = lists:delete(TransactionData,
                                State#transactionState.transactions),
                            NewTransactions = lists:map(fun(TransData) ->
                                        NewDep = lists:delete(TimeStamp,
                                            TransData#transactionData.dependencies),
                                        TransData#transactionData{dependencies = NewDep}
                                end,
                                TempTransactions),

                            ServerPid ! {transaction_done, {committed, TimeStamp}},

                            %return new state
                            State#transactionState{transactions=NewTransactions};
                        false ->
                            %Need to wait for dependencies so that they do not abort
                            %perform the check again by messaging ourself.
                            self() ! {is_done, {ServerPid, TimeStamp}},
                            State
                    end
            end,
            transaction_loop(EndState);
        {new_action, {ServerPid, TimeStamp, {write, Account, Value}}} ->
            NewState = case TimeStamp < RequestStamp(read, Account) of
                true ->
                    self() ! {abort, {ServerPid, TimeStamp}},
                    State;
                false ->
                    WriteStamp = RequestStamp(write, Account),
                    case TimeStamp >= WriteStamp of
                        true ->
                            %store old value in OldData
                            Data = RequestData(Account),

                            TransactionData = getTransactionData(TimeStamp,
                                State#transactionState.transactions),
                            NewOldValues = [Data |
                                TransactionData#transactionData.oldValues],

                            NewTransactionData =
                            TransactionData#transactionData{oldValues=NewOldValues},

                            NewTransactions = lists:keystore(TimeStamp,
                                #transactionData.timeStamp,
                                State#transactionState.transactions,
                                NewTransactionData),

                            %call to database to store the new value.
                            State#transactionState.storePid ! {{write,
                                    Account, Value}, TimeStamp, self()},

                            %New state
                            State#transactionState{transactions=NewTransactions};
                        false -> %If Thomas Write Rule
                            State
                    end
            end,
            transaction_loop(NewState);
        {new_action, {ServerPid, TimeStamp, {read, Account}}} ->
            WriteStamp = RequestStamp(write, Account),
            NewState = case TimeStamp < WriteStamp of
                true ->
                    self() ! {abort, {ServerPid, TimeStamp}},
                    State;
                false ->
                    %Updates the dependencies list
                    TransactionData = getTransactionData(TimeStamp,
                        State#transactionState.transactions),

                    %If the WriteStamp is either unwritten ({0,0,0}) or our own
                    %we do not want to add it to our dependencies
                    NewDependencies = case WriteStamp of
                        TimeStamp ->
                            TransactionData#transactionData.dependencies;
                        {0,0,0} ->
                            TransactionData#transactionData.dependencies;
                        _Else ->
                            [WriteStamp |
                                TransactionData#transactionData.dependencies]
                    end,

                    NewTransactionData =
                    TransactionData#transactionData{dependencies=NewDependencies},

                    NewTransactions = lists:keystore(TimeStamp,
                        #transactionData.timeStamp,
                        State#transactionState.transactions,
                        NewTransactionData),

                    %Tell db to read the value
                    State#transactionState.storePid ! {{read, Account}, TimeStamp, self()},

                    %New State
                    State#transactionState{transactions=NewTransactions}
            end,
            transaction_loop(NewState);
        {abort, {ServerPid, TimeStamp}} ->
            EndState = case getTransactionData(TimeStamp, State#transactionState.transactions) of
                aborted ->
                    State;
                TransactionData ->
                    %Tell db to restore old values
                    State#transactionState.storePid ! {rollback, {TimeStamp,
                            TransactionData#transactionData.oldValues}},

                    %Tell everyone else in dependencies to abort
                    lists:map(fun(DependentTimeStamp) ->
                                self() ! {abort, {ServerPid, DependentTimeStamp}}
                        end,
                        TransactionData#transactionData.dependencies),

                    %Remove myself from all lists
                    TempTransactions = lists:delete(TransactionData, State#transactionState.transactions),
                    NewTransactions = lists:map(fun(TransData) ->
                                NewDep = lists:delete(TimeStamp, TransData#transactionData.dependencies),
                                TransData#transactionData{dependencies = NewDep}
                        end,
                        TempTransactions),

                    %Return the new state
                    State#transactionState{transactions = NewTransactions}
            end,
            transaction_loop(EndState)
    end.

%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% - Low level function to handle lists
getTransactionData(TimeStamp, [TransactionData = #transactionData{timeStamp =
            TimeStamp} | _]) ->
    TransactionData;
getTransactionData(TimeStamp, [_ | Tail]) ->
    getTransactionData(TimeStamp, Tail);
getTransactionData(_,_) -> aborted.


add_client(C,T) -> [{C, idle}|T].

remove_client(_,[]) -> [];
remove_client(C, [{C, _}|T]) -> T;
remove_client(C, [H|T]) -> [H|remove_client(C,T)].

getTimeStamp(C, [{C, TimeStamp}|_]) -> TimeStamp;
getTimeStamp(C, [_|T]) -> getTimeStamp(C,T).

setTimeStamp(C, TimeStamp, [{C, _}|T]) -> [{C, TimeStamp}|T];
setTimeStamp(C, TimeStamp, [H|T]) -> [H|setTimeStamp(C,TimeStamp, T)].

getClient(TS, [{C, TS}|_]) -> C;
getClient(TS, [_|T]) -> getClient(TS,T).

all_gone([]) -> true;
all_gone(_) -> false.

updateDB(Account, Value, [Acc = #db{account=Account} | RestOfDatabase]) ->
    [ Acc#db{value = Value} | RestOfDatabase];
updateDB(Account, Value, [WrongAccount | RestOfDatabase]) ->
    [WrongAccount | updateDB(Account, Value, RestOfDatabase)].

%%Client list functions
%updateCL(transactionStart, Value, Client, ClientList) ->
%    {value, ClientTuple, TempCL} = lists:keytake(Client, #cl.cpid, ClientList),
%    [ClientTuple#cl{transactionStart = Value} | TempCL];
%updateCL(transactionStatus, Value, Client, ClientList) ->
%    {value, ClientTuple, TempCL} = lists:keytake(Client, #cl.cpid, ClientList),
%    [ClientTuple#cl{transactionStatus = Value} | TempCL].
%
getAccountStamp(write, Account, Database) ->
    Temp = lists:keyfind(Account, #db.account, Database),
    Temp#db.writeTime;
getAccountStamp(read, Account, Database) ->
    Temp = lists:keyfind(Account, #db.account, Database),
    Temp#db.readTime.

setAccountStamp(read, TimeStamp, Account, Database) ->
    {value, Temp, TempDB} = lists:keytake(Account, #db.account, Database),
    [Temp#db{readTime = erlang:max(Temp#db.readTime, TimeStamp)} | TempDB];
setAccountStamp(write, TimeStamp, Account, Database) ->
    {value, Temp, TempDB} = lists:keytake(Account, #db.account, Database),
    [Temp#db{writeTime = TimeStamp} | TempDB].
%
%findClient(TransactionStart, ClientList) ->
%    ClientTuple = lists:keyfind(TransactionStart, #cl.transactionStart, ClientList),
%    ClientTuple#cl.cpid.
