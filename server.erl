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
    Initialvals = [
        #db{account=a},
        #db{account=b},
        #db{account=c},
        #db{account=d}
    ],
    ServerPid = self(),
    StorePid = spawn_link(fun() -> store_loop(ServerPid,Initialvals) end),
    TransactionPid = spawn_link(fun() -> transaction_loop(
                    #transactionState{serverPid = ServerPid, storePid = StorePid}) end),
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
            if
                TimeStamp /= idle ->
                    State#serverState.transactionPid ! {abort, {self(), TimeStamp}}
            end,

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
store_loop(ServerPid, Database) ->
    receive
        {print, ServerPid} ->
            io:format("Database status:~n~p.~n",[Database]),
            store_loop(ServerPid,Database);
        {rollback, {TimeStamp, OldData}} ->
            %TODO PERFORM COMPLETE ROLLBACK ATOMICLY
            store_loop(ServerPid, NewDB);
        {{write, Account, Value}, TransactionStart, ServerPid} ->
            %TODO !!
            %TODO Add a check that the asking client is transaction server!!
            store_loop(ServerPid, NewDB);
        {{read, Account}, TransactionStart, ServerPid} ->
            %TODO !!
            %TODO Add a check that the asking client is transaction server!!
            store_loop(ServerPid, NewDB)
    end.

%% Process keeping track of the transactions.
transaction_loop(State) ->
    ServerPid = State#transactionState.serverPid,
    receive
        {new, {ServerPid, TimeStamp}} ->
            %create a new transaction and add it to the list of transactions.
            NewTransactionsData = #transactionData{timeStamp=TimeStamp},
            NewTransactions = [ NewTransactionsData | State#transactionState.transactions],

            EndState = State#transactionState{transactions = NewTransactions},
            transaction_loop(EndState);
        {is_done, {ServerPid, TimeStamp}} ->
            %get correct transactionData
            EndState = case getTransactionData(TimeStamp, State#transactionState.transactions) of
                aborted ->
                %if we can not find TimeStamp it has been aborted! reply aborted
                    ServerPid ! {transaction_done, {aborted, TimeStamp}},
                    State;
                TransactionData ->
                    %if actions and dependencies is empty then we are done
                    case TransactionData#transactionData.actions == [] andalso
                        TransactionData#transactionData.dependencies == [] of
                        true ->
                            %Commit
                            %TODO remove TimeStamp from all transactions
                            %dependencies in transactionstate and update State
                            ServerPid ! {transaction_done, {committed, TimeStamp}},
                            State;
                        false ->
                            %Need to wait for dependencies so that they do not abort
                            %perform the check again by messaging ourself.
                            self() ! {is_done, {ServerPid, TimeStamp}},
                            State
                    end
            end,
            transaction_loop(EndState);
        {new_action, {ServerPid, TimeStamp, {write, Account, Value}}} ->
            %TODO implement
            % NewDB = case TransactionStart < getAccountStamp(write, Account, Database) of
            %     true -> Database; %Skip the write
            %     false ->
            %         case TransactionStart < getAccountStamp(read, Account, Database) of
            %             true ->
            %                 ServerPid ! {abortTransaction, TransactionStart},
            %                 Database;
            %             false ->
            %                 io:format("Storing new value ~p in account ~p",[Value, Account]),
            %                 TempDB = updateDB(Account, Value, Database),
            %                 DB = setAccountStamp(write, TransactionStart, Account, TempDB),
            %                 io:format(".. Stored!~n"),
            %                 DB
            %         end
            % end,
            transaction_loop(State);
        {new_action, {ServerPid, TimeStamp, {read, Account}}} ->
            %TODO implement
            %NewDB = case TransactionStart < getAccountStamp(write, Account, Database) of
            %    true ->
            %        ServerPid ! {abortTransaction, TransactionStart},
            %        Database;
            %    false ->
            %        Value = readDB(Account, Database),
            %        io:format("Value of ~p is ~p~n", [Account, Value]),
            %        %set readTimestamp
            %        setAccountStamp(read, TransactionStart, Account, Database)
            %end,
            transaction_loop(State);
        {abort, {ServerPid, TimeStamp}} ->
            EndState = case getTransactionData(TimeStamp, State#transactionState.transactions) of
                aborted ->
                    State;
                TransactionData ->
                    %Tell db to restore old values
                    State#transactionState.storePid ! {rollback, {TimeStamp,
                            TransactionData#transactionData.oldValues}},

                    %Tell everyone else in dependecies to abort
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
getClient(TS, [_|T]) -> getTimeStamp(TS,T).

all_gone([]) -> true;
all_gone(_) -> false.

readDB(Account, [Acc = #db{account=Account} | _]) -> Acc#db.value;
readDB(Account, [ _ | RestOfDB]) -> readDB(Account, RestOfDB).

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
%getAccountStamp(write, Account, Database) ->
%    Temp = lists:keyfind(Account, #db.account, Database),
%    Temp#db.writeTime;
%getAccountStamp(read, Account, Database) ->
%    Temp = lists:keyfind(Account, #db.account, Database),
%    Temp#db.readTime.
%
%setAccountStamp(read, TransactionStart, Account, Database) ->
%    {value, Temp, TempDB} = lists:keytake(Account, #db.account, Database),
%    [Temp#db{readTime = erlang:max(Temp#db.readTime, TransactionStart)} | TempDB];
%setAccountStamp(write, TransactionStart, Account, Database) ->
%    {value, Temp, TempDB} = lists:keytake(Account, #db.account, Database),
%    [Temp#db{writeTime = TransactionStart} | TempDB].
%
%findClient(TransactionStart, ClientList) ->
%    ClientTuple = lists:keyfind(TransactionStart, #cl.transactionStart, ClientList),
%    ClientTuple#cl.cpid.
