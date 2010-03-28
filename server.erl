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

%Clientlist (cl) holds clinet information such as pid, transaction timestamp and transaction status
-record(cl,
  {cpid,
    transactionStart,
    transactionStatus = committed}).

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
  server_loop([],StorePid).
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store holding
%% the values of the global variable a, b, c and d
server_loop(ClientList,StorePid) ->
  receive
    {login, MM, Client} ->
      MM ! {ok, self()},
      io:format("New client has joined the server:~p.~n", [Client]),
      StorePid ! {print, self()},
      server_loop(add_client(Client,ClientList),StorePid);
    {close, Client} ->
      io:format("Client~p has left the server.~n", [Client]),
      StorePid ! {print, self()},
      server_loop(remove_client(Client,ClientList),StorePid);
    {request, Client} ->
      %Client is initializing a transaction here
      %Store initial transaction timestamp for Client
      TempCL = updateCL(transactionStart, now(), Client, ClientList),

      %we want to make sure that status is committed
      NewCL = updateCL(transactionStatus, committed, Client, TempCL),
      Client ! {proceed, self()},
      server_loop(NewCL,StorePid);
    {abortTransaction, TransactionStart} ->
      Client = findClient(TransactionStart, ClientList),
      io:format("Aborting transaction! Client: ~p~n", [Client]),
      NewCL = updateCL(transactionStatus, abort, Client, ClientList),
      io:format("Aborting transaction! Client: ~p~n", [NewCL]),
      server_loop(NewCL,StorePid);
    {confirm, Client} ->
      %Must wait until DB has proccessed the whole transaction we do this by
      %pinging the DB, if the DB replies then we know that it is done with the
      %transaction since erlang garantuees that our messages sent from the
      %server is inorderl. We can then check the status of transactionStatus.
      StorePid ! {ping, self()},
      receive
        {pong, StorePid} -> ok
      end,

      %Since we relied on the order of the messages, we need to make the commit
      %face a two step phase to allow the server to proccess any abort messages
      %that might be in the inbox, we do however know that the transaction has
      %been processed by the database and can therefore continue when we return
      %to phase 2 of the commit (realCommit)
      self() ! {realConfirm, Client},
      server_loop(ClientList,StorePid);
    {realConfirm, Client} ->
      %reply with value of transactionStatus (should be abort or committed)
      ClientTuple = lists:keyfind(Client, #cl.cpid, ClientList),
      io:format("Confirm, ClientTuple: ~p~n", [ClientTuple]),
      Client ! {ClientTuple#cl.transactionStatus, self()},
      server_loop(ClientList,StorePid);
    {action, Client, Act} ->
      io:format("Received~p from client~p~n", [Act, Client]),
      %Do the action (DB need the transaction timestamp)
      ClientTuple = lists:keyfind(Client, #cl.cpid, ClientList),
      StorePid ! {Act, ClientTuple#cl.transactionStart, self()},

      %Debugg print
      StorePid ! {print, self()},
      server_loop(ClientList,StorePid)
  after 50000 ->
      case all_gone(ClientList) of
        true -> exit(normal);
        false -> server_loop(ClientList,StorePid)
      end
  end.

%% - The values are maintained here
store_loop(ServerPid, Database) ->
  receive
    {print, ServerPid} ->
      io:format("Database status:~n~p.~n",[Database]),
      store_loop(ServerPid,Database);
    {ping, ServerPid} ->
      ServerPid ! {pong, self()},
      store_loop(ServerPid,Database);
    {{write, Account, Value}, TransactionStart, ServerPid} ->
      %TODO: if transactionTimestamp < readTimestamp -> abort (set transtactionStatus = abort)
      %TODO: if transactionTimestamp < writeTimestamp -> skip write
      %TODO: else proceed
      io:format("Storing new value ~p in account ~p",[Value, Account]),
      NewDB = updateDB(Account, Value, Database),
      %TODO: set writeTimestamp = now()
      io:format(".. Stored!~n"),
      store_loop(ServerPid, NewDB);
    {{read, Account}, TransactionStart, ServerPid} ->
      NewDB = case TransactionStart < getAccountStamp(write, Account, Database) of
        true ->
          ServerPid ! {abortTransaction, TransactionStart},
          Database;
        false ->
          Value = readDB(Account, Database),
          io:format("Value of ~p is ~p~n", [Account, Value]),
          %set readTimestamp
          setAccountStamp(read, Account, Database)
      end,
      store_loop(ServerPid, NewDB);
    {Action, ServerPid} ->
      io:format("Got Action ~p from server.~n", [Action])
  end.
%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% - Low level function to handle lists
add_client(C,T) -> [#cl{cpid = C}|T].

remove_client(_,[]) -> [];
remove_client(C, [#cl{cpid = C}|T]) -> T;
remove_client(C, [H|T]) -> [H|remove_client(C,T)].

all_gone([]) -> true;
all_gone(_) -> false.

readDB(Account, [Acc = #db{account=Account} | _]) -> Acc#db.value;
readDB(Account, [ _ | RestOfDB]) -> readDB(Account, RestOfDB).

updateDB(Account, Value, [Acc = #db{account=Account} | RestOfDatabase]) ->
  [ Acc#db{value = Value} | RestOfDatabase];
updateDB(Account, Value, [WrongAccount | RestOfDatabase]) ->
  [WrongAccount | updateDB(Account, Value, RestOfDatabase)].

%%Client list functions
updateCL(transactionStart, Value, Client, ClientList) ->
  {value, ClientTuple, TempCL} = lists:keytake(Client, #cl.cpid, ClientList),
  [ClientTuple#cl{transactionStart = Value} | TempCL];
updateCL(transactionStatus, Value, Client, ClientList) ->
  {value, ClientTuple, TempCL} = lists:keytake(Client, #cl.cpid, ClientList),
  [ClientTuple#cl{transactionStatus = Value} | TempCL].

getAccountStamp(write, Account, Database) ->
  Temp = lists:keyfind(Account, #db.account, Database),
  Temp#db.writeTime;
getAccountStamp(read, Account, Database) ->
  Temp = lists:keyfind(Account, #db.account, Database),
  Temp#db.readTime.

setAccountStamp(read, Account, Database) ->
  {value, Temp, TempDB} = lists:keytake(Account, #db.account, Database),
  [Temp#db{readTime = now()} | TempDB];
setAccountStamp(write, Account, Database) ->
  {value, Temp, TempDB} = lists:keytake(Account, #db.account, Database),
  [Temp#db{writeTime = now()} | TempDB].

findClient(TransactionStart, ClientList) ->
  ClientTuple = lists:keyfind(TransactionStart, #cl.transactionStart, ClientList),
  ClientTuple#cl.cpid.
