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
      Client ! {proceed, self()},
      server_loop(ClientList,StorePid);
    {confirm, Client} ->
      Client ! {abort, self()},
      server_loop(ClientList,StorePid);
    {action, Client, Act} ->
      io:format("Received~p from client~p.~n", [Act, Client]),

      %TODO: Add transaction management here

      %Do the action
      StorePid ! {Act, self()},
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
    {{write, Account, Value}, ServerPid} ->
      io:format("Storing new value ~p in account ~p",[Value, Account]),
      NewDB = updateDB(Account, Value, Database),
      io:format(".. Stored!~n"),
      store_loop(ServerPid, NewDB);
    {{read, Account}, ServerPid} ->
      Value = readDB(Account, Database),
      io:format("Value of ~p is ~p~n", [Account, Value]),
      store_loop(ServerPid, Database);
    {Action, ServerPid} ->
      io:format("Got Action ~p from server.~n", [Action])
  end.
%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%% - Low level function to handle lists
add_client(C,T) -> [C|T].

remove_client(_,[]) -> [];
remove_client(C, [C|T]) -> T;
remove_client(C, [H|T]) -> [H|remove_client(C,T)].

all_gone([]) -> true;
all_gone(_) -> false.

readDB(Account, [Acc = #db{account=Account} | _]) -> Acc#db.value;
readDB(Account, [ _ | RestOfDB]) -> readDB(Account, RestOfDB).


updateDB(Account, Value, [Acc = #db{account=Account} | RestOfDatabase]) ->
  [ Acc#db{value = Value} | RestOfDatabase];
updateDB(Account, Value, [WrongAccount | RestOfDatabase]) ->
  [WrongAccount | updateDB(Account, Value, RestOfDatabase)].
