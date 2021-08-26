%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_persistent_session_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% SUITE boilerplate
%%--------------------------------------------------------------------

all() ->
    [ {group, kill_connection_process}
    , {group, no_kill_connection_process}
    ].

%% A persistent session can be resumed in two ways:
%%    1. The old connection process is still alive, and the session is taken
%%       over by the new connection.
%%    2. The old session process has died (e.g., because of node down).
%%       The new process resumes the session from the stored state, and finds
%%       any subscribed messages from the persistent message store.
%%
%% We want to test both these implementations, which is done through the top
%% level groups {no_}kill_connection_process.
%%
%% In addition, we test both tcp and quic connections for both scenarios.

groups() ->
    TCs = emqx_ct:all(?MODULE),
    [ {no_kill_connection_process, [], [{group, tcp}, {group, quic}]}
    , {   kill_connection_process, [], [{group, tcp}, {group, quic}]}
    , {tcp,  [], TCs}
    , {quic, [], TCs}
    ].

init_per_group(tcp, Config) ->
    emqx_ct_helpers:start_apps([]),
    [ {port, 1883}, {conn_fun, connect},  {kill_connection_process, true}| Config];
init_per_group(no_kill_connection_process, Config) ->
    emqx_ct_helpers:start_apps([]),
    [ {kill_connection_process, false} | Config];
init_per_group(kill_connection_process, Config) ->
    emqx_ct_helpers:start_apps([]),
    [ {kill_connection_process, true} | Config];
init_per_group(quic, Config) ->
    emqx_ct_helpers:start_apps([]),
    [ {port, 14567}, {conn_fun, quic_connect} | Config];
init_per_group(_, Config) ->
    emqx_ct_helpers:stop_apps([]),
    Config.

init_per_suite(Config) ->
    %% Start Apps
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([emqx], fun set_special_confs/1),
    Config.

set_special_confs(emqx) ->
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"));
set_special_confs(_) ->
    ok.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config);
        _ -> Config
    end.

end_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_info(Key, Client) ->
    maps:get(Key, maps:from_list(emqtt:info(Client)), undefined).

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count-1, [Msg|Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 1000 ->
        Msgs
    end.

maybe_kill_connection_process(ClientId, Config) ->
    case ?config(kill_connection_process, Config) of
        true ->
            [ConnectionPid] = emqx_cm:lookup_channels(ClientId),
            ?assert(is_pid(ConnectionPid)),
            exit(ConnectionPid, kill),
            ok;
        false ->
            ok
    end.

publish(Topic, Payloads = [_|_], Config)->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client} = emqtt:start_link([ {proto_ver, v5}
                                    | Config]),
    {ok, _} = emqtt:ConnFun(Client),
    lists:foreach(fun(Payload) ->
                          {ok, _} = emqtt:publish(Client, Topic, Payload, 2)
                  end, Payloads),
    ok = emqtt:disconnect(Client);
publish(Topic, Payload, Config) ->
    publish(Topic, [Payload], Config).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

%% [MQTT-3.1.2-23]
t_connect_session_expiry_interval(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = <<"t_connect_session_expiry_interval/foo">>,
    Payload = <<"test message">>,
    ClientId = <<"t_connect_session_expiry_interval">>,

    {ok, Client1} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 7200}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload, Config),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 7200}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    [Msg | _ ] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg)),
    ok = emqtt:disconnect(Client2).

t_without_client_id(Config) ->
    process_flag(trap_exit, true), %% Emqtt client dies
    ConnFun = ?config(conn_fun, Config),
    {ok, Client0} = emqtt:start_link([ {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 7200}},
                                       {clean_start, false}
                                     | Config]),
    {error, {client_identifier_not_valid, _}} = emqtt:ConnFun(Client0),
    ok.

t_assigned_clientid_persistent_session(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 7200}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),

    AssignedClientId = client_info(clientid, Client1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(AssignedClientId, Config),

    {ok, Client2} = emqtt:start_link([ {clientid, AssignedClientId},
                                       {proto_ver, v5},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(1, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_cancel_on_disconnect(Config) ->
    %% Open a persistent session, but cancel the persistence when
    %% shutting down the connection.
    ConnFun = ?config(conn_fun, Config),
    ClientId = <<"t_cancel_on_disconnect">>,

    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 0}),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {clean_start, false},
                                       {properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_persist_on_disconnect(Config) ->
    %% Open a non-persistent session, but add the persistence when
    %% shutting down the connection. This is a protocol error, and
    %% should not convert the session into a persistent session.
    ConnFun = ?config(conn_fun, Config),
    ClientId = <<"t_persist_on_disconnect">>,

    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 0}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),

    %% Strangely enough, the disconnect is reported as successful by emqtt.
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 16#FFFFFFFF}),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {clean_start, false},
                                       {properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    %% The session should not be known, since it wasn't persisted because of the
    %% changed expiry interval in the disconnect call.
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).


t_process_dies_session_expires(Config) ->
    %% Emulate an error in the connect process,
    %% or that the node of the process goes down.
    %% A persistent session should eventually expire.
    ConnFun = ?config(conn_fun, Config),
    ClientId = <<"t_process_dies_session_expires">>,
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 1}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    timer:sleep(1000),

    {ok, Client2} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    emqtt:disconnect(Client2).

t_publish_while_client_is_gone(Config) ->
    %% A persistent session should receive messages in its
    %% subscription even if the process owning the session dies.
    ConnFun = ?config(conn_fun, Config),
    Topic = <<"t_publish_while_client_is_gone/bar">>,
    STopic = <<"t_publish_while_client_is_gone/+">>,
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    ClientId = <<"t_publish_while_client_is_gone">>,
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ok = publish(Topic, [Payload1, Payload2], Config),

    {ok, Client2} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    [Msg1] = receive_messages(1),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload1)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),

    ok = emqtt:disconnect(Client2).

t_clean_start_drops_subscriptions(Config) ->
    %% 1. A persistent session is started and disconnected.
    %% 2. While disconnected, a message is published and persisted.
    %% 3. When connecting again, the clean start flag is set, the subscription is renewed,
    %%    then we disconnect again.
    %% 4. Finally, a new connection is made with clean start set to false.
    %% The original message should not be delivered.

    ConnFun = ?config(conn_fun, Config),
    Topic = <<"t_clean_start_drops_subscriptions/bar">>,
    STopic = <<"t_clean_start_drops_subscriptions/+">>,
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    Payload3 = <<"hello3">>,
    ClientId = <<"t_clean_start_drops_subscriptions">>,

    %% 1.
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    %% 2.
    ok = publish(Topic, Payload1, Config),

    %% 3.
    {ok, Client2} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    {ok, _, [2]} = emqtt:subscribe(Client2, STopic, qos2),

    ok = publish(Topic, Payload2, Config),
    [Msg1] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg1)),

    ok = emqtt:disconnect(Client2),
    maybe_kill_connection_process(ClientId, Config),

    %% 4.
    {ok, Client3} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client3),

    ok = publish(Topic, Payload3, Config),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload3)}, maps:find(payload, Msg2)),

    ok = emqtt:disconnect(Client3).
