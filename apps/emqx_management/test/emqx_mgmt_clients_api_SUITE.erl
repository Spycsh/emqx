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
-module(emqx_mgmt_clients_api_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_clients(_) ->
    process_flag(trap_exit, true),

    Username1 = <<"user1">>,
    ClientId1 = <<"client1">>,

    Username2 = <<"user2">>,
    ClientId2 = <<"client2">>,

    Topic = <<"topic_1">>,
    Qos = 0,

    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),

    {ok, C1} = emqtt:start_link(#{username => Username1, clientid => ClientId1}),
    {ok, _}  = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link(#{username => Username2, clientid => ClientId2}),
    {ok, _}  = emqtt:connect(C2),

    timer:sleep(300),

    %% get /clients
    ClientsPath     = emqx_mgmt_api_test_util:api_path(["clients"]),
    {ok, Clients}   = emqx_mgmt_api_test_util:request_api(get, ClientsPath),
    ClientsResponse = emqx_json:decode(Clients, [return_maps]),
    ClientsMeta     = maps:get(<<"meta">>, ClientsResponse),
    ClientsPage     = maps:get(<<"page">>, ClientsMeta),
    ClientsLimit    = maps:get(<<"limit">>, ClientsMeta),
    ClientsCount    = maps:get(<<"count">>, ClientsMeta),
    ?assertEqual(ClientsPage, 1),
    ?assertEqual(ClientsLimit, emqx_mgmt:max_row_limit()),
    ?assertEqual(ClientsCount, 2),

    %% get /clients/:clientid
    Client1Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId1)]),
    {ok, Client1} = emqx_mgmt_api_test_util:request_api(get, Client1Path),
    Client1Response = emqx_json:decode(Client1, [return_maps]),
    ?assertEqual(Username1, maps:get(<<"username">>, Client1Response)),
    ?assertEqual(ClientId1, maps:get(<<"clientid">>, Client1Response)),

    %% delete /clients/:clientid kickout
    Client2Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId2)]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client2Path),
    timer:sleep(300),
    AfterKickoutResponse2 = emqx_mgmt_api_test_util:request_api(get, Client2Path),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, AfterKickoutResponse2),

    %% get /clients/:clientid/authz_cache should has no authz cache
    Client1AuthzCachePath = emqx_mgmt_api_test_util:api_path(["clients",
        binary_to_list(ClientId1), "authz_cache"]),
    {ok, Client1AuthzCache} = emqx_mgmt_api_test_util:request_api(get, Client1AuthzCachePath),
    ?assertEqual("[]", Client1AuthzCache),

    %% post /clients/:clientid/subscribe
    SubscribeBody = #{topic => Topic, qos => Qos},
    SubscribePath = emqx_mgmt_api_test_util:api_path(["clients",
        binary_to_list(ClientId1), "subscribe"]),
    {ok, _} =  emqx_mgmt_api_test_util:request_api(post, SubscribePath,
        "", AuthHeader, SubscribeBody),
    timer:sleep(100),
    [{{_, AfterSubTopic}, #{qos := AfterSubQos}}] = emqx_mgmt:lookup_subscriptions(ClientId1),
    ?assertEqual(AfterSubTopic, Topic),
    ?assertEqual(AfterSubQos, Qos),

    %% post /clients/:clientid/unsubscribe
    UnSubscribePath = emqx_mgmt_api_test_util:api_path(["clients",
        binary_to_list(ClientId1), "unsubscribe"]),
    {ok, _} =  emqx_mgmt_api_test_util:request_api(post, UnSubscribePath,
        "", AuthHeader, SubscribeBody),
    timer:sleep(100),
    ?assertEqual([], emqx_mgmt:lookup_subscriptions(Client1)),

    %% testcase cleanup, kickout client1
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client1Path),
    timer:sleep(300),
    AfterKickoutResponse1 = emqx_mgmt_api_test_util:request_api(get, Client1Path),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, AfterKickoutResponse1).

t_query_clients_with_time(_) ->
    process_flag(trap_exit, true),

    Username1 = <<"user1">>,
    ClientId1 = <<"client1">>,

    Username2 = <<"user2">>,
    ClientId2 = <<"client2">>,

    {ok, C1} = emqtt:start_link(#{username => Username1, clientid => ClientId1}),
    {ok, _}  = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link(#{username => Username2, clientid => ClientId2}),
    {ok, _}  = emqtt:connect(C2),

    timer:sleep(100),

    AuthHeader      = emqx_mgmt_api_test_util:auth_header_(),
    ClientsPath     = emqx_mgmt_api_test_util:api_path(["clients"]),
    %% get /clients with time(rfc3339)
    NowTimeStampInt = erlang:system_time(millisecond),
    %% Do not uri_encode `=` to `%3D`
    Rfc3339String   = emqx_http_lib:uri_encode(binary:bin_to_list(
        emqx_mgmt_api_clients:unix_ts_to_rfc3339_bin(NowTimeStampInt))),
    TimeStampString = emqx_http_lib:uri_encode(integer_to_list(NowTimeStampInt)),

    LteKeys         = ["lte_created_at=", "lte_connected_at="],
    GteKeys         = ["gte_created_at=", "gte_connected_at="],
    LteParamRfc3339 = [Param ++ Rfc3339String   || Param <- LteKeys],
    LteParamStamp   = [Param ++ TimeStampString || Param <- LteKeys],
    GteParamRfc3339 = [Param ++ Rfc3339String   || Param <- GteKeys],
    GteParamStamp   = [Param ++ TimeStampString || Param <- GteKeys],

    RequestResults  =
        [emqx_mgmt_api_test_util:request_api(get, ClientsPath, Param, AuthHeader)
                       || Param <- LteParamRfc3339 ++ LteParamStamp
            ++ GteParamRfc3339 ++ GteParamStamp],
    DecodedResults  = [emqx_json:decode(Response, [return_maps])
                       || {ok, Response} <- RequestResults],
    {LteResponseDecodeds, GteResponseDecodeds} = lists:split(4, DecodedResults),
    %% EachData :: list()
    [?assert( emqx_mgmt_api_clients:time_string_to_unix_ts_int(CreatedAt) < NowTimeStampInt)
     || #{<<"data">> := EachData} <- LteResponseDecodeds,
        #{<<"created_at">> := CreatedAt}     <- EachData],
    [?assert(emqx_mgmt_api_clients:time_string_to_unix_ts_int(ConnectedAt) < NowTimeStampInt)
     || #{<<"data">> := EachData} <- LteResponseDecodeds,
        #{<<"connected_at">> := ConnectedAt} <- EachData],
    [?assertEqual(EachData, [])
     || #{<<"data">> := EachData} <- GteResponseDecodeds],

    %% testcase cleanup, kickout client1 and client2
    Client1Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId1)]),
    Client2Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId2)]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client1Path),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client2Path).

t_keepalive(_Config) ->
    Username = "user_keepalive",
    ClientId = "client_keepalive",
    AuthHeader      = emqx_mgmt_api_test_util:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["clients", ClientId, "keepalive"]),
    Query = "interval=11",
    {error,{"HTTP/1.1",404,"Not Found"}} =
        emqx_mgmt_api_test_util:request_api(put, Path, Query, AuthHeader, <<"">>),
    {ok, C1} = emqtt:start_link(#{username => Username, clientid => ClientId}),
    {ok, _} = emqtt:connect(C1),
    {ok, Ok} = emqx_mgmt_api_test_util:request_api(put, Path, Query, AuthHeader, <<"">>),
    ?assertEqual("", Ok),
    [Pid] = emqx_cm:lookup_channels(list_to_binary(ClientId)),
    State = sys:get_state(Pid),
    ct:pal("~p~n", [State]),
    ?assertEqual(11000, element(2, element(5, element(11, State)))),
    emqtt:disconnect(C1),
    ok.
