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

-module(emqx_delayed_SUITE).

-import(emqx_delayed, [on_message_publish/1]).

-compile(export_all).
-compile(nowarn_export_all).

-record(delayed_message, {key, delayed, msg}).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    mria:start(),
    ok = emqx_delayed:mnesia(boot),
    emqx_common_test_helpers:start_apps([emqx_modules]),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_modules]).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_load_case(_) ->
    Hooks = emqx_hooks:lookup('message.publish'),
    MFA = {emqx_delayed,on_message_publish,[]},
    ?assertEqual(false, lists:keyfind(MFA, 2, Hooks)),
    ok = emqx_delayed:enable(),
    Hooks1 = emqx_hooks:lookup('message.publish'),
    ?assertNotEqual(false, lists:keyfind(MFA, 2, Hooks1)),
    ok.

t_delayed_message(_) ->
    ok = emqx_delayed:enable(),
    DelayedMsg = emqx_message:make(?MODULE, 1, <<"$delayed/1/publish">>, <<"delayed_m">>),
    ?assertEqual({stop, DelayedMsg#message{topic = <<"publish">>, headers = #{allow_publish => false}}}, on_message_publish(DelayedMsg)),

    Msg = emqx_message:make(?MODULE, 1, <<"no_delayed_msg">>, <<"no_delayed">>),
    ?assertEqual({ok, Msg}, on_message_publish(Msg)),

    [Key] = mnesia:dirty_all_keys(emqx_delayed),
    [#delayed_message{msg = #message{payload = Payload}}] = mnesia:dirty_read({emqx_delayed, Key}),
    ?assertEqual(<<"delayed_m">>, Payload),
    timer:sleep(5000),

    EmptyKey = mnesia:dirty_all_keys(emqx_delayed),
    ?assertEqual([], EmptyKey),
    ok = emqx_delayed:disable().
