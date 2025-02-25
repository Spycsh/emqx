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

-module(emqx_rule_runtime).

-include("rule_engine.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ apply_rule/2
        , apply_rules/2
        , clear_rule_payload/0
        ]).

-import(emqx_rule_maps,
        [ nested_get/2
        , range_gen/2
        , range_get/3
        ]).

-compile({no_auto_import,[alias/1]}).

-type input() :: map().
-type alias() :: atom().
-type collection() :: {alias(), [term()]}.

-define(ephemeral_alias(TYPE, NAME),
    iolist_to_binary(io_lib:format("_v_~ts_~p_~p", [TYPE, NAME, erlang:system_time()]))).

-define(ActionMaxRetry, 3).

%%------------------------------------------------------------------------------
%% Apply rules
%%------------------------------------------------------------------------------
-spec(apply_rules(list(rule()), input()) -> ok).
apply_rules([], _Input) ->
    ok;
apply_rules([#{enabled := false}|More], Input) ->
    apply_rules(More, Input);
apply_rules([Rule = #{id := RuleID}|More], Input) ->
    try apply_rule_discard_result(Rule, Input)
    catch
        %% ignore the errors if select or match failed
        _:{select_and_transform_error, Error} ->
            ?SLOG(warning, #{msg => "SELECT_clause_exception",
                             rule_id => RuleID, reason => Error});
        _:{match_conditions_error, Error} ->
            ?SLOG(warning, #{msg => "WHERE_clause_exception",
                             rule_id => RuleID, reason => Error});
        _:{select_and_collect_error, Error} ->
            ?SLOG(warning, #{msg => "FOREACH_clause_exception",
                             rule_id => RuleID, reason => Error});
        _:{match_incase_error, Error} ->
            ?SLOG(warning, #{msg => "INCASE_clause_exception",
                             rule_id => RuleID, reason => Error});
        Class:Error:StkTrace ->
            ?SLOG(error, #{msg => "apply_rule_failed",
                           rule_id => RuleID,
                           exception => Class,
                           reason => Error,
                           stacktrace => StkTrace
                          })
    end,
    apply_rules(More, Input).

apply_rule_discard_result(Rule, Input) ->
    _ = apply_rule(Rule, Input),
    ok.

apply_rule(Rule = #{id := RuleID}, Input) ->
    clear_rule_payload(),
    do_apply_rule(Rule, add_metadata(Input, #{rule_id => RuleID})).

do_apply_rule(#{
            id := RuleId,
            is_foreach := true,
            fields := Fields,
            doeach := DoEach,
            incase := InCase,
            conditions := Conditions,
            outputs := Outputs
        }, Input) ->
    {Selected, Collection} = ?RAISE(select_and_collect(Fields, Input),
                                        {select_and_collect_error, {_EXCLASS_,_EXCPTION_,_ST_}}),
    ColumnsAndSelected = maps:merge(Input, Selected),
    case ?RAISE(match_conditions(Conditions, ColumnsAndSelected),
                {match_conditions_error, {_EXCLASS_,_EXCPTION_,_ST_}}) of
        true ->
            ok = emqx_plugin_libs_metrics:inc_matched(rule_metrics, RuleId),
            Collection2 = filter_collection(Input, InCase, DoEach, Collection),
            {ok, [handle_output_list(Outputs, Coll, Input) || Coll <- Collection2]};
        false ->
            {error, nomatch}
    end;

do_apply_rule(#{id := RuleId,
                is_foreach := false,
                fields := Fields,
                conditions := Conditions,
                outputs := Outputs
            }, Input) ->
    Selected = ?RAISE(select_and_transform(Fields, Input),
                      {select_and_transform_error, {_EXCLASS_,_EXCPTION_,_ST_}}),
    case ?RAISE(match_conditions(Conditions, maps:merge(Input, Selected)),
                {match_conditions_error, {_EXCLASS_,_EXCPTION_,_ST_}}) of
        true ->
            ok = emqx_plugin_libs_metrics:inc_matched(rule_metrics, RuleId),
            {ok, handle_output_list(Outputs, Selected, Input)};
        false ->
            {error, nomatch}
    end.

clear_rule_payload() ->
    erlang:erase(rule_payload).

%% SELECT Clause
select_and_transform(Fields, Input) ->
    select_and_transform(Fields, Input, #{}).

select_and_transform([], _Input, Output) ->
    Output;
select_and_transform(['*'|More], Input, Output) ->
    select_and_transform(More, Input, maps:merge(Output, Input));
select_and_transform([{as, Field, Alias}|More], Input, Output) ->
    Val = eval(Field, Input),
    select_and_transform(More,
        nested_put(Alias, Val, Input),
        nested_put(Alias, Val, Output));
select_and_transform([Field|More], Input, Output) ->
    Val = eval(Field, Input),
    Key = alias(Field),
    select_and_transform(More,
        nested_put(Key, Val, Input),
        nested_put(Key, Val, Output)).

%% FOREACH Clause
-spec select_and_collect(list(), input()) -> {input(), collection()}.
select_and_collect(Fields, Input) ->
    select_and_collect(Fields, Input, {#{}, {'item', []}}).

select_and_collect([{as, Field, {_, A} = Alias}], Input, {Output, _}) ->
    Val = eval(Field, Input),
    {nested_put(Alias, Val, Output), {A, ensure_list(Val)}};
select_and_collect([{as, Field, Alias}|More], Input, {Output, LastKV}) ->
    Val = eval(Field, Input),
    select_and_collect(More,
        nested_put(Alias, Val, Input),
        {nested_put(Alias, Val, Output), LastKV});
select_and_collect([Field], Input, {Output, _}) ->
    Val = eval(Field, Input),
    Key = alias(Field),
    {nested_put(Key, Val, Output), {'item', ensure_list(Val)}};
select_and_collect([Field|More], Input, {Output, LastKV}) ->
    Val = eval(Field, Input),
    Key = alias(Field),
    select_and_collect(More,
        nested_put(Key, Val, Input),
        {nested_put(Key, Val, Output), LastKV}).

%% Filter each item got from FOREACH
filter_collection(Input, InCase, DoEach, {CollKey, CollVal}) ->
    lists:filtermap(
        fun(Item) ->
            InputAndItem = maps:merge(Input, #{CollKey => Item}),
            case ?RAISE(match_conditions(InCase, InputAndItem),
                    {match_incase_error, {_EXCLASS_,_EXCPTION_,_ST_}}) of
                true when DoEach == [] -> {true, InputAndItem};
                true ->
                    {true, ?RAISE(select_and_transform(DoEach, InputAndItem),
                                  {doeach_error, {_EXCLASS_,_EXCPTION_,_ST_}})};
                false -> false
            end
        end, CollVal).

%% Conditional Clauses such as WHERE, WHEN.
match_conditions({'and', L, R}, Data) ->
    match_conditions(L, Data) andalso match_conditions(R, Data);
match_conditions({'or', L, R}, Data) ->
    match_conditions(L, Data) orelse match_conditions(R, Data);
match_conditions({'not', Var}, Data) ->
    case eval(Var, Data) of
        Bool when is_boolean(Bool) ->
            not Bool;
        _other -> false
    end;
match_conditions({in, Var, {list, Vals}}, Data) ->
    lists:member(eval(Var, Data), [eval(V, Data) || V <- Vals]);
match_conditions({'fun', {_, Name}, Args}, Data) ->
    apply_func(Name, [eval(Arg, Data) || Arg <- Args], Data);
match_conditions({Op, L, R}, Data) when ?is_comp(Op) ->
    compare(Op, eval(L, Data), eval(R, Data));
match_conditions({}, _Data) ->
    true.

%% comparing numbers against strings
compare(Op, L, R) when is_number(L), is_binary(R) ->
    do_compare(Op, L, number(R));
compare(Op, L, R) when is_binary(L), is_number(R) ->
    do_compare(Op, number(L), R);
compare(Op, L, R) when is_atom(L), is_binary(R) ->
    do_compare(Op, atom_to_binary(L, utf8), R);
compare(Op, L, R) when is_binary(L), is_atom(R) ->
    do_compare(Op, L, atom_to_binary(R, utf8));
compare(Op, L, R) ->
    do_compare(Op, L, R).

do_compare('=', L, R) -> L == R;
do_compare('>', L, R) -> L > R;
do_compare('<', L, R) -> L < R;
do_compare('<=', L, R) -> L =< R;
do_compare('>=', L, R) -> L >= R;
do_compare('<>', L, R) -> L /= R;
do_compare('!=', L, R) -> L /= R;
do_compare('=~', T, F) -> emqx_topic:match(T, F).

number(Bin) ->
    try binary_to_integer(Bin)
    catch error:badarg -> binary_to_float(Bin)
    end.

handle_output_list(Outputs, Selected, Envs) ->
    [handle_output(Out, Selected, Envs) || Out <- Outputs].

handle_output(OutId, Selected, Envs) ->
    try
        do_handle_output(OutId, Selected, Envs)
    catch
        Err:Reason:ST ->
            ?SLOG(error, #{msg => "output_failed",
                           output => OutId,
                           exception => Err,
                           reason => Reason,
                           stacktrace => ST
                          })
    end.

do_handle_output(ChannelId, Selected, _Envs) when is_binary(ChannelId) ->
    ?SLOG(debug, #{msg => "output to bridge", channel_id => ChannelId}),
    emqx_bridge:send_message(ChannelId, Selected);
do_handle_output(#{mod := Mod, func := Func, args := Args}, Selected, Envs) ->
    Mod:Func(Selected, Envs, Args).

eval({path, [{key, <<"payload">>} | Path]}, #{payload := Payload}) ->
    nested_get({path, Path}, may_decode_payload(Payload));
eval({path, [{key, <<"payload">>} | Path]}, #{<<"payload">> := Payload}) ->
    nested_get({path, Path}, may_decode_payload(Payload));
eval({path, _} = Path, Input) ->
    nested_get(Path, Input);
eval({range, {Begin, End}}, _Input) ->
    range_gen(Begin, End);
eval({get_range, {Begin, End}, Data}, Input) ->
    range_get(Begin, End, eval(Data, Input));
eval({var, _} = Var, Input) ->
    nested_get(Var, Input);
eval({const, Val}, _Input) ->
    Val;
%% unary add
eval({'+', L}, Input) ->
    eval(L, Input);
%% unary subtract
eval({'-', L}, Input) ->
    -(eval(L, Input));
eval({Op, L, R}, Input) when ?is_arith(Op) ->
    apply_func(Op, [eval(L, Input), eval(R, Input)], Input);
eval({Op, L, R}, Input) when ?is_comp(Op) ->
    compare(Op, eval(L, Input), eval(R, Input));
eval({list, List}, Input) ->
    [eval(L, Input) || L <- List];
eval({'case', <<>>, CaseClauses, ElseClauses}, Input) ->
    eval_case_clauses(CaseClauses, ElseClauses, Input);
eval({'case', CaseOn, CaseClauses, ElseClauses}, Input) ->
    eval_switch_clauses(CaseOn, CaseClauses, ElseClauses, Input);
eval({'fun', {_, Name}, Args}, Input) ->
    apply_func(Name, [eval(Arg, Input) || Arg <- Args], Input).

handle_alias({path, [{key, <<"payload">>} | _]}, #{payload := Payload} = Input) ->
    Input#{payload => may_decode_payload(Payload)};
handle_alias({path, [{key, <<"payload">>} | _]}, #{<<"payload">> := Payload} = Input) ->
    Input#{<<"payload">> => may_decode_payload(Payload)};
handle_alias(_, Input) ->
    Input.

alias({var, Var}) ->
    {var, Var};
alias({const, Val}) when is_binary(Val) ->
    {var, Val};
alias({list, L}) ->
    {var, ?ephemeral_alias(list, length(L))};
alias({range, R}) ->
    {var, ?ephemeral_alias(range, R)};
alias({get_range, _, {var, Key}}) ->
    {var, Key};
alias({get_range, _, {path, Path}}) ->
    {path, Path};
alias({path, Path}) ->
    {path, Path};
alias({const, Val}) ->
    {var, ?ephemeral_alias(const, Val)};
alias({Op, _L, _R}) when ?is_arith(Op); ?is_comp(Op) ->
    {var, ?ephemeral_alias(op, Op)};
alias({'case', On, _, _}) ->
    {var, ?ephemeral_alias('case', On)};
alias({'fun', Name, _}) ->
    {var, ?ephemeral_alias('fun', Name)};
alias(_) ->
    ?ephemeral_alias(unknown, unknown).

eval_case_clauses([], ElseClauses, Input) ->
    case ElseClauses of
        {} -> undefined;
        _ -> eval(ElseClauses, Input)
    end;
eval_case_clauses([{Cond, Clause} | CaseClauses], ElseClauses, Input) ->
    case match_conditions(Cond, Input) of
        true ->
            eval(Clause, Input);
        _ ->
            eval_case_clauses(CaseClauses, ElseClauses, Input)
    end.

eval_switch_clauses(_CaseOn, [], ElseClauses, Input) ->
    case ElseClauses of
        {} -> undefined;
        _ -> eval(ElseClauses, Input)
    end;
eval_switch_clauses(CaseOn, [{Cond, Clause} | CaseClauses], ElseClauses, Input) ->
    ConResult = eval(Cond, Input),
    case eval(CaseOn, Input) of
        ConResult ->
            eval(Clause, Input);
        _ ->
            eval_switch_clauses(CaseOn, CaseClauses, ElseClauses, Input)
    end.

apply_func(Name, Args, Input) when is_atom(Name) ->
    do_apply_func(Name, Args, Input);
apply_func(Name, Args, Input) when is_binary(Name) ->
    FunName =
        try binary_to_existing_atom(Name, utf8)
        catch error:badarg -> error({sql_function_not_supported, Name})
        end,
    do_apply_func(FunName, Args, Input).

do_apply_func(Name, Args, Input) ->
    case erlang:apply(emqx_rule_funcs, Name, Args) of
        Func when is_function(Func) ->
            erlang:apply(Func, [Input]);
        Result -> Result
    end.

add_metadata(Input, Metadata) when is_map(Input), is_map(Metadata) ->
    NewMetadata = maps:merge(maps:get(metadata, Input, #{}), Metadata),
    Input#{metadata => NewMetadata}.

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------
may_decode_payload(Payload) when is_binary(Payload) ->
    case get_cached_payload() of
        undefined -> safe_decode_and_cache(Payload);
        DecodedP -> DecodedP
    end;
may_decode_payload(Payload) ->
    Payload.

get_cached_payload() ->
    erlang:get(rule_payload).

cache_payload(DecodedP) ->
    erlang:put(rule_payload, DecodedP),
    DecodedP.

safe_decode_and_cache(MaybeJson) ->
    try cache_payload(emqx_json:decode(MaybeJson, [return_maps]))
    catch _:_ -> #{}
    end.

ensure_list(List) when is_list(List) -> List;
ensure_list(_NotList) -> [].

nested_put(Alias, Val, Input0) ->
    Input = handle_alias(Alias, Input0),
    emqx_rule_maps:nested_put(Alias, Val, Input).
