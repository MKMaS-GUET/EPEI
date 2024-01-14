
#ifndef QUERY_RESULT_HPP
#define QUERY_RESULT_HPP

#include <vector>
#include "../parser/sparql_parser.hpp"
#include "../store/index.hpp"
#include "./query_plan.hpp"

int query_result(std::vector<std::vector<uint>>& results_id,
                 std::vector<std::vector<std::string>>& results,
                 const std::shared_ptr<Index>& index,
                 const std::shared_ptr<QueryPlan>& query_plan,
                 const std::shared_ptr<SPARQLParser>& parser) {
    auto last = results_id.end();
    const auto& modifier = parser->project_modifier();
    // 获取每一个变量的id（优先级顺序）
    const auto variable_indexes = query_plan->mapping_variable2idx(parser->project_variables());

    int cnt = 0;
    results.reserve(variable_indexes.size());
    if (modifier.modifier_type_ == SPARQLParser::ProjectModifier::Distinct) {
        last = std::unique(results_id.begin(), results_id.end(),
                           // 判断两个列表 a 和 b 是否相同，
                           [&](const std::vector<uint>& a, const std::vector<uint>& b) {
                               // std::all_of 可以用来判断数组中的值是否都满足一个条件
                               return std::all_of(variable_indexes.begin(), variable_indexes.end(),
                                                  // 判断依据是，列表中的每一个元素都相同
                                                  [&](size_t i) { return a[i] == b[i]; });
                           });
        for (auto it = results_id.begin(); it != last; ++it) {
            const auto& r = *it;
            auto result = std::vector<std::string>(variable_indexes.size());
            for (const auto& idx : variable_indexes) {
                result.push_back(index->id2entity[r[idx]]);
            }
            cnt++;
        }
    } else {
        cnt = results_id.size();
        for (auto it = results_id.begin(); it != last; ++it) {
            const auto& r = *it;
            auto result = std::vector<std::string>();
            result.reserve(variable_indexes.size());
            for (const auto& idx : variable_indexes) {
                result.push_back(index->id2entity[r[idx]]);
            }
            results.emplace_back(std::move(result));
        }
    }

    return cnt;
}

int query_result(std::vector<std::vector<uint>>& result,
                 const std::shared_ptr<Index>& index,
                 const std::shared_ptr<QueryPlan>& query_plan,
                 const std::shared_ptr<SPARQLParser>& parser) {
    auto last = result.end();
    const auto& modifier = parser->project_modifier();
    // 获取每一个变量的id（优先级顺序）
    const auto variable_indexes = query_plan->mapping_variable2idx(parser->project_variables());

    int cnt = 0;
    if (modifier.modifier_type_ == SPARQLParser::ProjectModifier::Distinct) {
        last = std::unique(result.begin(), result.end(),
                           // 判断两个列表 a 和 b 是否相同，
                           [&](const std::vector<uint>& a, const std::vector<uint>& b) {
                               // std::all_of 可以用来判断数组中的值是否都满足一个条件
                               return std::all_of(variable_indexes.begin(), variable_indexes.end(),
                                                  // 判断依据是，列表中的每一个元素都相同
                                                  [&](size_t i) { return a[i] == b[i]; });
                           });
        for (auto it = result.begin(); it != last; ++it) {
            const auto& item = *it;
            for (const auto& idx : variable_indexes) {
                // index->id2entity[item[idx]].c_str();
                printf("%s ", index->id2entity[item[idx]].c_str());
            }
            cnt++;
            printf("\n");
        }
    } else {
        cnt = result.size();
        for (auto it = result.begin(); it != last; ++it) {
            const auto& item = *it;
            for (const auto& idx : variable_indexes) {
                // index->id2entity[item[idx]].c_str();
                printf("%s ", index->id2entity[item[idx]].c_str());
            }
            printf("\n");
        }
    }

    return cnt;
}

#endif