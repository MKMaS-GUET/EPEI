
#ifndef QUERY_RESULT_HPP
#define QUERY_RESULT_HPP

#include <vector>
#include "../parser/sparql_parser.hpp"
#include "../store/index.hpp"
#include "./query_plan.hpp"

uint query_result(std::vector<std::vector<uint>>& results_id,
                  std::string& results,
                  const std::shared_ptr<Index>& index,
                  const std::shared_ptr<QueryPlan>& query_plan,
                  const std::shared_ptr<SPARQLParser>& parser) {
    auto last = results_id.end();
    const auto& modifier = parser->project_modifier();
    // 获取每一个变量的id（优先级顺序）
    const auto variable_indexes = query_plan->MappingVariable(parser->ProjectVariables());

    std::stringstream ss;
    uint cnt = 0;
    if (modifier.modifier_type_ == SPARQLParser::ProjectModifier::Distinct) {
        // last = std::unique(results_id.begin(), results_id.end(),
        //                    // 判断两个列表 a 和 b 是否相同，
        //                    [&](const std::vector<uint>& a, const std::vector<uint>& b) {
        //                        // std::all_of 可以用来判断数组中的值是否都满足一个条件
        //                        return std::all_of(variable_indexes.begin(), variable_indexes.end(),
        //                                           // 判断依据是，列表中的每一个元素都相同
        //                                           [&](size_t i) { return a[i] == b[i]; });
        //                    });
        // for (auto it = results_id.begin(); it != last; ++it) {
        //     const auto& item = *it;
        //     for (const auto& idx : variable_indexes) {
        //         ss << index->id2entity[item[idx.first]]->c_str();
        //         ss << "";
        //     }
        //     ss << "\n";
        //     cnt++;
        // }
    } else {
        cnt = results_id.size();
        for (auto it = results_id.begin(); it != last; ++it) {
            const auto& item = *it;
            for (const auto& idx : variable_indexes) {
                ss << index->id2entity[item[idx.first]]->c_str();
                ss << "";
            }
            ss << "\n";
        }
    }

    results = ss.str();
    return cnt;
}

int query_result(std::vector<std::vector<uint>>& result,
                 const std::shared_ptr<Index>& index,
                 const std::shared_ptr<QueryPlan>& query_plan,
                 const std::shared_ptr<SPARQLParser>& parser) {
    auto last = result.end();
    const auto& modifier = parser->project_modifier();
    // project_variables 是要输出的变量顺序
    // 而 result 的变量顺序是计划生成中的变量排序
    // 所以要获取每一个要输出的变量在 result 中的位置
    const auto variable_indexes = query_plan->MappingVariable(parser->ProjectVariables());

    int cnt = 0;
    if (modifier.modifier_type_ == SPARQLParser::ProjectModifier::Distinct) {
        // last = std::unique(result.begin(), result.end(),
        //                    // 判断两个列表 a 和 b 是否相同，
        //                    [&](const std::vector<uint>& a, const std::vector<uint>& b) {
        //                        // std::all_of 可以用来判断数组中的值是否都满足一个条件
        //                        return std::all_of(variable_indexes.begin(), variable_indexes.end(),
        //                                           // 判断依据是，列表中的每一个元素都相同
        //                                           [&](size_t i) { return a[i] == b[i]; });
        //                    });
        // for (auto it = result.begin(); it != last; ++it) {
        //     const auto& item = *it;
        //     for (const auto& idx : variable_indexes) {
        //         std::cout << *index->id2entity[item[idx.first]] << " ";
        //         // printf("%s ", index->id2entity[item[idx]]->c_str());
        //     }
        //     cnt++;
        //     std::cout << "\n";
        // }
    } else {
        cnt = result.size();
        for (auto it = result.begin(); it != last; ++it) {
            const auto& item = *it;
            for (const auto& idx : variable_indexes) {
                std::cout << *index->id2entity[item[idx.first]] << " ";
            }
            std::cout << "\n";
        }
    }

    return cnt;
}

int query_result(std::vector<std::vector<uint>>& result,
                 const std::shared_ptr<Index>& index,
                 const std::shared_ptr<QueryPlan>& query_plan,
                 const std::shared_ptr<SPARQLParser>& parser,
                 std::ofstream& output_file) {
    auto last = result.end();
    const auto& modifier = parser->project_modifier();
    // 获取每一个变量的id（优先级顺序）
    const auto variable_indexes = query_plan->MappingVariable(parser->ProjectVariables());

    int cnt = 0;
    if (modifier.modifier_type_ == SPARQLParser::ProjectModifier::Distinct) {
        // last = std::unique(result.begin(), result.end(),
        //                    // 判断两个列表 a 和 b 是否相同，
        //                    [&](const std::vector<uint>& a, const std::vector<uint>& b) {
        //                        // std::all_of 可以用来判断数组中的值是否都满足一个条件
        //                        return std::all_of(variable_indexes.begin(), variable_indexes.end(),
        //                                           // 判断依据是，列表中的每一个元素都相同
        //                                           [&](size_t i) { return a[i] == b[i]; });
        //                    });
        // for (auto it = result.begin(); it != last; ++it) {
        //     const auto& item = *it;
        //     for (const auto& idx : variable_indexes) {
        //         output_file << *index->id2entity[item[idx.first]] << " ";
        //     }
        //     cnt++;
        //     output_file << "\n";
        // }
    } else {
        cnt = result.size();
        for (auto it = result.begin(); it != last; ++it) {
            const auto& item = *it;
            for (const auto& idx : variable_indexes) {
                output_file << *index->id2entity[item[idx.first]] << " ";
            }
            output_file << "\n";
        }
    }

    return cnt;
}

#endif