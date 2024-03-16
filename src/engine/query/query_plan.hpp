/*
 * @FileName   : gen_plan.hpp
 * @CreateAt   : 2023/6/16
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef QUERY_PLAN_HPP
#define QUERY_PLAN_HPP

#include <numeric>  // 包含 accumulate 函数
#include <string>
#include <vector>

#include <parallel_hashmap/phmap.h>

#include "../store/index.hpp"

#include "result_vector_list.hpp"

class QueryPlan {
   public:
    // 一对迭代器
    using pair_begin_end = std::pair<std::vector<uint>::iterator, std::vector<uint>::iterator>;

   public:
    struct Item {
        enum Type_T { PS, PO, None };
        Type_T search_type;           // mark the current search type
        uint search_code;             // search this code from corresponding index according to
                                      // `curr_search_type`
        size_t candidate_result_idx;  // the next value location index
        // 一对迭代器，第一个是起始位置，第二个是结束位置
        // std::pair<Entity_Tree_Iterator, Entity_Tree_Iterator>
        //     search_range;  // the range of current search
        std::shared_ptr<Result_Vector> search_range;

        Item() = default;

        Item(const Item& other)
            : search_type(other.search_type),
              search_code(other.search_code),
              candidate_result_idx(other.candidate_result_idx),
              search_range(other.search_range) {}

        Item& operator=(const Item& other) {
            if (this != &other) {
                search_type = other.search_type;
                search_code = other.search_code;
                candidate_result_idx = other.candidate_result_idx;
                search_range = other.search_range;
            }
            return *this;
        }
    };

   public:
    QueryPlan(const std::shared_ptr<Index>& index,
              const std::vector<std::vector<std::string>>& triple_list,
              size_t limit_)
        : limit(limit_) {
        generate_test(index, triple_list);
        // generate(index, triple_list);
    }

    void generate_test(const std::shared_ptr<Index>& index,
                       const std::vector<std::vector<std::string>>& triple_list) {
        bool debug = false;

        // 变量名 -> 出现此变量的三元组id（可以重复）
        // 一个变量对应的的 id 的越多，表明在此变量上执行的连接次数就越多
        phmap::flat_hash_map<std::string, std::vector<size_t>> variable_in_list;
        phmap::flat_hash_map<std::string, size_t> variable_priority;

        phmap::flat_hash_map<std::string, uint> p_set_size;
        uint set_size;
        std::vector<bool> var_flags = {false, false, false};

        for (size_t i = 0; i < triple_list.size(); ++i) {
            const auto& triple = triple_list[i];
            const auto s = triple[0];
            const auto p = triple[1];
            const auto o = triple[2];

            phmap::flat_hash_set<std::string> var_in_triplet;

            if (p[0] == '?') {
                var_in_triplet.insert(p);
                variable_in_list[p].push_back(i);
                var_flags[1] = true;
            }

            if (s[0] == '?') {
                var_in_triplet.insert(s);
                variable_in_list[s].push_back(i);
                var_flags[0] = true;
            }

            if (o[0] == '?') {
                var_in_triplet.insert(o);
                variable_in_list[o].push_back(i);
                var_flags[2] = true;
            }

            if (!var_flags[1]) {
                uint pid = index->predicate2id[p];
                if (var_flags[0] && var_flags[2]) {
                    set_size = index->get_search_range_from_ps_tree(pid)->result.size();
                    if (p_set_size[s] != 0) {
                        if (p_set_size[s] > set_size)
                            p_set_size[s] = set_size;
                    } else {
                        p_set_size[s] = set_size;
                    }
                    set_size = index->get_search_range_from_po_tree(pid)->result.size();
                    if (p_set_size[o] != 0) {
                        if (p_set_size[o] > set_size)
                            p_set_size[o] = set_size;
                    } else {
                        p_set_size[o] = set_size;
                    }
                }
                if (var_flags[0] && !var_flags[2]) {
                    uint oid = index->get_entity_id(o);
                    pid = index->predicate2id[p];
                    set_size = index->get_by_po(pid, oid)->result.size();
                    if (p_set_size[s] != 0) {
                        if (p_set_size[s] > set_size)
                            p_set_size[s] = set_size;
                    } else {
                        p_set_size[s] = set_size;
                    }
                }
                if (!var_flags[0] && var_flags[2]) {
                    uint sid = index->get_entity_id(s);
                    pid = index->predicate2id[p];
                    set_size = index->get_by_ps(pid, sid)->result.size();
                    if (p_set_size[o] != 0) {
                        if (p_set_size[o] > set_size)
                            p_set_size[o] = set_size;
                    } else {
                        p_set_size[o] = set_size;
                    }
                }
            }

            // 变量的权重为包含有此变量的单变量三元组数量
            // 因为单变量的三元组的查询结果一般少于两个或三个变量的三元组
            for (const auto& var : var_in_triplet) {
                variable_priority[var] += (var_in_triplet.size() == 1 ? 1 : 0);
            }
        }

        // get the variable_count's key
        std::vector<std::string> variables(variable_in_list.size());
        // 获取所有变量名
        std::transform(variable_in_list.begin(), variable_in_list.end(), variables.begin(),
                       [](const auto& pair) { return pair.first; });

        // 按照变量在三元组列表中出现的次数从大到小排序，如果次数相等，则按照变量的优先级排序
        // sort 函数会将区间内的元素按照从小到大排序
        // [&] 表明使用引用的方式访问外部变量
        // lambda函数返回的是是否第一个参数小于第二个参数
        std::sort(variables.begin(), variables.end(), [&](const auto& var1, const auto& var2) {
            if (variable_in_list[var1].size() != variable_in_list[var2].size()) {
                return variable_in_list[var1].size() > variable_in_list[var2].size();
            }
            if (variable_priority[var1] != variable_priority[var2]) {
                return variable_priority[var1] > variable_priority[var2];
            }
            return p_set_size[var1] < p_set_size[var2];
        });

        if (debug) {
            for (auto it = p_set_size.begin(); it != p_set_size.end(); it++) {
                std::cout << it->first << " " << it->second << std::endl;
            }

            std::cout << "var order:" << std::endl;
            for (size_t i = 0; i < variables.size(); ++i) {
                std::cout << variables[i] << std::endl;
            }
        }
        for (size_t i = 0; i < variables.size(); ++i) {
            _variable2idx[variables[i]] = i;
        }

        size_t n = variables.size();
        _query_plan.resize(n);
        prestore_result.resize(n);
        other_type_indices.resize(n);
        none_type_indices.resize(n);

        int range_cnt = 0;

        for (const auto& triple : triple_list) {
            const std::string& s = triple[0];
            const std::string& p = triple[1];
            const std::string& o = triple[2];

            if (debug) {
                std::cout << "---------------------------------" << std::endl;
                std::cout << "(" << s << ", " << p << ", " << o << ") plan: " << std::endl;
            }

            if (p[0] == '?') {
                // skip
                // TODO: handle the situation of (s ?p o), (s ?p ?o), (?s ?p o), (?s ?p ?o)
                continue;
            }

            // handle the situation of (?s p ?o)
            uint64_t var_sid = _variable2idx[s];
            uint64_t var_oid = _variable2idx[o];
            if (s[0] == '?' && o[0] == '?') {
                Item item, candidate_result_item;

                // id 越小优先级越高
                if (var_sid < var_oid) {
                    // 先在 ps 索引树上根据已知的 p 查找所有的 s
                    item.search_type = Item::Type_T::PO;
                    item.search_code = index->predicate2id[p];
                    // 下一步应该查询的变量的索引
                    item.candidate_result_idx = var_oid;
                    item.search_range = index->get_search_range_from_ps_tree(item.search_code);
                    item.search_range->id = range_cnt;
                    range_cnt += 1;
                    _query_plan[var_sid].push_back(item);
                    // 非 none 的 item 的索引
                    other_type_indices[var_sid].push_back(_query_plan[var_sid].size() - 1);

                    // 然后根据每一对 ps 找到对应的 o
                    // 先用none来占位
                    candidate_result_item.search_type = Item::Type_T::None;
                    candidate_result_item.search_code = item.search_code;  // don't have the search code
                    candidate_result_item.candidate_result_idx = 0;        // don't have the candidate result
                    candidate_result_item.search_range =
                        std::make_shared<Result_Vector>();  // initialize search range state
                    _query_plan[var_oid].push_back(candidate_result_item);
                    // none item 的索引
                    none_type_indices[var_oid].push_back(_query_plan[var_oid].size() - 1);
                } else {
                    item.search_code = index->predicate2id[p];
                    item.search_type = Item::Type_T::PS;
                    item.candidate_result_idx = var_sid;
                    item.search_range = index->get_search_range_from_po_tree(item.search_code);
                    item.search_range->id = range_cnt;
                    range_cnt += 1;
                    _query_plan[var_oid].push_back(item);
                    other_type_indices[var_oid].push_back(_query_plan[var_oid].size() - 1);

                    candidate_result_item.search_type = Item::Type_T::None;
                    candidate_result_item.search_code = item.search_code;  // don't have the search code
                    candidate_result_item.candidate_result_idx = 0;        // don't have the candidate result
                    candidate_result_item.search_range =
                        std::make_shared<Result_Vector>();  // initialize search range state
                    _query_plan[var_sid].push_back(candidate_result_item);
                    none_type_indices[var_sid].push_back(_query_plan[var_sid].size() - 1);
                }

                if (debug) {
                    if (item.search_type == Item::Type_T::PO) {
                        std::cout << "Item type: PO" << std::endl;
                    } else {
                        std::cout << "Item type: PS" << std::endl;
                    }
                    std::cout << "search code (predict): " << item.search_code << std::endl;
                    if (var_sid < var_oid) {
                        std::cout << "plan level: " << var_sid << std::endl;
                    } else {
                        std::cout << "plan level: " << var_oid << std::endl;
                    }
                }
            }
            // handle the situation of (?s p o)
            else if (s[0] == '?') {
                uint oid = index->get_entity_id(o);
                uint pid = index->predicate2id[p];
                std::shared_ptr<Result_Vector> r = index->get_by_po(pid, oid);
                // std::shared_ptr<Result_Vector> r = single_variable_triple_results[p + "_" + o];
                r->id = range_cnt;
                prestore_result[var_sid].push_back(r);
                range_cnt++;
            }
            // handle the situation of (s p ?o)
            else if (o[0] == '?') {
                uint sid = index->get_entity_id(s);
                uint pid = index->predicate2id[p];
                std::shared_ptr<Result_Vector> r = index->get_by_ps(pid, sid);
                // std::shared_ptr<Result_Vector> r = single_variable_triple_results[p + "_" + s];
                r->id = range_cnt;
                prestore_result[var_oid].push_back(r);
                range_cnt++;
            }
        }

        if (debug) {
            std::cout << "prestore_result:" << std::endl;
            for (int level = 0; level < int(prestore_result.size()); level++) {
                // _prestore_result[level].size();
                std::cout << "level: " << level << " count: " << prestore_result[level].size() << std::endl;

                for (int i = 0; i < int(prestore_result[level].size()); i++) {
                    std::cout << "[";
                    std::cout << prestore_result[level][i]->result.size();

                    // std::for_each(
                    //     _prestore_result[level][i].first, _prestore_result[level][i].second,
                    //     [&](auto& node) { std::cout << index_structure->id2entity[node] << " ";
                    // });
                    std::cout << "] ";
                }

                std::cout << std::endl;
            }
        }

        if (debug) {
            std::cout << "all plan: " << std::endl;
            for (int i = 0; i < int(_query_plan.size()); i++) {
                for (int j = 0; j < int(_query_plan[i].size()); j++) {
                    Item item = _query_plan[i][j];
                    std::cout << item.search_range->id << " [";
                    std::cout << item.search_range->result.size();
                    // std::for_each(item.search_range.first, item.search_range.second,
                    //               [&](auto& node) { std::cout << index_structure->id2entity[node] << "
                    // ";
                    //               });
                    std::cout << "]";
                }
                std::cout << std::endl;
            }
            std::cout << "---------------------------------" << std::endl;
        }

        if (debug) {
            std::cout << "all none: " << std::endl;
            for (int i = 0; i < int(none_type_indices.size()); i++) {
                std::cout << "level: " << i << " size: " << none_type_indices[i].size() << std::endl;
            }
            std::cout << "---------------------------------" << std::endl;
        }
    }

    void generate(const std::shared_ptr<Index>& index,
                  const std::vector<std::vector<std::string>>& triple_list) {
        bool debug = true;

        // 变量名 -> 出现此变量的三元组id（可以重复）
        // 一个变量对应的的 id 的越多，表明在此变量上执行的连接次数就越多
        phmap::flat_hash_map<std::string, std::vector<size_t>> variable_in_list;
        phmap::flat_hash_map<std::string, size_t> variable_priority;

        for (size_t i = 0; i < triple_list.size(); ++i) {
            const auto& triple = triple_list[i];
            const auto s = triple[0];
            const auto p = triple[1];
            const auto o = triple[2];

            phmap::flat_hash_set<std::string> var_in_triplet;
            if (s[0] == '?') {
                var_in_triplet.insert(s);
                variable_in_list[s].push_back(i);
            }

            if (p[0] == '?') {
                var_in_triplet.insert(p);
                variable_in_list[p].push_back(i);
            }

            if (o[0] == '?') {
                var_in_triplet.insert(o);
                variable_in_list[o].push_back(i);
            }

            // 变量的权重为包含有此变量的单变量三元组数量
            // 因为单变量的三元组的查询结果一般少于两个或三个变量的三元组
            for (const auto& var : var_in_triplet) {
                variable_priority[var] += (var_in_triplet.size() == 1 ? 1 : 0);
            }
        }

        // get the variable_count's key
        std::vector<std::string> variables(variable_in_list.size());
        // 获取所有变量名
        std::transform(variable_in_list.begin(), variable_in_list.end(), variables.begin(),
                       [](const auto& pair) { return pair.first; });

        // 按照变量在三元组列表中出现的次数从大到小排序，如果次数相等，则按照变量的优先级排序
        // sort 函数会将区间内的元素按照从小到大排序
        // [&] 表明使用引用的方式访问外部变量
        // lambda函数返回的是是否第一个参数小于第二个参数
        std::sort(variables.begin(), variables.end(), [&](const auto& var1, const auto& var2) {
            if (variable_in_list[var1].size() != variable_in_list[var2].size()) {
                return variable_in_list[var1].size() > variable_in_list[var2].size();
            }
            return variable_priority[var1] > variable_priority[var2];
        });

        // mapping the variable to id
        // 变量 -> 排序顺序

        // variables = {"?X", "?Y", "?Z"}; // 3301.909289
        // variables = {"?X", "?Z", "?Y"}; // 4027.433499
        // variables = {"?Y", "?X", "?Z"}; // 2946.524109
        // variables = {"?Y", "?Z", "?X"}; // 4596.390094
        // variables = {"?Z", "?X", "?Y"}; // 6012.844853
        // variables = {"?Z", "?Y", "?X"}; // 4676.747373
        // variables = {"?v2", "?v1", "?v0", "?v3"};

        if (debug) {
            std::cout << "var order:" << std::endl;
        }
        for (size_t i = 0; i < variables.size(); ++i) {
            _variable2idx[variables[i]] = i;
            if (debug)
                std::cout << variables[i] << ": " << i << std::endl;
        }

        size_t n = variables.size();
        _query_plan.resize(n);
        prestore_result.resize(n);
        other_type_indices.resize(n);
        none_type_indices.resize(n);

        int range_cnt = 0;

        for (const auto& triple : triple_list) {
            const std::string& s = triple[0];
            const std::string& p = triple[1];
            const std::string& o = triple[2];

            if (debug) {
                std::cout << "---------------------------------" << std::endl;
                std::cout << "(" << s << ", " << p << ", " << o << ") plan: " << std::endl;
            }

            if (p[0] == '?') {
                // skip
                // TODO: handle the situation of (s ?p o), (s ?p ?o), (?s ?p o), (?s ?p ?o)
                continue;
            }

            // handle the situation of (?s p ?o)
            uint64_t var_sid = _variable2idx[s];
            uint64_t var_oid = _variable2idx[o];
            if (s[0] == '?' && o[0] == '?') {
                Item item, candidate_result_item;

                // id 越小优先级越高
                if (var_sid < var_oid) {
                    // 先在 ps 索引树上根据已知的 p 查找所有的 s
                    item.search_type = Item::Type_T::PO;
                    item.search_code = index->predicate2id[p];
                    // 下一步应该查询的变量的索引
                    item.candidate_result_idx = var_oid;
                    item.search_range = index->get_search_range_from_ps_tree(item.search_code);
                    item.search_range->id = range_cnt;
                    range_cnt += 1;
                    _query_plan[var_sid].push_back(item);
                    // 非 none 的 item 的索引
                    other_type_indices[var_sid].push_back(_query_plan[var_sid].size() - 1);

                    // 然后根据每一对 ps 找到对应的 o
                    // 先用none来占位
                    candidate_result_item.search_type = Item::Type_T::None;
                    candidate_result_item.search_code = item.search_code;  // don't have the search code
                    candidate_result_item.candidate_result_idx = 0;        // don't have the candidate result
                    candidate_result_item.search_range =
                        std::make_shared<Result_Vector>();  // initialize search range state
                    _query_plan[var_oid].push_back(candidate_result_item);
                    // none item 的索引
                    none_type_indices[var_oid].push_back(_query_plan[var_oid].size() - 1);
                } else {
                    item.search_code = index->predicate2id[p];
                    item.search_type = Item::Type_T::PS;
                    item.candidate_result_idx = var_sid;
                    item.search_range = index->get_search_range_from_po_tree(item.search_code);
                    item.search_range->id = range_cnt;
                    range_cnt += 1;
                    _query_plan[var_oid].push_back(item);
                    other_type_indices[var_oid].push_back(_query_plan[var_oid].size() - 1);

                    candidate_result_item.search_type = Item::Type_T::None;
                    candidate_result_item.search_code = item.search_code;  // don't have the search code
                    candidate_result_item.candidate_result_idx = 0;        // don't have the candidate result
                    candidate_result_item.search_range =
                        std::make_shared<Result_Vector>();  // initialize search range state
                    _query_plan[var_sid].push_back(candidate_result_item);
                    none_type_indices[var_sid].push_back(_query_plan[var_sid].size() - 1);
                }

                if (debug) {
                    if (item.search_type == Item::Type_T::PO) {
                        std::cout << "Item type: PO" << std::endl;
                    } else {
                        std::cout << "Item type: PS" << std::endl;
                    }
                    std::cout << "search code (predict): " << item.search_code << std::endl;
                    if (var_sid < var_oid) {
                        std::cout << "plan level: " << var_sid << std::endl;
                    } else {
                        std::cout << "plan level: " << var_oid << std::endl;
                    }
                }
            }
            // handle the situation of (?s p o)
            else if (s[0] == '?') {
                uint oid = index->get_entity_id(o);
                uint pid = index->predicate2id[p];
                std::shared_ptr<Result_Vector> r = index->get_by_po(pid, oid);
                // std::shared_ptr<Result_Vector> r = single_variable_triple_results[p + "_" + o];
                r->id = range_cnt;
                prestore_result[var_sid].push_back(r);
                range_cnt++;
            }
            // handle the situation of (s p ?o)
            else if (o[0] == '?') {
                uint sid = index->get_entity_id(s);
                uint pid = index->predicate2id[p];
                std::shared_ptr<Result_Vector> r = index->get_by_ps(pid, sid);
                // std::shared_ptr<Result_Vector> r = single_variable_triple_results[p + "_" + s];
                r->id = range_cnt;
                prestore_result[var_oid].push_back(r);
                range_cnt++;
            }
        }

        if (debug) {
            std::cout << "prestore_result:" << std::endl;
            for (int level = 0; level < int(prestore_result.size()); level++) {
                // _prestore_result[level].size();
                std::cout << "level: " << level << " count: " << prestore_result[level].size() << std::endl;

                for (int i = 0; i < int(prestore_result[level].size()); i++) {
                    std::cout << "[";
                    std::cout << prestore_result[level][i]->result.size();

                    // std::for_each(
                    //     _prestore_result[level][i].first, _prestore_result[level][i].second,
                    //     [&](auto& node) { std::cout << index_structure->id2entity[node] << " ";
                    // });
                    std::cout << "] ";
                }

                std::cout << std::endl;
            }
        }

        if (debug) {
            std::cout << "all plan: " << std::endl;
            for (int i = 0; i < int(_query_plan.size()); i++) {
                for (int j = 0; j < int(_query_plan[i].size()); j++) {
                    Item item = _query_plan[i][j];
                    std::cout << item.search_range->id << " [";
                    std::cout << item.search_range->result.size();
                    // std::for_each(item.search_range.first, item.search_range.second,
                    //               [&](auto& node) { std::cout << index_structure->id2entity[node] << "
                    // ";
                    //               });
                    std::cout << "]";
                }
                std::cout << std::endl;
            }
            std::cout << "---------------------------------" << std::endl;
        }

        if (debug) {
            std::cout << "all none: " << std::endl;
            for (int i = 0; i < int(none_type_indices.size()); i++) {
                std::cout << "level: " << i << " size: " << none_type_indices[i].size() << std::endl;
            }
            std::cout << "---------------------------------" << std::endl;
        }
    }

    [[nodiscard]] const phmap::flat_hash_map<std::string, uint64_t>& variable2idx() const {
        return _variable2idx;
    }

    std::vector<size_t> mapping_variable2idx(const std::vector<std::string>& variables) {
        std::vector<size_t> ret;
        ret.reserve(variables.size());
        for (const auto& var : variables) {
            ret.emplace_back(_variable2idx.at(var));
        }
        return ret;
    }

    [[nodiscard]] const std::vector<std::vector<Item>>& plan() const { return _query_plan; }

    // [[nodiscard]] const std::vector<std::vector<std::shared_ptr<Result_Vector>>>& prestore_result() const {
    //     return _prestore_result;
    // }

   public:
    size_t limit;
    std::vector<std::vector<size_t>> other_type_indices;
    std::vector<std::vector<size_t>> none_type_indices;
    std::vector<std::vector<std::shared_ptr<Result_Vector>>> prestore_result;

   private:
    // 二维数组，变量的优先级顺序id -> 此变量在不同的三元组中的查询结果
    std::vector<std::vector<Item>> _query_plan;
    phmap::flat_hash_map<std::string, uint64_t> _variable2idx;
};

#endif  // COMBINED_CODE_INDEX_GEN_PLAN_HPP
