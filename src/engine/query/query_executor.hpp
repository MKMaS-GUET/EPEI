#ifndef QUERY_EXECUTOR_HPP
#define QUERY_EXECUTOR_HPP

#include <parallel_hashmap/phmap.h>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stack>
#include <string>
#include <utility>
#include <vector>
#include "../parser/sparql_parser.hpp"
#include "../store/index.hpp"
#include "../tools/thread_pool.hpp"
#include "leapfrog_join.hpp"
#include "query_plan.hpp"

struct Stat {
   public:
    Stat(const std::vector<std::vector<QueryPlan::Item>>& p) : at_end(false), level(-1), plan(p) {
        size_t n = plan.size();
        indices.resize(n);
        candidate_result.resize(n);
        current_tuple.resize(n);

        for (long unsigned int i = 0; i < n; i++) {
            candidate_result[i] = std::make_shared<std::vector<uint>>();
        }
    }

    Stat(const Stat& other)
        : at_end(other.at_end),
          level(other.level),
          indices(other.indices),
          current_tuple(other.current_tuple),
          candidate_result(other.candidate_result),
          result(other.result),
          plan(other.plan) {}

    Stat& operator=(const Stat& other) {
        if (this != &other) {
            at_end = other.at_end;
            level = other.level;
            indices = other.indices;
            current_tuple = other.current_tuple;
            candidate_result = other.candidate_result;
            result = other.result;
            plan = other.plan;
        }
        return *this;
    }

    bool at_end;
    int level;
    // 用于记录每一个 level 的 candidate_result 已经处理过的结果的 id
    std::vector<uint> indices;
    std::vector<uint> current_tuple;
    std::vector<std::shared_ptr<std::vector<uint>>> candidate_result;
    std::vector<std::vector<uint>> result;
    std::vector<std::vector<QueryPlan::Item>> plan;
};

struct OutputStat {
    uint result_cnt = 0;
    std::vector<std::vector<uint>> result;             // 使用deque作为缓冲区
    std::mutex mtx;                                    // 互斥锁
    std::condition_variable cv_producer, cv_consumer;  // 条件变量
    bool finished = false;                             // 用来指示数据添加是否完成
    const size_t MAX_BUFFER_SIZE = 10000;              // 缓冲区的最大大小
};

class QueryExecutor {
    void output_result(SPARQLParser::ProjectModifier modifier) {
        const auto variable_indexes = _p_query_plan->mapping_variable2idx(*_p_project_variables);

        std::set<std::vector<uint>> distinct_results;

        while (true) {
            std::unique_lock<std::mutex> lk(_output_stat.mtx);
            _output_stat.cv_consumer.wait(lk, [this] {
                return _output_stat.finished ||
                       (_output_stat.result.size() % _output_stat.MAX_BUFFER_SIZE == 0 &&
                        !_output_stat.result.empty());
            });

            if (_output_stat.finished && _output_stat.result.size() == _output_stat.result_cnt)
                break;  // 如果完成且数据为空，则退出

            // 处理数据
            while (_output_stat.result.size() > _output_stat.result_cnt) {
                std::vector<uint>& item = _output_stat.result.front();
                if (modifier.modifier_type_ == SPARQLParser::ProjectModifier::Distinct) {
                    if (distinct_results.find(item) != distinct_results.end()) {
                        continue;
                    } else {
                        distinct_results.insert(item);
                    }
                }
                for (const auto& idx : variable_indexes) {
                    std::cout << *_p_index->id2entity[item[idx]] << " ";
                }
                std::cout << "\n";
                if (_output_stat.result_cnt >= _p_query_plan->limit) {
                    return;
                }
                _output_stat.result_cnt++;
            }

            lk.unlock();
            _output_stat.cv_producer.notify_one();  // 通知生产者可能有空间添加新数据了
        }
    }

   public:
    QueryExecutor(const std::shared_ptr<Index>& p_index, const std::shared_ptr<QueryPlan>& p_query_plan)
        : _stat(p_query_plan->plan()),
          _p_index(p_index),
          _p_query_plan(p_query_plan),
          _prestore_result(p_query_plan->prestore_result) {}

    QueryExecutor(const std::shared_ptr<Index>& p_index,
                  const std::shared_ptr<QueryPlan>& p_query_plan,
                  const std::shared_ptr<std::vector<std::string>>& p_project_variables)
        : _stat(p_query_plan->plan()),
          _p_index(p_index),
          _p_query_plan(p_query_plan),
          _prestore_result(p_query_plan->prestore_result),
          _p_project_variables(p_project_variables) {}

    uint query(SPARQLParser::ProjectModifier modifier) {
        _query_begin_time = std::chrono::high_resolution_clock::now();

        pre_join();

        std::thread t(&QueryExecutor::output_result, this, modifier);

        for (;;) {
            if (_stat.at_end) {
                if (_stat.level == 0) {
                    break;
                }
                up(_stat);
                next(_stat);
            } else {
                // 补完一个查询结果
                if (_stat.level == int(_stat.plan.size() - 1)) {
                    _output_stat.result.push_back(_stat.current_tuple);
                    std::unique_lock<std::mutex> lk(_output_stat.mtx);
                    _output_stat.result.push_back(_stat.current_tuple);
                    lk.unlock();
                    if (_output_stat.result.size() % _output_stat.MAX_BUFFER_SIZE == 0 &&
                        _output_stat.result.size()) {
                        _output_stat.cv_consumer.notify_one();  // 通知消费者有新数据
                    }

                    if (_stat.result.size() >= _p_query_plan->limit) {
                        break;
                    }
                    next(_stat);
                } else {
                    down(_stat);
                }
            }
        }

        {
            std::lock_guard<std::mutex> lk(_output_stat.mtx);
            _output_stat.finished = true;
        }
        _output_stat.cv_consumer.notify_all();  // 通知所有消费者完成
        t.join();

        _query_end_time = std::chrono::high_resolution_clock::now();

        return _output_stat.result_cnt;
    }

    void query() {
        _query_begin_time = std::chrono::high_resolution_clock::now();
        pre_join();

        for (;;) {
            if (_stat.at_end) {
                if (_stat.level == 0) {
                    break;
                }
                up(_stat);
                next(_stat);
            } else {
                // 补完一个查询结果
                if (_stat.level == int(_stat.plan.size() - 1)) {
                    _stat.result.push_back(_stat.current_tuple);
                    if (_stat.result.size() >= _p_query_plan->limit) {
                        break;
                    }
                    next(_stat);
                } else {
                    down(_stat);
                }
            }
        }

        _query_end_time = std::chrono::high_resolution_clock::now();
    }

    inline double duration() {
        return static_cast<std::chrono::duration<double, std::milli>>(_query_end_time - _query_begin_time)
            .count();
    }

    [[nodiscard]] std::vector<std::vector<uint>>& result() { return _stat.result; }

   private:
    bool pre_join() {
        ResultList result_list;
        std::stringstream key;
        for (long unsigned int level = 1; level < _stat.plan.size(); level++) {
            // for (long unsigned int i = 0; i < _prestore_result[level].size(); i++) {
            //     // _stat.at_end = true;
            //     // return false;
            //     key << _prestore_result[level][i]->get_id();
            //     key << "_";
            //     Result_Vector_list.add_range(_prestore_result[level][i]);
            // }

            if (!_p_query_plan->none_type_indices[level].empty())
                continue;

            if (!_p_query_plan->prestore_result[level].empty())
                continue;

            for (long unsigned int i = 0; i < _stat.plan[level].size(); i++) {
                if (_stat.plan[level][i].search_type != QueryPlan::Item::Type_T::None) {
                    key << _stat.plan[level][i].search_result->id;
                    key << "_";
                    result_list.add_vector(_stat.plan[level][i].search_result);
                }
            }
            if (result_list.size() > 1) {
                _pre_join_result[key.str()] = leapfrog_join(result_list);
            }
            result_list.clear();
            key.str("");
        }
        return true;
    }

    void down(Stat& stat) {
        ++stat.level;
        // sleep(2);

        // 如果当前层没有查询结果，就生成结果
        if (stat.candidate_result[stat.level]->empty()) {
            // check whether there are some have the Item::Type_T::None
            enumerate_items(stat);
            if (stat.at_end) {
                return;
            }
        }

        // 遍历当前level所有经过连接的得到的结果实体
        // 并将这些实体添加到存储结果的 current_tuple 中

        bool success = update_current_tuple(stat);
        // 不成功则继续
        while (!success && !stat.at_end) {
            success = update_current_tuple(stat);
        }
        // sleep(2);
    }

    void up(Stat& stat) {
        // 清除较高 level 的查询结果
        stat.candidate_result[stat.level]->clear();
        stat.indices[stat.level] = 0;

        --stat.level;
    }

    void next(Stat& stat) {
        // 当前 level 的下一个 candidate_result
        stat.at_end = false;
        bool success = update_current_tuple(stat);
        while (!success && !stat.at_end) {
            success = update_current_tuple(stat);
        }
    }

    void enumerate_items(Stat& stat) {
        // 每一层可能有
        // 1.单变量三元组的查询结果，存储在 _prestore_result 中，
        // 2.双变量三元组的 none 类型的 item，查询结果search_result在之前的层数被填充在 plan 中，
        // 3.双变量三元组的非 none 类型的 item

        ResultList result_list;

        // _prestore_result 是 (?s p o) 和 (?s p o) 的查询结果
        if (!_prestore_result[stat.level].empty()) {
            // join item for none type
            // 如果有此变量（level）在单变量三元组中，且有查询结果，
            // 就将此变量在所有三元组中的查询结果插入到查询结果列表的末尾
            if (!result_list.add_vectors(_prestore_result[stat.level])) {
                stat.at_end = true;
                return;
            }
        }

        // none 类型已经在之前的层数的时候就已经填补上了查询范围，
        // 而且 none 类型不可能在第 0 层
        for (const auto& idx : _p_query_plan->none_type_indices[stat.level]) {
            // 将 none 类型的 item（查询结果）放入查询列表中
            result_list.add_vector(stat.plan[stat.level][idx].search_result);
        }

        const auto& item_other_type_indices = _p_query_plan->other_type_indices[stat.level];
        uint join_case = result_list.size();
        if (join_case == 0) {
            if (_pre_join_result.size() != 0) {
                std::stringstream key_stream;
                for (const auto& idx : item_other_type_indices) {
                    if (_pre_join_result.size() != 0) {
                        key_stream << stat.plan[stat.level][idx].search_result->id;
                        key_stream << "_";
                    }
                }
                std::string key = key_stream.str();

                auto it = _pre_join_result.find(key);
                if (it == _pre_join_result.end()) {
                    for (const auto& idx : item_other_type_indices) {
                        result_list.add_vector(stat.plan[stat.level][idx].search_result);
                    }
                } else {
                    stat.candidate_result[stat.level]->reserve(it->second->size());
                    for (auto iter = it->second->begin(); iter != it->second->end(); iter++) {
                        stat.candidate_result[stat.level]->emplace_back(std::move(*iter));
                    }
                    return;
                }
            } else {
                for (const auto& idx : item_other_type_indices) {
                    result_list.add_vector(stat.plan[stat.level][idx].search_result);
                }
            }
            stat.candidate_result[stat.level] = leapfrog_join(result_list);
        }
        if (join_case == 1) {
            // for (const auto& idx : item_other_type_indices) {
            //     result_list.add_vector(stat.plan[stat.level][idx].search_result);
            // }
            // stat.candidate_result[stat.level] = leapfrog_join(result_Vector_list);
            std::shared_ptr<Result> range = result_list.get_range_by_index(0);
            stat.candidate_result[stat.level] = std::make_shared<std::vector<uint>>();
            for (uint i = 0; i < range->size(); i++) {
                stat.candidate_result[stat.level]->push_back(range->operator[](i));
            }
        }
        if (join_case > 1) {
            stat.candidate_result[stat.level] = leapfrog_join(result_list);
        }

        // 变量的交集为空
        if (stat.candidate_result[stat.level]->empty()) {
            stat.at_end = true;
            return;
        }
    }

    bool update_current_tuple(Stat& stat) {
        // stat.indices 用于更新已经处理过的交集结果在结果列表中的 id
        size_t idx = stat.indices[stat.level];

        bool have_other_type = !_p_query_plan->other_type_indices[stat.level].empty();
        if (idx < stat.candidate_result[stat.level]->size()) {
            // candidate_result 是每一个 level 交集的计算结果，
            // entity 是第 idx 个结果
            uint entity = stat.candidate_result[stat.level]->at(idx);

            // if also have the other type item(s), search predicate path for these item(s)
            if (!have_other_type) {
                stat.current_tuple[stat.level] = entity;
                ++idx;
                stat.indices[stat.level] = idx;
                return true;
            }

            // 填补当前level上的变量所在的三元组的 none 类型 item 的查找范围
            // 只更新 candidate_result 里的符合查询条件的 none 类型
            if (search_predicate_path(stat, entity)) {
                stat.current_tuple[stat.level] = entity;
                ++idx;
                stat.indices[stat.level] = idx;
                return true;
            }

            // 如果不存在三元组 (?s, search_code, entity)，则表明 entity 不符合查询条件
            // 切换到下一个 entity
            ++idx;
            stat.indices[stat.level] = idx;
        } else {
            stat.at_end = true;
        }

        return false;
    }

    bool search_predicate_path(Stat& stat, uint64_t entity) {
        bool match = true;
        // 遍历一个变量（level）的在所有三元组中的查询结果
        for (auto& item : stat.plan[stat.level]) {
            if (item.search_type == QueryPlan::Item::Type_T::PS) {
                size_t other = item.candidate_result_idx;

                // 遍历当前三元组none所在的level
                for (auto& other_item : stat.plan[other]) {
                    if (other_item.search_code != item.search_code) {
                        // 确保谓词相同，在一个三元组中
                        continue;
                    }

                    other_item.search_result = _p_index->get_by_po(item.search_code, entity);
                    if (other_item.search_result->size() == 0) {
                        match = false;
                    }

                    break;
                }
            } else if (item.search_type == QueryPlan::Item::Type_T::PO) {
                size_t other = item.candidate_result_idx;

                for (auto& other_item : stat.plan[other]) {
                    if (other_item.search_code != item.search_code) {
                        continue;
                    }

                    other_item.search_result = _p_index->get_by_ps(item.search_code, entity);
                    if (other_item.search_result->size() == 0) {
                        match = false;
                    }

                    break;
                }
            }
        }
        return match;
    }

   private:
    Stat _stat;
    OutputStat _output_stat;
    std::shared_ptr<Index> _p_index;
    std::shared_ptr<QueryPlan> _p_query_plan;
    std::vector<std::vector<std::shared_ptr<Result>>> _prestore_result;
    std::shared_ptr<std::vector<std::string>> _p_project_variables;

    std::chrono::system_clock::time_point _query_begin_time, _query_end_time;

    phmap::flat_hash_map<std::string, std::shared_ptr<std::vector<uint>>> _pre_join_result;
};

#endif  // QUERY_EXECUTOR_HPP
