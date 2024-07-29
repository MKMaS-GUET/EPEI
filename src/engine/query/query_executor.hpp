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
#include "../store/index_retriever.hpp"
#include "leapfrog_join.hpp"
#include "query_plan.hpp"

struct Stat {
   public:
    Stat(const std::vector<std::vector<QueryPlan::Item>>& p) : at_end_(false), level_(-1), plan_(p) {
        size_t n = plan_.size();
        indices_.resize(n);
        candidate_result_.resize(n);
        current_tuple_.resize(n);

        for (long unsigned int i = 0; i < n; i++) {
            candidate_result_[i] = std::make_shared<std::vector<uint>>();
        }
    }

    Stat(const Stat& other)
        : at_end_(other.at_end_),
          level_(other.level_),
          indices_(other.indices_),
          current_tuple_(other.current_tuple_),
          candidate_result_(other.candidate_result_),
          result_(other.result_),
          plan_(other.plan_) {}

    Stat& operator=(const Stat& other) {
        if (this != &other) {
            at_end_ = other.at_end_;
            level_ = other.level_;
            indices_ = other.indices_;
            current_tuple_ = other.current_tuple_;
            candidate_result_ = other.candidate_result_;
            result_ = other.result_;
            plan_ = other.plan_;
        }
        return *this;
    }

    bool at_end_;
    int level_;
    // 用于记录每一个 level_ 的 candidate_result_ 已经处理过的结果的 id
    std::vector<uint> indices_;
    std::vector<uint> current_tuple_;
    std::vector<std::shared_ptr<std::vector<uint>>> candidate_result_;
    std::vector<std::vector<uint>> result_;
    std::vector<std::vector<QueryPlan::Item>> plan_;
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
   public:
    QueryExecutor(const std::shared_ptr<IndexRetriever>& p_index,
                  const std::shared_ptr<QueryPlan>& p_query_plan)
        : _stat(p_query_plan->query_plan()),
          _p_index(p_index),
          _p_query_plan(p_query_plan),
          _prestore_result(p_query_plan->prestore_result_) {}

    QueryExecutor(const std::shared_ptr<IndexRetriever>& p_index,
                  const std::shared_ptr<QueryPlan>& p_query_plan,
                  const std::shared_ptr<std::vector<std::string>>& p_project_variables)
        : _stat(p_query_plan->query_plan()),
          _p_index(p_index),
          _p_query_plan(p_query_plan),
          _prestore_result(p_query_plan->prestore_result_),
          _p_project_variables(p_project_variables) {}

    void Query() {
        _query_begin_time = std::chrono::high_resolution_clock::now();
        PreJoin();

        for (;;) {
            if (_stat.at_end_) {
                if (_stat.level_ == 0) {
                    break;
                }
                Up(_stat);
                Next(_stat);
            } else {
                // 补完一个查询结果
                if (_stat.level_ == int(_stat.plan_.size() - 1)) {
                    _stat.result_.push_back(_stat.current_tuple_);
                    if (_stat.result_.size() >= _p_query_plan->limit_) {
                        break;
                    }
                    Next(_stat);
                } else {
                    Down(_stat);
                }
            }
        }

        _query_end_time = std::chrono::high_resolution_clock::now();
    }

    inline double Duration() {
        return static_cast<std::chrono::duration<double, std::milli>>(_query_end_time - _query_begin_time)
            .count();
    }

    [[nodiscard]] std::vector<std::vector<uint>>& query_result() { return _stat.result_; }

   private:
    bool PreJoin() {
        ResultList result_list;
        std::stringstream key;
        for (long unsigned int level_ = 1; level_ < _stat.plan_.size(); level_++) {
            // for (long unsigned int i = 0; i < _prestore_result[level_].size(); i++) {
            //     // _stat.at_end_ = true;
            //     // return false;
            //     key << _prestore_result[level_][i]->get_id();
            //     key << "_";
            //     Result_Vector_list.add_range(_prestore_result[level_][i]);
            // }

            if (!_p_query_plan->none_type_indices_[level_].empty())
                continue;

            if (!_p_query_plan->prestore_result_[level_].empty())
                continue;

            for (long unsigned int i = 0; i < _stat.plan_[level_].size(); i++) {
                if (_stat.plan_[level_][i].search_type_ != QueryPlan::Item::TypeT::kNone) {
                    key << _stat.plan_[level_][i].search_result_->id;
                    key << "_";
                    result_list.AddVector(_stat.plan_[level_][i].search_result_);
                }
            }
            if (result_list.Size() > 1) {
                _pre_join_result[key.str()] = LeapfrogJoin(result_list);
            }
            result_list.Clear();
            key.str("");
        }
        return true;
    }

    void Down(Stat& stat) {
        ++stat.level_;
        // sleep(2);

        // 如果当前层没有查询结果，就生成结果
        if (stat.candidate_result_[stat.level_]->empty()) {
            // check whether there are some have the Item::Type_T::None
            EnumerateItems(stat);
            if (stat.at_end_) {
                return;
            }
        }

        // 遍历当前level_所有经过连接的得到的结果实体
        // 并将这些实体添加到存储结果的 current_tuple_ 中

        bool success = UpdateCurrentTuple(stat);
        // 不成功则继续
        while (!success && !stat.at_end_) {
            success = UpdateCurrentTuple(stat);
        }
        // sleep(2);
    }

    void Up(Stat& stat) {
        // 清除较高 level_ 的查询结果
        stat.candidate_result_[stat.level_]->clear();
        stat.indices_[stat.level_] = 0;

        --stat.level_;
    }

    void Next(Stat& stat) {
        // 当前 level_ 的下一个 candidate_result_
        stat.at_end_ = false;
        bool success = UpdateCurrentTuple(stat);
        while (!success && !stat.at_end_) {
            success = UpdateCurrentTuple(stat);
        }
    }

    void EnumerateItems(Stat& stat) {
        // 每一层可能有
        // 1.单变量三元组的查询结果，存储在 _prestore_result 中，
        // 2.双变量三元组的 none 类型的 item，查询结果search_result在之前的层数被填充在 plan 中，
        // 3.双变量三元组的非 none 类型的 item

        ResultList result_list;

        // _prestore_result 是 (?s p o) 和 (?s p o) 的查询结果
        if (!_prestore_result[stat.level_].empty()) {
            // join item for none type
            // 如果有此变量（level_）在单变量三元组中，且有查询结果，
            // 就将此变量在所有三元组中的查询结果插入到查询结果列表的末尾
            if (!result_list.AddVectors(_prestore_result[stat.level_])) {
                stat.at_end_ = true;
                return;
            }
        }

        // none 类型已经在之前的层数的时候就已经填补上了查询范围，
        // 而且 none 类型不可能在第 0 层
        for (const auto& idx : _p_query_plan->none_type_indices_[stat.level_]) {
            // 将 none 类型的 item（查询结果）放入查询列表中
            result_list.AddVector(stat.plan_[stat.level_][idx].search_result_);
        }

        const auto& item_other_type_indices_ = _p_query_plan->other_type_indices_[stat.level_];
        uint join_case = result_list.Size();
        if (join_case == 0) {
            if (_pre_join_result.size() != 0) {
                std::stringstream key_stream;
                for (const auto& idx : item_other_type_indices_) {
                    if (_pre_join_result.size() != 0) {
                        key_stream << stat.plan_[stat.level_][idx].search_result_->id;
                        key_stream << "_";
                    }
                }
                std::string key = key_stream.str();

                auto it = _pre_join_result.find(key);
                if (it == _pre_join_result.end()) {
                    for (const auto& idx : item_other_type_indices_) {
                        result_list.AddVector(stat.plan_[stat.level_][idx].search_result_);
                    }
                } else {
                    stat.candidate_result_[stat.level_]->reserve(it->second->size());
                    for (auto iter = it->second->begin(); iter != it->second->end(); iter++) {
                        stat.candidate_result_[stat.level_]->emplace_back(std::move(*iter));
                    }
                    return;
                }
            } else {
                for (const auto& idx : item_other_type_indices_) {
                    result_list.AddVector(stat.plan_[stat.level_][idx].search_result_);
                }
            }
            stat.candidate_result_[stat.level_] = LeapfrogJoin(result_list);
        }
        if (join_case == 1) {
            // for (const auto& idx : item_other_type_indices_) {
            //     result_list.add_vector(stat.plan_[stat.level_][idx].search_result);
            // }
            // stat.candidate_result_[stat.level_] = leapfrog_join(result_Vector_list);
            std::shared_ptr<Result> range = result_list.GetRangeByIndex(0);
            stat.candidate_result_[stat.level_] = std::make_shared<std::vector<uint>>();
            for (uint i = 0; i < range->size(); i++) {
                stat.candidate_result_[stat.level_]->push_back(range->operator[](i));
            }
        }
        if (join_case > 1) {
            stat.candidate_result_[stat.level_] = LeapfrogJoin(result_list);
        }

        // 变量的交集为空
        if (stat.candidate_result_[stat.level_]->empty()) {
            stat.at_end_ = true;
            return;
        }
    }

    bool UpdateCurrentTuple(Stat& stat) {
        // stat.indices_ 用于更新已经处理过的交集结果在结果列表中的 id
        size_t idx = stat.indices_[stat.level_];

        bool have_other_type = !_p_query_plan->other_type_indices_[stat.level_].empty();
        if (idx < stat.candidate_result_[stat.level_]->size()) {
            // candidate_result_ 是每一个 level_ 交集的计算结果，
            // entity 是第 idx 个结果
            uint entity = stat.candidate_result_[stat.level_]->at(idx);

            // if also have the other type item(s), search predicate path for these item(s)
            if (!have_other_type) {
                stat.current_tuple_[stat.level_] = entity;
                ++idx;
                stat.indices_[stat.level_] = idx;
                return true;
            }

            // 填补当前level_上的变量所在的三元组的 none 类型 item 的查找范围
            // 只更新 candidate_result_ 里的符合查询条件的 none 类型
            if (search_predicate_path(stat, entity)) {
                stat.current_tuple_[stat.level_] = entity;
                ++idx;
                stat.indices_[stat.level_] = idx;
                return true;
            }

            // 如果不存在三元组 (?s, search_code, entity)，则表明 entity 不符合查询条件
            // 切换到下一个 entity
            ++idx;
            stat.indices_[stat.level_] = idx;
        } else {
            stat.at_end_ = true;
        }

        return false;
    }

    bool search_predicate_path(Stat& stat, uint64_t entity) {
        bool match = true;
        // 遍历一个变量（level_）的在所有三元组中的查询结果
        for (auto& item : stat.plan_[stat.level_]) {
            if (item.search_type_ == QueryPlan::Item::TypeT::kPS) {
                size_t other = item.candidate_result_idx_;

                // 遍历当前三元组none所在的level_
                for (auto& other_item : stat.plan_[other]) {
                    if (other_item.search_code_ != item.search_code_) {
                        // 确保谓词相同，在一个三元组中
                        continue;
                    }

                    other_item.search_result_ = _p_index->GetByPO(item.search_code_, entity);
                    if (other_item.search_result_->size() == 0) {
                        match = false;
                    }

                    break;
                }
            } else if (item.search_type_ == QueryPlan::Item::TypeT::kPO) {
                size_t other = item.candidate_result_idx_;

                for (auto& other_item : stat.plan_[other]) {
                    if (other_item.search_code_ != item.search_code_) {
                        continue;
                    }

                    other_item.search_result_ = _p_index->GetByPS(item.search_code_, entity);
                    if (other_item.search_result_->size() == 0) {
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
    std::shared_ptr<IndexRetriever> _p_index;
    std::shared_ptr<QueryPlan> _p_query_plan;
    std::vector<std::vector<std::shared_ptr<Result>>> _prestore_result;
    std::shared_ptr<std::vector<std::string>> _p_project_variables;

    std::chrono::system_clock::time_point _query_begin_time, _query_end_time;

    hash_map<std::string, std::shared_ptr<std::vector<uint>>> _pre_join_result;
};

#endif  // QUERY_EXECUTOR_HPP
