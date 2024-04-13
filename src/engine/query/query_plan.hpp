#ifndef QUERY_PLAN_HPP
#define QUERY_PLAN_HPP

#include <parallel_hashmap/phmap.h>
#include <climits>
#include <numeric>  // 包含 accumulate 函数
#include <string>
#include <vector>

#include "../store/index.hpp"
#include "leapfrog_join.hpp"


using AdjacencyList = phmap::flat_hash_map<std::string, std::vector<std::pair<std::string, uint>>>;

class QueryPlan {
   public:
    // 一对迭代器
    using pair_begin_end = std::pair<std::vector<uint>::iterator, std::vector<uint>::iterator>;

    struct Item {
        enum Type_T { PS, PO, None };
        Type_T search_type;           // mark the current search type
        uint search_code;             // search this code from corresponding index according to
                                      // `curr_search_type`
        size_t candidate_result_idx;  // the next value location index
        // 一对迭代器，第一个是起始位置，第二个是结束位置
        // std::pair<Entity_Tree_Iterator, Entity_Tree_Iterator>
        //     search_range;  // the range of current search
        std::shared_ptr<Result> search_result;

        Item() = default;

        Item(const Item& other)
            : search_type(other.search_type),
              search_code(other.search_code),
              candidate_result_idx(other.candidate_result_idx),
              search_result(other.search_result) {}

        Item& operator=(const Item& other) {
            if (this != &other) {
                search_type = other.search_type;
                search_code = other.search_code;
                candidate_result_idx = other.candidate_result_idx;
                search_result = other.search_result;
            }
            return *this;
        }
    };

    QueryPlan(const std::shared_ptr<Index>& index,
              const std::vector<std::vector<std::string>>& triple_list,
              size_t limit_)
        : limit(limit_) {
        generate(index, triple_list);
    }

    void DFS(const AdjacencyList& graph,
             std::string vertex,
             phmap::flat_hash_map<std::string, bool>& visited,
             AdjacencyList& tree,
             std::vector<std::string>& currentPath,
             std::vector<std::vector<std::string>>& allPaths) {
        currentPath.push_back(vertex);  // Add the current vertex to the path

        // Check if it's a leaf node in the spanning tree (no adjacent vertices)
        // if (graph.at(vertex).size() == 1 || visited[vertex] == true) {
        //     allPaths.push_back(currentPath);  // Save the current path if it's a leaf
        // }
        bool all_visited = true;
        for (const auto& edge : graph.at(vertex)) {
            if (!visited[edge.first]) {
                all_visited = false;
            }
        }
        if (all_visited)
            allPaths.push_back(currentPath);
        visited[vertex] = true;

        // Explore the adjacent vertices
        for (const auto& edge : graph.at(vertex)) {
            std::string adjVertex = edge.first;
            if (!visited[adjVertex]) {
                tree[vertex].emplace_back(adjVertex, edge.second);            // Add edge to the spanning tree
                DFS(graph, adjVertex, visited, tree, currentPath, allPaths);  // Continue DFS
            }
        }

        // Backtrack: remove the current vertex from the path
        currentPath.pop_back();
    }

    std::vector<std::vector<std::string>> findAllPathsInGraph(const AdjacencyList& graph,
                                                              const std::string& root) {
        phmap::flat_hash_map<std::string, bool> visited;  // Track visited vertices
        AdjacencyList tree;                               // The resulting spanning tree
        std::vector<std::string> currentPath;             // Current path from the root to the current vertex
        std::vector<std::vector<std::string>> allPaths;   // All paths from the root to the leaves

        // Initialize visited map
        for (const auto& pair : graph) {
            visited[pair.first] = false;
        }
        // visited[root] = true;

        // Perform DFS to fill the spanning tree and find all paths
        DFS(graph, root, visited, tree, currentPath, allPaths);

        return allPaths;
    }

    void generate(const std::shared_ptr<Index>& index,
                  const std::vector<std::vector<std::string>>& triple_list) {
        AdjacencyList query_graph_ud;
        phmap::flat_hash_map<std::string, uint> est_size;
        phmap::flat_hash_map<std::string, uint> univariates;

        for (size_t i = 0; i < triple_list.size(); ++i) {
            const auto& triple = triple_list[i];
            const std::string s = triple[0];
            const std::string p = triple[1];
            const std::string o = triple[2];

            std::string vertex1;
            uint edge;
            std::string vertex2;
            uint size;

            if (s[0] == '?' && o[0] == '?') {
                vertex1 = s;
                edge = index->predicate2id[p];
                vertex2 = o;
                // only support ?v predicate ?v
                size = index->get_s_set_size(edge);
                if (est_size[s] == 0 || est_size[s] > size) {
                    est_size[s] = size;
                }
                size = index->get_o_set_size(edge);
                if (est_size[o] == 0 || est_size[o] > size) {
                    est_size[o] = size;
                }
            } else if (s[0] == '?' && p[0] == '?') {
                vertex1 = s;
                edge = index->get_entity_id(o);
                vertex2 = p;
            } else if (p[0] == '?' && o[0] == '?') {
                vertex1 = p;
                edge = index->get_entity_id(s);
                vertex2 = o;
            } else {
                if (s[0] == '?') {
                    univariates[s] += 1;
                    size = index->get_by_po_size(index->predicate2id[p], index->get_entity_id(o));
                    if (est_size[s] == 0 || est_size[s] > size) {
                        est_size[s] = size;
                    }
                }
                if (o[0] == '?') {
                    univariates[o] += 1;
                    size = index->get_by_po_size(index->predicate2id[p], index->get_entity_id(o));
                    if (est_size[s] == 0 || est_size[s] > size) {
                        est_size[s] = size;
                    }
                }
                if (p[0] == '?')
                    univariates[p] += 1;
                continue;
            }

            query_graph_ud[vertex1].push_back({vertex2, edge});
            query_graph_ud[vertex2].push_back({vertex1, edge});
        }

        if (debug) {
            for (auto vertex_it = query_graph_ud.begin(); vertex_it != query_graph_ud.end(); vertex_it++) {
                std::cout << vertex_it->first << ": ";
                for (auto& edge : vertex_it->second) {
                    std::cout << " (" << edge.first << "," << edge.second << ") ";
                }
                std::cout << std::endl;
            }

            for (auto& pair : est_size) {
                std::cout << pair.first << ": " << pair.second << std::endl;
            }
        }

        std::vector<std::string> variable_priority(query_graph_ud.size());
        std::transform(query_graph_ud.begin(), query_graph_ud.end(), variable_priority.begin(),
                       [](const auto& pair) { return pair.first; });

        std::sort(variable_priority.begin(), variable_priority.end(),
                  [&](const auto& var1, const auto& var2) {
                      if (query_graph_ud[var1].size() + univariates[var1] !=
                          query_graph_ud[var2].size() + univariates[var2]) {
                          return query_graph_ud[var1].size() + univariates[var1] >
                                 query_graph_ud[var2].size() + univariates[var2];
                      }
                      return est_size[var1] < est_size[var2];
                  });

        if (debug) {
            std::cout << "------------------------------" << std::endl;
            for (auto& v : variable_priority) {
                std::cout << v << ": " << est_size[v] << std::endl;
            }
        }

        std::vector<std::string> one_degree_variables;
        uint degree_two = 0;
        uint other_degree = 0;
        for (auto vertex_it = query_graph_ud.begin(); vertex_it != query_graph_ud.end(); vertex_it++) {
            if (vertex_it->second.size() + univariates[vertex_it->first] == 1) {
                one_degree_variables.push_back(vertex_it->first);
            } else if (vertex_it->second.size() + univariates[vertex_it->first] == 2) {
                degree_two++;
            } else {
                other_degree++;
            }
        }

        std::vector<std::vector<std::string>> allPaths;
        std::vector<std::vector<std::string>> partialPaths;
        uint longest_path = 0;
        while (variable_priority.size() > 0) {
            partialPaths = findAllPathsInGraph(query_graph_ud, variable_priority[0]);
            for (auto& path : partialPaths) {
                for (auto& v : path) {
                    for (auto it = variable_priority.begin(); it != variable_priority.end(); it++) {
                        if (*it == v) {
                            variable_priority.erase(it);
                            break;
                        }
                    }
                }
                if (allPaths.size() == 0 || path.size() > allPaths[longest_path].size()) {
                    longest_path = allPaths.size();
                }
                allPaths.push_back(path);
            }
        }

        std::vector<std::string> plan;

        if (!(one_degree_variables.size() == query_graph_ud.size() - 1) &&  // 非star
            ((one_degree_variables.size() == 2 &&
              one_degree_variables.size() + degree_two == query_graph_ud.size() &&
              allPaths.size() == partialPaths.size()) ||  // 是连通图且是路径
             allPaths.size() == 1)) {                     // 是一个环
            if (allPaths.size() == 1) {                   // 是一个环/路径
                if (est_size[allPaths[0][0]] < est_size[allPaths[0].back()]) {
                    for (auto it = allPaths[0].begin(); it != allPaths[0].end(); it++) {
                        plan.push_back(*it);
                    }
                } else {
                    for (int i = allPaths[0].size() - 1; i > -1; i--) {
                        plan.push_back(allPaths[0][i]);
                    }
                }
            } else {  // 是一个路径
                if (est_size[one_degree_variables[0]] < est_size[one_degree_variables[1]])
                    plan.push_back(one_degree_variables[0]);
                else
                    plan.push_back(one_degree_variables[1]);
                std::string previews = plan.back();
                plan.push_back(query_graph_ud[previews][0].first);
                while (plan.size() != query_graph_ud.size()) {
                    if (query_graph_ud[plan.back()][0].first != previews) {
                        previews = plan.back();
                        plan.push_back(query_graph_ud[plan.back()][0].first);
                    } else {
                        previews = plan.back();
                        plan.push_back(query_graph_ud[plan.back()][1].first);
                    };
                }
            }
        } else {
            phmap::flat_hash_set<std::string> exist_variables;
            for (auto& v : allPaths[longest_path]) {
                exist_variables.insert(v);
            }
            for (uint i = 0; i < allPaths.size(); i++) {
                if (i != longest_path) {
                    for (auto it = allPaths[i].begin(); it != allPaths[i].end(); it++) {
                        if (exist_variables.contains(*it)) {
                            allPaths[i].erase(it);
                            it--;
                        } else {
                            exist_variables.insert(*it);
                        }
                    }
                }
            }

            if (debug) {
                std::cout << "longest_path: " << longest_path << std::endl;
                for (auto& path : allPaths) {
                    for (auto it = path.begin(); it != path.end(); it++) {
                        std::cout << *it << " ";
                    }
                    std::cout << std::endl;
                }
            }

            std::vector<std::string>::iterator path_its[allPaths.size()];
            for (uint i = 0; i < allPaths.size(); i++) {
                path_its[i] = allPaths[i].begin();
            }

            for (uint i = 0; i < exist_variables.size() - allPaths.size(); i++) {
                std::string min_variable = "";
                uint path_id = 0;
                for (uint p = 0; p < allPaths.size(); p++) {
                    if (path_its[p] != allPaths[p].end() - 1) {
                        if (min_variable == "" ||
                            query_graph_ud[*path_its[p]].size() + univariates[*path_its[p]] >
                                query_graph_ud[min_variable].size() + univariates[min_variable]) {
                            min_variable = *path_its[p];
                            path_id = p;
                        }
                    }
                }
                if (path_its[path_id] != allPaths[path_id].end() - 1)
                    path_its[path_id]++;
                plan.push_back(min_variable);
            }

            for (auto& path : allPaths) {
                plan.push_back(*(path.end() - 1));
            }
        }

        for (auto it = univariates.begin(); it != univariates.end(); it++) {
            bool contains = false;
            for (auto& v : plan) {
                if (v == it->first) {
                    contains = true;
                }
            }
            if (!contains) {
                plan.push_back(it->first);
            }
        }

        if (debug) {
            std::cout << "variables order: " << std::endl;
            for (auto it = plan.begin(); it != plan.end(); it++) {
                std::cout << *it << " ";
            }
            std::cout << std::endl;
        }

        gen_plan_table(index, triple_list, plan);
    }

    void gen_plan_table(const std::shared_ptr<Index>& index,
                        const std::vector<std::vector<std::string>>& triple_list,
                        std::vector<std::string>& variables) {
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
                    item.search_result = index->get_s_set(item.search_code);
                    item.search_result->id = range_cnt;
                    range_cnt += 1;
                    _query_plan[var_sid].push_back(item);
                    // 非 none 的 item 的索引
                    other_type_indices[var_sid].push_back(_query_plan[var_sid].size() - 1);

                    // 然后根据每一对 ps 找到对应的 o
                    // 先用none来占位
                    candidate_result_item.search_type = Item::Type_T::None;
                    candidate_result_item.search_code = item.search_code;  // don't have the search code
                    candidate_result_item.candidate_result_idx = 0;        // don't have the candidate result
                    candidate_result_item.search_result =
                        std::make_shared<Result>();  // initialize search range state
                    _query_plan[var_oid].push_back(candidate_result_item);
                    // none item 的索引
                    none_type_indices[var_oid].push_back(_query_plan[var_oid].size() - 1);
                } else {
                    item.search_code = index->predicate2id[p];
                    item.search_type = Item::Type_T::PS;
                    item.candidate_result_idx = var_sid;
                    item.search_result = index->get_o_set(item.search_code);
                    item.search_result->id = range_cnt;
                    range_cnt += 1;
                    _query_plan[var_oid].push_back(item);
                    other_type_indices[var_oid].push_back(_query_plan[var_oid].size() - 1);

                    candidate_result_item.search_type = Item::Type_T::None;
                    candidate_result_item.search_code = item.search_code;  // don't have the search code
                    candidate_result_item.candidate_result_idx = 0;        // don't have the candidate result
                    candidate_result_item.search_result =
                        std::make_shared<Result>();  // initialize search range state
                    _query_plan[var_sid].push_back(candidate_result_item);
                    none_type_indices[var_sid].push_back(_query_plan[var_sid].size() - 1);
                }
            }
            // handle the situation of (?s p o)
            else if (s[0] == '?') {
                uint oid = index->get_entity_id(o);
                uint pid = index->predicate2id[p];
                std::shared_ptr<Result> r = index->get_by_po(pid, oid);
                r->id = range_cnt;
                prestore_result[var_sid].push_back(r);
                range_cnt++;
            }
            // handle the situation of (s p ?o)
            else if (o[0] == '?') {
                uint sid = index->get_entity_id(s);
                uint pid = index->predicate2id[p];
                std::shared_ptr<Result> r = index->get_by_ps(pid, sid);
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
                    std::cout << prestore_result[level][i]->size();

                    // std::for_each(
                    //     _prestore_result[level][i].first, _prestore_result[level][i].second,
                    //     [&](auto& node) { std::cout << index_structure->id2entity[node] << " ";
                    // });
                    std::cout << "] ";
                }

                std::cout << std::endl;
            }
            std::cout << "---------------------------------" << std::endl;

            std::cout << "all plan: " << std::endl;
            for (int i = 0; i < int(_query_plan.size()); i++) {
                for (int j = 0; j < int(_query_plan[i].size()); j++) {
                    Item item = _query_plan[i][j];
                    std::cout << item.search_result->id << " [";
                    std::cout << item.search_result->size();
                    std::cout << "] ";
                }
                std::cout << std::endl;
            }
            std::cout << "---------------------------------" << std::endl;

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

    size_t limit;
    std::vector<std::vector<size_t>> other_type_indices;
    std::vector<std::vector<size_t>> none_type_indices;
    std::vector<std::vector<std::shared_ptr<Result>>> prestore_result;

   private:
    bool debug = false;
    // 二维数组，变量的优先级顺序id -> 此变量在不同的三元组中的查询结果
    std::vector<std::vector<Item>> _query_plan;
    phmap::flat_hash_map<std::string, uint64_t> _variable2idx;
};

#endif  // COMBINED_CODE_INDEX_GEN_PLAN_HPP
