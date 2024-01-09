/*
 * @FileName   : engine-impl.cpp
 * @CreateAt   : 2022/10/25
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef ENGINE_IMPL_HPP
#define ENGINE_IMPL_HPP

#include <fstream>
#include <memory>
#include <string>

#include <hsinDB/engine.hpp>

#include "parser/sparql_parser.hpp"
#include "query/query_executor.hpp"
#include "query/query_plan.hpp"
#include "store/build_index.hpp"
#include "store/index.hpp"
#include "server/server.hpp"

class hsinDB::Engine::Impl {
   public:
    void create(const std::string& db_name, const std::string& data_file) {
        auto beg = std::chrono::high_resolution_clock::now();

        IndexBuilder builder(db_name, data_file);
        if (!builder.build()) {
            std::cerr << "Building index data failed, terminal the process." << std::endl;
            exit(1);
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "Creating " << db_name << " takes " << diff.count() << " ms." << std::endl;
    }

    void query(const std::string& db_name, const std::string& sparql_file) {
        std::shared_ptr<Index> index = std::make_shared<Index>(db_name);

        // parse SPARQL statement
        std::vector<std::string> sparqls = read_sparql_file(sparql_file);

        for (long unsigned int i = 0; i < sparqls.size(); i++) {
            std::string sparql = sparqls[i];

            printf("%ld ------------------------------------------------------------------\n", i + 1);

            auto parser = std::make_shared<SPARQLParser>(sparql);
            auto triple_list = parser->triple_list();

            auto start = std::chrono::high_resolution_clock::now();

            // generate query plan
            auto query_plan = std::make_shared<QueryPlan>(index, triple_list, parser->limit());

            // execute query
            auto executor = std::make_shared<QueryExecutor>(index, query_plan);

            executor->query();

            auto mapping_start = std::chrono::high_resolution_clock::now();
            int cnt = project_result(executor->result(), index, query_plan, parser);
            auto mapping_finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> mapping_diff = mapping_finish - mapping_start;

            auto finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = finish - start;

            // printf("%s", sparql.c_str());
            printf("%d result(s).\n", cnt);
            printf("execute takes %lf ms.\n", executor->duration());
            printf("mapping_result takes %lf ms.\n", mapping_diff.count());

            printf("query time cost %lf ms.\n", diff.count());
        }
    }

    void server(const std::string& port) {
        start_server(port);
    }

   private:
    std::vector<std::string> read_sparql_file(const std::string& sparql_file) {
        std::vector<std::string> sparqls;
        std::string sparql = "";

        std::ifstream in(sparql_file, std::ifstream::in);
        if (in.is_open()) {
            std::string line;
            while (std::getline(in, sparql)) {
                sparqls.push_back(sparql);
            }
            in.close();
        }
        sparqls.push_back(sparql);

        return sparqls;
    }

    int project_result(std::vector<std::vector<uint>>& result,
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
};

#endif  // COMPRESSED_ENCODED_TREE_INDEX_ENGINE_IMPL_HPP
