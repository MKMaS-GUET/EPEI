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
#include "query/query_result.hpp"
#include "server/server.hpp"
#include "store/build_index.hpp"
#include "store/index.hpp"

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
        // parse SPARQL statement

        std::vector<std::string> sparqls;
        std::string sparql = "";
        phmap::flat_hash_set<std::string> entities;
        std::ifstream in(sparql_file, std::ifstream::in);
        if (in.is_open()) {
            std::string line;
            while (std::getline(in, sparql)) {
                get_entity(entities, sparql);
                sparqls.push_back(sparql);
            }
            in.close();
        }

        std::shared_ptr<Index> index = std::make_shared<Index>(db_name);
        index->load_data(entities);

        for (long unsigned int i = 0; i < sparqls.size(); i++) {
            std::string sparql = sparqls[i];

            printf("%ld ------------------------------------------------------------------\n", i + 1);

            auto start = std::chrono::high_resolution_clock::now();

            auto parser = std::make_shared<SPARQLParser>(sparql);
            auto triple_list = parser->triple_list();

            // generate query plan
            auto query_plan = std::make_shared<QueryPlan>(index, triple_list, parser->limit());

            auto plan_end = std::chrono::high_resolution_clock::now();

            // execute query
            auto executor = std::make_shared<QueryExecutor>(index, query_plan);

            executor->query();

            auto mapping_start = std::chrono::high_resolution_clock::now();
            int cnt = query_result(executor->result(), index, query_plan, parser);
            auto mapping_finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> mapping_diff = mapping_finish - mapping_start;

            auto finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = finish - start;

            std::chrono::duration<double, std::milli> plan_time = plan_end - start;

            // printf("%s", sparql.c_str());
            printf("%d result(s).\n", cnt);
            printf("generate plan takes %lf ms.\n", plan_time.count());
            printf("execute takes %lf ms.\n", executor->duration());
            printf("output result takes %lf ms.\n", mapping_diff.count());

            printf("query time cost %lf ms.\n", diff.count());

            malloc_trim(0);
        }
        exit(0);
    }

    void server(const std::string& port) { start_server(port); }

   private:
    void get_entity(phmap::flat_hash_set<std::string>& entities, std::string sparql) {
        std::regex pattern(R"(\{([^{}]*)\})");
        std::regex triplet_Pattern(
            R"(((\?.*?\s+)|(<.*?>)\s+)((\?.*?\s+)|(<.*?>)\s+)((\?.*?\s+)|(<.*?>)\s+)\.)");

        std::smatch match;
        if (std::regex_search(sparql, match, pattern)) {
            std::string triplets_str = match[1].str();

            std::sregex_iterator triplet_iter(triplets_str.begin(), triplets_str.end(), triplet_Pattern);
            std::sregex_iterator end;

            while (triplet_iter != end) {
                std::smatch triplet = *triplet_iter;

                std::istringstream iss(triplet[0]);
                std::string part1, part2, part3;
                iss >> part1 >> part2 >> part3;

                if (part1[0] == '<' && part1.back() == '>') {
                    entities.insert(part1);
                }
                if (part3[0] == '<' && part3.back() == '>') {
                    entities.insert(part3);
                }

                ++triplet_iter;
            }
        }

        return;
    }
};

#endif  // COMPRESSED_ENCODED_TREE_INDEX_ENGINE_IMPL_HPP
