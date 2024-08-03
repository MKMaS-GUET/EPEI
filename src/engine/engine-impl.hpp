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

#include <epei/engine.hpp>

#include "parser/sparql_parser.hpp"
#include "query/query_executor.hpp"
#include "query/query_plan.hpp"
#include "query/query_result.hpp"
#include "server/server.hpp"
#include "store/index_builder.hpp"
#include "store/index_retriever.hpp"

class epei::Engine::Impl {
   public:
    void Create(const std::string& db_name, const std::string& data_file) {
        auto beg = std::chrono::high_resolution_clock::now();

        IndexBuilder builder(db_name, data_file);
        if (!builder.Build()) {
            std::cerr << "Building index data failed, terminal the process." << std::endl;
            exit(1);
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "Creating " << db_name << " takes " << diff.count() << " ms." << std::endl;
    }

    void ExecuteSparql(std::vector<std::string> sparqls,
                       std::shared_ptr<IndexRetriever> index,
                       std::string file_name) {
        std::ofstream output_file;
        std::ios::sync_with_stdio(false);
        if (file_name != "")
            output_file = std::ofstream(file_name);

        for (long unsigned int i = 0; i < sparqls.size(); i++) {
            std::string sparql = sparqls[i];

            if (sparqls.size() > 1) {
                if (file_name == "") {
                    std::cout << i + 1
                              << " ------------------------------------------------------------------"
                              << std::endl;
                    std::cout << sparql << std::endl;
                } else {
                    output_file << (i + 1)
                                << " ------------------------------------------------------------------"
                                << std::endl;

                    std::cout << "finish " << i + 1 << " queries" << std::endl;
                }
            }

            auto start = std::chrono::high_resolution_clock::now();

            auto parser = std::make_shared<SPARQLParser>(sparql);
            auto triple_list = parser->TripleList();

            // generate query plan
            auto query_plan = std::make_shared<QueryPlan>(index, triple_list, parser->Limit());

            auto plan_end = std::chrono::high_resolution_clock::now();

            // execute query
            auto executor = std::make_shared<QueryExecutor>(index, query_plan);

            executor->Query();

            auto mapping_start = std::chrono::high_resolution_clock::now();
            uint cnt = 0;
            if (file_name == "")
                cnt = query_result(executor->query_result(), index, query_plan, parser);
            else
                cnt = query_result(executor->query_result(), index, query_plan, parser, output_file);
            auto mapping_finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> mapping_diff = mapping_finish - mapping_start;

            auto finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = finish - start;

            std::chrono::duration<double, std::milli> plan_time = plan_end - start;

            if (file_name == "") {
                std::cout << cnt << " result(s).\n";
                std::cout << "generate plan takes " << plan_time.count() << " ms.\n";
                std::cout << "execute takes " << executor->Duration() << " ms.\n";
                std::cout << "output result takes " << mapping_diff.count() << " ms.\n";
                std::cout << "query cost " << diff.count() << " ms." << std::endl;
            } else {
                output_file << cnt << "  result(s).\n";
                output_file << "generate plan takes " << plan_time.count() << " ms.\n";
                output_file << "execute takes " << executor->Duration() << " ms.\n";
                output_file << "output result takes " << mapping_diff.count() << " ms.\n";
                output_file << "query cost " << diff.count() << " ms." << std::endl;
            }

            // printf("%s", sparql.c_str());
        }
    }

    void Query(const std::string& name, const std::string& file) {
        if (name != "" and file != "") {
            std::shared_ptr<IndexRetriever> index = std::make_shared<IndexRetriever>(name);
            std::ifstream in(file, std::ifstream::in);
            std::vector<std::string> sparqls;
            if (in.is_open()) {
                std::string line;
                std::string sparql;
                while (std::getline(in, sparql)) {
                    sparqls.push_back(sparql);
                }
                in.close();
            }
            ExecuteSparql(sparqls, index, "");
            exit(0);
        }
    }

    void Server(const std::string& ip, const std::string& port) { start_server(ip, port); }

   private:
    void ListDB() {
        std::cout << ">select a database:" << std::endl;

        std::filesystem::path path("./DB_DATA_ARCHIVE");
        if (!std::filesystem::exists(path)) {
            return;
        }

        std::filesystem::directory_iterator end_iter;
        for (std::filesystem::directory_iterator iter(path); iter != end_iter; ++iter) {
            if (std::filesystem::is_directory(iter->status())) {
                std::string dir_name = iter->path().filename().string();
                std::cout << "   " << dir_name << std::endl;
            }
        }

        std::cout << "\nExample:" << std::endl;
        std::cout << "  select database_name\n" << std::endl;
    }
};

#endif  // ENGINE_IMPL_HPP
