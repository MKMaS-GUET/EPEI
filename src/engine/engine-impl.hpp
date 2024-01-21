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

    void list_db() {
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
    }

    void execute_sparql(std::vector<std::string> sparqls,
                        std::shared_ptr<Index> index,
                        std::string file_name) {
        // index->load_data(entities);
        FILE* output_file = NULL;
        if (file_name != "")
            output_file = fopen(file_name.c_str(), "w");

        for (long unsigned int i = 0; i < sparqls.size(); i++) {
            std::string sparql = sparqls[i];

            if (sparqls.size() > 1) {
                if (output_file == NULL)
                    printf("%ld ------------------------------------------------------------------\n", i + 1);
                else
                    fprintf(output_file,
                            "%ld ------------------------------------------------------------------\n",
                            i + 1);
            }

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
            int cnt = 0;
            if (output_file == NULL)
                cnt = query_result(executor->result(), index, query_plan, parser);
            else
                cnt = query_result(executor->result(), index, query_plan, parser, output_file);
            auto mapping_finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> mapping_diff = mapping_finish - mapping_start;

            auto finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = finish - start;

            std::chrono::duration<double, std::milli> plan_time = plan_end - start;

            if (output_file == NULL) {
                printf("%d result(s).\n", cnt);
                printf("generate plan takes %lf ms.\n", plan_time.count());
                printf("execute takes %lf ms.\n", executor->duration());
                printf("output result takes %lf ms.\n", mapping_diff.count());

                printf("query time cost %lf ms.\n", diff.count());
            } else {
                fprintf(output_file, "%d result(s).\n", cnt);
                fprintf(output_file, "generate plan takes %lf ms.\n", plan_time.count());
                fprintf(output_file, "execute takes %lf ms.\n", executor->duration());
                fprintf(output_file, "output result takes %lf ms.\n", mapping_diff.count());

                fprintf(output_file, "query time cost %lf ms.\n", diff.count());
            }

            // printf("%s", sparql.c_str());
        }
    }

    void query(const std::string& name, const std::string& file) {
        std::string cmd;
        std::string db;
        std::shared_ptr<Index> index;
        while (true) {
            list_db();
            std::cout << ">";

            std::cin >> cmd;
            if (cmd == "select") {
                std::cin >> db;
                index = std::make_shared<Index>(db);
                std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

                while (true) {
                    std::cout << ">";
                    std::string sparql;
                    std::string line;
                    std::string word;
                    std::getline(std::cin, line);
                    std::istringstream iss(line);
                    bool cmd_flag = true;
                    while (iss >> word) {
                        if (cmd_flag) {
                            cmd = word;
                            cmd_flag = false;
                        } else {
                            sparql += word + " ";
                        }
                    }

                    if (sparql.size() > 1)
                        sparql = sparql.substr(0, sparql.size() - 1);

                    std::vector<std::string> sparqls;
                    if (cmd == "sparql") {
                        sparqls.push_back(sparql);
                        execute_sparql(sparqls, index, "");
                    }

                    if (cmd == "file") {
                        std::string rest_cmd = sparql;
                        std::istringstream iss(sparql);
                        std::string token;

                        std::string input_file, output_file = "";
                        while (iss >> token) {
                            if (token == "-i") {
                                iss >> input_file;
                            } else if (token == "-o") {
                                iss >> output_file;
                            }
                        }

                        if (input_file != "") {
                            std::ifstream in(input_file, std::ifstream::in);
                            if (in.is_open()) {
                                std::string line;
                                while (std::getline(in, sparql)) {
                                    sparqls.push_back(sparql);
                                }
                                in.close();
                            }
                            execute_sparql(sparqls, index, output_file);
                        }
                    }

                    if (cmd == "change") {
                        index->close();
                        break;
                    }

                    if (cmd == "exit") {
                        exit(0);
                    }
                }

            } else if (cmd == "exit") {
                exit(0);
            }
        }

        // parse SPARQL statement
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

#endif  // ENGINE_IMPL_HPP
