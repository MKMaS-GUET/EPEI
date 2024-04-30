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

#include <ppfi/engine.hpp>

#include "parser/sparql_parser.hpp"
#include "query/query_executor.hpp"
#include "query/query_plan.hpp"
#include "query/query_result.hpp"
#include "server/server.hpp"
#include "store/build_index.hpp"
#include "store/index.hpp"

class ppfi::Engine::Impl {
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

    void execute_sparql(std::vector<std::string> sparqls,
                        std::shared_ptr<Index> index,
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
            auto triple_list = parser->triple_list();

            // generate query plan
            auto query_plan = std::make_shared<QueryPlan>(index, triple_list, parser->limit());

            auto plan_end = std::chrono::high_resolution_clock::now();

            // execute query
            auto executor = std::make_shared<QueryExecutor>(index, query_plan);

            executor->query();

            auto mapping_start = std::chrono::high_resolution_clock::now();
            uint cnt = 0;
            if (file_name == "")
                cnt = query_result(executor->result(), index, query_plan, parser);
            else
                cnt = query_result(executor->result(), index, query_plan, parser, output_file);
            auto mapping_finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> mapping_diff = mapping_finish - mapping_start;

            auto finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = finish - start;

            std::chrono::duration<double, std::milli> plan_time = plan_end - start;

            if (file_name == "") {
                std::cout << cnt << " result(s).\n";
                std::cout << "generate plan takes " << plan_time.count() << " ms.\n";
                std::cout << "execute takes " << executor->duration() << " ms.\n";
                std::cout << "output result takes " << mapping_diff.count() << " ms.\n";
                std::cout << "query cost " << diff.count() << " ms." << std::endl;
            } else {
                output_file << cnt << "  result(s).\n";
                output_file << "generate plan takes " << plan_time.count() << " ms.\n";
                output_file << "execute takes " << executor->duration() << " ms.\n";
                output_file << "output result takes " << mapping_diff.count() << " ms.\n";
                output_file << "query cost " << diff.count() << " ms." << std::endl;
            }

            // printf("%s", sparql.c_str());
        }
    }

    void query(const std::string& name, const std::string& file) {
        if (name != "" and file != "") {
            std::shared_ptr<Index> index = std::make_shared<Index>(name);
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
            execute_sparql(sparqls, index, "");
            exit(0);
        } else {
            std::string db_help = "select [database name]";
            std::string query_help =
                "Usage: sparql [options]\n"
                "\n"
                "Description:\n"
                "  Run a SPARQL query.\n"
                "\n\n"
                "Usage: file [options] [arguments]\n"
                "\n"
                "Description:\n"
                "  Run SPARQL queries from a file and output the results to a file.\n"
                "\n"
                "Options:\n"
                "  -i, --input <file>    Specify the input file containing SPARQL queries.\n"
                "  -o, --output [file]   Specify the output file for the query results.\n";

            std::string cmd;
            std::string db;
            std::shared_ptr<Index> index;
            list_db();
            while (true) {
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
                        } else if (cmd == "file") {
                            std::string rest_cmd = sparql;
                            std::istringstream iss(sparql);
                            std::string token;

                            std::string input_file, output_file = "";
                            while (iss >> token) {
                                if (token == "-i" || token == "--input") {
                                    iss >> input_file;
                                } else if (token == "-o" || token == "--output") {
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
                        } else if (cmd == "help") {
                            std::cout << query_help << std::endl;
                        } else if (cmd == "change") {
                            index->close();
                            list_db();
                            break;
                        } else if (cmd == "exit") {
                            exit(0);
                        } else {
                            std::cout << query_help << std::endl;
                        }

                        // sleep(1);
                    }

                } else if (cmd == "help") {
                    std::cout << db_help << std::endl;
                } else if (cmd == "exit") {
                    exit(0);
                } else {
                    list_db();
                }
            }
        }

        // parse SPARQL statement
    }

    void server(const std::string& ip, const std::string& port) { start_server(ip, port); }

   private:
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

        std::cout << "\nExample:" << std::endl;
        std::cout << "  select database_name\n" << std::endl;
    }

    // void get_entity(phmap::flat_hash_set<std::string>& entities, std::string sparql) {
    //     std::regex pattern(R"(\{([^{}]*)\})");
    //     std::regex triplet_Pattern(
    //         R"(((\?.*?\s+)|(<.*?>)\s+)((\?.*?\s+)|(<.*?>)\s+)((\?.*?\s+)|(<.*?>)\s+)\.)");
    //     std::smatch match;
    //     if (std::regex_search(sparql, match, pattern)) {
    //         std::string triplets_str = match[1].str();
    //         std::sregex_iterator triplet_iter(triplets_str.begin(), triplets_str.end(), triplet_Pattern);
    //         std::sregex_iterator end;
    //         while (triplet_iter != end) {
    //             std::smatch triplet = *triplet_iter;
    //             std::istringstream iss(triplet[0]);
    //             std::string part1, part2, part3;
    //             iss >> part1 >> part2 >> part3;
    //             if (part1[0] == '<' && part1.back() == '>') {
    //                 entities.insert(part1);
    //             }
    //             if (part3[0] == '<' && part3.back() == '>') {
    //                 entities.insert(part3);
    //             }
    //             ++triplet_iter;
    //         }
    //     }
    //     return;
    // }
};

#endif  // ENGINE_IMPL_HPP
