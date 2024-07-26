/*
 * @FileName   : args_parser.hpp
 * @CreateAt   : 2022/10/30
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef ARGS_PARSER_HPP
#define ARGS_PARSER_HPP

#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

class ArgsParser {
   public:
    enum CommandT {
        kNone,
        kBuild,
        kQuery,
        kServer,
    };

    const std::string arg_name_ = "name";
    const std::string arg_file_ = "file";
    const std::string arg_ip_ = "ip";
    const std::string arg_port_ = "port";
    const std::string arg_thread_num_ = "thread_num";
    const std::string arg_chunk_size_ = "chunk_size";

   private:
    std::unordered_map<std::string, CommandT> position_ = {
        {"-h", CommandT::kNone},     {"--help", CommandT::kNone},   {"build", CommandT::kBuild},
        {"query", CommandT::kQuery}, {"server", CommandT::kServer},
    };

    std::unordered_map<std::string, void (ArgsParser::*)(const std::unordered_map<std::string, std::string>&)>
        selector_ = {
            {"build", &ArgsParser::Build},
            {"query", &ArgsParser::Query},
            {"server", &ArgsParser::Server},
    };

    const std::string help_info_ =
        "Usage: epei <command> <args>\n"
        "\n"
        "Description:\n"
        "  Common commands for various situations using EPEI.\n"
        "\n"
        "Commands:\n"
        "  build      Build the data index for the given RDF data file path.\n"
        "  query      Query the SPARQL statement for the given file path.\n"
        "  server     Start the EPEI server.\n"
        "\n"
        "args:\n"
        "  -h, --help      Show this help message and exit.\n"
        "  --db, --name <NAME>    Specify the database name.\n"
        "  -f, --file <FILE>    Specify the RDF data file path.\n";

   private:
    std::unordered_map<std::string, std::string> arguments_;

   public:
    CommandT Parse(int argc, char** argv) {
        if (argc == 1) {
            std::cout << help_info_ << std::endl;
            std::cerr << "epei: error: the following arguments are required: command" << std::endl;
            exit(1);
        }

        std::string argv1 = std::string(argv[1]);

        if (argc == 2 && (argv1 == "-h" || argv1 == "--help")) {
            std::cout << help_info_ << std::endl;
            exit(1);
        }

        if (!position_.count(argv1)) {
            std::cout << help_info_ << std::endl;
            std::cerr << "epei: error: the following arguments are required: command" << std::endl;
            exit(1);
        }

        std::unordered_map<std::string, std::string> args;
        for (int i = 2; i < argc; i += 2) {
            if (argv[i][0] != '-') {
                std::cerr << "hinDB: error: unrecognized arguments: " << argv[i] << std::endl;
                exit(1);
            }
            if (std::strcmp(argv[i], "-h") == 0 || std::strcmp(argv[i], "--help") == 0) {
                args.emplace(argv[i], "");
                i = i - 1;  // because when it enters the next loop, the i will be plus 2,
                // so we need to decrease one in order to ensure the rest flags and arguments are one-to-one
                // correspondence
                continue;
            }
            if (i + 1 >= argc || argv[i + 1][0] == '-') {
                std::cerr << "hinDB: error: argument " << argv[i] << ": expected one argument" << std::endl;
                exit(1);
            }
            args.emplace(argv[i], argv[i + 1]);
        }

        // 执行对应的命令的解析器
        (this->*selector_[argv1])(args);
        return position_[argv1];
    }

    const std::unordered_map<std::string, std::string>& Arguments() const { return arguments_; }

   private:
    const std::string build_info_ =
        "Usage: epei build [--db, --database DATABASE] [-f,--file FILE]\n"
        "\n"
        "Description:\n"
        "Build the data index for the given RDF data file path.\n"
        "\n"
        "Options:\n"
        "  --db, --database <DATABASE>   Specify the database name.\n"
        "  -f, --file <FILE>   Specify the RDF data file path.\n"
        "\n"
        "Optional Arguments:\n"
        "  -h, --help          Show this help message and exit.\n"
        "\n"
        "Examples:\n"
        "  epei build --db my_database -f /path/to/data.rdf\n";

    const std::string query_info_ =
        "Usage: epei query [--db, --database DATABASE] [-f,--file FILE]\n"
        "\n"
        "Description:\n"
        "Query the data from the given RDF database using SPARQLs in the given file.\n"
        "\n"
        "Options:\n"
        "  --db, --database <DATABASE>   Specify the database name.\n"
        "  -f, --file <FILE>   Specify the RDF data file path.\n"
        "\n"
        "Optional Arguments:\n"
        "  -h, --help          Show this help message and exit.\n"
        "\n"
        "Examples:\n"
        "  epei query --db my_database -f /path/to/query.sparql\n";

    const std::string serve_info_ =
        "Usage: epei server [-p,--port PORT]\n"
        "\n"
        "Description:\n"
        "  Start the HTTP server for EPEI.\n"
        "\n"
        "Options:\n"
        "  -ip                 Specify the HTTP server ip.\n"
        "  -p, --port <PORT>   Specify the HTTP server port.\n"
        "\n"
        "Optional Arguments:\n"
        "  -h, --help          Show this help message and exit.\n"
        "\n"
        "Examples:\n"
        "  epei server --port 8080;\n";

   private:
    void Build(const std::unordered_map<std::string, std::string>& args) {
        if (args.empty() || args.count("-h") || args.count("--help")) {
            std::cout << build_info_ << std::endl;
            exit(1);
        }
        if ((!args.count("--db") && !args.count("--database")) ||
            (!args.count("-f") && !args.count("--file"))) {
            std::cerr << "usage: epei build [--db DATABASE] [-f FILE]" << std::endl;
            std::cerr << "epei: error: the following arguments are required: [--db DATABASE] [-f FILE]"
                      << std::endl;
            exit(1);
        }
        arguments_[arg_name_] = args.count("--db") ? args.at("--db") : args.at("--database");
        arguments_[arg_file_] = args.count("-f") ? args.at("-f") : args.at("--file");
    }

    void Query(const std::unordered_map<std::string, std::string>& args) {
        if (args.count("-h") || args.count("--help")) {
            std::cout << query_info_ << std::endl;
            exit(1);
        }

        // if (!args.count("--db") && !args.count("--database")) {
        //     std::cerr << "usage: epei query [--db DATABASE]" << std::endl;
        //     std::cerr << "epei: error: the following argument is required: [--db DATABASE]" << std::endl;
        //     exit(1);
        // }
        if (args.count("--db"))
            arguments_[arg_name_] = args.count("--db") ? args.at("--db") : args.at("--database");
        if (args.count("-f"))
            arguments_[arg_file_] = args.at("-f");
        else if (args.count("--file"))
            arguments_[arg_file_] = args.at("--file");
        else
            arguments_[arg_file_] = "";

        size_t default_thread_num = std::thread::hardware_concurrency();
        if (args.count("-t") && default_thread_num >= std::stoull(args.at("-t")))
            arguments_[arg_thread_num_] = args.at("-t");
        else
            arguments_[arg_thread_num_] = std::to_string(default_thread_num);
    }

    void Server(const std::unordered_map<std::string, std::string>& args) {
        if (args.empty() || args.count("-h") || args.count("--help")) {
            std::cout << serve_info_ << std::endl;
            exit(1);
        }
        if ((!args.count("-p") && !args.count("--port")) || !args.count("-ip")) {
            std::cerr << "usage: epei server [-ip IP] [-p PORT]" << std::endl;
            std::cerr << "epei: error: the following arguments are required: [-ip IP] [-p PORT]" << std::endl;
            exit(1);
        }
        arguments_[arg_ip_] = args.at("-ip");
        arguments_[arg_port_] = args.count("-p") ? args.at("-p") : args.at("--port");
        if (!IsNumber(arguments_[arg_port_])) {
            std::cerr << "epei: error: the argument [-p PORT] requires a number, but got "
                      << arguments_[arg_port_] << std::endl;
            exit(1);
        }
    }

    inline bool IsNumber(const std::string& s) {
        return std::all_of(s.begin(), s.end(), [](char c) { return std::isdigit(c); });
    }
};

#endif  // ARGS_PARSER_HPP
