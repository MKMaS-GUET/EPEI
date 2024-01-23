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
    enum Command_T {
        None,
        Build,
        Query,
        Server,
    };

    const std::string _arg_name = "name";
    const std::string _arg_file = "file";
    const std::string _arg_ip = "ip";
    const std::string _arg_port = "port";
    const std::string _arg_thread_num = "thread_num";
    const std::string _arg_chunk_size = "chunk_size";

   private:
    std::unordered_map<std::string, Command_T> _position = {
        {"-h", Command_T::None},     {"--help", Command_T::None},   {"build", Command_T::Build},
        {"query", Command_T::Query}, {"server", Command_T::Server},
    };

    std::unordered_map<std::string, void (ArgsParser::*)(const std::unordered_map<std::string, std::string>&)>
        _selector = {
            {"build", &ArgsParser::build},
            {"query", &ArgsParser::query},
            {"server", &ArgsParser::server},
    };

    const std::string _help_info =
        "Usage: epei <command> [<args>]\n"
        "\n"
        "Description:\n"
        "  Common commands for various situations using EPEI.\n"
        "\n"
        "Commands:\n"
        "  build      Build the data index for the given RDF data file path.\n"
        "  query      Query the SPARQL statement for the given file path.\n"
        "  server     Start the EPEI server.\n"
        "\n"
        "Options:\n"
        "  -h, --help      Show this help message and exit.\n"
        "\n"
        "Command-specific options:\n"
        "  build:\n"
        "    -n, --name <NAME>    Specify the database name.\n"
        "    -f, --file <FILE>    Specify the RDF data file path.\n"
        "\n"
        "Positional Arguments:\n"
        "  command       The command to run (e.g., build, query, server).\n";

   private:
    std::unordered_map<std::string, std::string> _arguments;

   public:
    Command_T parse(int argc, char** argv) {
        if (argc == 1) {
            std::cout << _help_info << std::endl;
            std::cerr << "epei: error: the following arguments are required: command" << std::endl;
            exit(1);
        }

        std::string argv1 = std::string(argv[1]);

        if (argc == 2 && (argv1 == "-h" || argv1 == "--help")) {
            std::cout << _help_info << std::endl;
            exit(1);
        }

        if (!_position.count(argv1)) {
            std::cout << _help_info << std::endl;
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
        (this->*_selector[argv1])(args);
        return _position[argv1];
    }

    const std::unordered_map<std::string, std::string>& arguments() const { return _arguments; }

   private:
    const std::string _build_info =
        "Usage: epei build [-n,--name NAME] [-f,--file FILE]\n"
        "\n"
        "Description:\n"
        "Build the data index for the given RDF data file path.\n"
        "\n"
        "Options:\n"
        "  -n, --name <NAME>   Specify the database name.\n"
        "  -f, --file <FILE>   Specify the RDF data file path.\n"
        "\n"
        "Optional Arguments:\n"
        "  -h, --help          Show this help message and exit.\n"
        "\n"
        "Examples:\n"
        "  epei build -n my_database -f /path/to/data.rdf\n";

    const std::string _query_info =
        "usage: epei query"
        "\n";

    const std::string _serve_info =
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
    void build(const std::unordered_map<std::string, std::string>& args) {
        if (args.empty() || args.count("-h") || args.count("--help")) {
            std::cout << _build_info << std::endl;
            exit(1);
        }
        if ((!args.count("-db") && !args.count("--database")) ||
            (!args.count("-f") && !args.count("--file"))) {
            std::cerr << "usage: epei build [-db DATABASE] [-f FILE]" << std::endl;
            std::cerr << "epei: error: the following arguments are required: [-db DATABASE] [-f FILE]"
                      << std::endl;
            exit(1);
        }
        _arguments[_arg_name] = args.count("-db") ? args.at("-db") : args.at("--database");
        _arguments[_arg_file] = args.count("-f") ? args.at("-f") : args.at("--file");
    }

    void query(const std::unordered_map<std::string, std::string>& args) {
        if (args.count("-h") || args.count("--help")) {
            std::cout << _query_info << std::endl;
            exit(1);
        }

        // if (!args.count("-db") && !args.count("--database")) {
        //     std::cerr << "usage: epei query [-db DATABASE]" << std::endl;
        //     std::cerr << "epei: error: the following argument is required: [-db DATABASE]" << std::endl;
        //     exit(1);
        // }
        // _arguments[_arg_name] = args.count("-db") ? args.at("-db") : args.at("--database");
        // if (args.count("-f"))
        //     _arguments[_arg_file] = args.at("-f");
        // else if (args.count("--file"))
        //     _arguments[_arg_file] = args.at("--file");
        // else
        //     _arguments[_arg_file] = "";

        // size_t default_thread_num = std::thread::hardware_concurrency();
        // if (args.count("-t") && default_thread_num >= std::stoull(args.at("-t")))
        //     _arguments[_arg_thread_num] = args.at("-t");
        // else
        //     _arguments[_arg_thread_num] = std::to_string(default_thread_num);
    }

    void server(const std::unordered_map<std::string, std::string>& args) {
        if (args.empty() || args.count("-h") || args.count("--help")) {
            std::cout << _serve_info << std::endl;
            exit(1);
        }
        if ((!args.count("-p") && !args.count("--port")) || !args.count("-ip")) {
            std::cerr << "usage: epei server [-ip IP] [-p PORT]" << std::endl;
            std::cerr << "epei: error: the following arguments are required: [-ip IP] [-p PORT]" << std::endl;
            exit(1);
        }
        _arguments[_arg_ip] = args.at("-ip");
        _arguments[_arg_port] = args.count("-p") ? args.at("-p") : args.at("--port");
        if (!isNumber(_arguments[_arg_port])) {
            std::cerr << "epei: error: the argument [-p PORT] requires a number, but got "
                      << _arguments[_arg_port] << std::endl;
            exit(1);
        }
    }

    inline bool isNumber(const std::string& s) {
        return std::all_of(s.begin(), s.end(), [](char c) { return std::isdigit(c); });
    }
};

#endif  // ARGS_PARSER_HPP
