#include <iostream>
#include <iterator>
#include <sstream>

#include <epei/engine.hpp>

#include "parser/args_parser.hpp"

void build(const std::unordered_map<std::string, std::string>& arguments) {
    std::string db_name = arguments.at("name");
    std::string data_file = arguments.at("file");
    epei::Engine::Create(db_name, data_file);
}

void query(const std::unordered_map<std::string, std::string>& arguments) {
    // std::string db_name = arguments.at("name");
    // std::string sparql_file = arguments.at("file");

    epei::Engine::Query("name", "file");
}

void server(const std::unordered_map<std::string, std::string>& arguments) {
    std::string ip = arguments.at("ip");
    std::string port = arguments.at("port");
    epei::Engine::Server(ip, port);
}

struct EnumClassHash {
    template <typename T>
    std::size_t operator()(T t) const {
        return static_cast<std::size_t>(t);
    }
};

std::unordered_map<ArgsParser::Command_T,
                   void (*)(const std::unordered_map<std::string, std::string>&),
                   EnumClassHash>
    selector;

int main(int argc, char** argv) {
    selector = {{ArgsParser::Command_T::Build, &build},
                {ArgsParser::Command_T::Query, &query},
                {ArgsParser::Command_T::Server, &server}};

    auto parser = ArgsParser();
    auto command = parser.parse(argc, argv);
    auto arguments = parser.arguments();
    selector[command](arguments);
    return 0;
}
