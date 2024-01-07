#include <iostream>
#include <iterator>
#include <sstream>

#include <hsinDB/engine.hpp>

#include "parser/args_parser.hpp"

void build(const std::unordered_map<std::string, std::string>& arguments) {
    std::string db_name = arguments.at("name");
    std::string data_file = arguments.at("file");
    hsinDB::Engine::Create(db_name, data_file);
}

void query(const std::unordered_map<std::string, std::string>& arguments) {

    std::string db_name = arguments.at("name");
    std::string sparql_file = arguments.at("file");

    hsinDB::Engine::Query(db_name, sparql_file);

    // auto result = hsinDB::Engine::Query(db_name, sparql_file);
    // std::cout << result->result_size() << " result(s)." << std::endl;
    // std::cout << "==================================================================" << std::endl;
    // std::copy(result->var_begin(), result->var_end(), std::ostream_iterator<std::string>(std::cout, "\t"));
    // std::cout << std::endl;
    // for (auto& item : *result) {
    //     std::copy(item.begin(), item.end(), std::ostream_iterator<std::string>(std::cout, " "));
    //     std::cout << std::endl;
    // }
    // std::cout << "------------------------------------------------------------------" << std::endl;
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
    selector = {
        {ArgsParser::Command_T::Build, &build},
        {ArgsParser::Command_T::Query, &query},
    };

    auto parser = ArgsParser();
    auto command = parser.parse(argc, argv);
    auto arguments = parser.arguments();
    selector[command](arguments);
    return 0;
}
