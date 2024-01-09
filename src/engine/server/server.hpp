/*
 * @FileName   : psoHttp.cpp
 * @CreateAt   : 2021/10/22
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>

#include <httplib.h>
#include <filesystem>
#include <nlohmann/json.hpp>

#include "../parser/sparql_parser.hpp"
#include "../query/query_executor.hpp"
#include "../query/query_plan.hpp"
#include "../store//build_index.hpp"
#include "../store/index.hpp"

std::string db_name;
std::shared_ptr<Index> db_index;

std::vector<std::string> list_db() {
    std::vector<std::string> rdf_db_list;
    std::filesystem::path path("./DB_DATA_ARCHIVE");
    if (!std::filesystem::exists(path)) {
        return rdf_db_list;
    }

    std::filesystem::directory_iterator end_iter;
    for (std::filesystem::directory_iterator iter(path); iter != end_iter; ++iter) {
        if (std::filesystem::is_directory(iter->status())) {
            std::string dir_name = iter->path().filename().string();
            rdf_db_list.emplace_back(dir_name);
        }
    }

    return rdf_db_list;
}

std::vector<std::unordered_map<std::string, std::string>> execute_query(std::string& sparql) {
    if (db_index == 0) {
        std::cout << "database doesn't be loaded correctly." << std::endl;
        return {};
    }

    auto parser = std::make_shared<SPARQLParser>(sparql);

    auto triple_list = parser->triple_list();

    // generate query plan
    auto query_plan = std::make_shared<QueryPlan>(db_index, triple_list, parser->limit());

    // execute query
    auto executor = std::make_shared<QueryExecutor>(db_index, query_plan);

    executor->query();

    auto result = executor->result();
    if (result.empty()) {
        return {};
    }

    std::vector<std::unordered_map<std::string, std::string>> ret;

    // auto variables = parser.getQueryVariables();
    // ret.reserve(variables.size());

    // for (const auto& row : result) {
    //     std::unordered_map<std::string, std::string> item;
    //     for (size_t i = 0; i < variables.size(); ++i) {
    //         std::string var = variables[i].substr(1);  // exclude the first symbol '?'
    //         std::string entity = row[i].substr(1);     // exclude the first symbol '"'
    //         entity.pop_back();                         // remove the last symbol '"'
    //         item.emplace(var, entity);
    //     }
    //     ret.emplace_back(std::move(item));
    // }

    return ret;
}

void list(const httplib::Request& req, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
    std::cout << "Catch list request from http://" << req.remote_addr << ":" << req.remote_port << std::endl;
    nlohmann::json j;
    j["data"] = list_db();
    res.set_content(j.dump(2), "text/plain;charset=utf-8");
}

void info(const httplib::Request& req, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
    std::cout << "Catch info request from http://" << req.remote_addr << ":" << req.remote_port << std::endl;
    std::unordered_map<std::string, uint32_t> data;

    data["triplets"] = db_index->get_triplet_cnt();
    data["predicates"] = db_index->get_predicate_cnt();
    data["entities"] = db_index->get_entity_cnt();

    nlohmann::json j;
    j["data"] = data;
    res.set_content(j.dump(2), "text/plain;charset=utf-8");
}

void query(const httplib::Request& req, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
    std::cout << "Catch query request from http://" << req.remote_addr << ":" << req.remote_port << std::endl;
    if (!req.has_param("sparql")) {
        return;
    }
    std::string sparql = req.get_param_value("sparql");
    nlohmann::json json;
    json["data"] = execute_query(sparql);
    res.set_content(json.dump(2), "text/plain;charset=utf-8");
}

void create(const httplib::Request& req, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
    std::cout << "Catch create request from http://" << req.remote_addr << ":" << req.remote_port
              << std::endl;

    nlohmann::json j;
    if (!req.has_param("rdf")) {
        j["code"] = 5;
        j["message"] = "Didn't specify a rdf name";
        res.set_content(j.dump(2), "text/plain;charset=utf-8");
        return;
    }
    if (!req.has_param("file_name")) {
        j["code"] = 6;
        j["message"] = "Didn't specify a data file name";
        res.set_content(j.dump(2), "text/plain;charset=utf-8");
        return;
    }

    std::string rdf = req.get_param_value("rdf");
    std::string file_name = req.get_param_value("file_name");
    std::cout << "rdf: " << rdf << ", file_name: " << file_name << std::endl;
    IndexBuilder builder(db_name, file_name);
    if (!builder.build()) {
        std::cerr << "Building index data failed, terminal the process." << std::endl;
        exit(1);
    }
    db_name = rdf;

    j["code"] = 1;
    j["message"] = "Create " + rdf + " successfully!";
    std::cout << "rdf have been changed into <" << rdf << ">." << std::endl;
    res.set_content(j.dump(2), "text/plain;charset=utf-8");
}

void load_db(const httplib::Request& req, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
    std::cout << "Catch switch request from http://" << req.remote_addr << ":" << req.remote_port
              << std::endl;

    nlohmann::json j;
    if (!req.has_param("db_name")) {
        j["code"] = 2;
        j["message"] = "Didn't specify a RDF name";
        res.set_content(j.dump(2), "text/plain;charset=utf-8");
        return;
    }

    std::string new_db_name = req.get_param_value("db_name");
    if (new_db_name == db_name) {
        j["code"] = 3;
        j["message"] = "Same RDF, no need to switch";
        res.set_content(j.dump(2), "text/plain;charset=utf-8");
        return;
    }

    if (db_index != 0) {
        db_index->~Index();
    }

    db_index = std::make_shared<Index>(new_db_name);
    db_name = new_db_name;

    j["code"] = 1;
    j["message"] = "RDF have been switched to " + new_db_name;
    std::cout << "RDF have been switched into <" << new_db_name << ">." << std::endl;
    res.set_content(j.dump(2), "text/plain;charset=utf-8");
}

void upload(const httplib::Request& req, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
    std::cout << "Catch upload request from http://" << req.remote_addr << ":" << req.remote_port
              << std::endl;

    nlohmann::json j;

    if (!req.has_file("rdf_file")) {
        j["code"] = 4;
        j["message"] = "Upload file failed";
        res.set_content(j.dump(2), "text/plain;charset=utf-8");
        return;
    }

    auto size = req.files.size();
    auto rdf_file = req.get_file_value("rdf_file");

    {
        std::ofstream ofs(rdf_file.filename);
        ofs << rdf_file.content;
    }

    j["code"] = 1;
    j["message"] =
        "Upload file successfully. file name is " + rdf_file.filename + ", size is " + std::to_string(size);
    res.set_content(j.dump(2), "text/plain;charset=utf-8");
}

bool start_server(std::string port) {
    std::cout << "Running at: http://127.0.0.1:" << port << std::endl;

    httplib::Server svr;
    svr.set_base_dir("./");

    std::string base_url = "/ceirs";

    svr.Post(base_url + "/create", create);    // create RDF
    svr.Post(base_url + "/load_db", load_db);  // load RDF
    svr.Get(base_url + "/list", list);         // list RDF
    svr.Get(base_url + "/info", info);         // show RDF information
    svr.Post(base_url + "/upload", upload);    // upload RDF file
    svr.Post(base_url + "/query", query);      // query on RDF

    // disconnect
    svr.Get(base_url + "/disconnect", [&](const httplib::Request& req, httplib::Response& res) {
        std::cout << "disconnection from http://" << req.remote_addr << ":" << req.remote_port << std::endl;
        nlohmann::json j;
        j["code"] = 1;
        j["message"] = "Disconnected";
        svr.stop();
        res.set_content(j.dump(2), "text/plain;charset=utf-8");
    });
    svr.listen("127.0.0.1", std::stoi(port));
    return 0;
}