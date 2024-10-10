#ifndef SERVER_HPP
#define SERVER_HPP

#include <httplib.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>

#include "../parser/sparql_parser.hpp"
#include "../query/query_executor.hpp"
#include "../query/query_plan.hpp"
#include "../query/query_result.hpp"
#include "../store/index.hpp"
#include "../store/index_builder.hpp"

std::string db_name;
std::shared_ptr<IndexRetriever> db_index;

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

void execute_query(std::string& sparql, nlohmann::json& res) {
    if (db_index == 0) {
        std::cout << "database doesn't be loaded correctly." << std::endl;
    }

    auto start = std::chrono::high_resolution_clock::now();

    auto parser = std::make_shared<SPARQLParser>(sparql);
    auto query_plan = std::make_shared<QueryPlan>(db_index, parser->TripleList(), parser->Limit());

    auto executor = std::make_shared<QueryExecutor>(db_index, query_plan);
    executor->Query();

    std::vector<std::string> variables = parser->ProjectVariables();
    const auto variable_indexes = query_plan->MappingVariable(variables);

    res["head"]["vars"] = variables;
    std::vector<std::vector<uint>>& results_id = executor->query_result();

    std::chrono::duration<double, std::milli> diff;
    std::chrono::high_resolution_clock::time_point finish;

    uint cnt = 0;
    if (results_id.size() == 0) {
        finish = std::chrono::high_resolution_clock::now();
        res["results"]["bindings"] = std::vector<uint>();
    } else {
        std::vector<std::vector<std::string>> results_str(results_id.size(),
                                                          std::vector<std::string>(variables.size()));
        cnt = query_result(results_id, results_str, db_index, variable_indexes, parser);
        finish = std::chrono::high_resolution_clock::now();
        res["results"]["bindings"] = results_str;
    }
    diff = finish - start;

    res["results"]["binding_cnt"] = res["results"]["bindings"].size();
    res["results"]["time_cost"] = diff.count();

    std::cout << cnt << " result(s)" << std::endl;
}

void list(const httplib::Request& req, httplib::Response& res) {
    std::cout << "Catch list request from http://" << req.remote_addr << ":" << req.remote_port << std::endl;
    nlohmann::json j;
    j["data"] = list_db();
    res.set_content(j.dump(2), "text/plain;charset=utf-8");
}

void info(const httplib::Request& req, httplib::Response& res) {
    std::cout << "Catch info request from http://" << req.remote_addr << ":" << req.remote_port << std::endl;
    std::unordered_map<std::string, uint32_t> data;

    if (db_index) {
        data["triplets"] = db_index->triplet_cnt();
        data["predicates"] = db_index->predicate_cnt();
        data["entities"] = db_index->entity_cnt();
    }

    nlohmann::json j;
    j["data"] = data;
    res.set_content(j.dump(2), "text/plain;charset=utf-8");
}

void query(const httplib::Request& req, httplib::Response& res) {
    std::cout << "Catch query request from http://" << req.remote_addr << ":" << req.remote_port << std::endl;

    std::string sparql = req.get_param_value("query");
    std::cout << db_name << " " << req.get_param_value("query") << std::endl;
    nlohmann::json response;
    if (db_name != "")
        execute_query(sparql, response);

    res.set_content(response.dump(2), "application/sparql-results+json;charset=utf-8");
}

void create(const httplib::Request& req, httplib::Response& res) {
    std::cout << "Catch create request from http://" << req.remote_addr << ":" << req.remote_port
              << std::endl;

    nlohmann::json body = nlohmann::json::parse(req.body);
    nlohmann::json response;
    if (!body.contains("db_name") || body["db_name"] == "") {
        response["code"] = 5;
        response["message"] = "Didn't specify a rdf name";
        res.set_content(response.dump(2), "text/plain;charset=utf-8");
        return;
    }
    if (!body.contains("file_name") || body["file_name"] == "") {
        response["code"] = 6;
        response["message"] = "Didn't specify a data file name";
        res.set_content(response.dump(2), "text/plain;charset=utf-8");
        return;
    }

    std::string db_name = body["db_name"];
    std::string file_name = body["file_name"];
    std::cout << "db_name: " << db_name << ", file_name: " << file_name << std::endl;
    IndexBuilder builder(db_name, file_name);
    if (!builder.Build()) {
        std::cerr << "Building index data failed, terminal the process." << std::endl;
        exit(1);
    }

    response["code"] = 1;
    response["message"] = "Create " + db_name + " successfully!";
    std::cout << "rdf have been changed into <" << db_name << ">." << std::endl;
    res.set_content(response.dump(2), "text/plain;charset=utf-8");
}

void load_db(const httplib::Request& req, httplib::Response& res) {
    std::cout << "Catch switch request from http://" << req.remote_addr << ":" << req.remote_port
              << std::endl;

    nlohmann::json response;
    nlohmann::json body = nlohmann::json::parse(req.body);

    if (!body.contains("db_name")) {
        response["code"] = 2;
        response["message"] = "Didn't specify a RDF name";
        res.status = 200;
        res.set_content(response.dump(2), "text/plain;charset=utf-8");
        return;
    }

    std::string new_db_name = body["db_name"];

    if (new_db_name == db_name) {
        response["code"] = 3;
        response["message"] = "Same RDF, no need to switch";
        res.status = 200;
        res.set_content(response.dump(2), "text/plain;charset=utf-8");
        return;
    }

    if (db_name != "")
        db_index->close();

    db_index = std::make_shared<IndexRetriever>(new_db_name);
    db_name = new_db_name;

    response["code"] = 1;
    response["message"] = "RDF have been switched to " + db_name;
    std::cout << "RDF have been switched into <" << db_name << ">." << std::endl;
    res.status = 200;
    res.set_content(response.dump(2), "text/plain;charset=utf-8");
}

void close_db(const httplib::Request& req, httplib::Response& res) {
    std::cout << "Catch close request from http://" << req.remote_addr << ":" << req.remote_port << std::endl;
    if (db_name != "") {
        db_index->close();
        db_name = "";
    }

    nlohmann::json response;
    response["code"] = 1;
    response["message"] = "RDF have been closed";
    std::cout << "RDF have been closed" << std::endl;
    res.status = 200;
    res.set_content(response.dump(2), "text/plain;charset=utf-8");
}

void delete_db(const httplib::Request& req, httplib::Response& res) {
    std::cout << "Catch delete request from http://" << req.remote_addr << ":" << req.remote_port
              << std::endl;

    nlohmann::json response;

    nlohmann::json body = nlohmann::json::parse(req.body);

    if (!body.contains("db_name")) {
        response["code"] = 2;
        response["message"] = "Didn't specify a RDF name";
        res.status = 200;
        res.set_content(response.dump(2), "text/plain;charset=utf-8");
        return;
    }

    std::string delete_db_name = body["db_name"];

    if (delete_db_name == db_name && db_name != "") {
        db_index->close();
    }
    db_name = "";

    try {
        std::string path = "./DB_DATA_ARCHIVE/" + delete_db_name;
        fs::remove_all(path);
    } catch (const std::exception& e) {
        response["code"] = 3;
        response["message"] = "Fail to delete DB";
        res.status = 200;
        res.set_content(response.dump(2), "text/plain;charset=utf-8");
        return;
    }

    response["code"] = 1;
    response["message"] = db_name + " RDF have been deleted";
    std::cout << "RDF have been deleted" << std::endl;
    res.status = 200;
    res.set_content(response.dump(2), "text/plain;charset=utf-8");
}

void upload(const httplib::Request& req, httplib::Response& res) {
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

bool start_server(const std::string& ip, const std::string& port, const std::string& db) {
    std::cout << "Running at:" + ip + ":" << port << std::endl;

    httplib::Server svr;

    svr.set_default_headers({{"Access-Control-Allow-Origin", "*"},
                             {"Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS, DELETE"},
                             {"Access-Control-Max-Age", "3600"},
                             {"Access-Control-Allow-Headers", "*"},
                             {"Content-Type", "application/json;charset=utf-8"}});

    svr.set_base_dir("./");

    std::string base_url = "/epei";

    if (db.empty()) {
        svr.Post(base_url + "/create", create);     // create RDF
        svr.Post(base_url + "/load_db", load_db);   // load RDF
        svr.Get(base_url + "/close", close_db);     // close RDF
        svr.Post(base_url + "/delete", delete_db);  // delete RDF
        svr.Get(base_url + "/list", list);          // list RDF
        svr.Get(base_url + "/info", info);          // show RDF information
        svr.Post(base_url + "/upload", upload);     // upload RDF file
        svr.Options(base_url + "/load_db",
                    [](const httplib::Request& req, httplib::Response& res) { res.status = 200; });
        svr.Options(base_url + "/create",
                    [](const httplib::Request& req, httplib::Response& res) { res.status = 200; });
        svr.Options(base_url + "/delete",
                    [](const httplib::Request& req, httplib::Response& res) { res.status = 200; });
    } else {
        db_index = std::make_shared<IndexRetriever>(db);
        db_name = db;
    }

    svr.Get(base_url + "/sparql", query);  // query on RDF
    svr.Options(base_url + "/sparql",
                [](const httplib::Request& req, httplib::Response& res) { res.status = 200; });

    // disconnect
    svr.Get(base_url + "/disconnect", [&](const httplib::Request& req, httplib::Response& res) {
        std::cout << "disconnection from http://" << req.remote_addr << ":" << req.remote_port << std::endl;
        nlohmann::json j;
        j["code"] = 1;
        j["message"] = "Disconnected";
        svr.stop();
        res.set_content(j.dump(2), "text/plain;charset=utf-8");
    });
    svr.listen(ip, std::stoi(port));
    return 0;
}

#endif