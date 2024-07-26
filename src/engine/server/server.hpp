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

std::string execute_query(std::string& sparql, nlohmann::json& res) {
    if (db_index == 0) {
        std::cout << "database doesn't be loaded correctly." << std::endl;
        return {};
    }
    auto start = std::chrono::high_resolution_clock::now();

    auto parser = std::make_shared<SPARQLParser>(sparql);

    // generate query plan
    auto query_plan = std::make_shared<QueryPlan>(db_index, parser->TripleList(), parser->Limit());

    // execute query
    auto executor = std::make_shared<QueryExecutor>(db_index, query_plan);

    executor->Query();

    std::vector<std::vector<uint>> results_id = executor->query_result();

    auto variables = parser->ProjectVariables();

    auto finish = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> diff = finish - start;

    std::string results = "0 result";

    uint cnt = 0;
    if (results_id.size() != 0) {
        cnt = query_result(results_id, results, db_index, query_plan, parser);
    }

    res["results"] = results;
    res["cnt"] = std::to_string(cnt);

    double time = diff.count();
    int integer_part = static_cast<int>(time);
    int decimal_part = static_cast<int>((time - integer_part) * 100);
    res["time"] = std::to_string(integer_part).append(".").append(std::to_string(decimal_part));
    return results;
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
        data["triplets"] = db_index->GetTripletCnt();
        data["predicates"] = db_index->GetPredicateCnt();
        data["entities"] = db_index->GetEntityCnt();
    }

    nlohmann::json j;
    j["data"] = data;
    res.set_content(j.dump(2), "text/plain;charset=utf-8");
}

void query(const httplib::Request& req, httplib::Response& res) {
    std::cout << "Catch query request from http://" << req.remote_addr << ":" << req.remote_port << std::endl;

    nlohmann::json body = nlohmann::json::parse(req.body);

    if (!body.contains("sparql")) {
        return;
    }
    std::string sparql = body["sparql"];
    nlohmann::json response;
    if (db_name != "")
        execute_query(sparql, response);

    res.set_content(response.dump(2), "text/plain;charset=utf-8");
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

    db_index = std::make_shared<Index>(new_db_name);
    phmap::flat_hash_set<std::string> entities;

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

bool start_server(std::string ip, std::string port) {
    std::cout << "Running at:" + ip + ":" << port << std::endl;

    httplib::Server svr;

    svr.set_default_headers({{"Access-Control-Allow-Origin", "*"},
                             {"Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS, DELETE"},
                             {"Access-Control-Max-Age", "3600"},
                             {"Access-Control-Allow-Headers", "*"},
                             {"Content-Type", "application/json;charset=utf-8"}});

    svr.set_base_dir("./");

    std::string base_url = "/peirs";

    svr.Post(base_url + "/create", create);     // create RDF
    svr.Post(base_url + "/load_db", load_db);   // load RDF
    svr.Get(base_url + "/close", close_db);     // close RDF
    svr.Post(base_url + "/delete", delete_db);  // delete RDF
    svr.Get(base_url + "/list", list);          // list RDF
    svr.Get(base_url + "/info", info);          // show RDF information
    svr.Post(base_url + "/upload", upload);     // upload RDF file
    svr.Post(base_url + "/query", query);       // query on RDF

    svr.Options(base_url + "/load_db",
                [](const httplib::Request& req, httplib::Response& res) { res.status = 200; });
    svr.Options(base_url + "/create",
                [](const httplib::Request& req, httplib::Response& res) { res.status = 200; });
    svr.Options(base_url + "/query",
                [](const httplib::Request& req, httplib::Response& res) { res.status = 200; });
    svr.Options(base_url + "/delete",
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