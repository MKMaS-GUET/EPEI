/*
 * @FileName   : routes.hpp
 * @CreateAt   : 2022/11/22
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef COMPRESSED_ENCODED_TREE_INDEX_SERVER_HPP
#define COMPRESSED_ENCODED_TREE_INDEX_SERVER_HPP

#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#if __has_include(<filesystem>)

#include <filesystem>

namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
error "Missing the <filesystem> header."
#endif

#include "httplib.h"
#include "nlohmann/json.hpp"

#include "hsinDB/engine.hpp"

using json = nlohmann::json;

class Server {
   private:
    std::string _db_name;
    //    httplib::Server _svr;
    //    std::shared_ptr<hsinDB::Engine::Option> _engine;

   public:
    explicit Server(const std::string& db_name,
                    const std::string& base_url = "",
                    const std::string& base_dir = "./")
        : _db_name(db_name) {
        //            _engine(hsinDB::Engine::Open(db_name)) {
        //        _svr.set_base_dir(base_dir);
        //        _svr.Get(base_url + "/", [&](const httplib::Request &req, httplib::Response &res) {
        //        base(req, res); }); _svr.Get(base_url + "/list", [&](const httplib::Request &req,
        //        httplib::Response &res) { list(req, res); }); _svr.Get(base_url + "/info", [&](const
        //        httplib::Request &req, httplib::Response &res) { info(req, res); }); _svr.Post(base_url +
        //        "/create", [&](const httplib::Request &req, httplib::Response &res) { create(req, res); });
        //        _svr.Post(base_url + "/open", [&](const httplib::Request &req, httplib::Response &res) {
        //        open(req, res); }); _svr.Post(base_url + "/upload", [&](const httplib::Request &req,
        //        httplib::Response &res) { upload(req, res); }); _svr.Post(base_url + "/execute", [&](const
        //        httplib::Request &req, httplib::Response &res) { execute(req, res); }); _svr.Post(base_url +
        //        "/stop", [&](const httplib::Request &req, httplib::Response &res) { stop(req, res); });
    }

    ~Server() {
        //        _engine->close();
    }

    //    void base(const httplib::Request &req, httplib::Response &res) {
    //        res.set_header("Access-Control-Allow-Origin", "*");
    //        std::cout << "Catch base request from http://" << req.remote_addr << ":" << req.remote_port <<
    //        std::endl;
    //
    //        json j;
    //        j["code"] = 200;
    //        j["message"] = "Remote database: " + _db_name + " is connected.";
    //        json data;
    //        data["name"] = _db_name;
    //        j["data"] = data;
    //        res.set_content(j.dump(2), "application/json;charset=utf-8");
    //    }
    //
    //    void list(const httplib::Request &req, httplib::Response &res) {
    //        res.set_header("Access-Control-Allow-Origin", "*");
    //        std::cout << "Catch list request from http://" << req.remote_addr << ":" << req.remote_port <<
    //        std::endl; json j; j["code"] = 200; j["message"] = "Get the list of RDF succeeded!"; j["data"] =
    //        get_database_list(); res.set_content(j.dump(2), "application/json;charset=utf-8");
    //    }
    //
    //    void info(const httplib::Request &req, httplib::Response &res) {
    //        res.set_header("Access-Control-Allow-Origin", "*");
    //        std::cout << "Catch info request from http://" << req.remote_addr << ":" << req.remote_port <<
    //        std::endl;
    //
    //        auto session = _engine->newSession();
    //        std::unordered_map<std::string, uint32_t> data = {
    //                {"triplets", session->getTripletSize()},
    //                {"predicates", session->getPredicateSize()},
    //                {"entities", session->getEntitySize()},
    //        };
    //
    //        json j;
    //        j["code"] = 200;
    //        j["message"] = "Get the basic information of " + _db_name + " succeeded!";
    //        j["data"] = data;
    //        res.set_content(j.dump(2), "application/json;charset=utf-8");
    //    }
    //
    //    void create(const httplib::Request &req, httplib::Response &res) {
    //        res.set_header("Access-Control-Allow-Origin", "*");
    //        std::cout << "Catch create request from http://" << req.remote_addr << ":" << req.remote_port <<
    //        std::endl;
    //
    //        json j;
    //        if (!req.has_param("rdf")) {
    //            j["code"] = 300;
    //            j["message"] = "Didn't specify an RDF name.";
    //            res.set_content(j.dump(2), "application/json;charset=utf-8");
    //            return;
    //        }
    //        if (!req.has_param("file")) {
    //            j["code"] = 300;
    //            j["message"] = "Didn't specify an RDF data filename.";
    //            res.set_content(j.dump(2), "application/json;charset=utf-8");
    //            return;
    //        }
    //
    //        std::string rdf_name = req.get_param_value("rdf");
    //        std::string data_file = req.get_param_value("file");
    //
    //        _engine = hsinDB::Engine::Create(rdf_name, data_file);
    //        _db_name = rdf_name;
    //
    //        j["code"] = 200;
    //        j["message"] = "Create and switch into " + rdf_name + " succeeded!";
    //        res.set_content(j.dump(2), "application/json;charset=utf-8");
    //    }
    //
    //    void open(const httplib::Request &req, httplib::Response &res) {
    //        res.set_header("Access-Control-Allow-Origin", "*");
    //        std::cout << "Catch open request from http://" << req.remote_addr << ":" << req.remote_port <<
    //        std::endl;
    //
    //        json j;
    //        if (!req.has_param("rdf")) {
    //            j["code"] = 300;
    //            j["message"] = "Need an RDF name.";
    //            res.set_content(j.dump(2), "application/json;charset=utf-8");
    //            return;
    //        }
    //
    //        std::string rdf_name = req.get_param_value("rdf");
    //        if (rdf_name == _db_name) {
    //            j["code"] = 304;
    //            j["message"] = "Same RDF, no need to be opened repeatedly.";
    //            res.set_content(j.dump(2), "application/json;charset=utf-8");
    //            return;
    //        }
    //
    //        _engine = hsinDB::Engine::Open(rdf_name);
    //        _db_name = rdf_name;
    //
    //        j["code"] = 200;
    //        j["message"] = "Open " + rdf_name + " succeeded.";
    //        res.set_content(j.dump(2), "application/json;charset=utf-8");
    //    }
    //
    //    void upload(const httplib::Request &req, httplib::Response &res) {
    //        res.set_header("Access-Control-Allow-Origin", "*");
    //        std::cout << "Catch upload request from http://" << req.remote_addr << ":" << req.remote_port <<
    //        std::endl;
    //
    //        json j;
    //        if (!req.has_file("file")) {
    //            j["code"] = 300;
    //            j["message"] = "Upload file failed! Nee an RDF file.";
    //            std::cout << j["message"] << std::endl;
    //            res.set_content(j.dump(2), "application/json;charset=utf-8");
    //            return;
    //        }
    //
    //        auto size = req.files.size();
    //        auto rdf_file = req.get_file_value("file");
    //
    //        {
    //            std::ofstream ofs(rdf_file.filename);
    //            ofs << rdf_file.content;
    //        }
    //
    //        j["code"] = 200;
    //        j["message"] = "Upload file " + rdf_file.filename
    //                       + " with size of " + std::to_string(size) + " succeeded!";
    //        std::cout << j["message"] << std::endl;
    //        res.set_content(j.dump(2), "application/json;charset=utf-8");
    //    }
    //
    //    void execute(const httplib::Request &req, httplib::Response &res) {
    //        res.set_header("Access-Control-Allow-Origin", "*");
    //        std::cout << "Catch execute request from http://" << req.remote_addr << ":" << req.remote_port
    //        << std::endl;
    //
    //        json j;
    //        if (!req.has_param("sparql")) {
    //            j["code"] = 300;
    //            j["message"] = "Execute SPARQL failed, please upload SPARQL statement.";
    //            res.set_content(j.dump(2), "application/json;charset=utf-8");
    //            return;
    //        }
    //        std::string sparql = req.get_param_value("sparql");
    //
    //        std::cout << "[INFO] SPARQL: " << sparql << std::endl;
    //
    //        j["code"] = 200;
    //        j["message"] = "Execute SPARQL succeeded.";
    //        j["data"] = execute_query(sparql);
    //        res.set_content(j.dump(2), "application/json;charset=utf-8");
    //    }
    //
    //    void stop(const httplib::Request &req, httplib::Response &res) {
    //        res.set_header("Access-Control-Allow-Origin", "*");
    //        std::cout << "Catch stop request from http://" << req.remote_addr << ":" << req.remote_port <<
    //        std::endl; json j; j["code"] = 200; j["message"] = "Remote database: " + _db_name + " is
    //        disconnected"; res.set_content(j.dump(2), "application/json;charset=utf-8");
    //
    //        _svr.stop();
    //    }

    void start(std::string&& host, int port) {
        //        _svr.listen(host, port);
    }

   private:
    //    std::vector<std::string> get_database_list() {
    //        std::vector<std::string> ret;
    //        fs::path path("./DB_DATA_ARCHIVE");
    //        if (!fs::exists(path)) {
    //            return ret;
    //        }
    //
    //        fs::directory_iterator end_iter;
    //        for (fs::directory_iterator iter(path); iter != end_iter; ++iter) {
    //            if (fs::is_directory(iter->status())) {
    //                std::string dir_name = iter->path().filename().string();
    //                ret.emplace_back(dir_name);
    //            }
    //        }
    //        return ret;
    //    }
    //
    //    json execute_query(const std::string &sparql) {
    //        if (sparql.empty()) {
    //            return {};
    //        }
    //        const auto &session = _engine->newSession();
    //        size_t thread_num = std::thread::hardware_concurrency();
    //        size_t chunk_size = thread_num >> 1;
    //        const auto &result = session->execute(sparql, thread_num, chunk_size);
    //        const auto &vars = result->query_variables();
    //        const auto &exist = result->exist_entities();
    //        const auto &pattern = result->query_pattern();
    //
    //        std::vector<std::unordered_map<std::string, std::string>> ret;
    //        ret.reserve(result->result_size());
    //        for (const auto &item: *result) {
    //            std::unordered_map<std::string, std::string> binding;
    //            for (size_t i = 0; i < vars.size(); ++i) {
    //                binding.emplace(vars[i], item[i]);
    //            }
    //            ret.emplace_back(std::move(binding));
    //        }
    //
    //        std::cout << "[INFO] Query result number: " << ret.size() << std::endl;
    //        json j;
    //        j["exist"] = exist;
    //        j["pattern"] = pattern;
    //        j["bindings"] = ret;
    //        return j;
    //    }
};

#endif  // COMPRESSED_ENCODED_TREE_INDEX_SERVER_HPP
