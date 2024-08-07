/*
 * @FileName   : engine.cpp
 * @CreateAt   : 2022/10/25
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#include "engine-impl.hpp"

namespace epei {

void Engine::Create(const std::string& db_name, const std::string& data_file) {
    auto impl = std::make_shared<Engine::Impl>();
    impl->Create(db_name, data_file);
}

void Engine::Query(const std::string& db_name, const std::string& data_file) {
    auto impl = std::make_shared<Engine::Impl>();
    impl->Query(db_name, data_file);
}

void Engine::Server(const std::string& ip, const std::string& port, const std::string& db) {
    auto impl = std::make_shared<Engine::Impl>();
    impl->Server(ip, port, db);
}

}  // namespace epei
