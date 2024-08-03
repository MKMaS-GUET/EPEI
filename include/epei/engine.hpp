/*
 * @FileName   : engine.hpp
 * @CreateAt   : 2022/10/25
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef ENGINE_HPP
#define ENGINE_HPP

#include <memory>
#include <string>
#include <vector>

namespace epei {

class Engine {
   private:
    class Impl;

   public:
    Engine() = delete;

    ~Engine() = delete;

    static void Create(const std::string& db_name, const std::string& data_file);

    static void Query(const std::string& db_name, const std::string& data_file);

    static void Server(const std::string& ip, const std::string& port, const std::string& db);

   public:
    std::shared_ptr<Impl> _impl;
};

}  // namespace epei

#endif  // ENGINE_HPP
