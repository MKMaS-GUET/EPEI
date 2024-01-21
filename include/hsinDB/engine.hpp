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

namespace hsinDB {

class Engine {
   private:
    class Impl;

   public:
    Engine() = delete;

    ~Engine() = delete;

    static void Create(const std::string& db_name, const std::string& data_file);

    static void Query(const std::string& db_name, const std::string& data_file);

    static void Server(const std::string& port);

   public:
    std::shared_ptr<Impl> _impl;
};

}  // namespace hsinDB

#endif  // ENGINE_HPP
