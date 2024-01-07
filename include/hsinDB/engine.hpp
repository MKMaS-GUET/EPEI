/*
 * @FileName   : engine.hpp
 * @CreateAt   : 2022/10/25
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: 
 */

#ifndef COMPRESSED_ENCODED_TREE_INDEX_ENGINE_HPP
#define COMPRESSED_ENCODED_TREE_INDEX_ENGINE_HPP

#include <string>
#include <memory>
#include <vector>
#include <hsinDB/result.hpp>

namespace hsinDB {

class Engine {
private:
    class Impl;

public:
    Engine() = delete;

    ~Engine() = delete;

    static void Create(const std::string &db_name, const std::string &data_file);

    static void Query(const std::string &db_name, const std::string &data_file);

public :
    std::shared_ptr<Impl> _impl;
};

}

#endif //COMPRESSED_ENCODED_TREE_INDEX_ENGINE_HPP
