/*
 * @FileName   : result.hpp
 * @CreateAt   : 2022/10/26
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: 
 */

#ifndef COMPRESSED_ENCODED_TREE_INDEX_RESULT_HPP
#define COMPRESSED_ENCODED_TREE_INDEX_RESULT_HPP

#include <vector>
#include <string>

namespace hsinDB {

class ResultSet {
public:
    ResultSet(std::vector<std::string> variables,
              std::vector<std::vector<std::string>> final_result)
            : _query_variables(std::move(variables)),
              _final_result(std::move(final_result)) {}

    ~ResultSet() = default;

    [[nodiscard]] const std::vector<std::string> &query_variables() const {
        return _query_variables;
    }

    [[nodiscard]] const std::vector<std::vector<std::string>> &final_result() const {
        return _final_result;
    }

    [[nodiscard]] size_t variable_size() const {
        return _query_variables.size();
    }

    [[nodiscard]] size_t result_size() const {
        return _final_result.size();
    }

    std::vector<std::string>::const_iterator var_begin() {
        return _query_variables.begin();
    }

    std::vector<std::string>::const_iterator var_end() {
        return _query_variables.end();
    }

    std::vector<std::vector<std::string>>::const_iterator begin() {
        return _final_result.begin();
    }

    std::vector<std::vector<std::string>>::const_iterator end() {
        return _final_result.end();
    }

private:
    std::vector<std::string> _query_variables;
    std::vector<std::vector<std::string>> _final_result;
};

}

#endif //COMPRESSED_ENCODED_TREE_INDEX_RESULT_HPP
