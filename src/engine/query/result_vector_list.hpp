#include <parallel_hashmap/phmap.h>
#include <string>
#include <utility>
#include <vector>

#ifndef RESULT_VECTOR_LIST_HPP
#define RESULT_VECTOR_LIST_HPP

struct ResultVector {
    int id = -1;

    std::vector<uint> result;

    ResultVector() {}

    ResultVector(uint size) { result = std::vector<uint>(size); }

    ~ResultVector() { std::vector<uint>().swap(result); }
};

class ResultVectorList {
    std::vector<std::shared_ptr<ResultVector>> result_vectors;

    phmap::flat_hash_map<int, std::vector<uint>::iterator> vector_current_pos;

    std::vector<uint>::iterator bin_searech(int i, uint val) {
        auto left = vector_current_pos[i];
        auto right = result_vectors[i]->result.end();

        while (left <= right) {
            auto mid = left + (right - left) / 2;

            if (*mid == val) {
                return mid;  // 找到目标，返回索引
            } else if (*mid < val) {
                left = mid + 1;  // 目标在右半部分
            } else {
                right = mid - 1;  // 目标在左半部分
            }
        }
    }

   public:
    void clear() {
        result_vectors.clear();
        vector_current_pos.clear();
    }

    void sizes() {
        for (auto r_v : result_vectors) {
            std::cout << r_v->result.size() << " ";
        }
        // std::cout << std::endl;
    }

    ResultVectorList() { result_vectors = std::vector<std::shared_ptr<ResultVector>>(); }

    bool add_vectors(std::vector<std::shared_ptr<ResultVector>> ranges) {
        // std::cout << "add: " << ranges.size() << " vectors" << std::endl;
        for (std::vector<std::shared_ptr<ResultVector>>::iterator it = ranges.begin(); it != ranges.end();
             it++) {
            // std::cout << "size: " << (*it)->size() << std::endl;
            if ((*it)->result.size() == 0)
                return 0;
            add_vector(*it);
        }
        return 1;
    }

    std::shared_ptr<ResultVector> shortest() {
        long unsigned int min_i = 0;
        long unsigned int min = result_vectors[0]->result.size();
        for (long unsigned int i = 0; i < result_vectors.size(); i++) {
            if (result_vectors[i]->result.size() < min) {
                min = result_vectors[i]->result.size();
                min_i = i;
            }
        }
        return result_vectors[min_i];
    }

    void add_vector(std::shared_ptr<ResultVector> range) {
        if (result_vectors.size() == 0) {
            result_vectors.push_back(range);
            return;
        }

        // std::cout << "add: " << range->result.size() << std::endl;
        uint first_val = range->result[0];
        for (long unsigned int i = 0; i < result_vectors.size(); i++) {
            if (result_vectors[i]->result[0] > first_val) {
                result_vectors.insert(result_vectors.begin() + i, range);
                return;
            }
        }

        result_vectors.push_back(range);
    }

    void update_current_postion() {
        for (long unsigned int i = 0; i < result_vectors.size(); i++) {
            std::shared_ptr<ResultVector> p_r = result_vectors[i];
            vector_current_pos[i] = p_r->result.begin();
        }
    }

    void seek(int i, uint val) {
        std::shared_ptr<ResultVector> p_r = result_vectors[i];

        auto it = vector_current_pos[i];
        auto end = p_r->result.end();
        for (; it < end; it = it + 2) {
            if (*it >= val) {
                if (*(it - 1) >= val) {
                    vector_current_pos[i] = it - 1;
                    return;
                }
                vector_current_pos[i] = it;
                return;
            }
        }
        if (it == end) {
            if (*(it - 1) >= val) {
                vector_current_pos[i] = it - 1;
                return;
            }
        }

        vector_current_pos[i] = end;
    }

    // 对range顺序排序后，获取第i个range第一个值
    uint get_current_val_of_range(int i) {
        phmap::flat_hash_map<int, std::vector<uint>::iterator>::iterator v_it = vector_current_pos.find(i);
        if (v_it != vector_current_pos.end()) {
            return *v_it->second;
        }

        return -1;
    }

    // 更新range的起始迭代器
    void next_val(int i) { vector_current_pos[i]++; }

    std::shared_ptr<ResultVector> get_range_by_index(int i) { return result_vectors[i]; }

    bool has_empty() {
        for (long unsigned int i = 0; i < result_vectors.size(); i++) {
            if (result_vectors[i]->result.size() == 0) {
                return true;
            }
        }
        return false;
    }

    bool at_end(int i) {
        std::shared_ptr<ResultVector> p_r = result_vectors[i];
        return vector_current_pos[i] == p_r->result.end();
    }

    int size() { return result_vectors.size(); }
};

#endif