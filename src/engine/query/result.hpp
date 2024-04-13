#ifndef RESULT_LIST_HPP
#define RESULT_LIST_HPP

#include <unistd.h>
#include <iterator>
#include <string>
#include <vector>

class Result {
    uint* _start;
    uint _size;
    bool _in_mem = false;

   public:
    int id = -1;
    Result() : _start(nullptr), _size(0) {}

    Result(uint* start, uint size) : _start(start), _size(size) {}

    Result(uint* start, uint size, bool in_mem) : _start(start), _size(size), _in_mem(in_mem) {}

    ~Result() {
        if (_in_mem) {
            delete[] _start;  // 释放数组
            _start = nullptr;
        }
    }

    class Iterator {
        uint* ptr;

       public:
        Iterator() : ptr(nullptr) {}
        Iterator(uint* p) : ptr(p) {}
        Iterator(const Iterator& it) : ptr(it.ptr) {}

        Iterator& operator++() {
            ++ptr;
            return *this;
        }
        Iterator operator++(int) {
            Iterator tmp(*this);
            operator++();
            return tmp;
        }
        Iterator operator-(int num) { return Iterator(ptr - num); }
        Iterator operator+(int num) { return Iterator(ptr + num); }
        bool operator==(const Iterator& rhs) const { return ptr == rhs.ptr; }
        bool operator!=(const Iterator& rhs) const { return ptr != rhs.ptr; }
        bool operator<(const Iterator& rhs) const { return ptr < rhs.ptr; }
        uint& operator*() { return *ptr; }
    };

    Iterator begin() { return Iterator(_start); }
    Iterator end() { return Iterator(_start + _size); }
    uint& operator[](uint i) {
        if (i >= 0 && i < _size) {
            return *(_start + i);
        }
        return *_start;
    }

    uint size() { return _size; }
};

class ResultList {
    std::vector<std::shared_ptr<Result>> results;

    phmap::flat_hash_map<int, Result::Iterator> vector_current_pos;

   public:
    void clear() {
        results.clear();
        vector_current_pos.clear();
    }

    void sizes() {
        for (long unsigned int i = 0; i < results.size(); i++) {
            std::cout << results[i]->size() << " ";
        }
        std::cout << std::endl;
    }

    ResultList() { results = std::vector<std::shared_ptr<Result>>(); }

    bool add_vectors(std::vector<std::shared_ptr<Result>> ranges) {
        // std::cout << "add: " << ranges.size() << " vectors" << std::endl;
        for (std::vector<std::shared_ptr<Result>>::iterator it = ranges.begin(); it != ranges.end(); it++) {
            // std::cout << "size: " << (*it)->size() << std::endl;
            if ((*it)->size() == 0)
                return 0;
            add_vector(*it);
        }
        return 1;
    }

    std::shared_ptr<Result> shortest() {
        long unsigned int min_i = 0;
        long unsigned int min = results[0]->size();
        for (long unsigned int i = 0; i < results.size(); i++) {
            if (results[i]->size() < min) {
                min = results[i]->size();
                min_i = i;
            }
        }
        return results[min_i];
    }

    void add_vector(std::shared_ptr<Result> range) {
        if (results.size() == 0) {
            results.push_back(range);
            return;
        }

        uint first_val = range->operator[](0);
        for (long unsigned int i = 0; i < results.size(); i++) {
            if (results[i]->operator[](0) > first_val) {
                results.insert(results.begin() + i, range);
                return;
            }
        }

        results.push_back(range);
    }

    void update_current_postion() {
        for (long unsigned int i = 0; i < results.size(); i++) {
            vector_current_pos[i] = results[i]->begin();
        }
    }

    void seek(int i, uint val) {
        std::shared_ptr<Result> p_r = results[i];

        auto it = vector_current_pos[i];
        auto end = p_r->end();
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
        phmap::flat_hash_map<int, Result::Iterator>::iterator v_it = vector_current_pos.find(i);
        if (v_it != vector_current_pos.end()) {
            return *v_it->second;
        }

        return -1;
    }

    // 更新range的起始迭代器
    void next_val(int i) { vector_current_pos[i]++; }

    std::shared_ptr<Result> get_range_by_index(int i) { return results[i]; }

    bool has_empty() {
        for (long unsigned int i = 0; i < results.size(); i++) {
            if (results[i]->size() == 0) {
                return true;
            }
        }
        return false;
    }

    bool at_end(int i) {
        std::shared_ptr<Result> p_r = results[i];
        return vector_current_pos[i] == p_r->end();
    }

    int size() { return results.size(); }
};

#endif