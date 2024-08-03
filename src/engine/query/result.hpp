#ifndef RESULT_LIST_HPP
#define RESULT_LIST_HPP

#include <iterator>
#include <string>
#include <vector>
#include <memory>


class ResultList {
   public:
    class Result {
        uint* start_;
        uint size_;
        bool in_mem_ = false;

       public:
        int id = -1;
        Result() : start_(nullptr), size_(0) {}

        Result(uint* start, uint size) : start_(start), size_(size) {}

        Result(uint* start, uint size, bool in_mem) : start_(start), size_(size), in_mem_(in_mem) {}

        ~Result() {
            if (in_mem_ && start_) {
                delete[] start_;  // 释放数组
                start_ = nullptr;
            }
        }

        class Iterator {
            uint* ptr_;

           public:
            Iterator() : ptr_(nullptr) {}
            Iterator(uint* p) : ptr_(p) {}
            Iterator(const Iterator& it) : ptr_(it.ptr_) {}

            Iterator& operator++() {
                ++ptr_;
                return *this;
            }
            Iterator operator++(int) {
                Iterator tmp(*this);
                operator++();
                return tmp;
            }
            uint operator-(Result::Iterator r_it) { return ptr_ - r_it.ptr_; }
            Iterator operator-(int num) { return Iterator(ptr_ - num); }
            Iterator operator+(int num) { return Iterator(ptr_ + num); }
            bool operator==(const Iterator& rhs) const { return ptr_ == rhs.ptr_; }
            bool operator!=(const Iterator& rhs) const { return ptr_ != rhs.ptr_; }
            bool operator<(const Iterator& rhs) const { return ptr_ < rhs.ptr_; }
            uint& operator*() { return *ptr_; }
        };

        Iterator begin() { return Iterator(start_); }
        Iterator end() { return Iterator(start_ + size_); }
        uint& operator[](uint i) {
            if (i >= 0 && i < size_) {
                return *(start_ + i);
            }
            return *start_;
        }

        uint size() { return size_; }
    };

    void Clear() {
        results_.clear();
        vector_current_pos_.clear();
    }

    void Sizes() {
        for (long unsigned int i = 0; i < results_.size(); i++) {
            std::cout << results_[i]->size() << " ";
        }
        std::cout << std::endl;
    }

    ResultList() { results_ = std::vector<std::shared_ptr<Result>>(); }

    bool AddVectors(std::vector<std::shared_ptr<Result>> ranges) {
        for (std::vector<std::shared_ptr<Result>>::iterator it = ranges.begin(); it != ranges.end(); it++) {
            if ((*it)->size() == 0)
                return 0;
            AddVector(*it);
        }
        return 1;
    }

    std::shared_ptr<Result> Shortest() {
        long unsigned int min_i = 0;
        long unsigned int min = results_[0]->size();
        for (long unsigned int i = 0; i < results_.size(); i++) {
            if (results_[i]->size() < min) {
                min = results_[i]->size();
                min_i = i;
            }
        }
        return results_[min_i];
    }

    void AddVector(std::shared_ptr<Result> range) {
        if (results_.size() == 0) {
            results_.push_back(range);
            return;
        }

        uint first_val = range->operator[](0);
        for (long unsigned int i = 0; i < results_.size(); i++) {
            if (results_[i]->operator[](0) > first_val) {
                results_.insert(results_.begin() + i, range);
                return;
            }
        }

        results_.push_back(range);
    }

    void UpdateCurrentPostion() {
        for (long unsigned int i = 0; i < results_.size(); i++) {
            vector_current_pos_.push_back(results_[i]->begin());
        }
    }

    void Seek(int i, uint val) {
        std::shared_ptr<Result> p_r = results_[i];

        auto it = vector_current_pos_[i];
        auto end = p_r->end();
        for (; it < end; it = it + 2) {
            if (*it >= val) {
                if (*(it - 1) >= val) {
                    vector_current_pos_[i] = it - 1;
                    return;
                }
                vector_current_pos_[i] = it;
                return;
            }
        }
        if (it == end) {
            if (*(it - 1) >= val) {
                vector_current_pos_[i] = it - 1;
                return;
            }
        }
        vector_current_pos_[i] = end;
    }

    // 对range顺序排序后，获取第i个range第一个值
    uint GetCurrentValOfRange(int i) { return *vector_current_pos_[i]; }

    // 更新range的起始迭代器
    void NextVal(int i) { vector_current_pos_[i]++; }

    std::shared_ptr<Result> GetRangeByIndex(int i) { return results_[i]; }

    bool HasEmpty() {
        for (long unsigned int i = 0; i < results_.size(); i++) {
            if (results_[i]->size() == 0) {
                return true;
            }
        }
        return false;
    }

    bool AtEnd(int i) {
        std::shared_ptr<Result> p_r = results_[i];
        return vector_current_pos_[i] == p_r->end();
    }

    int Size() { return results_.size(); }

   private:
    std::vector<std::shared_ptr<Result>> results_;

    std::vector<Result::Iterator> vector_current_pos_;
};

#endif