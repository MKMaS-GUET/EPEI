#ifndef INDEX_BUILDER_HPP
#define INDEX_BUILDER_HPP

#include <fcntl.h>
#include <malloc.h>
#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "dictionary.hpp"
#include "index.hpp"
#include "mmap.hpp"

namespace fs = std::filesystem;

class IndexBuilder {
    std::string data_file_;
    std::string db_index_path_;
    std::string db_dictionary_path_;
    std::string db_name_;
    uint threads_ = 1;

    Dictionary dict;

    MMap<uint> predicate_index_;
    MMap<uint> predicate_index_arrays_;
    MMap<uint> entity_index_;
    MMap<uint> po_predicate_map_;
    MMap<uint> ps_predicate_map_;
    MMap<uint> entity_index_arrays_;

    uint predicate_index_file_size_ = 0;
    uint predicate_index_arrays_file_size_ = 0;
    uint entity_index_file_size_ = 0;
    uint po_predicate_map_file_size_ = 0;
    uint ps_predicate_map_file_size_ = 0;
    uint entity_index_arrays_file_size_ = 0;

    std::mutex mtx;

    std::vector<std::pair<uint, uint>> predicate_rank_;

    hash_map<uint, std::vector<std::pair<uint, uint>>>* pso_;

    // e_id -> (s_to_p, o_to_p)
    std::vector<std::pair<uint, uint>> predicate_map_file_offset_;

   public:
    IndexBuilder(std::string db_name, std::string data_file) {
        db_name_ = db_name;
        data_file_ = data_file;
        db_index_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/index/";

        fs::path db_path = db_index_path_;
        if (!fs::exists(db_path)) {
            fs::create_directories(db_path);
        }

        db_dictionary_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/dictionary/";

        dict = Dictionary(db_dictionary_path_, data_file_);
    }

    ~IndexBuilder() {
        std::vector<std::pair<uint, uint>>().swap(predicate_rank_);
        hash_map<uint, std::vector<std::pair<uint, uint>>>().swap(*pso_);
        std::vector<std::pair<uint, uint>>().swap(predicate_map_file_offset_);
    }

    bool Build() {
        std::cout << "Indexing ..." << std::endl;

        LoadData();

        CalculatePredicateRank();

        // predict -> (o_set, s_set)
        std::vector<PredicateIndex> predicate_indexes(dict.predicate_cnt());
        // e -> (s_to_p, o_to_p)
        // _e_to_p = std::vector<std::std::pair<std::vector<uint>, std::vector<uint>>>(dict.max_id());
        // first: PS_PREDICATE_MAP file offset  second: PO_PREDICATE_MAP file offset
        predicate_map_file_offset_ = std::vector<std::pair<uint, uint>>(dict.max_id());

        uint* ps_predicate_map_size = (uint*)malloc(4 * dict.max_id());
        uint* po_predicate_map_size = (uint*)malloc(4 * dict.max_id());
        std::memset(ps_predicate_map_size, 0, 4 * dict.max_id());
        std::memset(po_predicate_map_size, 0, 4 * dict.max_id());

        BuildPredicateIndex(predicate_indexes);

        StorePredicateIndex(predicate_indexes, ps_predicate_map_size, po_predicate_map_size);

        StoreEntityIndex(ps_predicate_map_size, po_predicate_map_size);

        free(ps_predicate_map_size);
        free(po_predicate_map_size);

        BuildPredicateMaps();

        StoreDBInfo();

        return true;
    }

    void LoadData() {
        auto beg = std::chrono::high_resolution_clock::now();

        pso_ = dict.EncodeRDF();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "load data takes " << diff.count() << " ms." << std::endl;
    }

    void CalculatePredicateRank() {
        for (uint pid = 1; pid <= dict.predicate_cnt(); pid++) {
            pso_->at(pid).shrink_to_fit();
            uint i = 0, size = pso_->at(pid).size();
            for (; i < predicate_rank_.size(); i++) {
                if (predicate_rank_[i].second <= size)
                    break;
            }
            predicate_rank_.insert(predicate_rank_.begin() + i, {pid, size});
        }
        // for (uint rank = 0; rank < predicate_rank_.size(); rank++) {
        //     std::cout << rank << " " << predicate_rank_[rank].first << " " << predicate_rank_[rank].second
        //               << std::endl;
        // }
    }

    void BuildPredicateIndex(std::vector<PredicateIndex>& predicate_indexes) {
        auto beg = std::chrono::high_resolution_clock::now();

        uint pid = 0;
        double finished = 0;
        std::string info = "building predicate index: ";

        if (threads_ == 1) {
            Progress(finished, 0, info);
            for (uint tid = 0; tid < dict.predicate_cnt(); tid++) {
                pid = predicate_rank_[tid].first;
                predicate_indexes[pid - 1].Build(pso_->at(pid));
                // std::cout << pid << " " << predicate_indexes[pid - 1].s_set_.size() << " "
                //           << predicate_indexes[pid - 1].o_set_.size() << std::endl;

                UpdateFileSize(predicate_indexes[pid - 1].s_set_.size(),
                               predicate_indexes[pid - 1].o_set_.size());

                Progress(finished, pid, info);
            }
        } else {
            std::queue<uint> task_queue;
            for (uint i = 0; i < dict.predicate_cnt(); i++) {
                task_queue.push(predicate_rank_[i].first);
            }
            std::vector<std::thread> threads;
            for (uint tid = 0; tid < threads_; tid++) {
                threads.emplace_back(std::bind(&IndexBuilder::SubBuildPredicateIndex, this, &task_queue,
                                               &predicate_indexes, &finished, &info));
            }
            for (auto& t : threads) {
                t.join();
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "build predicate index takes " << diff.count() << " ms.                 " << std::endl;
    }

    void UpdateFileSize(uint ps_set_size, uint po_set_size) {
        predicate_index_arrays_file_size_ += ps_set_size * 4 + po_set_size * 4;
        ps_predicate_map_file_size_ += po_set_size * 3 * 4;
        po_predicate_map_file_size_ += ps_set_size * 3 * 4;
    }

    void SubBuildPredicateIndex(std::queue<uint>* task_queue,
                                std::vector<PredicateIndex>* predicate_indexes,
                                double* finished,
                                std::string* info) {
        uint pid;
        while (task_queue->size()) {
            mtx.lock();
            pid = task_queue->front();
            task_queue->pop();
            mtx.unlock();

            predicate_indexes->at(pid - 1).Build(pso_->at(pid));

            mtx.lock();
            UpdateFileSize(predicate_indexes->at(pid - 1).s_set_.size(),
                           predicate_indexes->at(pid - 1).o_set_.size());
            Progress(*finished, pid, *info);
            mtx.unlock();
        }
    }

    void StorePredicateIndex(std::vector<PredicateIndex>& predicate_indexes,
                             uint ps_predicate_map_size[],
                             uint po_predicate_map_size[]) {
        auto beg = std::chrono::high_resolution_clock::now();

        predicate_index_file_size_ = dict.predicate_cnt() * 2 * 4;

        predicate_index_ = MMap<uint>(db_index_path_ + "PREDICATE_INDEX", predicate_index_file_size_);
        predicate_index_arrays_ =
            MMap<uint>(db_index_path_ + "PREDICATE_INDEX_ARRAYS", predicate_index_arrays_file_size_);

        uint predicate_index_arrays_file_offset = 0;
        phmap::btree_set<uint>* ps_set;
        phmap::btree_set<uint>* po_set;
        for (uint pid = 1; pid <= dict.predicate_cnt(); pid++) {
            ps_set = &predicate_indexes[pid - 1].s_set_;
            po_set = &predicate_indexes[pid - 1].o_set_;
            // std::cout << pid << " " << ps_set->size() << " " << po_set->size() << std::endl;

            predicate_index_[(pid - 1) * 2] = predicate_index_arrays_file_offset;
            for (auto it = ps_set->begin(); it != ps_set->end(); it++) {
                po_predicate_map_size[*it - 1]++;
                predicate_index_arrays_[predicate_index_arrays_file_offset] = *it;
                predicate_index_arrays_file_offset++;
            }

            predicate_index_[(pid - 1) * 2 + 1] = predicate_index_arrays_file_offset;
            for (auto it = po_set->begin(); it != po_set->end(); it++) {
                ps_predicate_map_size[*it - 1]++;
                predicate_index_arrays_[predicate_index_arrays_file_offset] = *it;
                predicate_index_arrays_file_offset++;
            }

            predicate_indexes[pid - 1].Clear();
        }

        predicate_index_arrays_.CloseMap();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "store predicate index takes " << diff.count() << " ms.               " << std::endl;
    }

    void StoreEntityIndex(uint ps_predicate_map_size[], uint po_predicate_map_size[]) {
        auto beg = std::chrono::high_resolution_clock::now();

        // entity_index_file_size_ = dict.max_id() * 4 * 4;
        entity_index_file_size_ = dict.max_id() * 2 * 4;

        entity_index_ = MMap<uint>(db_index_path_ + "ENTITY_INDEX", entity_index_file_size_);

        // 两个循环合并为一个循环
        uint cnt;
        uint offset = 0;
        uint begin_offset = 0;
        for (uint id = 1; id <= dict.max_id(); id++) {
            cnt = po_predicate_map_size[id - 1];
            begin_offset = (id - 1) * 2;
            predicate_map_file_offset_[id - 1].first = offset;
            // PS_PREDICATE_MAP offset
            entity_index_[begin_offset] = offset;
            offset += cnt * 3;
        }
        offset = 0;
        for (uint id = 1; id <= dict.max_id(); id++) {
            cnt = ps_predicate_map_size[id - 1];
            begin_offset = (id - 1) * 2;
            predicate_map_file_offset_[id - 1].second = offset;
            entity_index_[begin_offset + 1] = offset;
            offset += cnt * 3;
        }

        entity_index_.CloseMap();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "store entity index takes " << diff.count() << " ms.               " << std::endl;
    }

    void BuildPredicateMaps() {
        auto beg = std::chrono::high_resolution_clock::now();

        entity_index_arrays_file_size_ = dict.triplet_cnt() * 2 * 4;

        po_predicate_map_ = MMap<uint>(db_index_path_ + "PO_PREDICATE_MAP", po_predicate_map_file_size_);
        ps_predicate_map_ = MMap<uint>(db_index_path_ + "PS_PREDICATE_MAP", ps_predicate_map_file_size_);
        entity_index_arrays_ =
            MMap<uint>(db_index_path_ + "ENTITY_INDEX_ARRAYS", entity_index_arrays_file_size_);

        double finished = 0;
        uint pid = 0;
        std::string info = "building and storing predicate maps: ";
        uint arrays_offset = 0;
        if (threads_ == 1) {
            EntityIndex entity_index;

            Progress(finished, 0, info);
            for (uint tid = 0; tid < dict.predicate_cnt(); tid++) {
                pid = predicate_rank_[tid].first;

                entity_index.Clear();
                entity_index.Build(pso_->at(pid));

                StorePredicateMaps(arrays_offset, pid, po_predicate_map_, entity_index.s_to_o_, true);
                StorePredicateMaps(arrays_offset, pid, ps_predicate_map_, entity_index.o_to_s_, false);

                std::vector<std::pair<uint, uint>>{}.swap(pso_->at(pid));
                malloc_trim(0);

                Progress(finished, pid, info);
            }
        } else {
            // [(pid, finish_flag) ,... ,(pid, finish_flag)]
            std::queue<uint> task_queue;
            for (uint i = 0; i < dict.predicate_cnt(); i++) {
                task_queue.push(predicate_rank_[i].first);
            }
            std::vector<std::thread> threads;
            for (uint tid = 0; tid < threads_; tid++) {
                threads.emplace_back(std::bind(&IndexBuilder::SubBuildPredicateMaps, this, &task_queue,
                                               &arrays_offset, &finished, &info));
            }
            for (auto& t : threads) {
                t.join();
            }
        }

        po_predicate_map_.CloseMap();
        ps_predicate_map_.CloseMap();
        if (entity_index_arrays_file_size_ != arrays_offset) {
            entity_index_arrays_file_size_ = arrays_offset * 4;
            entity_index_arrays_.Resize(arrays_offset * 4);
        }
        entity_index_arrays_.CloseMap();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "build and store predicate maps takes " << diff.count() << " ms.                      "
                  << std::endl;
    }

    void SubBuildPredicateMaps(std::queue<uint>* task_queue,
                               uint* arrays_offset,
                               double* finished,
                               std::string* info) {
        uint pid;
        while (task_queue->size()) {
            mtx.lock();
            pid = task_queue->front();
            task_queue->pop();
            mtx.unlock();

            EntityIndex entity_index = EntityIndex();
            entity_index.Build(pso_->at(pid));
            // store_predicate_maps(*arrays_offset, pid, entity_index);
            StorePredicateMaps(*arrays_offset, pid, po_predicate_map_, entity_index.s_to_o_, true);
            StorePredicateMaps(*arrays_offset, pid, ps_predicate_map_, entity_index.o_to_s_, false);

            mtx.lock();
            Progress(*finished, pid, *info);
            mtx.unlock();

            std::vector<std::pair<uint, uint>>{}.swap(pso_->at(pid));
            malloc_trim(0);
        }
    }

    void StorePredicateMaps(uint& arrays_offset,
                            uint pid,
                            MMap<uint>& vm,
                            hash_map<uint, LinkedArray<uint>>& e_to_e,
                            bool s_to_o) {
        Node<uint>* current_node;
        uint size = 0;
        uint eid = 0;
        uint map_offset;
        uint e_cnt = 0;
        uint arrays_size_sum = 0;
        uint arrays_start_offset;

        for (auto it = e_to_e.begin(); it != e_to_e.end(); it++) {
            current_node = &it->second;
            if (current_node->elements_.size()) {
                size = 0;
                eid = it->first;

                mtx.lock();
                if (s_to_o) {
                    map_offset = predicate_map_file_offset_[eid - 1].first;
                    predicate_map_file_offset_[eid - 1].first += 3;
                } else {
                    map_offset = predicate_map_file_offset_[eid - 1].second;
                    predicate_map_file_offset_[eid - 1].second += 3;
                }
                arrays_start_offset = arrays_offset;
                while (current_node) {
                    size += current_node->elements_.size();
                    current_node = current_node->next_;
                }
                if (size != 1)
                    arrays_offset += size;
                mtx.unlock();

                arrays_size_sum += size;

                vm[map_offset] = pid;
                map_offset++;

                current_node = &it->second;
                if (size != 1) {
                    vm[map_offset] = arrays_start_offset;
                    map_offset++;
                    vm[map_offset] = size;

                    while (current_node) {
                        for (long unsigned int i = 0; i < current_node->elements_.size(); i++) {
                            entity_index_arrays_[arrays_start_offset] = current_node->elements_[i];
                            arrays_start_offset++;
                        }
                        current_node->elements_.clear();
                        current_node = current_node->next_;
                    }
                } else {
                    vm[map_offset] = current_node->elements_[0];
                    map_offset++;
                    vm[map_offset] = 1;
                }
            }
            e_cnt += 1;
        }
    }

    void StoreDBInfo() {
        MMap<uint> vm = MMap<uint>(db_index_path_ + "DB_INFO", 6 * 4);

        vm[0] = predicate_index_file_size_;
        vm[1] = predicate_index_arrays_file_size_;
        vm[2] = entity_index_file_size_;
        vm[3] = po_predicate_map_file_size_;
        vm[4] = ps_predicate_map_file_size_;
        vm[5] = entity_index_arrays_file_size_;
        // vm[6] = dict.triplet_cnt();
        // vm[7] = dict.max_id();
        // vm[8] = dict.predicate_cnt();
        // vm[9] = entity_size_;

        vm.CloseMap();
    }

    void LoadDBInfo() {
        MMap<uint> vm = MMap<uint>(db_index_path_ + "DB_INFO", 6 * 4);

        predicate_index_file_size_ = vm[0];
        predicate_index_arrays_file_size_ = vm[1];
        entity_index_file_size_ = vm[2];
        po_predicate_map_file_size_ = vm[3];
        ps_predicate_map_file_size_ = vm[4];
        entity_index_arrays_file_size_ = vm[5];
        // dict.triplet_cnt() = vm[6];
        // dict.max_id() = vm[7];
        // dict.predicate_cnt() = vm[8];
        // entity_size_ = vm[9];

        vm.CloseMap();
    }

    void Progress(double& finished, uint pid, std::string& info) {
        if (pid != 0) {
            for (long unsigned int i = 0; i < predicate_rank_.size(); i++) {
                if (predicate_rank_[i].first == pid) {
                    finished += predicate_rank_[i].second * 1.0 / dict.triplet_cnt();
                    break;
                }
            }
        }

        std::cout << info << finished * 100 << "%    \r";
        std::cout.flush();
    }
};

#endif