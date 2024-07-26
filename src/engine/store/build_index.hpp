#ifndef BUILD_INDEX_HPP
#define BUILD_INDEX_HPP

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

#include "mmap.hpp"

#define MAX_SIZE 20000

namespace fs = std::filesystem;
using phmap::btree_map;
using phmap::flat_hash_map;
using phmap::flat_hash_set;
using std::pair;

template <typename T>
class Node {
   public:
    std::vector<T> elements_;
    Node<T>* next_ = nullptr;

    Node() : elements_() {}

    Node(int reverse_size) { elements_.reserve(reverse_size); }
};

template <typename T>
class LinkedArray : public Node<T> {
   public:
    void Add(Node<T>* target_node, T e) {
        if (target_node->elements_.size() == 0) {
            target_node->elements_.push_back(e);
            return;
        }

        auto iter = std::lower_bound(target_node->elements_.begin(), target_node->elements_.end(), e,
                                     [](T a, T b) { return a < b; });
        if (iter == target_node->elements_.end()) {
            target_node->elements_.push_back(e);
        } else {
            if (*iter != e)
                target_node->elements_.insert(iter, e);
        }
    }

    void MoveHalf(Node<T>* source_node, Node<T>* target_node) {
        auto middle = source_node->elements_.begin() + source_node->elements_.size() / 2;

        std::move(std::make_move_iterator(middle), std::make_move_iterator(source_node->elements_.end()),
                  std::back_inserter(target_node->elements_));

        source_node->elements_.erase(middle, source_node->elements_.end());
    }

    void AddByOrder(T e) {
        if (this->elements_.size() == 0) {
            this->elements_.push_back(e);
            return;
        }

        Node<T>* current_node = this;
        while (true) {
            if (current_node->elements_.back() >= e) {
                Add(current_node, e);
                // size++;
                if (current_node->elements_.size() == MAX_SIZE) {
                    Node<T>* new_node = new Node<T>(MAX_SIZE / 2);
                    new_node->next_ = current_node->next_;
                    current_node->next_ = new_node;
                    MoveHalf(current_node, new_node);
                }
                return;
            }

            if (current_node->next_ == NULL) {
                break;
            }
            current_node = current_node->next_;
        }

        current_node->elements_.push_back(e);
        // size++;
        if (current_node->elements_.size() == MAX_SIZE) {
            Node<T>* new_node = new Node<T>(MAX_SIZE / 2);
            new_node->next_ = current_node->next_;
            current_node->next_ = new_node;
            MoveHalf(current_node, new_node);
        }
    }
};

struct PredicateIndex {
    phmap::btree_set<uint> s_set_;
    phmap::btree_set<uint> o_set_;

    void Build(std::vector<pair<uint, uint>>& so_pairs) {
        for (const auto& so : so_pairs) {
            s_set_.insert(so.first);
            o_set_.insert(so.second);
        }
    }

    void Clear() {
        phmap::btree_set<uint>().swap(s_set_);
        phmap::btree_set<uint>().swap(o_set_);
    }
};

// index for a predicate
struct EntityIndex {
    flat_hash_map<uint, LinkedArray<uint>> s_to_o_;
    flat_hash_map<uint, LinkedArray<uint>> o_to_s_;

    void Build(std::vector<pair<uint, uint>>& so_pairs) {
        for (const auto& so : so_pairs) {
            s_to_o_[so.first].AddByOrder(so.second);
            o_to_s_[so.second].AddByOrder(so.first);
        }
    }

    void Clear() {
        flat_hash_map<uint, LinkedArray<uint>>().swap(s_to_o_);
        flat_hash_map<uint, LinkedArray<uint>>().swap(o_to_s_);
    }
};

class IndexBuilder {
    std::string data_file_;
    std::string db_data_path_;
    std::string db_name_;
    uint threads_ = 2;

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

    uint triplet_cnt_ = 0;
    uint entity_cnt_ = 1;
    uint predicate_cnt_ = 1;
    uint entity_size_ = 0;

    std::mutex mtx;

    std::vector<pair<uint, uint>> _predicate_rank;

    flat_hash_map<uint, std::vector<pair<uint, uint>>> _pso;

    // e_id -> (s_to_p, o_to_p)
    std::vector<std::pair<uint, uint>> _predicate_map_file_offset;

   public:
    IndexBuilder(std::string db_name, std::string data_file) {
        db_name_ = db_name;
        data_file_ = data_file;
        db_data_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/";

        fs::path db_path = db_data_path_;
        if (!fs::exists(db_path)) {
            fs::create_directories(db_path);
        }

        fs::path entity_path = db_data_path_ + "ENTITY";
        if (!fs::exists(entity_path)) {
            fs::create_directories(entity_path);
        }
    }

    bool Build() {
        std::cout << "Indexing ..." << std::endl;

        flat_hash_map<std::string, uint> entity2id;

        LoadData(entity2id);

        CalculatePredicateRank();

        // predict -> (o_set, s_set)
        std::vector<PredicateIndex> predicate_indexes(predicate_cnt_);
        // e -> (s_to_p, o_to_p)
        // _e_to_p = std::vector<std::pair<std::vector<uint>, std::vector<uint>>>(entity_cnt_);
        // first: PS_PREDICATE_MAP file offset  second: PO_PREDICATE_MAP file offset
        _predicate_map_file_offset = std::vector<std::pair<uint, uint>>(entity_cnt_);

        uint* ps_predicate_map_size = (uint*)malloc(4 * entity_cnt_);
        uint* po_predicate_map_size = (uint*)malloc(4 * entity_cnt_);
        std::memset(ps_predicate_map_size, 0, 4 * entity_cnt_);
        std::memset(po_predicate_map_size, 0, 4 * entity_cnt_);

        BuildPredicateIndex(predicate_indexes);

        StorePredicateIndex(predicate_indexes, ps_predicate_map_size, po_predicate_map_size);

        StoreEntityIndex(ps_predicate_map_size, po_predicate_map_size);

        free(ps_predicate_map_size);
        free(po_predicate_map_size);

        BuildPredicateMaps();

        StoreDBInfo();

        return true;
    }

    void LoadData(flat_hash_map<std::string, uint>& entity2id) {
        auto beg = std::chrono::high_resolution_clock::now();

        std::ifstream::sync_with_stdio(false);
        std::iostream::sync_with_stdio(false);
        std::ifstream fin(data_file_, std::ios::in);

        if (!fin.is_open()) {
            std::cerr << data_file_ << " doesn't exist, cannot build the database correctly." << std::endl;
            exit(1);
        }

        std::ofstream predict_out(db_data_path_ + "PREDICATE", std::ofstream::out | std::ofstream::binary);
        predict_out.tie(nullptr);

        std::ofstream entity_outs[6];
        for (int i = 0; i < 6; i++) {
            entity_outs[i] = std::ofstream(db_data_path_ + "ENTITY/" + std::to_string(i),
                                           std::ofstream::out | std::ofstream::binary);
            entity_outs[i].tie(nullptr);
        }

        // flat_hash_map<std::string, uint> entity2id;
        flat_hash_map<std::string, uint> predicate2id;

        std::pair<flat_hash_map<std::string, uint>::iterator, bool> ret;

        std::string s, p, o;
        while (fin >> s >> p) {
            fin.ignore();
            std::getline(fin, o);
            for (o.pop_back(); o.back() == ' ' || o.back() == '.'; o.pop_back()) {
            }

            ++triplet_cnt_;
            uint pid, sid, oid;

            ret = predicate2id.insert({p, predicate_cnt_});
            if (ret.second) {
                pid = predicate_cnt_;
                predicate_cnt_++;
                predict_out.write((p + "\n").c_str(), static_cast<long>(p.size() + 1));
            } else {
                pid = ret.first->second;
            }

            ret = entity2id.insert({s, entity_cnt_});
            if (ret.second) {
                sid = entity_cnt_;
                entity_cnt_++;
                entity_outs[sid % 6].write((s + "\n").c_str(), static_cast<long>(s.size() + 1));
            } else {
                sid = ret.first->second;
            }

            ret = entity2id.insert({o, entity_cnt_});
            if (ret.second) {
                oid = entity_cnt_;
                entity_cnt_++;
                entity_outs[oid % 6].write((o + "\n").c_str(), static_cast<long>(o.size() + 1));
            } else {
                oid = ret.first->second;
            }

            _pso[pid].push_back(std::make_pair(sid, oid));

            if (triplet_cnt_ % 100000 == 0) {
                std::cout << triplet_cnt_ << " triplet(s) have been loaded.\r";
                std::cout.flush();
            }
        }
        predicate_cnt_ -= 1;
        entity_cnt_ -= 1;
        std::cout << triplet_cnt_ << " triplet(s) have been loaded.\r";
        std::cout.flush();

        std::cout << std::endl;

        fin.close();
        predict_out.close();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "load data takes " << diff.count() << " ms." << std::endl;
    }

    // void serialize_entity2id(flat_hash_map<std::string, uint>& entity2id) {
    //     auto beg = std::chrono::high_resolution_clock::now();

    //     btree_map<ulong, uint> hash2id;
    //     flat_hash_map<ulong, std::vector<std::pair<std::string, uint>>> conflicts;
    //     std::vector<const std::string*> id2entity;
    //     id2entity.reserve(entity_cnt_ + 1);

    //     std::pair<btree_map<ulong, uint>::iterator, bool> ret;
    //     uint id = 0;
    //     ulong hash;
    //     std::hash<std::string> hash_func = std::hash<std::string>();
    //     for (auto it = entity2id.begin(); it != entity2id.end(); it++) {
    //         hash = hash_func(it->first);
    //         ret = hash2id.insert({hash, it->second});
    //         id2entity[it->second] = &it->first;
    //         if (ret.second == false) {
    //             // hash 碰撞
    //             if (ret.first->second != UINT_MAX) {
    //                 conflicts[hash] = {{*id2entity[id], id}, {it->first, it->second}};
    //                 hash2id[hash] = UINT_MAX;
    //             } else {
    //                 conflicts[hash].push_back({it->first, it->second});
    //             }
    //         }
    //     }
    //     auto end = std::chrono::high_resolution_clock::now();
    //     std::chrono::duration<double, std::milli> diff = end - beg;
    //     std::cout << "phase 1: " << diff.count() << " ms." << std::endl;

    //     MMap<uint> id2pos = MMap<uint>(db_data_path_ + "ENTITY/ID2POS", entity_cnt_ * 4);
    //     std::ofstream entity_out =
    //         std::ofstream(db_data_path_ + "ENTITY/STRINGS", std::ofstream::out | std::ofstream::binary);
    //     MMap<ulong> entity_hash =
    //         MMap<ulong>(db_data_path_ + "ENTITY/ENTITY_HASH", entity_cnt_ * 2 * 4);
    //     MMap<uint> entity_id =
    //         MMap<uint>(db_data_path_ + "ENTITY/ENTITY_ID", entity_cnt_ * 4);
    //     uint size;
    //     uint file_offset = 0;
    //     uint hash_offset = 0;
    //     id = 1;
    //     for (auto hash2id_iter = hash2id.begin(); hash2id_iter != hash2id.end(); hash2id_iter++) {
    //         size = id2entity[id]->size();
    //         entity_out.write(id2entity[id]->c_str(), static_cast<long>(size));
    //         id2pos[id - 1] = file_offset;
    //         file_offset += size;

    //         entity_hash[id - 1] = hash2id_iter->first;
    //         entity_id[id - 1] = hash2id_iter->second;
    //         id++;
    //     }
    //     entity_size_ = file_offset;
    //     entity_out.close();
    //     id2pos.CloseMap();
    //     entity_hash.CloseMap();
    //     entity_id.CloseMap();

    //     flat_hash_map<std::string, uint>().swap(entity2id);
    //     std::vector<const std::string*>().swap(id2entity);
    //     malloc_trim(0);

    //     if (conflicts.size() != 0) {
    //         std::ofstream conflicts_out =
    //             std::ofstream(db_data_path_ + "ENTITY/CONFLICTS", std::ofstream::out |
    //             std::ofstream::binary);

    //         for (auto it = conflicts.begin(); it != conflicts.end(); it++) {
    //             std::string hash_str = std::to_string(it->first) + " ";
    //             for (uint i = 0; i < it->second.size(); i++) {
    //                 hash_str += it->second[i].first + " " + std::to_string(it->second[i].second) + " ";
    //             }
    //             hash_str += "\n";
    //             conflicts_out.write(hash_str.c_str(), hash_str.size());
    //         }

    //         conflicts_out.close();
    //     }

    //     end = std::chrono::high_resolution_clock::now();
    //     diff = end - beg;
    //     std::cout << "takes " << diff.count() << " ms." << std::endl;
    // }

    void CalculatePredicateRank() {
        for (uint pid = 1; pid <= predicate_cnt_; pid++) {
            _pso[pid].shrink_to_fit();
            uint i = 0;
            for (; i < _predicate_rank.size(); i++) {
                if (_predicate_rank[i].second <= _pso[pid].size())
                    break;
            }
            _predicate_rank.insert(_predicate_rank.begin() + i, {pid, _pso[pid].size()});
        }
        // for (uint rank = 0; rank < _predicate_rank.size(); rank++) {
        //     std::cout << rank << " " << _predicate_rank[rank].first << " " <<
        //     _predicate_rank[rank].second
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
            for (uint tid = 0; tid < predicate_cnt_; tid++) {
                pid = _predicate_rank[tid].first;
                predicate_indexes[pid - 1].Build(_pso[pid]);

                UpdateFileSize(predicate_indexes[pid - 1].s_set_.size(),
                                 predicate_indexes[pid - 1].o_set_.size());

                Progress(finished, pid, info);
            }
        } else {
            std::queue<uint> task_queue;
            for (uint i = 0; i < predicate_cnt_; i++) {
                task_queue.push(_predicate_rank[i].first);
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

            predicate_indexes->at(pid - 1).Build(_pso[pid]);

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

        predicate_index_file_size_ = predicate_cnt_ * 2 * 4;

        predicate_index_ = MMap<uint>(db_data_path_ + "PREDICATE_INDEX", predicate_index_file_size_);
        predicate_index_arrays_ =
            MMap<uint>(db_data_path_ + "PREDICATE_INDEX_ARRAYS", predicate_index_arrays_file_size_);

        uint predicate_index_arrays_file_offset = 0;
        phmap::btree_set<uint>* ps_set;
        phmap::btree_set<uint>* po_set;
        for (uint pid = 1; pid <= predicate_cnt_; pid++) {
            ps_set = &predicate_indexes[pid - 1].s_set_;
            po_set = &predicate_indexes[pid - 1].o_set_;

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

        // entity_index_file_size_ = entity_cnt_ * 4 * 4;
        entity_index_file_size_ = entity_cnt_ * 2 * 4;

        entity_index_ = MMap<uint>(db_data_path_ + "ENTITY_INDEX", entity_index_file_size_);

        // 两个循环合并为一个循环
        uint cnt;
        uint offset = 0;
        uint begin_offset = 0;
        for (uint id = 1; id <= entity_cnt_; id++) {
            cnt = po_predicate_map_size[id - 1];
            begin_offset = (id - 1) * 2;
            _predicate_map_file_offset[id - 1].first = offset;
            // PS_PREDICATE_MAP offset
            entity_index_[begin_offset] = offset;
            offset += cnt * 3;
        }
        offset = 0;
        for (uint id = 1; id <= entity_cnt_; id++) {
            cnt = ps_predicate_map_size[id - 1];
            begin_offset = (id - 1) * 2;
            _predicate_map_file_offset[id - 1].second = offset;
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

        entity_index_arrays_file_size_ = triplet_cnt_ * 2 * 4;

        po_predicate_map_ =
            MMap<uint>(db_data_path_ + "PO_PREDICATE_MAP", po_predicate_map_file_size_);
        ps_predicate_map_ =
            MMap<uint>(db_data_path_ + "PS_PREDICATE_MAP", ps_predicate_map_file_size_);
        entity_index_arrays_ =
            MMap<uint>(db_data_path_ + "ENTITY_INDEX_ARRAYS", entity_index_arrays_file_size_);

        double finished = 0;
        uint pid = 0;
        std::string info = "building and storing predicate maps: ";
        uint arrays_offset = 0;
        if (threads_ == 1) {
            EntityIndex entity_index;

            Progress(finished, 0, info);
            for (uint tid = 0; tid < predicate_cnt_; tid++) {
                pid = _predicate_rank[tid].first;

                entity_index.Clear();
                entity_index.Build(_pso[pid]);

                StorePredicateMaps(arrays_offset, pid, po_predicate_map_, entity_index.s_to_o_, true);
                StorePredicateMaps(arrays_offset, pid, ps_predicate_map_, entity_index.o_to_s_, false);

                std::vector<pair<uint, uint>>{}.swap(_pso[pid]);
                malloc_trim(0);

                Progress(finished, pid, info);
            }
        } else {
            // [(pid, finish_flag) ,... ,(pid, finish_flag)]
            std::queue<uint> task_queue;
            for (uint i = 0; i < predicate_cnt_; i++) {
                task_queue.push(_predicate_rank[i].first);
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
            entity_index.Build(_pso[pid]);
            // store_predicate_maps(*arrays_offset, pid, entity_index);
            StorePredicateMaps(*arrays_offset, pid, po_predicate_map_, entity_index.s_to_o_, true);
            StorePredicateMaps(*arrays_offset, pid, ps_predicate_map_, entity_index.o_to_s_, false);

            mtx.lock();
            Progress(*finished, pid, *info);
            mtx.unlock();

            std::vector<pair<uint, uint>>{}.swap(_pso[pid]);
            malloc_trim(0);
        }
    }

    void StorePredicateMaps(uint& arrays_offset,
                              uint pid,
                              MMap<uint>& vm,
                              flat_hash_map<uint, LinkedArray<uint>>& e_to_e,
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
                    map_offset = _predicate_map_file_offset[eid - 1].first;
                    _predicate_map_file_offset[eid - 1].first += 3;
                } else {
                    map_offset = _predicate_map_file_offset[eid - 1].second;
                    _predicate_map_file_offset[eid - 1].second += 3;
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
        MMap<uint> vm = MMap<uint>(db_data_path_ + "DB_INFO", 10 * 4);

        vm[0] = predicate_index_file_size_;
        vm[1] = predicate_index_arrays_file_size_;
        vm[2] = entity_index_file_size_;
        vm[3] = po_predicate_map_file_size_;
        vm[4] = ps_predicate_map_file_size_;
        vm[5] = entity_index_arrays_file_size_;
        vm[6] = triplet_cnt_;
        vm[7] = entity_cnt_;
        vm[8] = predicate_cnt_;
        vm[9] = entity_size_;

        vm.CloseMap();
    }

    void LoadDBInfo() {
        MMap<uint> vm = MMap<uint>(db_data_path_ + "DB_INFO", 10 * 4);

        predicate_index_file_size_ = vm[0];
        predicate_index_arrays_file_size_ = vm[1];
        entity_index_file_size_ = vm[2];
        po_predicate_map_file_size_ = vm[3];
        ps_predicate_map_file_size_ = vm[4];
        entity_index_arrays_file_size_ = vm[5];
        triplet_cnt_ = vm[6];
        entity_cnt_ = vm[7];
        predicate_cnt_ = vm[8];
        entity_size_ = vm[9];

        vm.CloseMap();
    }

    void Progress(double& finished, uint pid, std::string& info) {
        if (pid != 0) {
            for (long unsigned int i = 0; i < _predicate_rank.size(); i++) {
                if (_predicate_rank[i].first == pid) {
                    finished += _predicate_rank[i].second * 1.0 / triplet_cnt_;
                    break;
                }
            }
        }

        std::cout << info << finished * 100 << "%    \r";
        std::cout.flush();
    }
};

#endif