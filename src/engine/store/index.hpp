#ifndef INDEX_HPP
#define INDEX_HPP

#include <malloc.h>
#include <parallel_hashmap/phmap.h>
#include <fstream>
#include <future>
#include <iostream>
#include <vector>
#include "../query/result.hpp"
#include "mmap.hpp"

class Index {
    std::string db_data_path_;
    std::string db_name_;

    uint predicate_index_file_size_ = 0;
    uint predicate_index_arrays_file_size_ = 0;
    uint entity_index_file_size_ = 0;
    uint po_predicate_map_file_size_ = 0;
    uint ps_predicate_map_file_size_ = 0;
    uint entity_index_arrays_file_size_ = 0;

    uint triplet_cnt_ = 0;
    uint entity_cnt_ = 0;
    uint predicate_cnt_ = 0;
    uint entity_size_ = 0;

    MMap<uint> predicate_index_;
    MMap<uint> predicate_index_arrays_;
    MMap<uint> entity_index_;
    MMap<uint> ps_predicate_map_;
    MMap<uint> po_predicate_map_;
    MMap<uint> entity_index_arrays_;

    // MMap<ulong> _entity_hash;
    // MMap<uint> _entity_id;

    // MMap<uint> _id2pos;
    // MMap<char> _entities;

    // char** entities_cache;

    // char* _entities_mem;

    // phmap::flat_hash_map<ulong, std::vector<std::pair<std::string, uint>>> _conflicts;

    std::vector<std::shared_ptr<Result>> ps_sets_;
    std::vector<std::shared_ptr<Result>> po_sets_;

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

        // std::cout << _btree_pos_file_size << std::endl;
        // std::cout << _btrees_file_size << std::endl;
        // std::cout << _predicate_array_pos_file_size << std::endl;
        // std::cout << _so_predicate_array_file_size << std::endl;
        // std::cout << _os_predicate_array_file_size << std::endl;
        // std::cout << _array_file_size << std::endl;
        // std::cout << triplet_cnt_ << std::endl;
        // std::cout << entity_cnt_ << std::endl;
        // std::cout << predicate_cnt_ << std::endl;

        vm.CloseMap();
    }

    bool PreLoadTree() {
        uint s_array_offset;
        uint s_array_size;
        uint o_array_offset;
        uint o_array_size;

        for (uint pid = 1; pid <= predicate_cnt_; pid++) {
            s_array_offset = predicate_index_[(pid - 1) * 2];
            o_array_offset = predicate_index_[(pid - 1) * 2 + 1];
            s_array_size = o_array_offset - s_array_offset;
            if (pid != predicate_cnt_)
                o_array_size = predicate_index_[pid * 2] - o_array_offset;
            else
                o_array_size = predicate_index_arrays_file_size_ / 4 - o_array_offset;

            uint* set = new uint[s_array_size];
            for (uint i = 0; i < s_array_size; i++) {
                set[i] = predicate_index_arrays_[s_array_offset + i];
            }
            ps_sets_.push_back(std::make_shared<Result>(set, s_array_size, true));

            set = new uint[o_array_size];
            for (uint i = 0; i < o_array_size; i++) {
                set[i] = predicate_index_arrays_[o_array_offset + i];
            }
            po_sets_.push_back(std::make_shared<Result>(set, o_array_size, true));
        }
        return true;
    }

    bool LoadPredicate() {
        std::ifstream predicate_in(db_data_path_ + "PREDICATE", std::ofstream::out | std::ofstream::binary);
        std::string predicate;
        uint id = 1;
        id2predicate.push_back("");
        while (std::getline(predicate_in, predicate)) {
            predicate2id[predicate] = id;
            id2predicate.push_back(predicate);
            id++;
        }
        predicate_in.close();
        return true;
    }

    bool SubBuildEntity2Id(int part) {
        std::ifstream entity_in(db_data_path_ + "ENTITY/" + std::to_string(part),
                                std::ofstream::out | std::ofstream::binary);

        std::string entity;
        uint id = part;
        if (part == 0)
            id = 6;
        while (std::getline(entity_in, entity)) {
            entity2id[part][entity] = id;
            id += 6;
        }

        entity_in.close();
        return true;
    };

    void LoadData() {
        std::vector<std::future<bool>> sub_task_list;

        entity2id = std::vector<phmap::flat_hash_map<std::string, uint>>(6);

        for (int t = 0; t < 6; t++) {
            sub_task_list.emplace_back(std::async(std::launch::async, &Index::SubBuildEntity2Id, this, t));
        }

        sub_task_list.emplace_back(std::async(std::launch::async, &Index::PreLoadTree, this));
        sub_task_list.emplace_back(std::async(std::launch::async, &Index::LoadPredicate, this));

        for (std::future<bool>& task : sub_task_list) {
            task.get();
        }

        id2entity.reserve(entity_cnt_ + 1);
        for (int part = 0; part < 6; part++) {
            for (auto it = entity2id[part].begin(); it != entity2id[part].end(); it++) {
                id2entity[it->second] = &it->first;
            }
        }

    }

   public:
    std::vector<const std::string*> id2entity;
    std::vector<phmap::flat_hash_map<std::string, uint>> entity2id;
    std::vector<std::string> id2predicate;
    phmap::flat_hash_map<std::string, uint> predicate2id;

    Index() {}

    Index(std::string db_name) : db_name_(db_name) {
        db_data_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/";
        auto beg = std::chrono::high_resolution_clock::now();

        LoadDBInfo();
        InitMMap();
        LoadData();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "load database success. takes " << diff.count() << " ms." << std::endl;
    }

    void InitMMap() {
        predicate_index_ = MMap<uint>(db_data_path_ + "PREDICATE_INDEX", predicate_index_file_size_);
        predicate_index_arrays_ =
            MMap<uint>(db_data_path_ + "PREDICATE_INDEX_ARRAYS", predicate_index_arrays_file_size_);
        entity_index_ = MMap<uint>(db_data_path_ + "ENTITY_INDEX", entity_index_file_size_);
        po_predicate_map_ = MMap<uint>(db_data_path_ + "PO_PREDICATE_MAP", po_predicate_map_file_size_);
        ps_predicate_map_ = MMap<uint>(db_data_path_ + "PS_PREDICATE_MAP", ps_predicate_map_file_size_);
        entity_index_arrays_ =
            MMap<uint>(db_data_path_ + "ENTITY_INDEX_ARRAYS", entity_index_arrays_file_size_);
    }

    void close() {
        predicate_index_.CloseMap();
        predicate_index_arrays_.CloseMap();
        entity_index_.CloseMap();
        po_predicate_map_.CloseMap();
        ps_predicate_map_.CloseMap();
        entity_index_arrays_.CloseMap();

        std::vector<std::future<void>> sub_task_list;

        sub_task_list.emplace_back(std::async(std::launch::async, [&]() {
            std::vector<std::string>().swap(id2predicate);
            phmap::flat_hash_map<std::string, uint>().swap(predicate2id);
        }));

        for (std::future<void>& task : sub_task_list) {
            task.get();
        }

        malloc_trim(0);
    }

    uint GetEntityID(std::string entity) {
        for (int part = 0; part < 6; part++) {
            auto it = entity2id[part].find(entity);
            if (it != entity2id[part].end()) {
                return it->second;
            }
        }
        return 0;
    }

    uint GetTripletCnt() { return triplet_cnt_; }

    uint GetEntityCnt() { return entity_cnt_; }

    uint GetPredicateCnt() { return predicate_cnt_; }

    std::shared_ptr<Result> GetSSet(uint pid) {
        // uint s_array_offset = predicate_index_[(pid - 1) * 4];
        // uint o_array_offset = predicate_index_[(pid - 1) * 4 + 2];
        // uint s_array_size = o_array_offset - s_array_offset;

        // return std::make_shared<Result>(&predicate_index_arrays_[s_array_offset], s_array_size);
        // std::cout << ps_sets_[pid - 1]->size() << std::endl;
        return ps_sets_[pid - 1];
    }

    uint GetSSetSize(uint pid) {
        return predicate_index_[(pid - 1) * 4 + 2] - predicate_index_[(pid - 1) * 4];
    }

    std::shared_ptr<Result> GetOSet(uint pid) {
        // uint o_array_offset = predicate_index_[(pid - 1) * 4 + 2];
        // uint o_array_size;
        // if (pid != predicate_cnt_)
        //     o_array_size = predicate_index_[pid * 4] - o_array_offset;
        // else
        //     o_array_size = predicate_index_arrays_file_size_ / 4 - o_array_offset;

        // return std::make_shared<Result>(&predicate_index_arrays_[o_array_offset], o_array_size);
        // std::cout << po_sets_[pid - 1]->size() << std::endl;
        return po_sets_[pid - 1];
    }

    uint GetOSetSize(uint pid) {
        uint o_array_offset = predicate_index_[(pid - 1) * 4 + 2];
        if (pid != predicate_cnt_)
            return predicate_index_[pid * 4] - o_array_offset;
        else
            return predicate_index_arrays_file_size_ / 4 - o_array_offset;
    }

    uint PSSize(uint pid) { return predicate_index_[(pid - 1) * 4 + 1]; }

    uint POSize(uint pid) { return predicate_index_[(pid - 1) * 4 + 3]; }

    std::shared_ptr<Result> GetByPS(uint p, uint s) {
        // sleep(1);
        // std::cout << "p: " << p << " " << "s: " << s;
        uint offset = entity_index_[(s - 1) * 2];
        uint size;
        if (s != entity_cnt_ - 1) {
            size = (entity_index_[s * 2] - offset) / 3;
        } else {
            size = (ps_predicate_map_file_size_ / 4 - offset) / 3;
        }

        uint array_offset;
        uint array_size;

        uint pos = 0;
        for (; pos < size; pos++) {
            if (po_predicate_map_[offset + 3 * pos] == p) {
                array_offset = po_predicate_map_[offset + 3 * pos + 1];
                array_size = po_predicate_map_[offset + 3 * pos + 2];
                // std::cout << " size: " << array_size << std::endl;
                if (array_size != 1)
                    return std::make_shared<Result>(&entity_index_arrays_[array_offset], array_size);
                else {
                    uint* data = (uint*)malloc(4);
                    data[0] = array_offset;
                    return std::make_shared<Result>(data, 1, true);
                }
            }
        }
        // std::cout << " size: " << 0 << std::endl;
        return std::make_shared<Result>();
    }

    uint GetByPSSize(uint p, uint s) {
        uint offset = entity_index_[(s - 1) * 2];
        uint size;
        if (s != entity_cnt_ - 1) {
            size = (entity_index_[s * 2] - offset) / 3;
        } else {
            size = (po_predicate_map_file_size_ / 4 - offset) / 3;
        }

        for (uint i = 0; i < size; i++) {
            if (po_predicate_map_[offset + 3 * i] == p) {
                return po_predicate_map_[offset + 3 * i + 2];
            }
        }
        return UINT_MAX;
    }

    std::shared_ptr<Result> GetByPO(uint p, uint o) {
        // sleep(1);
        // std::cout << "p: " << p << " " << "o: " << o;
        uint offset = entity_index_[(o - 1) * 2 + 1];
        uint size;
        if (o != entity_cnt_ - 1) {
            size = (entity_index_[o * 2 + 1] - offset) / 3;
        } else {
            size = (entity_index_file_size_ / 4 - offset) / 3;
        }

        uint array_offset;
        uint array_size;

        uint pos = 0;
        for (; pos < size; pos++) {
            if (ps_predicate_map_[offset + 3 * pos] == p) {
                array_offset = ps_predicate_map_[offset + 3 * pos + 1];
                array_size = ps_predicate_map_[offset + 3 * pos + 2];
                // std::cout << " size: " << array_size << std::endl;
                if (array_size != 1) {
                    return std::make_shared<Result>(&entity_index_arrays_[array_offset], array_size);
                } else {
                    uint* data = (uint*)malloc(4);
                    data[0] = array_offset;
                    return std::make_shared<Result>(data, 1, true);
                }
            }
        }
        // std::cout << " size: " << 0 << std::endl;
        return std::make_shared<Result>();
    }

    uint GetByPoSize(uint p, uint o) {
        uint offset = entity_index_[(o - 1) * 2 + 1];
        uint size;
        if (o != entity_cnt_ - 1) {
            size = (entity_index_[o * 2 + 1] - offset) / 3;
        } else {
            size = (entity_index_file_size_ / 4 - offset) / 3;
        }

        for (uint i = 0; i < size; i++) {
            if (ps_predicate_map_[offset + 3 * i] == p) {
                return ps_predicate_map_[offset + 3 * i + 2];
            }
        }
        return UINT_MAX;
    }
};

#endif
