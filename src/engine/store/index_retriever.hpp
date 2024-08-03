#ifndef INDEX_RETRIEVER_HPP
#define INDEX_RETRIEVER_HPP

#include <limits.h>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include "../query/result.hpp"
#include "dictionary.hpp"
#include "mmap.hpp"

using Result = ResultList::Result;

enum Order { kSPO, kOPS };

class IndexRetriever {
    std::string db_name_;
    std::string db_dictionary_path_;

    std::string db_index_path_;

    uint predicate_index_file_size_ = 0;
    uint predicate_index_arrays_file_size_ = 0;
    uint entity_index_file_size_ = 0;
    uint po_predicate_map_file_size_ = 0;
    uint ps_predicate_map_file_size_ = 0;
    uint entity_index_arrays_file_size_ = 0;

    MMap<uint> predicate_index_;
    MMap<uint> predicate_index_arrays_;
    MMap<uint> entity_index_;
    MMap<uint> ps_predicate_map_;
    MMap<uint> po_predicate_map_;
    MMap<uint> entity_index_arrays_;

    void LoadDBInfo() {
        MMap<uint> vm = MMap<uint>(db_index_path_ + "DB_INFO", 6 * 4);

        predicate_index_file_size_ = vm[0];
        predicate_index_arrays_file_size_ = vm[1];
        entity_index_file_size_ = vm[2];
        po_predicate_map_file_size_ = vm[3];
        ps_predicate_map_file_size_ = vm[4];
        entity_index_arrays_file_size_ = vm[5];

        vm.CloseMap();
    }

    void InitMMap() {
        predicate_index_ = MMap<uint>(db_index_path_ + "PREDICATE_INDEX", predicate_index_file_size_);
        predicate_index_arrays_ =
            MMap<uint>(db_index_path_ + "PREDICATE_INDEX_ARRAYS", predicate_index_arrays_file_size_);
        entity_index_ = MMap<uint>(db_index_path_ + "ENTITY_INDEX", entity_index_file_size_);
        po_predicate_map_ = MMap<uint>(db_index_path_ + "PO_PREDICATE_MAP", po_predicate_map_file_size_);
        ps_predicate_map_ = MMap<uint>(db_index_path_ + "PS_PREDICATE_MAP", ps_predicate_map_file_size_);
        entity_index_arrays_ =
            MMap<uint>(db_index_path_ + "ENTITY_INDEX_ARRAYS", entity_index_arrays_file_size_);
    }

    Dictionary dict_;

    // IndexMap maps_;

    std::vector<std::shared_ptr<Result>> ps_sets_;
    std::vector<std::shared_ptr<Result>> po_sets_;

    bool PreLoadTree() {
        uint s_array_offset;
        uint s_array_size;
        uint o_array_offset;
        uint o_array_size;

        for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
            s_array_offset = predicate_index_[(pid - 1) * 2];
            o_array_offset = predicate_index_[(pid - 1) * 2 + 1];
            s_array_size = o_array_offset - s_array_offset;
            if (pid != dict_.predicate_cnt())
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

   public:
    uint retrive_cnt = 0;
    uint empty_cnt = 0;
    double empty_time = 0;

    IndexRetriever() {}

    IndexRetriever(std::string db_name) : db_name_(db_name) {
        auto beg = std::chrono::high_resolution_clock::now();

        db_dictionary_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/dictionary/";
        db_index_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/index/";

        LoadDBInfo();
        InitMMap();

        dict_ = Dictionary(db_dictionary_path_);
        std::thread t([&]() { dict_.Load(); });

        PreLoadTree();
        t.join();

        // LoadData();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "load database success. takes " << diff.count() << " ms." << std::endl;
    }

    void close() {
        predicate_index_.CloseMap();
        predicate_index_arrays_.CloseMap();
        entity_index_.CloseMap();
        po_predicate_map_.CloseMap();
        ps_predicate_map_.CloseMap();
        entity_index_arrays_.CloseMap();
    }

    std::string& ID2String(uint id, Pos pos) { return dict_.ID2String(id, pos); }

    uint String2ID(const std::string& str, Pos pos) { return dict_.String2ID(str, pos); }

    uint triplet_cnt() { return dict_.triplet_cnt(); }

    uint predicate_cnt() { return dict_.predicate_cnt(); }

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
        if (pid != dict_.predicate_cnt())
            return predicate_index_[pid * 4] - o_array_offset;
        else
            return predicate_index_arrays_file_size_ / 4 - o_array_offset;
    }

    uint PSSize(uint pid) { return predicate_index_[(pid - 1) * 4 + 1]; }

    uint POSize(uint pid) { return predicate_index_[(pid - 1) * 4 + 3]; }

    std::pair<uint, uint> GetPrediacateSet(uint e, Order order) {
        uint offset;
        uint size;
        if (order == Order::kSPO) {
            offset = entity_index_[(e - 1) * 2];
            return {offset, (entity_index_[e * 2] - offset) / 3};
        }

        offset = entity_index_[(e - 1) * 2 + 1];
        if (e != dict_.max_id()) {
            size = (entity_index_[e * 2 + 1] - offset) / 3;
        } else {
            size = (entity_index_file_size_ / 4 - offset) / 3;
        }

        return {offset, size};
    }

    std::shared_ptr<Result> GetByPS(uint p, uint s) {
        if (s > dict_.shared_cnt() + dict_.subject_cnt())
            return std::make_shared<Result>();

        std::pair<uint, uint> predicate_set = GetPrediacateSet(s, Order::kSPO);

        uint array_offset;
        uint array_size;

        uint pos = 0;
        for (; pos < predicate_set.second; pos++) {
            if (po_predicate_map_[predicate_set.first + 3 * pos] == p) {
                array_offset = po_predicate_map_[predicate_set.first + 3 * pos + 1];
                array_size = po_predicate_map_[predicate_set.first + 3 * pos + 2];
                if (array_size != 1)
                    return std::make_shared<Result>(&entity_index_arrays_[array_offset], array_size);
                else {
                    uint* data = (uint*)malloc(4);
                    data[0] = array_offset;
                    return std::make_shared<Result>(data, 1, true);
                }
            }
        }
        return std::make_shared<Result>();
    }

    uint GetByPSSize(uint p, uint s) {
        std::pair<uint, uint> predicate_set = GetPrediacateSet(s, Order::kSPO);

        for (uint i = 0; i < predicate_set.second; i++) {
            if (po_predicate_map_[predicate_set.first + 3 * i] == p) {
                return po_predicate_map_[predicate_set.first + 3 * i + 2];
            }
        }
        return UINT_MAX;
    }

    std::shared_ptr<Result> GetByPO(uint p, uint o) {
        if (dict_.shared_cnt() < o && o <= dict_.shared_cnt() + dict_.subject_cnt())
            return std::make_shared<Result>();

        std::pair<uint, uint> predicate_set = GetPrediacateSet(o, Order::kOPS);

        uint array_offset;
        uint array_size;

        uint pos = 0;
        for (; pos < predicate_set.second; pos++) {
            if (ps_predicate_map_[predicate_set.first + 3 * pos] == p) {
                array_offset = ps_predicate_map_[predicate_set.first + 3 * pos + 1];
                array_size = ps_predicate_map_[predicate_set.first + 3 * pos + 2];
                if (array_size != 1) {
                    return std::make_shared<Result>(&entity_index_arrays_[array_offset], array_size);
                } else {
                    uint* data = (uint*)malloc(4);
                    data[0] = array_offset;
                    return std::make_shared<Result>(data, 1, true);
                }
            }
        }
        return std::make_shared<Result>();
    }

    uint GetByPOSize(uint p, uint o) {
        std::pair<uint, uint> predicate_set = GetPrediacateSet(o, Order::kOPS);

        for (uint i = 0; i < predicate_set.second; i++) {
            if (ps_predicate_map_[predicate_set.first + 3 * i] == p) {
                return ps_predicate_map_[predicate_set.first + 3 * i + 2];
            }
        }
        return UINT_MAX;
    }
};

#endif
