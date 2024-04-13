#ifndef INDEX_HPP
#define INDEX_HPP

#include <malloc.h>
#include <parallel_hashmap/phmap.h>
#include <fstream>
#include <future>
#include <iostream>
#include <vector>
#include "../query/result.hpp"
#include "virtual_memory.hpp"

class Index {
    std::string _db_data_path;
    std::string _db_name;

    uint _predicate_index_file_size = 0;
    uint _predicate_index_arrays_file_size = 0;
    uint _entity_index_file_size = 0;
    uint _po_predicate_map_file_size = 0;
    uint _ps_predicate_map_file_size = 0;
    uint _entity_index_arrays_file_size = 0;

    uint _triplet_cnt = 0;
    uint _entity_cnt = 0;
    uint _predicate_cnt = 0;

    VirtualMemory _predicate_index;
    VirtualMemory _predicate_index_arrays;
    VirtualMemory _entity_index;
    VirtualMemory _ps_predicate_map;
    VirtualMemory _po_predicate_map;
    VirtualMemory _entity_index_arrays;

    std::vector<std::shared_ptr<Result>> ps_sets;
    std::vector<std::shared_ptr<Result>> po_sets;

    void load_db_info() {
        VirtualMemory vm = VirtualMemory(_db_data_path + "DB_INFO", 9 * 4);

        _predicate_index_file_size = vm[0];
        _predicate_index_arrays_file_size = vm[1];
        _entity_index_file_size = vm[2];
        _po_predicate_map_file_size = vm[3];
        _ps_predicate_map_file_size = vm[4];
        _entity_index_arrays_file_size = vm[5];
        _triplet_cnt = vm[6];
        _entity_cnt = vm[7];
        _predicate_cnt = vm[8];

        // std::cout << _btree_pos_file_size << std::endl;
        // std::cout << _btrees_file_size << std::endl;
        // std::cout << _predicate_array_pos_file_size << std::endl;
        // std::cout << _so_predicate_array_file_size << std::endl;
        // std::cout << _os_predicate_array_file_size << std::endl;
        // std::cout << _array_file_size << std::endl;
        // std::cout << _triplet_cnt << std::endl;
        // std::cout << _entity_cnt << std::endl;
        // std::cout << _predicate_cnt << std::endl;

        vm.close_vm();
    }

    bool load_predicate() {
        std::ifstream predicate_in(_db_data_path + "PREDICATE", std::ofstream::out | std::ofstream::binary);
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

    bool pre_load_tree() {
        uint s_array_offset;
        uint s_array_size;
        uint o_array_offset;
        uint o_array_size;

        for (uint pid = 1; pid <= _predicate_cnt; pid++) {
            s_array_offset = _predicate_index[(pid - 1) * 4];
            o_array_offset = _predicate_index[(pid - 1) * 4 + 2];
            s_array_size = o_array_offset - s_array_offset;
            if (pid != _predicate_cnt)
                o_array_size = _predicate_index[pid * 4] - o_array_offset;
            else
                o_array_size = _predicate_index_arrays_file_size / 4 - o_array_offset;

            uint* set = new uint[s_array_size];
            for (uint i = 0; i < s_array_size; i++) {
                set[i] = _predicate_index_arrays[s_array_offset + i];
            }
            ps_sets.push_back(std::make_shared<Result>(set, s_array_size, true));

            set = new uint[o_array_size];
            for (uint i = 0; i < o_array_size; i++) {
                set[i] = _predicate_index_arrays[o_array_offset + i];
            }
            po_sets.push_back(std::make_shared<Result>(set, o_array_size, true));
        }
        return true;
    }

    bool sub_build_entity2id(int part) {
        std::ifstream entity_in(_db_data_path + "ENTITY/" + std::to_string(part),
                                std::ofstream::out | std::ofstream::binary);

        std::string entity;
        uint id = part;
        if (part == 0)
            id = 4;
        while (std::getline(entity_in, entity)) {
            entity2id[part][entity] = id;
            id += 4;
        }

        entity_in.close();
        return true;
    };

    void load_data() {
        std::vector<std::future<bool>> sub_task_list;

        entity2id = std::vector<phmap::flat_hash_map<std::string, uint>>(4);

        for (int t = 0; t < 4; t++) {
            sub_task_list.emplace_back(std::async(std::launch::async, &Index::sub_build_entity2id, this, t));
        }

        sub_task_list.emplace_back(std::async(std::launch::async, &Index::pre_load_tree, this));
        sub_task_list.emplace_back(std::async(std::launch::async, &Index::load_predicate, this));

        for (std::future<bool>& task : sub_task_list) {
            task.get();
        }

        id2entity.reserve(_entity_cnt + 1);
        for (int part = 0; part < 4; part++) {
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

    Index(std::string db_name) : _db_name(db_name) {
        _db_data_path = "./DB_DATA_ARCHIVE/" + _db_name + "/";
        auto beg = std::chrono::high_resolution_clock::now();

        load_db_info();
        init_vm();
        load_data();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "load database success. takes " << diff.count() << " ms." << std::endl;
    }

    void init_vm() {
        _predicate_index = VirtualMemory(_db_data_path + "PREDICATE_INDEX", _predicate_index_file_size);
        _predicate_index_arrays =
            VirtualMemory(_db_data_path + "PREDICATE_INDEX_ARRAYS", _predicate_index_arrays_file_size);
        _entity_index = VirtualMemory(_db_data_path + "ENTITY_INDEX", _entity_index_file_size);
        _po_predicate_map = VirtualMemory(_db_data_path + "PO_PREDICATE_MAP", _po_predicate_map_file_size);
        _ps_predicate_map = VirtualMemory(_db_data_path + "PS_PREDICATE_MAP", _ps_predicate_map_file_size);
        _entity_index_arrays =
            VirtualMemory(_db_data_path + "ENTITY_INDEX_ARRAYS", _entity_index_arrays_file_size);
    }

    void close() {
        _predicate_index.close_vm();
        _predicate_index_arrays.close_vm();
        _entity_index.close_vm();
        _po_predicate_map.close_vm();
        _ps_predicate_map.close_vm();
        _entity_index_arrays.close_vm();

        std::vector<std::future<void>> sub_task_list;

        sub_task_list.emplace_back(
            std::async(std::launch::async, [&]() { std::vector<const std::string*>().swap(id2entity); }));

        sub_task_list.emplace_back(std::async(
            std::launch::async, [&]() { phmap::flat_hash_map<std::string, uint>().swap(entity2id[0]); }));
        sub_task_list.emplace_back(std::async(
            std::launch::async, [&]() { phmap::flat_hash_map<std::string, uint>().swap(entity2id[1]); }));
        sub_task_list.emplace_back(std::async(
            std::launch::async, [&]() { phmap::flat_hash_map<std::string, uint>().swap(entity2id[2]); }));
        sub_task_list.emplace_back(std::async(
            std::launch::async, [&]() { phmap::flat_hash_map<std::string, uint>().swap(entity2id[3]); }));

        sub_task_list.emplace_back(std::async(std::launch::async, [&]() {
            std::vector<std::string>().swap(id2predicate);
            phmap::flat_hash_map<std::string, uint>().swap(predicate2id);
        }));

        for (std::future<void>& task : sub_task_list) {
            task.get();
        }

        malloc_trim(0);
    }

    uint get_entity_id(std::string entity) {
        for (int part = 0; part < 4; part++) {
            auto it = entity2id[part].find(entity);
            if (it != entity2id[part].end()) {
                return it->second;
            }
        }
        return 0;
    }

    uint get_triplet_cnt() { return _triplet_cnt; }

    uint get_entity_cnt() { return _entity_cnt; }

    uint get_predicate_cnt() { return _predicate_cnt; }

    std::shared_ptr<Result> get_s_set(uint pid) {
        // uint s_array_offset = _predicate_index[(pid - 1) * 4];
        // uint o_array_offset = _predicate_index[(pid - 1) * 4 + 2];
        // uint s_array_size = o_array_offset - s_array_offset;

        // return std::make_shared<Result>(&_predicate_index_arrays[s_array_offset], s_array_size);
        // std::cout << ps_sets[pid - 1]->size() << std::endl;
        return ps_sets[pid - 1];
    }

    uint get_s_set_size(uint pid) {
        return _predicate_index[(pid - 1) * 4 + 2] - _predicate_index[(pid - 1) * 4];
    }

    std::shared_ptr<Result> get_o_set(uint pid) {
        // uint o_array_offset = _predicate_index[(pid - 1) * 4 + 2];
        // uint o_array_size;
        // if (pid != _predicate_cnt)
        //     o_array_size = _predicate_index[pid * 4] - o_array_offset;
        // else
        //     o_array_size = _predicate_index_arrays_file_size / 4 - o_array_offset;

        // return std::make_shared<Result>(&_predicate_index_arrays[o_array_offset], o_array_size);
        // std::cout << po_sets[pid - 1]->size() << std::endl;
        return po_sets[pid - 1];
    }

    uint get_o_set_size(uint pid) {
        uint o_array_offset = _predicate_index[(pid - 1) * 4 + 2];
        if (pid != _predicate_cnt)
            return _predicate_index[pid * 4] - o_array_offset;
        else
            return _predicate_index_arrays_file_size / 4 - o_array_offset;
    }

    uint ps_size(uint pid) { return _predicate_index[(pid - 1) * 4 + 1]; }

    uint po_size(uint pid) { return _predicate_index[(pid - 1) * 4 + 3]; }

    uint search(VirtualMemory& vm, uint offset, uint size, uint pid) {
        uint low = 0;
        uint high = size - 1;
        while (low <= high) {
            uint mid = low + (high - low) / 2;  // 防止溢出
            if (vm[offset + 3 * mid] == pid) {
                return mid;
            } else if (vm[offset + 3 * mid] < pid) {
                low = mid + 1;  // 查找右侧区间
            } else {
                if (mid == 0)
                    break;
                high = mid - 1;  // 查找左侧区间
            }
        }

        return UINT_MAX;  // 没有找到元素
    }

    std::shared_ptr<Result> get_by_ps(uint p, uint s) {
        uint offset = _entity_index[(s - 1) * 4];
        uint size = _entity_index[(s - 1) * 4 + 1];

        uint array_offset;
        uint array_size;

        uint pos = 0;
        for (; pos < size; pos++) {
            if (_po_predicate_map[offset + 3 * pos] == p) {
                array_offset = _po_predicate_map[offset + 3 * pos + 1];
                array_size = _po_predicate_map[offset + 3 * pos + 2];
                return std::make_shared<Result>(&_entity_index_arrays[array_offset], array_size);
            }
        }
        return std::make_shared<Result>();
    }

    uint get_by_ps_size(uint p, uint s) {
        uint offset = _entity_index[(s - 1) * 4];
        uint size = _entity_index[(s - 1) * 4 + 1];

        for (uint i = 0; i < size; i++) {
            if (_po_predicate_map[offset + 3 * i] == p) {
                return _po_predicate_map[offset + 3 * i + 2];
            }
        }
        return UINT_MAX;
    }

    std::shared_ptr<Result> get_by_po(uint p, uint o) {
        uint offset = _entity_index[(o - 1) * 4 + 2];
        uint size = _entity_index[(o - 1) * 4 + 3];

        uint array_offset;
        uint array_size;

        uint pos = 0;
        for (; pos < size; pos++) {
            if (_ps_predicate_map[offset + 3 * pos] == p) {
                array_offset = _ps_predicate_map[offset + 3 * pos + 1];
                array_size = _ps_predicate_map[offset + 3 * pos + 2];
                return std::make_shared<Result>(&_entity_index_arrays[array_offset], array_size);
            }
        }
        return std::make_shared<Result>();
    }

    uint get_by_po_size(uint p, uint o) {
        uint offset = _entity_index[(o - 1) * 4 + 2];
        uint size = _entity_index[(o - 1) * 4 + 3];

        for (uint i = 0; i < size; i++) {
            if (_ps_predicate_map[offset + 3 * i] == p) {
                return _ps_predicate_map[offset + 3 * i + 2];
            }
        }
        return UINT_MAX;
    }
};

#endif