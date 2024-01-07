#include <parallel_hashmap/phmap.h>
#include <fstream>
#include <future>
#include <iostream>
#include <vector>
#include "../query/result_vector_list.hpp"
#include "virtual_memory.hpp"

#ifndef INDEX_HPP
#define INDEX_HPP

class Index {
    std::string _db_data_path;
    std::string _db_name;

    uint _btree_pos_file_size = 0;
    uint _btrees_file_size = 0;
    uint _predicate_array_pos_file_size = 0;
    uint _so_predicate_array_file_size = 0;
    uint _os_predicate_array_file_size = 0;
    uint _array_file_size = 0;

    uint _triplet_cnt = 0;
    uint _entity_cnt = 0;
    uint _predicate_cnt = 0;

    Virtual_Memory _btree_pos;
    Virtual_Memory _btrees;
    Virtual_Memory _predicate_array_pos;
    Virtual_Memory _os_predicate_array;
    Virtual_Memory _so_predicate_array;
    Virtual_Memory _arrays;

    std::vector<std::shared_ptr<Result_Vector>> ps_sets;
    std::vector<std::shared_ptr<Result_Vector>> po_sets;

    void load_db_info() {
        Virtual_Memory vm = Virtual_Memory(_db_data_path + "DB_INFO", 9 * 4);

        _btree_pos_file_size = vm[0];
        _btrees_file_size = vm[1];
        _predicate_array_pos_file_size = vm[2];
        _so_predicate_array_file_size = vm[3];
        _os_predicate_array_file_size = vm[4];
        _array_file_size = vm[5];
        _triplet_cnt = vm[6];
        _entity_cnt = vm[7];
        _predicate_cnt = vm[8];

        // std::cout << _btree_pos_file_size << std::endl;
        // std::cout << _btrees_file_size << std::endl;
        // std::cout << _predicate_array_pos_file_size << std::endl;
        // std::cout << _so_predicate_array_file_size << std::endl;
        // std::cout << _os_predicate_array_file_size << std::endl;
        // std::cout << _array_file_size << std::endl;

        vm.close_vm();
    }

    void load_entity() {
        auto beg = std::chrono::high_resolution_clock::now();

        // for (int i = 0; i < 4; i++)
        //     entity2id[i] = phmap::flat_hash_map<std::string, uint>();

        entity2id = std::vector<phmap::flat_hash_map<std::string, uint>>(4);

        std::vector<std::future<bool>> sub_build_task_list;
        for (int t = 0; t < 4; t++) {
            sub_build_task_list.emplace_back(
                std::async(std::launch::async, &Index::sub_build_entity2id, this, t));
        }
        sub_build_task_list.emplace_back(
            std::async(std::launch::async, &Index::sub_build_id2entity, this, _entity_cnt));

        for (std::future<bool>& task : sub_build_task_list) {
            task.get();
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "takes " << diff.count() << " ms." << std::endl;
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

    bool sub_build_id2entity(uint entity_cnt) {
        std::ifstream entity_ins[4];
        for (int i = 0; i < 4; i++) {
            entity_ins[i] = std::ifstream(_db_data_path + "ENTITY/" + std::to_string(i),
                                          std::ofstream::out | std::ofstream::binary);
        }

        std::string entity;
        id2entity.reserve(_entity_cnt + 1);
        id2entity.push_back("");
        for (uint id = 1; id <= entity_cnt; id++) {
            std::getline(entity_ins[id % 4], entity);
            id2entity.push_back(entity);
        }

        for (int i = 0; i < 4; i++)
            entity_ins[i].close();

        return true;
    };

    void load_predicate() {
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
    }

   public:
    std::vector<std::string> id2entity;
    std::vector<phmap::flat_hash_map<std::string, uint>> entity2id;
    std::vector<std::string> id2predicate;
    phmap::flat_hash_map<std::string, uint> predicate2id;

    Index() {}

    Index(std::string db_name) : _db_name(db_name) {
        _db_data_path = "./DB_DATA_ARCHIVE/" + _db_name + "/";
        load_db_info();
        init_vm();
        load_predicate();
        load_entity();
        // pre_load_tree();
    }

    void init_vm() {
        _btree_pos = Virtual_Memory(_db_data_path + "BTREE_POS", _btree_pos_file_size);
        _btrees = Virtual_Memory(_db_data_path + "BTREES", _btrees_file_size);
        _predicate_array_pos =
            Virtual_Memory(_db_data_path + "PREDICATE_ARRAY_POS", _predicate_array_pos_file_size);
        _so_predicate_array =
            Virtual_Memory(_db_data_path + "SO_PREDICATE_ARRAY", _so_predicate_array_file_size);
        _os_predicate_array =
            Virtual_Memory(_db_data_path + "OS_PREDICATE_ARRAY", _os_predicate_array_file_size);
        _arrays = Virtual_Memory(_db_data_path + "ARRAYS", _array_file_size);
    }

    void close_vm() {
        _btree_pos.close_vm();
        _btrees.close_vm();
        _predicate_array_pos.close_vm();
        _os_predicate_array.close_vm();
        _os_predicate_array.close_vm();
        _arrays.close_vm();
    }

    ~Index() {
        _btree_pos.close_vm();
        _btrees.close_vm();
        _predicate_array_pos.close_vm();
        _os_predicate_array.close_vm();
        _os_predicate_array.close_vm();
        _arrays.close_vm();
    };

    uint get_entity_id(std::string entity) {
        for (int part = 0; part < 4; part++) {
            auto it = entity2id[part].find(entity);
            if (it != entity2id[part].end()) {
                return it->second;
            }
        }
        return 0;
    }

    // void pre_load_tree() {
    //     uint offset;
    //     uint size;
    //     std::shared_ptr<Result_Vector> rv;
    //     for (uint pid = 1; pid <= _predicate_cnt; pid++) {
    //         offset = _btree_pos[(pid - 1) * 4];
    //         size = _btree_pos[(pid - 1) * 4 + 1];
    //         rv = std::make_shared<Result_Vector>(size);
    //         for (uint i = 0; i < size; i++) {
    //             rv->result[i] = _btrees[offset + i];
    //         }
    //         ps_sets.push_back(rv);

    //         offset = _btree_pos[(pid - 1) * 4 + 2];
    //         size = _btree_pos[(pid - 1) * 4 + 3];
    //         rv = std::make_shared<Result_Vector>(size);
    //         for (uint i = 0; i < size; i++) {
    //             rv->result[i] = _btrees[offset + i];
    //         }
    //         po_sets.push_back(rv);
    //     }
    // }

    std::shared_ptr<Result_Vector> get_search_range_from_ps_tree(uint pid) {
        uint offset = _btree_pos[(pid - 1) * 4];
        uint size = _btree_pos[(pid - 1) * 4 + 1];

        std::shared_ptr<Result_Vector> rv = std::make_shared<Result_Vector>(size);

        for (uint i = 0; i < size; i++) {
            rv->result[i] = _btrees[offset + i];
        }
        return rv;
        // return ps_sets[pid - 1];
    }

    std::shared_ptr<Result_Vector> get_search_range_from_po_tree(uint pid) {
        uint offset = _btree_pos[(pid - 1) * 4 + 2];
        uint size = _btree_pos[(pid - 1) * 4 + 3];

        std::shared_ptr<Result_Vector> rv = std::make_shared<Result_Vector>(size);

        for (uint i = 0; i < size; i++) {
            rv->result[i] = _btrees[offset + i];
        }

        return rv;
        // return po_sets[pid - 1];
    }

    uint ps_size(uint pid) { return _btree_pos[(pid - 1) * 4 + 1]; }

    uint po_size(uint pid) { return _btree_pos[(pid - 1) * 4 + 3]; }

    std::shared_ptr<Result_Vector> get_by_ps(uint p, uint s) {
        uint offset = _predicate_array_pos[(s - 1) * 4];
        uint size = _predicate_array_pos[(s - 1) * 4 + 1];

        uint array_offset;
        uint array_size;

        std::shared_ptr<Result_Vector> rv;

        for (uint i = 0; i < size; i++) {
            if (_so_predicate_array[offset + 3 * i] == p) {
                array_offset = _so_predicate_array[offset + 3 * i + 1];
                array_size = _so_predicate_array[offset + 3 * i + 2];
                rv = std::make_shared<Result_Vector>(array_size);
                for (uint j = 0; j < array_size; j++) {
                    rv->result[j] = _arrays[array_offset + j];
                }
                return rv;
            }
        }
        return std::make_shared<Result_Vector>(0);
    }

    std::shared_ptr<Result_Vector> get_by_po(uint p, uint o) {
        uint offset = _predicate_array_pos[(o - 1) * 4 + 2];
        uint size = _predicate_array_pos[(o - 1) * 4 + 3];

        uint array_offset;
        uint array_size;

        std::shared_ptr<Result_Vector> rv;

        for (uint i = 0; i < size; i++) {
            if (_os_predicate_array[offset + 3 * i] == p) {
                array_offset = _os_predicate_array[offset + 3 * i + 1];
                array_size = _os_predicate_array[offset + 3 * i + 2];
                rv = std::make_shared<Result_Vector>(array_size);
                for (uint j = 0; j < array_size; j++) {
                    rv->result[j] = _arrays[array_offset + j];
                }
                return rv;
            }
        }
        return std::make_shared<Result_Vector>(0);
    }
};

#endif