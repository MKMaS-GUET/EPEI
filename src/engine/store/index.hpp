#ifndef INDEX_HPP
#define INDEX_HPP

#include <malloc.h>
#include <parallel_hashmap/phmap.h>
#include <fstream>
#include <future>
#include <iostream>
#include <vector>
#include "../query/result_vector_list.hpp"
#include "virtual_memory.hpp"

class Index {
    std::string _db_data_path;
    std::string _db_name;

    // uint _predicate_index_file_size = 0;
    // uint _predicate_index_arrays_file_size = 0;
    // uint _entity_index_file_size = 0;
    // uint _ps_predicate_map_file_size = 0;
    // uint _po_predicate_map_file_size = 0;
    // uint _entity_index_arrays_file_size = 0;

    // uint _triplet_cnt = 0;
    // uint _entity_cnt = 0;
    // uint _predicate_cnt = 0;

    // Virtual_Memory _predicate_index;
    // Virtual_Memory _predicate_index_arrays;
    // Virtual_Memory _entity_index;
    // Virtual_Memory _ps_predicate_map;
    // Virtual_Memory _po_predicate_map;
    // Virtual_Memory _entity_index_arrays;

    uint _predicate_index_file_size = 0;
    uint _predicate_index_arrays_file_size = 0;
    uint _entity_index_file_size = 0;
    uint _po_predicate_map_file_size = 0;
    uint _ps_predicate_map_file_size = 0;
    uint _entity_index_arrays_file_size = 0;

    uint _triplet_cnt = 0;
    uint _entity_cnt = 0;
    uint _predicate_cnt = 0;

    Virtual_Memory _predicate_index;
    Virtual_Memory _predicate_index_arrays;
    Virtual_Memory _entity_index;
    Virtual_Memory _ps_predicate_map;
    Virtual_Memory _po_predicate_map;
    Virtual_Memory _entity_index_arrays;

    std::vector<std::shared_ptr<Result_Vector>> ps_sets;
    std::vector<std::shared_ptr<Result_Vector>> po_sets;

    void load_db_info() {
        Virtual_Memory vm = Virtual_Memory(_db_data_path + "DB_INFO", 9 * 4);

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

    // bool sub_build_id2entity(uint entity_cnt) {
    //     std::ifstream entity_ins[4];
    //     for (int i = 0; i < 4; i++) {
    //         entity_ins[i] = std::ifstream(_db_data_path + "ENTITY/" + std::to_string(i),
    //                                       std::ofstream::out | std::ofstream::binary);
    //     }

    //     std::string entity;
    //     id2entity.reserve(_entity_cnt + 1);
    //     id2entity.push_back("");
    //     for (uint id = 1; id <= entity_cnt; id++) {
    //         std::getline(entity_ins[id % 4], entity);
    //         id2entity.push_back(entity);
    //     }

    //     for (int i = 0; i < 4; i++)
    //         entity_ins[i].close();

    //     return true;
    // };

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
        uint offset;
        uint size;
        std::shared_ptr<Result_Vector> rv;
        for (uint pid = 1; pid <= _predicate_cnt; pid++) {
            offset = _predicate_index[(pid - 1) * 4];
            size = _predicate_index[(pid - 1) * 4 + 1];
            rv = std::make_shared<Result_Vector>(size);
            for (uint i = 0; i < size; i++) {
                rv->result[i] = _predicate_index_arrays[offset + i];
            }
            ps_sets.push_back(rv);

            offset = _predicate_index[(pid - 1) * 4 + 2];
            size = _predicate_index[(pid - 1) * 4 + 3];
            rv = std::make_shared<Result_Vector>(size);
            for (uint i = 0; i < size; i++) {
                rv->result[i] = _predicate_index_arrays[offset + i];
            }
            po_sets.push_back(rv);
        }
        return true;
    }

    // bool sub_load_entity(int part, bool* get_all_id, phmap::flat_hash_set<std::string>* entities) {
    //     std::ifstream entity_in(_db_data_path + "ENTITY/" + std::to_string(part),
    //                             std::ofstream::out | std::ofstream::binary);
    //     std::string entity;
    //     uint id = part;
    //     if (part == 0)
    //         id = 4;
    //     while (std::getline(entity_in, entity)) {
    //         id2entity[id] = entity;
    //         entity2id[entity] = id;
    //         if (!(*get_all_id)) {
    //             auto it = entities->find(entity);
    //             if (it != entities->end()) {
    //                 entity2id[entity] = id;
    //                 *get_all_id = entities->size() == entity2id.size();
    //             }
    //         }
    //         id += 4;
    //     }

    //     entity_in.close();
    //     return true;
    // }

    // bool sub_load_entity(int part) {
    //     std::ifstream entity_in(_db_data_path + "ENTITY/" + std::to_string(part),
    //                             std::ofstream::out | std::ofstream::binary);
    //     std::string entity;
    //     uint id = part;
    //     if (part == 0)
    //         id = 4;
    //     while (std::getline(entity_in, entity)) {
    //         id2entity[id] = entity;
    //         id += 4;
    //     }

    //     entity_in.close();
    //     return true;
    // }

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

        // bool get_all_id = entities.size() == entity2id.size();
        entity2id = std::vector<phmap::flat_hash_map<std::string, uint>>(4);

        for (int t = 0; t < 4; t++) {
            // sub_task_list.emplace_back(std::async(std::launch::async, &Index::sub_load_entity, this, t));
            sub_task_list.emplace_back(std::async(std::launch::async, &Index::sub_build_entity2id, this, t));
        }

        sub_task_list.emplace_back(std::async(std::launch::async, &Index::pre_load_tree, this));
        sub_task_list.emplace_back(std::async(std::launch::async, &Index::load_predicate, this));

        // std::ifstream entity_ins[4];
        // for (int i = 0; i < 4; i++) {
        //     entity_ins[i] = std::ifstream(_db_data_path + "ENTITY/" + std::to_string(i),
        //                                   std::ofstream::out | std::ofstream::binary);
        // }

        // std::string entity;
        // id2entity.reserve(_entity_cnt + 1);
        // id2entity.push_back("");
        // for (uint id = 1; id <= _entity_cnt; id++) {
        //     std::getline(entity_ins[id % 4], entity);
        //     id2entity.push_back(entity);
        // }
        // for (int i = 0; i < 4; i++)
        //     entity_ins[i].close();

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
    // std::vector<std::string> id2entity;
    std::vector<const std::string*> id2entity;
    std::vector<phmap::flat_hash_map<std::string, uint>> entity2id;
    // phmap::flat_hash_map<std::string, uint> entity2id;
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

    // void load_data(phmap::flat_hash_set<std::string>& entities) {
    //     auto beg = std::chrono::high_resolution_clock::now();

    //     id2entity.reserve(_entity_cnt + 1);
    //     id2entity.push_back("");

    //     std::vector<std::future<bool>> sub_task_list;

    //     bool get_all_id = entities.size() == entity2id.size();
    //     entity2id = std::vector<phmap::flat_hash_map<std::string, uint>>(4);

    //     for (int t = 0; t < 4; t++) {
    //         sub_task_list.emplace_back(
    //             std::async(std::launch::async, &Index::sub_load_entity, this, t, &get_all_id, &entities));
    //         sub_task_list.emplace_back(std::async(std::launch::async, &Index::sub_build_entity2id, this,
    //         t));
    //     }

    //     sub_task_list.emplace_back(std::async(std::launch::async, &Index::pre_load_tree, this));
    //     sub_task_list.emplace_back(std::async(std::launch::async, &Index::load_predicate, this));

    //     for (std::future<bool>& task : sub_task_list) {
    //         task.get();
    //     }

    //     auto end = std::chrono::high_resolution_clock::now();
    //     std::chrono::duration<double, std::milli> diff = end - beg;
    //     std::cout << "load entity takes " << diff.count() << " ms." << std::endl;
    // }

    void init_vm() {
        _predicate_index = Virtual_Memory(_db_data_path + "PREDICATE_INDEX", _predicate_index_file_size);
        _predicate_index_arrays =
            Virtual_Memory(_db_data_path + "PREDICATE_INDEX_ARRAYS", _predicate_index_arrays_file_size);
        _entity_index = Virtual_Memory(_db_data_path + "ENTITY_INDEX", _entity_index_file_size);
        _po_predicate_map = Virtual_Memory(_db_data_path + "PO_PREDICATE_MAP", _po_predicate_map_file_size);
        _ps_predicate_map = Virtual_Memory(_db_data_path + "PS_PREDICATE_MAP", _ps_predicate_map_file_size);
        _entity_index_arrays =
            Virtual_Memory(_db_data_path + "ENTITY_INDEX_ARRAYS", _entity_index_arrays_file_size);
    }

    void close() {
        _predicate_index.close_vm();
        _predicate_index_arrays.close_vm();
        _entity_index.close_vm();
        _po_predicate_map.close_vm();
        _ps_predicate_map.close_vm();
        _entity_index_arrays.close_vm();

        std::vector<std::future<void>> sub_task_list;

        sub_task_list.emplace_back(std::async(std::launch::async, [&]() {
            for (long unsigned int i = 0; i < ps_sets.size(); i++) {
                std::vector<uint>().swap(ps_sets[i]->result);
                std::vector<uint>().swap(po_sets[i]->result);
            }
        }));

        sub_task_list.emplace_back(
            std::async(std::launch::async, [&]() { std::vector<const std::string*>().swap(id2entity); }));
        // sub_task_list.emplace_back(
        // std::async(std::launch::async, [&]() { std::vector<std::string>().swap(id2entity); }));

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

    // uint get_entity_id(std::string entity) {
    //     auto it = entity2id.find(entity);
    //     if (it != entity2id.end()) {
    //         return it->second;
    //     }
    //     return 0;
    // }

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

    std::shared_ptr<Result_Vector> get_search_range_from_ps_tree(uint pid) {
        // uint offset = _btree_pos[(pid - 1) * 4];
        // uint size = _btree_pos[(pid - 1) * 4 + 1];

        // std::shared_ptr<Result_Vector> rv = std::make_shared<Result_Vector>(size);

        // for (uint i = 0; i < size; i++) {
        //     rv->result[i] = _btrees[offset + i];
        // }
        // return rv;
        return ps_sets[pid - 1];
    }

    std::shared_ptr<Result_Vector> get_search_range_from_po_tree(uint pid) {
        // uint offset = _btree_pos[(pid - 1) * 4 + 2];
        // uint size = _btree_pos[(pid - 1) * 4 + 3];

        // std::shared_ptr<Result_Vector> rv = std::make_shared<Result_Vector>(size);

        // for (uint i = 0; i < size; i++) {
        //     rv->result[i] = _btrees[offset + i];
        // }

        // return rv;
        return po_sets[pid - 1];
    }

    uint ps_size(uint pid) { return _predicate_index[(pid - 1) * 4 + 1]; }

    uint po_size(uint pid) { return _predicate_index[(pid - 1) * 4 + 3]; }

    std::shared_ptr<Result_Vector> get_by_ps(uint p, uint s) {
        uint offset = _entity_index[(s - 1) * 4];
        uint size = _entity_index[(s - 1) * 4 + 1];

        uint array_offset;
        uint array_size;

        std::shared_ptr<Result_Vector> rv;

        for (uint i = 0; i < size; i++) {
            if (_po_predicate_map[offset + 3 * i] == p) {
                array_offset = _po_predicate_map[offset + 3 * i + 1];
                array_size = _po_predicate_map[offset + 3 * i + 2];
                rv = std::make_shared<Result_Vector>(array_size);
                for (uint j = 0; j < array_size; j++) {
                    rv->result[j] = _entity_index_arrays[array_offset + j];
                }
                // std::cout << "ps " << p << " " << s << " " << rv->result.size() << std::endl;
                // usleep(100000);
                return rv;
            }
        }
        return std::make_shared<Result_Vector>(0);
    }

    std::shared_ptr<Result_Vector> get_by_po(uint p, uint o) {
        uint offset = _entity_index[(o - 1) * 4 + 2];
        uint size = _entity_index[(o - 1) * 4 + 3];

        uint array_offset;
        uint array_size;

        std::shared_ptr<Result_Vector> rv;

        for (uint i = 0; i < size; i++) {
            if (_ps_predicate_map[offset + 3 * i] == p) {
                array_offset = _ps_predicate_map[offset + 3 * i + 1];
                array_size = _ps_predicate_map[offset + 3 * i + 2];
                rv = std::make_shared<Result_Vector>(array_size);
                for (uint j = 0; j < array_size; j++) {
                    rv->result[j] = _entity_index_arrays[array_offset + j];
                }
                // std::cout << "po " << p << " " << o << " " << rv->result.size() << std::endl;
                // usleep(100000);
                return rv;
            }
        }
        return std::make_shared<Result_Vector>(0);
    }
};

#endif