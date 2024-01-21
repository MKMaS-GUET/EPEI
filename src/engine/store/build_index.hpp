#ifndef BUILD_INDEX_HPP
#define BUILD_INDEX_HPP

#include <fcntl.h>
#include <malloc.h>
#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include <sys/mman.h>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "virtual_memory.hpp"

#define MAX_SIZE 20000

namespace fs = std::filesystem;
using entity_set = phmap::btree_set<uint>;
using phmap::btree_map;
using phmap::flat_hash_map;
using phmap::flat_hash_set;
using std::pair;

class Node {
   public:
    std::vector<uint> nums;
    Node* next = 0;

    Node() { nums = std::vector<uint>(); }

    Node(int reverse_size) {
        nums = std::vector<uint>();
        nums.reserve(reverse_size);
    }

    void move_halh_to(Node* target_node) {
        auto middle = nums.begin() + nums.size() / 2;

        std::move(std::make_move_iterator(middle), std::make_move_iterator(nums.end()),
                  std::back_inserter(target_node->nums));

        nums.erase(middle, nums.end());
    }

    void add(uint num) {
        if (nums.size() == 0) {
            nums.push_back(num);
            return;
        }

        auto iter = std::lower_bound(nums.begin(), nums.end(), num, [](uint a, uint b) { return a < b; });
        if (iter == nums.end()) {
            nums.push_back(num);
        } else {
            nums.insert(iter, num);
        }
    }
};

void add_by_order(Node* first, uint num) {
    if (first->nums.size() == 0) {
        first->nums.push_back(num);
        return;
    }

    Node* current_node = first;
    while (true) {
        if (current_node->nums.back() >= num) {
            current_node->add(num);
            // size++;
            if (current_node->nums.size() == MAX_SIZE) {
                Node* new_node = new Node(MAX_SIZE / 2);
                new_node->next = current_node->next;
                current_node->next = new_node;
                current_node->move_halh_to(new_node);
            }
            return;
        }

        if (current_node->next == NULL) {
            break;
        }
        current_node = current_node->next;
    }

    current_node->nums.push_back(num);
    // size++;
    if (current_node->nums.size() == MAX_SIZE) {
        Node* new_node = new Node(MAX_SIZE / 2);
        new_node->next = current_node->next;
        current_node->next = new_node;
        current_node->move_halh_to(new_node);
    }
}

class IndexBuilder {
    std::string _data_file;
    std::string _db_data_path;
    std::string _db_name;
    uint _threads = 3;

    // uint _btree_file_size = 0;
    uint _btree_pos_file_size = 0;
    uint _btrees_file_size = 0;
    uint _predicate_array_pos_file_size = 0;
    uint _so_predicate_array_file_size = 0;
    uint _os_predicate_array_file_size = 0;
    uint _array_file_size = 0;

    uint _triplet_cnt = 0;
    uint _entity_cnt = 0;
    uint _predicate_cnt = 0;

    std::vector<pair<uint, uint>> _predicate_rank;

    flat_hash_map<uint, flat_hash_set<pair<uint, uint>>> _pso;

   public:
    IndexBuilder(std::string db_name, std::string data_file) {
        _db_name = db_name;
        _data_file = data_file;
        _db_data_path = "./DB_DATA_ARCHIVE/" + _db_name + "/";

        fs::path db_path = _db_data_path;
        if (!fs::exists(db_path)) {
            fs::create_directories(db_path);
        }

        fs::path entity_path = _db_data_path + "ENTITY";
        if (!fs::exists(entity_path)) {
            fs::create_directories(entity_path);
        }
    }

    bool build() {
        std::cout << "Indexing ..." << std::endl;

        load_data();

        calculate_predicate_rank();

        // predict -> (o_set, s_set)
        std::vector<pair<entity_set, entity_set>> predicate_entity_sets(_predicate_cnt);
        build_btree(predicate_entity_sets);

        uint* so_predicate_cnt = (uint*)malloc(4 * _entity_cnt);
        uint* os_predicate_cnt = (uint*)malloc(4 * _entity_cnt);
        std::memset(so_predicate_cnt, 0, 4 * _entity_cnt);
        std::memset(os_predicate_cnt, 0, 4 * _entity_cnt);

        store_btree(predicate_entity_sets, so_predicate_cnt, os_predicate_cnt);

        std::vector<std::pair<uint, uint>> predicate_array_offset(_entity_cnt);
        store_predicate_array_pos(so_predicate_cnt, os_predicate_cnt, predicate_array_offset);

        free(so_predicate_cnt);
        free(os_predicate_cnt);

        build_entity_to_entity(predicate_array_offset);

        store_db_info();

        // while(1);
        return true;
    }

    void load_data() {
        auto beg = std::chrono::high_resolution_clock::now();

        std::ifstream::sync_with_stdio(false);
        std::iostream::sync_with_stdio(false);
        std::ifstream fin(_data_file, std::ios::in);

        if (!fin.is_open()) {
            std::cerr << _data_file << " doesn't exist, cannot build the database correctly." << std::endl;
            exit(1);
        }

        std::ofstream predict_out(_db_data_path + "PREDICATE", std::ofstream::out | std::ofstream::binary);
        predict_out.tie(nullptr);

        std::ofstream entity_outs[4];
        for (int i = 0; i < 4; i++) {
            entity_outs[i] = std::ofstream(_db_data_path + "ENTITY/" + std::to_string(i),
                                           std::ofstream::out | std::ofstream::binary);
            entity_outs[i].tie(nullptr);
        }

        flat_hash_map<std::string, uint> entity2id;
        flat_hash_map<std::string, uint> predicate2id;

        std::string s, p, o;
        while (fin >> s >> p) {
            fin.ignore();
            std::getline(fin, o);
            for (o.pop_back(); o.back() == ' ' || o.back() == '.'; o.pop_back()) {
            }

            ++_triplet_cnt;
            uint pid, sid, oid;

            pid = predicate2id[p];
            if (pid == 0) {
                pid = ++_predicate_cnt;
                predicate2id[p] = pid;
                p = p + "\n";
                predict_out.write(p.c_str(), static_cast<long>(p.size()));
            }

            sid = entity2id[s];
            if (sid == 0) {
                sid = ++_entity_cnt;
                entity2id[s] = sid;
                s = s + "\n";
                entity_outs[sid % 4].write(s.c_str(), static_cast<long>(s.size()));
            }

            oid = entity2id[o];
            if (oid == 0) {
                oid = ++_entity_cnt;
                entity2id[o] = oid;
                o = o + "\n";
                entity_outs[oid % 4].write(o.c_str(), static_cast<long>(o.size()));
            }

            _pso[pid].insert(std::make_pair(sid, oid));

            if (_triplet_cnt % 100000 == 0) {
                std::cout << _triplet_cnt << " triplet(s) have been loaded.\r";
                std::cout.flush();
            }
        }
        std::cout << _triplet_cnt << " triplet(s) have been loaded.\r";
        std::cout.flush();

        std::cout << std::endl;

        fin.close();
        predict_out.close();
        for (int i = 0; i < 4; i++)
            entity_outs[i].close();

        flat_hash_map<std::string, uint>().swap(entity2id);
        malloc_trim(0);

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "load data takes " << diff.count() << " ms." << std::endl;
    }

    void calculate_predicate_rank() {
        for (uint pid = 1; pid <= _predicate_cnt; pid++) {
            uint i = 0;
            for (; i < _predicate_rank.size(); i++) {
                if (_predicate_rank[i].second <= _pso[pid].size())
                    break;
            }
            _predicate_rank.insert(_predicate_rank.begin() + i, {pid, _pso[pid].size()});
        }
        // for (uint rank = 0; rank < _predicate_rank.size(); rank++) {
        //     std::cout << rank << " " << _predicate_rank[rank].first << " " << _predicate_rank[rank].second
        //               << std::endl;
        // }
    }

    void build_btree(std::vector<pair<entity_set, entity_set>>& predicate_entity_sets) {
        auto beg = std::chrono::high_resolution_clock::now();

        if (_threads != 1)
            multi_thread_build_btree(predicate_entity_sets);
        else
            single_thread_build_btree(predicate_entity_sets);

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "build index takes " << diff.count() << " ms.                 " << std::endl;
    }

    void single_thread_build_btree(std::vector<pair<entity_set, entity_set>>& predicate_entity_sets) {
        uint ps_set_size;
        uint po_set_size;
        uint pid = 0;
        double finished = 0;
        std::string info = "building predicate to entity index: ";
        progress(finished, 0, info);
        for (uint tid = 0; tid < _predicate_cnt; tid++) {
            pid = _predicate_rank[tid].first;

            for (const auto& so : _pso[pid]) {
                predicate_entity_sets[pid - 1].first.insert(so.first);
                predicate_entity_sets[pid - 1].second.insert(so.second);
            }

            ps_set_size = predicate_entity_sets[pid - 1].first.size();
            po_set_size = predicate_entity_sets[pid - 1].second.size();
            _btrees_file_size += ps_set_size * 4 + po_set_size * 4;
            _so_predicate_array_file_size += ps_set_size * 3 * 4;
            _os_predicate_array_file_size += po_set_size * 3 * 4;

            progress(finished, pid, info);
        }
    }

    void multi_thread_build_btree(std::vector<pair<entity_set, entity_set>>& predicate_entity_sets) {
        std::vector<pair<uint, uint>> task_status(_threads);

        uint pid = 0;
        for (uint tid = 0; tid < _threads; tid++) {
            pid = _predicate_rank[tid].first;
            task_status[tid] = {pid, 0};
            std::thread t(std::bind(&IndexBuilder::sub_build_btree_task, this, pid, &task_status[tid].second,
                                    &predicate_entity_sets[pid - 1]));
            t.detach();
        }

        uint ps_set_size;
        uint po_set_size;
        uint rank = _threads;
        uint finish_cnt = 0;
        double finished = 0;
        std::string info = "building predicate to entity index: ";
        progress(finished, 0, info);
        while (finish_cnt < _predicate_cnt) {
            for (uint tid = 0; tid < _threads; tid++) {
                if (task_status[tid].first != 0 && task_status[tid].second == 1) {
                    finish_cnt++;
                    pid = task_status[tid].first;
                    progress(finished, pid, info);

                    ps_set_size = predicate_entity_sets[pid - 1].first.size();
                    po_set_size = predicate_entity_sets[pid - 1].second.size();
                    _btrees_file_size += ps_set_size * 4 + po_set_size * 4;
                    _so_predicate_array_file_size += ps_set_size * 3 * 4;
                    _os_predicate_array_file_size += po_set_size * 3 * 4;

                    if (rank < _predicate_rank.size()) {
                        pid = _predicate_rank[rank].first;
                        task_status[tid] = {pid, 0};
                        std::thread t(std::bind(&IndexBuilder::sub_build_btree_task, this, pid,
                                                &task_status[tid].second, &predicate_entity_sets[pid - 1]));
                        t.detach();
                        rank++;
                    } else {
                        task_status[tid] = {0, 0};
                    }
                }
            }

            usleep(10000);
        }
    }

    void sub_build_btree_task(uint pid, uint* finish, pair<entity_set, entity_set>* s_o_set) {
        for (const auto& so : _pso[pid]) {
            s_o_set->first.insert(so.first);
            s_o_set->second.insert(so.second);
        }

        *finish = 1;
    }

    void store_btree(std::vector<pair<entity_set, entity_set>>& predicate_entity_sets,
                     uint so_predicate_cnt[],
                     uint os_predicate_cnt[]) {
        auto beg = std::chrono::high_resolution_clock::now();

        _btree_pos_file_size = _predicate_cnt * 4 * 4;

        Virtual_Memory btree_pos_vm = Virtual_Memory(_db_data_path + "BTREE_POS", _btree_pos_file_size);
        Virtual_Memory btrees_vm = Virtual_Memory(_db_data_path + "BTREES", _btrees_file_size);

        uint btrees_offset = 0;
        entity_set* ps_set;
        entity_set* po_set;
        for (uint pid = 1; pid <= _predicate_cnt; pid++) {
            ps_set = &predicate_entity_sets[pid - 1].first;
            po_set = &predicate_entity_sets[pid - 1].second;

            btree_pos_vm[(pid - 1) * 4] = btrees_offset;
            btree_pos_vm[(pid - 1) * 4 + 1] = ps_set->size();
            for (auto it = ps_set->begin(); it != ps_set->end(); it++) {
                so_predicate_cnt[*it - 1]++;
                btrees_vm[btrees_offset] = *it;
                btrees_offset++;
            }

            btree_pos_vm[(pid - 1) * 4 + 2] = btrees_offset;
            btree_pos_vm[(pid - 1) * 4 + 3] = po_set->size();
            for (auto it = po_set->begin(); it != po_set->end(); it++) {
                os_predicate_cnt[*it - 1]++;
                btrees_vm[btrees_offset] = *it;
                btrees_offset++;
            }
        }

        btree_pos_vm.close_vm();
        btrees_vm.close_vm();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "store index takes " << diff.count() << " ms.               " << std::endl;
    }

    void store_predicate_array_pos(uint so_predicate_cnt[],
                                   uint os_predicate_cnt[],
                                   std::vector<std::pair<uint, uint>>& predicate_array_offset) {
        auto beg = std::chrono::high_resolution_clock::now();

        _predicate_array_pos_file_size = _entity_cnt * 4 * 4;

        Virtual_Memory predicate_array_pos_vm =
            Virtual_Memory(_db_data_path + "PREDICATE_ARRAY_POS", _predicate_array_pos_file_size);

        uint cnt;
        uint offset = 0;
        uint begin_offset = 0;
        for (uint id = 1; id <= _entity_cnt; id++) {
            cnt = so_predicate_cnt[id - 1];
            begin_offset = (id - 1) * 4;
            predicate_array_offset[id - 1].first = offset;
            predicate_array_pos_vm[begin_offset] = offset;
            predicate_array_pos_vm[begin_offset + 1] = cnt;
            offset += cnt * 3;
        }
        offset = 0;
        for (uint id = 1; id <= _entity_cnt; id++) {
            cnt = os_predicate_cnt[id - 1];
            begin_offset = (id - 1) * 4;
            predicate_array_offset[id - 1].second = offset;
            predicate_array_pos_vm[begin_offset + 2] = offset;
            predicate_array_pos_vm[begin_offset + 3] = cnt;
            offset += cnt * 3;
        }

        predicate_array_pos_vm.close_vm();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "store predicate array pos takes " << diff.count() << " ms.               " << std::endl;
    }

    void build_entity_to_entity(std::vector<std::pair<uint, uint>>& predicate_array_offset) {
        auto beg = std::chrono::high_resolution_clock::now();

        if (_threads != 1)
            multi_thread_build_entity_to_entity(predicate_array_offset);
        else
            single_thread_build_entity_to_entity(predicate_array_offset);

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "build index takes " << diff.count() << " ms.                      " << std::endl;
    }

    void single_thread_build_entity_to_entity(std::vector<std::pair<uint, uint>>& predicate_array_offset) {
        _array_file_size = _triplet_cnt * 2 * 4;

        Virtual_Memory so_predicate_array_vm =
            Virtual_Memory(_db_data_path + "SO_PREDICATE_ARRAY", _so_predicate_array_file_size);
        Virtual_Memory os_predicate_array_vm =
            Virtual_Memory(_db_data_path + "OS_PREDICATE_ARRAY", _os_predicate_array_file_size);
        Virtual_Memory arrays_vm = Virtual_Memory(_db_data_path + "ARRAYS", _array_file_size);

        // [(pid, finish_flag) ,... ,(pid, finish_flag)]
        std::vector<pair<uint, uint>> task_status(_threads);
        flat_hash_map<uint, Node> s_to_o;
        flat_hash_map<uint, Node> o_to_s;

        uint arrays_offset = 0;
        uint pid;
        double finished = 0;
        std::string info = "building entity to entity index: ";
        progress(finished, 0, info);
        for (uint tid = 0; tid < _predicate_cnt; tid++) {
            pid = _predicate_rank[tid].first;

            if (s_to_o.size() != 0) {
                flat_hash_map<uint, Node>().swap(s_to_o);
            }
            if (o_to_s.size() != 0) {
                flat_hash_map<uint, Node>().swap(o_to_s);
            }

            for (const auto& so : _pso[pid]) {
                add_by_order(&s_to_o[so.first], so.second);
                add_by_order(&o_to_s[so.second], so.first);
            }

            store_array(arrays_offset, pid, s_to_o, o_to_s, arrays_vm, so_predicate_array_vm,
                        os_predicate_array_vm, predicate_array_offset);

            flat_hash_set<pair<uint, uint>>{}.swap(_pso[pid]);
            malloc_trim(0);

            progress(finished, pid, info);
        }
    }

    void multi_thread_build_entity_to_entity(std::vector<std::pair<uint, uint>>& predicate_array_offset) {
        _array_file_size = _triplet_cnt * 2 * 4;

        Virtual_Memory so_predicate_array_vm =
            Virtual_Memory(_db_data_path + "SO_PREDICATE_ARRAY", _so_predicate_array_file_size);
        Virtual_Memory os_predicate_array_vm =
            Virtual_Memory(_db_data_path + "OS_PREDICATE_ARRAY", _os_predicate_array_file_size);
        Virtual_Memory arrays_vm = Virtual_Memory(_db_data_path + "ARRAYS", _array_file_size);

        // [(pid, finish_flag) ,... ,(pid, finish_flag)]
        std::vector<pair<uint, uint>> task_status(_threads);
        std::vector<flat_hash_map<uint, Node>> s_to_o(_threads);
        std::vector<flat_hash_map<uint, Node>> o_to_s(_threads);

        uint pid = 0;
        for (uint tid = 0; tid < _threads; tid++) {
            pid = _predicate_rank[tid].first;
            task_status[tid] = {pid, 0};
            std::thread t(std::bind(&IndexBuilder::sub_build_entity_to_entity, this, pid,
                                    &task_status[tid].second, &s_to_o[tid], &o_to_s[tid]));
            t.detach();
        }

        uint arrays_offset = 0;
        uint rank = _threads;
        uint finish_cnt = 0;
        double finished = 0;
        std::string info = "building entity to entity index: ";
        progress(finished, 0, info);
        while (finish_cnt < _predicate_cnt) {
            for (uint tid = 0; tid < _threads; tid++) {
                if (task_status[tid].first != 0 && task_status[tid].second == 1) {
                    finish_cnt++;
                    pid = task_status[tid].first;
                    progress(finished, pid, info);

                    store_array(arrays_offset, pid, s_to_o[tid], o_to_s[tid], arrays_vm,
                                so_predicate_array_vm, os_predicate_array_vm, predicate_array_offset);

                    if (rank < _predicate_rank.size()) {
                        pid = _predicate_rank[rank].first;
                        task_status[tid] = {pid, 0};
                        std::thread t(std::bind(&IndexBuilder::sub_build_entity_to_entity, this, pid,
                                                &task_status[tid].second, &s_to_o[tid], &o_to_s[tid]));
                        t.detach();
                        rank++;
                    } else {
                        task_status[tid] = {0, 0};
                    }
                }
            }

            usleep(5000);
        }

        so_predicate_array_vm.close_vm();
        os_predicate_array_vm.close_vm();
        arrays_vm.close_vm();
    }

    void sub_build_entity_to_entity(uint pid,
                                    uint* finish,
                                    flat_hash_map<uint, Node>* p_s_to_o,
                                    flat_hash_map<uint, Node>* p_o_to_s) {
        if (p_s_to_o->size() != 0) {
            flat_hash_map<uint, Node>().swap(*p_s_to_o);
        }
        if (p_o_to_s->size() != 0) {
            flat_hash_map<uint, Node>().swap(*p_o_to_s);
        }

        for (const auto& so : _pso[pid]) {
            add_by_order(&p_s_to_o->operator[](so.first), so.second);
            add_by_order(&p_o_to_s->operator[](so.second), so.first);
        }

        flat_hash_set<pair<uint, uint>>{}.swap(_pso[pid]);
        malloc_trim(0);

        *finish = 1;
    }

    void store_array(uint& arrays_offset,
                     uint pid,
                     flat_hash_map<uint, Node>& s_to_o,
                     flat_hash_map<uint, Node>& o_to_s,
                     Virtual_Memory& arrays_vm,
                     Virtual_Memory& so_predicate_array_vm,
                     Virtual_Memory& os_predicate_array_vm,
                     std::vector<std::pair<uint, uint>>& predicate_array_offset) {
        Node* current_node;
        uint size = 0;
        uint sid = 0;
        uint oid = 0;
        uint array_offset;

        for (auto it = s_to_o.begin(); it != s_to_o.end(); it++) {
            current_node = &it->second;
            if (current_node->nums.size()) {
                size = 0;
                sid = it->first;
                array_offset = predicate_array_offset[sid - 1].first;
                so_predicate_array_vm[array_offset] = pid;
                array_offset++;
                so_predicate_array_vm[array_offset] = arrays_offset;
                array_offset++;
                while (current_node) {
                    for (long unsigned int i = 0; i < current_node->nums.size(); i++) {
                        arrays_vm[arrays_offset] = current_node->nums[i];
                        arrays_offset++;
                        size++;
                    }
                    current_node->nums.clear();
                    current_node = current_node->next;
                }
                so_predicate_array_vm[array_offset] = size;
                array_offset++;
                predicate_array_offset[sid - 1].first = array_offset;
            }
        }

        for (auto it = o_to_s.begin(); it != o_to_s.end(); it++) {
            current_node = &it->second;
            if (current_node->nums.size()) {
                size = 0;
                oid = it->first;
                array_offset = predicate_array_offset[oid - 1].second;
                os_predicate_array_vm[array_offset] = pid;
                array_offset++;
                os_predicate_array_vm[array_offset] = arrays_offset;
                array_offset++;
                while (current_node) {
                    for (long unsigned int i = 0; i < current_node->nums.size(); i++) {
                        arrays_vm[arrays_offset] = current_node->nums[i];
                        arrays_offset++;
                        size++;
                    }
                    current_node->nums.clear();
                    current_node = current_node->next;
                }
                os_predicate_array_vm[array_offset] = size;
                array_offset++;
                predicate_array_offset[oid - 1].second = array_offset;
            }
        }
    }

    void store_db_info() {
        Virtual_Memory vm = Virtual_Memory(_db_data_path + "DB_INFO", 9 * 4);

        vm[0] = _btree_pos_file_size;
        vm[1] = _btrees_file_size;
        vm[2] = _predicate_array_pos_file_size;
        vm[3] = _so_predicate_array_file_size;
        vm[4] = _os_predicate_array_file_size;
        vm[5] = _array_file_size;
        vm[6] = _triplet_cnt;
        vm[7] = _entity_cnt;
        vm[8] = _predicate_cnt;

        // std::cout << vm[0] << " " << vm[1] << " " << vm[2] << " " << vm[3] << " " << vm[4] << " " << vm[5]
        //           << " " << vm[6] << " " << vm[7] << std::endl;

        vm.close_vm();
    }

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

        vm.close_vm();
    }

    void progress(double& finished, uint pid, std::string& info) {
        if (pid != 0) {
            for (long unsigned int i = 0; i < _predicate_rank.size(); i++) {
                if (_predicate_rank[i].first == pid) {
                    finished += _predicate_rank[i].second * 1.0 / _triplet_cnt;
                    break;
                }
            }
        }

        std::cout << info << finished * 100 << "%    \r";
        std::cout.flush();
        // std::cout << info << finished * 100 << std::endl;
    }

    // munmap(data, file_size);
};

#endif