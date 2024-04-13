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

#include "virtual_memory.hpp"

#define MAX_SIZE 20000

namespace fs = std::filesystem;
using phmap::btree_map;
using phmap::flat_hash_map;
using phmap::flat_hash_set;
using std::pair;

class Node {
    void add(uint num) {
        if (nums.size() == 0) {
            nums.push_back(num);
            return;
        }

        auto iter = std::lower_bound(nums.begin(), nums.end(), num, [](uint a, uint b) { return a < b; });
        if (iter == nums.end()) {
            if (*iter != num)
                nums.push_back(num);
        } else {
            nums.insert(iter, num);
        }
    }

    void move_halh_to(Node* target_node) {
        auto middle = nums.begin() + nums.size() / 2;

        std::move(std::make_move_iterator(middle), std::make_move_iterator(nums.end()),
                  std::back_inserter(target_node->nums));

        nums.erase(middle, nums.end());
    }

   public:
    std::vector<uint> nums;
    Node* next = 0;

    Node() { nums = std::vector<uint>(); }

    Node(int reverse_size) {
        nums = std::vector<uint>();
        nums.reserve(reverse_size);
    }

    void add_by_order(uint num) {
        if (nums.size() == 0) {
            nums.push_back(num);
            return;
        }

        Node* current_node = this;
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
};

struct PredicateIndex {
    phmap::btree_set<uint> s_set;
    phmap::btree_set<uint> o_set;

    void build(std::vector<pair<uint, uint>>& so_pairs) {
        for (const auto& so : so_pairs) {
            s_set.insert(so.first);
            o_set.insert(so.second);
        }
    }

    void clear() {
        phmap::btree_set<uint>().swap(s_set);
        phmap::btree_set<uint>().swap(o_set);
    }
};

// index for a predicate
struct EntityIndex {
    flat_hash_map<uint, Node> s_to_o;
    flat_hash_map<uint, Node> o_to_s;

    void build(std::vector<pair<uint, uint>>& so_pairs) {
        for (const auto& so : so_pairs) {
            s_to_o[so.first].add_by_order(so.second);
            o_to_s[so.second].add_by_order(so.first);
        }
    }

    void clear() {
        flat_hash_map<uint, Node>().swap(s_to_o);
        flat_hash_map<uint, Node>().swap(o_to_s);
    }
};

class IndexBuilder {
    std::string _data_file;
    std::string _db_data_path;
    std::string _db_name;
    uint _threads = 2;

    VirtualMemory _predicate_index;
    VirtualMemory _predicate_index_arrays;
    VirtualMemory _entity_index;
    VirtualMemory _po_predicate_map;
    VirtualMemory _ps_predicate_map;
    VirtualMemory _entity_index_arrays;

    uint _predicate_index_file_size = 0;
    uint _predicate_index_arrays_file_size = 0;
    uint _entity_index_file_size = 0;
    uint _po_predicate_map_file_size = 0;
    uint _ps_predicate_map_file_size = 0;
    uint _entity_index_arrays_file_size = 0;

    uint _triplet_cnt = 0;
    uint _entity_cnt = 0;
    uint _predicate_cnt = 0;

    std::mutex mtx;

    std::vector<pair<uint, uint>> _predicate_rank;

    flat_hash_map<uint, std::vector<pair<uint, uint>>> _pso;

    // e_id -> (s_to_p, o_to_p)
    std::vector<std::pair<uint, uint>> _predicate_map_file_offset;

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
        std::vector<PredicateIndex> predicate_indexes(_predicate_cnt);
        // e -> (s_to_p, o_to_p)
        // _e_to_p = std::vector<std::pair<std::vector<uint>, std::vector<uint>>>(_entity_cnt);
        // first: PS_PREDICATE_MAP file offset  second: PO_PREDICATE_MAP file offset
        _predicate_map_file_offset = std::vector<std::pair<uint, uint>>(_entity_cnt);

        uint* ps_predicate_map_size = (uint*)malloc(4 * _entity_cnt);
        uint* po_predicate_map_size = (uint*)malloc(4 * _entity_cnt);
        std::memset(ps_predicate_map_size, 0, 4 * _entity_cnt);
        std::memset(po_predicate_map_size, 0, 4 * _entity_cnt);

        build_predicate_index(predicate_indexes);

        store_predicate_index(predicate_indexes, ps_predicate_map_size, po_predicate_map_size);

        store_entity_index(ps_predicate_map_size, po_predicate_map_size);

        free(ps_predicate_map_size);
        free(po_predicate_map_size);

        build_predicate_maps();

        store_db_info();

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

            _pso[pid].push_back(std::make_pair(sid, oid));

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
        std::thread t([&]() { malloc_trim(0); });
        t.detach();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "load data takes " << diff.count() << " ms." << std::endl;
    }

    void calculate_predicate_rank() {
        for (uint pid = 1; pid <= _predicate_cnt; pid++) {
            _pso[pid].shrink_to_fit();
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

    void build_predicate_index(std::vector<PredicateIndex>& predicate_indexes) {
        auto beg = std::chrono::high_resolution_clock::now();

        uint pid = 0;
        double finished = 0;
        std::string info = "building predicate index: ";

        if (_threads == 1) {
            progress(finished, 0, info);
            for (uint tid = 0; tid < _predicate_cnt; tid++) {
                pid = _predicate_rank[tid].first;
                predicate_indexes[pid - 1].build(_pso[pid]);

                update_file_size(predicate_indexes[pid - 1].s_set.size(),
                                 predicate_indexes[pid - 1].o_set.size());

                progress(finished, pid, info);
            }
        } else {
            std::queue<uint> task_queue;
            for (uint i = 0; i < _predicate_cnt; i++) {
                task_queue.push(_predicate_rank[i].first);
            }
            std::vector<std::thread> threads;
            for (uint tid = 0; tid < _threads; tid++) {
                threads.emplace_back(std::bind(&IndexBuilder::sub_build_predicate_index, this, &task_queue,
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

    void update_file_size(uint ps_set_size, uint po_set_size) {
        _predicate_index_arrays_file_size += ps_set_size * 4 + po_set_size * 4;
        _ps_predicate_map_file_size += po_set_size * 3 * 4;
        _po_predicate_map_file_size += ps_set_size * 3 * 4;
    }

    void sub_build_predicate_index(std::queue<uint>* task_queue,
                                   std::vector<PredicateIndex>* predicate_indexes,
                                   double* finished,
                                   std::string* info) {
        uint pid;
        while (task_queue->size()) {
            mtx.lock();
            pid = task_queue->front();
            task_queue->pop();
            mtx.unlock();

            predicate_indexes->at(pid - 1).build(_pso[pid]);

            mtx.lock();
            update_file_size(predicate_indexes->at(pid - 1).s_set.size(),
                             predicate_indexes->at(pid - 1).o_set.size());
            progress(*finished, pid, *info);
            mtx.unlock();
        }
    }

    void store_predicate_index(std::vector<PredicateIndex>& predicate_indexes,
                               uint ps_predicate_map_size[],
                               uint po_predicate_map_size[]) {
        auto beg = std::chrono::high_resolution_clock::now();

        _predicate_index_file_size = _predicate_cnt * 4 * 4;

        _predicate_index = VirtualMemory(_db_data_path + "PREDICATE_INDEX", _predicate_index_file_size);
        _predicate_index_arrays =
            VirtualMemory(_db_data_path + "PREDICATE_INDEX_ARRAYS", _predicate_index_arrays_file_size);

        uint predicate_index_arrays_file_offset = 0;
        phmap::btree_set<uint>* ps_set;
        phmap::btree_set<uint>* po_set;
        for (uint pid = 1; pid <= _predicate_cnt; pid++) {
            ps_set = &predicate_indexes[pid - 1].s_set;
            po_set = &predicate_indexes[pid - 1].o_set;

            _predicate_index[(pid - 1) * 4] = predicate_index_arrays_file_offset;
            for (auto it = ps_set->begin(); it != ps_set->end(); it++) {
                po_predicate_map_size[*it - 1]++;
                _predicate_index_arrays[predicate_index_arrays_file_offset] = *it;
                predicate_index_arrays_file_offset++;
            }

            _predicate_index[(pid - 1) * 4 + 2] = predicate_index_arrays_file_offset;
            for (auto it = po_set->begin(); it != po_set->end(); it++) {
                ps_predicate_map_size[*it - 1]++;
                _predicate_index_arrays[predicate_index_arrays_file_offset] = *it;
                predicate_index_arrays_file_offset++;
            }

            predicate_indexes[pid - 1].clear();
        }

        _predicate_index_arrays.close_vm();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "store predicate index takes " << diff.count() << " ms.               " << std::endl;
    }

    void store_entity_index(uint ps_predicate_map_size[], uint po_predicate_map_size[]) {
        auto beg = std::chrono::high_resolution_clock::now();

        _entity_index_file_size = _entity_cnt * 4 * 4;

        _entity_index = VirtualMemory(_db_data_path + "ENTITY_INDEX", _entity_index_file_size);

        // 两个循环合并为一个循环
        uint cnt;
        uint offset = 0;
        uint begin_offset = 0;
        for (uint id = 1; id <= _entity_cnt; id++) {
            cnt = po_predicate_map_size[id - 1];
            begin_offset = (id - 1) * 4;
            _predicate_map_file_offset[id - 1].first = offset;
            // PS_PREDICATE_MAP offset and size
            _entity_index[begin_offset] = offset;
            _entity_index[begin_offset + 1] = cnt;
            offset += cnt * 3;
        }
        offset = 0;
        for (uint id = 1; id <= _entity_cnt; id++) {
            cnt = ps_predicate_map_size[id - 1];
            begin_offset = (id - 1) * 4;
            _predicate_map_file_offset[id - 1].second = offset;
            _entity_index[begin_offset + 2] = offset;
            _entity_index[begin_offset + 3] = cnt;
            offset += cnt * 3;
        }

        _entity_index.close_vm();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "store entity index takes " << diff.count() << " ms.               " << std::endl;
    }

    void build_predicate_maps() {
        auto beg = std::chrono::high_resolution_clock::now();

        _entity_index_arrays_file_size = _triplet_cnt * 2 * 4;

        _po_predicate_map = VirtualMemory(_db_data_path + "PO_PREDICATE_MAP", _po_predicate_map_file_size);
        _ps_predicate_map = VirtualMemory(_db_data_path + "PS_PREDICATE_MAP", _ps_predicate_map_file_size);
        _entity_index_arrays =
            VirtualMemory(_db_data_path + "ENTITY_INDEX_ARRAYS", _entity_index_arrays_file_size);

        double finished = 0;
        uint pid = 0;
        std::string info = "building and storing predicate maps: ";
        uint arrays_offset = 0;
        if (_threads == 1) {
            EntityIndex entity_index;

            progress(finished, 0, info);
            for (uint tid = 0; tid < _predicate_cnt; tid++) {
                pid = _predicate_rank[tid].first;

                entity_index.clear();
                entity_index.build(_pso[pid]);

                store_predicate_maps(arrays_offset, pid, _po_predicate_map, entity_index.s_to_o, true);
                store_predicate_maps(arrays_offset, pid, _ps_predicate_map, entity_index.o_to_s, false);

                std::vector<pair<uint, uint>>{}.swap(_pso[pid]);
                malloc_trim(0);

                progress(finished, pid, info);
            }
        } else {
            // [(pid, finish_flag) ,... ,(pid, finish_flag)]
            std::queue<uint> task_queue;
            for (uint i = 0; i < _predicate_cnt; i++) {
                task_queue.push(_predicate_rank[i].first);
            }
            std::vector<std::thread> threads;
            for (uint tid = 0; tid < _threads; tid++) {
                threads.emplace_back(std::bind(&IndexBuilder::sub_build_predicate_maps, this, &task_queue,
                                               &arrays_offset, &finished, &info));
            }
            for (auto& t : threads) {
                t.join();
            }
        }

        _po_predicate_map.close_vm();
        _ps_predicate_map.close_vm();
        _entity_index_arrays.close_vm();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = end - beg;
        std::cout << "build and store predicate maps takes " << diff.count() << " ms.                      "
                  << std::endl;
    }

    void sub_build_predicate_maps(std::queue<uint>* task_queue,
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
            entity_index.build(_pso[pid]);
            // store_predicate_maps(*arrays_offset, pid, entity_index);
            store_predicate_maps(*arrays_offset, pid, _po_predicate_map, entity_index.s_to_o, true);
            store_predicate_maps(*arrays_offset, pid, _ps_predicate_map, entity_index.o_to_s, false);

            mtx.lock();
            progress(*finished, pid, *info);
            mtx.unlock();

            std::vector<pair<uint, uint>>{}.swap(_pso[pid]);
            malloc_trim(0);
        }
    }

    void store_predicate_maps(uint& arrays_offset,
                              uint pid,
                              VirtualMemory& vm,
                              flat_hash_map<uint, Node>& e_to_e,
                              bool s_to_o) {
        Node* current_node;
        uint size = 0;
        uint eid = 0;
        uint map_offset;
        uint e_cnt = 0;
        uint arrays_size_sum = 0;
        FloatInt fi;
        uint arrays_start_offset;

        for (auto it = e_to_e.begin(); it != e_to_e.end(); it++) {
            current_node = &it->second;
            if (current_node->nums.size()) {
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
                    size += current_node->nums.size();
                    current_node = current_node->next;
                }
                arrays_offset += size;
                mtx.unlock();

                arrays_size_sum += size;

                vm[map_offset] = pid;
                map_offset++;
                vm[map_offset] = arrays_start_offset;
                map_offset++;
                vm[map_offset] = size;

                current_node = &it->second;
                while (current_node) {
                    for (long unsigned int i = 0; i < current_node->nums.size(); i++) {
                        _entity_index_arrays[arrays_start_offset] = current_node->nums[i];
                        arrays_start_offset++;
                    }
                    current_node->nums.clear();
                    current_node = current_node->next;
                }
            }
            e_cnt += 1;
        }
        fi.f = arrays_size_sum * 1.0 / e_cnt;
        if (s_to_o)
            _predicate_index[(pid - 1) * 4 + 1] = fi.i;
        else
            _predicate_index[(pid - 1) * 4 + 3] = fi.i;
    }

    void store_db_info() {
        VirtualMemory vm = VirtualMemory(_db_data_path + "DB_INFO", 9 * 4);

        vm[0] = _predicate_index_file_size;
        vm[1] = _predicate_index_arrays_file_size;
        vm[2] = _entity_index_file_size;
        vm[3] = _po_predicate_map_file_size;
        vm[4] = _ps_predicate_map_file_size;
        vm[5] = _entity_index_arrays_file_size;
        vm[6] = _triplet_cnt;
        vm[7] = _entity_cnt;
        vm[8] = _predicate_cnt;

        // std::cout << vm[0] << " " << vm[1] << " " << vm[2] << " " << vm[3] << " " << vm[4] << " " << vm[5]
        //           << " " << vm[6] << " " << vm[7] << std::endl;

        vm.close_vm();
    }

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
    }
};

#endif