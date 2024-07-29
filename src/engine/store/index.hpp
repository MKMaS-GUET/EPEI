#ifndef INDEX_HPP
#define INDEX_HPP

#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include "./linked_array.hpp"

struct PredicateIndex {
    phmap::btree_set<uint> s_set_;
    phmap::btree_set<uint> o_set_;

    void Build(std::vector<std::pair<uint, uint>>& so_pairs) {
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
    phmap::flat_hash_map<uint, LinkedArray<uint>> s_to_o_;
    phmap::flat_hash_map<uint, LinkedArray<uint>> o_to_s_;

    void Build(std::vector<std::pair<uint, uint>>& so_pairs) {
        for (const auto& so : so_pairs) {
            s_to_o_[so.first].AddByOrder(so.second);
            o_to_s_[so.second].AddByOrder(so.first);
        }
    }

    void Clear() {
        phmap::flat_hash_map<uint, LinkedArray<uint>>().swap(s_to_o_);
        phmap::flat_hash_map<uint, LinkedArray<uint>>().swap(o_to_s_);
    }
};

#endif