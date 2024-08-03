#ifndef DICTIONARY_HPP
#define DICTIONARY_HPP

#include <parallel_hashmap/phmap.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <list>
#include "./mmap.hpp"

template <typename Key, typename Value>
using hash_map = phmap::flat_hash_map<Key, Value>;

enum Pos { kSubject, kPredicate, kObject, kShared };
enum Map { kSubjectMap, kPredicateMap, kObjectMap, kSharedMap };

class Dictionary {
    std::string dict_path_;
    std::string file_path_;
    uint triplet_cnt_ = 0;

    uint subject_cnt_;
    uint predicate_cnt_;
    uint object_cnt_;
    uint shared_cnt_;

    bool build_ = false;
    hash_map<std::string, uint> subjects_;
    hash_map<std::string, uint> predicates_;
    hash_map<std::string, uint> objects_;
    hash_map<std::string, uint> shared_;

    // phmap::flat_hash_set<std::string> subject_set_;
    // phmap::flat_hash_set<std::string> object_set_;

    std::array<hash_map<std::string, uint>, 6> subject2id_;
    hash_map<std::string, uint> predicate2id_;
    std::array<hash_map<std::string, uint>, 6> object2id_;
    std::array<hash_map<std::string, uint>, 6> shared2id_;

    std::vector<std::string> id2subject_;
    std::vector<std::string> id2predicate_;
    std::vector<std::string> id2object_;
    std::vector<std::string> id2shared_;

    void InitSerialize() {
        std::filesystem::path subjects_path = dict_path_ + "/subjects";
        std::filesystem::path objects_path = dict_path_ + "/objects";
        std::filesystem::path shared_path = dict_path_ + "/shared";

        if (!std::filesystem::exists(subjects_path))
            std::filesystem::create_directories(subjects_path);
        if (!std::filesystem::exists(objects_path))
            std::filesystem::create_directories(objects_path);
        if (!std::filesystem::exists(shared_path))
            std::filesystem::create_directories(shared_path);
    }

    void InitLoad() {
        // for (uint i = 0; i < 6; i++) {
        //     subject2id_[i] = hash_map<std::string, uint>();
        //     object2id_[i] = hash_map<std::string, uint>();
        //     shared2id_[i] = hash_map<std::string, uint>();
        // }

        id2subject_ = std::vector<std::string>();
        id2predicate_ = std::vector<std::string>();
        id2object_ = std::vector<std::string>();
        id2shared_ = std::vector<std::string>();

        std::string cnt;
        std::ifstream db_info(dict_path_ + "/dict_info", std::ofstream::out | std::ofstream::binary);

        std::getline(db_info, cnt);
        subject_cnt_ = std::stoi(cnt);
        id2subject_ = std::vector<std::string>(subject_cnt_ + 1);

        std::getline(db_info, cnt);
        predicate_cnt_ = std::stoi(cnt);
        id2predicate_ = std::vector<std::string>(predicate_cnt_ + 1);

        std::getline(db_info, cnt);
        object_cnt_ = std::stoi(cnt);
        id2object_ = std::vector<std::string>(object_cnt_ + 1);

        std::getline(db_info, cnt);
        shared_cnt_ = std::stoi(cnt);
        id2shared_ = std::vector<std::string>(shared_cnt_ + 1);

        std::getline(db_info, cnt);
        triplet_cnt_ = std::stoi(cnt);

        db_info.close();
    }

    void EncodeRDF(hash_map<uint, std::vector<std::pair<uint, uint>>>& pso) {
        uint temp_entity_id = 1;
        uint sid = 0, oid = 0;
        hash_map<std::string, uint>::iterator s_it, o_it, shared_it;
        std::string s, p, o;
        std::ifstream fin(file_path_, std::ios::in);
        while (fin >> s >> p) {
            fin.ignore();
            std::getline(fin, o);
            for (o.pop_back(); o.back() == ' ' || o.back() == '.'; o.pop_back()) {
            }

            shared_it = shared_.find(s);
            if (shared_it == shared_.end()) {
                o_it = objects_.find(s);
                if (o_it != objects_.end()) {
                    // std::cout << "find s in o" << std::endl;
                    sid = o_it->second;
                    shared_.insert({s, sid});
                    objects_.erase(s);
                } else {
                    // std::cout << "insert s" << std::endl;
                    auto ret = subjects_.insert({s, temp_entity_id});
                    if (ret.second) {
                        sid = temp_entity_id;
                        temp_entity_id++;
                    } else {
                        // std::cout << "insert s fail" << std::endl;
                        sid = ret.first->second;
                    }
                }
            } else {
                // std::cout << "find s in shared" << std::endl;
                sid = shared_it->second;
            }

            shared_it = shared_.find(o);
            if (shared_it == shared_.end()) {
                s_it = subjects_.find(o);
                if (s_it != subjects_.end()) {
                    // std::cout << "find o in s" << std::endl;
                    oid = s_it->second;
                    shared_.insert({o, oid});
                    subjects_.erase(o);
                } else {
                    // std::cout << "insert o" << std::endl;
                    auto ret = objects_.insert({o, temp_entity_id});

                    if (ret.second) {
                        oid = temp_entity_id;
                        temp_entity_id++;
                    } else {
                        // std::cout << "insert s fail" << std::endl;
                        oid = ret.first->second;
                    }
                }
            } else {
                // std::cout << "find o in shared" << std::endl;
                oid = shared_it->second;
            }

            auto ret = predicates_.insert({p, predicates_.size() + 1});

            // std::cout << sid << " " << oid << std::endl;

            pso[ret.first->second].push_back({sid, oid});

            ++triplet_cnt_;

            if (triplet_cnt_ % 100000 == 0) {
                std::cout << triplet_cnt_ << '\r' << std::flush;
            }
        }
        std::cout << std::endl;

        subject_cnt_ = subjects_.size();
        object_cnt_ = objects_.size();
        shared_cnt_ = shared_.size();
        predicate_cnt_ = pso.size();

        fin.close();

        build_ = true;
    }

    void ReassignID(hash_map<uint, std::vector<std::pair<uint, uint>>>& pso) {
        std::ofstream predicate_out =
            std::ofstream(dict_path_ + "/predicates", std::ofstream::out | std::ofstream::binary);
        std::ofstream subject_outs[6];
        std::ofstream object_outs[6];
        std::ofstream shared_outs[6];
        for (int i = 0; i < 6; i++) {
            subject_outs[i] = std::ofstream(dict_path_ + "/subjects/" + std::to_string(i),
                                            std::ofstream::out | std::ofstream::binary);
            object_outs[i] = std::ofstream(dict_path_ + "/objects/" + std::to_string(i),
                                           std::ofstream::out | std::ofstream::binary);
            shared_outs[i] = std::ofstream(dict_path_ + "/shared/" + std::to_string(i),
                                           std::ofstream::out | std::ofstream::binary);
            subject_outs[i].tie(nullptr);
            object_outs[i].tie(nullptr);
            shared_outs[i].tie(nullptr);
        }

        hash_map<uint, uint> subject_reasign_id;
        hash_map<uint, uint> object_reasign_id;
        hash_map<uint, uint> shared_reasign_id;

        uint subject_id = 1;
        uint object_id = 1;
        uint shared_id = 1;
        for (auto it = subjects_.begin(); it != subjects_.end(); it++) {
            subject_reasign_id[it->second] = subject_id;
            it->second = subject_id;
            subject_outs[subject_id % 6].write((it->first + "\n").c_str(),
                                               static_cast<long>(it->first.size() + 1));
            subject_id++;
        }
        for (auto it = objects_.begin(); it != objects_.end(); it++) {
            object_reasign_id[it->second] = object_id;
            it->second = object_id;
            object_outs[object_id % 6].write((it->first + "\n").c_str(),
                                             static_cast<long>(it->first.size() + 1));
            object_id++;
        }
        for (auto it = shared_.begin(); it != shared_.end(); it++) {
            shared_reasign_id[it->second] = shared_id;
            it->second = shared_id;
            shared_outs[shared_id % 6].write((it->first + "\n").c_str(),
                                             static_cast<long>(it->first.size() + 1));
            shared_id++;
        }

        std::vector<const std::string*> predicates(predicate_cnt_ + 1);
        for (auto& p_pair : predicates_) {
            predicates[p_pair.second] = &p_pair.first;
        }
        for (uint pid = 1; pid <= predicate_cnt_; pid++) {
            predicate_out.write((*predicates[pid] + "\n").c_str(),
                                static_cast<long>(predicates[pid]->size() + 1));
        }

        for (uint pid = 1; pid <= predicate_cnt_; pid++) {
            for (auto it = pso[pid].begin(); it != pso[pid].end(); it++) {
                auto s_it = shared_reasign_id.find(it->first);
                if (s_it != shared_reasign_id.end()) {
                    it->first = shared_reasign_id[it->first];
                } else {
                    it->first = shared_cnt_ + subject_reasign_id[it->first];
                }
                s_it = shared_reasign_id.find(it->second);
                if (s_it != shared_reasign_id.end()) {
                    it->second = shared_reasign_id[it->second];
                } else {
                    it->second = shared_cnt_ + subject_cnt_ + object_reasign_id[it->second];
                }
                // std::cout << it->first << " " << it->second << std::endl;
            }
        }

        for (int i = 0; i < 6; i++) {
            subject_outs[i].close();
            object_outs[i].close();
            shared_outs[i].close();
        }
        predicate_out.close();
    }

    void SaveDictInfo() {
        std::ofstream dict_info(dict_path_ + "/dict_info", std::ofstream::out | std::ofstream::binary);

        std::string cnt = std::to_string(subjects_.size()) + "\n";
        dict_info.write(cnt.c_str(), cnt.size());
        cnt = std::to_string(predicates_.size()) + "\n";
        dict_info.write(cnt.c_str(), cnt.size());
        cnt = std::to_string(objects_.size()) + "\n";
        dict_info.write(cnt.c_str(), cnt.size());
        cnt = std::to_string(shared_.size()) + "\n";
        dict_info.write(cnt.c_str(), cnt.size());
        cnt = std::to_string(triplet_cnt_) + "\n";
        dict_info.write(cnt.c_str(), cnt.size());

        dict_info.close();
    }

    uint Find(Map map, const std::string& str) {
        hash_map<std::string, uint>::iterator it;
        if (map == Map::kPredicateMap) {
            it = predicate2id_.find(str);
            if (it != predicate2id_.end())
                return it->second;
            else
                return 0;
        }

        for (uint part = 0; part < 6; part++) {
            if (map == Map::kSubjectMap) {
                it = subject2id_[part].find(str);
                if (it != subject2id_[part].end())
                    return it->second;
            }
            if (map == Map::kObjectMap) {
                it = object2id_[part].find(str);
                if (it != object2id_[part].end())
                    return it->second;
            }
            if (map == Map::kSharedMap) {
                it = shared2id_[part].find(str);
                if (it != shared2id_[part].end())
                    return it->second;
            }
        }
        return 0;
    }

    int FindInMaps(hash_map<std::string, uint>& map, const std::string& str) {
        if (shared_.size() > map.size()) {
            auto it = shared_.find(str);
            if (it != shared_.end()) {
                return it->second;
            }
            it = map.find(str);
            if (it != map.end()) {
                return it->second;
            }
            return 0;
        } else {
            auto it = map.find(str);
            if (it != map.end()) {
                return it->second;
            }
            it = map.find(str);
            if (it != map.end()) {
                return it->second;
            }
            return 0;
        }
    }

    uint String2IDAfterBuild(const std::string& str, Pos pos) {
        switch (pos) {
            case Pos::kSubject:  // subject
                return shared_cnt_ + FindInMaps(subjects_, str);
            case Pos::kPredicate: {
                // predicate
                auto it = predicates_.find(str);
                if (it != predicates_.end()) {
                    return it->second;
                }
                return 0;
            }
            case Pos::kObject:  // object
                return shared_cnt_ + subject_cnt_ + FindInMaps(objects_, str);
            default:
                break;
        }
        return 0;
    }

    uint FindInMaps(uint cnt, Map map, const std::string& str) {
        uint ret;
        if (shared_cnt_ > cnt) {
            ret = Find(kSharedMap, str);
            if (ret)
                return ret;
            ret = Find(map, str);
            if (ret)
                return ret;
        } else {
            ret = Find(map, str);
            if (ret)
                return ret;
            ret = Find(kSharedMap, str);
            if (ret)
                return ret;
        }
        return 0;
    }

    uint String2IDAfterLoad(const std::string& str, Pos pos) {
        switch (pos) {
            case kSubject:  // subject
                return shared_cnt_ + FindInMaps(subject_cnt_, kSubjectMap, str);
            case kPredicate: {  // predicate
                return Find(kPredicateMap, str);
            }
            case kObject:  // object
                return shared_cnt_ + subject_cnt_ + FindInMaps(object_cnt_, kObjectMap, str);
            default:
                break;
        }
        return 0;
    }

    bool SubLoadDict(Pos pos, int part) {
        hash_map<std::string, uint>* map = nullptr;
        std::vector<std::string>* vec = nullptr;
        std::string path = dict_path_;

        if (pos == Pos::kSubject) {
            map = &subject2id_[part];
            vec = &id2subject_;
            path += "/subjects/";
        }
        if (pos == Pos::kObject) {
            map = &object2id_[part];
            vec = &id2object_;
            path += "/objects/";
        }
        if (pos == Pos::kShared) {
            map = &shared2id_[part];
            vec = &id2shared_;
            path += "/shared/";
        }
        path += std::to_string(part);

        std::ifstream file_in(path, std::ofstream::out | std::ofstream::binary);

        std::string entity;
        uint id = part;
        if (part == 0)
            id = 6;
        while (std::getline(file_in, entity)) {
            map->insert({entity, id});
            vec->at(id) = entity;
            id += 6;
        }
        file_in.close();

        return true;
    };

    bool LoadPredicate() {
        std::ifstream predicate_in(dict_path_ + "/predicates", std::ofstream::out | std::ofstream::binary);
        std::string predicate;
        uint id = 1;
        while (std::getline(predicate_in, predicate)) {
            predicate2id_[predicate] = id;
            id2predicate_[id] = predicate;
            id++;
        }
        predicate_in.close();
        return true;
    }

   public:
    Dictionary() {}

    Dictionary(std::string& dict_path_) : dict_path_(dict_path_) { InitLoad(); }

    Dictionary(std::string& dict_path_, std::string& file_path_)
        : dict_path_(dict_path_), file_path_(file_path_) {}

    ~Dictionary() {
        hash_map<std::string, uint>().swap(subjects_);
        hash_map<std::string, uint>().swap(objects_);
        hash_map<std::string, uint>().swap(predicates_);
        hash_map<std::string, uint>().swap(shared_);

        std::vector<std::string>().swap(id2subject_);
        std::vector<std::string>().swap(id2predicate_);
        std::vector<std::string>().swap(id2object_);
        std::vector<std::string>().swap(id2shared_);

        hash_map<std::string, uint>().swap(predicate2id_);
        for (uint i = 0; i < 6; i++) {
            hash_map<std::string, uint>().swap(subject2id_[i]);
            hash_map<std::string, uint>().swap(object2id_[i]);
            hash_map<std::string, uint>().swap(shared2id_[i]);
        }
    }

    hash_map<uint, std::vector<std::pair<uint, uint>>>* EncodeRDF() {
        if (file_path_.empty()) {
            return nullptr;
        }

        hash_map<uint, std::vector<std::pair<uint, uint>>>* pso =
            new hash_map<uint, std::vector<std::pair<uint, uint>>>();

        InitSerialize();

        EncodeRDF(*pso);

        ReassignID(*pso);

        SaveDictInfo();
        build_ = true;

        return pso;
    }

    void Load() {
        LoadPredicate();

        std::vector<std::future<bool>> sub_task_list;

        for (int t = 0; t < 6; t++) {
            sub_task_list.emplace_back(
                std::async(std::launch::async, &Dictionary::SubLoadDict, this, Pos::kSubject, t));
        }
        for (std::future<bool>& task : sub_task_list) {
            task.get();
        }
        sub_task_list.clear();
        for (int t = 0; t < 6; t++) {
            sub_task_list.emplace_back(
                std::async(std::launch::async, &Dictionary::SubLoadDict, this, Pos::kObject, t));
        }
        for (std::future<bool>& task : sub_task_list) {
            task.get();
        }
        sub_task_list.clear();
        for (int t = 0; t < 6; t++) {
            sub_task_list.emplace_back(
                std::async(std::launch::async, &Dictionary::SubLoadDict, this, Pos::kShared, t));
        }
        for (std::future<bool>& task : sub_task_list) {
            task.get();
        }

        // std::cout << subjects.size() << predicates.size() << " " << objects.size() << " " << shared.size()
        //           << std::endl;
    }

    std::string& ID2String(uint id, Pos pos) {
        if (pos == kPredicate) {
            return id2predicate_[id];
        }

        if (id <= shared_cnt_) {
            return id2shared_[id];
        }

        switch (pos) {
            case kSubject:
                return id2subject_[id - shared_cnt_];
            case kObject:
                return id2object_[id - shared_cnt_ - subject_cnt_];
            default:
                break;
        }
        throw std::runtime_error("Unhandled case in ID2String");
    }

    uint String2ID(const std::string& str, Pos pos) {
        if (build_)
            return String2IDAfterBuild(str, pos);
        else
            return String2IDAfterLoad(str, pos);
        return 0;
    }

    void RDF2BINFile(std::string map_path) {
        MMap<uint> map = MMap<uint>(map_path, triplet_cnt_ * 3 * 4);
        uint offset = 0;

        uint cnt = 0;
        std::string s, p, o;
        std::ifstream fin(file_path_, std::ios::in);
        while (fin >> s >> p) {
            fin.ignore();
            std::getline(fin, o);
            for (o.pop_back(); o.back() == ' ' || o.back() == '.'; o.pop_back()) {
            }
            map[offset] = String2IDAfterBuild(s, Pos::kSubject);
            offset++;
            map[offset] = String2IDAfterBuild(p, Pos::kPredicate);
            offset++;
            map[offset] = String2IDAfterBuild(o, Pos::kObject);
            offset++;

            ++cnt;
            if (cnt % 100000 == 0) {
                std::cout << cnt << '\r' << std::flush;
            }
        }

        map.CloseMap();
        fin.close();
    }

    uint subject_cnt() { return subject_cnt_; }

    uint predicate_cnt() { return predicate_cnt_; }

    uint object_cnt() { return object_cnt_; }

    uint shared_cnt() { return shared_cnt_; }

    uint triplet_cnt() { return triplet_cnt_; }

    uint max_id() {
        return shared_cnt_ + subject_cnt_ + object_cnt_;
    };
};

#endif

// void Build() {
//     if (file_path_.empty()) {
//         return;
//     }

//     InitSerialize();

//     std::ofstream predicate_out =
//         std::ofstream(dict_path_ + "/predicates", std::ofstream::out | std::ofstream::binary);
//     std::ofstream subject_outs[6];
//     std::ofstream object_outs[6];
//     std::ofstream shared_outs[6];
//     for (int i = 0; i < 6; i++) {
//         subject_outs[i] = std::ofstream(dict_path_ + "/subjects/" + std::to_string(i),
//                                         std::ofstream::out | std::ofstream::binary);
//         object_outs[i] = std::ofstream(dict_path_ + "/objects/" + std::to_string(i),
//                                        std::ofstream::out | std::ofstream::binary);
//         shared_outs[i] = std::ofstream(dict_path_ + "/shared/" + std::to_string(i),
//                                        std::ofstream::out | std::ofstream::binary);
//         subject_outs[i].tie(nullptr);
//         object_outs[i].tie(nullptr);
//         shared_outs[i].tie(nullptr);
//     }

//     std::pair<hash_map<std::string, uint>::iterator, bool> ret;

//     std::string s, p, o;
//     std::ifstream fin(file_path_, std::ios::in);
//     while (fin >> s >> p) {
//         fin.ignore();
//         std::getline(fin, o);
//         for (o.pop_back(); o.back() == ' ' || o.back() == '.'; o.pop_back()) {
//         }

//         subject_set_.insert(s);
//         object_set_.insert(o);
//         ret = predicates_.insert({p, predicates_.size() + 1});
//         if (ret.second) {
//             predicate_out.write((p + "\n").c_str(), static_cast<long>(p.size() + 1));
//         }

//         ++triplet_cnt_;

//         if (triplet_cnt_ % 100000 == 0) {
//             std::cout << triplet_cnt_ << '\r' << std::flush;
//         }
//     }

//     fin.close();

//     uint id;
//     for (auto it = subject_set_.begin(); it != subject_set_.end(); it++) {
//         if (object_set_.contains(*it)) {
//             id = shared_.size() + 1;
//             ret = shared_.insert({*it, id});
//             if (ret.second) {
//                 shared_outs[id % 6].write((*it + "\n").c_str(), static_cast<long>(it->size() + 1));
//             }
//         } else {
//             id = subjects_.size() + 1;
//             ret = subjects_.insert({*it, id});
//             if (ret.second) {
//                 subject_outs[id % 6].write((*it + "\n").c_str(), static_cast<long>(it->size() + 1));
//             }
//         }
//     }

//     for (auto it = object_set_.begin(); it != object_set_.end(); it++) {
//         if (!subject_set_.contains(*it)) {
//             id = objects_.size() + 1;
//             ret = objects_.insert({*it, id});
//             if (ret.second) {
//                 object_outs[id % 6].write((*it + "\n").c_str(), static_cast<long>(it->size() + 1));
//             }
//         }
//     }

//     for (int i = 0; i < 6; i++) {
//         subject_outs[i].close();
//         object_outs[i].close();
//         shared_outs[i].close();
//     }
//     predicate_out.close();

//     phmap::flat_hash_set<std::string>().swap(subject_set_);
//     phmap::flat_hash_set<std::string>().swap(object_set_);

//     SaveDictInfo();
//     build_ = false;

//     // hash_map<std::string, uint>().swap(subjects);
//     // hash_map<std::string, uint>().swap(objects);
//     // hash_map<std::string, uint>().swap(predicates);
// }