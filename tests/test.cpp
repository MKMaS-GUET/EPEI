#include <gtest/gtest.h>

#include <iostream>
#include <iterator>
#include <chrono>
#include <set>

#include <parallel_hashmap/btree.h>

#include "hsinDB/engine.hpp"
/*
TEST(CreateTest, CreateLUBM) {
    auto engine = hsinDB::Engine::Create("lubm", "/mnt/d/projects/cpp/index-structure/data/lubm/lubm.nt");
    engine->close();
}

TEST(QueryTest, QueryLUBM) {
    auto engine = hsinDB::Engine::Open("lubm");
    auto session = engine->newSession();
    auto result = session->execute_f("/mnt/d/projects/cpp/index-structure/data/lubm/lubm_q0.sql", 8, 1);

    std::cout << "\n===========================================" << std::endl;
    std::copy(result->var_begin(), result->var_end(), std::ostream_iterator<std::string>(std::cout, "\t"));
    std::cout << std::endl;
    for (auto &item: *result) {
        std::copy(item.begin(), item.end(), std::ostream_iterator<std::string>(std::cout, " "));
        std::cout << std::endl;
    }
    std::cout << "-------------------------------------------" << std::endl;
    std::cout << std::dec << result->result_size() << " result(s)." << std::endl;
    engine->close();
}

void benchmark(std::string &&name, const std::function<void(void)> &fun) {
    auto start_time = std::chrono::high_resolution_clock::now();
    fun();
    auto stop_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> used_time = stop_time - start_time;
    std::cout << name << " used " << used_time.count() << " ms." << std::endl;
}

TEST(BenchmarkTest, CompareContainers) {
    size_t n = 10000000;
    std::vector<size_t> a;
    benchmark("vector add", [&]() {
        for (size_t i = 0; i < n; ++ i) {
            a.push_back(i);
        }
    });
    benchmark("vector search", [&]() {
        for (size_t i = 0; i < n; ++ i) {
            auto _ = std::lower_bound(a.begin(), a.end(), i);
        }
    });
    benchmark("vector iteration", [&]() {
        for (auto &item: a) {
            item;
        }
    });

    std::set<size_t> b;
    benchmark("btree add", [&]() {
        for (size_t i = 0; i < n; ++ i) {
            b.insert(i);
        }
    });
    benchmark("btree search", [&]() {
        for (size_t i = 0; i < n; ++ i) {
            b.lower_bound(i);
        }
    });
    benchmark("btree iteration", [&]() {
        for (auto &item: b) {
            item;
        }
    });

    phmap::btree_set<size_t> c;
    benchmark("btree_set add", [&]() {
        for (size_t i = 0; i < n; ++ i) {
            c.insert(i);
        }
    });
    benchmark("btree_set search", [&]() {
        for (size_t i = 0; i < n; ++ i) {
            c.lower_bound(i);
        }
    });
    benchmark("btree_set iteration", [&]() {
        for (auto &item: c) {
            item;
        }
    });
}
*/
int main(int argc, char** argv) {
    printf("Running main() from %s\n", __FILE__);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
