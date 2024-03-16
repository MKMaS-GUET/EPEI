/*
 * @FileName   : leapfrog_join.hpp
 * @CreateAt   : 2023/6/18
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef LEAPFROG_JOIN_HPP
#define LEAPFROG_JOIN_HPP

#include <unistd.h>
#include <algorithm>
#include <vector>
// #include "query_plan.hpp"

#include "result_vector_list.hpp"

void leapfrog_join(Result_Vector_List& pair_begin_end, std::vector<uint>& result_set) {
    uint value;

    // Check if any index is empty => Intersection empty

    if (pair_begin_end.has_empty())
        return;

    pair_begin_end.update_current_postion();
    // 创建指向每一个列表的指针，初始指向列表的第一个值

    //  max 是所有指针指向位置的最大值，初始的最大值就是对列表排序后，最后一个列表的第一个值
    size_t max = pair_begin_end.get_current_val_of_range(pair_begin_end.size() - 1);

    // 当前迭代器的 id
    size_t idx = 0;

    // double tree_seek_time = 0;
    // double vector_seek_time = 0;

    // double get_current_val_time = 0;

    while (true) {
        // if (pair_begin_end.Result_Vectors[2]->size() == 1) {
        //     std::cout << "idx: " << idx << " max: " << max << std::endl;
        //     sleep(1);
        // }
        // 当前迭代器的第一个值
        value = pair_begin_end.get_current_val_of_range(idx);

        // get_current_val_time += diff.count();
        // An intersecting value has been found!
        // 在没有找到交集中的值时，
        // 当前迭代器指向的值 (max) 都要 > 此迭代器之前的迭代器指向的值，
        // 第一个迭代器指向的值 > 最后一个迭代器指向的值，
        // 所以 max 一定大于下一个迭代器指向的值。
        // 若在迭代器 i 中的新 max 等于上一个 max 的情况，之后遍历了一遍迭代器列表再次回到迭代器 i，
        // 但是 max 依旧没有变化，此时才会出现当前迭代器的 value 与 max 相同。
        // 因为此时已经遍历了所有迭代器，都找到了相同的值，所以就找到了交集中的值
        if (value == max) {
            result_set.push_back(value);
            pair_begin_end.next_val(idx);
            // We shall find a value greater or equal than the current max
        } else {
            // 将当前迭代器指向的位置变为第一个大于 max 的值的位置
            pair_begin_end.seek(idx, max);
        }

        if (pair_begin_end.at_end(idx)) {
            break;
        }

        // Store the maximum

        max = pair_begin_end.get_current_val_of_range(idx);

        idx++;
        idx = idx % pair_begin_end.size();
    }
}

std::shared_ptr<std::vector<uint>> leapfrog_join(Result_Vector_List& indexes) {
    std::shared_ptr<std::vector<uint>> resultSet = std::make_shared<std::vector<uint>>();

    // std::cout << "size: " << indexes.size() << std::endl;ge

    leapfrog_join(indexes, *resultSet);

    return resultSet;
}

#endif  // COMBINED_CODE_INDEX_LEAPFROG_JOIN_HPP
