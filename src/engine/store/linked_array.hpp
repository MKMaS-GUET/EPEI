#ifndef LINKED_ARRAY_HPP
#define LINKED_ARRAY_HPP

#include <vector>

#define MAX_SIZE 20000

template <typename T>
class Node {
   public:
    std::vector<T> elements_;
    Node<T>* next_ = nullptr;

    Node() : elements_() {}

    Node(int reverse_size) { elements_.reserve(reverse_size); }
};

template <typename T>
class LinkedArray : public Node<T> {
   public:
    void Add(Node<T>* target_node, T e) {
        if (target_node->elements_.size() == 0) {
            target_node->elements_.push_back(e);
            return;
        }

        auto iter = std::lower_bound(target_node->elements_.begin(), target_node->elements_.end(), e,
                                     [](T a, T b) { return a < b; });
        if (iter == target_node->elements_.end()) {
            target_node->elements_.push_back(e);
        } else {
            if (*iter != e)
                target_node->elements_.insert(iter, e);
        }
    }

    void MoveHalf(Node<T>* source_node, Node<T>* target_node) {
        auto middle = source_node->elements_.begin() + source_node->elements_.size() / 2;

        std::move(std::make_move_iterator(middle), std::make_move_iterator(source_node->elements_.end()),
                  std::back_inserter(target_node->elements_));

        source_node->elements_.erase(middle, source_node->elements_.end());
    }

    void AddByOrder(T e) {
        if (this->elements_.size() == 0) {
            this->elements_.push_back(e);
            return;
        }

        Node<T>* current_node = this;
        while (true) {
            if (current_node->elements_.back() >= e) {
                Add(current_node, e);
                // size++;
                if (current_node->elements_.size() == MAX_SIZE) {
                    Node<T>* new_node = new Node<T>(MAX_SIZE / 2);
                    new_node->next_ = current_node->next_;
                    current_node->next_ = new_node;
                    MoveHalf(current_node, new_node);
                }
                return;
            }

            if (current_node->next_ == NULL) {
                break;
            }
            current_node = current_node->next_;
        }

        current_node->elements_.push_back(e);
        // size++;
        if (current_node->elements_.size() == MAX_SIZE) {
            Node<T>* new_node = new Node<T>(MAX_SIZE / 2);
            new_node->next_ = current_node->next_;
            current_node->next_ = new_node;
            MoveHalf(current_node, new_node);
        }
    }
};

#endif