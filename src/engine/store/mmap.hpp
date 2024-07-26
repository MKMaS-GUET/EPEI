#ifndef MMAP_HPP
#define MMAP_HPP

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string>

template <typename T>
struct MMap {
    T* map_;
    int fd_;
    std::string path_;
    uint fileSize_;  // bytes

    MMap() {}

    MMap(std::string path, uint fileSize) : path_(path), fileSize_(fileSize) {
        fd_ = open((path).c_str(), O_RDWR | O_CREAT, (mode_t)0600);
        if (fd_ == -1) {
            perror("Error opening file for mmap");
            exit(1);
        }

        if (ftruncate(fd_, fileSize) == -1) {
            perror("Error truncating file for mmap");
            close(fd_);
            exit(1);
        }

        map_ = static_cast<T*>(mmap(nullptr, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        if (map_ == MAP_FAILED) {
            perror("Error mapping file for mmap");
            close(fd_);
            exit(1);
        }
    }

    void CloseMap() {
        if (msync(map_, fileSize_, MS_SYNC) == -1) {
            perror("Error syncing memory to disk");
        }

        if (munmap(map_, fileSize_) == -1) {
            perror("Error unmapping memory");
        }

        if (close(fd_) == -1) {
            perror("Error closing file descriptor");
        }
    }

    void Resize(uint new_size) {
        fileSize_ = new_size;
        if (ftruncate(fd_, fileSize_) == -1) {
            perror("Error truncating file for mmap");
            close(fd_);
            exit(1);
        }
    }

    T& operator[](uint offset) {
        if (offset >= 0 && offset < fileSize_ / sizeof(T)) {
            return map_[offset];
        }
        static T error = 0;

        return error;
    }
};

#endif