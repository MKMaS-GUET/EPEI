#ifndef VIRTUAL_MEMORY_HPP
#define VIRTUAL_MEMORY_HPP

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string>

class Virtual_Memory {
    uint* _vm;
    int _fd;
    std::string _path;
    uint _file_size;

    void new_virtual_mem() {
        _fd = open((_path).c_str(), O_RDWR | O_CREAT, (mode_t)0600);
        if (_fd == -1) {
            perror("Error opening file for mmap");
            exit(1);
        }

        if (ftruncate(_fd, _file_size) == -1) {
            perror("Error truncating file for mmap");
            close(_fd);
            exit(1);
        }

        _vm = static_cast<uint*>(mmap(nullptr, _file_size, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0));
        if (_vm == MAP_FAILED) {
            perror("Error mapping file for mmap");
            close(_fd);
            exit(1);
        }
    }

   public:
    Virtual_Memory() {}

    Virtual_Memory(std::string path, uint file_size) : _path(path), _file_size(file_size) {
        new_virtual_mem();
    }

    void close_vm() {
        if (msync(_vm, _file_size, MS_SYNC) == -1) {
            std::cout << _file_size << std::endl;
            perror("Error syncing memory to disk");
        }

        if (munmap(_vm, _file_size) == -1) {
            std::cout << _file_size << std::endl;
            perror("Error unmapping memory");
        }

        if (close(_fd) == -1) {
            std::cout << _file_size << std::endl;
            perror("Error closing file descriptor");
        }
    }

    uint& operator[](uint index) {
        if (index >= 0 && index < _file_size / 4) {
            return _vm[index];
        }
        static uint error = 0;

        return error;
    }
};

#endif