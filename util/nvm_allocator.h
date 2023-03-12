#ifndef STORAGE_LEVELDB_NVM_ALLOCATOR_H_
#define STORAGE_LEVELDB_NVM_ALLOCATOR_H_

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <map>
#include <numa.h>
#include "leveldb/options.h"
#include "mutex"

namespace leveldb{

class  NvmSimpleAllocator
{
private:
    std::atomic<size_t> memory_usage_;
    std::map<char*,size_t> nvm_blocks_;
    std::mutex mutex_;

public:
    NvmSimpleAllocator();

    NvmSimpleAllocator(const NvmSimpleAllocator&) = delete;
    NvmSimpleAllocator& operator=(const NvmSimpleAllocator&) = delete;

    char* AllocateNode(size_t bytes);
    void FreeNode(char* ptr);
    size_t MemoryUsage() const {
        return memory_usage_.load(std::memory_order_relaxed);
    }

    ~ NvmSimpleAllocator();
};

}

#endif