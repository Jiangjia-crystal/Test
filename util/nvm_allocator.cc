#include "util/nvm_allocator.h"

namespace leveldb{

NvmSimpleAllocator::NvmSimpleAllocator()
    : memory_usage_(0) {}

NvmSimpleAllocator::~NvmSimpleAllocator() {
  //for (size_t i = 0; i < nvm_blocks_.size(); i++) {
  //  numa_free(nvm_blocks_[i][0], nvm_blocks_[i][1]);
  //}
  std::map<char*,size_t>::iterator iter;
  for(iter = nvm_blocks_.begin(); iter != nvm_blocks_.end(); iter++) {
    numa_free(iter->first, iter->second);
  }
}

char* NvmSimpleAllocator::AllocateNode(size_t bytes) {
  char* result = (char*)numa_alloc_onnode(bytes, Options::nvm_node);
  // nvm_blocks_[result] = bytes;
  // nvm_blocks_.insert(std::pair<char *, size_t>(result,bytes));
  /*
  mutex_.lock();
  memory_usage_.fetch_add(bytes + sizeof(char*),
                          std::memory_order_seq_cst);
  mutex_.unlock(); 
  */                 
  return result;   
}

void NvmSimpleAllocator::FreeNode(char* ptr) {
  numa_free(ptr, nvm_blocks_[ptr]); 
  memory_usage_.fetch_sub(nvm_blocks_[ptr] + sizeof(char*),
                          std::memory_order_relaxed);
}

}