#ifndef STORAGE_LEVELDB_DB_BTABLE_H_
#define STORAGE_LEVELDB_DB_BTABLE_H_

#include "db/dbformat.h"
#include "leveldb/db.h"
#include "util/nvm_allocator.h"
#include "db/nvm_skiplist.h"

namespace leveldb{

class ImmutableSmallTableList;
class InternalKeyComparator;
class BTableIterator;

class BTable {
 public:
  explicit BTable(const InternalKeyComparator& comparator);

  BTable(const BTable&) = delete;
  BTable& operator=(const BTable&) = delete;

  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  size_t ApproximateMemoryUsage();
  
  Iterator* NewIterator();

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  typedef NvmSkipList<const char*, KeyComparator> Table;

  Table::Node* GetHead();  

  Table::Node* Add(const char* key, size_t encoded_length, Table::Node* start);
  
  bool Get(const LookupKey& key, std::string* value, Status* s);

  bool NeedToSplit();

  void Split(ImmutableSmallTableList* imm_list);

  void Lock();

  void Unlock();

  ~BTable();  

 private:
  friend class BTableIterator;
  friend class MemTableBackwardIterator;
  friend class ImmutableSmallTableList;

  KeyComparator comparator_;
  int refs_;
  port::Mutex btable_mutex_;
  NvmSimpleAllocator nsa_;
  Table table_;  
};

}

#endif