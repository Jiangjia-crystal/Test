// Temporarily stores splitTables that are not fully inserted into the accelerated index.
#ifndef STORAGE_LEVELDB_DB_IMMUTABLE_SMALL_TABLE_LISTH_
#define STORAGE_LEVELDB_DB_IMMUTABLE_SMALL_TABLE_LISTH_

#include <list>
#include "port/port_stdcxx.h"
#include "db/dbformat.h"
#include "util/radix_tree.hpp"

namespace leveldb{ 

class STable;

class ImmutableSmallTableList{
 public:
  explicit ImmutableSmallTableList(const InternalKeyComparator& comparator);

  ImmutableSmallTableList(const ImmutableSmallTableList&) = delete;
  ImmutableSmallTableList& operator=(const ImmutableSmallTableList&) = delete;

  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  bool Get(const LookupKey& key, std::string* value, Status* s);

  // Create a new table from the incoming Node and push it into the head of the list.
  void Push(void*); 

  // Pop from the end of the list.
  void Pop();

  STable* Front();

  STable* Back();

  bool Empty();

  size_t Num();

  size_t ApproximateMemoryUsage();

  STable* CreateSTable(void*);

 private:
  // friend class NvmSkipList<char* , BTable::KeyComparator>;
  // the same as the BTable, MemTable.
  int refs_;
  port::Mutex imm_list_mutex_;
  InternalKeyComparator comparator_;
  std::list<STable*> SmallTableList_;
  size_t nvm_usage_;
  // radix_tree<std::string, STable*> tree_;
};

}

#endif