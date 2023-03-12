#ifndef STORAGE_LEVELDB_DB_NVM_ACCINDEX_H_
#define STORAGE_LEVELDB_DB_NVM_ACCINDEX_H_

#include "db/dbformat.h"
#include "leveldb/db.h"
#include "util/nvm_allocator.h"
#include "db/nvm_skiplist_index.h"

namespace leveldb{

class STable;
class InternalKeyComparator;
class AccIndexIterator;

class  AccIndex{
 public:
  explicit AccIndex(const InternalKeyComparator& comparator);

  AccIndex(const AccIndex&) = delete;
  AccIndex& operator=(const AccIndex&) = delete;

  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }
  
  AccIndexIterator* NewIterator();

  bool Get(const LookupKey& key, std::string* value, Status* s);

  void Add(Slice key, STable* value);

  void Delete(Slice key);

  void Merge(STable*);

  bool MergeNode(AccIndexIterator* indexIt, STable* splitTable);

  bool Empty();

  bool HasPop();

  STable* GetPopTable();

  void SetPopTableNull();

  void RandomRemove();

  int Num();

  STable* CreateSTable(void*);

  STable* CreateSTable();

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  typedef NVMSkipListIndex<const char*, KeyComparator> Table;
  
  ~AccIndex();  

 private:
  friend class AccIndexIterator;

  KeyComparator comparator_;
  int refs_;
  NvmSimpleAllocator nsa_;
  Table table_; 
  STable* pop_table_;   
  port::Mutex acc_mutex_;
};

}

#endif