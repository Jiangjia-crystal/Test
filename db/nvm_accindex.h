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

  // Add a stable into acc_index.
  void Add(Slice& key, STable* value);

  // Delete a table that anchor key is key.
  // void Delete(Slice key);

  // Merge a table into acc_index.
  void Merge(STable* splitTable);

  // Merge a table with another node.
  bool MergeNode(AccIndexIterator* indexIt, STable* splitTable);

  // acc_index is empty.
  bool Empty();

  // check if poptable is not empty.
  bool HasPop();

  // Get the popTable.
  STable* GetPopTable();

  // Set popTable null.
  void SetPopTableNull();

  // Choose a table in acc_index and set null.
  void RandomRemove();

  // Return the num of tables in acc_index
  int Num();

  STable* CreateSTable(void* head);

  STable* CreateSTable();

  void TraversalAnchorKey();

  void TraversalAll();

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