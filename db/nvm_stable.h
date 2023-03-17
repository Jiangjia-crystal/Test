#ifndef STORAGE_LEVELDB_DB_STABLE_H_
#define STORAGE_LEVELDB_DB_STABLE_H_

#include "db/dbformat.h"
#include "leveldb/db.h"
#include "util/nvm_allocator.h"
#include "db/nvm_skiplist.h"
#include "port/port_stdcxx.h"

namespace leveldb{

class ImmutableSmallTableList;
class InternalKeyComparator;
class STableIterator;

class STable {
 public:
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
    int NewCompare(const char* a, const char* b, bool hasseq, SequenceNumber snum) const;
    bool NewCompare(const char* a, const char* b) const;
  };
  
  typedef NvmSkipList<const char*, KeyComparator> Table;
  
  explicit STable(const InternalKeyComparator& comparator);

  STable(const STable&) = delete;
  STable& operator=(const STable&) = delete;

  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }
  
  Iterator* NewIterator();

  const char* getMemKey(Iterator* iter) const;

  Table::Node* GetHead(); 

  Slice GetSmallestInternalKey();

  Slice GetLargestInternalKey();

  const char* GetSmallestMemKey();

  const char* GetLargestMemKey();

  size_t ApproximateMemoryUsage();

  // Slice GetLargestMemTableKey();

  bool Get(const LookupKey& key, std::string* value, Status* s);

  void Add(const char* key, size_t encoded_length);

  const char* getKey(Iterator* iter);

  size_t getEncodedLength(Iterator* iter) const;

  void Traversal(); // For test 3/7
  // Table::Node* GetSTableHead();
  void MergeNode(STable*, const char*, SequenceNumber);

  void MergeNode(STable*, SequenceNumber);

  ~STable();  


 private:
  friend class STableIterator;
  friend class MemTableBackwardIterator;
  friend class ImmutableSmallTableList;
  friend class AccIndex;

  KeyComparator comparator_;
  int refs_;
  NvmSimpleAllocator nsa_;
  Table table_;
  // port::Mutex stable_mutex_;  

  void ChangeHead(Slice key);

  void CutTail(Slice key);

};

}

#endif