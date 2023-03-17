#include "db/nvm_accindex.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "db/nvm_stable.h"
#include "util/mutexlock.h"

namespace leveldb {

// Return the internal key (Slice).
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

AccIndex::AccIndex(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &nsa_), pop_table_(nullptr) {} 

AccIndex::~AccIndex() { assert(refs_ == 0); }

int AccIndex::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);

  Slice auserKey = ExtractUserKey(a);
  Slice buserKey = ExtractUserKey(b);

  std::string astring = auserKey.ToString();
  std::string bstring = buserKey.ToString();
  
  return astring.compare(bstring);
  
  // return comparator.Compare(a, b);
}

// Return memkey (const char*) of a internal key (Slice).
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class AccIndexIterator {
 public:
  explicit AccIndexIterator(AccIndex::Table* table) : iter_(table) {}

  AccIndexIterator(const AccIndexIterator&) = delete;
  AccIndexIterator& operator=(const AccIndexIterator&) = delete;

  bool Valid() { return iter_.Valid(); }
  bool NextValid() { return iter_.NextValid(); }
  void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekInsertNode(const Slice& k) { return iter_.SeekInsertNode(EncodeKey(&tmp_, k));}
  void SeekToFirst() { iter_.SeekToFirst(); }
  void SeekToLast() { iter_.SeekToLast(); }
  void Next() { iter_.Next(); }
  void Prev() { iter_.Prev(); }
  void Lock() { iter_.Lock(); }
  void UnLock() { iter_.UnLock(); }
  Slice key() const{ return GetLengthPrefixedSlice(iter_.key()); }
  Slice nextKey() const { return GetLengthPrefixedSlice(iter_.nextKey()); }
  STable* value() {
    return (STable*)iter_.value();
  }
  const char* memKey() { return iter_.key(); }
  const char* nextMemKey() { return iter_.nextKey(); }
  AccIndex::Table::Node* GetNode() {
    return iter_.GetNode();
  }

  Status status() const { return Status::OK(); }

  AccIndex::Table::Iterator iter_;

 private:
  std::string tmp_;  // For passing to EncodeKey
};

AccIndexIterator* AccIndex::NewIterator() { return new AccIndexIterator(&table_); }

// Insert a new node into the AccIndex
void AccIndex::Add(Slice& key, STable* value) {
  // Convert Slice key to the type of internalkey (const char*)
  // MutexLock l(&acc_mutex_);
  std::string internalKey;
  EncodeKey(&internalKey, key);
  size_t encodedLen = internalKey.size();
  // Allocate a block of nvm memory to the key.
  char* key_memory = nsa_.AllocateNode(encodedLen);
  memcpy(key_memory, internalKey.data(), encodedLen);
  table_.Insert(key_memory, value);
  // std::printf("Add a table in accindex:%x\n", value);
}

size_t AccIndex::ApproximateMemoryUsage() {
  size_t tmp = 0;
  Table::Iterator iter(&table_);
  iter.SeekToFirst();
  while(iter.Valid()) {
    STable* table = (STable*)iter.value();
    tmp += table->ApproximateMemoryUsage();
    iter.Next();
  }
  return tmp;
}

// Merge a table into the AccIndex.
void AccIndex::Merge(STable* splitTable, SequenceNumber seq) {
  Slice smallestInsertKey = splitTable->GetSmallestInternalKey();
  const char* largestMemKey = splitTable->GetLargestMemKey();

  MutexLock l(&acc_mutex_); // Now only global lock are implemented 

  if (table_.Num() == 0) {
    if (pop_table_ == nullptr) {    
      pop_table_ = CreateSTable(splitTable->GetHead());
    } 
    else {
      STable* insertTable = CreateSTable(splitTable->GetHead());
      Add(smallestInsertKey, insertTable);
    }
    return;
  }

  AccIndexIterator* indexIt = NewIterator();
  indexIt->SeekInsertNode(smallestInsertKey);

  if (!indexIt->Valid()) {
    STable* insertTable = CreateSTable();
    Add(smallestInsertKey, insertTable);
  }

  // bool continueMerge = true;
  for (indexIt->SeekInsertNode(smallestInsertKey); indexIt->Valid(); indexIt->Next()) {
    if (comparator_(largestMemKey, indexIt->memKey()) < 0) {
      break;
    }
    STable* src = indexIt->value();
    if (indexIt->NextValid()) {
      const char* nextSmallestKey = indexIt->nextMemKey();
      MergeNode(src, splitTable, nextSmallestKey, seq);
    } else {
      MergeNode(src, splitTable, seq);
    }
    // MergeNode(indexIt, splitTable);
  }
}  

void AccIndex::MergeNode(STable* srcTable, STable* splitTable, const char* nextMemKey, SequenceNumber seq) {
  srcTable->MergeNode(splitTable, nextMemKey, seq);
}

void AccIndex::MergeNode(STable* srcTable, STable* splitTable, SequenceNumber seq) {
  srcTable->MergeNode(splitTable, seq);
}
/*
bool AccIndex::MergeNode(AccIndexIterator* indexIt, STable* splitTable) {

}
*/

bool AccIndex::MergeNode(AccIndexIterator* indexIt, STable* splitTable) {
  // S1: Decide whether to continue to merge the next node. 
  bool flag = false;
  // S2: Get the table that merged
  STable* insertTable = indexIt->value();
  assert(insertTable != nullptr);

  Iterator* splitTableIt = splitTable->NewIterator();
  // S3: Get the anchor key.
  Slice key = indexIt->key();
  Slice insertKey; // the insert key

  if (indexIt->NextValid()) {
    Slice stopKey = indexIt->nextKey();
    for (splitTableIt->Seek(key);  splitTableIt->Valid(); splitTableIt->Next()) {
      insertKey = splitTableIt->key();
      if (insertTable->comparator_.comparator.Compare(insertKey, stopKey) > 0) {
        flag = true;
        break;        
      }

      const char* key = splitTable->getKey(splitTableIt);
      size_t encoded_len = splitTable->getEncodedLength(splitTableIt);
      insertTable->Add(key, encoded_len);
    }
  }
  else {
    for (splitTableIt->Seek(key); splitTableIt->Valid(); splitTableIt->Next()) { // 3/9
      const char* key = splitTable->getKey(splitTableIt);
      size_t encoded_len = splitTable->getEncodedLength(splitTableIt);
      // std::printf("insert_num is:%lld\n", insert_num_);
      insertTable->Add(key, encoded_len);
    }    
  }
  return flag;
}

STable* AccIndex::CreateSTable(void* head) {
  // STable* insert_table = new STable(STable::KeyComparator::comparator);
  STable* insert_table = new STable(comparator_.comparator);
  insert_table->Ref();
  insert_table->table_.head_ = (STable::Table::Node*)head;
  insert_table->table_.max_height_.store(insert_table->table_.kMaxHeight, std::memory_order_relaxed); // maybe kmaxheight is not tree, influence the read performance
  return insert_table;
}

STable* AccIndex::CreateSTable() {
  // STable* newTable = new STable(STable::KeyComparator::comparator);
  STable* newTable = new STable(comparator_.comparator);
  newTable->Ref();
  return newTable;
}

bool AccIndex::Empty() {
  return table_.Num() == 0;
}

bool AccIndex::HasPop() {
  // MutexLock l(&acc_mutex_);
  return pop_table_ != nullptr;
}

STable* AccIndex::GetPopTable() {
  // MutexLock l(&acc_mutex_);
  return pop_table_;
}

void AccIndex::SetPopTableNull() {
  // MutexLock l(&acc_mutex_);
  pop_table_ = nullptr;
}

void AccIndex::RandomRemove() {
  assert(pop_table_ == nullptr);
  MutexLock l(&acc_mutex_);
  pop_table_ = (STable*)(table_.RandomRemove());  
  // pop_table_->Traversal();
}

int AccIndex::Num() {
  // MutexLock l(&acc_mutex_);
  return table_.Num();
}

void AccIndex::TraversalAnchorKey() {
  std::printf("AccIndex traversal begin:\n");
  Table::Iterator iter(&table_);
  iter.SeekToFirst();
  while(iter.Valid()) {
    std::printf("anchor key:%s\n", ExtractUserKey(GetLengthPrefixedSlice(iter.key())).ToString().data());
    iter.Next();
  }
}

void AccIndex::TraversalAll() {
  std::printf("AccIndex traversal all begin:\n");
  Table::Iterator iter(&table_);
  iter.SeekToFirst();
  while(iter.Valid()) {
    STable* tmp = (STable*)iter.value();
    tmp->Traversal();
    iter.Next();
  }
}

bool AccIndex::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.SeekInsertNode(memkey.data());

  if (iter.Valid()) {
    STable* target = (STable*)iter.value();
    return target->Get(key, value, s);
  }
  else return false;
}

}