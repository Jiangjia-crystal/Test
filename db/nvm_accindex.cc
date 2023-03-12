#include "db/nvm_accindex.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "db/nvm_stable.h"
#include "util/mutexlock.h"

namespace leveldb {

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
  return comparator.Compare(a, b);
}

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
void AccIndex::Add(Slice key, STable* value) {
  // Convert Slice key to the type of internalkey (const char*)
  // MutexLock l(&acc_mutex_);
  std::string internalKey;
  EncodeKey(&internalKey, key);
  size_t encodedLen = internalKey.size();
  // Allocate a block of nvm memory to the key.
  char* key_memory = nsa_.AllocateNode(encodedLen);
  memcpy(key_memory, internalKey.data(), encodedLen);
  table_.Insert(key_memory, value);
  std::printf("Add a table in accindex:%x\n", value);
}

// Delete a node that has been popped.
void AccIndex::Delete(Slice key) {
  MutexLock l(&acc_mutex_);
  std::string tmp;
  table_.Delete(EncodeKey(&tmp, key));                      
}

// Merge a table into the AccIndex.
void AccIndex::Merge(STable* splitTable) {
  // S1:Lock the acc_index all the merge process(latest version).
  acc_mutex_.Lock();  // 3/6
  // MutexLock l(&acc_mutex_);
  Slice smallestInsertKey = splitTable->GetSmallestInternalKey();
  /* For test3.7
  if (table_.Num() == 0) {
    Add(smallestInsertKey, splitTable);
    std::printf("directly add splitTable %x to acc_index\n", splitTable);
    acc_mutex_.Unlock();  
    return;
  }
  */
  // S2:Check if the acc_index is empry.
  if (table_.Num() == 0) {
    if (pop_table_ == nullptr) {    
      pop_table_ = CreateSTable(splitTable->GetHead());
      std::printf("directly push table %x's head %x in acc_index\n", pop_table_, pop_table_->GetHead());
      std::printf("input table:\n");
      // pop_table_->Traversal();  // test 3/7
    } 
    else {
      STable* insertTable = CreateSTable(splitTable->GetHead());
      Add(smallestInsertKey, insertTable);
      std::printf("directly add table %x's head %x to acc_index\n", insertTable, insertTable->GetHead());
      // insertTable->Traversal();
    }
    acc_mutex_.Unlock();  // 3/6
    return;
  }
  // S3:If the acc_index is not empty, find the merge start location(table).
  AccIndexIterator* indexIt = NewIterator();
  indexIt->SeekInsertNode(smallestInsertKey);

  // S4:If the splitTable smallestkey is less than all the key in the acc_index, create a new node.
  if (!indexIt->Valid()) {
    STable* insertTable = CreateSTable();
    Add(smallestInsertKey, insertTable);
    std::printf("add table %x's head %x to acc_index\n", insertTable, insertTable->GetHead());
  }

  // S5:Start Merge into acc_index.
  bool continueMerge = true;
  std::printf("start merge:\n");
  std::printf("table %x's head is %x\n", splitTable, splitTable->GetHead());
  for (indexIt->SeekInsertNode(smallestInsertKey); indexIt->Valid() && continueMerge; indexIt->Next()) {
    continueMerge = MergeNode(indexIt, splitTable);
  }
  acc_mutex_.Unlock();
}  

  /*
  indexIt->iter_.SeekInsertNode(smallestInternalKey);
  bool continueMerge = true;
  if (indexIt->Valid()) {
    while (indexIt->Valid() && continueMerge) {
      // indexIt->iter_.Lock();
      STable* dst = indexIt->value();
      if (dst == nullptr) {
        Iterator* tmpIt = splitTable->NewIterator();
        tmpIt->Seek(indexIt->key());
        Slice insertKey = tmpIt->key();
        Delete(indexIt->key());
        dst = CreateSTable();
        Add(insertKey, insertNewTable);
      }
      indexIt->Next();
      if (indexIt->Valid()) { // is not the tail in the accindex.
        STable* nextSrc = indexIt->value();
        Slice nextSmallestInternalKey = nextSrc->GetSmallestInternalKey();
        MergeTable(splitTable, dst,  nextSmallestInternalKey);
      }
      else { // is the tail in the accindex.
        MergeTable(splitTable, dst);
      }
    }
  }
  else {
    STable* insertTable = CreateSTable();
    STable* nextTable = indexIt->SeekToFirst()->value();
    Add(smallestInternalKey, insertTable);
    indexIt->SeekToFirst();
    // indexIt->iter_.Lock();
    MergeTable(insertTable, splitTable, nextTable->GetSmallestInternalKey());
    while (indexIt->Valid() && continueMerge) {
      indexIt->iter_.Lock();
      STable* dst = indexIt->value();
      indexIt->Next();
      if (indexIt->Valid()) { // is not the tail in the accindex.
        STable* nextSrc = indexIt->value();
        Slice nextSmallestInternalKey = nextSrc->GetSmallestInternalKey();
        continueMerge = MergeTable(splitTable, dst, nextSmallestInternalKey);
      }
      else { // is the tail in the accindex.
        continueMerge = MergeTable(splitTable, dst);
      }
    }  
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
      /* Deleted by 3/6
      Slice internalKey = splitTableIt->key();
      if (internalKey.size() <= 8 || internalKey.data() == "") {
        std::printf("wrong\n");
      }
      */
      insertKey = splitTableIt->key();
      if (comparator_.comparator.Compare(insertKey, stopKey) > 0) {
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
      /*
      Slice internalKey = splitTableIt->key();
      if (strcmp(ExtractUserKey(internalKey).ToString().data(), "gNabDXpLenQlyl0j") == 0) {
        std::printf("insert gNabDXpLenQlyl0j key table:\n");
        insertTable->Traversal();
      }
      */
    }    
  }
  return flag;
}

STable* AccIndex::CreateSTable(void* head) {
  STable* insert_table = new STable(comparator_.comparator);
  insert_table->Ref();
  insert_table->table_.head_ = (STable::Table::Node*)head;
  insert_table->table_.max_height_.store(insert_table->table_.kMaxHeight, std::memory_order_relaxed); // maybe kmaxheight is not tree, influence the read performance
  return insert_table;
}

STable* AccIndex::CreateSTable() {
  STable* newTable = new STable(comparator_.comparator);
  newTable->Ref();
  return newTable;
}

bool AccIndex::Empty() {
  // MutexLock l(&acc_mutex_);
  return (table_.Num() <= 0 && pop_table_ == nullptr);
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
  MutexLock l(&acc_mutex_);
  // For test
  // 3/8
  // std::printf("start:\n");
  if (pop_table_ == nullptr) {
    // std::printf("poptable is null\n");
    pop_table_ = (STable*)(table_.RandomRemove());  // 3/6
  }
  // std::printf("the pop_table_ is %x\n", pop_table_);
  // return pop_table_;
}

int AccIndex::Num() {
  // MutexLock l(&acc_mutex_);
  return table_.Num();
}

bool AccIndex::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.SeekInsertNode(memkey.data());
  if (!iter.Valid() && pop_table_ == nullptr) {  // 3.7
    return false;
  }
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    STable* target = (STable*)iter.value();
    if (target == nullptr) {
      return false;
    }
    else {
      bool flag = target->Get(key, value, s);
      // Find in the acc_index.
      if (flag) {
        return true;
      }
    }
  }
  // std::printf("popTable:\n");
  // pop_table_->Traversal();  // test 3/7
  return pop_table_->Get(key, value, s);
}
/*
bool AccIndex::MergeTable(STable* src, STable* dst, Slice& startKey, Slice nextSmallKey) {
  bool continueMerge = false;
  Iterator* iter = src->NewIterator();
  Slice smallKey = dst->GetSmallestInternalKey();
  STable::Table::Node* start = dst->GetHead();
  for (iter->Seek(smallKey); iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    if (comparator_.comparator.Compare(key, nextSmallKey) < 0) {
      startKey = key;
      continueMerge = true;
      break;
    }
    else {
      std::string internalKey;
      EncodeKey(&internalKey, key);
      // int len = key.size();
      // size_t encodedLen = VarintLength(len) + len;
      size_t encodedLen = internalKey.size();
      char* key_memory = nsa_.AllocateNode(encodedLen);
      memcpy(key_memory, internalKey.data(), encodedLen);
      start = dst->Add(key_memory, encodedLen, start);
    }
  }
  return continueMerge; 
}

bool AccIndex::MergeTable(STable* src, STable* dst){
  Iterator* iter = src->NewIterator();
  Slice smallKey = dst->GetSmallestInternalKey();
  STable::Table::Node* start = dst->GetHead();
  for (iter->Seek(smallKey); iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    std::string internalKey;
    EncodeKey(&internalKey, key);
    // int len = key.size();
    // size_t encodedLen = VarintLength(len) + len;
    size_t encodedLen = internalKey.size();
    char* key_memory = nsa_.AllocateNode(encodedLen);
    memcpy(key_memory, internalKey.data(), encodedLen);
    start = dst->Add(key_memory, encodedLen, start);
  }
}
*/

}