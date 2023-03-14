#include "db/nvm_stable.h"
#include "db/nvm_skiplist.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb{

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

STable::STable(const InternalKeyComparator& comparator)
    : comparator_(comparator), table_(comparator_, &nsa_), refs_(0) {}

STable::~STable(){}

//size_t STable::ApproximateMemoryUsage() { return table_.nvm_usage_; }

int STable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class STableIterator : public Iterator {
 public:
  explicit STableIterator(STable::Table* table) : iter_(table) {}

  STableIterator(const STableIterator&) = delete;
  STableIterator& operator=(const STableIterator&) = delete;

  ~STableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.SeekInNVMSmallTable(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirstInNVMSmallTable(); }
  void SeekToLast() override { iter_.SeekToLastInNVMSmallTable(); }
  void Next() override { iter_.NextInNVMSmallTable(); }
  void Prev() override { iter_.PrevInSmallTable(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  friend class STable;
  STable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* STable::NewIterator() { return new STableIterator(&table_); }

const char* STable::getMemKey(Iterator* iter) const {
  STableIterator* stableIter = (STableIterator*) iter;
  return stableIter->iter_.key(); 
}

STable::Table::Node* STable::GetHead() {
  return table_.GetHead();
}


void STable::ChangeHead(Slice key) {
  table_.FindGreaterOrEqualInNVMSmallTable(key.data(), nullptr);
  Table::Node* head_next= table_.FindGreaterOrEqualInNVMSmallTable(key.data(), nullptr);;
  table_.ChangeHead(head_next);
}

void STable::CutTail(Slice key) {
  Table::Node* tail = table_.FindLessThanInNVMSmallTable(key.data());
  table_.CutTail(tail);
}
/*
std::string STable::GetSmallestKey() {
  Table::Iterator iter(&table_);
  iter.SeekToFirstInNVMSmallTable();
  
  const char* entry = iter.key();
  uint32_t key_length;
  const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);

  Slice smallestKey = Slice(key_ptr, key_length - 8);
  return smallestKey.ToString();
} 

std::string STable::GetLargestKey() {
  Table::Iterator iter(&table_);
  iter.SeekToLastInNVMSmallTable();

  const char* entry = iter.key();
  uint32_t key_length;
  const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);

  Slice largestKey = Slice(key_ptr, key_length - 8);
  return largestKey.ToString();
}
*/

Slice STable::GetSmallestInternalKey() {
  Iterator* it = this->NewIterator();
  it->SeekToFirst();
  Slice smallkey = it->key();
  delete it;
  return smallkey;
}

Slice STable::GetLargestInternalKey() {
  Iterator* it = this->NewIterator();
  it->SeekToLast();
  Slice smallkey = it->key();
  delete it;
  return smallkey;
}

bool STable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.SeekInNVMSmallTable(memkey.data());
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
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);   //从entry中获取klength
    // 3/7 for test
    std::string userkey = Slice(key_ptr, key_length - 8).ToString();

    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {  //获取userkey并判断是否正确
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

void STable::Add(const char* key, size_t encoded_length) {
  // std::printf("%s ", key);
  // std::printf("%d\n", encoded_length);
  char* key_memory = nsa_.AllocateNode(encoded_length);
  memcpy(key_memory, key, encoded_length);
  table_.Insert(key_memory, encoded_length);
}

const char* STable::getKey(Iterator* iter) {
  STableIterator* stableIter = (STableIterator*) iter;
  // For test 3/7
  // Slice internalKey = iter->key();
  // std::printf("add a key to acc_index table:%s\n", ExtractUserKey(internalKey).ToString().data());
  return stableIter->iter_.key(); 
}

size_t STable::getEncodedLength(Iterator* iter) const { 
  Slice k = iter->key();
  Slice v = iter->value();
  size_t internal_key_size = k.size() + 8;
  size_t encoded_length = VarintLength(internal_key_size) +  
                      internal_key_size + VarintLength(v.size()) +
                      v.size();
  return encoded_length;
}

void STable::Traversal() {
  // std::printf("table %x's head is %x\n", this, GetHead());
  Table::Iterator iter(&table_);
  iter.SeekToFirst();
  while (iter.Valid()) {
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);   //从entry中获取klength
    // 3/7 for test
    std::string userkey = Slice(key_ptr, key_length - 8).ToString();
    /*
    if (strcmp(userkey.data(), "XlVGXI1ZA7hnIdeb") == 0) {
      std::printf("XlVGXI1ZA7hnIdeb write into stable\n");
    }
    */
    std::printf("key:%s\n", userkey.data());
    iter.NextInNVMSmallTable();
  }
}

}