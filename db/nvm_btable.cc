#include "db/nvm_btable.h"
#include "db/nvm_skiplist.h"
#include "db/immutable_small_table_list.h"
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

BTable::BTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), table_(comparator_, &nsa_), refs_(0) {}

BTable::~BTable(){}

size_t BTable::ApproximateMemoryUsage() { return table_.nvm_usage_; }

int BTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

int BTable::KeyComparator::NewCompare(const char* aptr,
										 const char* bptr,
										 const bool hasseq,
										 const SequenceNumber snum) const {
	Slice a = GetLengthPrefixedSlice(aptr);
	Slice b = GetLengthPrefixedSlice(bptr);
	return comparator.NewCompare(a, b, hasseq, snum);
}
bool BTable::KeyComparator::NewCompare(const char* aptr, const char* bptr) const {
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.NewCompare(a, b);
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

class BTableIterator : public Iterator {
 public:
  explicit BTableIterator(BTable::Table* table) : iter_(table) {}

  BTableIterator(const BTableIterator&) = delete;
  BTableIterator& operator=(const BTableIterator&) = delete;

  ~BTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  BTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* BTable::NewIterator() { return new BTableIterator(&table_); }

BTable::Table::Node* BTable::Add(const char* key, size_t encoded_length, Table::Node* start) {
  char* key_memory = nsa_.AllocateNode(encoded_length);
  memcpy(key_memory, key, encoded_length);
  Table::Node* next_start = table_.Insert(key_memory, encoded_length, start);
  return next_start;
}

bool BTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
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
/*
bool BTable::Empty() {
  return (table_.tobeSplitTableList_.empty());
}
*/
/*
void BTable::BackgroundSplitSmallTableToImmList(ImmutableSmallTableList* list) {
  assert(table_.nvm_usage_ >= config::big_memtable_stop_size);
  table_.BackgroundAutomaticSplit(list);
}
*/

BTable::Table::Node* BTable::GetHead() {
  BTable::Table::Node* head = table_.GetHead();
  return head;
}

bool BTable::NeedToSplit() {
  return table_.NeedToSplit();
}

void BTable::Split(ImmutableSmallTableList* imm_list) {
  assert(table_.NeedToSplit());
  table_.SplitSmallTableToList(imm_list);
}

void BTable::Lock() {
  btable_mutex_.Lock();
}

void BTable::Unlock() {
  btable_mutex_.Unlock();
}

}