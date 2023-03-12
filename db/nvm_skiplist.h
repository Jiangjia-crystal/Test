// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_NVM_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_NVM_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the NvmSkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the NvmSkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the NvmSkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <queue>
#include <set>
#include <sys/time.h>
#include <unistd.h>

#include "util/nvm_allocator.h"
#include "util/random.h"
#include "db/immutable_small_table_list.h"

namespace leveldb {

template <typename Key, class Comparator>
class NvmSkipList {
 private:
  friend class ImmutableSmallTableList;  // By JJia
  friend class AccIndex;
  friend class BTable;
  friend class STable;
 public:
  struct Node;
  // Create a new NvmSkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*nsa".  Objects allocated in the nsa
  // must remain allocated for the lifetime of the NvmSkipList object.
  explicit NvmSkipList(Comparator cmp, NvmSimpleAllocator* nsa);

  NvmSkipList(const NvmSkipList&) = delete;
  NvmSkipList& operator=(const NvmSkipList&) = delete;

  // struct SplitTable; // Record the head and tail of the split table. By JJia 10/20

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list. By JJia
  Node* Insert(const Key& key, size_t encoded_size, Node* start);

  void Insert(const Key& key, size_t encoded_size);

  // when a SmallTale is full, trigger this function. By JJia
  void SplitSmallTableToList(ImmutableSmallTableList* imm_list);

  // void PushTempSetIntoQueue();

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  Node* GetHead();

  bool NeedToSplit(); // By JJia 2/12

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const NvmSkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    void NextInNVMSmallTable();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    void PrevInSmallTable();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    void SeekInNVMSmallTable(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    void SeekToFirstInNVMSmallTable();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

    void SeekToLastInNVMSmallTable();

   private:
    friend class STable;
    const NvmSkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 16 };

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
    // return max_height_.load(std::memory_order_relaxed);
  }

/*  static void BackgroundThreadEntryPoint(NvmSkipList* nvmSkiplist) {
    nvmSkiplist->GenerateSmallImmutableMemTable();
  }
*/

  Node* NewNode(const Key& key, int height, size_t encoded_size);  // Add the passed parameter v_size. By JJia
  
  int RandomHeight(); 
  
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node* start, Node** prev) const;

  Node* FindGreaterOrEqualInNVMSmallTable(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  Node* FindLessThanInNVMSmallTable(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  Node* FindLastInNVMSmallTable() const;

  // Immutable after construction
  Comparator const compare_;
  NvmSimpleAllocator* const nsa_;  // NvmSimpleAllocator used for allocations of nodes

  Node* head_; // By JJia

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  // Read/written only by Insert().
  Random rnd_;

  // The probability_ of a node being a boundary. By JJia
  int probability_;

  // Record the total kvSize in the list. By JJia
  // size_t nvmUsage_;
  
  // If a split process is encountered during the access process, the node should jump to. By JJia
  // std::atomic<Node*> jump_node_;
  
  void ChangeHead(Node*);

  void CutTail(Node*);

  std::set<Node*> tobeSplitTableBorder_;  // To temporarily save split information, by JJia 2/12

  unsigned long long int nvm_usage_;

  int small_table_num_;
};

// Implementation details follow
template <typename Key, class Comparator>
struct NvmSkipList<Key, Comparator>::Node {
  explicit Node(const Key& k, bool c, size_t s, size_t n) : key(k), cont(c), s_size(s), encoded_size(n) {}

  Key const key;

  // When access to the tail node, determine whether the current access needs to jump. By JJia
  bool cont;

  // if the node is a border, it save the smallTable's size, the default value is 0. By JJia
  size_t s_size;

  // the encoded size of kv. By JJia
  size_t encoded_size;

  // Reload 
  bool operator<(const Node& N) const 
  {
    return compare_(N.key, key);
  }

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    // If access a SmallTable that has just been split. By JJia
    if (next_[n] == nullptr && cont == true) {
      return next_jump.load(std::memory_order_acquire);
    } else return next_[n].load(std::memory_order_acquire);
  }

  Node* NextInNVMSmallTable(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_acquire);
  }

  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].store(x, std::memory_order_release);
  }

  void SetNextJumpNode(Node* x) {
    next_jump.store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    if (next_[n] == nullptr && cont == true) {
      return next_jump.load(std::memory_order_acquire);
    } else return next_[n].load(std::memory_order_relaxed);
  }

  Node* NoBarrier_Next_InSmallTable(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }

  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  std::atomic<Node*> next_jump;
  // const NvmSkipList* list_;
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<Node*> next_[1]; //柔性数组
};

// Create a new property to record the size of node n.
template <typename Key, class Comparator>
typename NvmSkipList<Key, Comparator>::Node* NvmSkipList<Key, Comparator>::NewNode(
    const Key& key, int height, size_t encoded_size) {
  char* const node_memory = nsa_->AllocateNode(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  return new (node_memory) Node(key, false, 0, encoded_size);
}

template <typename Key, class Comparator>
inline NvmSkipList<Key, Comparator>::Iterator::Iterator(const NvmSkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool NvmSkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& NvmSkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::NextInNVMSmallTable() {
  assert(Valid());
  node_ = node_->NextInNVMSmallTable(0);
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::PrevInSmallTable() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThanInNVMSmallTable(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, list_->head_, nullptr);
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::SeekInNVMSmallTable(const Key& target) {
  node_ = list_->FindGreaterOrEqualInNVMSmallTable(target, nullptr);
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::SeekToFirstInNVMSmallTable() {
  node_ = list_->head_->NextInNVMSmallTable(0);
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void NvmSkipList<Key, Comparator>::Iterator::SeekToLastInNVMSmallTable() {
  node_ = list_->FindLastInNVMSmallTable();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// By JJia
template <typename Key, class Comparator>
void NvmSkipList<Key, Comparator>::SplitSmallTableToList(ImmutableSmallTableList* imm_list) {
  assert(!tobeSplitTableBorder_.empty());
  Node* b = *tobeSplitTableBorder_.begin();
  // S1:Find the next border of SplitTable.
  Node* e = b->Next(kMaxHeight-1);
  size_t smallSPlitTableSize = b->s_size;
  // New border is created in the smallTable.
  if (smallSPlitTableSize < config::nvm_splitTable_limitSize) {
    tobeSplitTableBorder_.erase(b);
    return;
  }
  // Indicate if the head is changed.
  // bool flag = false; 
  // S2:There are Four cases of Full division, head division, middle division and tail division.
  // S2.1:Full division.
  if (b == head_ && e == nullptr) {
    Node* newHead = NewNode(0, kMaxHeight, 0);
    // Frist add to imm_list and then reset the head
    // BTable* smallTable = CreateBTable(b);
    imm_list->Push(head_);  
    // std::printf("full division\n");
    // std::printf("newHead address:%p\n", newHead);
    head_ = newHead;
    // flag = true;
  }
  // S2.2:Head division.
  else if (b == head_ && e != nullptr) {
    Node* prev[kMaxHeight];
    // Create a new head.
    Node* newHead = NewNode(0, kMaxHeight, 0);
    // Set the successor node of the new node.
    for(int i = 0; i < kMaxHeight; i++)
    {
      newHead->SetNext(i, e);
    }
    // Find the predecessors of the end node.
    FindGreaterOrEqual(e->key, head_, prev);
    // Set the predecessors's cont to true, to ensure the correctness of access. When access the node, next will jump to the jump_node_.
    for(int i = 0; i < kMaxHeight; i++)
    {
      prev[i]->cont = true;
      prev[i]->SetNextJumpNode(e);
    }
    // Set the jump_node_.
    // jump_node_ = e;
    // Split smallTable.
    for(int i = kMaxHeight-1; i >= 0; i--)
    {
      prev[i]->SetNext(i, nullptr);
    }
    // BTable* smallTable = CreateBTable(b);
    imm_list->Push(head_);
    head_ = newHead;
    // flag = true;
    // std::printf("head division\n");
    // std::printf("newHead address:%p\n", newHead);
  }
  // S2.3:Middle division.
  else if (b != head_ && e != nullptr) {
    Node* bprev[kMaxHeight];
    Node* eprev[kMaxHeight];

    Node* newHead = NewNode(0, kMaxHeight, 0);

    FindGreaterOrEqual(b->key, head_, bprev);
    FindGreaterOrEqual(e->key, b, eprev);
    
    for (int i = 0; i < kMaxHeight; i++)
    {
      newHead->SetNext(i, b);
      eprev[i]->cont = true;
      eprev[i]->SetNextJumpNode(e);
    }
    // jump_node_ = e;
    for (int i = 0; i < kMaxHeight; i++)
    {
      eprev[i]->SetNext(i, nullptr);
    }
    imm_list->Push(newHead);
    for (int i = 0; i < kMaxHeight; i++)
    {
      bprev[i]->SetNext(i, e);
    }
    // std::printf("middle division\n");
    // std::printf("newHead address:%p\n", newHead);
  }
  // S2.4:Tail division
  else if (b != head_ && e == nullptr) {
    Node* newHead = NewNode(0, kMaxHeight, 0);

    Node* prev[kMaxHeight];
    FindGreaterOrEqual(b->key, head_, prev);
    for(int i = 0; i < kMaxHeight; i++)
    {
      newHead->SetNext(i,b); 
    }
    // BTable* smallTable = CreateBTable(b);
    imm_list->Push(newHead);
    for (int i = 0; i < kMaxHeight; i++)
    {
      prev[i]->SetNext(i, nullptr);
      prev[i]->cont = false;
    }
    // std::printf("tail division\n");
    // std::printf("newHead address:%p\n", newHead);
  }
  // S3:Update the nvm_usage_ and small_table_num_.
  nvm_usage_ -= smallSPlitTableSize;
  assert(nvm_usage_ >= 0);
  tobeSplitTableBorder_.erase(b);
  small_table_num_  -= 1;
  // return flag;
}

template <typename Key, class Comparator>
int NvmSkipList<Key, Comparator>::RandomHeight() {
  static const unsigned int kBranching = 4;
  int height = 1;
  // Increases the probability_ that a node becomes a boundary. By JJia
  if (rnd_.OneIn(probability_) && small_table_num_ < config::max_nvm_small_table_num) {
    return kMaxHeight;
  }

  // Increase height with probability_ 1 in kBranching
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

// Check if the key is smaller than the key of node n.
template <typename Key, class Comparator>
bool NvmSkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  if (n == head_) return true;
  else return (n != nullptr) && (compare_(n->key, key) < 0);
}

// Find the smallest node that is greater than or equal to key.
template <typename Key, class Comparator>
typename NvmSkipList<Key, Comparator>::Node*
NvmSkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node* start,
                                              Node** prev) const {
  Node* x = start;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);   // Start with the top-level index.
    if (KeyIsAfterNode(key, next)) {  // key is larger than the index node and next is not nullptr.
      // Keep searching in this list.
      x = next; // Proceed to the next node in this level.
    } else {    
      if (prev != nullptr) prev[level] = x; // Prev takes the largest node in each layer that is smaller than the key.
                                            // If prev is nullptr, this means the caller does not need to know the prev information.
      if (level == 0) { // if is already in the lowest level, just return the next, not found will return nullptr.
        return next;
      } else {
        // Switch to next list
        level--; 
      }
    }
  }
}

template <typename Key, class Comparator>
typename NvmSkipList<Key, Comparator>::Node*
NvmSkipList<Key, Comparator>::FindGreaterOrEqualInNVMSmallTable(const Key& key,
                                              Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->NextInNVMSmallTable(level);   // Start with the top-level index.
    if (KeyIsAfterNode(key, next)) {  // key is larger than the index node and next is not nullptr.
      // Keep searching in this list.
      x = next; // Proceed to the next node in this level.
    } else {    
      if (prev != nullptr) prev[level] = x; // Prev takes the largest node in each layer that is smaller than the key.
                                            // If prev is nullptr, this means the caller does not need to know the prev information.
      if (level == 0) { // if is already in the lowest level, just return the next, not found will return nullptr.
        return next;
      } else {
        // Switch to next list
        level--; 
      }
    }
  }
}

// Find the largest node that is smaller than Key.
template <typename Key, class Comparator>
typename NvmSkipList<Key, Comparator>::Node*
NvmSkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
typename NvmSkipList<Key, Comparator>::Node*
NvmSkipList<Key, Comparator>::FindLessThanInNVMSmallTable(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->NextInNVMSmallTable(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// Find the last node of skiplist.
template <typename Key, class Comparator>
typename NvmSkipList<Key, Comparator>::Node* NvmSkipList<Key, Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
typename NvmSkipList<Key, Comparator>::Node* NvmSkipList<Key, Comparator>::FindLastInNVMSmallTable()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->NextInNVMSmallTable(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// The constructor for skiplist
template <typename Key, class Comparator>
NvmSkipList<Key, Comparator>::NvmSkipList(Comparator cmp, NvmSimpleAllocator* nsa)
    : compare_(cmp),
      nsa_(nsa),
      head_(NewNode(0 /* any key will do */, kMaxHeight, 0)),
      max_height_(1),
      rnd_(0xdeadbeef), 
      probability_(0.8 * config::max_nvm_small_table_num),
      // jump_node_(nullptr),
      nvm_usage_(0),
      small_table_num_(0) {
  // Set the head node
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
  // std::printf("initial head:%p\n", head_);
}

// Add the ability to determine whether an insert is a border or not, and to change the size of the smallTable saved in the border at insertion time, By JJia.
// May trigger skiplist split, imm_list is the target of the split table that pushed into. 
template <typename Key, class Comparator> 
typename NvmSkipList<Key, Comparator>::Node* 
NvmSkipList<Key, Comparator>::Insert(const Key& key, size_t encoded_size, Node* start) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  // S1:Find all the successors of the key and store them in prev.
  // static constexpr uint64_t kUsecondsPerSecond = 1000000;
  // struct ::timeval tv_begin, tv_end, tv_split_begin, tv_split_end;
  // ::gettimeofday(&tv_begin, NULL);

  // uint64_t start_insert = static_cast<uint64_t>(tv_begin.tv_sec) * kUsecondsPerSecond + tv_begin.tv_usec;
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, start, prev);

  /*
  bool flag = false;
  bool head_change = false;
  // Determine if it a head split.
  if(start == head_) flag = true;
  */ 

  // Our data structure does not allow duplicate insertion.
  assert(x == nullptr || !Equal(key, x->key));

  // S2:Determine the random height of the node generation.
  int height = RandomHeight();
  for (int i = GetMaxHeight(); i < kMaxHeight; i++) {
    prev[i] = head_;
  }
  if (height > GetMaxHeight()) {
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.

    // max_height_.store(height, std::memory_order_relaxed);
    max_height_.store(height, std::memory_order_relaxed);
  }
  // S3:Create a new node.
  x = NewNode(key, height, encoded_size);  
  
  // S4:Determine the borders before and after the node:b_border and e_border.
  Node* b_border = prev[kMaxHeight-1];
  assert(b_border != nullptr);
  //Node* e_border = prev[kMaxHeight-1]->Next(kMaxHeight-1);
  
  // S5:Perform insert, set the pointers.
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x); 
  }
  // S6:The size of the skiplist is added after the insertion is successful.
  nvm_usage_ += encoded_size;

  // S7.1:If you are inserting a new border, the s_size of the SmallTable to which it is inserted needs to be reset.
  if (height == kMaxHeight) {
    // std::printf("new border address:%p\n", x);
    Node* temp = b_border;
    size_t temp_size = 0;
    // S7.1.1:Count the sum of kv size that less than the inserted key.
    // // std::printf("generate a border.\n");
    while (KeyIsAfterNode(key, temp)) { // key >= temp.key
      temp_size += temp->encoded_size;
      temp = temp->Next(0);
    }
    // S7.1.2:Reset the s_size of inserted key and b_border.
    x->s_size = b_border->s_size - temp_size + encoded_size;
    b_border->s_size = temp_size;
    // S7.1.3:Increase the number of small_tables.
    small_table_num_ += 1;
  }
  else {  // S7.2:Insert a normal node may cause split.
    // std::printf("new node address:%p\n", x);
    b_border->s_size += x->encoded_size;
    // Put the border to tobeSplitList.
    if (b_border->s_size >= config::nvm_splitTable_limitSize && nvm_usage_ >= 0.6 * config::big_memtable_stop_size) {
      // ::gettimeofday(&tv_split_begin, NULL);
      // uint64_t start_split = static_cast<uint64_t>(tv_split_begin.tv_sec) * kUsecondsPerSecond + tv_split_begin.tv_usec;
      // head_change = SplitSmallTableToList(b_border, imm_list);
      tobeSplitTableBorder_.emplace(b_border);
      // ::gettimeofday(&tv_split_end, NULL);
      // uint64_t end_split = static_cast<uint64_t>(tv_split_end.tv_sec) * kUsecondsPerSecond + tv_split_end.tv_usec;
      // std::printf("split time is:%llu\n", end_split - start_split);
    }
  }
  return b_border;
  /*
  ::gettimeofday(&tv_end, NULL);
  uint64_t end_insert = static_cast<uint64_t>(tv_end.tv_sec) * kUsecondsPerSecond + tv_end.tv_usec;
  std::printf("insert time is:%llu\n", end_insert - start_insert);
  if (head_change) return head_;
  */
}

template <typename Key, class Comparator>  
void NvmSkipList<Key, Comparator>::Insert(const Key& key, size_t encoded_size) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqualInNVMSmallTable(key, prev);

  // Our data structure does not allow duplicate insertion

  if (x != nullptr && Equal(key, x->key)) {
    std::printf("the same key is insert:%s\n", key);
    // Node* x = FindGreaterOrEqualInNVMSmallTable(key, prev);
  }
  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed);
  }
  //上述步骤找到插入结点的前继结点
  x = NewNode(key, height, encoded_size);  //创建新结点
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next_InSmallTable(i)); //设置插入结点的后继指针
    prev[i]->SetNext(i, x); //设置前继结点指向新结点
  }
}

template <typename Key, class Comparator>
typename NvmSkipList<Key, Comparator>::Node* NvmSkipList<Key, Comparator>::GetHead() {
  return head_;
}

template <typename Key, class Comparator>
void NvmSkipList<Key, Comparator>::ChangeHead(Node* firstNode) {
  Node* newHead = NewNode(0, kMaxHeight, 0);
  // Set the successor node of the new node.
  for(int i = 0; i < kMaxHeight; i++)
  {
    newHead->SetNext(i, firstNode);
  }
  head_ = newHead;
}

template <typename Key, class Comparator>
void NvmSkipList<Key, Comparator>::CutTail(Node* tail) {
  for(int i = 0; i < kMaxHeight; i++)
  {
    tail->SetNext(i, nullptr);
  }
}

template <typename Key, class Comparator>
bool NvmSkipList<Key, Comparator>::NeedToSplit() {
  return !tobeSplitTableBorder_.empty();
}

template <typename Key, class Comparator>
bool NvmSkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, head_, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_NVM_SKIPLIST_H_
