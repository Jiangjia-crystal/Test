// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_NVM_SKIPLIST_INDEX_H_
#define STORAGE_LEVELDB_DB_NVM_SKIPLIST_INDEX_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the NVMSkipListIndex will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the NVMSkipListIndex is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the NVMSkipListIndex.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/nvm_allocator.h"
#include "util/random.h"
#include "port/port_stdcxx.h" 

namespace leveldb {

template <typename Key, class Comparator>
class NVMSkipListIndex {
 // private:

 public:
  struct Node;
  // Create a new NVMSkipListIndex object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit NVMSkipListIndex(Comparator cmp, NvmSimpleAllocator* nsa);

  NVMSkipListIndex(const NVMSkipListIndex&) = delete;
  NVMSkipListIndex& operator=(const NVMSkipListIndex&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key, void* value);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // return the num of node in the skip list,do not include the to be delete node.
  int Num();

  void Delete(const Key& key);

  void* RandomRemove();

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const NVMSkipListIndex* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    bool NextValid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    const Key& nextKey() const;

    void* value();

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    bool SeekToLast();

    void SeekInsertNode(const Key& target);

    void Lock();

    void UnLock();

    void SetValueNull();

    Node* GetNode();

   private:
    const NVMSkipListIndex* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  Node* NewNode(const Key& key, int height, void* value);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  Node* FindLessOrEqual(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Immutable after construction
  Comparator const compare_;
  NvmSimpleAllocator* const nsa_;

  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  // Read/written only by Insert().
  Random rnd_;

  std::atomic<int> num_;
};

// Implementation details follow
template <typename Key, class Comparator>
struct NVMSkipListIndex<Key, Comparator>::Node {
  explicit Node(const Key& k, void* v, int h) : key(k), value(v), height(h) {}

  Key const key;

  void* value;

  int height;

  std::mutex node_mutex;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }
  bool Lock() {
    return node_mutex.try_lock();
  }

  void Unlock() {
    node_mutex.unlock();
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<Node*> next_[1]; //柔性数组
};

template <typename Key, class Comparator>
typename NVMSkipListIndex<Key, Comparator>::Node* NVMSkipListIndex<Key, Comparator>::NewNode(
    const Key& key, int height, void* value) {
  char* const node_memory = nsa_->AllocateNode(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  return new (node_memory) Node(key, value, height);
}

template <typename Key, class Comparator>
inline NVMSkipListIndex<Key, Comparator>::Iterator::Iterator(const NVMSkipListIndex* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool NVMSkipListIndex<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline bool NVMSkipListIndex<Key, Comparator>::Iterator::NextValid() const {
  return node_->Next(0) != nullptr;
}

template <typename Key, class Comparator>
inline const Key& NVMSkipListIndex<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline const Key& NVMSkipListIndex<Key, Comparator>::Iterator::nextKey() const {
  assert(Valid());
  return node_->Next(0)->key;
}

template <typename Key, class Comparator>
inline void* NVMSkipListIndex<Key, Comparator>::Iterator::value() {
  assert(Valid());
  return node_->value;
}

template <typename Key, class Comparator>
inline void NVMSkipListIndex<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void NVMSkipListIndex<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void NVMSkipListIndex<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void NVMSkipListIndex<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline bool NVMSkipListIndex<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
    return false;
  }
  else return true;
}

template <typename Key, class Comparator>
inline void NVMSkipListIndex<Key, Comparator>::Iterator::SeekInsertNode(const Key& target) {
  node_ = list_->FindLessOrEqual(target);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void NVMSkipListIndex<Key, Comparator>::Iterator::Lock() {
  node_->Lock();
}

template <typename Key, class Comparator>
inline void NVMSkipListIndex<Key, Comparator>::Iterator::UnLock() {
  node_->Unlock();
}

template <typename Key, class Comparator>
inline void NVMSkipListIndex<Key, Comparator>::Iterator::SetValueNull() {
  node_->value = nullptr;
}

template <typename Key, class Comparator>
inline typename NVMSkipListIndex<Key, Comparator>::Node* 
NVMSkipListIndex<Key, Comparator>::Iterator::GetNode() {
  return node_;
}

template <typename Key, class Comparator>
int NVMSkipListIndex<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

//判断key是不是比结点n的key值小
template <typename Key, class Comparator>
bool NVMSkipListIndex<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

//找到每一层大于或等于key的结点
template <typename Key, class Comparator>
typename NVMSkipListIndex<Key, Comparator>::Node*
NVMSkipListIndex<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);   //先从最高层索引开始查找
    if (KeyIsAfterNode(key, next)) {  //如果要查找的key在该索引结点后
      // Keep searching in this list
      x = next; //继续向后
    } else {    //要查找的key在x和x->next之间
      if (prev != nullptr) prev[level] = x; //prev记下每一层的比key小但key值最大的结点
      if (level == 0) { //如果已经是最底层，那么next就是大于key的最小结点
        return next;
      } else {
        // Switch to next list
        level--; //如果不是最底层，就向下一层索引层继续向后查询
      }
    }
  }
}

//查找比Key小的最大结点
template <typename Key, class Comparator>
typename NVMSkipListIndex<Key, Comparator>::Node*
NVMSkipListIndex<Key, Comparator>::FindLessThan(const Key& key) const {
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
typename NVMSkipListIndex<Key, Comparator>::Node*
NVMSkipListIndex<Key, Comparator>::FindLessOrEqual(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) <= 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) > 0) {
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

//找到skiplist的最后一个结点
template <typename Key, class Comparator>
typename NVMSkipListIndex<Key, Comparator>::Node* NVMSkipListIndex<Key, Comparator>::FindLast()
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

//skiplist的构造函数
template <typename Key, class Comparator>
NVMSkipListIndex<Key, Comparator>::NVMSkipListIndex(Comparator cmp, NvmSimpleAllocator* nsa)
    : compare_(cmp),
      nsa_(nsa),
      head_(NewNode(0 /* any key will do */, kMaxHeight, nullptr)),
      max_height_(1),
      rnd_(0xdeadbeef),
      num_(0) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

template <typename Key, class Comparator>
void NVMSkipListIndex<Key, Comparator>::Insert(const Key& key, void* value) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
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
  x = NewNode(key, height, value);  //创建新结点
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i)); //设置插入结点的后继指针
    prev[i]->SetNext(i, x); //设置前继结点指向新结点
  }
  num_.fetch_add(1);
}

template <typename Key, class Comparator>
void NVMSkipListIndex<Key, Comparator>::Delete(const Key& key) {
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);  

  for (int i = x->height - 1; i >= 0; i--) {
    prev[i]->SetNext(i, x->Next(i)); //设置前继结点指向后继结点
  }
  // num_--;
}

template <typename Key, class Comparator>
bool NVMSkipListIndex<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

template <typename Key, class Comparator>
int NVMSkipListIndex<Key, Comparator>::Num() {
  return num_.load();
}

// Now the lock partition is not be considered.
template <typename Key, class Comparator>
void* NVMSkipListIndex<Key, Comparator>::RandomRemove() {
  assert(Num() != 0);
  // void* stable = nullptr;
  Node* delNode = head_->Next(0);
  /*
  std::srand((unsigned)std::time(NULL)); 
  int num = random()%Num();
  
  for (int i = 0; i < num-1; i++) {
    delNode = delNode->Next(0);
  }
  */
  while (delNode->value == nullptr) {
    delNode = delNode->Next(0);
    // find to the last already
    if (delNode == nullptr) {
      break;
    }
  }
  /*
  if (delNode->value == nullptr) {
    while (delNode->value == nullptr) {
      delNode = delNode->Next(0);
      if (delNode == nullptr) {
        delNode = head_;
      }
    }
  }
  */
  // delNode->Lock();
  // std::printf("delNode value:%x\n", delNode->value);
  // stable = delNode->value;
  // std::printf("stable is:%x\n", stable);
  Delete(delNode->key);
  
  // delNode->value = nullptr;
  // delNode->Unlock();
  // std::printf("after delete delNode value:%x\n", delNode->value);
  num_.fetch_sub(1);
  assert(delNode->value != nullptr);
  return delNode->value;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_ACCELERATED_INDEX_H_
