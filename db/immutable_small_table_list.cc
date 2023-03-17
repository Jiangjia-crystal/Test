#include "db/immutable_small_table_list.h"
#include "db/nvm_stable.h"
#include "util/mutexlock.h"
#include <iostream>

namespace leveldb{

ImmutableSmallTableList::ImmutableSmallTableList(const InternalKeyComparator& comparator)
    :comparator_(comparator), refs_(0), nvm_usage_(0) {}

bool ImmutableSmallTableList::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  MutexLock l(&imm_list_mutex_);
  for (auto iter = SmallTableList_.begin(); iter != SmallTableList_.end(); iter++) {
    (*iter)->Ref();
    if ((*iter)->Get(key, value, s)) {
      (*iter)->Unref();
      return true;
    }
    (*iter)->Unref();
  }
  return false;  
}

void ImmutableSmallTableList::Push(void* node) {
  STable* insertTable = CreateSTable(node);
  // For test
  // Slice key = insertTable->GetSmallestInternalKey();
  MutexLock l(&imm_list_mutex_);
  SmallTableList_.emplace_front(insertTable);
  nvm_usage_ += insertTable->ApproximateMemoryUsage();
}

void ImmutableSmallTableList::Pop() {
  MutexLock l(&imm_list_mutex_);
  STable* back_small_table = SmallTableList_.back();
  SmallTableList_.pop_back();
  nvm_usage_ -= back_small_table->ApproximateMemoryUsage();
  back_small_table->Unref();
}

STable* ImmutableSmallTableList::Front() {
  STable* tmp_stable;
  MutexLock l(&imm_list_mutex_);
  tmp_stable = SmallTableList_.front();
  return tmp_stable;
}

STable* ImmutableSmallTableList::Back() {
  STable* tmp_stable;
  MutexLock l(&imm_list_mutex_);
  tmp_stable = SmallTableList_.back();
  return tmp_stable;
}

bool ImmutableSmallTableList::Empty() {
  return Num() == 0;
}

size_t ImmutableSmallTableList::ApproximateMemoryUsage() {
  return nvm_usage_;
}

size_t ImmutableSmallTableList::Num() {
  MutexLock l(&imm_list_mutex_);
  return SmallTableList_.size();
}
/*
void ImmutableSmallTableList::MarkPushTable(void* splitTable) {
  // Create a New STable with the Node* splitTable.
  STable* immutableSmallTable = new STable(comparator_);
  immutableSmallTable->Ref();
  immutableSmallTable->table_.head_ = (STable::Table::Node*)splitTable;
  immutableSmallTable->table_.max_height_.store(immutableSmallTable->table_.kMaxHeight, std::memory_order_relaxed);

  push_table_ = immutableSmallTable;
}

void ImmutableSmallTableList::MergeTree() {
  // S1: Get the push_table border.
  const std::string smallkey = push_table_->GetSmallestKey();
  // const std::string largekey = push_table_->GetLargestKey();
  // S2: Determine if there are overlapping intervals in the radix tree.
  // S3: The tree is empty now, just push in.
  if (tree_.empty()) {
    if (pop_table_ == nullptr) {
      pop_table_ = push_table_;
      push_table_ = nullptr;
    }
    else {
      tree_mutex_.Lock();
      tree_[smallkey] = push_table_;
      tree_mutex_.Unlock();
    }
  } 
  else {
    // S4: The tree is non-empty, find the range to scan in the radix tree.
    // Build tree iterators.
    // radix_tree<std::string, STable*>::iterator tree_beginIt = tree_.altree_find(smallkey);
    // radix_tree<std::string, STable*>::iterator tree_endIt = tree_.altree_find(largekey);
    radix_tree<std::string, STable*>::iterator it;
    radix_tree<std::string, STable*>::iterator nextTreeNodeIt;
    STable* nextTable;
    STable* insert_table;
    Slice nextTableFirstKey;
    // Build table iterators.
    Iterator* pushTable_it = push_table_->NewIterator();
    pushTable_it->SeekToFirst();

    // S4.1: if the minmum border is less than the tree.
    if (tree_beginIt == tree_.end()) {
      // S4.2: no overlap.
      if (tree_endIt == tree_.end()) {
        tree_[smallkey] = push_table_;
        tree_[largekey] = nullptr;
        Push(push_table_);
      } else {
        // S4.3: has overlap.
        // S4.3.1: get next table. 
        nextTreeNodeIt = tree_.begin();
        // S4.3.2: Insert the previous non-overlapping partial table, using data from the original table.
        insert_table = CreateSTable(push_table_->GetHead());        
        tree_[smallkey] = insert_table;

        nextTable = nextTreeNodeIt->second;

        nextTableFirstKey = nextTable->GetSmallestInternalKey();
        // The bottom of seek is to find the first node that is greater than or equal to the key
        push_table_->ChangeHead(nextTableFirstKey);
        insert_table->CutTail(nextTableFirstKey);

        // S4.3.3: Start merge the overlapping table.
        for (it = tree_.begin(); it != tree_endIt; ++it) {
          nextTreeNodeIt++;
          nextTable = nextTreeNodeIt->second;
          nextTableFirstKey = nextTable->GetSmallestInternalKey();
          MergeTable(it->second, nextTableFirstKey);
          push_table_->ChangeHead(nextTableFirstKey);
          insert_table->CutTail(nextTableFirstKey);
        }
        // S4: deal with the last table.
        if (tree_endIt == nullptr) {
          tree_[tree_endIt->first] = push_table_;
          tree_[largekey] = nullptr;          
        } else {
          MergeTable(tree_endIt, NULL);
        }
      }
    } else {
      if (smallkey > GetTreeLargestKey()) {
        tree_[smallkey] = push_table_;
        tree_[largekey] = nullptr;        
      } else {
        nextTreeNodeIt = tree_beginIt;
        for (it = tree_beginIt; it != tree_endIt; ++it) {
          nextTreeNodeIt++;
          nextTable = nextTreeNodeIt->second;
          nextTableFirstKey = nextTable->GetSmallestInternalKey();
          MergeTable(it->second, nextTableFirstKey);
          push_table_->ChangeHead(nextTableFirstKey);
          insert_table->CutTail(nextTableFirstKey);
        }
        
        if (tree_endIt == nullptr) {
          tree_[tree_endIt->first] = push_table_;
          tree_[largekey] = nullptr;          
        } else {
          MergeTable(tree_endIt, NULL);
        }        
      }      
    }
  }
}

void ImmutableSmallTableList::MergeTable(STable* original_table, Slice nextTableKey) {
  if (it->second == nullptr) {
    STable* insert_table = CreateSTable(push_table_->GetHead());
    tree_[it->first] = insert_table;
  }
  else {
    Iterator* original_iter = original_table->NewIterator();
    Iterator* push_iter = push_table_->NewIterator();
    STable::Table::Node* start = push_table_->GetHead();
    if (nextTableKey == NULL) {
      for(push_iter->SeekToFirst(); push_iter->Valid(); push_iter->Next()) {
        Slice internalKey = push_iter->key();
        const char* key = push_table_->getMemKey(push_iter);
        size_t encoded_len = push_table_->getEncodedLength(push_iter);
        start = b_mem->Add(key, encoded_len, start);
      }
    }
    else {
      for(push_iter->SeekToFirst(); push_iter->Valid(); push_iter->Next()) {
        Slice internalKey = push_iter->key();
        // The key traversed is already greater than or equal to the first key of the next table
        if (comparator_(internalKey, nextTableKey) <= 0) {
          break;
        }
        const char* key = push_table_->getMemKey(push_iter);
        size_t encoded_len = push_table_->getEncodedLength(push_iter);
        start = b_mem->Add(key, encoded_len, start);
      }      
    }
    delete push_iter;
    delete original_iter;
  }

}
*/
STable* ImmutableSmallTableList::CreateSTable(void* head) {
  STable* insert_table = new STable(comparator_);
  insert_table->Ref();
  insert_table->table_.head_ = (STable::Table::Node*)head;
  insert_table->table_.max_height_.store(insert_table->table_.kMaxHeight, std::memory_order_relaxed); // maybe kmaxheight is not true, influence the read performance
  insert_table->table_.nvm_usage_ = insert_table->table_.head_->s_size;
  return insert_table;
}

/*
std::string ImmutableSmallTableList::GetTreeSmallestKey() {
  radix_tree<std::string, void*>::iterator it = tree_.begin();
  STable* firstSTable = (STable*)it->second;
  std::string smallestkey = firstSTable->GetSmallestKey();
  return smallestkey;
}

std::string ImmutableSmallTableList::GetTreeLargestKey() {
  radix_tree<std::string, void*>::iterator it = tree_.begin();
  radix_tree<std::string, void*>::iterator it_next = ++tree_.begin();
  while (it_next != tree_.end()) {
    it++;
    it_next++;
  }
  STable* lastSTable = (STable*)it->second;
  std::string largestkey = lastSTable->GetLargestKey();
  return largestkey;
}
*/

}