// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"


namespace leveldb {

class MemTable;
class BTable;
class STable;
class ImmutableSmallTableList;
class AccIndex;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RemoveLog();

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Compact the memtable in dram to nvm BTable. By JJia
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Flush data from DRAM to NVM, maybe trigger the split of BTable.
  void WriteBTable(MemTable* mem, BTable* b_mem);
  
  // the function used in original version to flush immutable MemTable in DRAM to SSD.
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // flush the last table in the immu_list to the SSD.
  Status FlushNVMImmutableMemTableToSSD(STable* mem, VersionEdit* edit, Version* base);

  // Prepare for writing, check if there is enough space, maybe blocking.
  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForNVMCompact();  // By JJia
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  //void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void MaybeScheduleMemTableCompaction();
  void MaybeScheduleBTableSplit();
  void MaybeScheduleInsertIntoAccIndex();
  void MaybeScheduleNVMImmutableMemTableCompaction();
  void MaybeScheduleSSDCompaction();

  // static void BGWork(void* db);
  static void BGMemCompactWork(void* db);
  static void BGSplitBTable(void* db);
  static void BGInsertIntoAccIndex(void* db);
  static void BGNVMImmutableMemTableCompactWork(void* db);
  static void BGSSDCompactWork(void* db);
  // void BackgroundCall();
  void BackgroundCallMemCompact();
  void BackgroundCallSplitBTable();
  void BackgroundCallInsertIntoAccIndex();
  void BackgroundCallImmutableMemTableCompact();
  void BackgroundCallSSDCompact();
  
  // void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // void CompactNVMImmutableMemTable(BTable* last);
  // void BackgroundMemTableCompact();
  void BackgroundCompactImmutableMemTable();
  void BackgroundSplitBTable(BTable* b_mem, ImmutableSmallTableList* imm_list);
  void BackgroundInsertIntoAccIndex();
  void BackgroundSSDCompact();
  
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // will not change after initialization in the constructor
  // Constant after construction
  Env* const env_;  //Encapsulates system-related file operations, threads, etc.
  const InternalKeyComparator internal_comparator_;    // key comparator
  const InternalFilterPolicy internal_filter_policy_;    // filter policy
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  //
  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::Mutex btable_mutex_;  // By JJia
  // port::Mutex imm_list_mutex_;  
  // port::Mutex log_and_apply_mutex_;
  std::atomic<bool> shutting_down_;
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  port::CondVar logAndApply_finished_signal_;
  port::CondVar background_mem_compact_finished_signal_; // By JJia
  port::CondVar background_compact_ssd_finished_signal; //By JJia
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  BTable* big_mem_;  // By JJia
  ImmutableSmallTableList* imm_list_;  // By JJia
  AccIndex* acc_index_;
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_

  size_t nvm_imm_number_; // For test
  size_t nvm_btable_size_;
  size_t read_in_dram_mem_;
  size_t read_in_dram_imm_;
  size_t read_in_nvm_btable_;
  size_t read_in_nvm_imm_list_;
  size_t read_in_acc_index_;
  size_t read_in_ssd_;

  uint64_t tmp_level0_number_;
  uint64_t interval_stall_time_;
  uint64_t write_btable_time_;
  uint64_t wait_lock_time_;
  uint64_t release_lock_time_;

  // These three are log-related
  WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  int prev_logfile_number_;
  log::Writer* log_;

  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  std::atomic<bool> logAndApply_scheduled_;

  std::atomic<bool> background_compact_memtable_scheduled;

  bool background_nvm_immutable_memtable_scheduled;

  bool background_split_btable_scheduled_; // By JJia 2/12

  bool background_insert_into_accIndex_scheduled_;

  bool background_ssd_compact_scheduled_;  // By JJia

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

  VersionSet* const versions_ GUARDED_BY(mutex_);

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);

  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_