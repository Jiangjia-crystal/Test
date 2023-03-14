// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>
#include <sstream>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/nvm_accindex.h"
#include "db/immutable_small_table_list.h"
#include "db/nvm_stable.h"
#include "db/nvm_btable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

class MemTableIterator;
class BTableIterator;
class STableIterator;

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      background_mem_compact_finished_signal_(&mutex_),
      background_compact_ssd_finished_signal(&mutex_),
      logAndApply_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      big_mem_(nullptr),
      imm_list_(nullptr),
      acc_index_(nullptr),
      has_imm_(false),
      nvm_imm_number_(0),
      nvm_btable_size_(0),
      read_in_dram_mem_(0),
      read_in_dram_imm_(0),
      read_in_nvm_btable_(0),
      read_in_nvm_imm_list_(0),
      read_in_acc_index_(0),
      read_in_pop_table_(0),
      read_in_ssd_(0),
      logfile_(nullptr),
      logfile_number_(0),
      prev_logfile_number_(0),
      log_(nullptr),
      interval_stall_time_(0),
      write_btable_time_(0),
      wait_lock_time_(0),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      background_nvm_immutable_memtable_scheduled(false),
      background_compact_memtable_scheduled(false),
      background_ssd_compact_scheduled_(false),
      background_split_btable_scheduled_(false),
      background_insert_into_accIndex_scheduled_(false),
      logAndApply_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  Log(options_.info_log, "The read hit in dram mem is:%d\n", read_in_dram_mem_);
  Log(options_.info_log, "The read hit in dram imm is:%d\n", read_in_dram_imm_);
  Log(options_.info_log, "The read hit in nvm btable is:%d\n", read_in_nvm_btable_);
  Log(options_.info_log, "The read hit in nvm immlist is:%d\n", read_in_nvm_imm_list_);
  Log(options_.info_log, "The read hit in acc index is:%d\n", read_in_acc_index_);
  Log(options_.info_log, "The read hit in pop_table is:%d\n", read_in_pop_table_);
  Log(options_.info_log, "The read hit in ssd is:%d\n", read_in_ssd_);
  // std::printf("interval stall time: %llu\n", interval_stall_time_);
  // std::printf("write btable time: %llu\n", write_btable_time_);
  // std::printf("wait lock time: %llu\n", wait_lock_time_);
  Log(options_.info_log, "max nvm imm number: %llu\n", nvm_imm_number_);
  Log(options_.info_log, "max big memtable size: %llu\n", nvm_btable_size_);
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compact_memtable_scheduled.load(std::memory_order_acquire)) {
    background_mem_compact_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  // if (big_mem_ != nullptr) big_mem_->Unref();  // By JJia
  // if (imm_list_ != nullptr) imm_list_->Unref(); 
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
// S1 首先生产DB元信息，设置comparator名，以及log文件编号、文件编号，以及seq no。
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

// S2 生成MANIFEST文件，将db元信息写入MANIFEST文件。
  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
//  S3 如果成功，就把MANIFEST文件名写到CURRENT文件中。
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
//      case kLogFile:
//        keep = ((number >= versions_->LogNumber()) ||
//                (number == versions_->PrevLogNumber()));
//        break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
}

void DBImpl::RemoveLog() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  char filename[10];
  std::sprintf(filename, "%06d%s", prev_logfile_number_, ".log");
  // std::printf("%s\n", filename);
  std::stringstream ss;
  ss << filename;
  std::string temp_logfile_name;
  ss >> temp_logfile_name;
  // std::printf("%s\n", temp_logfile_name.c_str());
  Log(options_.info_log, "Delete type=0 #%lld\n", static_cast<unsigned long long>(prev_logfile_number_));

  mutex_.Unlock();
  env_->RemoveFile(dbname_ + "/" + temp_logfile_name);
  mutex_.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  // S1 创建目录，目录以db name命名，忽略任何创建错误，然后尝试获取db name/LOCK文件锁，失败则返回。
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  // S2 根据CURRENT文件是否存在，以及options参数执行检查。
  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {   // 如果文件不存在就调用NewDB创建。
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else { 
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {   // 如果文件存在则报错
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  // 调用VersionSet的Recover函数从文件中恢复数据。
  s = versions_->Recover(save_manifest); 
  if (!s.ok()) {
    return s;
  }

  // S4.1 这里先找出所有满足条件的log文件：比manifest文件记录的log编号更新。
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  // S4尝试从所有比manifest文件中记录的log要新的log文件中恢复（前一个版本可能会添加新的log文件，却没有记录在manifest中）。
  // 另外，函数PrevLogNumber()已经不再用了，仅为了兼容老版本。
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

// Similar to WriteLevel0Table, By JJia
Status DBImpl::FlushNVMImmutableMemTableToSSD(STable* mem, VersionEdit* edit,
                                           Version* base) {
  mutex_.AssertHeld();

  if (imm_ != nullptr && !background_compact_memtable_scheduled.load(std::memory_order_consume)) {  // Add by JJia, 12/2
    background_compact_memtable_scheduled.store(true, std::memory_order_release);
    // Log(options_.info_log, "FlushNVMImm compact memtable start...\n");
    CompactMemTable();
    // Log(options_.info_log, "FlushNVMImm compact memtable finished...\n");
    background_compact_memtable_scheduled.store(false, std::memory_order_release);
    background_mem_compact_finished_signal_.SignalAll();
    release_lock_time_ = env_->NowMicros();
  }

  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  tmp_level0_number_ = meta.number;
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  // pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;                                            
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
    //  MaybeScheduleCompaction(); //By JJia 11/3
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

/*
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}
*/

/*
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BGSplitBTableWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallSplitBTable();
}
*/
void DBImpl::MaybeScheduleSSDCompaction() {
  mutex_.AssertHeld();
  if (background_ssd_compact_scheduled_) {
    // Log(options_.info_log, "Background SSDCompact already start.\n");
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (versions_->NeedsCompaction()) {
    background_ssd_compact_scheduled_ = true;
    env_->Schedule(&DBImpl::BGSSDCompactWork, this, 4);
  }
}

void DBImpl::BGSSDCompactWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallSSDCompact();
}

void DBImpl::BackgroundCallSSDCompact() {
  MutexLock l(&mutex_);
  assert(background_ssd_compact_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundSSDCompact();
  }
  background_ssd_compact_scheduled_ = false;
  MaybeScheduleSSDCompaction();
  background_compact_ssd_finished_signal.SignalAll();
}

void DBImpl::BackgroundSSDCompact() {
  mutex_.AssertHeld();

  if (imm_ != nullptr && !background_compact_memtable_scheduled.load(std::memory_order_consume)) {  // Add by JJia, 12/2
    background_compact_memtable_scheduled.store(true, std::memory_order_release);
    // Log(options_.info_log, "BGSSD compact memtable start...\n");
    CompactMemTable();
    // Log(options_.info_log, "BGSSD compact memtable finished...\n");
    background_compact_memtable_scheduled.store(false, std::memory_order_release);
    background_mem_compact_finished_signal_.SignalAll();
    release_lock_time_ = env_->NowMicros();
  }

  // Log(options_.info_log, "BackgroundSSDCompact started...");
  Compaction* c;
  c = versions_->PickCompaction();
  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);

    while (logAndApply_scheduled_.load()) {
      logAndApply_finished_signal_.Wait(); 
    }
    // log_and_apply_mutex_.Lock();
    logAndApply_scheduled_.store(true); 

    // Log(options_.info_log, "BackgroundSSDCompact trivialMove call LogAndApply"); 
    status = versions_->LogAndApply(c->edit(), &mutex_);
    // Log(options_.info_log, "BackgroundSSDCompact trivialMove finished LogAndApply");

    logAndApply_finished_signal_.SignalAll();
    logAndApply_scheduled_.store(false);
    // log_and_apply_mutex_.Unlock();

    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    // Log(options_.info_log, "BackgroundSSDCompact remove files...");
    RemoveObsoleteFiles();
    // Log(options_.info_log, "BackgroundSSDCompact finished remove files...");
    // versions_->PrintLevelInfo();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }
}

void DBImpl::MaybeScheduleNVMImmutableMemTableCompaction() {
  MutexLock l(&mutex_);
  if (background_nvm_immutable_memtable_scheduled) {

  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (!acc_index_->Empty() || acc_index_->HasPop()) {  // test 3/7
    env_->Schedule(&DBImpl::BGNVMImmutableMemTableCompactWork, this, 3);
    background_nvm_immutable_memtable_scheduled = true;
  }
}

void DBImpl::BGNVMImmutableMemTableCompactWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallImmutableMemTableCompact();
}

void DBImpl::BackgroundCallImmutableMemTableCompact() {
  MutexLock l(&mutex_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompactImmutableMemTable(); // 3/13 for test
  }
  background_nvm_immutable_memtable_scheduled = false;
}

void DBImpl::BackgroundCompactImmutableMemTable() {
  mutex_.AssertHeld();
  while (!acc_index_->Empty() || acc_index_->HasPop()) // test by 3/6
  {
    if (imm_ != nullptr && !background_compact_memtable_scheduled.load(std::memory_order_consume)) {  // Add by JJia, 12/2
      background_compact_memtable_scheduled.store(true, std::memory_order_release);
      // Log(options_.info_log, "BGCompactImm compact memtable start...\n");
      CompactMemTable();
      // Log(options_.info_log, "BGCompactImm compact memtable finished...\n");
      background_compact_memtable_scheduled.store(false, std::memory_order_release);
      background_mem_compact_finished_signal_.SignalAll();
      release_lock_time_ = env_->NowMicros();
    }
    // Add an operation to get the mutex.
    mutex_.Unlock();
    if (!acc_index_->HasPop()) {
      acc_index_->RandomRemove();  // 3/6
    }
    STable* last = acc_index_->GetPopTable();  // 3/6
    // std::printf("popTable:%x\n", last);
    // last->Traversal();
    // STable* last = imm_list_->Back();
    assert(last != nullptr);
    mutex_.Lock();
    MakeRoomForNVMCompact();
    // Save the contents of the memtable as a new Table
    VersionEdit edit;
    Version* base = versions_->current();
    base->Ref();
    Status s = FlushNVMImmutableMemTableToSSD(last, &edit, base);
    base->Unref();

    if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
      s = Status::IOError("Deleting DB during memtable compaction");
    }

    // Replace immutable memtable with the generated Table
    if (s.ok()) {
      // edit.SetPrevLogNumber(0);
      // edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
      while (logAndApply_scheduled_.load()) {
        logAndApply_finished_signal_.Wait(); 
      }  
      logAndApply_scheduled_.store(true); 
      // log_and_apply_mutex_.Lock();  
      // Log(options_.info_log, "BackgroundNVMImmCompact call LogAndApply");                
      s = versions_->LogAndApply(&edit, &mutex_);
      // log_and_apply_mutex_.Unlock();
      logAndApply_finished_signal_.SignalAll();
      logAndApply_scheduled_.store(false);
      // Log(options_.info_log, "BackgroundNVMImmCompact finished LogAndApply");
    }
    pending_outputs_.erase(tmp_level0_number_);

    // versions_->PrintLevelInfo();

    if (s.ok()) {
      acc_index_->SetPopTableNull();  // 3.6
      // imm_list_->Pop();
      MaybeScheduleSSDCompaction();
    } else {
      RecordBackgroundError(s);
    } 
  }
  // DoCompactionWork();
}

Status DBImpl::MakeRoomForNVMCompact() {
  mutex_.AssertHeld();
  // assert(!acc_index_->Empty());
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_compact_ssd_finished_signal.Wait();
    } else break;
  }
  return s;
}

void DBImpl::MaybeScheduleInsertIntoAccIndex() {
  if (background_insert_into_accIndex_scheduled_) {
    // Do nothing.
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_list_->Empty()) {
    // No work to be done.
  } else {
    background_insert_into_accIndex_scheduled_ = true;
    env_->Schedule(&DBImpl::BGInsertIntoAccIndex, this, 2);
  }
}

void DBImpl::BGInsertIntoAccIndex(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallInsertIntoAccIndex();
}

void DBImpl::BackgroundCallInsertIntoAccIndex() {
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else if (!imm_list_->Empty()){
    BackgroundInsertIntoAccIndex();
  }  
  background_insert_into_accIndex_scheduled_ = false; // Test by 3/7
  MaybeScheduleInsertIntoAccIndex();
  // Maybe trigger the flush to SSD level0.
  MaybeScheduleNVMImmutableMemTableCompaction(); // Test by 3/6
}

void DBImpl::BackgroundInsertIntoAccIndex() {
  // STable* insertTable = imm_list_->Back();
  // STable::Table::Node* watch = insertTable->GetHead();
  // std::printf("insertTable:\n");
  // insertTable->Traversal();
  STable* stable = imm_list_->Back();
  // std::printf("BeforeBackgroundInsertIntoAccIndex: Table:%x, head:%x\n", imm_list_->Back(), imm_list_->Back()->GetHead());
  // imm_list_->Back()->Traversal();
  
  acc_index_->Merge(imm_list_->Back());
  // std::printf("BackgroundInsertIntoAccIndex: Table:%x, head:%x\n", imm_list_->Back(), imm_list_->Back()->GetHead());
  // imm_list_->Back()->Traversal();
  // For test
  // Slice key = insertTable->GetSmallestInternalKey();
  // std::printf("acc_index table num is: %d\n", acc_index_->Num());
  imm_list_->Pop();
  // std::printf("just test\n");
  // std::printf("insertTable %x 's head is %x\n", insertTable, insertTable->GetHead());
}

void DBImpl::MaybeScheduleBTableSplit() {
  // btable_mutex_.AssertHeld();
  if (background_split_btable_scheduled_) {

  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (!big_mem_->NeedToSplit()) {
    // No work to be done;
  } else {
    background_split_btable_scheduled_ = true;
    // Log(options_.info_log, "Set mem_scheduled true, push BGMemCompactWork to background thread...\n");
    env_->Schedule(&DBImpl::BGSplitBTable, this, 1);
  }
}

void DBImpl::BGSplitBTable(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallSplitBTable();
}

void DBImpl::BackgroundCallSplitBTable() {
  big_mem_->Lock();
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundSplitBTable(big_mem_, imm_list_);
    // BackgroundMemTableCompact();
  }
  background_split_btable_scheduled_ = false;
  // btable_mutex_.Unlock();
  // Log(options_.info_log, "Set mem_scheduled false, memCompaction is finished.\n");
  MaybeScheduleBTableSplit();
  big_mem_->Unlock();
  MaybeScheduleInsertIntoAccIndex(); // 3/6 test for keep only dram btable and immlist
  // MaybeScheduleNVMImmutableMemTableCompaction();
}

void DBImpl::BackgroundSplitBTable(BTable* b_mem, ImmutableSmallTableList* imm_list) {
  // S1: Split the BTable and mark the push table.
  b_mem->Split(imm_list);
  // For test 3/9
  // std::printf("nvm last push table:%x", imm_list->Front());
  // imm_list->Front()->Traversal();
  // std::printf("nvm imm number now is: %llu\n", imm_list->Num());
}

// The case of manual merging is not considered now, By JJia.
void DBImpl::MaybeScheduleMemTableCompaction() {
  mutex_.AssertHeld();
  if (background_compact_memtable_scheduled.load(std::memory_order_consume)) {
    
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr) {
    // No work to be done;
  } else {
    background_compact_memtable_scheduled.store(true, std::memory_order_release);
    // Log(options_.info_log, "Set mem_scheduled true, push BGMemCompactWork to background thread...\n");
    env_->Schedule(&DBImpl::BGMemCompactWork, this, 0);
  }
}

void DBImpl::BGMemCompactWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallMemCompact();
}

void DBImpl::BackgroundCallMemCompact() {
  // Log(options_.info_log, "Background MemCompact try to get mutex lock...\n");
  // const uint64_t start_micros = env_->NowMicros();
  // MutexLock l(&mutex_);
  mutex_.Lock();
  // wait_lock_time_ = env_->NowMicros() - start_micros;
  // Log(options_.info_log, "Start BackgroundCallMemCompact...\n");
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    CompactMemTable();
    // BackgroundMemTableCompact();
  }
  background_compact_memtable_scheduled.store(false);
  // Log(options_.info_log, "Set mem_scheduled false, memCompaction is finished.\n");
  MaybeScheduleMemTableCompaction();
  background_mem_compact_finished_signal_.SignalAll();
  // release_lock_time_ = env_->NowMicros();
  // std::printf("single compact memtable time is:%llu\n", release_lock_time_ - start_micros);
  // Log(options_.info_log, "Finished BackgroundCallMemCompact...\n");
  /*
  if (!imm_list_->Empty() && !background_nvm_immutable_memtable_scheduled) {
    MaybeScheduleNVMImmutableMemTableCompaction();
  }
  */
  mutex_.Unlock();
  big_mem_->Lock();
  if (big_mem_->NeedToSplit()) {
    MaybeScheduleBTableSplit();
  }
  big_mem_->Unlock();
}

// By JJia
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);
  // uint64_t start_micros = env_->NowMicros();
  // VersionEdit edit;
  // Version* base = versions_->current();
  // base->Ref();
  mutex_.Unlock();
  {
    big_mem_->Lock();
    WriteBTable(imm_, big_mem_);
    big_mem_->Unlock();
  }
  // uint64_t finish_writebtable = env_->NowMicros();
  // write_btable_time_ = finish_writebtable - start_micros;
  // Log(options_.info_log, "Big memtable memory size is:%llu\n", big_mem_->ApproximateMemoryUsage()); 
  mutex_.Lock();
  // wait_lock_time_ += env_->NowMicros() - finish_writebtable;
  // base->Unref();

  Status s = Status::OK();
  if (shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }
  /*
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    while (logAndApply_scheduled_.load()) {
      logAndApply_finished_signal_.Wait(); 
    }  
    logAndApply_scheduled_.store(true);
    // log_and_apply_mutex_.Lock();
    // Log(options_.info_log, "BackgroundMemCompact call LogAndApply");
    // logAndApply_scheduled_ = true;               
    s = versions_->LogAndApply(&edit, &mutex_);
    // log_and_apply_mutex_.Unlock();
    logAndApply_finished_signal_.SignalAll();
    logAndApply_scheduled_.store(false);
    // Log(options_.info_log, "BackgroundMemCompact finished LogAndApply");
  }
  */
  
  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    // Log(options_.info_log, "BackgroundMemCompact remove files...");
    RemoveLog();
    // Log(options_.info_log, "BackgroundMemCompact finished removing files...");
  } else {
    RecordBackgroundError(s);
  }
  // uint64_t end_micros = env_->NowMicros();
  // uint64_t compactmemtable_time = end_micros - start_micros;
  // std::printf("compactmem time is:%lld\n", compactmemtable_time);
  // std::printf("write btable time is:%lld\n", write_btable_time_);
}

// By JJia
void DBImpl::WriteBTable(MemTable* mem, BTable* b_mem) {
  // Log(options_.info_log, "Start flushing memtable to BTable...\n");
  Iterator* iter = mem->NewIterator();
  BTable::Table::Node* start = b_mem->GetHead();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    /* Track the wrong key. 3/13
    Slice interKey = iter->key();
    Slice userKey = ExtractUserKey(interKey);
    if (strcmp(userKey.ToString().data(), "XlVGXI1ZA7hnIdeb") == 0) {
      std::printf("XlVGXI1ZA7hnIdeb insert to btable\n");
    }
    */
    const char* key = mem->getKey(iter);
    size_t encoded_len = mem->getEncodedLength(iter);
    // std::printf("insert_num is:%lld\n", insert_num_);
    start = b_mem->Add(key, encoded_len, start);
  }
  delete iter;
  // Log(options_.info_log, "Flushing memtable to BTable finished.\n");
  
  if (imm_list_->Num() > nvm_imm_number_) {
    nvm_imm_number_ = imm_list_->Num();
    // Log(options_.info_log, "imm_list has %d small tables...\n", nvm_imm_number_);
  }
  
  if (b_mem->ApproximateMemoryUsage() > nvm_btable_size_) {
    nvm_btable_size_ = b_mem->ApproximateMemoryUsage(); 
  }
}


void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
    // if (!s.ok()) Log(options_.info_log, "FinishCompactionOutputFile error 1.\n");
  } else {
    // Log(options_.info_log, "FinishCompactionOutputFile error 2.\n");
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
    // if (!s.ok()) Log(options_.info_log, "FinishCompactionOutputFile error 3.\n");
  }
  if (s.ok()) {
    s = compact->outfile->Close();
    // if (!s.ok()) Log(options_.info_log, "FinishCompactionOutputFile error 4.\n");
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    // if (!s.ok()) Log(options_.info_log, "FinishCompactionOutputFile error 5.\n");
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  while (logAndApply_scheduled_.load()) {
    logAndApply_finished_signal_.Wait(); 
  }
  logAndApply_scheduled_.store(true);
  // log_and_apply_mutex_.Lock();
  // Log(options_.info_log, "BackgroundSSDCompact call LogAndApply");
  // logAndApply_scheduled_ = true;                  
  Status status = versions_->LogAndApply(compact->compaction->edit(), &mutex_);
  logAndApply_finished_signal_.SignalAll();
  logAndApply_scheduled_.store(false);
  // log_and_apply_mutex_.Unlock();
  return status;
  // return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  // If snapshots exist, only the old k/v data of sequenceNumber whose sequenceNumber value 
  // is smaller than that of the oldest snapshot can be deleted. Otherwise, all old kv data can be deleted.
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {

    if (has_imm_.load(std::memory_order_relaxed) && !background_compact_memtable_scheduled.load(std::memory_order_consume)) {  // Add by JJia, 12/2
      mutex_.Lock();
      if (!background_compact_memtable_scheduled.load(std::memory_order_consume)) {
        background_compact_memtable_scheduled.store(true, std::memory_order_release);
        // Log(options_.info_log, "DocompactionWork compact memtable start...\n");
        CompactMemTable();
        // Log(options_.info_log, "DocompactionWork compact memtable finished...\n");
        background_compact_memtable_scheduled.store(false, std::memory_order_release);
        background_mem_compact_finished_signal_.SignalAll();
        release_lock_time_ = env_->NowMicros();
      }
      mutex_.Unlock();
    }
    // Prioritize immutable compaction work
    /*
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }
    */
    // Check whether there are too many conflicts between the current output file and level+2 files.
    // If so, complete the current output file and generate a new output file
    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        // Log(options_.info_log, "FinishCompactionOutputFile error");
        break;
      }
    }

    // Handle key/value, add to state, etc.
    // Whether delete the key.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
    //This user_key is displayed for the first time
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        // The last_sequence_for_key value is set to the maximum value because the user_key that appears for the first time cannot be deleted
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          Log(options_.info_log, "OpenCompactionOutputFile error");
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          // Log(options_.info_log, "FinishCompactionOutputFile error");         
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
    // Log(options_.info_log, "BackgroundSSDCompact finished LogAndApply");
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  // VersionSet::LevelSummaryStorage tmp;
  // Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();

  // 根据last sequence设置latest snapshot，并收集所有的子iterator
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());  // >memtable
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());  // >immutable memtable
    imm_->Ref();
  }
  if (big_mem_ != nullptr) {  // >big memtable
    list.push_back(big_mem_->NewIterator());
    big_mem_->Ref();
  }
  /*
  if (imm_list_->Num() != 0) {  // >imm_list
    // 
    imm_list_->Ref();
  }
  */
  versions_->current()->AddIterators(options, &list);  // >current的所有sstable
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  // 注册清理机制
  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  // S1:Lock mutex to prevent concurrency. If option is specified, snapshot is attempted. Then increase the reference value of the memtable.
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();  // If snapshot is not specified, the last Sequence is taken.
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  // Add nvm_btable and nvm_smallTableList here. By JJia
  BTable* big_mem = big_mem_;
  ImmutableSmallTableList* imm_list = imm_list_;
  AccIndex* acc_index = acc_index_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  big_mem->Ref();
  imm_list->Ref();
  acc_index->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // S2:Unlock while reading from files and memtables.
  {
    mutex_.Unlock();  // test by JJia 11/27
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      read_in_dram_mem_++;
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      read_in_dram_imm_++;
    } else if (big_mem != nullptr && big_mem->Get(lkey, value, &s)) {
      read_in_nvm_btable_++;
    } else if (imm_list->Num() != 0 && imm_list->Get(lkey, value, &s)) {
      read_in_nvm_imm_list_++;
    } else if (!acc_index_->Empty() && acc_index_->Get(lkey, value, &s)) {
      read_in_acc_index_++;
    } else if (acc_index->HasPop()) {
      STable* stable = acc_index->GetPopTable();
      if (stable != nullptr && stable->Get(lkey, value, &s)) {
        read_in_pop_table_++;
      } else { // need to search for SSTable.
        s = current->Get(options, lkey, value, &stats);
        if(s.ok()) read_in_ssd_++;
        have_stat_update = true; // Write it down and use it in compaction.
      }
    } else { // need to search for SSTable.
      s = current->Get(options, lkey, value, &stats);
      if(s.ok()) read_in_ssd_++;
      have_stat_update = true; // Write it down and use it in compaction.
    }
    /*
    if(!s.ok()) {
      if (mem->Get(lkey, value, &s)) {
        read_in_dram_mem_++;
      } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
        read_in_dram_imm_++;
      } else if (big_mem != nullptr && big_mem->Get(lkey, value, &s)) {
        read_in_nvm_btable_++;
      } else if (imm_list->Num() != 0 && imm_list->Get(lkey, value, &s)) {
        read_in_nvm_imm_list_++;
      } else { // need to search for SSTable.
        s = current->Get(options, lkey, value, &stats);
        if(s.ok()) read_in_ssd_++;
        have_stat_update = true; // Write it down and use it in compaction.
      }
    } 
    */
    mutex_.Lock();
  }
  // S3:If a compaction occurs from an sstable file, check that every compaction needs to be performed. Finally, we subtract the reference count of the MemTable by 1.。
  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleSSDCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  big_mem->Unref();
  imm_list->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    //MaybeScheduleCompaction(); By JJia 11/3
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

// Changed by JJia
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  // S1:Initialize a writer object with options and updates 
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  // S2:Acquire the lock mutex and then push the writer into the queue.If the queue is not empty, the writer need to wait.
  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  } 
  if (w.done) {
    return w.status;
  }

  // S3:Prepare enough space for writing, may temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  // S4:Batch mutilple writes
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);
    // S5:Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();
    // S6:Set the version sequence number.
    versions_->SetLastSequence(last_sequence);
  }
  // S7:Pop the done writers.
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // S8:Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// Changed by JJia. 10/31
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      const uint64_t start_micros = env_->NowMicros();
      background_mem_compact_finished_signal_.Wait();

      // interval_stall_time_ += (env_->NowMicros() - start_micros);
      // wait_lock_time_ += (env_->NowMicros() - release_lock_time_);

      // std::printf("single interval stall time:%llu\n", env_->NowMicros() - start_micros);
      // std::printf("single write btable time:%llu\n", write_btable_time_);
      // std::printf("single wait lock time:%llu\n", wait_lock_time_);
    } 
//    else if (big_mem_->ApproximateMemoryUsage() > config::big_memtable_stop_size) {
//    Log(options_.info_log, "Big MemTable is filled; waiting...\n");
//    background_mem_compact_finished_signal_.Wait(); }
      else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      // 
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      prev_logfile_number_ = logfile_number_;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleMemTableCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  // S1:创建DBImpl对象，锁定并试图做recover操作。
  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  // S2:尝试恢复之前已经存在的数据库文件中的数据
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  // 如果Recover返回成功
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),    // 创建新的log文件
                                     &lfile);
    if (s.ok()) {    // 如果log文件创建成功，则根据log文件创建log::Writer
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->big_mem_ = new BTable(impl->internal_comparator_);
      impl->imm_list_ = new ImmutableSmallTableList(impl->internal_comparator_);  // By JJia
      impl->acc_index_ = new AccIndex(impl->internal_comparator_);
      impl->mem_->Ref();
      impl->big_mem_->Ref();
      impl->imm_list_->Ref();
      impl->acc_index_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }

  // S3 如果VersionSet::LogAndApply返回成功，则删除过期文件，检查是否需要执行compaction，最终返回创建的DBImpl对象。
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleSSDCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb