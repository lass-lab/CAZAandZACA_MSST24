// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "zbd_zenfs.h"

// #include "fs_zenfs.h"

namespace ROCKSDB_NAMESPACE {

// class ZenFS;
struct SSTBuffer{
  SSTBuffer(char* data,uint32_t size,bool positioned,uint64_t offset) : 
                          content_(data),size_(size),positioned_(positioned),offset_(offset) { }
  SSTBuffer() {}
  ~SSTBuffer(){ free(content_); }
  char* content_=nullptr;
  uint32_t size_=-1;
  bool positioned_;
  uint64_t offset_;
};
class ZoneFile;

class ZoneExtent {
 public:
  /*
  |
  |header    start                       pad
  |------------|---------------------------|---------|
  */
  volatile uint64_t start_;
  uint64_t length_;
  uint64_t pad_size_ = 0;
  Zone* zone_ = nullptr;
  std::mutex lock_;
  bool is_invalid_;
  bool position_pulled_ = false;
  std::string fname_;
  uint64_t header_size_;

  uint64_t last_accessed_;
  std::shared_ptr<char> page_cache_= nullptr;
  std::mutex page_cache_lock_ ; 
  ZoneFile* zfile_;
  explicit ZoneExtent(uint64_t start, uint64_t length, Zone* zone,std::string fname,uint64_t now_micros,ZoneFile* zfile);
  // to be push front
  explicit ZoneExtent(uint64_t start, uint64_t length, Zone* zone);
  explicit ZoneExtent(uint64_t start, uint64_t length, Zone* zone, std::string fname, uint64_t header_size,uint64_t now_micros,
  ZoneFile* zfile);
  static bool cmp(ZoneExtent* e1/*big*/, ZoneExtent* e2/*small*/)
  {
      return e1->start_ < e2->start_;
  }
  ~ZoneExtent(){
    // if(zone_ && zone_->GetZBD() && page_cache_ != nullptr && page_cache_.use_count()==1){

    //   zone_->GetZBD()->page_cache_size_ -=length_;
    //   // printf("After page cache %lu\n",zone_->GetZBD()->page_cache_size_.load());
    // }
    // printf("delete %p\n",page_cache_.get());
    page_cache_.reset();
  }
  Status DecodeFrom(Slice* input);
  void EncodeTo(std::string* output);
  void EncodeJson(std::ostream& json_stream);
  uint64_t PrintExtentInfo(bool print);

  static bool SortByLeastRecentlyUsed(ZoneExtent* ea, ZoneExtent* eb){
    // if(!ea){
    //   return true;
    // }
    // if(!eb){
    //   return false;
    // }
    if(ea->last_accessed_==eb->last_accessed_){
      return ea->fname_ > eb->fname_;
    }

    return ea->last_accessed_ < eb->last_accessed_;
  }
};



/* Interface for persisting metadata for files */
class MetadataWriter {
 public:
  virtual ~MetadataWriter();
  virtual IOStatus Persist(ZoneFile* zoneFile) = 0;
};

class ZoneFile {
 private:
  const uint64_t NO_EXTENT = 0xffffffffffffffff;

  ZonedBlockDevice* zbd_;

  

  std::vector<std::string> linkfiles_;
  Zone* active_zone_;
  uint64_t extent_start_ = NO_EXTENT;
  uint64_t extent_filepos_ = 0;

  Env::WriteLifeTimeHint lifetime_;
  IOType io_type_; /* Only used when writing */
  uint64_t file_size_;
  uint64_t file_id_;

  uint32_t nr_synced_extents_ = 0;
  bool open_for_wr_ = false;
  std::mutex open_for_wr_mtx_;

  time_t m_time_;
  bool is_sparse_ = false;
  bool is_deleted_ = false;
  
  MetadataWriter* metadata_writer_ = NULL;

  std::vector<SSTBuffer*> sst_buffers_;

  // std::mutex writer_mtx_;

  FileSystemWrapper* zenfs_;
 public:
  std::mutex writer_mtx_;
    std::atomic<int> readers_{0};
  // std::<atomic> uint64_t on_zc_{0};
  std::atomic<uint64_t> on_zc_{0};
  bool is_sst_ = false;
  bool is_wal_ = false;
  uint64_t fno_;
  uint64_t predicted_size_= 0;
  std::vector<uint64_t> input_fno_;
  Slice smallest_;
  Slice largest_;
  int level_;

  std::vector<ZoneExtent*> extents_;
  static const uint64_t SPARSE_HEADER_SIZE = 8;
  bool selected_as_input_ = false;

  uint64_t buffer_size_;

  ZoneFile(ZonedBlockDevice* zbd, uint64_t file_id_,
                    MetadataWriter* metadata_writer,FileSystemWrapper* zenfs);

  virtual ~ZoneFile();

  void AcquireWRLock();
  bool TryAcquireWRLock();
  void ReleaseWRLock();

  inline bool IsSST() { return is_sst_; }
  inline uint64_t GetAllocationScheme()  { return zbd_->GetAllocationScheme(); }
  IOStatus CloseWR();
  bool IsOpenForWR();

  IOStatus PersistMetadata();

  IOStatus Append(void* buffer, uint64_t data_size);
  IOStatus BufferedAppend(char** _data, uint64_t size);
  IOStatus SparseAppend(char** _data, uint64_t size);

  IOStatus CAZAAppend(const char* data, uint32_t size,bool positioned,uint64_t offset);
  std::vector<SSTBuffer*>* GetSSTBuffers(void) { return &sst_buffers_; }

  IOStatus SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime, int level =0);
  void SetIOType(IOType io_type);
  std::string GetFilename();
  time_t GetFileModificationTime();
  void SetFileModificationTime(time_t mt);
  uint64_t GetFileSize();
  void SetFileSize(uint64_t sz);
  void ClearExtents();

  uint64_t GetBlockSize() { return zbd_->GetBlockSize(); }
  ZonedBlockDevice* GetZbd() { return zbd_; }
  std::vector<ZoneExtent*> GetExtents() { return extents_; }
  Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return lifetime_; }

  IOStatus PositionedRead(uint64_t offset, size_t n,const IOOptions& ioptions ,Slice* result,
                          char* scratch, bool direct );
  ZoneExtent* GetExtent(uint64_t file_offset, uint64_t* dev_offset);
  void PushExtent();
  IOStatus AllocateNewZone(uint64_t min_capacity = 0);
  void EncodeTo(std::string* output, uint32_t extent_start);
  void EncodeUpdateTo(std::string* output) {
    EncodeTo(output, nr_synced_extents_);
  };
  void EncodeSnapshotTo(std::string* output) { EncodeTo(output, 0); };
  void EncodeJson(std::ostream& json_stream);
  void MetadataSynced() { nr_synced_extents_ = extents_.size(); };
  void MetadataUnsynced() { nr_synced_extents_ = 0; };

  IOStatus MigrateData(uint64_t offset, uint64_t length, Zone* target_zone,
                      std::shared_ptr<char> page_cache );

  Status DecodeFrom(Slice* input);
  Status MergeUpdate(std::shared_ptr<ZoneFile> update, bool replace);

  uint64_t GetID() { return file_id_; }

  bool IsSparse() { return is_sparse_; };

  void SetSparse(bool is_sparse) { is_sparse_ = is_sparse; };
  uint64_t HasActiveExtent() { return extent_start_ != NO_EXTENT; };
  uint64_t GetExtentStart() { return extent_start_; };

  IOStatus Recover();

  void ReplaceExtentList(std::vector<ZoneExtent*> new_list);
  void AddLinkName(const std::string& linkfile);
  IOStatus RemoveLinkName(const std::string& linkfile);
  IOStatus RenameLink(const std::string& src, const std::string& dest);
  uint32_t GetNrLinks() { return linkfiles_.size(); }
  const std::vector<std::string>& GetLinkFiles() const { return linkfiles_; }

 private:
  void ReleaseActiveZone();
  void SetActiveZone(Zone* zone);
  IOStatus CloseActiveZone();

 public:
  std::shared_ptr<ZenFSMetrics> GetZBDMetrics() { return zbd_->GetMetrics(); };
  IOType GetIOType() const { return io_type_; };
  bool IsDeleted() const { return is_deleted_; };
  void SetDeleted() { is_deleted_ = true; };
  IOStatus RecoverSparseExtents(uint64_t start, uint64_t end, Zone* zone);

 public:
  class ReadLock {
   public:
    ReadLock(ZoneFile* zfile) : zfile_(zfile) {
      // while(zfile->GetZbd()->GetZCRunning());
      zfile_->writer_mtx_.lock();
      zfile_->readers_++;
      zfile_->writer_mtx_.unlock();
    }
    ~ReadLock() { zfile_->readers_--; }

   private:
    ZoneFile* zfile_;
  };
  class WriteLock {
   public:
    WriteLock(ZoneFile* zfile) : zfile_(zfile) {
      // while(zfile->GetZbd()->GetZCRunning());
      zfile_->writer_mtx_.lock();
      while (zfile_->readers_ > 0) {
      }
    }
    ~WriteLock() { zfile_->writer_mtx_.unlock(); }

   private:
    ZoneFile* zfile_;
  };
};

class ZonedWritableFile : public FSWritableFile {
 public:
  explicit ZonedWritableFile(ZonedBlockDevice* zbd, bool buffered,
                             std::shared_ptr<ZoneFile> zoneFile);
  virtual ~ZonedWritableFile();

  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          IODebugContext* dbg) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& opts,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }
  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& options,
                                    IODebugContext* dbg) override;
  virtual IOStatus PositionedAppend(
      const Slice& data, uint64_t offset, const IOOptions& opts,
      const DataVerificationInfo& /* verification_info */,
      IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }
  virtual IOStatus Truncate(uint64_t size, const IOOptions& options,
                            IODebugContext* dbg) override;
  virtual IOStatus Close(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Flush(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  virtual IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                             const IOOptions& options,
                             IODebugContext* dbg) override;
  virtual IOStatus Fsync(const IOOptions& options,
                         IODebugContext* dbg) override;

  bool use_direct_io() const override { return !buffered; }
  bool IsSyncThreadSafe() const override { return true; };
  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }
  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;


  IOStatus CAZAFlushSST() override;
  
  void SetMinMaxKeyAndLevel(const Slice& s,const Slice& l,const int output_level) override;

  virtual Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return zoneFile_->GetWriteLifeTimeHint();
  }

 private:
  IOStatus BufferedWrite(char* data, uint32_t data_left);
  IOStatus BufferedWrite(const Slice& data);
  IOStatus FlushBuffer();
  IOStatus DataSync();
  IOStatus CloseInternal();

  bool buffered;
  char* sparse_buffer;
  char* buffer;
  size_t buffer_sz;
  uint64_t block_sz;
  uint64_t buffer_pos;
  uint64_t wp;
  int write_temp;
  bool open;

  std::shared_ptr<ZoneFile> zoneFile_;
  MetadataWriter* metadata_writer_;
  uint64_t default_buffer_size_;

  std::mutex buffer_mtx_;
};

class ZonedSequentialFile : public FSSequentialFile {
 private:
  std::shared_ptr<ZoneFile> zoneFile_;
  uint64_t rp;
  bool direct_;

 public:
  explicit ZonedSequentialFile(std::shared_ptr<ZoneFile> zoneFile,
                               const FileOptions& file_opts)
      : zoneFile_(zoneFile),
        rp(0),
        direct_(file_opts.use_direct_reads && !zoneFile->IsSparse()) {}

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
  IOStatus Skip(uint64_t n) override;

  bool use_direct_io() const override { return direct_; };

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return IOStatus::OK();
  }
};

class ZonedRandomAccessFile : public FSRandomAccessFile {
 private:
  std::shared_ptr<ZoneFile> zoneFile_;
  bool direct_;

 public:
  explicit ZonedRandomAccessFile(std::shared_ptr<ZoneFile> zoneFile,
                                 const FileOptions& file_opts)
      : zoneFile_(zoneFile),
        direct_(file_opts.use_direct_reads && !zoneFile->IsSparse()) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  bool use_direct_io() const override { return direct_; }

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return IOStatus::OK();
  }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
