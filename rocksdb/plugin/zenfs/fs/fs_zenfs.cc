// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include "fs_zenfs.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <mntent.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <iostream>
#include <set>
#include <sstream>
#include <utility>
#include <vector>
#include <sys/mman.h>
#include <cfloat>

#ifdef ZENFS_EXPORT_PROMETHEUS
#include "metrics_prometheus.h"
#endif
#include "rocksdb/utilities/object_registry.h"
#include "snapshot.h"
#include "util/coding.h"
#include "util/crc32c.h"

#define DEFAULT_ZENV_LOG_PATH "/tmp/"

namespace ROCKSDB_NAMESPACE {

Status Superblock::DecodeFrom(Slice* input) {
  if (input->size() != ENCODED_SIZE) {
    return Status::Corruption("ZenFS Superblock",
                              "Error: Superblock size missmatch");
  }

  GetFixed32(input, &magic_);
  memcpy(&uuid_, input->data(), sizeof(uuid_));
  input->remove_prefix(sizeof(uuid_));
  GetFixed32(input, &sequence_);
  GetFixed32(input, &superblock_version_);
  GetFixed32(input, &flags_);
  GetFixed32(input, &block_size_);
  GetFixed32(input, &zone_size_);
  GetFixed32(input, &nr_zones_);
  GetFixed32(input, &finish_treshold_);
  memcpy(&aux_fs_path_, input->data(), sizeof(aux_fs_path_));
  input->remove_prefix(sizeof(aux_fs_path_));
  memcpy(&zenfs_version_, input->data(), sizeof(zenfs_version_));
  input->remove_prefix(sizeof(zenfs_version_));
  memcpy(&reserved_, input->data(), sizeof(reserved_));
  input->remove_prefix(sizeof(reserved_));
  assert(input->size() == 0);

  if (magic_ != MAGIC)
    return Status::Corruption("ZenFS Superblock", "Error: Magic missmatch");
  if (superblock_version_ != CURRENT_SUPERBLOCK_VERSION) {
    return Status::Corruption(
        "ZenFS Superblock",
        "Error: Incompatible ZenFS on-disk format version, "
        "please migrate data or switch to previously used ZenFS version. "
        "See the ZenFS README for instructions.");
  }

  return Status::OK();
}

void Superblock::EncodeTo(std::string* output) {
  sequence_++; /* Ensure that this superblock representation is unique */
  output->clear();
  PutFixed32(output, magic_);
  output->append(uuid_, sizeof(uuid_));
  PutFixed32(output, sequence_);
  PutFixed32(output, superblock_version_);
  PutFixed32(output, flags_);
  PutFixed32(output, block_size_);
  PutFixed32(output, zone_size_);
  PutFixed32(output, nr_zones_);
  PutFixed32(output, finish_treshold_);
  output->append(aux_fs_path_, sizeof(aux_fs_path_));
  output->append(zenfs_version_, sizeof(zenfs_version_));
  output->append(reserved_, sizeof(reserved_));
  assert(output->length() == ENCODED_SIZE);
}

void Superblock::GetReport(std::string* reportString) {
  reportString->append("Magic:\t\t\t\t");
  PutFixed32(reportString, magic_);
  reportString->append("\nUUID:\t\t\t\t");
  reportString->append(uuid_);
  reportString->append("\nSequence Number:\t\t");
  reportString->append(std::to_string(sequence_));
  reportString->append("\nSuperblock Version:\t\t");
  reportString->append(std::to_string(superblock_version_));
  reportString->append("\nFlags [Decimal]:\t\t");
  reportString->append(std::to_string(flags_));
  reportString->append("\nBlock Size [Bytes]:\t\t");
  reportString->append(std::to_string(block_size_));
  reportString->append("\nZone Size [Blocks]:\t\t");
  reportString->append(std::to_string(zone_size_));
  reportString->append("\nNumber of Zones:\t\t");
  reportString->append(std::to_string(nr_zones_));
  reportString->append("\nFinish Threshold [%]:\t\t");
  reportString->append(std::to_string(finish_treshold_));
  reportString->append("\nGarbage Collection Enabled:\t");
  reportString->append(std::to_string(!!(flags_ & FLAGS_ENABLE_GC)));
  reportString->append("\nAuxiliary FS Path:\t\t");
  reportString->append(aux_fs_path_);
  reportString->append("\nZenFS Version:\t\t\t");
  std::string zenfs_version = zenfs_version_;
  if (zenfs_version.length() == 0) {
    zenfs_version = "Not Available";
  }
  reportString->append(zenfs_version);
}

Status Superblock::CompatibleWith(ZonedBlockDevice* zbd) {
  if (block_size_ != zbd->GetBlockSize())
    return Status::Corruption("ZenFS Superblock",
                              "Error: block size missmatch");
  if (zone_size_ != (zbd->GetZoneSize() / block_size_))
    return Status::Corruption("ZenFS Superblock", "Error: zone size missmatch");
  if (nr_zones_ > zbd->GetNrZones())
    return Status::Corruption("ZenFS Superblock",
                              "Error: nr of zones missmatch");

  return Status::OK();
}

IOStatus ZenMetaLog::AddRecord(const Slice& slice) {
  uint32_t record_sz = slice.size();
  const char* data = slice.data();
  size_t phys_sz;
  uint32_t crc = 0;
  char* buffer;
  int ret;
  IOStatus s;

  phys_sz = record_sz + zMetaHeaderSize;

  if (phys_sz % bs_) phys_sz += bs_ - phys_sz % bs_;

  assert(data != nullptr);
  assert((phys_sz % bs_) == 0);

  ret = posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), phys_sz);
  if (ret) return IOStatus::IOError("Failed to allocate memory");

  memset(buffer, 0, phys_sz);

  crc = crc32c::Extend(crc, (const char*)&record_sz, sizeof(uint32_t));
  crc = crc32c::Extend(crc, data, record_sz);
  crc = crc32c::Mask(crc);

  EncodeFixed32(buffer, crc);
  EncodeFixed32(buffer + sizeof(uint32_t), record_sz);
  memcpy(buffer + sizeof(uint32_t) * 2, data, record_sz);

  s = zone_->Append(buffer, phys_sz,true);

  free(buffer);
  return s;
}

IOStatus ZenMetaLog::Read(Slice* slice) {
  char* data = (char*)slice->data();
  size_t read = 0;
  size_t to_read = slice->size();
  int ret;

  if (read_pos_ >= zone_->wp_) {
    // EOF
    slice->clear();
    return IOStatus::OK();
  }

  if ((read_pos_ + to_read) > (zone_->start_ + zone_->max_capacity_)) {
    return IOStatus::IOError("Read across zone");
  }

  while (read < to_read) {
    ret = zbd_->Read(data + read, read_pos_, to_read - read, false);

    if (ret == -1 && errno == EINTR) continue;
    if (ret < 0) return IOStatus::IOError("Read failed");

    read += ret;
    read_pos_ += ret;
  }

  return IOStatus::OK();
}

IOStatus ZenMetaLog::ReadRecord(Slice* record, std::string* scratch) {
  Slice header;
  uint32_t record_sz = 0;
  uint32_t record_crc = 0;
  uint32_t actual_crc;
  IOStatus s;

  scratch->clear();
  record->clear();

  scratch->append(zMetaHeaderSize, 0);
  header = Slice(scratch->c_str(), zMetaHeaderSize);

  s = Read(&header);
  if (!s.ok()) return s;

  // EOF?
  if (header.size() == 0) {
    record->clear();
    return IOStatus::OK();
  }

  GetFixed32(&header, &record_crc);
  GetFixed32(&header, &record_sz);

  scratch->clear();
  scratch->append(record_sz, 0);

  *record = Slice(scratch->c_str(), record_sz);
  s = Read(record);
  if (!s.ok()) return s;

  actual_crc = crc32c::Value((const char*)&record_sz, sizeof(uint32_t));
  actual_crc = crc32c::Extend(actual_crc, record->data(), record->size());

  if (actual_crc != crc32c::Unmask(record_crc)) {
    return IOStatus::IOError("Not a valid record");
  }

  /* Next record starts on a block boundary */
  if (read_pos_ % bs_) read_pos_ += bs_ - (read_pos_ % bs_);

  return IOStatus::OK();
}

ZenFS::ZenFS(ZonedBlockDevice* zbd, std::shared_ptr<FileSystem> aux_fs,
             std::shared_ptr<Logger> logger)
    : FileSystemWrapper(aux_fs), zbd_(zbd), logger_(logger) {
  Info(logger_, "ZenFS initializing");
  Info(logger_, "ZenFS parameters: block device: %s, aux filesystem: %s",
       zbd_->GetFilename().c_str(), target()->Name());

  Info(logger_, "ZenFS initializing");
  next_file_id_ = 1;
  metadata_writer_.zenFS = this;
  zbd_->SetFSptr(this);
  printf("zenfs ptr %p\n",this);
  //   io_uring* read_ring_to_be_reap_[1000];
  // io_context_t* write_ioctx_to_be_reap_[1000];

  // memset(read_ring_to_be_reap_,0,sizeof(read_ring_to_be_reap_));
  // memset(write_ioctx_to_be_reap_,0,sizeof(write_ioctx_to_be_reap_));
  // printf("Reset scheme :: %d\n",reset_scheme_);

  


}

ZenFS::~ZenFS() {
  Status s;
  
  sleep(1);
  Info(logger_, "ZenFS shutting down");
  zbd_->LogZoneUsage();
  

  LogFiles();

  run_bg_partial_reset_worker_=false;
  run_bg_stats_worker_= false;
  if(bg_stats_worker_) {
    bg_stats_worker_->join();
  }

  run_gc_worker_ = false;


  zbd_->migrating_=false;
  zbd_->migrate_resource_.notify_one();
  // migrating_=false;

  if(bg_partial_reset_worker_){
    bg_partial_reset_worker_->join();
  }
  if(async_cleaner_worker_){
    async_cleaner_worker_->join();
  }
  if (gc_worker_) {
    gc_worker_->join();
  }
    // db_ptr_ = nullptr;





  std::cout << "FAR STAT 0 :: ZC Triggered Count : "
            << zc_triggerd_count_.load()
            << "\n";
  meta_log_.reset(nullptr);

  ClearFiles();
  zbd_->PrintAllStat();
  delete zbd_;
}

void ZenFS::RocksDBStatTimeLapse(void){
  // int num_not_flushed = 0;
  // int l0_files = 0;
  // uint64_t pending_compaction_bytes = 0;
  // if(db_ptr_==nullptr){
  //   return;
  // }
  // for(auto cfd : db_ptr_->GetVersionSet()->GetColumnFamilySet()){
  //   if(cfd->IsDropped()){
  //     continue;
  //   }
  //   num_not_flushed += cfd->imm()->NumNotFlushed();
  //   l0_files+=cfd->current()->storage_info()->l0_delay_trigger_count();
  //   pending_compaction_bytes+=cfd->current()->storage_info()->estimated_compaction_needed_bytes();
  // }
  return;
}

void ZenFS::BackgroundStatTimeLapse(){
  // int mt;
  // printf(" I am timelapse thread\n");

  // sleep(1);
  // files_mtx_.lock();
  // // for(auto f : files_){
  // //   if(f.second->is_sst_==false&& f.second->is_wal_==false){
  // //     DeleteFileNoLock(f.first, IOOptions(), nullptr);
  // //   }
  // // }
  // files_mtx_.unlock();

  while (run_bg_stats_worker_) {
    free_percent_ = zbd_->CalculateFreePercent();
    // for(int l = 0 ;l < 4 ;l++){
    //   printf("[%d] : %lf , %lu\n",l,zbd_->PredictCompactionScore(l),zbd_->lsm_tree_[l].load()>>20 );
    // }
    
    if(zbd_!=nullptr){
      zbd_->AddTimeLapse(mount_time_,cur_ops_);
    }
    sleep(1);
    mount_time_.fetch_add(1);
    // if(mount_time_.load()>4000){
    //   // exit(-1);
    // }
    // zbd_->PrintZoneMutexHolder();
    // printf("sec : %d\n",mt);
    // if(mt>100 && mt%3==0 ){
    //   zbd_->PrintZoneMutexHolder();
    // }
    
  }
  // printf(" I am timelapse thread bybye\n");
}
void ZenFS::PartialResetWorker(uint64_t T){
  // (void)(run_once);
  IOStatus s;
  uint32_t nr_zone = zbd_->GetNrZones();
  while(run_bg_partial_reset_worker_){
    // printf("hello worker?\n");
    std::vector<bool> is_reseted;
    is_reseted.assign(nr_zone,false);
    if(free_percent_<=T){
      s=zbd_->RuntimePartialZoneReset(is_reseted);
      if(!s.ok()){
          printf("BG Partial Reset Error@@@@@@@@@@\n");
        }
    }
    sleep(1);
    // if(run_once){
    //   return;
    // }
  }
}


size_t ZenFS::ZoneCleaning(bool forced){
  // uint64_t MODIFIED_ZC_KICKING_POINT=zbd_->GetZoneCleaningKickingPoint();
  size_t should_be_copied=0;
  (void)(forced);
  uint64_t page_cache_hit_size=0;
  // uint64_t zone_size;
  // uint64_t zone_per_erase_unit_ratio=(zbd_->GetEraseUnitSize()*100)/zone_size;
  // uint64_t erase_unit_size=zbd_->GetEraseUnitSize();
  int start = GetMountTime();
  // auto start_chrono = std::chrono::high_resolution_clock::now();
  struct timespec start_timespec, end_timespec;
  bool everything_in_page_cache = false;
  if(page_cache_hit_mmap_addr_==nullptr){
    io_zone_start_offset_ = zbd_->GetIOZoneByIndex(0)->start_;
    uint64_t device_total_size= 1<<30;
    device_total_size <<=log2_DEVICE_IO_CAPACITY;
    page_cache_hit_mmap_addr_ = 
    (char*)mmap(NULL, device_total_size, PROT_READ, MAP_SHARED, zbd_->GetFD(READ_FD), io_zone_start_offset_);
    page_size_=getpagesize();
    page_cache_check_hit_buffer_=(unsigned char*)malloc((zbd_->GetIOZoneByIndex(0)->max_capacity_/page_size_)+page_size_);
  }
  ZenFSSnapshot snapshot;
  ZenFSSnapshotOptions options;

  options.zone_ = 1;
  options.zone_file_ = 1;
  options.log_garbage_ = 1;
  // zbd_->EntireZoneReadLock();
  {
    // ZenFSStopWatch z2("GetZenFSSnapshot");
    GetZenFSSnapshot(snapshot, options);
  }
  // size_t all_inval_zone_n = 0;
  // std::vector<VictimZoneCandiate> victim_candidate;
  std::vector<std::pair<uint64_t, uint64_t>> victim_candidate;
  // std::set<uint64_t> migrate_zones_start;
  // std::vector<Zone*> migrate_zones_;
  std::vector<ZonedBlockDevice::ZoneReadLock> zone_read_locks;
  ZonedBlockDevice::ZoneReadLock zone_read_lock;
  
  uint64_t invalid_data_size = 0;
  uint64_t valid_data_size = 0;
  for(auto& z : snapshot.zones_){
   valid_data_size+=z.used_capacity; 
   invalid_data_size+=(z.wp-z.start - z.used_capacity);
  }

  
  uint64_t selected_victim_zone_start = 0;
  // uint64_t previous_mlock_addr = 0;
  // uint64_t average_gc_cost;

  // if(zbd_->PageCacheLimit()>1 && zbd_->PCAEnabled()){
  if(zbd_->PageCacheLimit()>1 && false){
    for(size_t i = 0; i < snapshot.extents_.size(); i++){
      ZoneExtentSnapshot* ext = &snapshot.extents_[i];
      uint64_t zidx = ext->zone_p->zidx_
                          -ZENFS_SPARE_ZONES-ZENFS_META_ZONES;
      
      // for(size_t j = 0 ; j < snapshot.zones_.size(); j++){
      //   if(snapshot.zones_[j].zidx==zidx){
      //     snapshot.zones_[j].extents_in_zone.push_back(ext);
      //     break;
      //   }
      // }
      snapshot.zones_[zidx].extents_in_zone.push_back(ext);
    }

      double min_gc_cost= DBL_MAX;
      
      for (const auto& zone : snapshot.zones_) {
        uint64_t size_mb_sum = 0;
        double gc_cost = 0.0;
        // if(zone.lock_held==false){
        //   continue;
        // }
        if(zone.capacity !=0 ){
          
          continue;
        }
        for(ZoneExtentSnapshot* ext : zone.extents_in_zone){
          uint64_t size_mb= (ext->length>>20);
          size_mb_sum+=size_mb;
          if(ext->page_cache == nullptr){
            // gc_cost+=zbd_->ReadDiskCost(size_mb);
            gc_cost+=zbd_->GetCost(READ_DISK_COST,size_mb);
          }else{
            gc_cost+=zbd_->GetCost(READ_PAGE_COST,size_mb);
          }
          

          // gc_cost *=(double)size_mb;
          // gc_cost+=zbd_->GetCost(WRITE_COST,size_mb);
          // gc_cost+=zbd_->GetCost(FREE_SPACE_COST,size_mb);
          

        }
        gc_cost+=zbd_->GetCost(WRITE_COST,size_mb_sum);

        uint64_t reclaimed_net_free_space = ((zone.max_capacity) >>20) - size_mb_sum;
        // gc_cost*=(double)(size_mb_sum);
        if(reclaimed_net_free_space==0){
          // zbd_->GetIOZone(zone.start)->Release();
          continue;
        }
        // printf("time cost : %lf /  reclaimed mb %lu = gc_cost %lf\n",
        //   gc_cost,reclaimed_net_free_space,gc_cost / (double)(reclaimed_net_free_space));
        gc_cost = gc_cost / (double)(reclaimed_net_free_space);
        // gc_cost*=(double)sqrt(size_mb_sum);

        // gc_cost+=zbd_->FreeSpaceCost(size_mb_sum);

        if(gc_cost<min_gc_cost){
          // gc_cost = min_gc_cost;
          // if(selected_victim_zone_start!=0){
          //   // zbd_->GetIOZone(selected_victim_zone_start)->Release();
          // }
          min_gc_cost=gc_cost;
          selected_victim_zone_start=zone.start;
          continue;
        }

        // zbd_->GetIOZone(zone.start)->Release();

    }
  }else{
    uint64_t min_gc_cost= UINT64_MAX;
    for (const auto& zone : snapshot.zones_) {
      if(zone.capacity !=0 ){
        continue;
      }
      if(zone.used_capacity*100/(zone.max_capacity-zone.is_finished)
        >=98){
        continue;
      }

      // if(zone.used_capacity>(zone.max_capacity*95)/100){
      //   continue;
      // }
      // if(zone.lock_held==false){
      //   continue;
      // }
      // if(zone.wp-zone.start==0){
      //   continue;
      // }
      uint64_t gc_cost=100 * zone.used_capacity / (zone.max_capacity);
      if(gc_cost<min_gc_cost){
        // if(selected_victim_zone_start!=0){
        //   zbd_->GetIOZone(selected_victim_zone_start)->Release();
        // }
        min_gc_cost=gc_cost;
        selected_victim_zone_start=zone.start;
        continue;
      }
      //  zbd_->GetIOZone(zone.start)->Release();
    }

    if(min_gc_cost>=99){
      return 0;
    }
  }


  // sort(victim_candidate.rbegin(), victim_candidate.rend());

  // sort(victim_candidate.begin(), victim_candidate.end());
  // uint64_t threshold = 0;
  // uint64_t reclaimed_zone_n=one_zc_reclaimed_zone_n_;


  // reclaimed_zone_n = reclaimed_zone_n > victim_candidate.size() ? victim_candidate.size() : reclaimed_zone_n;
  
  // for (size_t i = 0; (i < reclaimed_zone_n && migrate_zones_start.size()<reclaimed_zone_n ); i++) {
  //   if(victim_candidate[i].first>threshold){
  //     // should_be_copied+=(zone_size-(victim_candidate[i].first*zone_size/100) );
  //     migrate_zones_start.emplace(victim_candidate[i].second);
  //   }
  // }


  // if(migrate_zones_.empty()){
  //   return;
  // }
  
  std::vector<ZoneExtentSnapshot*> migrate_exts;
  // if(zbd_->PCAEnabled() && selected_victim_zone_start){
  if(false){
    // snapshot.zones_[zidx].extents_in_zone.push_back(ext);
    uint64_t zidx = zbd_->GetIOZone(selected_victim_zone_start)->zidx_
                        -ZENFS_SPARE_ZONES-ZENFS_META_ZONES;
    migrate_exts=snapshot.zones_[zidx].extents_in_zone;
    for(auto ext : migrate_exts){
      should_be_copied+=ext->length+ ext->header_size;
    }
  }else{
    for (auto& ext : snapshot.extents_) {
      if(selected_victim_zone_start==ext.zone_start){
        migrate_exts.push_back(&ext);
        should_be_copied+=ext.length + ext.header_size;
      }
    }
  }
  


  if (migrate_exts.size() > 0) {
    IOStatus s;
    Info(logger_, "Garbage collecting %d extents \n",
         (int)migrate_exts.size());

    // clock_gettime(CLOCK_MONOTONIC, &start_timespec);
    {    
        clock_gettime(CLOCK_MONOTONIC, &start_timespec);
        std::sort(migrate_exts.begin(),migrate_exts.end(),ZoneExtentSnapshot::SortByLBA);
        // if(zbd_->GetZoneSize() < (1<<29) ){ // SMR
        if(false ){ // SMR

          page_cache_hit_size = SMRLargeIOMigrateExtents(migrate_exts,should_be_copied,everything_in_page_cache);
        }else{
          page_cache_hit_size=MigrateExtents(migrate_exts);
        }

        // page_cache_hit_size = SMRLargeIOMigrateExtents(migrate_exts,should_be_copied,everything_in_page_cache);

        clock_gettime(CLOCK_MONOTONIC, &end_timespec);
    }

    
    if(!run_gc_worker_){
      zbd_->SetZCRunning(false);
      // zbd_->ZCorPartialUnLock();
      return 0;
    }

    if (!s.ok()) {
      Error(logger_, "Garbage collection failed");
    }
    int end=GetMountTime();
    if(should_be_copied>0){
      // auto elapsed = std::chrono::high_resolution_clock::now() - start_chrono;
      // long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
      // (void)(microseconds);
      // uint64_t invalid_data_size = 0;
      // uint64_t valid_data_size = 0;
      long elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
      zbd_->AddCumulativeIOBlocking(elapsed_ns_timespec);
      zbd_->AddZCTimeLapse(start, end,(elapsed_ns_timespec/1000),
                          1,should_be_copied, forced,
                          invalid_data_size,valid_data_size,page_cache_hit_size);
    }
    zc_triggerd_count_.fetch_add(1);
  }else{
    zbd_->SetZCRunning(false);
  }
    // if(selected_victim_zone_start!=0){
    //   zbd_->GetIOZone(selected_victim_zone_start)->Release();
    // }
    {
      // ZenFSStopWatch z2("ZC Large Reset",zbd_);
      // zbd_->ResetMultipleUnusedIOZones();
      zbd_->ResetUnusedIOZones();
    }
  // zbd_->SetZCRunning(false);
  // for(size_t i = 0; i<zone_read_locks.size();i++){
  //   // zone_read_lock.ReadLockZone(migrate_zones_[i]);
  //   zone_read_locks[i].ReadUnLockZone();
  // }
  // zbd_->EntireZoneReadUnLock();
  // 
  // for(auto z : migrate_zones_){
  //   z->Release();
  // }
  
  // zbd_->SetZCRunning(false);
  // zc_lock_.unlock();
  // zbd_->ExchangeSpareZone(migrate_zones_);

  // for(auto zone_start : migrate_zones_start){
  //   Zone* z =zbd_->GetIOZone(zone_start);
  //   if(z->Acquire()){
  //     z->Reset();
  //   }
  // }

// zbd_->ZCorPartialUnLock();
  // return migrate_zones_start.size() + all_inval_zone_n;
  return 1;
}

void ZenFS::AsyncZoneCleaning(void){
  size_t should_be_copied=0;
  // (void)(forced);
  // printf("AsyncZoneCleaning\n");

  // uint64_t zone_size;

  int start = GetMountTime();
  auto start_chrono = std::chrono::high_resolution_clock::now();
    struct timespec start_timespec, end_timespec;
  

  // zbd_->ZCorPartialLock();

  ZenFSSnapshot snapshot;
  ZenFSSnapshotOptions options;

  options.zone_ = 1;
  options.zone_file_ = 1;
  options.log_garbage_ = 1;
  // zbd_->EntireZoneReadLock();
  GetZenFSSnapshot(snapshot, options);
  
  size_t all_inval_zone_n = 0;
  std::vector<std::pair<uint64_t, uint64_t>> victim_candidate;
  std::set<uint64_t> migrate_zones_start;

  std::vector<ZonedBlockDevice::ZoneReadLock> zone_read_locks;
  ZonedBlockDevice::ZoneReadLock zone_read_lock;
  


  for (const auto& zone : snapshot.zones_) {
    // zone_size=zone.max_capacity;
    if(zone.capacity !=0 ){
      continue;
    }
    if(zone.used_capacity==zone.max_capacity){
      continue;
    }

    // if (zone.capacity == 0) { 
//  select:   
      uint64_t garbage_percent_approx =
        100 - 100 * zone.used_capacity / zone.max_capacity; // invalid capacity
        
      // uint64_t garbage_percent_approx=zone.max_capacity-zone.used_capacity;
      if(zone.used_capacity>0){ // valid copy zone
        // victim_candidate.push_back({garbage_percent_approx, zone.start});
        victim_candidate.emplace_back(garbage_percent_approx,zone.start);
      }
      else{ // no valid copy zone
        all_inval_zone_n++;
      }
    // }
  }
  // sort(victim_candidate.begin(), victim_candidate.end(),VictimZoneCandiate::cmp);
  sort(victim_candidate.rbegin(), victim_candidate.rend());

  uint64_t threshold = 0;
  uint64_t reclaimed_zone_n=one_zc_reclaimed_zone_n_;


  reclaimed_zone_n = reclaimed_zone_n > victim_candidate.size() ? victim_candidate.size() : reclaimed_zone_n;
  for (size_t i = 0; (i < reclaimed_zone_n && migrate_zones_start.size()<reclaimed_zone_n ); i++) {
    if(victim_candidate[i].first>threshold){
      // should_be_copied+=(zone_size-(victim_candidate[i].first*zone_size/100) );
      migrate_zones_start.emplace(victim_candidate[i].second);
    }
  }


  // if(migrate_zones_.empty()){
  //   return;
  // }
  
  std::vector<ZoneExtentSnapshot*> migrate_exts;
  for (auto& ext : snapshot.extents_) {
    if (migrate_zones_start.find(ext.zone_start) !=
        migrate_zones_start.end()) {
      migrate_exts.push_back(&ext);
    }
  }

  if (migrate_zones_start.size() > 0) {

    IOStatus s;

    Info(logger_, "Garbage collecting %d extents \n",
         (int)migrate_exts.size());
    
    
    clock_gettime(CLOCK_MONOTONIC, &start_timespec);
    // if(zbd_->AsyncZCEnabled()){
      should_be_copied = AsyncMigrateExtents(migrate_exts);
    // }
    
    clock_gettime(CLOCK_MONOTONIC, &end_timespec);


    if(!run_gc_worker_){
      zbd_->SetZCRunning(false);
      // zbd_->ZCorPartialUnLock();
      // return 0;
      return;
    }
    zc_triggerd_count_.fetch_add(1);
    if (!s.ok()) {
      Error(logger_, "Garbage collection failed");
    }
    int end=GetMountTime();
    if(should_be_copied>0){
      auto elapsed = std::chrono::high_resolution_clock::now() - start_chrono;
      
      long elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
      long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
      (void)(microseconds);
      (void)(elapsed_ns_timespec);
      (void)(end);
      (void)(start);
      // zbd_->AddZCTimeLapse(start, end,(elapsed_ns_timespec/1000),
      //                     migrate_zones_start.size(),should_be_copied, false);
    }
  }



  // zbd_->ZCorPartialUnLock();
}




void ZenFS::ZoneCleaningWorker(bool run_once) {
  uint64_t MODIFIED_ZC_KICKING_POINT=zbd_->GetZoneCleaningKickingPoint();
  uint64_t reclaim_until;
  if(zbd_->ProactiveZoneCleaning()){
    MODIFIED_ZC_KICKING_POINT+=10;
  }
  // int ret_ioprio=ioprio_set(IOPRIO_WHO_PROCESS,0,ZC_COMPACTION_IO_PRIORITY);
  // if(ret_ioprio){
  //   printf("ioprio_set error %d , %ld\n",ret_ioprio,ioprio_get(IOPRIO_WHO_PROCESS,0));
  // }
  // printf("ZC ioprio_set , %ld\n",ioprio_get(IOPRIO_WHO_PROCESS,0));
  
  (void) run_once;
  // bool force=false;
  // uint64_t before_free_percent;

  while (run_gc_worker_) {

    MODIFIED_ZC_KICKING_POINT=zbd_->GetZoneCleaningKickingPoint();
    reclaim_until=zbd_->GetReclaimUntil();
    // if(free_percent_<MODIFIED_ZC_KICKING_POINT+10
    //   && free_percent_>=MODIFIED_ZC_KICKING_POINT+5
    // ){
    //   zbd_->ResetUnusedIOZones();
      
    // }
    free_percent_ = zbd_->CalculateFreePercent();
    
    if(free_percent_>MODIFIED_ZC_KICKING_POINT+10){
      usleep(100 * 1000);
    }

    zbd_->SetZCRunning(false);
    uint64_t prev_zc_z= zbd_->reset_count_zc();
    if(free_percent_<MODIFIED_ZC_KICKING_POINT&&
        run_gc_worker_){ // IO BLOCK
      free_percent_ = zbd_->CalculateFreePercent();
      // force=false;
      
      (void)(reclaim_until);
      {
        zbd_->SetZCRunning(true);

        for(;
        // zbd_->GetFullZoneN()&&
        free_percent_< (reclaim_until) && run_gc_worker_;)
        {
          if(!zbd_->GetFullZoneN()&& free_percent_<reclaim_until){
              // if(!zbd_->FinishThereIsInvalidIOZone()){
              //   break;
              // }
              // zbd_->FinishCheapestIOZone(true);
          }
          if(!zbd_->GetFullZoneN()){
            break;
          }
          if(!ZoneCleaning(false)){
            break;
          }
          free_percent_ = zbd_->CalculateFreePercent();
          // force=(before_free_percent==free_percent_);

          if(prev_zc_z==zbd_->reset_count_zc()){
            break;
          }else{
            prev_zc_z=zbd_->reset_count_zc();
          }

        }
        // page_cache_mtx_.unlock();
      }
    }
    zbd_->SetZCRunning(false);
    
    // zbd_->ResetUnusedIOZones();
  }
}

IOStatus ZenFS::Repair() {
  std::map<std::string, std::shared_ptr<ZoneFile>>::iterator it;
  for (it = files_.begin(); it != files_.end(); it++) {
    std::shared_ptr<ZoneFile> zFile = it->second;
    if (zFile->HasActiveExtent()) {
      IOStatus s = zFile->Recover();
      if (!s.ok()) return s;
    }
  }

  return IOStatus::OK();
}

std::string ZenFS::FormatPathLexically(fs::path filepath) {
  fs::path ret = fs::path("/") / filepath.lexically_normal();
  return ret.string();
}

void ZenFS::LogFiles() {
  std::map<std::string, std::shared_ptr<ZoneFile>>::iterator it;
  uint64_t total_size = 0;

  Info(logger_, "  Files:\n");
  for (it = files_.begin(); it != files_.end(); it++) {
    std::shared_ptr<ZoneFile> zFile = it->second;
    std::vector<ZoneExtent*> extents = zFile->GetExtents();

    Info(logger_, "    %-45s sz: %lu lh: %d sparse: %u", it->first.c_str(),
         zFile->GetFileSize(), zFile->GetWriteLifeTimeHint(),
         zFile->IsSparse());
    for (unsigned int i = 0; i < extents.size(); i++) {
      ZoneExtent* extent = extents[i];
      Info(logger_, "          Extent %u {start=0x%lx, zone=%u, len=%lu} ", i,
           extent->start_,
           (uint32_t)(extent->zone_->start_ / zbd_->GetZoneSize()),
           extent->length_);

      total_size += extent->length_;
    }
  }
  Info(logger_, "Sum of all files: %lu MB of data \n",
       total_size / (1024 * 1024));
}

void ZenFS::ClearFiles() {
  std::map<std::string, std::shared_ptr<ZoneFile>>::iterator it;
  std::lock_guard<std::mutex> file_lock(files_mtx_);
  for (it = files_.begin(); it != files_.end(); it++) it->second.reset();
  files_.clear();
}

/* Assumes that files_mutex_ is held */
IOStatus ZenFS::WriteSnapshotLocked(ZenMetaLog* meta_log) {
  IOStatus s;
  std::string snapshot;

  EncodeSnapshotTo(&snapshot);
  s = meta_log->AddRecord(snapshot);
  if (s.ok()) {
    for (auto it = files_.begin(); it != files_.end(); it++) {
      std::shared_ptr<ZoneFile> zoneFile = it->second;
      zoneFile->MetadataSynced();
    }
  }
  return s;
}

IOStatus ZenFS::WriteEndRecord(ZenMetaLog* meta_log) {
  std::string endRecord;

  PutFixed32(&endRecord, kEndRecord);
  return meta_log->AddRecord(endRecord);
}

/* Assumes the files_mtx_ is held */
IOStatus ZenFS::RollMetaZoneLocked() {
  std::unique_ptr<ZenMetaLog> new_meta_log, old_meta_log;
  Zone* new_meta_zone = nullptr;
  IOStatus s;

  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_ROLL_LATENCY,
                                 Env::Default());
  zbd_->GetMetrics()->ReportQPS(ZENFS_ROLL_QPS, 1);

  IOStatus status = zbd_->AllocateMetaZone(&new_meta_zone);
  if (!status.ok()) return status;

  if (!new_meta_zone) {
    assert(false);  // TMP
    Error(logger_, "Out of metadata zones, we should go to read only now.");
    return IOStatus::NoSpace("Out of metadata zones");
  }

  Info(logger_, "Rolling to metazone %d\n", (int)new_meta_zone->GetZoneNr());
  new_meta_log.reset(new ZenMetaLog(zbd_, new_meta_zone));

  old_meta_log.swap(meta_log_);
  meta_log_.swap(new_meta_log);

  /* Write an end record and finish the meta data zone if there is space left */
  if (old_meta_log->GetZone()->GetCapacityLeft())
    WriteEndRecord(old_meta_log.get());
  if (old_meta_log->GetZone()->GetCapacityLeft())
    old_meta_log->GetZone()->Finish();

  std::string super_string;
  superblock_->EncodeTo(&super_string);

  s = meta_log_->AddRecord(super_string);
  if (!s.ok()) {
    Error(logger_,
          "Could not write super block when rolling to a new meta zone");
    return IOStatus::IOError("Failed writing a new superblock");
  }

  s = WriteSnapshotLocked(meta_log_.get());

  /* We've rolled successfully, we can reset the old zone now */
  if (s.ok()) old_meta_log->GetZone()->Reset();

  return s;
}

IOStatus ZenFS::PersistSnapshot(ZenMetaLog* meta_writer) {
  IOStatus s;

  std::lock_guard<std::mutex> file_lock(files_mtx_);
  std::lock_guard<std::mutex> metadata_lock(metadata_sync_mtx_);

  s = WriteSnapshotLocked(meta_writer);
  if (s == IOStatus::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZoneLocked();
  }

  if (!s.ok()) {
    Error(logger_,
          "Failed persisting a snapshot, we should go to read only now!");
  }

  return s;
}

IOStatus ZenFS::PersistRecord(std::string record) {
  IOStatus s;
  (void)(record);
  std::lock_guard<std::mutex> lock(metadata_sync_mtx_);
#if 0
  s = meta_log_->AddRecord(record);
  if (s == IOStatus::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZoneLocked();
    /* After a successfull roll, a complete snapshot has been persisted
     * - no need to write the record update */
  }
#endif
  return s;
}


void ZenFS::LargeIOSyncFileExtents(std::map<ZoneFile*,std::vector<ZoneExtent*>>& lock_acquired_files){
  
  std::vector<ZoneFile*> zfiles;
  for(auto file : lock_acquired_files){
    ZoneFile* zfile=file.first;
    std::vector<ZoneExtent*> new_extents = file.second;
    ZoneFile::WriteLock wrlock(zfile);
    for(size_t i = 0 ;i < new_extents.size(); i++){
      ZoneExtent* old_ext=zfile->extents_[i];
      

      // ZoneExtent* new_ext=new_extents[i];
      if(old_ext->start_!=new_extents[i]->start_){

        zfile->extents_[i]=new_extents[i];

        old_ext->zone_->used_capacity_.fetch_sub(old_ext->length_);
        delete old_ext;
      }else{
        zfile->extents_[i]->page_cache_ = std::move(new_extents[i]->page_cache_);
        delete new_extents[i];
      }
     
    }
    zfile->MetadataUnsynced();
    zfiles.push_back(zfile);

  }
 { 
ZenFSStopWatch z3("LARGE IO metadata sync",zbd_);  
  LargeZCSyncFileMetadata(zfiles);
}
// {
//   ZenFSStopWatch z2("ZC Large Reset",zbd_);
//   zbd_->ResetMultipleUnusedIOZones();
//   }
}

IOStatus ZenFS::SyncFileExtents(ZoneFile* zoneFile,
                                std::vector<ZoneExtent*> new_extents) {
  IOStatus s;
  ZoneExtent* old_ext;

  {
    ZoneFile::WriteLock wrlock(zoneFile);
    for (size_t i = 0; i < new_extents.size(); ++i) {
      if (zoneFile->extents_[i]->start_ != new_extents[i]->start_) {
        old_ext=zoneFile->extents_[i];
        zoneFile->extents_[i]=new_extents[i];


        old_ext->zone_->used_capacity_.fetch_sub(old_ext->length_);
        // old_ext->is_invalid_=true;
        // if(old_ext->zone_->used_capacity_==0&&old_ext->zone_->Acquire()){
        //   if(!old_ext->zone_->IsUsed()){
        //     old_ext->zone_->Reset();
        //     zbd_->AddEraseSizeZC();
        //   }
        //   old_ext->zone_->Release();
        // }
        delete old_ext;
      }
      else{
        zoneFile->extents_[i]->page_cache_ = std::move(new_extents[i]->page_cache_);
        delete new_extents[i];
      }
    }
  }

  zoneFile->MetadataUnsynced();
{
  ZenFSStopWatch z3("metadata sync",zbd_);  
  s = SyncFileMetadata(zoneFile, true);
}
  if (!s.ok()) {
    return s;
  }
{
  ZenFSStopWatch z4("ZC Small reset",zbd_);
  zbd_->ResetUnusedIOZones();
}
  return IOStatus::OK();
}

/* Must hold files_mtx_ */
IOStatus ZenFS::SyncFileMetadataNoLock(ZoneFile* zoneFile, bool replace) {


  std::string fileRecord;
  std::string output;
  IOStatus s;
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_META_SYNC_LATENCY,
                                 Env::Default());

  if (zoneFile->IsDeleted()) {
    Info(logger_, "File %s has been deleted, skip sync file metadata!",
         zoneFile->GetFilename().c_str());
    return IOStatus::OK();
  }

  if (replace) {
    PutFixed32(&output, kFileReplace);
  } else {
    zoneFile->SetFileModificationTime(time(0));
    PutFixed32(&output, kFileUpdate);
  }

  zoneFile->EncodeUpdateTo(&fileRecord);
  PutLengthPrefixedSlice(&output, Slice(fileRecord));
#if 0
  s = PersistRecord(output);
#endif
  if (s.ok()) zoneFile->MetadataSynced();

  return s;
}

void ZenFS::LargeZCSyncFileMetadata(std::vector<ZoneFile*>& zfiles){
  std::lock_guard<std::mutex> lock(files_mtx_);
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_META_SYNC_LATENCY,
                                 Env::Default());
  
  IOStatus s;
  std::string output;

  for(auto zfile : zfiles){
    std::string fileRecord;
    fileRecord.clear();
    if(zfile->IsDeleted()){
      continue;
    }
    PutFixed32(&output, kFileReplace);
    zfile->EncodeUpdateTo(&fileRecord);
    PutLengthPrefixedSlice(&output, Slice(fileRecord));
  }
  s = PersistRecord(output);
  if (s.ok()){ 
    for(auto zfile: zfiles){
      zfile->MetadataSynced();
    }
  }
}

IOStatus ZenFS::SyncFileMetadata(ZoneFile* zoneFile, bool replace) {
  (void)(zoneFile);
  (void)(replace);
  // std::lock_guard<std::mutex> lock(files_mtx_);
  // return IOStatus::OK();
  std::lock_guard<std::mutex> lock(files_mtx_);
  return SyncFileMetadataNoLock(zoneFile, replace);
}

/* Must hold files_mtx_ */
std::shared_ptr<ZoneFile> ZenFS::GetFileNoLock(std::string fname) {
  std::shared_ptr<ZoneFile> zoneFile(nullptr);
  fname = FormatPathLexically(fname);
  if (files_.find(fname) != files_.end()) {
    zoneFile = files_[fname];
  }
  return zoneFile;
}

std::shared_ptr<ZoneFile> ZenFS::GetFile(std::string fname) {
  std::shared_ptr<ZoneFile> zoneFile(nullptr);
  std::lock_guard<std::mutex> lock(files_mtx_);
  zoneFile = GetFileNoLock(fname);
  return zoneFile;
}

/* Must hold files_mtx_ */
IOStatus ZenFS::DeleteFileNoLock(std::string fname, const IOOptions& options,
                                 IODebugContext* dbg) {
  std::shared_ptr<ZoneFile> zoneFile(nullptr);
  IOStatus s;

  fname = FormatPathLexically(fname);
  zoneFile = GetFileNoLock(fname);

  if (zoneFile != nullptr) {
    if(zoneFile->on_zc_.load()){
      files_mtx_.unlock();
      // while(zoneFile->TryAcquireWRLock()==false);
      zoneFile->AcquireWRLock();
      files_mtx_.lock();
    }
    

    std::string record;

    files_.erase(fname);
    s = zoneFile->RemoveLinkName(fname);
    if (!s.ok()) return s;
    EncodeFileDeletionTo(zoneFile, &record, fname);
    s = PersistRecord(record);
    if (!s.ok()) {
      /* Failed to persist the delete, return to a consistent state */
      files_.insert(std::make_pair(fname.c_str(), zoneFile));
      zoneFile->AddLinkName(fname);
    } else {
      if (zoneFile->GetNrLinks() > 0) return s;
      /* Mark up the file as deleted so it won't be migrated by GC */
      // printf("\t\t\t\t%s : extent n : %lu\n",fname.c_str(),zoneFile->extents_.size());

      zoneFile->SetDeleted();
      zoneFile->ReleaseWRLock();
      zoneFile.reset();
    }
  } else {
    s = target()->DeleteFile(ToAuxPath(fname), options, dbg);
  }

  return s;
}

IOStatus ZenFS::NewSequentialFile(const std::string& filename,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSSequentialFile>* result,
                                  IODebugContext* dbg) {
  std::string fname = FormatPathLexically(filename);
  std::shared_ptr<ZoneFile> zoneFile = GetFile(fname);

  Debug(logger_, "New sequential file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_reads);

  if (zoneFile == nullptr) {
    return target()->NewSequentialFile(ToAuxPath(fname), file_opts, result,
                                       dbg);
  }

  result->reset(new ZonedSequentialFile(zoneFile, file_opts));
  return IOStatus::OK();
}

IOStatus ZenFS::NewRandomAccessFile(const std::string& filename,
                                    const FileOptions& file_opts,
                                    std::unique_ptr<FSRandomAccessFile>* result,
                                    IODebugContext* dbg) {
  std::string fname = FormatPathLexically(filename);
  std::shared_ptr<ZoneFile> zoneFile = GetFile(fname);

  Debug(logger_, "New random access file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_reads);

  if (zoneFile == nullptr) {
    return target()->NewRandomAccessFile(ToAuxPath(fname), file_opts, result,
                                         dbg);
  }

  result->reset(new ZonedRandomAccessFile(files_[fname], file_opts));
  return IOStatus::OK();
}



IOStatus ZenFS::NewWritableFile(const std::string& filename,
                                const FileOptions& file_opts,
                                std::unique_ptr<FSWritableFile>* result,
                                IODebugContext* /*dbg*/) {
  std::string fname = FormatPathLexically(filename);
  Debug(logger_, "New writable file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_writes);

  return OpenWritableFile(fname, file_opts, result, nullptr, false);
}

IOStatus ZenFS::ReuseWritableFile(const std::string& filename,
                                  const std::string& old_filename,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSWritableFile>* result,
                                  IODebugContext* dbg) {
  IOStatus s;
  std::string fname = FormatPathLexically(filename);
  std::string old_fname = FormatPathLexically(old_filename);
  Debug(logger_, "Reuse writable file: %s old name: %s\n", fname.c_str(),
        old_fname.c_str());

  if (GetFile(old_fname) == nullptr)
    return IOStatus::NotFound("Old file does not exist");

  /*
   * Delete the old file as it cannot be written from start of file
   * and create a new file with fname
   */
  s = DeleteFile(old_fname, file_opts.io_options, dbg);
  if (!s.ok()) {
    Error(logger_, "Failed to delete file %s\n", old_fname.c_str());
    return s;
  }

  return OpenWritableFile(fname, file_opts, result, dbg, false);
}

IOStatus ZenFS::FileExists(const std::string& filename,
                           const IOOptions& options, IODebugContext* dbg) {
  std::string fname = FormatPathLexically(filename);
  Debug(logger_, "FileExists: %s \n", fname.c_str());

  if (GetFile(fname) == nullptr) {
    return target()->FileExists(ToAuxPath(fname), options, dbg);
  } else {
    return IOStatus::OK();
  }
}

/* If the file does not exist, create a new one,
 * else return the existing file
 */
IOStatus ZenFS::ReopenWritableFile(const std::string& filename,
                                   const FileOptions& file_opts,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) {
  std::string fname = FormatPathLexically(filename);
  Debug(logger_, "Reopen writable file: %s \n", fname.c_str());

  return OpenWritableFile(fname, file_opts, result, dbg, true);
}

/* Must hold files_mtx_ */
void ZenFS::GetZenFSChildrenNoLock(const std::string& dir,
                                   bool include_grandchildren,
                                   std::vector<std::string>* result) {
  auto path_as_string_with_separator_at_end = [](fs::path const& path) {
    fs::path with_sep = path / fs::path("");
    return with_sep.lexically_normal().string();
  };

  auto string_starts_with = [](std::string const& string,
                               std::string const& needle) {
    return string.rfind(needle, 0) == 0;
  };

  std::string dir_with_terminating_seperator =
      path_as_string_with_separator_at_end(fs::path(dir));

  auto relative_child_path =
      [&dir_with_terminating_seperator](std::string const& full_path) {
        return full_path.substr(dir_with_terminating_seperator.length());
      };

  for (auto const& it : files_) {
    fs::path file_path(it.first);
    assert(file_path.has_filename());

    std::string file_dir =
        path_as_string_with_separator_at_end(file_path.parent_path());

    if (string_starts_with(file_dir, dir_with_terminating_seperator)) {
      if (include_grandchildren ||
          file_dir.length() == dir_with_terminating_seperator.length()) {
        result->push_back(relative_child_path(file_path.string()));
      }
    }
  }
}

/* Must hold files_mtx_ */
IOStatus ZenFS::GetChildrenNoLock(const std::string& dir_path,
                                  const IOOptions& options,
                                  std::vector<std::string>* result,
                                  IODebugContext* dbg) {
  std::vector<std::string> auxfiles;
  std::string dir = FormatPathLexically(dir_path);
  IOStatus s;

  Debug(logger_, "GetChildrenNoLock: %s \n", dir.c_str());

  s = target()->GetChildren(ToAuxPath(dir), options, &auxfiles, dbg);
  if (!s.ok()) {
    /* On ZenFS empty directories cannot be created, therefore we cannot
       distinguish between "Directory not found" and "Directory is empty"
       and always return empty lists with OK status in both cases. */
    if (s.IsNotFound()) {
      return IOStatus::OK();
    }
    return s;
  }

  for (const auto& f : auxfiles) {
    if (f != "." && f != "..") result->push_back(f);
  }

  GetZenFSChildrenNoLock(dir, false, result);

  return s;
}

IOStatus ZenFS::GetChildren(const std::string& dir, const IOOptions& options,
                            std::vector<std::string>* result,
                            IODebugContext* dbg) {
  std::lock_guard<std::mutex> lock(files_mtx_);
  return GetChildrenNoLock(dir, options, result, dbg);
}

/* Must hold files_mtx_ */
IOStatus ZenFS::DeleteDirRecursiveNoLock(const std::string& dir,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  std::vector<std::string> children;
  std::string d = FormatPathLexically(dir);
  IOStatus s;

  Debug(logger_, "DeleteDirRecursiveNoLock: %s aux: %s\n", d.c_str(),
        ToAuxPath(d).c_str());

  s = GetChildrenNoLock(d, options, &children, dbg);
  if (!s.ok()) {
    return s;
  }

  for (const auto& child : children) {
    std::string file_to_delete = (fs::path(d) / fs::path(child)).string();
    bool is_dir;

    s = IsDirectoryNoLock(file_to_delete, options, &is_dir, dbg);
    if (!s.ok()) {
      return s;
    }

    if (is_dir) {
      s = DeleteDirRecursiveNoLock(file_to_delete, options, dbg);
    } else {
      s = DeleteFileNoLock(file_to_delete, options, dbg);
    }
    if (!s.ok()) {
      return s;
    }
  }

  return target()->DeleteDir(ToAuxPath(d), options, dbg);
}

IOStatus ZenFS::DeleteDirRecursive(const std::string& d,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  IOStatus s;
  {
    std::lock_guard<std::mutex> lock(files_mtx_);
    s = DeleteDirRecursiveNoLock(d, options, dbg);
  }
  if (s.ok()){ 
    s=zbd_->RuntimeReset();
  }
  return s;
}

IOStatus ZenFS::OpenWritableFile(const std::string& filename,
                                 const FileOptions& file_opts,
                                 std::unique_ptr<FSWritableFile>* result,
                                 IODebugContext* dbg, bool reopen) {
  IOStatus s;
  std::string fname = FormatPathLexically(filename);
  bool resetIOZones = false;
  {
    std::lock_guard<std::mutex> file_lock(files_mtx_);
    std::shared_ptr<ZoneFile> zoneFile = GetFileNoLock(fname);
    // int start_time=mount_time_.load();
    /* if reopen is true and the file exists, return it */
    if (reopen && zoneFile != nullptr) {
      zoneFile->AcquireWRLock();
      result->reset(
          new ZonedWritableFile(zbd_, !file_opts.use_direct_writes, zoneFile));
      return IOStatus::OK();
    }

    if (zoneFile != nullptr) {
      s = DeleteFileNoLock(fname, file_opts.io_options, dbg);
      if (!s.ok()) return s;
      resetIOZones = true;
    }
    // while(zbd_->GetZCRunning()){
    //   // sleeping_time++;
    //   // sleep(1);
    //   if(mount_time_-start_time){
    //     break;
    //   }
    //   // if(sleeping_time>5){
    //   //   break;
    //   // }
    // }

    zoneFile =
        std::make_shared<ZoneFile>(zbd_, next_file_id_++, &metadata_writer_,this);
    zoneFile->SetFileModificationTime(time(0));
    zoneFile->AddLinkName(fname);

    /* RocksDB does not set the right io type(!)*/
    zoneFile->is_sst_=ends_with(fname,".sst");
    if (ends_with(fname, ".log")) {
      // printf("%s\n",fname.c_str());
      zoneFile->SetIOType(IOType::kWAL);
      // zoneFile->SetSparse(!file_opts.use_direct_writes);
      zoneFile->is_wal_=true;
    } else {
      
      zoneFile->SetIOType(IOType::kUnknown);
    }
    
    // printf("fname : %s\n",fname.c_str());
    /* Persist the creation of the file */
    s = SyncFileMetadataNoLock(zoneFile);
    if (!s.ok()) {
      zoneFile.reset();
      return s;
    }

    zoneFile->AcquireWRLock();
    files_.insert(std::make_pair(fname.c_str(), zoneFile));
    result->reset(
        new ZonedWritableFile(zbd_, !file_opts.use_direct_writes, zoneFile));
  }

  if (resetIOZones) {
    s = zbd_->RuntimeReset();    
  }

  return s;
}

IOStatus ZenFS::DeleteFile(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  IOStatus s;

  Debug(logger_, "DeleteFile: %s \n", fname.c_str());

  // while(files_mtx_.try_lock()==false);
  files_mtx_.lock();
  s = DeleteFileNoLock(fname, options, dbg);
  files_mtx_.unlock();
  if (s.ok()){
    s = zbd_->RuntimeReset();
    
  }
  zbd_->LogZoneStats();

  return s;
}

IOStatus ZenFS::GetFileModificationTime(const std::string& filename,
                                        const IOOptions& options,
                                        uint64_t* mtime, IODebugContext* dbg) {
  std::shared_ptr<ZoneFile> zoneFile(nullptr);
  std::string f = FormatPathLexically(filename);
  IOStatus s;

  Debug(logger_, "GetFileModificationTime: %s \n", f.c_str());
  std::lock_guard<std::mutex> lock(files_mtx_);
  if (files_.find(f) != files_.end()) {
    zoneFile = files_[f];
    *mtime = (uint64_t)zoneFile->GetFileModificationTime();
  } else {
    s = target()->GetFileModificationTime(ToAuxPath(f), options, mtime, dbg);
  }
  return s;
}

IOStatus ZenFS::GetFileSize(const std::string& filename,
                            const IOOptions& options, uint64_t* size,
                            IODebugContext* dbg) {
  std::shared_ptr<ZoneFile> zoneFile(nullptr);
  std::string f = FormatPathLexically(filename);
  IOStatus s;

  Debug(logger_, "GetFileSize: %s \n", f.c_str());

  std::lock_guard<std::mutex> lock(files_mtx_);
  if (files_.find(f) != files_.end()) {
    zoneFile = files_[f];
    *size = zoneFile->GetFileSize();
  } else {
    s = target()->GetFileSize(ToAuxPath(f), options, size, dbg);
  }
  // printf("%lu\n",*size);
  return s;
}

void ZenFS::SetResetScheme(uint32_t r,uint32_t partial_reset_scheme,uint64_t T,uint64_t zc,
                  uint64_t until,uint64_t allocation_scheme,std::vector<uint64_t>& other_options) {
  zbd_->SetResetScheme(r,partial_reset_scheme,T,zc,until,allocation_scheme,other_options);
  
  // if(gc_worker_!=nullptr){

  // }

  if(partial_reset_scheme==PARTIAL_RESET_AT_BACKGROUND|| 
      partial_reset_scheme==PARTIAL_RESET_BACKGROUND_T_WITH_ZONE_RESET  ){
      printf("PARTIAL RESET AT BG\n");
      if(bg_partial_reset_worker_==nullptr){
        run_bg_partial_reset_worker_=true;
        bg_partial_reset_worker_.reset(new std::thread(&ZenFS::PartialResetWorker, this,T));
      }
      return;
  }
  switch (partial_reset_scheme)
  {
  case RUNTIME_ZONE_RESET_DISABLED:
    printf("RUNTIME_ZONE_RESET_DISABLED\n");
    return;
  case RUNTIME_ZONE_RESET_ONLY:
    printf("RUNTIME_ZONE_RESET_ONLY\n");
    return;
  case PARTIAL_RESET_WITH_ZONE_RESET:
    printf("PARTIAL_RESET_WITH_ZONE_RESET\n");
    return;
  case PARTIAL_RESET_ONLY:
    printf("PARTIAL_RESET_ONLY\n");
    return;
  case PROACTIVE_ZONECLEANING:
    printf("PROACTIVE_ZONECLEANING\n");
    return;
  default:
    printf("UNKNOWN SCHEME!\n");
    // exit(-1);
    break;
  }

}

double ZenFS::GetMaxInvalidateCompactionScore(std::vector<uint64_t>& file_candidates,uint64_t * candidate_size) {
  // return file_candidates.size();
  // double ret = zbd_->GetMaxSameZoneScore(file_candidates);
  // // printf()
  // *candidate_size=1;
  // return ret;
  // (void)(candidate_size);
  return zbd_->GetMaxInvalidateCompactionScore(file_candidates,candidate_size,false);
}


/* Must hold files_mtx_ */
IOStatus ZenFS::RenameChildNoLock(std::string const& source_dir,
                                  std::string const& dest_dir,
                                  std::string const& child,
                                  const IOOptions& options,
                                  IODebugContext* dbg) {
  std::string source_child = (fs::path(source_dir) / fs::path(child)).string();
  std::string dest_child = (fs::path(dest_dir) / fs::path(child)).string();
  return RenameFileNoLock(source_child, dest_child, options, dbg);
}

/* Must hold files_mtx_ */
IOStatus ZenFS::RollbackAuxDirRenameNoLock(
    const std::string& source_path, const std::string& dest_path,
    const std::vector<std::string>& renamed_children, const IOOptions& options,
    IODebugContext* dbg) {
  IOStatus s;

  for (const auto& rollback_child : renamed_children) {
    s = RenameChildNoLock(dest_path, source_path, rollback_child, options, dbg);
    if (!s.ok()) {
      return IOStatus::Corruption(
          "RollbackAuxDirRenameNoLock: Failed to roll back directory rename");
    }
  }

  s = target()->RenameFile(ToAuxPath(dest_path), ToAuxPath(source_path),
                           options, dbg);
  if (!s.ok()) {
    return IOStatus::Corruption(
        "RollbackAuxDirRenameNoLock: Failed to roll back auxiliary path "
        "renaming");
  }

  return s;
}

/* Must hold files_mtx_ */
IOStatus ZenFS::RenameAuxPathNoLock(const std::string& source_path,
                                    const std::string& dest_path,
                                    const IOOptions& options,
                                    IODebugContext* dbg) {
  IOStatus s;
  std::vector<std::string> children;
  std::vector<std::string> renamed_children;

  s = target()->RenameFile(ToAuxPath(source_path), ToAuxPath(dest_path),
                           options, dbg);
  if (!s.ok()) {
    return s;
  }

  GetZenFSChildrenNoLock(source_path, true, &children);

  for (const auto& child : children) {
    s = RenameChildNoLock(source_path, dest_path, child, options, dbg);
    if (!s.ok()) {
      IOStatus failed_rename = s;
      s = RollbackAuxDirRenameNoLock(source_path, dest_path, renamed_children,
                                     options, dbg);
      if (!s.ok()) {
        return s;
      }
      return failed_rename;
    }
    renamed_children.push_back(child);
  }

  return s;
}

/* Must hold files_mtx_ */
IOStatus ZenFS::RenameFileNoLock(const std::string& src_path,
                                 const std::string& dst_path,
                                 const IOOptions& options,
                                 IODebugContext* dbg) {
  std::shared_ptr<ZoneFile> source_file(nullptr);
  std::shared_ptr<ZoneFile> existing_dest_file(nullptr);
  std::string source_path = FormatPathLexically(src_path);
  std::string dest_path = FormatPathLexically(dst_path);
  IOStatus s;

  Debug(logger_, "Rename file: %s to : %s\n", source_path.c_str(),
        dest_path.c_str());

  source_file = GetFileNoLock(source_path);
  if (source_file != nullptr) {
    existing_dest_file = GetFileNoLock(dest_path);
    if (existing_dest_file != nullptr) {
      s = DeleteFileNoLock(dest_path, options, dbg);
      if (!s.ok()) {
        return s;
      }
    }

    s = source_file->RenameLink(source_path, dest_path);
    if (!s.ok()) return s;
    files_.erase(source_path);

    files_.insert(std::make_pair(dest_path, source_file));

    s = SyncFileMetadataNoLock(source_file);
    if (!s.ok()) {
      /* Failed to persist the rename, roll back */
      files_.erase(dest_path);
      s = source_file->RenameLink(dest_path, source_path);
      if (!s.ok()) return s;
      files_.insert(std::make_pair(source_path, source_file));
    }
  } else {
    s = RenameAuxPathNoLock(source_path, dest_path, options, dbg);
  }

  return s;
}

IOStatus ZenFS::RenameFile(const std::string& source_path,
                           const std::string& dest_path,
                           const IOOptions& options, IODebugContext* dbg) {
  IOStatus s;
  {
    std::lock_guard<std::mutex> lock(files_mtx_);
    s = RenameFileNoLock(source_path, dest_path, options, dbg);
  }
  if (s.ok()){
      s = zbd_->RuntimeReset();
  }
  return s;
}

IOStatus ZenFS::LinkFile(const std::string& file, const std::string& link,
                         const IOOptions& options, IODebugContext* dbg) {
  std::shared_ptr<ZoneFile> src_file(nullptr);
  std::string fname = FormatPathLexically(file);
  std::string lname = FormatPathLexically(link);
  IOStatus s;

  Debug(logger_, "LinkFile: %s to %s\n", fname.c_str(), lname.c_str());
  {
    std::lock_guard<std::mutex> lock(files_mtx_);

    if (GetFileNoLock(lname) != nullptr)
      return IOStatus::InvalidArgument("Failed to create link, target exists");

    src_file = GetFileNoLock(fname);
    if (src_file != nullptr) {
      src_file->AddLinkName(lname);
      files_.insert(std::make_pair(lname, src_file));
      s = SyncFileMetadataNoLock(src_file);
      if (!s.ok()) {
        s = src_file->RemoveLinkName(lname);
        if (!s.ok()) return s;
        files_.erase(lname);
      }
      return s;
    }
  }
  s = target()->LinkFile(ToAuxPath(fname), ToAuxPath(lname), options, dbg);
  return s;
}

IOStatus ZenFS::NumFileLinks(const std::string& file, const IOOptions& options,
                             uint64_t* nr_links, IODebugContext* dbg) {
  std::shared_ptr<ZoneFile> src_file(nullptr);
  std::string fname = FormatPathLexically(file);
  IOStatus s;

  Debug(logger_, "NumFileLinks: %s\n", fname.c_str());
  {
    std::lock_guard<std::mutex> lock(files_mtx_);

    src_file = GetFileNoLock(fname);
    if (src_file != nullptr) {
      *nr_links = (uint64_t)src_file->GetNrLinks();
      return IOStatus::OK();
    }
  }
  s = target()->NumFileLinks(ToAuxPath(fname), options, nr_links, dbg);
  return s;
}

IOStatus ZenFS::AreFilesSame(const std::string& file, const std::string& linkf,
                             const IOOptions& options, bool* res,
                             IODebugContext* dbg) {
  std::shared_ptr<ZoneFile> src_file(nullptr);
  std::shared_ptr<ZoneFile> dst_file(nullptr);
  std::string fname = FormatPathLexically(file);
  std::string link = FormatPathLexically(linkf);
  IOStatus s;
  printf("ZenFS :: AreFilesSame\n");
  Debug(logger_, "AreFilesSame: %s, %s\n", fname.c_str(), link.c_str());

  {
    std::lock_guard<std::mutex> lock(files_mtx_);
    src_file = GetFileNoLock(fname);
    dst_file = GetFileNoLock(link);
    // printf("src_file %s %p, dst_file %s %p\n",fname.c_str(),src_file.get(),link.c_str(),dst_file.get());
    if (src_file != nullptr && dst_file != nullptr) {
      if (src_file->GetID() == dst_file->GetID()){
        printf("ZenFS :: AreFilesSame return true\n");
        *res = true;
      }
      else{
        printf("ZenFS :: AreFilesSame return false\n");
        *res = false;
      }
    
      return IOStatus::OK();
    }
    if(fname=="/rocksdbtest/dbbench/blob_dir"){
      *res=false;
      return IOStatus::OK();
    }
  }
  printf("ZenFS arfile same reach ehre\n");
  s = target()->AreFilesSame(fname, link, options, res, dbg);
  return s;
}

void ZenFS::EncodeSnapshotTo(std::string* output) {
  std::map<std::string, std::shared_ptr<ZoneFile>>::iterator it;
  std::string files_string;
  PutFixed32(output, kCompleteFilesSnapshot);
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string file_string;
    std::shared_ptr<ZoneFile> zFile = it->second;

    zFile->EncodeSnapshotTo(&file_string);
    PutLengthPrefixedSlice(&files_string, Slice(file_string));
  }
  PutLengthPrefixedSlice(output, Slice(files_string));
}

void ZenFS::EncodeJson(std::ostream& json_stream) {
  bool first_element = true;
  json_stream << "[";
  for (const auto& file : files_) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    file.second->EncodeJson(json_stream);
  }
  json_stream << "]";
}

Status ZenFS::DecodeFileUpdateFrom(Slice* slice, bool replace) {
  std::shared_ptr<ZoneFile> update(new ZoneFile(zbd_, 0, &metadata_writer_,this));
  uint64_t id;
  Status s;

  s = update->DecodeFrom(slice);
  if (!s.ok()) return s;

  id = update->GetID();
  if (id >= next_file_id_) next_file_id_ = id + 1;

  /* Check if this is an update or an replace to an existing file */
  for (auto it = files_.begin(); it != files_.end(); it++) {
    std::shared_ptr<ZoneFile> zFile = it->second;
    if (id == zFile->GetID()) {
      for (const auto& name : zFile->GetLinkFiles()) {
        if (files_.find(name) != files_.end())
          files_.erase(name);
        else
          return Status::Corruption("DecodeFileUpdateFrom: missing link file");
      }

      s = zFile->MergeUpdate(update, replace);
      update.reset();

      if (!s.ok()) return s;

      for (const auto& name : zFile->GetLinkFiles())
        files_.insert(std::make_pair(name, zFile));

      return Status::OK();
    }
  }

  /* The update is a new file */
  assert(GetFile(update->GetFilename()) == nullptr);
  files_.insert(std::make_pair(update->GetFilename(), update));

  return Status::OK();
}

Status ZenFS::DecodeSnapshotFrom(Slice* input) {
  Slice slice;

  assert(files_.size() == 0);

  while (GetLengthPrefixedSlice(input, &slice)) {
    std::shared_ptr<ZoneFile> zoneFile(
        new ZoneFile(zbd_, 0, &metadata_writer_,this));
    Status s = zoneFile->DecodeFrom(&slice);
    if (!s.ok()) return s;

    if (zoneFile->GetID() >= next_file_id_)
      next_file_id_ = zoneFile->GetID() + 1;

    for (const auto& name : zoneFile->GetLinkFiles())
      files_.insert(std::make_pair(name, zoneFile));
  }

  return Status::OK();
}

void ZenFS::EncodeFileDeletionTo(std::shared_ptr<ZoneFile> zoneFile,
                                 std::string* output, std::string linkf) {
  std::string file_string;

  PutFixed64(&file_string, zoneFile->GetID());
  PutLengthPrefixedSlice(&file_string, Slice(linkf));

  PutFixed32(output, kFileDeletion);
  PutLengthPrefixedSlice(output, Slice(file_string));
}

Status ZenFS::DecodeFileDeletionFrom(Slice* input) {
  uint64_t fileID;
  std::string fileName;
  Slice slice;
  IOStatus s;

  if (!GetFixed64(input, &fileID))
    return Status::Corruption("Zone file deletion: file id missing");

  if (!GetLengthPrefixedSlice(input, &slice))
    return Status::Corruption("Zone file deletion: file name missing");

  fileName = slice.ToString();
  if (files_.find(fileName) == files_.end())
    return Status::Corruption("Zone file deletion: no such file");

  std::shared_ptr<ZoneFile> zoneFile = files_[fileName];
  if (zoneFile->GetID() != fileID)
    return Status::Corruption("Zone file deletion: file ID missmatch");

  files_.erase(fileName);
  s = zoneFile->RemoveLinkName(fileName);
  if (!s.ok())
    return Status::Corruption("Zone file deletion: file links missmatch");

  return Status::OK();
}

Status ZenFS::RecoverFrom(ZenMetaLog* log) {
  bool at_least_one_snapshot = false;
  std::string scratch;
  uint32_t tag = 0;
  Slice record;
  Slice data;
  Status s;
  bool done = false;

  while (!done) {
    IOStatus rs = log->ReadRecord(&record, &scratch);
    if (!rs.ok()) {
      Error(logger_, "Read recovery record failed with error: %s",
            rs.ToString().c_str());
      return Status::Corruption("ZenFS", "Metadata corruption");
    }

    if (!GetFixed32(&record, &tag)) break;

    if (tag == kEndRecord) break;

    if (!GetLengthPrefixedSlice(&record, &data)) {
      return Status::Corruption("ZenFS", "No recovery record data");
    }

    switch (tag) {
      case kCompleteFilesSnapshot:
        ClearFiles();
        s = DecodeSnapshotFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode complete snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        at_least_one_snapshot = true;
        break;

      case kFileUpdate:
        s = DecodeFileUpdateFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      case kFileReplace:
        s = DecodeFileUpdateFrom(&data, true);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      case kFileDeletion:
        s = DecodeFileDeletionFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file deletion: %s",
               s.ToString().c_str());
          return s;
        }
        break;

      default:
        Warn(logger_, "Unexpected metadata record tag: %u", tag);
        return Status::Corruption("ZenFS", "Unexpected tag");
    }
  }

  if (at_least_one_snapshot)
    return Status::OK();
  else
    return Status::NotFound("ZenFS", "No snapshot found");
}

/* Mount the filesystem by recovering form the latest valid metadata zone */
Status ZenFS::Mount(bool readonly) {
  std::vector<Zone*> metazones = zbd_->GetMetaZones();
  std::vector<std::unique_ptr<Superblock>> valid_superblocks;
  std::vector<std::unique_ptr<ZenMetaLog>> valid_logs;
  std::vector<Zone*> valid_zones;
  std::vector<std::pair<uint32_t, uint32_t>> seq_map;

  Status s;

  /* We need a minimum of two non-offline meta data zones */
  if (metazones.size() < 2) {
    Error(logger_,
          "Need at least two non-offline meta zones to open for write");
    return Status::NotSupported();
  }

  /* Find all valid superblocks */
  for (const auto z : metazones) {
    std::unique_ptr<ZenMetaLog> log;
    std::string scratch;
    Slice super_record;

    if (!z->Acquire()) {
      assert(false);
      return Status::Aborted("Could not aquire busy flag of zone" +
                             std::to_string(z->GetZoneNr()));
    }

    // log takes the ownership of z's busy flag.
    log.reset(new ZenMetaLog(zbd_, z));

    if (!log->ReadRecord(&super_record, &scratch).ok()) continue;

    if (super_record.size() == 0) continue;

    std::unique_ptr<Superblock> super_block;

    super_block.reset(new Superblock());
    s = super_block->DecodeFrom(&super_record);
    if (s.ok()) s = super_block->CompatibleWith(zbd_);
    if (!s.ok()) return s;

    Info(logger_, "Found OK superblock in zone %lu seq: %u\n", z->GetZoneNr(),
         super_block->GetSeq());

    seq_map.push_back(std::make_pair(super_block->GetSeq(), seq_map.size()));
    valid_superblocks.push_back(std::move(super_block));
    valid_logs.push_back(std::move(log));
    valid_zones.push_back(z);
  }

  if (!seq_map.size()) return Status::NotFound("No valid superblock found");

  /* Sort superblocks by descending sequence number */
  std::sort(seq_map.begin(), seq_map.end(),
            std::greater<std::pair<uint32_t, uint32_t>>());

  bool recovery_ok = false;
  unsigned int r = 0;

  /* Recover from the zone with the highest superblock sequence number.
     If that fails go to the previous as we might have crashed when rolling
     metadata zone.
  */
  for (const auto& sm : seq_map) {
    uint32_t i = sm.second;
    std::string scratch;
    std::unique_ptr<ZenMetaLog> log = std::move(valid_logs[i]);

    s = RecoverFrom(log.get());
    if (!s.ok()) {
      if (s.IsNotFound()) {
        Warn(logger_,
             "Did not find a valid snapshot, trying next meta zone. Error: %s",
             s.ToString().c_str());
        continue;
      }

      Error(logger_, "Metadata corruption. Error: %s", s.ToString().c_str());
      return s;
    }

    r = i;
    recovery_ok = true;
    meta_log_ = std::move(log);
    break;
  }

  if (!recovery_ok) {
    return Status::IOError("Failed to mount filesystem");
  }

  Info(logger_, "Recovered from zone: %d", (int)valid_zones[r]->GetZoneNr());
  superblock_ = std::move(valid_superblocks[r]);
  zbd_->SetFinishTreshold(superblock_->GetFinishTreshold());

  IOOptions foo;
  IODebugContext bar;
  s = target()->CreateDirIfMissing(superblock_->GetAuxFsPath(), foo, &bar);
  if (!s.ok()) {
    Error(logger_, "Failed to create aux filesystem directory.");
    return s;
  }

  /* Free up old metadata zones, to get ready to roll */
  for (const auto& sm : seq_map) {
    uint32_t i = sm.second;
    /* Don't reset the current metadata zone */
    if (i != r) {
      /* Metadata zones are not marked as having valid data, so they can be
       * reset */
      valid_logs[i].reset();
    }
  }

  if (!readonly) {
    s = Repair();
    if (!s.ok()) return s;
  }

  if (readonly) {
    Info(logger_, "Mounting READ ONLY");
  } else {
    std::lock_guard<std::mutex> lock(files_mtx_);
    s = RollMetaZoneLocked();
    if (!s.ok()) {
      Error(logger_, "Failed to roll metadata zone.");
      return s;
    }
  }

  Info(logger_, "Superblock sequence %d", (int)superblock_->GetSeq());
  Info(logger_, "Finish threshold %u", superblock_->GetFinishTreshold());
  Info(logger_, "Filesystem mount OK");

  if (!readonly) {
    Info(logger_, "Resetting unused IO Zones..");
    IOStatus status = zbd_->ResetUnusedIOZones();
    if (!status.ok()) return status;
    Info(logger_, "  Done");

    if (superblock_->IsGCEnabled()) {
      Info(logger_, "Starting garbage collection worker");
      run_gc_worker_ = true;
      gc_worker_.reset(new std::thread(&ZenFS::ZoneCleaningWorker, this,false));

      

    }
    run_bg_stats_worker_=true;
    if(bg_stats_worker_==nullptr){
        bg_stats_worker_.reset(new std::thread(&ZenFS::BackgroundStatTimeLapse, this));
    }

    if(async_cleaner_worker_==nullptr){
      async_cleaner_worker_.reset(new std::thread(&ZenFS::BackgroundPageCacheEviction,this));
    }

  }

  LogFiles();

  return Status::OK();
}

Status ZenFS::MkFS(std::string aux_fs_p, uint32_t finish_threshold,
                   bool enable_gc) {
  std::vector<Zone*> metazones = zbd_->GetMetaZones();
  std::unique_ptr<ZenMetaLog> log;
  Zone* meta_zone = nullptr;
  std::string aux_fs_path = FormatPathLexically(aux_fs_p);
  IOStatus s;

  if (aux_fs_path.length() > 255) {
    return Status::InvalidArgument(
        "Aux filesystem path must be less than 256 bytes\n");
  }
  ClearFiles();
  IOStatus status = zbd_->ResetUnusedIOZones();
  if (!status.ok()) return status;

  for (const auto mz : metazones) {
    if (!mz->Acquire()) {
      assert(false);
      return Status::Aborted("Could not aquire busy flag of zone " +
                             std::to_string(mz->GetZoneNr()));
    }

    if (mz->Reset().ok()) {
      if (!meta_zone) meta_zone = mz;
    } else {
      Warn(logger_, "Failed to reset meta zone\n");
    }

    if (meta_zone != mz) {
      // for meta_zone == mz the ownership of mz's busy flag is passed to log.
      // if (!mz->Release()) {
      //   assert(false);
      //   return Status::Aborted("Could not unset busy flag of zone " +
      //                          std::to_string(mz->GetZoneNr()));
      // }
      mz->Release();
    }
  }

  if (!meta_zone) {
    return Status::IOError("Not available meta zones\n");
  }

  log.reset(new ZenMetaLog(zbd_, meta_zone));

  Superblock super(zbd_, aux_fs_path, finish_threshold, enable_gc);
  std::string super_string;
  super.EncodeTo(&super_string);

  s = log->AddRecord(super_string);
  if (!s.ok()) return std::move(s);

  /* Write an empty snapshot to make the metadata zone valid */
  s = PersistSnapshot(log.get());
  if (!s.ok()) {
    Error(logger_, "Failed to persist snapshot: %s", s.ToString().c_str());
    return Status::IOError("Failed persist snapshot");
  }

  Info(logger_, "Empty filesystem created");
  return Status::OK();
}

std::map<std::string, Env::WriteLifeTimeHint> ZenFS::GetWriteLifeTimeHints() {
  std::map<std::string, Env::WriteLifeTimeHint> hint_map;

  for (auto it = files_.begin(); it != files_.end(); it++) {
    std::shared_ptr<ZoneFile> zoneFile = it->second;
    std::string filename = it->first;
    hint_map.insert(std::make_pair(filename, zoneFile->GetWriteLifeTimeHint()));
  }

  return hint_map;
}

#if !defined(NDEBUG) || defined(WITH_TERARKDB)
static std::string GetLogFilename(std::string bdev) {
  std::ostringstream ss;
  time_t t = time(0);
  struct tm* log_start = std::localtime(&t);
  char buf[40];

  std::strftime(buf, sizeof(buf), "%Y-%m-%d_%H:%M:%S.log", log_start);
  ss << DEFAULT_ZENV_LOG_PATH << std::string("zenfs_") << bdev << "_" << buf;

  return ss.str();
}
#endif

Status NewZenFS(FileSystem** fs, const std::string& bdevname,
                std::shared_ptr<ZenFSMetrics> metrics) {
  return NewZenFS(fs, ZbdBackendType::kBlockDev, bdevname, metrics);
}

Status NewZenFS(FileSystem** fs, const ZbdBackendType backend_type,
                const std::string& backend_name,
                std::shared_ptr<ZenFSMetrics> metrics) {
  std::shared_ptr<Logger> logger;
  Status s;

  // TerarkDB needs to log important information in production while ZenFS
  // doesn't (currently).
  //
  // TODO(guokuankuan@bytedance.com) We need to figure out how to reuse
  // RocksDB's logger in the future.
#if !defined(NDEBUG) || defined(WITH_TERARKDB)
  s = Env::Default()->NewLogger(GetLogFilename(backend_name), &logger);
  if (!s.ok()) {
    fprintf(stderr, "ZenFS: Could not create logger");
  } else {
    logger->SetInfoLogLevel(DEBUG_LEVEL);
#ifdef WITH_TERARKDB
    logger->SetInfoLogLevel(INFO_LEVEL);
#endif
  }
#endif

  ZonedBlockDevice* zbd =
      new ZonedBlockDevice(backend_name, backend_type, logger, metrics);
  IOStatus zbd_status = zbd->Open(false, true);
  if (!zbd_status.ok()) {
    Error(logger, "mkfs: Failed to open zoned block device: %s",
          zbd_status.ToString().c_str());
    return Status::IOError(zbd_status.ToString());
  }

  ZenFS* zenFS = new ZenFS(zbd, FileSystem::Default(), logger);
  s = zenFS->Mount(false);
  if (!s.ok()) {
    delete zenFS;
    return s;
  }

  *fs = zenFS;
  return Status::OK();
}

Status AppendZenFileSystem(
    std::string path, ZbdBackendType backend,
    std::map<std::string, std::pair<std::string, ZbdBackendType>>& fs_map) {
  std::unique_ptr<ZonedBlockDevice> zbd{
      new ZonedBlockDevice(path, backend, nullptr)};
  IOStatus zbd_status = zbd->Open(true, false);

  if (zbd_status.ok()) {
    std::vector<Zone*> metazones = zbd->GetMetaZones();
    std::string scratch;
    Slice super_record;
    Status s;

    for (const auto z : metazones) {
      Superblock super_block;
      std::unique_ptr<ZenMetaLog> log;
      if (!z->Acquire()) {
        return Status::Aborted("Could not aquire busy flag of zone" +
                               std::to_string(z->GetZoneNr()));
      }
      log.reset(new ZenMetaLog(zbd.get(), z));

      if (!log->ReadRecord(&super_record, &scratch).ok()) continue;
      s = super_block.DecodeFrom(&super_record);
      if (s.ok()) {
        /* Map the uuid to the device-mapped (i.g dm-linear) block device to
           avoid trying to mount the whole block device in case of a split
           device */
        if (fs_map.find(super_block.GetUUID()) != fs_map.end() &&
            fs_map[super_block.GetUUID()].first.rfind("dm-", 0) == 0) {
          break;
        }
        fs_map[super_block.GetUUID()] = std::make_pair(path, backend);
        break;
      }
    }
  }
  return Status::OK();
}

Status ListZenFileSystems(
    std::map<std::string, std::pair<std::string, ZbdBackendType>>& out_list) {
  std::map<std::string, std::pair<std::string, ZbdBackendType>> zenFileSystems;

  auto closedirDeleter = [](DIR* d) {
    if (d != nullptr) closedir(d);
  };
  std::unique_ptr<DIR, decltype(closedirDeleter)> dir{
      opendir("/sys/class/block"), std::move(closedirDeleter)};
  struct dirent* entry;

  while (NULL != (entry = readdir(dir.get()))) {
    if (entry->d_type == DT_LNK) {
      Status status =
          AppendZenFileSystem(std::string(entry->d_name),
                              ZbdBackendType::kBlockDev, zenFileSystems);
      if (!status.ok()) return status;
    }
  }

  struct mntent* mnt = NULL;
  FILE* file = NULL;

  file = setmntent("/proc/mounts", "r");
  if (file != NULL) {
    while ((mnt = getmntent(file)) != NULL) {
      if (!strcmp(mnt->mnt_type, "zonefs")) {
        Status status = AppendZenFileSystem(
            std::string(mnt->mnt_dir), ZbdBackendType::kZoneFS, zenFileSystems);
        if (!status.ok()) return status;
      }
    }
  }

  out_list = std::move(zenFileSystems);
  return Status::OK();
}

void ZenFS::GetZenFSSnapshot(ZenFSSnapshot& snapshot,
                             const ZenFSSnapshotOptions& options) {
  
  if (options.zbd_) {
    snapshot.zbd_ = ZBDSnapshot(*zbd_);
  }
  if (options.zone_) {
    zbd_->GetZoneSnapshot(snapshot.zones_);
  }
  // printf("GetZenFSSnapshot @@\n");
  if (options.zone_file_) {
    std::lock_guard<std::mutex> file_lock(files_mtx_);




    for (const auto& file_it : files_) {
      ZoneFile& file = *(file_it.second);

      /* Skip files open for writing, as extents are being updated */
      if (!file.TryAcquireWRLock()) continue;
      // if(file.is_sst_==false&&file.is_wal_==false){
      //   // files_.erase(file_it);
      //   delete file_it.second.get();
      //   continue;
      // }
      // file -> extents mapping
      snapshot.zone_files_.emplace_back(file);
      // extent -> file mapping
      std::vector<ZoneExtent*> extents=file.GetExtents();



      for (ZoneExtent* ext : extents ) {
        if(ext->zone_==nullptr){
            ext->zone_=zbd_->GetIOZone(ext->start_);
        }
        if(ext->is_invalid_==false){

          ZoneExtentSnapshot ext_snapshot;
          ext_snapshot.start=ext->start_;
          ext_snapshot.length=ext->length_;
          ext_snapshot.zone_start=ext->zone_->start_;
          ext_snapshot.filename=file.GetFilename();
          ext_snapshot.header_size=ext->header_size_;
          ext_snapshot.zone_p=ext->zone_;
          // printf("GetZenFSSnapshot :: ext->start %lu\n",ext->start_);


          // if( ext->page_cache_!=nullptr){

          // }
          ext_snapshot.page_cache=ext->page_cache_;
          snapshot.extents_.push_back(ext_snapshot);
        }
      }

      file.ReleaseWRLock();
    }
  }
  // printf("After page cache loaded  :%lu\n",page_cache_size);
//  printf("GetZenFSSnapshot return @@\n");
  // if (options.trigger_report_) {
  //   zbd_->GetMetrics()->ReportSnapshot(snapshot);
  // }
  // 
  // if (options.log_garbage_) {
  //   zbd_->LogGarbageInfo();
  // }
}


uint64_t ZenFS::AsyncUringMigrateExtents(const std::vector<ZoneExtentSnapshot*>& extents){

  uint64_t ret = 0;
  std::map<std::string, std::vector<ZoneExtentSnapshot*>> file_extents;
  std::map<std::string, bool> migration_done;
  std::map<std::string, std::vector<AsyncZoneCleaningIocb*>> reaped_read_file_extents;
  std::vector<AsyncZoneCleaningIocb*> to_be_freed;
  std::vector<std::thread*> writer_thread_pool;
        
  int read_reaped_n = 0;
  // io_context_t read_ioctx;
  // io_context_t ctx;
  // io_context_t read_ioctx = 0;


  int extent_n = (int)extents.size();
  int read_fd=zbd_->GetFD(READ_FD);
  // int err=io_queue_init(extent_n,&read_ioctx);
    

  io_uring read_ring;
  unsigned flags = IORING_SETUP_SQPOLL;
  // unsigned flags = 0;
  int err=io_uring_queue_init(extent_n, &read_ring, flags);




  if(err){
    printf("\t\t\tio_uring_queue_init error@@@@@ %d %d\n",err,extent_n);
  }
  //  struct iocb* iocb_arr[extent_n];
  //  memset(iocb_arr, 0, sizeof(iocb_arr));
  // struct iocb* iocb_arr[extent_n];
  // int index = 0;

  // throw all read
  for (auto* ext : extents) {
    
   


    struct AsyncZoneCleaningIocb* async_zc_read_iocb = 
          new AsyncZoneCleaningIocb(ext->filename,ext->start,ext->length,ext->header_size);
    to_be_freed.push_back(async_zc_read_iocb);

    ret+=async_zc_read_iocb->length_+async_zc_read_iocb->header_size_;
    

    struct io_uring_sqe *sqe = io_uring_get_sqe(&read_ring);
    io_uring_sqe_set_data(sqe,async_zc_read_iocb);
    io_uring_prep_read(sqe,read_fd,async_zc_read_iocb->buffer_,
                      async_zc_read_iocb->length_+async_zc_read_iocb->header_size_,
                      async_zc_read_iocb->start_-async_zc_read_iocb->header_size_);
    // io_prep_pread(&(async_zc_read_iocb->iocb_), read_fd, async_zc_read_iocb->buffer_, 
    //     (async_zc_read_iocb->length_+async_zc_read_iocb->header_size_), 
    //     (async_zc_read_iocb->start_-async_zc_read_iocb->header_size_));
    // async_zc_read_iocb->iocb_.data=async_zc_read_iocb;
    // iocb_arr[index]=&(async_zc_read_iocb->iocb_);
    // iocb_arr[to_be_freed.size()-1]=&(async_zc_read_iocb->iocb_);
    // struct iocb* iocb= (async_zc_read_iocb->iocb_);
    // err=io_submit(read_ioctx,1,&(iocb));
    // if(err!=1){
    //   printf("io submit err? %d\n",err);
    // }

    io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);
    err=io_uring_submit(&read_ring);
    if(err==-errno){
      printf("io_uring_submit err? %d\n",err);
    }
    // index++;
    file_extents[ext->filename].emplace_back(ext);
    migration_done[ext->filename]= false;
  }

    // err=io_submit(read_ioctx,extent_n,iocb_arr);
    



  // reap here
  // struct io_event read_events[extent_n];
  //  = new io_event[extent_n];
  while(read_reaped_n < extent_n){



    // int num_events;
    // struct timespec timeout;
    // timeout.tv_sec = 0;
    // timeout.tv_nsec = 10000;

    // // int reap_min_nr ; 

    // num_events = io_getevents(read_ioctx, 1, extent_n, read_events,
    //                           &timeout);
    // 
    struct io_uring_cqe* cqe = nullptr;
    // int result = io_uring_wait_cqe(&read_ring, &cqe);
    int result = io_uring_peek_cqe(&read_ring, &cqe);
    if(result!=0){
      continue;
    }


    AsyncZoneCleaningIocb* reaped_read_iocb=reinterpret_cast<AsyncZoneCleaningIocb*>(cqe->user_data);
    // reap.r
    io_uring_cqe_seen(&read_ring,cqe);
    reaped_read_file_extents[reaped_read_iocb->filename_].emplace_back(reaped_read_iocb);
    read_reaped_n++;
    // for (int i = 0; i < num_events; i++) {
    //   struct io_event event = read_events[i];
    //   AsyncZoneCleaningIocb* reaped_read_iocb = static_cast<AsyncZoneCleaningIocb*>(event.data);
    //   reaped_read_file_extents[reaped_read_iocb->filename_].emplace_back(reaped_read_iocb);
    //   read_reaped_n++;
    // }


    // throw write
    for (const auto& it : file_extents) {
      if( migration_done[it.first.c_str()]==false &&
        it.second.size() == reaped_read_file_extents[it.first.c_str()].size() ){

        file_extents[it.first.c_str()].clear();
        migration_done[it.first.c_str()]=true;
        // writer_thread_pool.push_back(
        //   new std::thread(&ZenFS::AsyncUringMigrateFileExtentsWorker,this,
        //       it.first, &reaped_read_file_extents[it.first.c_str()]  )
        //   );
        // if(zbd_->AsyncZCEnabled()>=2){ //single thread
        //   AsyncMigrateFileExtentsWriteWorker(it.first, &reaped_read_file_extents[it.first.c_str()]);
        // }else{
        //   writer_thread_pool.push_back(
        //     new std::thread(&ZenFS::AsyncMigrateFileExtentsWriteWorker,this,
        //         it.first, &reaped_read_file_extents[it.first.c_str()]  )
        //     );
        
        // }

      }


    }
    // read_reaped_n+=num_events;
  }
  
  for(auto file : reaped_read_file_extents){
    // std::string filename = file.first;
    MigrateFileExtentsWorker(file.first,&(file.second));
  }
  // MigrateFileExtentsWorker(it.first, &reaped_read_file_extents[it.first.c_str()]);



  // io_destroy(read_ioctx);

   io_uring_queue_exit(&read_ring);

  
  // free(read_events);

  for(size_t t = 0; t <writer_thread_pool.size(); t++){
    writer_thread_pool[t]->join();
  }

  for(size_t a = 0 ;a < to_be_freed.size();a++){
    free(to_be_freed[a]);
  }

  return ret;
}




uint64_t ZenFS::AsyncReadMigrateExtents(const std::vector<ZoneExtentSnapshot*>& extents){
  // throw all read

  uint64_t ret = 0;
  std::map<std::string, std::vector<ZoneExtentSnapshot*>> file_extents;
  std::map<std::string, bool> migration_done;
  std::map<std::string, std::vector<AsyncZoneCleaningIocb*>> reaped_read_file_extents;
  std::vector<AsyncZoneCleaningIocb*> to_be_freed;
  std::vector<std::thread*> writer_thread_pool;
        
  int read_reaped_n = 0;
  // io_context_t read_ioctx;
  // io_context_t ctx;
  io_context_t read_ioctx = 0;


  int extent_n = (int)extents.size();
  int read_fd=zbd_->GetFD(READ_FD);
  int err=io_queue_init(extent_n,&read_ioctx);
  if(err){
    // EINVAL;
    printf("\t\t\tio_setup error@@@@@ %d %d\n",err,extent_n);
  }
   struct iocb* iocb_arr[extent_n];
   memset(iocb_arr, 0, sizeof(iocb_arr));
  // struct iocb* iocb_arr[extent_n];
  int index = 0;
  for (auto* ext : extents) {
    // ThrowAsyncExtentsRead(ext);
    // uint64_t start,legnth;
    // if (ends_with(ext->filename, ".log")) {
    //   // start=ext->start-ZoneFile::SPARSE_HEADER_SIZE;
    //   ext->header_size=ZoneFile::SPARSE_HEADER_SIZE;
    // }else{
    //   ext->header_size=0;
    // }
    
   


    struct AsyncZoneCleaningIocb* async_zc_read_iocb = 
          new AsyncZoneCleaningIocb(ext->filename,ext->start,ext->length,ext->header_size);
    to_be_freed.push_back(async_zc_read_iocb);
    ret+=async_zc_read_iocb->length_+async_zc_read_iocb->header_size_;
    
    
    io_prep_pread(&(async_zc_read_iocb->iocb_), read_fd, async_zc_read_iocb->buffer_, 
        (async_zc_read_iocb->length_+async_zc_read_iocb->header_size_), 
        (async_zc_read_iocb->start_-async_zc_read_iocb->header_size_));
    async_zc_read_iocb->iocb_.data=async_zc_read_iocb;
    iocb_arr[index]=&(async_zc_read_iocb->iocb_);
    // iocb_arr[to_be_freed.size()-1]=&(async_zc_read_iocb->iocb_);
    // struct iocb* iocb= (async_zc_read_iocb->iocb_);
    // err=io_submit(read_ioctx,1,&(iocb));
    // if(err!=1){
    //   printf("io submit err? %d\n",err);
    // }
    index++;
    file_extents[ext->filename].emplace_back(ext);
    migration_done[ext->filename]= false;
  }

    err=io_submit(read_ioctx,extent_n,iocb_arr);
    if(err!=extent_n){
      printf("io submit err? %d\n",err);
    }


  // reap here
  struct io_event read_events[extent_n];
  //  = new io_event[extent_n];
  while(read_reaped_n < extent_n){
    int num_events;
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 10000;

    // int reap_min_nr ; 

    num_events = io_getevents(read_ioctx, 1, extent_n, read_events,
                              &timeout);
    // for(const auto& it : file_extents){
    //   if(GetFileNoLock(it.first)==nullptr){
    //     extent_n-=(int)reaped_read_file_extents[it.first.c_str()].size();
    //   }
    // }
    if(num_events<1){
      continue;
    }

    // reap.
    for (int i = 0; i < num_events; i++) {
      struct io_event event = read_events[i];
      AsyncZoneCleaningIocb* reaped_read_iocb = static_cast<AsyncZoneCleaningIocb*>(event.data);

      // if(GetFileNoLock(reaped_read_iocb->filename_)==nullptr){
      //   continue;
      // }

      reaped_read_file_extents[reaped_read_iocb->filename_].emplace_back(reaped_read_iocb);
      read_reaped_n++;
    }


    // throw write

    for (const auto& it : file_extents) {
      if( migration_done[it.first.c_str()]==false &&
        it.second.size() == reaped_read_file_extents[it.first.c_str()].size() ){
        // Async write everything
        // it.second.clear();
        // printf("%s done\n",it.first.c_str());
        file_extents[it.first.c_str()].clear();
        migration_done[it.first.c_str()]=true;






        writer_thread_pool.push_back(
          new std::thread(&ZenFS::AsyncMigrateFileExtentsWriteWorker,this,
              it.first, &reaped_read_file_extents[it.first.c_str()])
          );

        // AsyncMigrateFileExtentsWorker(it.first,reaped_read_file_extents[it.first.c_str()]);

        // MigrateFileExtentsWorker(it.first,reaped_read_file_extents[it.first.c_str()]);
        // reaped_read_file_extents[it.first.c_str()].clear();
        // if(writer_thread_pool.size()==2){
        //   for(size_t t = 0; t <writer_thread_pool.size(); t++){
        //     writer_thread_pool[t]->join();
        //   }
        //   writer_thread_pool.clear();
        // }
      }


    }
    // read_reaped_n+=num_events;
  }
  

  io_destroy(read_ioctx);
  // free(read_events);

  for(size_t t = 0; t <writer_thread_pool.size(); t++){
    writer_thread_pool[t]->join();
  }

  for(size_t a = 0 ;a < to_be_freed.size();a++){
    free(to_be_freed[a]);
  }

  return ret;
}

std::vector<ZoneExtent*> ZenFS::MemoryMoveExtents(ZoneFile* zfile,
                                const std::vector<ZoneExtentSnapshot*>& migrate_exts,
                              char*read_buf,char* write_buf,Zone* new_zone,size_t* pos){
  
  std::vector<ZoneExtent*> new_extent_list;
  std::vector<ZoneExtent*> extents = zfile->GetExtents(); // old ext

  for (size_t i = 0; i < extents.size();i++) {
    ZoneExtent* ext=extents[i];
    // if(!run_gc_worker_){
    //   breka;
    // }
    ZoneExtent* new_ext=new ZoneExtent(ext->start_,ext->length_,nullptr,
          ext->fname_,ext->header_size_,
          NowMicros(),
          zfile
          );
    new_ext->zone_=ext->zone_;
    new_ext->page_cache_=std::move(ext->page_cache_);
    new_extent_list.push_back(new_ext);    

  }
  
  
  uint64_t copied = 0;

  for (ZoneExtent* ext : new_extent_list) {
    auto it = std::find_if(migrate_exts.begin(), migrate_exts.end(),
                           [&](const ZoneExtentSnapshot* ext_snapshot) {
                              if(ext_snapshot==nullptr){
                                printf("ext_snapshot nullptr\n");
                                return false;
                              }
                              if(ext==nullptr){
                                printf("ext nullptr\n");
                                return false;
                              }
                                                            // new ext
                             return ext_snapshot->start == ext->start_ &&
                                    ext_snapshot->length == ext->length_;
                           });  
  
    if (it == migrate_exts.end()) {
      Info(logger_, "Migrate extent not found, ext_start: %lu", ext->start_);
      continue;
    }
    uint64_t prev_relative_start = ext->start_ - ext->zone_->start_;

    uint64_t target_start = new_zone->wp_ + (*pos);

    if(zfile->IsSparse()){
      target_start = new_zone->wp_ + (*pos) + ZoneFile::SPARSE_HEADER_SIZE;
      ext->header_size_=ZoneFile::SPARSE_HEADER_SIZE;
      copied+=ZoneFile::SPARSE_HEADER_SIZE;
    }else{
      ext->header_size_=0;
    }
    

    

    ext->start_=target_start;
    ext->zone_=new_zone;
    new_zone->PushExtent(ext);
    new_zone->used_capacity_+=ext->length_;
    
    copied+=ext->length_;

    uint64_t align = (ext->header_size_ + ext->length_) % 4096;


    uint64_t tmp;
    if(align){
      tmp= ext->header_size_ + ext->length_ + (4096-align);
    }else{
      tmp= ext->header_size_ + ext->length_;
    }
    // if(ext->page_cache_==nullptr){
    //   char* page_cache_ptr = nullptr;
    //   if(posix_memalign((void**)(&page_cache_ptr),sysconf(_SC_PAGESIZE), tmp)){
    //     printf("ZenFS::MemoryMoveExtents memory allocation failed\n");
    //   }
    //   memmove(page_cache_ptr,read_buf+(prev_relative_start-ext->header_size_),tmp);
    //   ext->page_cache_.reset(page_cache_ptr);
    //   zbd_->page_cache_size_+=ext->length_;
    // }

    memmove(write_buf+(*pos), read_buf+(prev_relative_start-ext->header_size_)  ,tmp);

    (*pos)+=tmp;

  }


  zbd_->AddGCBytesWritten(copied);
  return new_extent_list;
}

uint64_t ZenFS::SMRLargeIOMigrateExtents(const std::vector<ZoneExtentSnapshot*>& extents,uint64_t should_be_copied,bool everything_in_page_cache) {
  Zone* victim_zone= zbd_->GetIOZone(extents[0]->start);
  Zone* new_zone =nullptr;
  // int read_fd = zbd_->GetFD(READ_FD);
  int read_fd = zbd_->GetFD(READ_DIRECT_FD);
  (void)(everything_in_page_cache);
  (void)(should_be_copied);
  double measured_ms;
  uint64_t page_cache_hit_size = 0 ;
  // uint64_t disk_io_size = 0;
  // uint64_t io_zone_start_offset = zbd_->GetIOZoneByIndex(0)->start_;


  int err=0;
  std::map<std::string, std::vector<ZoneExtentSnapshot*>> file_extents;

  std::map<ZoneFile*,std::vector<ZoneExtent*>> lock_acquired_zfiles;

  if(ZC_write_buffer_==nullptr){
    err=posix_memalign((void**)&ZC_write_buffer_, sysconf(_SC_PAGESIZE), victim_zone->max_capacity_);
    if(err){
      printf("SMRLargeIOMigrateExtents fail to allocate ZC_write_buffer_\n");
    }
  }
  if(ZC_read_buffer_==nullptr){
    err=posix_memalign((void**)&ZC_read_buffer_, sysconf(_SC_PAGESIZE), victim_zone->max_capacity_);
    if(err){
      printf("SMRLargeIOMigrateExtents fail to allocate ZC_read_buffer_\n");
    }
    err=posix_memalign((void**)&ZC_read_buffer2_, sysconf(_SC_PAGESIZE), victim_zone->max_capacity_);
    if(err){
      printf("SMRLargeIOMigrateExtents fail to allocate ZC_read_buffer_\n");
    }
  }


  char stopwatch_buf[50];

  sprintf((char*)stopwatch_buf, "ZC COPY size (%lu)",should_be_copied>>20 );

  ZenFSStopWatch ZC_size_measure((const char*)stopwatch_buf,zbd_);

{
    ZenFSStopWatch z1("Large IO pread",zbd_);

    for(auto ext : extents){
      uint64_t read_size = ext->length+ext->header_size;
      size_t align =   (read_size) % 4096;
      if(align){
        read_size += 4096-align;
      }

      
      if(ext->page_cache==nullptr ){
        ZenFSStopWatch sw("",nullptr);
        // disk_io_size+= ext->length>>20;

        // err=pread(read_fd,ZC_read_buffer_+(ext->start-victim_zone->start_ -(ext->header_size)),
        //     (read_size),
        //     (ext->start-ext->header_size));
        uint64_t aligned_start= ext->start;
        uint64_t aligned_length = ext->length;
        align= aligned_start  % 4096;
        if(align){
          aligned_start-=align;
        }
        align=aligned_length%4096;
        if(align){
          aligned_length+=4096-align;
        }

        err=pread(read_fd,ZC_read_buffer2_,aligned_length,aligned_start);

        memmove(ZC_read_buffer_+(ext->start-victim_zone->start_ ),ZC_read_buffer2_, ext->length);
        
        // err=pread(read_fd,ZC_read_buffer_+(ext->start-victim_zone->start_ ),
        //     (ext->length),
        //     (ext->start));

        
        if(err<0){
          printf("SMRLargeIOMigrateExtents err %d ext->start %lu victim_zone->start_ %lu ext->length %lu header %lu\n",
          err,ext->start,victim_zone->start_,ext->length,ext->header_size);
        }
        measured_ms=sw.RecordTickMS();
        zbd_->CorrectCost(READ_DISK_COST,(read_size>>20),measured_ms);
      }else{
        page_cache_hit_size+=(ext->length)>>20;
        ZenFSStopWatch sw("",nullptr);
        // memmove(ZC_read_buffer_+(ext->start-ext->header_size -victim_zone->start_), ext->page_cache.get(),
        //       read_size);

            // memmove(ZC_read_buffer_+(ext->start-victim_zone->start_), ext->page_cache.get(),
            //   ext->length + ext->header_size);    
      
            memmove(ZC_read_buffer_+(ext->start-victim_zone->start_), ext->page_cache.get()+ext->header_size,
              ext->length);    
        measured_ms=sw.RecordTickMS();
        // printf("\t\tzc cache hit %lf (%lu MB)\n",measured_ms,ext->length>>20);
        zbd_->CorrectCost(READ_PAGE_COST,( (ext->length + ext->header_size)>>20),measured_ms);
      }

    }
}

    // err=posix_memalign((void**)&tmp_buf, sysconf(_SC_PAGESIZE), max_end-min_start);
    // if(err){
    //   printf("SMRLargeIOMigrateExtents fail to allocate tmp buf\n");
    // }
  //   if(everything_in_page_cache==true){
  //     {
  //       ZenFSStopWatch zread("EVERY THING IN CACHE READ",zbd_);
  //       uint64_t copied_tmp  =0 ;
  //       for(auto ext: extents){
  //         if(ext->page_cache!=nullptr){
  //           memmove(ZC_read_buffer_+(ext->start-victim_zone->start_), ext->page_cache.get(),
  //             ext->length + ext->header_size);
  //         }else{
  //           err=pread(read_fd,ZC_read_buffer_+(ext->start-victim_zone->start_),ext->length,ext->start );
  //         }
  //         // err=pread(read_fd,tmp_buf + ((ext->start)-min_start),ext->length, ext->start );


  //         // memmove(tmp_buf+(ext->start-min_start),
  //         //     page_cache_hit_mmap_addr_+(ext->start-io_zone_start_offset_),
  //         //     ext->length);
  //             copied_tmp +=ext->length;


  //       }
  //       printf("in large I/O\t%lu ms\tcopied %lu (MB)\n",(zread.RecordTickNS()/1000/1000),copied_tmp>>20);
  //     }

  //     munlock((const void*)(page_cache_hit_mmap_addr_ + (victim_zone->start_- io_zone_start_offset_)) ,
  //         victim_zone->max_capacity_);
      
  //   }else{
  //     ZenFSStopWatch zread("max end min start READ",zbd_);

  //     err=(int)pread(read_fd,ZC_read_buffer_ +(min_start-victim_zone->start_) ,
  //         max_end-min_start,min_start);
  //     zbd_->AddZCRead(max_end-min_start);
  //   }
  // }

  

  zbd_->TakeSMRMigrateZone(&new_zone,victim_zone->lifetime_,should_be_copied);


  for (auto* ext : extents) {
    std::string fname = ext->filename;
    file_extents[fname].emplace_back(ext);
  }


  size_t pos = 0;
{  
  ZenFSStopWatch z2("MemoryMoveExtents",zbd_);
  for (const auto& it : file_extents) {
    std::string fname =it.first;
    auto zfile = GetFile(fname);
    if (zfile == nullptr) {
      continue;
    }
    if (!zfile->TryAcquireWRLock()) {
      continue;
    }
    zfile->on_zc_.store(1);
    // lock_acquired_zfiles.push_back(zfile);


    lock_acquired_zfiles[zfile.get()]=
                          MemoryMoveExtents(zfile.get(),it.second,
                          ZC_read_buffer_,
                          ZC_write_buffer_,
                          new_zone,&pos);
    // new_zone->lifetime_=zfile->GetWriteLifeTimeHint();
    if(pos>new_zone->max_capacity_){
      printf("SMRLargeIOMigrateExtents ???? pos %lu\n",pos);
    }
  }
}

  {
    ZenFSStopWatch sw("Large IO pwrite",zbd_);
    new_zone->Append(ZC_write_buffer_,pos,true);
    measured_ms=sw.RecordTickMS();
    zbd_->CorrectCost(WRITE_COST,( (should_be_copied)>>20),measured_ms);
    
  }
  // munlock((const void*)ZC_write_buffer_,victim_zone->max_capacity_);
  // if(new_zone->wp_-new_zone->start_ != pos){
  //   printf("SMRLargeIOMigrateExtents after append pos : %lu , relative wp %lu\n",pos,new_zone->wp_-new_zone->start_);
  // }
  zbd_->ReleaseSMRMigrateZone(new_zone);


  // for(auto it : lock_acquired_zfiles){
  //   SyncFileExtents(it.first, it.second);
  //   it.first->ReleaseWRLock();
  // }

  LargeIOSyncFileExtents(lock_acquired_zfiles);
  for(auto it : lock_acquired_zfiles){
    it.first->ReleaseWRLock();
    it.first->on_zc_.store(0);
  }

  // zbd_->AddGCBytesWritten(pos);
  // printf("%d %s %lu\n",mount_time_.load(),stopwatch_buf,ZC_size_measure.RecordTickNS()/1000/1000);
  // printf("%s %lu\n",stopwatch_buf2,ZC_size_measure2.RecordTickNS()/1000/1000);
  // return IOStatus::OK();
  return page_cache_hit_size;
}

uint64_t ZenFS::MigrateExtents(
    const std::vector<ZoneExtentSnapshot*>& extents) {
  // IOStatus s;
  uint64_t ret = 0;
  // ZenFSStopWatch("MigrateExtents");
  // (void) run_once;
  // Group extents by their filename
  std::map<std::string, std::vector<ZoneExtentSnapshot*>> file_extents;

  for (auto* ext : extents) {
    
    std::string fname = ext->filename;
    
    // if(fname.ene)
    if(ends_with(fname,".sst")||ends_with(fname,".log")){
      file_extents[fname].emplace_back(ext);
    }else{
      ext->zone_p->used_capacity_-=ext->length;
    }
    
  }
  //  printf("after MigrateExtents\n");e
  for (const auto& it : file_extents) {
    ret+= MigrateFileExtents(it.first, it.second);
    // if (!s.ok()) break;
    if(!run_gc_worker_){
      // return IOStatus::OK();
      return ret;
    }
  }
  {
    // ZenFSStopWatch z1("ResetUnsedZones");
    // s=zbd_->ResetUnusedIOZones();
  }
  // return s;
  // if(ret==){

  // }
  // if(ext->zone_p->used_capacity_>)
  // extents[0]->zone_p->used_capacity_=0;
  return ret;
}


uint64_t ZenFS::AsyncMigrateExtents(
    const std::vector<ZoneExtentSnapshot*>& extents) {
  IOStatus s;
  std::vector<AsyncWorker*> thread_pool;
  uint64_t ret = 0;
  std::map<std::string, std::vector<ZoneExtentSnapshot*>> file_extents;
// ZenFSStopWatch z4("AsyncMigrateExtents");
{  // long elapsed_ns_timespec;
  // ZenFSStopWatch("Prepare");
  // (void) run_once;
  // Group extents by their filename


  // printf("before MigrateExtents\n");
  for (auto* ext : extents) {
    std::string fname = ext->filename;
    file_extents[fname].emplace_back(ext);
    ret+=ext->length;
  }

  // clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  // elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  
  }
  // printf("prepare breaktown %lu ms\n",(elapsed_ns_timespec/1000)/1000 );
  
  // clock_gettime(CLOCK_MONOTONIC, &start_timespec);
{
  // ZenFSStopWatch z5("Sum");
  for (auto& it : file_extents) {

    io_context_t* write_ioctx=nullptr;
    io_uring* read_ring= nullptr;
    
    int err=GetAsyncStructure(&read_ring,&write_ioctx);
    if(err){
      printf("GetAsyncStructure Err @@@\n");
    }

    if(zbd_->AsyncZCEnabled()>=2){ //single thread

      AsyncMigrateFileExtentsWorker(it.first,&(it.second),
     write_ioctx, read_ring);

    }else{
       std::thread* t=new std::thread(&ZenFS::AsyncMigrateFileExtentsWorker,this,
            it.first, &(it.second),write_ioctx, read_ring);
      thread_pool.push_back(new AsyncWorker(t,write_ioctx,read_ring));

    }
    // read_rings.push_back(read_ring);
    // write_ioctxes.push_back(write_ioctx);





    if(!run_gc_worker_){
      for(size_t t = 0 ;t < thread_pool.size(); t++){
        // thread_pool[t]->join();


        delete thread_pool[t];

      }
      return ret;
    }
    
  }
  }
  // clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  // elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  // printf("sum breaktown %lu ms\n",(elapsed_ns_timespec/1000)/1000 );
  
  
  // clock_gettime(CLOCK_MONOTONIC, &start_timespec);
{
  // ZenFSStopWatch z6("Delete thread");
  for(size_t t = 0 ;t < thread_pool.size(); t++){
    // thread_pool[t]->join();
    delete thread_pool[t];
  }
}
{
  // r  u

}  
  // clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  // elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  // printf("end breaktown %lu ms\n\n",(elapsed_ns_timespec/1000)/1000 );
  
  return ret;
}



IOStatus ZenFS::MigrateFileExtentsWorker(
    std::string fname,
    std::vector<AsyncZoneCleaningIocb*>* migrate_exts) {
  IOStatus s = IOStatus::OK();
  uint64_t copied = 0;
  // io_context_t write_ioctx = 0;
  int extent_n = (int)migrate_exts->size();
  // int write_reaped_n = 0;
  // int err = io_queue_init(extent_n,&write_ioctx);

  Info(logger_, "MigrateFileExtents, fname: %s, extent count: %lu",
       fname.data(), migrate_exts->size());
  
  // The file may be deleted by other threads, better double check.
  auto zfile = GetFile(fname);
  if (zfile == nullptr) {
    return IOStatus::OK();
  }

  // Don't migrate open for write files and prevent write reopens while we
  // migrate
  if (!zfile->TryAcquireWRLock()) {
    // io_destroy(write_ioctx);
    return IOStatus::OK();
  }

  std::vector<ZoneExtent*> new_extent_list;
  std::vector<ZoneExtent*> extents = zfile->GetExtents(); // old ext

  for (size_t i = 0; i < extents.size();i++) {
    ZoneExtent* ext=extents[i];
    ZoneExtent* new_ext=new ZoneExtent(ext->start_,ext->length_,nullptr,
          ext->fname_,ext->header_size_,zfile.get());
    new_ext->zone_=ext->zone_;
    new_extent_list.push_back(new_ext);
  }
  extent_n = 0;
  for (ZoneExtent* ext : new_extent_list) {
    auto it = std::find_if(migrate_exts->begin(), migrate_exts->end(),
                           [&](const AsyncZoneCleaningIocb* ext_snapshot) {
                              if(ext_snapshot==nullptr){
                                printf("ext_snapshot nullptr\n");
                                return false;
                              }
                              if(ext==nullptr){
                                printf("ext nullptr\n");
                                return false;
                              }
                                                            // new ext
                             return ext_snapshot->start_ == ext->start_ &&
                                    ext_snapshot->length_ == ext->length_;
                           });
    if (it == migrate_exts->end()) {
      Info(logger_, "Migrate extent not found, ext_start: %lu", ext->start_);
      continue;
    }
    // copied+=ext->length_;



    Zone* target_zone = nullptr; 
    // if(target_zone->)

    s = zbd_->TakeMigrateZone(zfile->smallest_,zfile->largest_,zfile->level_, &target_zone, zfile->GetWriteLifeTimeHint(),
                              zfile->predicted_size_,
                              ext->length_,&run_gc_worker_,zfile->IsSST());
    if(!run_gc_worker_){
      // io_destroy(write_ioctx);
      zbd_->ReleaseMigrateZone(target_zone);
      return IOStatus::OK();
    }
    if(target_zone!=nullptr&&target_zone->lifetime_==Env::WriteLifeTimeHint::WLTH_NOT_SET){
      target_zone->lifetime_=Env::WriteLifeTimeHint::WLTH_LONG;
    }
    // throw it
    
    uint64_t target_start = target_zone->wp_;
    if(zfile->IsSparse()){
      target_start= target_zone->wp_ + ZoneFile::SPARSE_HEADER_SIZE;
      ext->header_size_=ZoneFile::SPARSE_HEADER_SIZE;
    }
    ext->start_=target_start;
    ext->zone_ = target_zone;
    uint64_t size = (*it)->length_ + (*it)->header_size_;
    uint64_t pad = size % 4096;
    if(pad){
      size += 4096-pad;
    }

    target_zone->Append((*it)->buffer_,size);

    extent_n++;

    target_zone->PushExtent(ext);
    ext->zone_->used_capacity_ += ext->length_;

    copied+=ext->length_;
    


    if (GetFileNoLock(fname) == nullptr) {
      Info(logger_, "Migrate file not exist anymore.");
      // zbd_->ReleaseMigrateZone(target_zone);
      // for (ZoneExtent* de : new_extent_list) {
      //   de->is_invalid_=true;
      // }
      break;
    }
    zbd_->ReleaseMigrateZone(target_zone);
  }

  // reap it
  // struct io_event* write_events = new io_event[extent_n];
  // while(write_reaped_n<extent_n){
  //   int num_events;
  //   struct timespec timeout;
  //   timeout.tv_sec = 0;
  //   timeout.tv_nsec = 100000000;
  //   num_events = io_getevents(write_ioctx, 1, extent_n, write_events,
  //                             &timeout);
  //   if(num_events<1){
  //     continue;
  //   }
  //   write_reaped_n+=num_events;
  // }

  // sync it
  zbd_->AddGCBytesWritten(copied);
  SyncFileExtents(zfile.get(), new_extent_list);
  zfile->ReleaseWRLock();
  // io_destroy(write_ioctx);
  return IOStatus::OK();
}

IOStatus ZenFS::AsyncUringMigrateFileExtentsWorker(
    std::string fname,
    std::vector<AsyncZoneCleaningIocb*>* migrate_exts) {
  IOStatus s = IOStatus::OK();
  uint64_t copied = 0;
  // io_context_t write_ioctx = 0;
  int extent_n = (int)migrate_exts->size();
  int write_reaped_n = 0;
  // int err = io_queue_init(extent_n,&write_ioctx);
  io_uring write_ring;
  unsigned flags = IORING_SETUP_SQPOLL;
  int err=io_uring_queue_init(extent_n, &write_ring, flags);

  if(err){
    printf("\t\t\t\t AsyncMigrateFileExtentsWorker io_uring_queue_init err %d %d",err,extent_n);
  }


  Info(logger_, "MigrateFileExtents, fname: %s, extent count: %lu",
       fname.data(), migrate_exts->size());
  
  // The file may be deleted by other threads, better double check.
  auto zfile = GetFile(fname);
  if (zfile == nullptr) {
    return IOStatus::OK();
  }

  // Don't migrate open for write files and prevent write reopens while we
  // migrate
  if (!zfile->TryAcquireWRLock()) {
    io_uring_queue_exit(&write_ring);
    return IOStatus::OK();
  }

  std::vector<ZoneExtent*> new_extent_list;
  std::vector<ZoneExtent*> extents = zfile->GetExtents(); // old ext

  for (size_t i = 0; i < extents.size();i++) {
    ZoneExtent* ext=extents[i];
    ZoneExtent* new_ext=new ZoneExtent(ext->start_,ext->length_,nullptr,
          ext->fname_,ext->header_size_,zfile.get());
    new_ext->zone_=ext->zone_;
    new_extent_list.push_back(new_ext);
  }
  extent_n = 0;
  for (ZoneExtent* ext : new_extent_list) {
    auto it = std::find_if(migrate_exts->begin(), migrate_exts->end(),
                           [&](const AsyncZoneCleaningIocb* ext_snapshot) {
                              if(ext_snapshot==nullptr){
                                printf("ext_snapshot nullptr\n");
                                return false;
                              }
                              if(ext==nullptr){
                                printf("ext nullptr\n");
                                return false;
                              }
                                                            // new ext
                             return ext_snapshot->start_ == ext->start_ &&
                                    ext_snapshot->length_ == ext->length_;
                           });
    if (it == migrate_exts->end()) {
      Info(logger_, "Migrate extent not found, ext_start: %lu", ext->start_);
      continue;
    }
    // copied+=ext->length_;



    Zone* target_zone = nullptr; 
    s = zbd_->TakeMigrateZone(zfile->smallest_,zfile->largest_,zfile->level_, &target_zone, zfile->GetWriteLifeTimeHint(),
                              zfile->predicted_size_,
                              ext->length_,&run_gc_worker_,zfile->IsSST());
    if(!run_gc_worker_){
      io_uring_queue_exit(&write_ring);
      zbd_->ReleaseMigrateZone(target_zone);
      return IOStatus::OK();
    }
    if(target_zone!=nullptr&&target_zone->lifetime_==Env::WriteLifeTimeHint::WLTH_NOT_SET){
      target_zone->lifetime_=Env::WriteLifeTimeHint::WLTH_LONG;
    }
    // throw it
    
    uint64_t target_start = target_zone->wp_ + (*it)->header_size_;
    ext->header_size_=(*it)->header_size_;
    // if(zfile->IsSparse()){
    //   target_start= target_zone->wp_ + ZoneFile::SPARSE_HEADER_SIZE;
    //   ext->header_size_=ZoneFile::SPARSE_HEADER_SIZE;
    // }
    ext->start_=target_start;
    ext->zone_ = target_zone;
    // io_uring_sqe_set_data(sqe,(*it));
    target_zone->ThrowAsyncUringZCWrite(&write_ring,*it);
    (*it)->index_=extent_n;
    extent_n++;

    target_zone->PushExtent(ext);
    ext->zone_->used_capacity_ += ext->length_;

    copied+=ext->length_;
    


    if (GetFileNoLock(fname) == nullptr) {
      Info(logger_, "Migrate file not exist anymore.");
      zbd_->ReleaseMigrateZone(target_zone);
      // for (ZoneExtent* de : new_extent_list) {
      //   de->is_invalid_=true;
      // }
      break;
    }
    zbd_->ReleaseMigrateZone(target_zone);
  }

  // reap it
  // struct io_event* write_events = new io_event[extent_n];
  // struct io_event write_events[extent_n];
  while(write_reaped_n<extent_n){
    struct io_uring_cqe* cqe = nullptr;
 
    int result = io_uring_peek_cqe(&write_ring, &cqe); // success 0 , polling

    // int result = io_uring_wait_cqe(&write_ring, &cqe); // success 0
    
    if(result!=0){
      
      continue;
    }
    AsyncZoneCleaningIocb* reaped_write_iocb=reinterpret_cast<AsyncZoneCleaningIocb*>(cqe->user_data);
    if(reaped_write_iocb->index_!=write_reaped_n){
      printf("[write_reaped_n %d]!= reaped_write_iocb->index_ %d extent_n %d\n",write_reaped_n,reaped_write_iocb->index_,extent_n);
    }
    write_reaped_n++;
    io_uring_cqe_seen(&write_ring,cqe);
  }

    // sync it
    zbd_->AddGCBytesWritten(copied);
    SyncFileExtents(zfile.get(), new_extent_list);
    zfile->ReleaseWRLock();
    io_uring_queue_exit(&write_ring);
  return IOStatus::OK();
}



IOStatus ZenFS::AsyncMigrateFileExtentsWriteWorker(
    std::string fname,
    std::vector<AsyncZoneCleaningIocb*>* migrate_exts) {
  IOStatus s = IOStatus::OK();
  uint64_t copied = 0;
  io_context_t write_ioctx = 0;
  int extent_n = (int)migrate_exts->size();
  int write_reaped_n = 0;
  int err = io_queue_init(extent_n,&write_ioctx);

  if(err){
    printf("\t\t\t\t AsyncMigrateFileExtentsWorker io_queue_init err %d %d",err,extent_n);
  }
  Info(logger_, "MigrateFileExtents, fname: %s, extent count: %lu",
       fname.data(), migrate_exts->size());
  
  // The file may be deleted by other threads, better double check.
  auto zfile = GetFile(fname);
  if (zfile == nullptr) {
    return IOStatus::OK();
  }

  // Don't migrate open for write files and prevent write reopens while we
  // migrate
  if (!zfile->TryAcquireWRLock()) {
    io_destroy(write_ioctx);
    return IOStatus::OK();
  }

  std::vector<ZoneExtent*> new_extent_list;
  std::vector<ZoneExtent*> extents = zfile->GetExtents(); // old ext

  for (size_t i = 0; i < extents.size();i++) {
    ZoneExtent* ext=extents[i];
    ZoneExtent* new_ext=new ZoneExtent(ext->start_,ext->length_,nullptr,
          ext->fname_,ext->header_size_,zfile.get());
    new_ext->zone_=ext->zone_;
    new_extent_list.push_back(new_ext);
  }
  extent_n = 0;
  for (ZoneExtent* ext : new_extent_list) {
    auto it = std::find_if(migrate_exts->begin(), migrate_exts->end(),
                           [&](const AsyncZoneCleaningIocb* ext_snapshot) {
                              if(ext_snapshot==nullptr){
                                printf("ext_snapshot nullptr\n");
                                return false;
                              }
                              if(ext==nullptr){
                                printf("ext nullptr\n");
                                return false;
                              }
                                                            // new ext
                             return ext_snapshot->start_ == ext->start_ &&
                                    ext_snapshot->length_ == ext->length_;
                           });
    if (it == migrate_exts->end()) {
      Info(logger_, "Migrate extent not found, ext_start: %lu", ext->start_);
      continue;
    }


    Zone* target_zone = nullptr; 
    s = zbd_->TakeMigrateZone(zfile->smallest_,zfile->largest_,zfile->level_, &target_zone, zfile->GetWriteLifeTimeHint(),
                              zfile->predicted_size_,
                              ext->length_,&run_gc_worker_,zfile->IsSST());
    if(!run_gc_worker_){
      io_destroy(write_ioctx);
      zbd_->ReleaseMigrateZone(target_zone);
      return IOStatus::OK();
    }
    if(target_zone!=nullptr&&target_zone->lifetime_==Env::WriteLifeTimeHint::WLTH_NOT_SET){
      target_zone->lifetime_=Env::WriteLifeTimeHint::WLTH_LONG;
    }
    // throw it
    
    uint64_t target_start = target_zone->wp_ + (*it)->header_size_;
    ext->header_size_=(*it)->header_size_;
    // if(zfile->IsSparse()){
    //   target_start= target_zone->wp_ + ZoneFile::SPARSE_HEADER_SIZE;
    //   ext->header_size_=ZoneFile::SPARSE_HEADER_SIZE;
    // }
    ext->start_=target_start;
    ext->zone_ = target_zone;
    target_zone->ThrowAsyncZCWrite(write_ioctx,*it);
    extent_n++;

    target_zone->PushExtent(ext);
    ext->zone_->used_capacity_ += ext->length_;

    copied+=ext->length_;
    


    if (GetFileNoLock(fname) == nullptr) {
      Info(logger_, "Migrate file not exist anymore.");
      zbd_->ReleaseMigrateZone(target_zone);
      // for (ZoneExtent* de : new_extent_list) {
      //   de->is_invalid_=true;
      // }
      break;
    }
    zbd_->ReleaseMigrateZone(target_zone);
  }

  // reap it
  // struct io_event* write_events = new io_event[extent_n];
  struct io_event write_events[extent_n];
  while(write_reaped_n<extent_n){
    int num_events;
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 10000; // 100ms
    // timeout.tv_sec = 0;
    // timeout.tv_nsec = 1000000000; // 1000ms
    int write_reap_min_nr = (extent_n-write_reaped_n) > 1 ? (extent_n-write_reaped_n) : 1;
    // write_reap_min_nr=1;

    num_events = io_getevents(write_ioctx, write_reap_min_nr, extent_n, write_events,
                              &timeout);
    if(num_events<1){
      continue;
    }
    write_reaped_n+=num_events;
  }

    // sync it
    zbd_->AddGCBytesWritten(copied);
    SyncFileExtents(zfile.get(), new_extent_list);
    zfile->ReleaseWRLock();
    io_destroy(write_ioctx);
  return IOStatus::OK();
}

void ZenFS::BackgroundPageCacheEviction(void){
  while(run_gc_worker_){
    usleep(1000*1000);
    uint64_t page_cache_size = zbd_->page_cache_size_;
    while(page_cache_size>zbd_->PageCacheLimit() && run_gc_worker_){
      std::lock_guard<std::mutex> lg(page_cache_mtx_);
      std::lock_guard<std::mutex> file_lock(files_mtx_);

      // uint64_t invalid_data_size = 0;
      // uint64_t valid_data_size = 0;
      // std::vector<Zone*> io_zones =  *zbd_->GetIOZones();
      // for(Zone* z : io_zones){
      //   valid_data_size+=z->used_capacity_; 
      //   invalid_data_size+=(z->wp_-z->start_ - z->used_capacity_);
      // }
      // uint64_t invalid_ratio = (invalid_data_size*100)/(valid_data_size+invalid_data_size);

      // bool do_zc_page_cache_eviction = (free_percent_<zbd_->until_ || free_percent_<23)  
      //                            && invalid_ratio<25 &&zbd_->PCAEnabled();

      // if( do_zc_page_cache_eviction
      // ){
      //   ZCPageCacheEviction();
      // }else{
      if(zbd_->PCAEnabled()){
        OpenZonePageCacheEviction();
      }else{
        LRUPageCacheEviction();
      }
      
      
      // }
      // LRUPageCacheEviction(free_percent_<23 && zbd_->PCAEnabled());
    }
  }
}

void ZenFS::OpenZonePageCacheEviction(void){

        std::vector< std::pair< uint64_t,ZoneExtent* >> all_extents;
      // uint64_t page_cache_size = zbd_->page_cache_size_;
      // uint64_t page_cache_size = 0;
      all_extents.clear();


  // evict open zone, closed zone first

      for (const auto& file_it : files_) {
        std::shared_ptr<ZoneFile> file = (file_it.second);
        if(!file->TryAcquireWRLock()){
          continue;
        }
        std::vector<ZoneExtent*> extents;
        extents.clear();
        extents=file->GetExtents();
        for (ZoneExtent* ext : extents ) {
          if(ext){
            // if(ext->page_cache_ !=nullptr){
            //   // zbd_->page_cache_size_ = page_cache_size;
            //   page_cache_size+=ext->length_;
            // }
            all_extents.push_back({ext->last_accessed_,ext});
            
          }
          if(!ext){
            printf("why?? %p\n",ext);
          }
        }
        file->ReleaseWRLock();
      }
      // zbd_->page_cache_size_ = page_cache_size;

      sort(all_extents.begin(),all_extents.end());
      for(int evict_open_first = 1 ; evict_open_first>=0; evict_open_first--){
        if(zbd_->page_cache_size_<zbd_->PageCacheLimit() ){
          break;
        }
        for(std::pair<uint64_t,ZoneExtent*> ext : all_extents){
            if(!ext.second){
              continue;
            }
            if(evict_open_first==1 && ext.second->zone_->state_ ==Zone::State::FINISH){ // if FINISH OR FULL
              continue;
            }
            if(!ext.second->zfile_){
              continue;
            }
            // if(!ext.second->zfile_->writer_mtx_.try_lock()){
            //   continue;
            // }

            std::shared_ptr<char> tmp_cache = std::move(ext.second->page_cache_);
            // if(tmp_cache==nullptr){
            //   ext.second->zfile_->writer_mtx_.unlock();
            //   continue;
            // }
            // if(tmp_cache.use_count()>1){
            //   continue;
            // }
            zbd_->page_cache_size_-=ext.second->length_;
            // page_cache_size-=ext.second->length_;
            tmp_cache.reset();
            // ext.second->zfile_->writer_mtx_.unlock();
            if(zbd_->page_cache_size_<zbd_->PageCacheLimit() ){
              break;
            }
        }
    }
}

void ZenFS::ZCPageCacheEviction(void){
      std::vector<std::pair <uint64_t, std::vector<ZoneExtent*> > > extent_to_zone;

      uint64_t invalid_data_size = 0;
      uint64_t valid_data_size = 0;
      std::vector<Zone*> io_zones =  *zbd_->GetIOZones();
      for(Zone* z : io_zones){
        valid_data_size+=z->used_capacity_; 
        invalid_data_size+=(z->wp_-z->start_ - z->used_capacity_);
      }
      uint64_t invalid_ratio = (invalid_data_size*100)/(valid_data_size+invalid_data_size);
      (void)(invalid_ratio);
      // std::vector<std::pair<uint64_t,uint64_t>> zone_to_be_pinned=zbd_->HighPosibilityTobeVictim( 
      //   invalid_ratio == 0 ? 10 : 100/invalid_ratio );


      for(auto z : io_zones ){
        extent_to_zone.push_back( {z->wp_-z->start_- z->used_capacity_ , std::vector<ZoneExtent*>() });
      }


      // phase 1 : separate it to zone - extent
      for (const auto& file_it : files_) {
        if(!run_gc_worker_){
          break;
        }
        std::shared_ptr<ZoneFile> file = (file_it.second);
        std::vector<ZoneExtent*> extents=file->GetExtents();
        for (ZoneExtent* ext : extents ) {
          if(!ext){
            continue;
          }
          extent_to_zone[ext->zone_->zidx_-ZENFS_SPARE_ZONES-ZENFS_META_ZONES].second.push_back(ext);
        }
      }

      


      // phase 2 : sort zone by ZC posilbiity, reclaimed spae 1 ,2, 33, 56, 7
      std::sort(extent_to_zone.begin(),extent_to_zone.end());
      // for(auto ez : extent_to_zone){
      //   std::sort(ez.second.begin(),ez.second.end(),ZoneExtent::SortByLeastRecentlyUsed);
      // }
      for(auto ez : extent_to_zone){
        if(!run_gc_worker_){
          break;
        }
        std::sort(ez.second.begin(),ez.second.end(),ZoneExtent::SortByLeastRecentlyUsed);


        
        for(ZoneExtent* ext : ez.second){
          // if(std::find_if(zone_to_be_pinned.begin() ,
          //                 zone_to_be_pinned.end(),[&](const std::pair<uint64_t,uint64_t> valid_zidx ){
          //                   return valid_zidx.second==ext->zone_->zidx_;
          //                 }) != zone_to_be_pinned.end() ){
          //   break;
          // }

          if(ext->page_cache_==nullptr){
            continue;
          }

          std::shared_ptr<char> tmp_cache = std::move(ext->page_cache_);
          if(tmp_cache==nullptr){
            continue;
          }
          // if(tmp_cache.use_count()>1){
          //   continue;
          // }
          zbd_->page_cache_size_-=ext->length_;
          tmp_cache.reset();
          if(zbd_->page_cache_size_<zbd_->PageCacheLimit()){
            break;
          }
        }
        if(zbd_->page_cache_size_<zbd_->PageCacheLimit()){
          break;
        }
      }


  
}

void ZenFS::LRUPageCacheEviction(){
      std::vector< std::pair< uint64_t,ZoneExtent* >> all_extents;
      // uint64_t page_cache_size = zbd_->page_cache_size_;
      // uint64_t page_cache_size=0;
      all_extents.clear();
      // all_extents_tmp.clear();



      // std::vector<std::pair<uint64_t,uint64_t>> zone_to_be_pinned;
      // zone_to_be_pinned.clear();
      // invalid_ratio = invalid_ratio==0 ? 1 : invalid_ratio;
      // if(zc_aware){
      //   zone_to_be_pinned=zbd_->HighPosibilityTobeVictim(
      //    ( (100 - invalid_ratio)*zbd_->PageCacheLimit()) /100 / zbd_->GetZoneSize()
      //   );
      // }


      for (const auto& file_it : files_) {
        std::shared_ptr<ZoneFile> file = (file_it.second);
        if(!file->TryAcquireWRLock()){
          continue;
        }
        // ZoneFile::ReadLock readlck(file);
        // if(!file->writer_mtx_.try_lock()){
        //   continue;
        // }
        // file->readers_++;
        // file->writer_mtx_.unlock();
        std::vector<ZoneExtent*> extents;
        extents.clear();
        extents=file->GetExtents();
        for (ZoneExtent* ext : extents ) {
          if(ext){
            // all_extents_tmp.push_back(ext);
            // if(ext->page_cache_!=nullptr){
            //   page_cache_size+=ext->length_;
            // }
            all_extents.push_back({ext->last_accessed_,ext});
            
          }
          if(!ext){
            printf("why?? %p\n",ext);
          }
        }
        file->ReleaseWRLock();
        // file->readers_--;


        // if(zbd_->page_cache_size_<zbd_->PageCacheLimit()){
        //   break;
        // }
      }
      // zbd_->page_cache_size_ = page_cache_size;
      // for(auto ext: all_extents_tmp){
      //   if(ext){
      //     all_extents.push_back(ext);
      //   }
      // }
      // sort(all_extents.begin(),all_extents.end(),ZoneExtent::SortByLeastRecentlyUsed);
      sort(all_extents.begin(),all_extents.end());

      for(std::pair<uint64_t,ZoneExtent*> ext : all_extents){
          if(!ext.second){
            continue;
          }
          if(ext.second->page_cache_==nullptr){
            continue;
          }
          if(!ext.second->zfile_){
            continue;
          }

          // if(!ext.second->zfile_->writer_mtx_.try_lock()){
          //   continue;
          // }
          // if(zc_aware && std::find_if(zone_to_be_pinned.begin() ,
          //                 zone_to_be_pinned.end(),[&](const std::pair<uint64_t,uint64_t> valid_zidx ){
          //                   return valid_zidx.second==ext.second->zone_->zidx_;
          //                 }) != zone_to_be_pinned.end() ){
          //   continue;
          // }


          std::shared_ptr<char> tmp_cache = std::move(ext.second->page_cache_);
          // if(tmp_cache==nullptr){
          //   ext.second->zfile_->writer_mtx_.unlock();
          //   continue;
          // }
          // if(tmp_cache.use_count()>1){
          //   // ext->page_cache_
          //   ext.second->zfile_->writer_mtx_.unlock();
          //   continue;
          // }
          // page_cache_size-=ext.second->length_;
          zbd_->page_cache_size_-=ext.second->length_;
          tmp_cache.reset();
          // ext.second->zfile_->writer_mtx_.unlock();

          if(zbd_->page_cache_size_<zbd_->PageCacheLimit()){
            break;
          }
      }
}

///////////// this async
IOStatus ZenFS::AsyncMigrateFileExtentsWorker(
      std::string fname,
      std::vector<ZoneExtentSnapshot*>* migrate_exts,
      io_context_t* write_ioctx,
      io_uring* read_ring){
  // struct timespec start_timespec, end_timespec;

  // clock_gettime(CLOCK_MONOTONIC, &start_timespec);

  IOStatus s;
  // io_uring* read_ring= new io_uring;
  // io_context_t* write_ioctx = new io_context_t;

  // *ret_write_ioctx=write_ioctx;
  // *ret_read_ring=read_ring;
  int write_reaped_n = 0;
  int read_reaped_n = 0;
  // std::vector<AsyncZoneCleaningIocb*> to_be_freed;
  uint64_t copied = 0;
  int read_fd=zbd_->GetFD(READ_FD);
  int extent_n = (int)migrate_exts->size();


 



  // The file may be deleted by other threads, better double check.
  auto zfile = GetFile(fname);
  if (zfile == nullptr) {
    // io_uring_queue_exit(&read_ring);
    return IOStatus::OK();
  }
  int err;
  ///////////////////
  // unsigned flags = IORING_SETUP_SQPOLL;
  // int err=io_uring_queue_init(extent_n, read_ring, flags);
  // if(err){
  //   printf("\t\t\tAsyncMigrateFileExtentsWorker io_uring_queue_init error@@@@@ %d %d\n",err,extent_n);
  // }
  // err = io_queue_init(extent_n,write_ioctx);

  // if(err){
  //   printf("\t\t\t\t AsyncMigrateFileExtentsWorker io_queue_init err %d %d",err,extent_n);
  // }
  //////////////////////////////
  
  // Don't migrate open for write files and prevent write reopens while we
  // migrate

// read throw

{
  // ZenFSStopWatch z1("Sum-read throw");
  for(auto* ext : (*migrate_exts)){
    struct AsyncZoneCleaningIocb* async_zc_read_iocb = 
          new AsyncZoneCleaningIocb(ext->filename,ext->start,ext->length,ext->header_size);
    // to_be_freed.push_back(async_zc_read_iocb);
    struct io_uring_sqe *sqe = io_uring_get_sqe(read_ring);
    io_uring_sqe_set_data(sqe,async_zc_read_iocb);
    io_uring_prep_read(sqe,read_fd,async_zc_read_iocb->buffer_,
                      async_zc_read_iocb->length_+async_zc_read_iocb->header_size_,
                      async_zc_read_iocb->start_-async_zc_read_iocb->header_size_);
    io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);
    err=io_uring_submit(read_ring);
    if(err==-errno){
      printf("io_uring_submit err? %d\n",err);
    }
    if (GetFileNoLock(fname) == nullptr) {
      Info(logger_, "Migrate file not exist anymore.");
      // for(size_t a = 0 ;a < to_be_freed.size();a++){
      //   free(to_be_freed[a]);
      // } 
      // io_uring_queue_exit(read_ring);

      return IOStatus::OK();
    }
  }
}

  // clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  // long elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  // printf("read throw breaktown %lu ms\n",(elapsed_ns_timespec/1000)/1000 );
  // clock_gettime(CLOCK_MONOTONIC, &start_timespec);

  std::vector<ZoneExtent*> new_extent_list;
  std::vector<ZoneExtent*> extents = zfile->GetExtents(); // old ext




  for (size_t i = 0; i < extents.size();i++) {
    ZoneExtent* ext=extents[i];
    ZoneExtent* new_ext=new ZoneExtent(ext->start_,ext->length_,nullptr,
          ext->fname_,ext->header_size_,zfile.get());
    new_ext->zone_=ext->zone_;
    new_extent_list.push_back(new_ext);    
  }
  struct timespec timeout;
  timeout.tv_sec=0;
  timeout.tv_nsec=0;
  struct __kernel_timespec kernel_timeout;
  kernel_timeout.tv_sec=0;
  kernel_timeout.tv_nsec=0;
  (void)(kernel_timeout);
  struct io_event write_events[1000];

// read reap, write throw
{
  // ZenFSStopWatch z1("Sum-read reap write throw");
  if (!zfile->TryAcquireWRLock()) {
    // io_uring_queue_exit(read_ring);
    return IOStatus::OK();
  }

  // std::vector<ZoneExtent*> tmp_migrated_ext;


  while(read_reaped_n < extent_n){
    struct io_uring_cqe* cqe = nullptr;


    {
      // ZenFSStopWatch cqewait("io_uring_wait_cqe");
      int result=io_uring_wait_cqe(read_ring, &cqe);
      if(result!=0){
        continue;
      }
    }
    // int result = io_uring_peek_cqe(read_ring, &cqe);
    // {
    //   ZenFSStopWatch cqewait("io_uring_peek_cqe");
    //   while(io_uring_peek_cqe(read_ring, &cqe)!=0);
    // }

    // int result = io_uring_wait_cqe_timeout(&read_ring, &cqe, &kernel_timeout);

    AsyncZoneCleaningIocb* reaped_read_iocb=reinterpret_cast<AsyncZoneCleaningIocb*>(cqe->user_data);

    // io_uring_cqe_seen(read_ring,cqe);


    auto it = std::find_if(new_extent_list.begin(), new_extent_list.end(),
                           [&](const ZoneExtent* new_ext) {
                              if(new_ext==nullptr){
                                printf("new_ext nullptr\n");
                                return false;
                              }
                              if(reaped_read_iocb==nullptr){
                                printf("reaped_read_iocb nullptr\n");
                                return false;
                              }
                                                            // new ext
                             return new_ext->start_ == reaped_read_iocb->start_ &&
                                    new_ext->length_ == reaped_read_iocb->length_;
                           });

    if (it == new_extent_list.end()) {
      Info(logger_, "Migrate extent not found, ext_start: %lu", reaped_read_iocb->start_);
      
      io_uring_cqe_seen(read_ring,cqe);
      free(reaped_read_iocb);
      continue;
      
    }
    Zone* target_zone=nullptr;
    ZoneExtent* cur_ext = (*it);

    s = zbd_->TakeMigrateZone(zfile->smallest_,zfile->largest_,zfile->level_, &target_zone, zfile->GetWriteLifeTimeHint(),
                                zfile->predicted_size_,
                                cur_ext->length_,&run_gc_worker_,zfile->IsSST());
  
    if(!run_gc_worker_){
      // for(size_t a = 0 ;a < to_be_freed.size();a++){
      //   free(to_be_freed[a]);
      // }

      // io_uring_queue_exit(read_ring);
      // io_destroy(*write_ioctx);
       io_uring_cqe_seen(read_ring,cqe);
      zbd_->ReleaseMigrateZone(target_zone);
      return IOStatus::OK();
    }
    if(target_zone!=nullptr&&target_zone->lifetime_==Env::WriteLifeTimeHint::WLTH_NOT_SET){
      target_zone->lifetime_=Env::WriteLifeTimeHint::WLTH_LONG;
    }
    uint64_t target_start = target_zone->wp_ + reaped_read_iocb->header_size_;
    cur_ext->header_size_=(*it)->header_size_;
    cur_ext->start_=target_start;
    cur_ext->zone_= target_zone;
    // reaped_read_iocb->iocb_.data=reaped_read_iocb;
    

    target_zone->ThrowAsyncZCWrite((*write_ioctx),reaped_read_iocb);

    target_zone->PushExtent(cur_ext);
    // tmp_migrated_ext.push_back(cur_ext);
    cur_ext->zone_->used_capacity_.fetch_add(cur_ext->length_);
    copied+=cur_ext->length_;
    read_reaped_n++;

    if (GetFileNoLock(fname) == nullptr) {
       io_uring_cqe_seen(read_ring,cqe);
      Info(logger_, "Migrate file not exist anymore.");
      zbd_->ReleaseMigrateZone(target_zone);
      // zfile->ReleaseWRLock();
      // for(ZoneExtent* de : new_extent_list){
        
      //   if(de==cur_ext){
      //     break;
      //   }
      // }


      break;
    }
    zbd_->ReleaseMigrateZone(target_zone);

    io_uring_cqe_seen(read_ring,cqe);

    
    
    // aggresive write reap
    int num_events;
    num_events = io_getevents((*write_ioctx), 1, 1000, write_events,
                              &timeout);
    if(num_events>=1){
      for(int e = 0 ;e <num_events;e++){
        struct io_event event = write_events[e];
        AsyncZoneCleaningIocb* reaped_write_iocb = static_cast<AsyncZoneCleaningIocb*>(event.data);
        if(reaped_write_iocb&&reaped_write_iocb->filename_==fname){
          write_reaped_n++;
          free(reaped_write_iocb);
        }
      }
    }



  }
}

    timeout.tv_sec = 0;
    timeout.tv_nsec = 1000; // 30ms
// write reap
{
    // ZenFSStopWatch z1("Sum-write reap");

  while(write_reaped_n<read_reaped_n){
    int num_events;
    // timeout;

    // timeout.tv_sec = 0;
    // timeout.tv_nsec = 1000000000; // 1000ms
    // int write_reap_min_nr = (extent_n-write_reaped_n) > 1 ? (extent_n-write_reaped_n) : 1;
    // write_reap_min_nr=1;

    num_events = io_getevents((*write_ioctx), (read_reaped_n-write_reaped_n), 1000, write_events,
                              &timeout);
    
    if(num_events<1){
      continue;
    }

    // if (GetFileNoLock(fname) == nullptr) {
    //   write_reaped_n++num_events
    //   continue;
    // }
    for(int e = 0 ;e <num_events;e++){
      struct io_event event = write_events[e];
      AsyncZoneCleaningIocb* reaped_write_iocb = static_cast<AsyncZoneCleaningIocb*>(event.data);
      if(reaped_write_iocb&&reaped_write_iocb->filename_==fname){
        write_reaped_n++;
        free(reaped_write_iocb);
      }
    }
    // write_reaped_n+=num_events;
  }
  PushBackAsyncStructure(read_ring,write_ioctx);
}

  // clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  // elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  // printf("write reap breaktown %lu ms\n",(elapsed_ns_timespec/1000)/1000 );
// sync
  // for(size_t a = 0 ;a < to_be_freed.size();a++){
  //   free(to_be_freed[a]);
  // }
{
  // ZenFSStopWatch z1("Sum-sync");
  // clock_gettime(CLOCK_MONOTONIC, &start_timespec);
  zbd_->AddGCBytesWritten(copied);
  SyncFileExtents(zfile.get(), new_extent_list);
  zfile->ReleaseWRLock();
  }
  // io_uring_queue_exit(&read_ring);
  // io_destroy(write_ioctx);

  // clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  // elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  // printf("cleanup breaktown %lu ms\n",(elapsed_ns_timespec/1000)/1000 );
  return IOStatus::OK();
}

uint64_t ZenFS::MigrateFileExtents(
    const std::string& fname,
    const std::vector<ZoneExtentSnapshot*>& migrate_exts) {
  IOStatus s = IOStatus::OK();
  uint64_t copied = 0;
  Info(logger_, "MigrateFileExtents, fname: %s, extent count: %lu",
       fname.data(), migrate_exts.size());
  uint64_t ret = 0;
  // The file may be deleted by other threads, better double check.
  auto zfile = GetFile(fname);
  if (zfile == nullptr) {
    return ret;
  }

  // Don't migrate open for write files and prevent write reopens while we
  // migrate
  if (!zfile->TryAcquireWRLock()) {
    return ret;
  }
  zfile->on_zc_.store(1);

  std::vector<ZoneExtent*> new_extent_list;
  std::vector<ZoneExtent*> extents = zfile->GetExtents(); // old ext

  for (size_t i = 0; i < extents.size();i++) {
    ZoneExtent* ext=extents[i];
    ZoneExtent* new_ext=new ZoneExtent(ext->start_,ext->length_,nullptr,
          ext->fname_,ext->header_size_,NowMicros(),zfile.get());
    new_ext->zone_=ext->zone_;
    new_ext->page_cache_ = std::move(ext->page_cache_);
    new_extent_list.push_back(new_ext);    
  }
{
    // ZenFSStopWatch z1("Sum-pread pwrite");
  // Modify the new extent list
  // printf("before finding in MigrateFileExtents\n");
  for (ZoneExtent* ext : new_extent_list) {
    // Check if current extent need to be migrated
    // printf("MigrateFileExtents :: %lu\n",ext->start_);
    auto it = std::find_if(migrate_exts.begin(), migrate_exts.end(),
                           [&](const ZoneExtentSnapshot* ext_snapshot) {
                              if(ext_snapshot==nullptr){
                                printf("ext_snapshot nullptr\n");
                                return false;
                              }
                              if(ext==nullptr){
                                printf("ext nullptr\n");
                                return false;
                              }
                                                            // new ext
                             return ext_snapshot->start == ext->start_ &&
                                    ext_snapshot->length == ext->length_;
                           });



    if (it == migrate_exts.end()) {
      Info(logger_, "Migrate extent not found, ext_start: %lu", ext->start_);
      // delete ext;
      continue;
    }


    Zone* target_zone = nullptr;
    // Zone* prev_zone = nullptr;
    // ZonedBlockDevice::ZoneReadLock zone_read_lock;
    // Allocate a new migration zone.
    // s = 

    // s = zbd_->TakeMigrateZone(&target_zone, zfile->GetWriteLifeTimeHint(),
    //                           ext->length_,&run_gc_worker_);
    // if(!zfile->IsSST()){
    //   zfile->level
    // }
      s = zbd_->TakeMigrateZone(zfile->smallest_,zfile->largest_,zfile->level_, &target_zone, zfile->GetWriteLifeTimeHint(),
                              zfile->predicted_size_,
                              ext->length_,&run_gc_worker_,zfile->IsSST());
    if(!run_gc_worker_){
      zfile->ReleaseWRLock();
      return ret;
    }
    if(target_zone!=nullptr&&target_zone->lifetime_==Env::WriteLifeTimeHint::WLTH_NOT_SET){
      target_zone->lifetime_=Env::WriteLifeTimeHint::WLTH_LONG;
    }
    if (!s.ok()) {
      continue;
    }

    if (target_zone == nullptr) {
      zbd_->ReleaseMigrateZone(target_zone);
      Info(logger_, "Migrate Zone Acquire Failed, Ignore Task.");
      printf("Migrate Zone Acquire Failed, Ignore Task.\n");
      continue;
    }
    // prev_zone = ext->zone_;
    // zone_read_lock.ReadLockZone(prev_zone);
    uint64_t target_start = target_zone->wp_;
    copied += ext->length_;
    if(ext->page_cache_!=nullptr){
      ret+=(ext->length_>>20);
    }
    if (zfile->IsSparse()) {
      // For buffered write, ZenFS use inlined metadata for extents and each
      // extent has a SPARSE_HEADER_SIZE.
      target_start = target_zone->wp_ + ZoneFile::SPARSE_HEADER_SIZE;
      zfile->MigrateData(ext->start_ - ZoneFile::SPARSE_HEADER_SIZE,
                         ext->length_ + ZoneFile::SPARSE_HEADER_SIZE,
                         target_zone,   ext->page_cache_);
      ext->header_size_=ZoneFile::SPARSE_HEADER_SIZE;
      copied +=ZoneFile::SPARSE_HEADER_SIZE;
    } else {
      zfile->MigrateData(ext->start_, ext->length_, target_zone,
      ext->page_cache_      );
    }

    if(!run_gc_worker_){
      zfile->ReleaseWRLock();
      return ret;
    }

    // prev_zone = ext->zone_;
    // zone_read_lock.ReadLockZone(prev_zone);
    uint64_t align = (ext->length_+ext->header_size_)%4096;
    if(align){
      ext->pad_size_=4096-align;
    }
    ext->start_ = target_start;
    ext->zone_ = target_zone;
    if(ext->zone_==nullptr){
      printf("target zone nullpt@@@@\n");
    }
    target_zone->PushExtent(ext);
    ext->zone_->used_capacity_ += ext->length_;
    
    // If the file doesn't exist, skip
    if (GetFileNoLock(fname) == nullptr) {
      Info(logger_, "Migrate file not exist anymore.");
      zbd_->ReleaseMigrateZone(target_zone);
      // for (ZoneExtent* de : new_extent_list) {
      //   de->is_invalid_=true;
      // }
      break;
    }
    zbd_->ReleaseMigrateZone(target_zone);
  }
}
{
    // ZenFSStopWatch z1("Sum-sync");
  // printf("after finding in MigrateFileExtents 1\n");
  zbd_->AddGCBytesWritten(copied);
// {

  SyncFileExtents(zfile.get(), new_extent_list);
  // }
  zfile->ReleaseWRLock();
  zfile->on_zc_.store(0);
  // printf("after finding in MigrateFileExtents 1.5\n");
  Info(logger_, "MigrateFileExtents Finished, fname: %s, extent count: %lu",
       fname.data(), migrate_exts.size());
    // printf("after finding in MigrateFileExtents 22\n");

    }
  return ret;
}

extern "C" FactoryFunc<FileSystem> zenfs_filesystem_reg;

FactoryFunc<FileSystem> zenfs_filesystem_reg =
#if (ROCKSDB_MAJOR < 6) || (ROCKSDB_MAJOR <= 6 && ROCKSDB_MINOR < 28)
    ObjectLibrary::Default()->Register<FileSystem>(
        "zenfs://.*", [](const std::string& uri, std::unique_ptr<FileSystem>* f,
                         std::string* errmsg) {
#else
    ObjectLibrary::Default()->AddFactory<FileSystem>(
        ObjectLibrary::PatternEntry("zenfs", false)
            .AddSeparator("://"), /* "zenfs://.+" */
        [](const std::string& uri, std::unique_ptr<FileSystem>* f,
           std::string* errmsg) {
#endif
          std::string devID = uri;
          FileSystem* fs = nullptr;
          Status s;

          devID.replace(0, strlen("zenfs://"), "");
          if (devID.rfind("dev:") == 0) {
            devID.replace(0, strlen("dev:"), "");
#ifdef ZENFS_EXPORT_PROMETHEUS
            s = NewZenFS(&fs, ZbdBackendType::kBlockDev, devID,
                         std::make_shared<ZenFSPrometheusMetrics>());
#else
            s = NewZenFS(&fs, ZbdBackendType::kBlockDev, devID);
#endif
            if (!s.ok()) {
              *errmsg = s.ToString();
            }
          } else if (devID.rfind("uuid:") == 0) {
            std::map<std::string, std::pair<std::string, ZbdBackendType>>
                zenFileSystems;
            s = ListZenFileSystems(zenFileSystems);
            if (!s.ok()) {
              *errmsg = s.ToString();
            } else {
              devID.replace(0, strlen("uuid:"), "");

              if (zenFileSystems.find(devID) == zenFileSystems.end()) {
                *errmsg = "UUID not found";
              } else {

#ifdef ZENFS_EXPORT_PROMETHEUS
                s = NewZenFS(&fs, zenFileSystems[devID].second,
                             zenFileSystems[devID].first,
                             std::make_shared<ZenFSPrometheusMetrics>());
#else
                s = NewZenFS(&fs, zenFileSystems[devID].second,
                             zenFileSystems[devID].first);
#endif
                if (!s.ok()) {
                  *errmsg = s.ToString();
                }
              }
            }
          } else if (devID.rfind("zonefs:") == 0) {
            devID.replace(0, strlen("zonefs:"), "");
            s = NewZenFS(&fs, ZbdBackendType::kZoneFS, devID);
            if (!s.ok()) {
              *errmsg = s.ToString();
            }
          } else {
            *errmsg = "Malformed URI";
          }
          f->reset(fs);
          return f->get();
        });
};  // namespace ROCKSDB_NAMESPACE

#else

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
Status NewZenFS(FileSystem** /*fs*/, const ZbdBackendType /*backend_type*/,
                const std::string& /*backend_name*/,
                ZenFSMetrics* /*metrics*/) {
  return Status::NotSupported("Not built with ZenFS support\n");
}
std::map<std::string, std::string> ListZenFileSystems() {
  std::map<std::string, std::pair<std::string, ZbdBackendType>> zenFileSystems;
  return zenFileSystems;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
