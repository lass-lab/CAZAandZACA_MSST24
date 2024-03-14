// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <cmath>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "snapshot.h"
#include "zbdlib_zenfs.h"
#include "zonefs_zenfs.h"
#include <time.h>


/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */


/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (1)

namespace ROCKSDB_NAMESPACE {

Zone::Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
           std::unique_ptr<ZoneList> &zones, uint64_t idx)
    : zbd_(zbd),
      zbd_be_(zbd_be),
      start_(zbd_be->ZoneStart(zones, idx)), 
      max_capacity_(zbd_be->ZoneMaxCapacity(zones, idx)),
      wp_(zbd_be->ZoneWp(zones, idx)),
      zidx_(idx),
      log2_erase_unit_size_(0) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  zone_sz_= zbd_be_->GetZoneSize();
  block_sz_ = zbd_be_->GetBlockSize();
  if (zbd_be->ZoneIsWritable(zones, idx)){
    uint64_t relative_wp = wp_ - start_;
    if(relative_wp>max_capacity_){
      capacity_=0;
    }else{
      capacity_=max_capacity_-relative_wp;
    }
    // capacity_ = max_capacity_ - (wp_ - start_);
  
  }
  zidx_=idx-zbd_->ZENFS_CONVENTIONAL_ZONE;
  // printf("max %lu cap %lu\n",max_capacity_,capacity_);
}
Zone::Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
           std::unique_ptr<ZoneList> &zones, uint64_t idx,
           unsigned int log2_erase_unit_size)
    : zbd_(zbd),
      zbd_be_(zbd_be),
      start_(zbd_be->ZoneStart(zones, idx)), 
      max_capacity_(zbd_be->ZoneMaxCapacity(zones, idx)),
      wp_(zbd_be->ZoneWp(zones, idx)),
      zidx_(idx),
      log2_erase_unit_size_(log2_erase_unit_size) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  zone_sz_= zbd_be_->GetZoneSize();
  block_sz_ = zbd_be_->GetBlockSize();
  if (zbd_be->ZoneIsWritable(zones, idx)){
    uint64_t relative_wp = wp_ - start_;
    if(relative_wp>max_capacity_){
      capacity_=0;
    }else{
      capacity_=max_capacity_-relative_wp;
    }
  }
  zidx_=idx-(zbd_->ZENFS_CONVENTIONAL_ZONE);
  // printf("max %lu cap %lu\n",max_capacity_,capacity_);
  erase_unit_size_=(1<<log2_erase_unit_size_);
}

bool Zone::IsUsed() { return (used_capacity_ > 0); }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}


void ZonedBlockDevice::AddBreakDown(std::string name, uint64_t us){
// std::map<std::string, std::pair<std::atomic<uint64_t>,std::atomic<uint64_t>>> breakdown_map;
  if(breakdown_map.find(name)==breakdown_map.end()){
    // breakdown_map[name].first=1;
    // breakdown_map[name].second=us/1000;

    // BreakDown* new_breakdown= new BreakDown();
    // new_breakdown
    breakdown_map[name]=(new BreakDown(1,us));


  }else{
    breakdown_map[name]->count_++;
    breakdown_map[name]->us_+=us;
    //  breakdown_map[name].first++;
    //  breakdown_map[name].second+=us/1000;
  }
}

void ZonedBlockDevice::PrintCumulativeBreakDown(){
  for(auto it : breakdown_map){
    std::string name = it.first;
    uint64_t count=it.second->count_;
    uint64_t ms = it.second->us_/1000;
    printf("%s : %lu/%lu = %lu\n",name.c_str(),ms,count,ms/count);
  }
}
uint64_t ZenFSStopWatch::RecordTickNS(){
  clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  return (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  // return 
}
double ZenFSStopWatch::RecordTickMS(){
  clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  long long ns =(end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  // return 
  return ((double)( (double)(ns/1000) )/1000.0);
}

ZenFSStopWatch::~ZenFSStopWatch(){
    clock_gettime(CLOCK_MONOTONIC, &end_timespec);
    long elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
    // printf("\t\t\t\t\t%s breakdown %lu (ms)\n",name.c_str(),(elapsed_ns_timespec/1000)/1000);
    if(zbd_!=nullptr){
      zbd_->AddBreakDown(name,elapsed_ns_timespec/1000);
    }
    // else{
      
    // }
    // printf("\t\t\t\t\t%s breakdown %lu (ms)\n",name.c_str(),(elapsed_ns_timespec/1000)/1000);
}

IOStatus Zone::AsyncReset(){
  bool offline;
  uint64_t max_capacity;

  assert(!IsUsed());
 {
  // ZenFSStopWatch z1("async-reset");


  IOStatus ios = zbd_be_->Reset(start_, &offline, &max_capacity);
  if (ios != IOStatus::OK()) return ios;

  if (offline)
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = max_capacity;



  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;



    for(ZoneExtent* ze : zone_extents_ ){
      delete ze;
    }
    zone_extents_.clear();
  // Release();
  zone_lock_.unlock();
  }
  return IOStatus::OK();
}


IOStatus Zone::Reset() {
  bool offline;
  uint64_t max_capacity;

  assert(!IsUsed());

  // ZenFSStopWatch z1("zone-reset");

  IOStatus ios = zbd_be_->Reset(start_, &offline, &max_capacity);
  if (ios != IOStatus::OK()) return ios;

  if (offline)
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = max_capacity;

  state_=EMPTY;

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;



    for(ZoneExtent* ze : zone_extents_ ){
      delete ze;
    }
    zone_extents_.clear();

  return IOStatus::OK();
}

IOStatus Zone::PartialResetToAllInvalidZone(size_t erase_sz){
  // bool offline;
  // uint64_t max_capacity;
  uint64_t pad_size;
  IOStatus ios;
  assert(!IsUsed());
  for(ZoneExtent* ze : zone_extents_ ){
    delete ze;
  }
  zone_extents_.clear();
  pad_size=(wp_-start_)-erase_sz;
  if(pad_size){
    new ZoneExtent(start_,pad_size,this);
  }
  ios = zbd_be_->PartialReset( (zidx_* zbd_be_->GetZoneSize()) ,erase_sz,false);
  if (ios != IOStatus::OK()) return ios;
  capacity_+=erase_sz;
  wp_-=erase_sz;
  // lifetime_ = Env::WLTH_NOT_SET;
  return ios;
}

/*
eu size 64
valid starts at 67
64 67
0~63 reset

pad : 67(valid starts at) - 64 = 3, length 3
64 -> 0
65 -> 1
66 -> 2 
valid
67 -> 3 : wp - erase unit
68 -> 4

ppppppppp
67-64
*/
//////////////// zone extent lock should be held
IOStatus Zone::PartialReset(size_t* erase_sz){

  // assert(IsBusy());
  size_t i;
  uint64_t invalid_wp = 0;
  uint64_t erase_size = 0;
  uint64_t to_be_erased_unit_n = 0;
  uint64_t pad_size = 0;
  // std::deque<ZoneExtent*> to_be_deleted_extent;
  bool after_cut= false;

  // to_be_deleted_extent.clear();
  if(zbd_->RuntimeZoneResetOnly()){
    return IOStatus::IOError("Not Supported\n");
  }
  sort(zone_extents_.begin(),zone_extents_.end(),ZoneExtent::cmp);
  for(i = 0; i<zone_extents_.size();i++){
    if(zone_extents_[i]->is_invalid_==false){
      break;
    }
    invalid_wp += zone_extents_[i]->length_ + zone_extents_[i]->header_size_ + zone_extents_[i]->pad_size_;
  }

  to_be_erased_unit_n = invalid_wp >> log2_erase_unit_size_;
  
  erase_size = to_be_erased_unit_n << log2_erase_unit_size_;

  if(to_be_erased_unit_n == 0){
    return IOStatus::OK();
  }


 

  zbd_->StatsPartialReset(to_be_erased_unit_n);
  

  // uint64_t relative_wp =wp_-start_;
  // uint64_t expected_wp = PrintZoneExtent(false);
 
  // if(expected_wp!=relative_wp){
  //   printf("invalid wp : %lu, erase size :%lu\n",invalid_wp,erase_size);
  //   printf("before %lu expected : %lu\n",relative_wp,expected_wp);
  //   PrintZoneExtent(true);
  //   sleep(1);
  //   exit(-1);
  //   return IOStatus::IOError("Before partial reset : wp not aligned\n");
  // }

  //////////////////////////////

  invalid_wp=0;

  // delete until erase size, not all

  while(after_cut==false){
    // if(after_cut==true){
    //   break;
    // }


    ZoneExtent* ze = zone_extents_.front();
    invalid_wp += ze->length_  + ze->header_size_ + ze->pad_size_;
    if(invalid_wp==erase_size){
      // to_be_deleted_extent.push_back(zone_extents_[i]);
      after_cut=true;
    }else if(invalid_wp>erase_size ){
      pad_size = invalid_wp-erase_size;
      // printf("BREAK AT [%lu] %lu>%lu\n",i,invalid_wp,erase_size);
      after_cut=true;
    }
    if(ze->is_invalid_==false){
      printf("@@@@@@@@@@@@ error here@@@@@@@@@@@22\n");
      sleep(10);
    }

    zone_extents_.pop_front();
    delete ze;
  }


  // printf("@@@@@@@ USER LEVEL PARITAL RESET [%lu] start_ %lu %lu zsize %lu extents size %lu\n",zidx_,start_,erase_size,zbd_be_->GetZoneSize(),zone_extents_.size());
  IOStatus ios = zbd_be_->PartialReset( (zidx_* zbd_be_->GetZoneSize()) ,erase_size,true);

  if (ios != IOStatus::OK()){ 
    return ios;
  }
  // // update valid
  for(i = 0; i < zone_extents_.size();i++){
    // ZoneExtent* ext= zone_extents_[i];
    // ZoneExtent* d_ext= to_be_deleted_extent[i];
      zone_extents_[i]->start_-=erase_size;
      zone_extents_[i]->position_pulled_= true;
  }



  


  // push invalid pad
  // printf("PUSH FRONT PAD :%lu\n",pad_size);
  if(pad_size){
    
    new ZoneExtent(start_,pad_size,this);
  }

  wp_-=erase_size;
  capacity_+=erase_size;
  // uint64_t after_wp =  PrintZoneExtent(false);
  // if(after_wp!=wp_-start_){

  //   after_wp =  PrintZoneExtent(true);
  //   printf("after expected wp : %lu / actual : %lu // erase size :%lu\n",after_wp,wp_-start_,erase_size);
  //   sleep(1);
  //   exit(-1);
  //   return IOStatus::IOError("After partial reset : wp not aligned\n");
  // } 


  
  *erase_sz=erase_size;
  return IOStatus::OK();
}


IOStatus Zone::Finish() {
  // assert(IsBusy());

  IOStatus ios = zbd_be_->Finish(start_);
  if (ios != IOStatus::OK()) return ios;
  state_=FINISH;
  capacity_ = 0;
  wp_ = start_ + zbd_->GetZoneSize();

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  // assert(IsBusy());

  if (! (IsEmpty() || IsFull()) ) {
    IOStatus ios = zbd_be_->Close(start_);

   
    if (ios != IOStatus::OK()) return ios;
  }

  if(IsFull()){
    state_=FINISH;
  }else{
    state_=CLOSE;
  }

  return IOStatus::OK();
}

IOStatus Zone::ThrowAsyncZCWrite(io_context_t& ioctx, AsyncZoneCleaningIocb* aiocb){
  uint64_t align = 0;
  uint64_t wr_size = aiocb->length_+aiocb->header_size_;
  align = (wr_size) % zbd_->GetBlockSize();
  if(align != 0){
    // printf("ThrowAsyncZCWrite %lu %lu",aiocb->length_,aiocb->header_size_);
    wr_size= (wr_size+ zbd_->GetBlockSize()-align);
  }
  
  struct iocb* iocb=&(aiocb->iocb_);
  
  io_prep_pwrite((iocb), zbd_->GetFD(WRITE_DIRECT_FD), 
    aiocb->buffer_, wr_size, wp_);
  aiocb->iocb_.data=aiocb;
  int res = io_submit(ioctx, 1, &(iocb));
  if(res==1){

    wp_+=wr_size;
    capacity_-=wr_size;
    zbd_->AddBytesWritten(wr_size);
    // zbd_->AddBytesWritten(wr_size);
    return IOStatus::OK();
  }
  printf("ThrowAsyncZCWrite res %d\n",res);
  return IOStatus::IOError(strerror(errno));
}

IOStatus Zone::ThrowAsyncUringZCWrite(io_uring* write_ring, AsyncZoneCleaningIocb* aiocb){
  uint64_t align = 0;
  uint64_t wr_size = aiocb->length_+aiocb->header_size_;
  align = (wr_size) % zbd_->GetBlockSize();
  if(align != 0){
    // printf("ThrowAsyncZCWrite %lu %lu",aiocb->length_,aiocb->header_size_);
    wr_size= (wr_size+ zbd_->GetBlockSize()-align);
  }

  // int res = io_submit(ioctx, 1, &(iocb));
  struct io_uring_sqe *sqe = io_uring_get_sqe(write_ring);
  io_uring_sqe_set_flags(sqe, IOSQE_ASYNC); // sqe polling
  if(sqe==nullptr){
    printf("ThrowAsyncUringZCWrite sqe nullptr\n");
  }
  io_uring_sqe_set_data(sqe,aiocb);
  io_uring_prep_write(sqe,zbd_->GetFD(WRITE_DIRECT_FD), 
                aiocb->buffer_,
                wr_size,
                wp_);

  int res=io_uring_submit(write_ring);
  if(res==-errno){
    printf("ThrowAsyncZCWrite res %d\n",res);
    return IOStatus::IOError(strerror(errno));
  }
  // if(res==-errno){

    wp_+=wr_size;
    capacity_-=wr_size;
    zbd_->AddBytesWritten(wr_size);
    // zbd_->AddBytesWritten(wr_size);
    if(wp_%4096){
      printf("ThrowAsyncUringZCWrite error ? %lu mod 4096 = %lu",wp_,wp_%4096);
    }
    return IOStatus::OK();
  // }

}

IOStatus Zone::Append(char *data, uint64_t size, bool zc) {
  // ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_ZONE_WRITE_LATENCY,
  //                                Env::Default());
  // zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_WRITE_THROUGHPUT, size);e
  char *ptr = data;
  uint64_t left = size;
  int ret;
  (void)(zc);
  // if(!zc  && zbd_->GetZCRunning()){
  //   while(zbd_->GetZCRunning());
  // }

  // std::lock_guard<std::mutex> lg_wp(wp_lock_);
  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  while (left) {
    ret = zbd_be_->Write(ptr, left, wp_);
    if (ret < 0) {
      printf("Error@@@@@@@@@\n");
      return IOStatus::IOError(strerror(errno));
    }


    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
    // if(zc==false){
      if(zbd_){
      zbd_->AddBytesWritten(ret);
      }
    // }
  }

  return IOStatus::OK();
}


Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zbd_be_->GetZoneSize()))
      return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string path, ZbdBackendType backend,
                                   std::shared_ptr<Logger> logger,
                                   std::shared_ptr<ZenFSMetrics> metrics)
    : logger_(logger), metrics_(metrics) {
  ZBD_mount_time_=clock();
  if (backend == ZbdBackendType::kBlockDev) {
    zbd_be_ = std::unique_ptr<ZbdlibBackend>(new ZbdlibBackend(path));
    Info(logger_, "New Zoned Block Device: %s", zbd_be_->GetFilename().c_str());
  } else if (backend == ZbdBackendType::kZoneFS) {
    zbd_be_ = std::unique_ptr<ZoneFsBackend>(new ZoneFsBackend(path));
    Info(logger_, "New zonefs backing: %s", zbd_be_->GetFilename().c_str());
  }
  zone_sz_ = zbd_be_->GetZoneSize();
  
  sst_file_bitmap_= new ZoneFile*[1<<20];
  memset(sst_file_bitmap_,0,(1<<20));

}

IOStatus ZonedBlockDevice::Open(bool readonly, bool exclusive) {



  std::unique_ptr<ZoneList> zone_rep;
  unsigned int max_nr_active_zones;
  unsigned int max_nr_open_zones;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
#if ZENFS_SPARE_ZONES>0
  uint64_t sp = 0;
#endif
  // Reserve one zone for metadata and another one for extent migration
  int reserved_zones = 4;
  // int migrate_zones = 1;
  // printf("ZonedBlockDevice::Open@@\n");
  if (!readonly && !exclusive)
    return IOStatus::InvalidArgument("Write opens must be exclusive");

  IOStatus ios = zbd_be_->Open(readonly, exclusive, &max_nr_active_zones,
                               &max_nr_open_zones,&log2_erase_unit_size_);
  printf("ZonedBlockDevice::Open :: %u\n",log2_erase_unit_size_);
  if (ios != IOStatus::OK()) return ios;

  if (zbd_be_->GetNrZones() < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported("To few zones on zoned backend (" +
                                  std::to_string(ZENFS_MIN_ZONES) +
                                  " required)");
  }

  if (max_nr_active_zones == 0)
    max_nr_active_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_active_io_zones_ = max_nr_active_zones - reserved_zones - max_migrate_zones_;

  if (max_nr_open_zones == 0)
    max_nr_open_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_open_io_zones_ = max_nr_open_zones - reserved_zones - max_migrate_zones_;

  Info(logger_, "Zone block device nr zones: %lu max active: %u max open: %u \n",
       zbd_be_->GetNrZones(), max_nr_active_zones, max_nr_open_zones);
  printf("Zone block device nr zones: %lu max active: %u (%d) max open: %u(%d) \n",
       zbd_be_->GetNrZones(), max_nr_active_zones,max_nr_active_io_zones_.load(), max_nr_open_zones,max_nr_open_io_zones_);
  zone_rep = zbd_be_->ListZones();

  if(log2_erase_unit_size_>0){
    printf("PARTIAL RESET ENABLED log2_erase_unit_size_:: log2 size : %u\n",log2_erase_unit_size_);
  }else{
    printf("PARTIAL RESET DISABLED\n");
  }

  if (zone_rep == nullptr || zone_rep->ZoneCount() != zbd_be_->GetNrZones()) {
    Error(logger_, "Failed to list zones");
    return IOStatus::IOError("Failed to list zones");
  }
#if ZENFS_SPARE_ZONES>0
  while(sp<ZENFS_SPARE_ZONES&&i<zone_rep->ZoneCount()){
    if(zbd_be_->ZoneIsSwr(zone_rep,i)){
      if(!zbd_be_->ZoneIsOffline(zone_rep,i)){
        spare_zones.push_back(new Zone(this,zbd_be_.get(),zone_rep,i));
      }
      sp++;
    }else{
      ZENFS_CONVENTIONAL_ZONE++;
    }
    i++;
  }
#endif
  for(int level = 0; level < 9; level++){
    // std::atomic<uint64_t> a;
    // lsm_tree_.push_back(a);
    lsm_tree_[level]=0;
    same_zone_score_atomic_[level]=0;
    invalidate_score_atomic_[level]=0;
    compaction_triggered_[level]=0;
  }
  while (m < ZENFS_META_ZONES && i < zone_rep->ZoneCount()) {
    /* Only use sequential write required zones */
    // if(i<73){
    //   i++;
    //   continue;
    // }
    if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        meta_zones.push_back(new Zone(this, zbd_be_.get(), zone_rep, i));
      }
      m++;
    }else{
      ZENFS_CONVENTIONAL_ZONE++;
    }
    i++;
  }
// zbd_be->ZoneMaxCapacity
  active_io_zones_ = 0;
  open_io_zones_ = 0;
  uint64_t device_io_capacity= (1<<log2_DEVICE_IO_CAPACITY);
  device_io_capacity=device_io_capacity<<30;
  for (; i < zone_rep->ZoneCount() && (io_zones.size()*meta_zones[0]->max_capacity_)<(device_io_capacity);  i++) {
    /* Only use sequential write required zones */

    if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        Zone *newZone = new Zone(this, zbd_be_.get(), zone_rep, i,log2_erase_unit_size_);
        if (!newZone->Acquire()) {
          assert(false);
          return IOStatus::Corruption("Failed to set busy flag of zone " +
                                      std::to_string(newZone->GetZoneNr()));
        }
        io_zones.push_back(newZone);
        if (zbd_be_->ZoneIsActive(zone_rep, i)) {
          printf("active resoruced %lu\n",active_io_zones_.load());
          active_io_zones_++;
          if (zbd_be_->ZoneIsOpen(zone_rep, i)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
        newZone->Release();
      }
    }else{
      ZENFS_CONVENTIONAL_ZONE++;
    }
  }
    for(uint64_t f=0;f<=100;f++){
      CalculateResetThreshold(f);
    }
  // uint64_t device_free_space=(i-ZENFS_META_ZONES-ZENFS_SPARE_ZONES)*zbd_be_->GetZoneSize();
  uint64_t device_free_space=io_zones.size()*meta_zones[0]->max_capacity_;
  printf("device free space : %ld\n",BYTES_TO_MB(device_free_space));
  printf("zone sz %lu\n",zone_sz_);
  device_free_space_.store(device_free_space);

  // if(zone_sz_>(1<<30)){
  //   ZONE_CLEANING_KICKING_POINT=40;
  // }else{
    ZONE_CLEANING_KICKING_POINT=20;
  // }
  // for(uint64_t fr = 100;fr>0;fr--){
  //   uint64_t lazylog=LazyLog(io_zones[0]->max_capacity_,fr,70);
  //   uint64_t lazylinear=LazyLinear(io_zones[0]->max_capacity_,fr,70);


  //   printf("%ld : %ld / %ld , diff : %ld\n",fr,lazylinear,lazylog,lazylinear-lazylog);
  // }
  // start_time_ = time(NULL);
  for(i = 0 ; i<=256; i++){
    cost_[READ_DISK_COST][i]=ReadDiskCost(i);
    cost_[READ_PAGE_COST][i]=ReadPageCacheCost(i);
    cost_[WRITE_COST][i]=WriteCost(i);
  }
  return IOStatus::OK();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
  }
  return reclaimable;
}

uint64_t ZonedBlockDevice::GetFreePercent(void ){
    // uint64_t non_free = zbd_->GetUsedSpace() + zbd_->GetReclaimableSpace();
    uint64_t free = GetFreeSpace();
    return (100 * free) / io_zones.size()*io_zones[0]->max_capacity_;
}
uint64_t ZonedBlockDevice::GetFreePercent(uint64_t diskfree){
  return (100 * diskfree) / (io_zones.size()*io_zones[0]->max_capacity_);
}
void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());
}

void ZonedBlockDevice::LogZoneUsage() {
  int i = 0;
  for (const auto z : io_zones) {
    uint64_t used = z->used_capacity_;
    // printf("@@@ LogZoneUsage [%d] :  remaining : %lu used : %lu invalid : %lu wp : %lu\n",
    //             i,z->capacity_,used,
    //             z->wp_-z->start_-used ,
    //             z->wp_-z->start_ );
    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
    i++;
  }
}

void ZonedBlockDevice::LogGarbageInfo() {
  // Log zone garbage stats vector.
  //
  // The values in the vector represents how many zones with target garbage
  // percent. Garbage percent of each index: [0%, <10%, < 20%, ... <100%, 100%]
  // For example `[100, 1, 2, 3....]` means 100 zones are empty, 1 zone has less
  // than 10% garbage, 2 zones have  10% ~ 20% garbage ect.
  //
  // We don't need to lock io_zones since we only read data and we don't need
  // the result to be precise.
  int zone_gc_stat[12] = {0};
  for (auto z : io_zones) {
    if (!z->Acquire()) {
      continue;
    }

    if (z->IsEmpty()) {
      zone_gc_stat[0]++;
      z->Release();
      continue;
    }

    double garbage_rate =
        double(z->wp_ - z->start_ - z->used_capacity_) / z->max_capacity_;
    assert(garbage_rate > 0);
    int idx = int((garbage_rate + 0.1) * 10);
    zone_gc_stat[idx]++;

    z->Release();
  }

  std::stringstream ss;
  ss << "Zone Garbage Stats: [";
  for (int i = 0; i < 12; i++) {
    ss << zone_gc_stat[i] << " ";
  }
  ss << "]";
  Info(logger_, "%s", ss.str().data());
}

ZonedBlockDevice::~ZonedBlockDevice() {
  size_t rc = reset_count_.load();
  uint64_t wwp=BYTES_TO_MB(wasted_wp_.load()); //MB
  uint64_t R_wp;
  uint64_t zone_sz=BYTES_TO_MB(GetZoneSize()); // MB
  size_t total_erased;
  long long read_lock_overhead_sum = 0;
  printf("zone size at ~ %lu\n",zone_sz);
  if((rc+reset_count_zc_.load())==0){
    R_wp= 100;
  }else{
    R_wp= (zone_sz*100-wwp*100/(rc+reset_count_zc_.load()))/zone_sz; // MB
  }
  printf("============================================================\n");
  printf("FAR STAT 1 :: WWP (MB) : %lu, R_wp : %lu\n",
         BYTES_TO_MB(wasted_wp_.load()), R_wp);
  printf("FAR STAT 2 :: ZC IO Blocking time : %d, Compaction Refused : %lu\n",
         zone_cleaning_io_block_, compaction_blocked_at_amount_.size());
  printf("FAR STAT 4 :: Zone Cleaning Trigger Time Lapse\n");
  printf("============================================================\n");
  uint64_t total_copied = 0;
  size_t rc_zc = 0;
  int io_blocking_sum=0;
  long long io_blocking_ms_sum= 0;
  uint64_t page_cache_hit_size_sum = 0;
  for(size_t i=0;i<zc_timelapse_.size();i++){
    // bool forced=zc_timelapse_[i].forced;
    // size_t zc_z=zc_timelapse_[i].zc_z;
    int s= zc_timelapse_[i].s;
    int e= zc_timelapse_[i].e;
    long long us=zc_timelapse_[i].us;
    io_blocking_sum+=e-s+1;
    io_blocking_ms_sum+=us/1000;
    uint64_t invalid_data_size = zc_timelapse_[i].invalid_data_size;
    uint64_t valid_data_size = zc_timelapse_[i].valid_data_size;
    uint64_t invalid_ratio = zc_timelapse_[i].invalid_ratio;
    uint64_t page_cache_hit_size = zc_timelapse_[i].page_cache_hit_size;
    page_cache_hit_size_sum+=page_cache_hit_size;
    printf("[%lu]\t%d ~ %d\t %llu ms,\t %ld (MB)\t%lu\t%lu\t%lu\t%lu\n",i+1,s,e,us/1000,(zc_timelapse_[i].copied>>20),
        invalid_data_size,valid_data_size,invalid_ratio,page_cache_hit_size );
    total_copied+=zc_timelapse_[i].copied;
    rc_zc ++;
  }
  
  printf("Total ZC Copied (MB) :: %lu(%lu), Recaimed by ZC :: %lu \n",
         BYTES_TO_MB(total_copied),(gc_bytes_written_.load())>>20, rc_zc);
  printf("============================================================\n\n");
  // printf("FAR STAT  :: Reset Count (R+ZC) : %ld+%ld=%ld\n", rc - rc_zc, rc_zc,
        //  rc);
  // printf(
  //     "FAR STAT 5 :: Total Run-time Reset Latency : %0.2lf, avg Zone Reset "
  //     "Latency : %0.2lf\n",
  //     (double)runtime_reset_latency_.load() / CLOCKS_PER_SEC,
  //     (double)runtime_reset_reset_latency_.load() /
  //         (CLOCKS_PER_SEC * (rc - rc_zc)));

  // printf("============================================================\n\n");
  // printf("FAR STAT 6-1 :: I/O Blocked Time Lapse\n");
  // printf("============================================================\n");

  printf("FAR STAT 6-2 :: Compaction Blocked TimeLapse\n");
  printf("============================================================\n");
  for(size_t i =0 ;i <compaction_blocked_at_amount_.size();i++){
    printf(" %d | ",compaction_blocked_at_amount_[i]);
  }
  printf("\n============================================================\n");
  // size_t j = 0;
  for (auto it=compaction_blocked_at_.begin(); it != compaction_blocked_at_.end(); ++it){     
    std::cout <<*it<<" | ";
  }
  printf("\n============================================================\n");
  for(size_t i=0;i<io_block_timelapse_.size() ;i++){
    int s= io_block_timelapse_[i].s;
    int e= io_block_timelapse_[i].e;
    pid_t tid=io_block_timelapse_[i].tid;
    printf("[%lu] :: (%d) %d ~ %d\n",i+1,tid%100,s,e );
  }
  printf("============================================================\n\n");
  printf("FAR STAT :: Time Lapse\n");
  //   T       fr      rc   rc_zc p_rc  Rwp     Twp   erase_sz   p_er_sz
  // printf(
  //     "Sec    | Free |  RC |  RCZ |  RCP  | R_wp    |   Twp    |   erase_sz     |    erase_sz_zc |   p_er_sz      |\n");
  printf(
    "Sec \tFree \tRC \tRCZ \tRCP \tR_wp \tTwp \terase_sz \terase_sz_zc \tp_er_sz\n");
  
  
  for(size_t i=0;i<far_stats_.size();i++){
    far_stats_[i].PrintStat();
    // write_stall_timelapse_[i].PrintStat();
  }

  printf("============================================================\n");
  printf("FAR STAT :: Partial Zone Reset %u\n",partial_reset_scheme_);
  printf("called n : %lu erased unit n : %lu\n",partial_reset_called_n.load(),partial_reset_total_erased_n.load());
  printf("Partial Zone Reset Decoupling : total : %lu, to valid %lu\n",
                                                    partial_erase_size_.load()>>20,
                                                    partial_to_valid_erase_size_.load()>>20);
  if(reset_resigned_.load()!=0){
    printf("RUNTIME ZONE RESET CANDIDATE RATIO : %lu\n",100 - (reset_resigned_.load()*100)/reset_tried_.load());
    if(runtime_zone_reset_called_n_.load()!=0){
    printf("RUNTIME ZONE RESET RECLAIMED RATIO : %lu, called(%lu)\n",(ratio_sum2_.load()/runtime_zone_reset_called_n_.load()),runtime_zone_reset_called_n_.load());
    }
    printf("RUNTIME ZONE RESET RESIGNED INVALID RATIO : %lu\n",100-(ratio_sum_.load()/reset_resigned_.load()));

  }
  // if(far_stats_.size()){
  //   printf("CANDIDATE RATIO  : %lu(%lu)\n",(candidate_ratio_sum_/far_stats_.size()),candidate_ratio_sum_);
  //   printf("CANDIDATE VALID RATIO : %lu(%lu)\n",candidate_valid_ratio_sum_/far_stats_.size(),candidate_valid_ratio_sum_);
  //   printf("NO CANDIDATE VALID RATIO : %lu(%lu)\n",no_candidate_valid_ratio_sum_/far_stats_.size(),no_candidate_valid_ratio_sum_);
  // }
  // if(before_zc_T_+1){
  //   printf("BEFORE ZC T : %lu\n",before_zc_T_);
  //   printf("BEFORE ZC CANDIDATE RATIO  : %lu(%lu)\n",(candidate_ratio_sum_before_zc_/(before_zc_T_+1)),candidate_ratio_sum_before_zc_);
  //   printf("BEFORE ZC CANDIDATE VALID RATIO : %lu(%lu)\n",candidate_valid_ratio_sum_before_zc_/(before_zc_T_+1),candidate_valid_ratio_sum_before_zc_);
  //   printf("BEFORE ZC NO CANDIDATE VALID RATIO : %lu(%lu)\n",no_candidate_valid_ratio_sum_before_zc_/(before_zc_T_+1),no_candidate_valid_ratio_sum_before_zc_);
  // }
  for(const auto z : io_zones){
    read_lock_overhead_sum += z->read_lock_overhead_.load();
  }
  total_erased=erase_size_zc_.load()+erase_size_.load()+partial_erase_size_.load()+erase_size_proactive_zc_.load();
  if(total_erased){
    printf("copied/erased ratio : %lu/%lu=%lu\n",gc_bytes_written_.load(),(total_erased),(gc_bytes_written_.load()*100)/total_erased);
  }else{
    printf("copied/erased ratio : (NULL)\n");
  }
  if(gc_bytes_written_.load()){
      printf("ZC cache hit ratio  %lu/%lu = %lu\n"
      ,page_cache_hit_size_sum
      ,(gc_bytes_written_.load()>>20)+page_cache_hit_size_sum
      ,(page_cache_hit_size_sum*100)/((gc_bytes_written_.load()>>20)+page_cache_hit_size_sum ));
  }
  //   std::atomic<uint64_t> rocksdb_page_cache_fault_size_{0};
  // std::atomic<uint64_t> rocksdb_page_cache_hit_size_{0};
  if(rocksdb_page_cache_hit_size_.load()){
      printf("RocksDB cache hit ratio  %lu/%lu = %lu\n"
      ,rocksdb_page_cache_hit_size_.load()
      ,(rocksdb_page_cache_fault_size_.load()+rocksdb_page_cache_hit_size_.load() )
      ,(rocksdb_page_cache_hit_size_.load()*100)/((rocksdb_page_cache_hit_size_.load())+rocksdb_page_cache_fault_size_.load() ));
  }
  if(GetUserBytesWritten()){
    printf("copy/written ratio : %lu/%lu=%lu\n",gc_bytes_written_.load(),GetUserBytesWritten(),(gc_bytes_written_.load()*100)/GetUserBytesWritten());
  }
  printf("ZC read amp %lu\n",zc_read_amp_);
  printf("TOTAL I/O BLOKCING TIME %d\n",io_blocking_sum);
  printf("TOTAL I/O BLOCKING TIME(ms) %llu\n",io_blocking_ms_sum);
  if(io_blocking_ms_sum/1000){
    printf("COPY(MB/s) %llu\n",(gc_bytes_written_.load()>>20)/(io_blocking_ms_sum/1000));
  }

  printf("TOTAL ERASED AT RZR DEVICE VIEW : %lu(MB)\n",(wasted_wp_.load()+erase_size_.load())>>20 );
  printf("READ LOCK OVERHEAD %llu\n",read_lock_overhead_sum);
  // printf("runtime reset latency : %llu(ms)\n",runtime_reset_latency_.load()/1000);
  // if(compaction_triggered_.load()){
  // printf("avg. compaction input size : %lu MB (= %lu/%lu)\n",(total_compaction_input_size_.load()>>20)/compaction_triggered_.load()
  //                                                         ,total_compaction_input_size_.load()>>20,compaction_triggered_.load());
  // }else printf("no compaction triggered\n");

  uint64_t sum_in_is = 0;
  uint64_t sum_in_os = 0;
  uint64_t sum_out_s = 0;
  uint64_t sum_triggered = 0;
  for(int l = 0;l<10;l++){
    CompactionStats* cstat=&compaction_stats_[l];
    uint64_t in_is=cstat->input_size_input_level_.load();
    uint64_t in_os=cstat->input_size_output_level_.load();
    uint64_t out_s=cstat->output_size_.load();
    uint64_t triggered=cstat->compaction_triggered_.load();


    sum_in_is+=in_is;
    sum_in_os+=in_os;
    sum_out_s+=out_s;
    sum_triggered+=triggered;
    printf("LEVEL %d :: ",l);
    if(triggered==0 || out_s == 0){
      printf("\n");
      continue;
    }

    printf("%lu,%lu -> %lu(%lu%%) // %lu triggered\n",(
            (in_is>>20)/triggered),((in_os>>20)/triggered),
            ((out_s>>20)/triggered),((in_is+in_os)*100/out_s) ,triggered);
  }
  if(sum_triggered!=0 && sum_out_s!=0 ){
    printf("AVG.   :: %lu,%lu -> %lu(%lu%%) // %lu triggered\n",(
            (sum_in_is>>20)/sum_triggered),((sum_in_os>>20)/sum_triggered),
            ((sum_out_s>>20)/sum_triggered),((sum_in_is+sum_in_os)*100/sum_out_s) ,sum_triggered);
  }
    
    // uint64_t sum_score = 0;
    // uint64_t sum_n = 0;
    // for(int level = 0; level < 5; level++){
    //   if(compaction_triggered_[level].load()){
    //     printf("[%d] Same zone score : %lu/%lu = %lu\n",level,
    //       same_zone_score_atomic_[level].load(),compaction_triggered_[level].load(),
    //       same_zone_score_atomic_[level].load()/compaction_triggered_[level].load());
    //     sum_score+=same_zone_score_atomic_[level].load();
    //     sum_n+=compaction_triggered_[level].load();
    //   }
    // }
    // printf("avg Same zone score : %lu/%lu = %lu\n",sum_score,sum_n,sum_score/sum_n);
    // sum_score=0;
    // sum_n =0;
    // for(int level = 0; level < 5; level++){
    //   if(compaction_triggered_[level].load()){
    //     printf("[%d] invalidation score : %lu/%lu = %lu\n",level,
    //     invalidate_score_atomic_[level].load(),compaction_triggered_[level].load(),
    //     invalidate_score_atomic_[level].load()/compaction_triggered_[level].load());
    //     sum_score+=same_zone_score_atomic_[level].load();
    //     sum_n+=compaction_triggered_[level].load();
    //   }
    // }
    // printf("avg inval zone score : %lu/%lu = %lu\n",sum_score,sum_n,sum_score/sum_n);
    {
    std::lock_guard<std::mutex> lg(same_zone_score_mutex_);

    double avg_same_zone_score,avg_inval_score;
    double sum_sum_score=0.0, sum_sum_inval_score=0.0;
    size_t total_n = 0;
    for(int i = 0; i<5; i++){

      double sum_score=0.0;
      double sum_inval_score=0.0;
      size_t score_n=same_zone_score_[i].size();
      if(score_n>0){
        for(size_t j = 0; j < same_zone_score_[i].size(); j++){
          sum_score+=same_zone_score_[i][j];
        }
        avg_same_zone_score=sum_score/score_n;

        for(size_t j =0;j<invalidate_score_[i].size();j++){
          sum_inval_score+=invalidate_score_[i][j];
        }
        avg_inval_score=sum_inval_score/score_n;
        printf("[%d] samezone score : %lf\tinvalidate score %lf\n",i,avg_same_zone_score,avg_inval_score);
      }else{
        printf("[%d] samezone score : (none)\tinvalidate score (none)\n",i);
      }
      
      
      sum_sum_score+=sum_score;
      sum_sum_inval_score+=sum_inval_score;
      total_n+=score_n;
      
    }
    if(total_n){
      printf("total samezone score : %lf\tinvalidate score %lf\n",sum_sum_score/total_n,sum_sum_inval_score/total_n);
    }
  }
    // read_latency_sum_.fetch_add(microseconds);
  // read_n_.fetch_add(1);
  // if(read_n_.load()){
  //   printf("read latency %lu / %lu = %lu us\n",
  //     read_latency_sum_.load(),read_n_.load(),read_latency_sum_.load()/read_n_.load());
  // }
  printf("Cumulative I/O Blocking %lu\n",cumulative_io_blocking_);
  PrintCumulativeBreakDown();
  printf("%lu~%lu\n",GetZoneCleaningKickingPoint(),GetReclaimUntil());
  
  printf("============================================================\n\n");
  // for(size_t i=0;i<far_stats_.size();i++){
  //   far_stats_[i].PrintInvalidZoneDist();
  //   // write_stall_timelapse_[i].PrintStat();
  // }
  // std::vector<uint64_t> sst_file_size_last_;
  // std::mutex sst_file_size_last_lock_;
  // std::vector<uint64_t> sst_file_size_else_;
  // std::mutex sst_file_size_else_lock_;
  // for()
  
  // printf("============last sst files================\n");
  // {
  //   std::lock_guard<std::mutex> lg(sst_file_size_last_lock_);
  //   for(auto size : sst_file_size_last_){
  //     if(size.first==1){
  //       continue;
  //     }
  //     printf("%lu\t",size.second>>20);
  //   }
  // }
  // printf("\n");
  // printf("============else sst files================\n");
  // {
  //   std::lock_guard<std::mutex> lg(sst_file_size_else_lock_);
  //   for(auto size : sst_file_size_else_){
  //     if(size.first==1){
  //       continue;
  //     }
  //     printf("%lu\t",size.second>>20);
  //   }
  // }
  // printf("==========================================\n");

  // printf("============last sst files(l1)================\n");
  // {
  //   std::lock_guard<std::mutex> lg(sst_file_size_last_lock_);
  //   for(auto size : sst_file_size_last_){
  //     if(size.first!=1){
  //       continue;
  //     }
  //     printf("%lu\t",size.second>>20);
  //   }
  // }
  // printf("\n");
  // printf("============else sst files(l1)================\n");
  // {
  //   std::lock_guard<std::mutex> lg(sst_file_size_else_lock_);
  //   for(auto size : sst_file_size_else_){
  //     if(size.first!=1){
  //       continue;
  //     }
  //     printf("%lu\t",size.second>>20);
  //   }
  // }
  // printf("==========================================\n");
  // db_ptr_-
  // if(db_ptr_){
  //   std::string stats;
  //   db_ptr_->GetProperty("rocksdb.stats",&stats);
  //   printf("%s\n",stats.c_str());
  // }

  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }
}

#define LIFETIME_DIFF_NOT_GOOD (100)
#define LIFETIME_DIFF_COULD_BE_WORSE (50)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime >= file_lifetime) return zone_lifetime - file_lifetime;
  else return file_lifetime-zone_lifetime;

  return LIFETIME_DIFF_NOT_GOOD;
}

IOStatus ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone) {
  assert(out_meta_zone);
  *out_meta_zone = nullptr;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_META_ALLOC_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_META_ALLOC_QPS, 1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          z->Release();
          continue;
        }
        *out_meta_zone = z;
        return IOStatus::OK();
      }
    }
  }
  assert(true);
  Error(logger_, "Out of metadata zones, we should go to read only now.");
  return IOStatus::NoSpace("Out of metadata zones");
}

void  ZonedBlockDevice::GiveZenFStoLSMTreeHint(std::vector<uint64_t>& compaction_inputs_input_level_fno,
                            std::vector<uint64_t>& compaction_inputs_output_level_fno,int output_level,bool trivial_move){
  ZoneFile* zfile=nullptr;

  if(allocation_scheme_==LIZA){
    return;
  }
  if(trivial_move){
    // if(! ( compaction_inputs_input_level_fno.size()!=1 && compaction_inputs_output_level_fno.size()!=0 ) ){
    //   printf("????? %lu %lu\n",compaction_inputs_input_level_fno.size(),compaction_inputs_output_level_fno.size());
    //   return;
    // }

    if(output_level==1){
      printf("0->1 trivial move??\n");
    }
  

    for(uint64_t fno : compaction_inputs_input_level_fno){
      zfile=GetSSTZoneFileInZBDNoLock(fno);
      
      if(zfile==nullptr){
        printf("why nullptr? %lu\n",fno);
        continue;
      }
      uint64_t file_size=zfile->predicted_size_;
      lsm_tree_[output_level-1].fetch_sub(file_size);
      lsm_tree_[output_level].fetch_add(file_size);
    }
    return;
  }


//////////////// invaldation compaction


  for(uint64_t fno : compaction_inputs_input_level_fno){
      zfile=GetSSTZoneFileInZBDNoLock(fno);
      
      if(zfile==nullptr){
        printf("why nullptr? %lu\n",fno);
        continue;
      }
      uint64_t file_size=zfile->predicted_size_;
      lsm_tree_[output_level-1].fetch_sub(file_size);
  }
  for(uint64_t fno : compaction_inputs_output_level_fno){
      zfile=GetSSTZoneFileInZBDNoLock(fno);
      
      if(zfile==nullptr){
        printf("why nullptr? %lu\n",fno);
        continue;
      }
      uint64_t file_size=zfile->predicted_size_;
      lsm_tree_[output_level].fetch_sub(file_size);
  }

    std::vector<uint64_t> input_fno = compaction_inputs_input_level_fno;
    input_fno.insert(input_fno.end(),
            compaction_inputs_output_level_fno.begin(),
            compaction_inputs_output_level_fno.end());
  if(input_aware_scheme_ == 1){

    for(auto fno : input_fno){
      auto zFile  = GetSSTZoneFileInZBDNoLock(fno);
      if(!zFile){
        zFile->selected_as_input_= true;
        // level = zFile->level_ > level ? zFile->level_ : level;
      }
    }
  }

  double score = GetMaxSameZoneScore(input_fno);
  uint64_t none;
  double inval_score = GetMaxInvalidateCompactionScore(input_fno,&none,true);
  {
    std::lock_guard<std::mutex> lg(same_zone_score_mutex_);
    same_zone_score_[output_level].push_back(score);
    invalidate_score_[output_level].push_back(inval_score);

    same_zone_score_for_timelapse_[output_level].clear();
    same_zone_score_for_timelapse_[output_level]=same_zone_score_[output_level];

    // uint64_t same_zone_score_uint64t=score*10000;
    // uint64_t inval_score_uint64t=inval_score*100;
    // same_zone_score_atomic_[output_level].fetch_add(same_zone_score_uint64t);
    // invalidate_score_atomic_[output_level].fetch_add(inval_score_uint64t);
    // compaction_triggered_[output_level].fetch_add(1);
    // printf("%lu %lu\n",same_zone_score_uint64t,inval_score_uint64t);
  }
}



void ZonedBlockDevice::AddTimeLapse(int T,uint64_t cur_ops) {
  (void)(T);
  // size_t reclaimable= 0;
  // size_t written = 0;
  // size_t candidate_ratio;
  // size_t candidate_valid= 0;
  // size_t candidate_valid_ratio;

  // size_t no_candidate_valid=0;
  // size_t no_candidate_valid_ratio;

  // for(auto z : io_zones){
  //   written+=z->wp_-z->start_;
  //   if(z->IsFull()){
  //     candidate_valid+=z->used_capacity_;
  //     reclaimable+=z->max_capacity_;
  //   }else{ // no candidate;
  //     no_candidate_valid+=z->used_capacity_;
  //   }
  // }
  // if(written){
  //   candidate_ratio=(reclaimable*100/written);
  // }else{
  //   candidate_ratio=0;
  // }

  // if(reclaimable){ // candidate
  //   candidate_valid_ratio=(candidate_valid*100)/reclaimable;
  // }else{
  //   candidate_valid_ratio=0;
  // }

  // if(written-reclaimable>0){ // no candidate
  //   no_candidate_valid_ratio=(no_candidate_valid*100)/(written-reclaimable);
  // }else{
  //   no_candidate_valid_ratio=0;
  // }
  // candidate_ratio_sum_+=candidate_ratio;
  // candidate_valid_ratio_sum_+=candidate_valid_ratio;
  // no_candidate_valid_ratio_sum_+=no_candidate_valid_ratio;
  // if(before_zc_){
  //   candidate_ratio_sum_before_zc_+=candidate_ratio;
  //   candidate_valid_ratio_sum_before_zc_+=candidate_valid_ratio;
  //   no_candidate_valid_ratio_sum_before_zc_+=no_candidate_valid_ratio;
  //   before_zc_T_=T;
  //   if(cur_free_percent_<ZONE_CLEANING_KICKING_POINT){
  //     before_zc_=false;
  //   }
  // }

  {
    std::lock_guard<std::mutex> lg(same_zone_score_mutex_);
    // same_zone_score_.push_back(score);
    // same_zone_score_for_timelapse_.clear();
    for(int i = 0; i <5; i++){
      same_zone_score_for_timelapse_[i]=same_zone_score_[i];
      invalidate_score_for_timelapse_[i]=invalidate_score_[i];
    }
  }
  // double ratio_sum = 0.0;
  // double ratio;

  std::vector<uint64_t> invalid_percent_per_zone(101,0);

  // for(auto z : io_zones){
  //   if(z==nullptr){

  //     break;
  //   }
  //   if(z->IsEmpty()){
  //     // invalid_percent_per_zone.push_back(0);
  //     invalid_percent_per_zone[0]++;
  //     continue;
  //   }
    
  //   uint64_t relative_wp = (z->wp_ -z->start_);
  //   uint64_t invalid_size = relative_wp - z->used_capacity_;
  //   uint64_t invalid_percent = (invalid_size*100)/(relative_wp) ;
  //   if(invalid_percent >100){
  //     printf("AddTimeLapse ?? %lu %lu %lu\n",relative_wp,invalid_size,invalid_percent);
  //     invalid_percent_per_zone[0]++;
  //     continue;
  //   }
  //   invalid_percent_per_zone[invalid_percent]++;
  // }
  // double avg_invalid_ratio = ratio_sum/(io_zones.size());
  if(cur_free_percent_>100){
    printf("Addtimelapse cur_free_percent_ ? %lu\n",cur_free_percent_);
  }


  uint64_t invalid_data_size = 0;
  uint64_t valid_data_size = 0;
  for(auto z : io_zones){
   valid_data_size+=z->used_capacity_; 
   invalid_data_size+=(z->wp_-z->start_ - z->used_capacity_);
  }

  far_stats_.emplace_back(cur_free_percent_, 
          reset_count_.load(), reset_count_zc_.load(), partial_reset_count_.load(),
          erase_size_.load(),erase_size_zc_.load(),erase_size_proactive_zc_.load(),partial_erase_size_.load(),
                wasted_wp_.load() , T, reset_threshold_arr_[cur_free_percent_],
                // GetZoneSize(),
                io_zones[0]->max_capacity_,
                // db_ptr_ ? db_ptr_->NumLevelsFiles() : 
                std::vector<int>(0),
                // db_ptr_ ? db_ptr_->LevelsCompactionScore() : 
                std::vector<double>(0),
                // db_ptr_ ? db_ptr_->LevelsSize() : 
                std::vector<uint64_t>(0),compaction_stats_,
                same_zone_score_for_timelapse_,invalidate_score_for_timelapse_,
                0.0,invalid_percent_per_zone,
                cur_ops,gc_bytes_written_.load(),
                valid_data_size,
                invalid_data_size,cumulative_io_blocking_);
}

inline uint64_t ZonedBlockDevice::LazyLog(uint64_t sz,uint64_t fr,uint64_t T){
    T++;
    if(fr>=T){
        return 0+(1<<14);
    }
    return sz*(1-log(fr+1)/log(T));
}

inline uint64_t ZonedBlockDevice::LazyLinear(uint64_t sz,uint64_t fr,uint64_t T){
    if(fr>=T){
        return 0+(1<<14);
    }
    (void)(T);
    return sz- (sz*fr)/T;
}  
inline uint64_t ZonedBlockDevice::Custom(uint64_t sz,uint64_t fr,uint64_t T){
    if(fr>=T){
        return sz-sz*T/100;
    }
    return sz-(fr*sz)/100;
}

inline uint64_t ZonedBlockDevice::LogLinear(uint64_t sz,uint64_t fr,uint64_t T){
  double ratio;
  if(fr>=T){
    ratio=(1-log(fr+1)/log(101));
    return ratio*sz;
  }
  ratio = (1-log(T+1)/log(101))*100;
  double slope=(100-ratio)/T;
  ratio = 100-slope*fr;

  return ratio*sz/100;
}
inline uint64_t ZonedBlockDevice::LazyExponential(uint64_t sz, uint64_t fr,
                                                  uint64_t T) {
  if (fr >= T) {
    return 0 + (1 << 14);
  }
  if (fr == 0) {
    return sz;
  }
  double b = pow(100, 1 / (float)T);
  b = pow(b, fr);
  return sz - (b * sz / 100);
}

void ZonedBlockDevice::CalculateResetThreshold(uint64_t free_percent) {
  uint64_t rt = 0;
  // uint64_t max_capacity = (1<<io_zones[0]->log2_erase_unit_size_);
  // uint64_t free_percent = cur_free_percent_;
  uint64_t max_capacity = io_zones[0]->max_capacity_;

  // printf("CalculateResetThreshold : %lu\n",max_capacity);
  switch (reset_scheme_)
  {
  case kEager:
    rt = max_capacity;
    break;
  case kLazy:
    rt = 0;
    break;
  case kFAR: // Constant scale
    rt = max_capacity - (max_capacity * free_percent) / 100;
    break;
  case kLazy_Log:
    rt = LazyLog(max_capacity, free_percent, tuning_point_);
    break;
  case kNoRuntimeLinear:
  case kLazy_Linear:
    rt = LazyLinear(max_capacity, free_percent, tuning_point_);
    break;
  case kCustom:
    rt = Custom(max_capacity, free_percent, tuning_point_);
    break;
  case kLogLinear:
    rt = LogLinear(max_capacity, free_percent, tuning_point_);
    break;
  case kLazyExponential:
    rt = LazyExponential(max_capacity, free_percent, tuning_point_);
    break;
  default:
    break;
  }
  reset_threshold_ = rt;
  reset_threshold_arr_[free_percent]=rt;
  // printf("%lu : %lu\n",free_percent,rt);
}


// only called at zone cleaning

IOStatus ZonedBlockDevice::AsyncResetUnusedIOZones(void) {

  IOStatus reset_status;
  std::vector<AsyncReset*> async_pool;

  for (size_t i =0;i<io_zones.size();i++) {
    const auto z = io_zones[i];
    bool full=z->IsFull() ;
    if(!full){
      continue;
    }
    if(z->IsUsed()){
      continue;
    }
    if(z->IsEmpty()){
      continue;
    }
    
    if ( z->Acquire() ) {


      if(z->IsEmpty()){
        z->Release();
        continue;
      }
      // if(!ProactiveZoneCleaning() && !full){
      //   z->Release();
      //   continue;
      // }
      if(!full){
        z->Release();
        continue;  
      }

      if (!z->IsUsed()) {
        // bool full=
        if(full){
          erase_size_zc_.fetch_add(io_zones[i]->max_capacity_);
        }else{
          erase_size_proactive_zc_.fetch_add(io_zones[i]->wp_-io_zones[i]->start_);
        }
        wasted_wp_.fetch_add(io_zones[i]->capacity_);
        
        // reset_status = z->Reset();
        // z->AsyncReset();

        std::thread* _thread= new std::thread(&Zone::AsyncReset,z);
        AsyncReset* async_reset= new AsyncReset(_thread,z);
        async_pool.push_back(async_reset);


        reset_count_zc_.fetch_add(1);
        // if (!reset_status.ok()) {
        //   z->Release();
        //   return reset_status;
        // }
        // if (!full) PutActiveIOZoneToken();
        continue;
      } 

      z->Release();
    }
  }
  for(size_t azr= 0 ; azr<async_pool.size();azr++){
    delete async_pool[azr];
  }
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::ResetMultipleUnusedIOZones(void) {
  ZenFSStopWatch z4("Large reset",this);
  IOStatus reset_status;
  std::vector<std::pair<Zone*,bool>> to_be_reseted;
  uint64_t next_offset = UINT64_MAX;
  to_be_reseted.clear();
  for (size_t i =0;i<io_zones.size();i++) {
    const auto z = io_zones[i];
    bool full=z->IsFull() ;
    if(!full){
      continue;
    }
    if(z->IsUsed()){
      continue;
    }
    if(z->IsEmpty()){
      continue;
    }
    
    if ( z->Acquire() ) {


      if(z->IsEmpty()){
        z->Release();
        continue;
      }

      if(!full){
        z->Release();
        continue;  
      }

      if (!z->IsUsed()) {
        // bool full=
        if(full){
          erase_size_zc_.fetch_add(io_zones[i]->max_capacity_);
        }else{
          erase_size_proactive_zc_.fetch_add(io_zones[i]->wp_-io_zones[i]->start_);
        }
        wasted_wp_.fetch_add(io_zones[i]->capacity_);
        if(next_offset==UINT64_MAX){
          to_be_reseted.push_back({z,full});
          next_offset=z->start_ + z->max_capacity_;
          continue;
        }

        if(next_offset == z->start_){
          to_be_reseted.push_back({z,full});
          next_offset=z->start_ + z->max_capacity_;
          continue;
        }

        if(next_offset!=z->start_){
          {
            char multireset_stopwatch[30];
            sprintf((char*)multireset_stopwatch,"multi reset %lu",to_be_reseted.size());
            ZenFSStopWatch multireset((const char*)multireset_stopwatch,this);
          zbd_be_->MultiReset(to_be_reseted[0].first->start_,
                    (to_be_reseted[0].first->max_capacity_ * to_be_reseted.size()));
          }
          for(auto do_reset : to_be_reseted){
            Zone* unused_zone= do_reset.first;
            bool was_full = do_reset.second;
            unused_zone->capacity_=unused_zone->max_capacity_;
            unused_zone->wp_=unused_zone->start_;
            unused_zone->lifetime_=Env::WLTH_NOT_SET;
            unused_zone->Release();
            if(!was_full){
              PutActiveIOZoneToken();
            }
          }
          to_be_reseted.clear();
          to_be_reseted.push_back({z,full});
          next_offset=z->start_+z->max_capacity_;
        }


        reset_count_zc_.fetch_add(1);
      } 

      z->Release();
    }
  }

  if(to_be_reseted.size()){
      zbd_be_->MultiReset(to_be_reseted[0].first->start_,(to_be_reseted[0].first->max_capacity_ * to_be_reseted.size()));
    
      for(auto do_reset : to_be_reseted){
      Zone* unused_zone= do_reset.first;
      bool was_full = do_reset.second;
      unused_zone->capacity_=unused_zone->max_capacity_;
      unused_zone->wp_=unused_zone->start_;
      unused_zone->lifetime_=Env::WLTH_NOT_SET;
      unused_zone->Release();
      if(!was_full){
        PutActiveIOZoneToken();
      }
    }
  }


  return IOStatus::OK();
}


IOStatus ZonedBlockDevice::ResetUnusedIOZones(void) {
  ZenFSStopWatch z4("Small reset",this);
  IOStatus reset_status;


  for (size_t i =0;i<io_zones.size();i++) {
    const auto z = io_zones[i];
    bool full=z->IsFull() ;
    if(!full){
      continue;
    }
    if(z->IsUsed()){
      continue;
    }
    if(z->IsEmpty()){
      continue;
    }
    
    if ( z->Acquire() ) {


      if(z->IsEmpty()){
        z->Release();
        continue;
      }
      // if(!ProactiveZoneCleaning() && !full){
      //   z->Release();
      //   continue;
      // }
      if(!full){
        z->Release();
        continue;  
      }

      if (!z->IsUsed()) {
        // bool full=
        if(full){
          erase_size_zc_.fetch_add(io_zones[i]->max_capacity_);
        }else{
          erase_size_proactive_zc_.fetch_add(io_zones[i]->wp_-io_zones[i]->start_);
        }
        wasted_wp_.fetch_add(io_zones[i]->capacity_);
        reset_status = z->Reset();
        reset_count_zc_.fetch_add(1);
        if (!reset_status.ok()) {
          z->Release();
          return reset_status;
        }
        if (!full) {
          PutActiveIOZoneToken();
          // PutOpenIOZoneToken();
        }

      } 

      z->Release();
    }
  }

  return IOStatus::OK();
}


// IOStatus ZonedBlockDevice::RuntimeZoneReset(std::vector<bool>& is_reseted) {
//   // size_t total_invalid=0;
//   // size_t reclaimed_invalid=0;

//   IOStatus reset_status=IOStatus::OK();
//   if(cur_free_percent_>=99){
//     return reset_status;
//   }
  

//   for (size_t i =0;i<io_zones.size();i++) {
//     const auto z = io_zones[i];
//     if(is_reseted[i]){
//       continue;
//     }
//     if(z->IsEmpty()){
//       continue;
//     }
//     if(z->IsUsed()){
//       continue;
//     }
//     if ( z->Acquire() ) {
//       if(z->IsEmpty()){
//           z->Release();
//           continue;
//       }
//       if(z->IsUsed()){
//           z->Release();
//           continue;
//       }


//       bool full = z->IsFull();
//       total_invalid=(z->wp_ - z->start_);
//       // printf("total invalid %lu end erase unit wrttien %lu total_full_erase_unit written %lu erase unit size %lu\n",total_invalid,end_erase_unit_written,total_full_erase_unit_written,erase_unit_size);
//       // if(end_erase_unit_written>reset_threshold_){ //eu
//       // printf("end")
//       // }
//       // 
//       if(total_invalid>reset_threshold_arr_[cur_free_percent_]){
//         goto no_reset;
//       }
//       erase_size_.fetch_add(total_invalid);
//       wasted_wp_.fecth_add(z->max_capacity_ - total_invalid);
//       // printf("end erase written  : %lu rt %lu is_end_erase_unit_should_be_erased %d\n",end_erase_unit_written,reset_threshold_,is_end_erase_unit_should_be_erased);
//       reset_status = z->Reset();


//       if (!reset_status.ok()) return reset_status;
//         is_reseted[i]=true;
//         reset_count_.fetch_add(1);
//         z->reset_count_++;
//         // 

//         // clock_t end=clock();
// no_reset:
//         if (!full&&z->IsEmpty()) { // not full -> empty
//           PutActiveIOZoneToken();
//         } 
//         z->Release();
//     }
//   }
 
//   return IOStatus::OK();
// }


// IOStatus ZonedBlockDevice::RuntimeZoneReset(std::vector<bool>& is_reseted) {
//   // clock_t reset_latency{0};
//   // size_t ratio = 0;
//   size_t total_invalid=0;
//   size_t reclaimed_invalid=0;
//   uint64_t erase_unit_size=(1<<(io_zones[0]->log2_erase_unit_size_));
//   // uint64_t total_written=0;
//   uint64_t end_erase_unit_written;
//   uint64_t total_full_erase_unit_written;
//   bool is_end_erase_unit_should_be_erased;
//   IOStatus reset_status=IOStatus::OK();
//   if(cur_free_percent_>=99){
//     return reset_status;
//   }
  

//   for (size_t i =0;i<io_zones.size();i++) {
//     const auto z = io_zones[i];
//     if(is_reseted[i]){
//       continue;
//     }
//     if(z->IsEmpty()){
//       continue;
//     }
//     if(z->IsUsed()){
//       continue;
//     }
//     if ( z->Acquire() ) {
//       if(z->IsEmpty()){
//           z->Release();
//           continue;
//       }
//       if(z->IsUsed()){
//           z->Release();
//           continue;
//       }


//       bool full = z->IsFull();
//       total_invalid=(z->wp_ - z->start_);
//       end_erase_unit_written=total_invalid%erase_unit_size;
//       total_full_erase_unit_written=(total_invalid/erase_unit_size)*erase_unit_size;
//       // printf("total invalid %lu end erase unit wrttien %lu total_full_erase_unit written %lu erase unit size %lu\n",total_invalid,end_erase_unit_written,total_full_erase_unit_written,erase_unit_size);
//       // if(end_erase_unit_written>reset_threshold_){ //eu
//       // printf("end")
//       // }
//       // 
//       if(reset_scheme_==kEager){
//         is_end_erase_unit_should_be_erased=true;
//       }else{
//         is_end_erase_unit_should_be_erased=(erase_unit_size-end_erase_unit_written) < reset_threshold_arr_[cur_free_percent_];
//       }

//       // printf("end erase written  : %lu rt %lu is_end_erase_unit_should_be_erased %d\n",end_erase_unit_written,reset_threshold_,is_end_erase_unit_should_be_erased);
       
//       if(is_end_erase_unit_should_be_erased){
//         reclaimed_invalid+=(z->wp_-z->start_);
//         erase_size_.fetch_add(z->wp_-z->start_);
//         wasted_wp_.fetch_add((erase_unit_size-end_erase_unit_written));
//         reset_status = z->Reset();
//       }else if(total_full_erase_unit_written>0){
//         reclaimed_invalid+=total_full_erase_unit_written;
//         erase_size_.fetch_add(total_full_erase_unit_written);
//         if(total_full_erase_unit_written==z->max_capacity_){
//           reset_status = z->Reset();
//         }else{
//           reset_status = z->PartialResetToAllInvalidZone(total_full_erase_unit_written);
//         }
//       }else{
//         goto no_reset;
//       }

//       if (!reset_status.ok()) return reset_status;
//       // if (!z->IsEmpty() && !z->IsUsed()) {

//         // uint64_t cp=z->GetCapacityLeft();
//         // if (cp > reset_threshold_) {
//         //   z->Release();
//         //   continue;
//         // }


//         // clock_t start=clock();

//         // reset_status = z->Reset();
//         is_reseted[i]=true;
//         reset_count_.fetch_add(1);
//         z->reset_count_++;
//         // 

//         // clock_t end=clock();
// no_reset:
//         if (!full&&z->IsEmpty()) { // not full -> empty
//           PutActiveIOZoneToken();
//         } 
//         z->Release();
//         // if (!reset_status.ok()) return reset_status;

//       // } 
//       // else {
//       //   z->Release();
//       // }
//     }
//   }
//   // if(total_written){
//   //   runtime_zone_reset_called_n_.fetch_add(1);
//   //   ratio_sum2_.fetch_add((100*reclaimed_invalid)/total_written);
//   // }
//   return IOStatus::OK();
// }




IOStatus ZonedBlockDevice::RuntimeZoneReset(std::vector<bool>& is_reseted) {
  size_t total_invalid=0;
  // size_t reclaimed_invalid=0;

  IOStatus reset_status=IOStatus::OK();
  // if(cur_free_percent_>=99){
  //   return reset_status;
  // }
  

  for (size_t i =0;i<io_zones.size();i++) {
    const auto z = io_zones[i];
    if(is_reseted[i]){
      continue;
    }
    if(z->IsEmpty()){
      continue;
    }
    if(z->IsUsed()){
      continue;
    }
    if ( z->Acquire() ) {
      if(z->IsEmpty()){
          z->Release();
          continue;
      }
      if(z->IsUsed()){
          z->Release();
          continue;
      }


      bool full = z->IsFull();

      total_invalid= z->wp_-z->start_ < z->max_capacity_  ? (z->wp_ - z->start_) : z->max_capacity_;

      // printf("total invalid %lu end erase unit wrttien %lu total_full_erase_unit written %lu erase unit size %lu\n",total_invalid,end_erase_unit_written,total_full_erase_unit_written,erase_unit_size);
      // if(end_erase_unit_written>reset_threshold_){ //eu
      // printf("end")
      // }
      // 
      if((z->max_capacity_-total_invalid)>reset_threshold_arr_[cur_free_percent_]){
        goto no_reset;
      }
      erase_size_.fetch_add(total_invalid);
      wasted_wp_.fetch_add(z->max_capacity_ - total_invalid);
      // printf("end erase written  : %lu rt %lu is_end_erase_unit_should_be_erased %d\n",end_erase_unit_written,reset_threshold_,is_end_erase_unit_should_be_erased);
      reset_status = z->Reset();
      // printf("Reset !! %lu\n",i);


      if (!reset_status.ok()) return reset_status;
        is_reseted[i]=true;
        reset_count_.fetch_add(1);
        z->reset_count_++;
        // 

        // clock_t end=clock();
no_reset:
        if (!full&&z->IsEmpty()) { // not full -> empty
          PutActiveIOZoneToken();
        } 
        z->Release();
    }
  }
 
  return IOStatus::OK();
}


/*
Partial Reset::
if reader in, pass it
if no reader, block reader and partial reset 

Reader ::
if partial Reset, wait it
if readeing, block partial reset
*/
IOStatus ZonedBlockDevice::RuntimePartialZoneReset(std::vector<bool>& is_reseted){
  size_t erase_size;
  IOStatus s=IOStatus::OK();

  if(RuntimeZoneResetOnly()){
    return IOStatus::IOError("Not Supported\n");
  }    
  // if(!ZCorPartialTryLock()){
  //   return IOStatus::OK();
  // }
  // if(zc_running_==true){
  //   return IOStatus::OK();
  // }
  
  // int i = 0;
  for(size_t i = 0;i<io_zones.size();i++){
    const auto z = io_zones[i];
    if(is_reseted[i]){
      continue;
    }
    // if(io_zones_[i]->capacity_>)
    if(!z->TryZoneWriteLock()){
      continue;
    }
    bool valid_mixed=z->IsUsed();
    bool full=z->IsFull();
    erase_size=0;
    s = z->PartialReset(&erase_size);

    if(!s.ok()){ 
      z->ReleaseZoneWriteLock();
      // ZCorPartialUnLock();
      return s;
    }
    
    if(erase_size>0){

      is_reseted[i]=true;
      
      if(valid_mixed){
        partial_reset_to_valid_count_.fetch_add(1);
        partial_to_valid_erase_size_.fetch_add(erase_size);
      }

      partial_reset_count_.fetch_add(1);
      partial_erase_size_.fetch_add(erase_size);
      
    }
    z->ReleaseZoneWriteLock();
    if(!full){ //was not full
      if(z->IsEmpty()){
        PutActiveIOZoneToken();
        // PutOpenIOZoneToken();
      }
    }
  }
  // ZCorPartialUnLock();
  return IOStatus::OK();
}


bool ZonedBlockDevice::GetMigrationIOZoneToken(void){
  std::unique_lock<std::mutex> lk(migrate_zone_mtx_);
  // migrate_resource_.wait(lk,[this]{
  //   if(migration_io_zones_.load()<max_migrate_zones_){
  //     migration_io_zones_++;
  //     return true;
  //   }else{
  //     return false;
  //   }
  // });

  if(migration_io_zones_.load()<max_migrate_zones_){
    migration_io_zones_++;
    return true;
  }
  return false;
}

void ZonedBlockDevice::MoveResources(bool to_migration){
  if(to_migration){
    if(GetActiveIOZoneTokenIfAvailable()){
      PutMigrationIOZoneToken();
    }
  }else{
    if(GetMigrationIOZoneToken()){
      PutMigrationIOZoneToken();
    }
  }
}


void ZonedBlockDevice::PutMigrationIOZoneToken(void) {
  {
    std::unique_lock<std::mutex> lk(migrate_zone_mtx_);
    if(migration_io_zones_.load()!=0){
      migration_io_zones_--;
    }
  }
  migrate_resource_.notify_one();
}

void ZonedBlockDevice::WaitForOpenIOZoneToken(bool prioritized,WaitForOpenZoneClass open_class) {
  long allocator_open_limit;

  char stopwatch_buf[50];
//  L0,L1,L2,L3,L4,ZC,WAL
  // ZenFSStopWatch z1("WaitForOpenIOZoneToken",this);
      switch (open_class)
      {
      case L0: // full
        sprintf((char*)stopwatch_buf, "WaitForOpenIOZoneToken L0");
        break;
      case L1:
        sprintf((char*)stopwatch_buf, "WaitForOpenIOZoneToken L1");
        break;
      case L2:
        sprintf((char*)stopwatch_buf, "WaitForOpenIOZoneToken L2");
        break;
      case L3:
        sprintf((char*)stopwatch_buf, "WaitForOpenIOZoneToken L3");
        break;
      case L4:
        sprintf((char*)stopwatch_buf, "WaitForOpenIOZoneToken L4");
        break;
      case ZC:
        sprintf((char*)stopwatch_buf, "WaitForOpenIOZoneToken ZC");
        break;
      case WAL:
        sprintf((char*)stopwatch_buf, "WaitForOpenIOZoneToken WAL");
        break;
      default:
        sprintf((char*)stopwatch_buf, "WaitForOpenIOZoneToken Unknown class ? %d",open_class);
        break;
      }
     ZenFSStopWatch z1((const char*)stopwatch_buf,this);
  /* Avoid non-priortized allocators from starving prioritized ones */
  // if (prioritized) {
  //   allocator_open_limit = max_nr_open_io_zones_;
  // } else {
  //   allocator_open_limit = max_nr_open_io_zones_ - 1;
  // }
// allocator_open_limit = max_nr_open_io_zones_;
  /* Wait for an open IO Zone token - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutOpenIOZoneToken to return the resource
   */




  

  (void)(prioritized);
  if(AsyncZCEnabled() ){
    // // push priority queue to my level
    if(prioritized){
      open_io_zones_++;
      cur_open_zone_per_class_[WAL]++;
      return;
    }
      // allocator_open_limit = max_nr_open_io_zones_-1;
      // std::unique_lock<std::mutex> lk(zone_resources_mtx_);
      // zone_resources_priority_queue_.push((int)open_class);

      // zone_resources_.wait(lk, [this, allocator_open_limit,open_class] {
      //   // if( zone_resources_priority_queue && open_io_zones_.load() < allocator_open_limit){

      //   // }
      //   if (
      //     open_class == zone_resources_priority_queue_.top()&&
      //     open_io_zones_.load() < allocator_open_limit
      //          ) {
      //     open_io_zones_++;
      //     zone_resources_priority_queue_.pop();
      //     return true;
      //   } else {
      //     zone_resources_.notify_all();
      //     return false;
      //   }
      // });
    ////////////////////

      allocator_open_limit = max_nr_open_io_zones_-1;
      std::unique_lock<std::mutex> lk(zone_resources_mtx_);
      if(open_class == L1){
        // priority_zone_resources_[L0].wait(lk, [this,allocator_open_limit] {
        //   if (open_io_zones_.load() < allocator_open_limit) {
        //     open_io_zones_++;
        //     return true;
        //   } else {
        //     return false;
        //   }
        // });
        open_class=L0;
      }
      // else{
        priority_zone_resources_[open_class].wait(lk, [this,allocator_open_limit,open_class] {
          int cur_open_classes=0;
          for(int oc = 0; oc<open_class; oc++){
            cur_open_classes+=cur_open_zone_per_class_[oc];
            
            printf("[%d] %d\n",oc,cur_open_zone_per_class_[oc]);

            if(cur_open_classes>saturation_point_){
              // printf("%d return false\n",cur_open_classes);
                          printf("\n");
              return false;
            }

          }
                      printf("\n");
          if (open_io_zones_.load() < allocator_open_limit) {
            open_io_zones_++;
            cur_open_zone_per_class_[open_class]++;
            return true;
          } else {
            return false;
          }
        });
      // }


  }else{
    allocator_open_limit = max_nr_open_io_zones_;
    
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    zone_resources_.wait(lk, [this, allocator_open_limit] {
      if (open_io_zones_.load() < allocator_open_limit) {
        open_io_zones_++;
        return true;
      } else {
        return false;
      }
    });
  }





}



IOStatus ZonedBlockDevice::RuntimeReset(void){
  IOStatus s=IOStatus::OK();
  if(RuntimeZoneResetDisabled()){
    return s;
  }
  if(ProactiveZoneCleaning()){
    return s;
  }
  std::vector<bool> is_reseted;
  // auto start_chrono = std::chrono::high_resolution_clock::now();
  is_reseted.assign(io_zones.size(),false);
  switch (GetPartialResetScheme())
  {
    case PARTIAL_RESET_ONLY:
      s = RuntimePartialZoneReset(is_reseted);
      break;
    case PARTIAL_RESET_WITH_ZONE_RESET:
      s = RuntimeZoneReset(is_reseted);
      if(!s.ok()) break;
      s = RuntimePartialZoneReset(is_reseted);
      break;
    case PARTIAL_RESET_BACKGROUND_T_WITH_ZONE_RESET:
      /* fall through */
    case PARTIAL_RESET_AT_BACKGROUND:
      /* fall through */
    case RUNTIME_ZONE_RESET_ONLY:
      // if(AsyncZCEnabled()){
      //   ResetMultipleUnusedIOZones();
      // }else{
        ResetUnusedIOZones();
      // }
      // s = RuntimeZoneReset(is_reseted);
      
      break;
    default:
      break;
  }
  // auto elapsed = std::chrono::high_resolution_clock::now() - start_chrono;
  // long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  return s;
}


bool ZonedBlockDevice::GetActiveIOZoneTokenIfAvailable() {
  /* Grap an active IO Zone token if available - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutActiveIOZoneToken to return the resource
   */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  if (active_io_zones_.load() < max_nr_active_io_zones_) {
    active_io_zones_++;
    return true;
  }
  return false;
}

void ZonedBlockDevice::PutOpenIOZoneToken(WaitForOpenZoneClass open_class) {
  if(open_class==L1){
    open_class=L0;
  }
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    if(open_io_zones_.load()!=0){
      open_io_zones_--;
      cur_open_zone_per_class_[open_class]--;
    }
  }
  if(AsyncZCEnabled()){
    for(size_t oc =0 ; oc< 10; oc++){
      priority_zone_resources_[oc].notify_one();
    }
  }else{
    zone_resources_.notify_all();
  }
  



}

void ZonedBlockDevice::PutActiveIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    if(active_io_zones_.load()!=0){
      active_io_zones_--;
    }
  }

  if(AsyncZCEnabled()){
    for(int oc =0 ; oc< 10; oc++){
      priority_zone_resources_[oc].notify_one();
    }
  }else{
    zone_resources_.notify_all();
  }
}

IOStatus ZonedBlockDevice::ApplyFinishThreshold() {
  IOStatus s;
  // finish_threshold_=0;
  if (finish_threshold_ == 0) return IOStatus::OK();

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      bool within_finish_threshold =
          z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100);
      if (!(z->IsEmpty() || z->IsFull()) && within_finish_threshold) {
        /* If there is less than finish_threshold_% remaining capacity in a
         * non-open-zone, finish the zone */
        s = z->Finish();
        if (!s.ok()) {
          z->Release();
          Debug(logger_, "Failed finishing zone");
          return s;
        }
        z->Release();
        PutActiveIOZoneToken();
      } else {
        z->Release();
      }
    }
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::FinishCheapestIOZone(bool put_token) {
  IOStatus s;
  Zone *finish_victim = nullptr;

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty() || z->IsFull()) {
        z->Release();
        if (!s.ok()) return s;
        continue;
      }
      if (finish_victim == nullptr) {
        finish_victim = z;
        continue;
      }
      if (finish_victim->capacity_ > z->capacity_) {
        finish_victim->Release();
        if (!s.ok()) return s;
        finish_victim = z;
      } else {
        z->Release();
      }
    }
  }

  // If all non-busy zones are empty or full, we should return success.
  if (finish_victim == nullptr) {
    Info(logger_, "All non-busy zones are empty or full, skip.");
    return IOStatus::OK();
  }

  s = finish_victim->Finish();
  finish_victim->Release();

  // if (s.ok()) {
  if(put_token){
    PutActiveIOZoneToken();
  }
  // }


  return s;
}
IOStatus ZonedBlockDevice::GetAnyLargestRemainingZone(Zone** zone_out,bool force,uint64_t min_capacity){
  (void)(force);
  IOStatus s=IOStatus::OK();
  Zone* allocated_zone=nullptr;

  for(const auto z : io_zones){
    if(!z->Acquire()){
      continue;
    }
    if(z->IsEmpty()){
      z->Release();
      continue;
    }
    if(z->capacity_>min_capacity ){
      if(allocated_zone){
        allocated_zone->Release();
      }
      allocated_zone=z;
      min_capacity=z->capacity_;
      continue;
    }


    z->Release();
  }
  
  *zone_out=allocated_zone;
  return s;
}
// IOStatus ZonedBlockDevice::GetAnyLargestRemainingZone(Zone** zone_out,bool force,uint64_t min_capacity){
//   IOStatus s=IOStatus::OK();
//   Zone* allocated_zone=nullptr;
//   min_capacity = min_capacity > (1<<20) ? min_capacity : (1<<20);
//   uint64_t most_min_cap=io_zones[0]->max_capacity_;
//   // }
//   (void)(force);
//   for(const auto z : io_zones){
//     if(!z->Acquire()){
//       continue;
//     }
//     if(z->capacity_<most_min_cap && z->capacity_>min_capacity){
//       if(allocated_zone){
//         allocated_zone->Release();
//       }
//       allocated_zone=z;
//       most_min_cap=z->capacity_;
//       continue;
//     }


//     z->Release();
//     // if(!s.ok()){
//     //   return s;
//     // }
//   }
  
//   *zone_out=allocated_zone;
//   return s;
// }

IOStatus ZonedBlockDevice::GetBestOpenZoneMatch(
    Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out,std::vector<uint64_t> input_fno,
    Zone **zone_out, uint64_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;
  int i =0;
  // uint64_t zidx;
  (void)(input_fno);
  // bool input_in_zone[io_zones.size()];
  // std::vector<bool> is_input_in_zone(io_zones.size(),false);
  // for(uint64_t fno : input_fno){
  //   ZoneFile* zFile=GetSSTZoneFileInZBDNoLock(fno);
  //   if(zFile==nullptr){
  //     continue;
  //   }
  //   auto extents=zFile->GetExtents();
  //   for(ZoneExtent* extent : extents){
  //     zidx=extent->zone_->zidx_ - ZENFS_META_ZONES-ZENFS_SPARE_ZONES;
  //     is_input_in_zone[zidx]=true;
  //   }
  // }

  for (const auto z : io_zones) {
    // printf("1 : [%d]\n",i);
    // if()
    // zidx = z->zidx_- ZENFS_META_ZONES-ZENFS_SPARE_ZONES;
    // if(is_input_in_zone[zidx]){
    //   continue;
    // }
    if(z->lifetime_==Env::WLTH_NOT_SET || z->lifetime_==Env::WLTH_NONE){
      continue;
    }
    if (z->Acquire()) {
      if(z->IsEmpty()){
        z->Release();
        continue;
      }

      if(z->capacity_<min_capacity){
        z->Release();
        continue;
      }
      if(z->IsFull()){
        z->Release();
        continue;
      }
      unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
      if (diff < best_diff) {
        if (allocated_zone != nullptr) {
          allocated_zone->Release();
      }
        allocated_zone = z;
        best_diff = diff;
      } else {
        z->Release();
        if (!s.ok()) return s;
      }


      // if ((z->used_capacity_ > 0) && !z->IsFull() &&
      //     z->capacity_ >= min_capacity) {
      //           // printf("2 : [%d]\n",i);
      //   unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
      //   if (diff < best_diff) {
      //     if (allocated_zone != nullptr) {
      //       allocated_zone->Release();
      //     }
      //     allocated_zone = z;
      //     best_diff = diff;
      //   } else {
      //     z->Release();
      //     if (!s.ok()) return s;
      //   }
      // } else {
      //       // printf("4 : [%d]\n",i);
      //   z->Release();
      //   if (!s.ok()) return s;
      // }
    }
    i++;
  }

  *best_diff_out = best_diff;
  *zone_out = allocated_zone;

  return IOStatus::OK();
}

double ZonedBlockDevice::GetMaxSameZoneScore(std::vector<uint64_t>& compaction_inputs_fno){
  uint64_t total_size=0,initial_total_size;
  uint64_t sst_in_zone_square = 0;
  double score = 0.0;
  // uint64_t avg_score = GetMaxInvalidateCompactionScore(compaction_inputs_fno,&total_size,true);
  std::vector<uint64_t> sst_in_zone(io_zones.size(),0);
  for(uint64_t fno : compaction_inputs_fno){
    ZoneFile* zFile=GetSSTZoneFileInZBDNoLock(fno);
    auto extents=zFile->GetExtents();
    for(ZoneExtent* extent : extents){
      uint64_t zidx=extent->zone_->zidx_ - ZENFS_META_ZONES-ZENFS_SPARE_ZONES;
      // is_sst_in_zone[zidx]=true;
      sst_in_zone[zidx]+=extent->length_;
      total_size+=extent->length_;
    }
  }
  initial_total_size=total_size;
  /*
  2048
  1024 -> 0.5
  512
  512

  (512*512 + 512*512) / (1024*1024) * 0.5


  1024 -> 0.5
  1024 -> 0.5

  1024 -> 0.5
  768
  256
  (768^2 + 256 ^2) ÷ (1024^2) × 0.5= 0.3125 
  */
  for(size_t i =0; i < io_zones.size(); i ++){
    if(sst_in_zone[i] > io_zones[i]->max_capacity_ - (1<<24) ){
      score += ((double)sst_in_zone[i] /(double)initial_total_size);
      total_size-=sst_in_zone[i];
      continue;
    }
    // if(sst_in_zone[i]!=0){
    //   printf("\t\t\t%lu\n",sst_in_zone[i]>>20);
    // }
    sst_in_zone_square+=(sst_in_zone[i]*sst_in_zone[i]);
  }
  // score += sum(sst_in_zone^2) /(total_size^2) * (total_size/initial_total_size)
  // score += sst_in_zone_square/(total_size*total_size)*(total_size/initial_total_size); // mabye overflow
  // printf("before score : %.2lf , sst_in_zone_square %lu , total size %lu, init_total size %lu\n",
          // score,sst_in_zone_square,total_size,initial_total_size);
  if(total_size>0 && initial_total_size> 0){
    // score+= (sst_in_zone_square/initial_total_size);
    score+= (double(sst_in_zone_square/total_size)/(double)initial_total_size);
  }
  // printf("\t\t\tscore : %lf     \n",score);
  return score;
}

double ZonedBlockDevice::GetMaxInvalidateCompactionScore(std::vector<uint64_t>& file_candidates,uint64_t * candidate_size,bool stats){
  // std::vector<std::pair<bool,uint64_t>> zone_score;
  printf("io zones n %lu\n",io_zones.size());
  std::vector<bool> is_sst_in_zone(io_zones.size(),false);
  std::vector<uint64_t> sst_in_zone(io_zones.size(),0);
  std::vector<uint64_t> zone_score(io_zones.size(),0);
  printf("io zones n %lu 22222222\n",io_zones.size());
  uint64_t total_size = 0;
  uint64_t zidx;
  uint64_t zone_size= io_zones[0]->max_capacity_;
  uint64_t zone_score_sum = 0;
  uint64_t sst_in_zone_n = 0;
  uint64_t zone_score_max = 0;

  for(uint64_t fno : file_candidates){
    ZoneFile* zFile=GetSSTZoneFileInZBDNoLock(fno);
    if(zFile==nullptr){
      continue;
    }
    auto extents=zFile->GetExtents();
    for(ZoneExtent* extent : extents){
      zidx=extent->zone_->zidx_ - ZENFS_META_ZONES-ZENFS_SPARE_ZONES;
      is_sst_in_zone[zidx]=true;
      sst_in_zone[zidx]+=extent->length_;
    }
  }

  for(size_t i = 0; i < io_zones.size(); i++){
    if(is_sst_in_zone[i]==false){
      continue;
    }
    total_size+=sst_in_zone[i];
    uint64_t relative_wp = io_zones[i]->wp_ - io_zones[i]->start_;
    relative_wp= relative_wp > io_zones[i]->max_capacity_ ? io_zones[i]->max_capacity_ : relative_wp;
    uint64_t after_valid_capacity= io_zones[i]->used_capacity_ - sst_in_zone[i];
    uint64_t after_invalid_capacity = relative_wp - after_valid_capacity;
    
    
    if(after_invalid_capacity>zone_size){ //error point
      printf("after_invalid_capacity %lu > zone_size %lu??\n",after_invalid_capacity,zone_size); 
      zone_score[i]=100;
    }else{
      if(relative_wp != 0){
        zone_score[i]=(after_invalid_capacity)*100/(relative_wp);
      }else{
        zone_score[i]=0;
      }
    }

    if(zone_score[i]>zone_score_max){
      zone_score_max=zone_score[i];
    }
    
    
    if(stats){
      zone_score_sum+=zone_score[i];
    }else{
      zone_score_sum+=zone_score[i]*zone_score[i];
    }
    sst_in_zone_n++;
  }
  // for(size_t i = 0; i<zone_score.size(); i++){
  //   if(is_sst_in_zone[i]==false){
  //     continue;
  //   }

  // }
  *candidate_size=total_size;

  if(sst_in_zone_n==0){
    return 0;
  }

  // if free space high, capacity first
  // if free space low, invalid score first

  (void)(zone_score_sum);
  (void)(zone_score_max);
  return (double)zone_score_sum/(double)sst_in_zone_n;
  // return zone_score_max;
}
bool ZonedBlockDevice::CalculateZoneScore(std::vector<uint64_t>& fno_list,std::vector<uint64_t>& zone_score){

  bool there_is_near_level_files=false;

  {

    // zone_score.clear();
    // zone_score.assign(io_zones.size(),0);



    for (auto fno : fno_list){
      // auto it=std::find(input_fno.begin(),input_fno.end(),fno);
      // if(it!=input_fno.end()){
      //   continue;
      // }
      ZoneFile* zFile= GetSSTZoneFileInZBDNoLock(fno);
      if(zFile==nullptr){
        continue;
      }
      if(zFile->selected_as_input_){
        continue;
      }
      auto extents=zFile->GetExtents();
      for(auto extent: extents){
        if(!extent->zone_->IsFull()){
          // zone_->index do not skip meta,spare zone
          zone_score[extent->zone_->zidx_-ZENFS_META_ZONES-ZENFS_SPARE_ZONES]+=extent->length_;
          there_is_near_level_files=true;
        }
      }
    }
  }
  return there_is_near_level_files;
}

void ZonedBlockDevice::AllocateZoneBySortedScore(std::vector<std::pair<uint64_t,uint64_t>>& sorted,Zone** allocated_zone,uint64_t min_capacity){
    uint64_t cur_score;
    // uint64_t max_score=0;
    Zone* target_zone;
    for(auto zidx : sorted){
      // if(is_input_in_zone[i-ZENFS_META_ZONES-ZENFS_SPARE_ZONES]==true){
      //   continue;
      // }
      
      cur_score=zidx.first;
      target_zone=io_zones[zidx.second];
      // cur_invalid_data=(target_zone->wp_-target_zone->start_) - target_zone->used_capacity_;
      if(cur_score==0){
        break;
      }
      if(target_zone->IsFull()){
        continue;
      }


      if(!target_zone->Acquire()){
        continue;
      }

      if(target_zone->capacity_<=min_capacity){
        target_zone->Release();
        continue;
      }
      (*allocated_zone)=target_zone;
      break;
    }
}

/*
AdvancedCAZA(level i,key_range, SST_size, level_score[i],level_score[i-1] )
If Type(SST_size) == LARGE : 
	if (level_score[i-1] > level_score[i] )
		UpperLevelOverlappedSST = GetOverlappedSST(i-1,key_range) 
	 	if ( Type(UpperLevelOverlappedSST.size) == LARGE )
	 		AppendUpperLevelOverlappedSST()
	 	else
	 		AppendDownwardLevelOverlappedSST()
	else
		AppendDownwardLevelOverlappedSST()
else	
	UpperLevelOverlappedSST = GetOverlappedSST(i-1,key_range) 
	if(Type(UpperLevelOverlappedSST.size) == LARGE)
		AppendUpperLevelOverlappedSST()
	else
		AppendSameLevelNearKeyrangeSST()

*/
bool ZonedBlockDevice::CompactionSimulator(uint64_t predicted_size,int level,Slice& smallest, Slice& largest){
    std::vector<uint64_t> upper_fno_list;
    std::vector<uint64_t> this_fno_list;
    uint64_t upper_level_file_fno= MostLargeUpperAdjacentFile(smallest, largest, level);
    ZoneFile* upper_level_file = GetSSTZoneFileInZBDNoLock(upper_level_file_fno);

    uint64_t upper_level_file_size;
    bool upper_level_occurs_first = true;
    if(!upper_level_file){
      return upper_level_occurs_first;
      
    }
    upper_level_file_size=upper_level_file->predicted_size_;
    uint64_t current_upper_level_size =0;
    uint64_t current_this_level_size = 0;


    uint64_t upper_level_size_limit = max_bytes_for_level_base_;
    uint64_t this_level_size_limit = max_bytes_for_level_base_;

    for(int l = 1 ;l < level-1;l++){
      upper_level_size_limit*=10;
    }
    for(int l = 1 ; l < level ; l++){
      this_level_size_limit*=10;
    }

    SameLevelFileList(level-1,upper_fno_list,false);
    SameLevelFileList(level,this_fno_list,false);



    std::vector<uint64_t> predicted_down_level;
    std::vector<uint64_t> predicted_upper_level;
    predicted_down_level.push_back(predicted_size);
    current_this_level_size+=predicted_size;

    for(auto fno : this_fno_list){
      ZoneFile* zfile = GetSSTZoneFileInZBDNoLock(fno);
      if(!zfile){
        continue;
      }
      predicted_down_level.push_back(zfile->predicted_size_);
      current_this_level_size+=zfile->predicted_size_;
    }


    for(auto fno : upper_fno_list){
      ZoneFile* zfile = GetSSTZoneFileInZBDNoLock(fno);
      if(!zfile){
        continue;
      }
      predicted_upper_level.push_back(zfile->predicted_size_);
      current_upper_level_size+=zfile->predicted_size_; 
    }
    sort(predicted_upper_level.rbegin(),predicted_upper_level.rend());




    // sort(predicted_down_level.rbegin(),predicted_down_level.rend());

    while(true){
      double this_level_score = (double)current_this_level_size/this_level_size_limit;
      double upper_level_score = (double)current_upper_level_size/upper_level_size_limit;

      if(this_level_score>upper_level_score){
        sort(predicted_down_level.rbegin(),predicted_down_level.rend());
        if(predicted_down_level[0]<=predicted_size){
          // this occurs first
          upper_level_occurs_first=false;
          break;
        }
        current_this_level_size-=predicted_down_level[0];
        predicted_down_level.erase(predicted_down_level.begin());
      }else{
        if(predicted_upper_level[0]<=upper_level_file_size){
          upper_level_occurs_first=true;
          break;
        }
        current_upper_level_size-=predicted_upper_level[0];
        current_this_level_size+=predicted_upper_level[0];
        predicted_down_level.push_back(predicted_upper_level[0]);
        predicted_upper_level.erase(predicted_upper_level.begin());
      }
    }

    
    // for(auto this_size : predicted_down_level){
    //   this_index++;
    //   if(this_size<=predicted_size){
    //     break;
    //   }
    // }
  return upper_level_occurs_first;
}

IOStatus ZonedBlockDevice::AllocateCompactionAwaredZoneV2(Slice& smallest, Slice& largest,
                                                        int level,Env::WriteLifeTimeHint file_lifetime, 
                                                        std::vector<uint64_t> input_fno,uint64_t predicted_size,
                                                        Zone **zone_out,uint64_t min_capacity){
  
  /////////////////////////////// CAZA
  if(allocation_scheme_==LIZA){
    return IOStatus::OK();
  }

  (void)(file_lifetime);

  // return IOStatus::OK();
  IOStatus s;
  // uint64_t cur_score;
  // uint64_t cur_invalid_data;
  // bool no_near_level_files=true;
  Zone* allocated_zone = nullptr;
  // Zone* target_zone;
  std::vector<uint64_t> fno_list;
  // uint64_t max_score=0;
  uint64_t max_invalid_data=0;
  std::vector<uint64_t> zone_score(io_zones.size(),0);
  std::vector<std::pair<uint64_t,uint64_t>>  sorted;
  std::vector<bool> is_input_in_zone(io_zones.size(),false);
  (void)(input_fno);
  (void)(predicted_size);
  // (void)(cur_invalid_data);
  (void)(max_invalid_data);

  if(level==0){
    goto l0;
  }
  // bool is_big_sstable = IS_BIG_SSTABLE(predicted_size);

  if(IS_BIG_SSTABLE(predicted_size)){
    double upper_level_score = PredictCompactionScore(level-1);
    double this_level_score = PredictCompactionScore(level);
    double l0_score= PredictCompactionScore(0);

    if(level == 1){
        // append to downward
      fno_list.clear();
      zone_score.clear();
      zone_score.assign(io_zones.size(),0);
      DownwardAdjacentFileList(smallest, largest, level, fno_list);
      if(CalculateZoneScore(fno_list,zone_score)){
        sorted = SortedByZoneScore(zone_score);
        AllocateZoneBySortedScore(sorted,&allocated_zone,min_capacity);
      }
    } 
    else if(level==2 && (l0_score > this_level_score ) &&  (l0_score>upper_level_score) ) {
        fno_list.clear();
        zone_score.clear();
        zone_score.assign(io_zones.size(),0);
        DownwardAdjacentFileList(smallest, largest, level, fno_list);



        if(CalculateZoneScore(fno_list,zone_score)){
          sorted = SortedByZoneScore(zone_score);
          AllocateZoneBySortedScore(sorted,&allocated_zone,min_capacity);
        }
    }
    // else if(!CompactionSimulator(predicted_size,level,smallest,largest)){ // if upper level occur first == false
      else if (this_level_score> upper_level_score){
        
        fno_list.clear();
        zone_score.clear();
        zone_score.assign(io_zones.size(),0);
        DownwardAdjacentFileList(smallest, largest, level, fno_list);



        if(CalculateZoneScore(fno_list,zone_score)){
          sorted = SortedByZoneScore(zone_score);
          AllocateZoneBySortedScore(sorted,&allocated_zone,min_capacity);
        }

      // double downward_level_score = PredictCompactionScore(level+1);
      // if(this_level_score>downward_level_score){
      //   // append to downward
      //   fno_list.clear();
      //   zone_score.clear();
      //   zone_score.assign(io_zones.size(),0);
      //   DownwardAdjacentFileList(smallest, largest, level, fno_list);



      //   if(CalculateZoneScore(fno_list,zone_score)){
      //     sorted = SortedByZoneScore(zone_score);
      //     AllocateZoneBySortedScore(sorted,&allocated_zone,min_capacity);
      //   }
      // }else{ // downward level score > this level score 
      //   uint64_t downward_level_sst_fno = MostSmallDownwardAdjacentFile(smallest,largest,level);
      //   ZoneFile* zfile=GetSSTZoneFileInZBDNoLock(downward_level_sst_fno);
      //   if(zfile && IS_BIG_SSTABLE(zfile->predicted_size_)){
      //     // append to same levels
      //     fno_list.clear();
      //     // zone_score.assign(0,zone_score.size());
      //     zone_score.clear();
      //     zone_score.assign(io_zones.size(),0);
      //     SameLevelFileList(level,fno_list);
      //     s = AllocateSameLevelFilesZone(smallest,largest,fno_list,is_input_in_zone,&allocated_zone,
      //                                   min_capacity);
      //   }else{
      //     // append downward
      //     fno_list.clear();
      //     zone_score.clear();
      //     zone_score.assign(io_zones.size(),0);
      //     DownwardAdjacentFileList(smallest, largest, level, fno_list);



      //     if(CalculateZoneScore(fno_list,zone_score)){
      //       sorted = SortedByZoneScore(zone_score);
      //       AllocateZoneBySortedScore(sorted,&allocated_zone,min_capacity);
      //     }
      //   }
      // }
      



    }else{ // upper_level_score>this_level_score
      uint64_t upper_level_sst_fno = MostLargeUpperAdjacentFile(smallest, largest, level);
    
      ZoneFile* zfile=GetSSTZoneFileInZBDNoLock(upper_level_sst_fno);
      
      if(zfile&& (IS_BIG_SSTABLE(zfile->GetFileSize()) || level == 1) ){
        // append to upper,this zfile
        GetNearestZoneFromZoneFile(zfile,is_input_in_zone,&allocated_zone,min_capacity);
      }else{
        // append to downward
        fno_list.clear();
        zone_score.clear();
        zone_score.assign(io_zones.size(),0);
        DownwardAdjacentFileList(smallest, largest, level, fno_list);
        if(CalculateZoneScore(fno_list,zone_score)){
          sorted = SortedByZoneScore(zone_score);
          AllocateZoneBySortedScore(sorted,&allocated_zone,min_capacity);
        }
      }
    }

  }else{
    uint64_t upper_level_sst_fno=MostLargeUpperAdjacentFile(smallest, largest, level);
    
    ZoneFile* zfile=GetSSTZoneFileInZBDNoLock(upper_level_sst_fno);
    if(level == 1){
      fno_list.clear();
      // zone_score.assign(0,zone_score.size());
      zone_score.clear();
      zone_score.assign(io_zones.size(),0);
      SameLevelFileList(0,fno_list);
      SameLevelFileList(1,fno_list);
      s = AllocateMostL0FilesZone(zone_score,fno_list,is_input_in_zone,&allocated_zone,
                                  min_capacity);
    }
    else if( zfile && IS_BIG_SSTABLE(zfile->predicted_size_)){
      // append to upper zfile
      GetNearestZoneFromZoneFile(zfile,is_input_in_zone,&allocated_zone,min_capacity);
    }else{
      // append to same levels
      fno_list.clear();
      // zone_score.assign(0,zone_score.size());
      zone_score.clear();
      zone_score.assign(io_zones.size(),0);
      SameLevelFileList(level,fno_list);
      s = AllocateSameLevelFilesZone(smallest,largest,fno_list,is_input_in_zone,&allocated_zone,
                                    min_capacity);
    }
  }
  if(allocated_zone!=nullptr){
    *zone_out=allocated_zone;
    return IOStatus::OK();
  }

/////////////////////////////
l0:
  // return IOStatus::OK();
// if level 0, most level 0 zone
  if(level==0 || level==100){
    fno_list.clear();
    // zone_score.assign(0,zone_score.size());
    zone_score.clear();
    zone_score.assign(io_zones.size(),0);
    SameLevelFileList(0,fno_list);
    SameLevelFileList(1,fno_list);
    s = AllocateMostL0FilesZone(zone_score,fno_list,is_input_in_zone,&allocated_zone,
                                min_capacity);
    // if(allocated_zone!=nullptr){
    //   // printf("CAZA 2.1\n");
    // }
  }
  // else{ // if other level, same level but near key-sstfile zone
  //   fno_list.clear();

  //   zone_score.clear();
  //   zone_score.assign(io_zones.size(),0);
  //   SameLevelFileList(level,fno_list);
  //   s = AllocateSameLevelFilesZone(smallest,largest,fno_list,is_input_in_zone,&allocated_zone,
  //                                 min_capacity);
  //   if(allocated_zone!=nullptr){
  //     //  printf("CAZA 2.2\n");
  //   }
  // }


  if(!s.ok()){
    return s;
  }
  if(allocated_zone!=nullptr){
    // printf("CAZA 3\n");
    *zone_out=allocated_zone;
    return s;
  }




  // Empty zone allocation should set lifetime for zone
  // if(GetActiveIOZoneTokenIfAvailable()){
  // s = AllocateEmptyZone(&allocated_zone);
  // }
  // if(allocated_zone!=nullptr){
  //   *zone_out=allocated_zone;
  //   allocated_zone->lifetime_ = file_lifetime;
  // }


  // if(!s.ok()){
  //   return s;
  // }
  // if(allocated_zone!=nullptr){
  //   *zone_out=allocated_zone;
  // }
  return s;
}


IOStatus ZonedBlockDevice::AllocateCompactionAwaredZone(Slice& smallest, Slice& largest,
                                                        int level,Env::WriteLifeTimeHint file_lifetime, 
                                                        std::vector<uint64_t> input_fno,uint64_t predicted_size,
                                                        Zone **zone_out,uint64_t min_capacity){
  
  /////////////////////////////// CAZA
  if(allocation_scheme_==LIZA){
    return IOStatus::OK();
  }

  (void)(file_lifetime);

  // return IOStatus::OK();
  IOStatus s;
  uint64_t cur_score;
  uint64_t cur_invalid_data;
  bool no_near_level_files=true;
  Zone* allocated_zone = nullptr;
  Zone* target_zone;
  std::vector<uint64_t> fno_list;
  uint64_t max_score=0;
  uint64_t max_invalid_data=0;
  // printf("caza worksd?\n");
  std::vector<bool> is_input_in_zone(io_zones.size(),false);
  (void)(input_fno);
  (void)(predicted_size);
  (void)(cur_invalid_data);
  (void)(max_invalid_data);


  // for(uint64_t fno : input_fno){
  //   ZoneFile* zFile=GetSSTZoneFileInZBDNoLock(fno);
  //   if(zFile==nullptr){
  //     continue;
  //   }
  //   auto extents=zFile->GetExtents();
  //   for(ZoneExtent* extent : extents){
  //     uint64_t zidx=extent->zone_->zidx_ - ZENFS_META_ZONES-ZENFS_SPARE_ZONES;
  //     is_input_in_zone[zidx]=true;
  //   }
  // }

  // zone valid overlapping capacity
  // 1. find UPPER/LOWER OVERLAPP RANGE zone

  std::vector<uint64_t> zone_score(io_zones.size(),0);
  std::vector<std::pair<uint64_t,uint64_t>>  sorted;
  if(level==0){
    goto l0;
  }  

  // if(level==1){
  //   // fno_list.clear();
  //   // zone_score.clear();
  //   // zone_score.assign(io_zones.size()+ZENFS_META_ZONES+ZENFS_SPARE_ZONES,0);
  //   // SameLevelFileList(0,fno_list);
  //   // s = AllocateMostL0FilesZone(zone_score,fno_list,is_input_in_zone,&allocated_zone,
  //   //                             min_capacity);

  //   // if(allocated_zone!=nullptr){
  //   //   // printf("CAZA 1 \n");
  //   //   *zone_out=allocated_zone;
  //   //   return IOStatus::OK();
  //   // }
  // }

  {
    fno_list.clear();
    zone_score.clear();
    zone_score.assign(io_zones.size(),0);
    AdjacentFileList(smallest, largest, level, fno_list);


    for (auto fno : fno_list){
      // auto it=std::find(input_fno.begin(),input_fno.end(),fno);
      // if(it!=input_fno.end()){
      //   continue;
      // }
      ZoneFile* zFile= GetSSTZoneFileInZBDNoLock(fno);
      if(zFile==nullptr){
        continue;
      }
      if(zFile->selected_as_input_){
        continue;
      }
      auto extents=zFile->GetExtents();
      for(auto extent: extents){
        if(!extent->zone_->IsFull()){
          // zone_->index do not skip meta,spare zone
          zone_score[extent->zone_->zidx_-ZENFS_META_ZONES-ZENFS_SPARE_ZONES]+=extent->length_;
          no_near_level_files=false;
        }
      }
    }
  }

  sorted = SortedByZoneScore(zone_score);

  if(!no_near_level_files){
    for(auto zidx : sorted){
      // if(is_input_in_zone[i-ZENFS_META_ZONES-ZENFS_SPARE_ZONES]==true){
      //   continue;
      // }
      
      cur_score=zidx.first;
      target_zone=io_zones[zidx.second];
      // cur_invalid_data=(target_zone->wp_-target_zone->start_) - target_zone->used_capacity_;

      if(cur_score==0||target_zone->IsFull()){
        continue;
      }

      
      if(cur_score<max_score){
        continue;
      }

      if(!target_zone->Acquire()){
        continue;
      }

      if(target_zone->capacity_<=min_capacity){
        target_zone->Release();
        continue;
      }
      allocated_zone=target_zone;
      break;

      // if(cur_score > max_score){
      //   if(allocated_zone){
      //     allocated_zone->Release();
      //   }
      //   allocated_zone=target_zone;
      //   max_score=cur_score;
      //   max_invalid_data=cur_invalid_data;
      //   continue;
      // }

      // if(cur_score == max_score && cur_invalid_data>max_invalid_data){
      //   if(allocated_zone){
      //     allocated_zone->Release();
      //   }
      //   allocated_zone=target_zone;
      //   max_invalid_data=cur_invalid_data;
      //   continue;
      // }

      // target_zone->Release();

    }
  }

  if(allocated_zone!=nullptr){
    // printf("CAZA 1 \n");
    *zone_out=allocated_zone;
    return IOStatus::OK();
  }


/////////////////////////////
l0:
  // return IOStatus::OK();
// if level 0, most level 0 zone
  if(level==0 ||level==1 ||level==100){
    fno_list.clear();
    // zone_score.assign(0,zone_score.size());
    zone_score.clear();
    zone_score.assign(io_zones.size(),0);
    SameLevelFileList(0,fno_list);
    SameLevelFileList(1,fno_list);
    s = AllocateMostL0FilesZone(zone_score,fno_list,is_input_in_zone,&allocated_zone,
                                min_capacity);
    // if(allocated_zone!=nullptr){
    //   // printf("CAZA 2.1\n");
    // }
  }else{ // if other level, same level but near key-sstfile zone
    fno_list.clear();
    // zone_score.assign(0,zone_score.size());
    zone_score.clear();
    zone_score.assign(io_zones.size()-ZENFS_META_ZONES-ZENFS_SPARE_ZONES,0);
    SameLevelFileList(level,fno_list);
    s = AllocateSameLevelFilesZone(smallest,largest,fno_list,is_input_in_zone,&allocated_zone,
                                  min_capacity);
    if(allocated_zone!=nullptr){
      //  printf("CAZA 2.2\n");
    }
  }


  if(!s.ok()){
    return s;
  }
  if(allocated_zone!=nullptr){
    // printf("CAZA 3\n");
    *zone_out=allocated_zone;
    return s;
  }




  // Empty zone allocation should set lifetime for zone
      if(GetActiveIOZoneTokenIfAvailable()){
  s = AllocateEmptyZone(&allocated_zone);
  }
  // if(!s.ok()){
  //   return s;
  // }
  if(allocated_zone!=nullptr){
    *zone_out=allocated_zone;
    allocated_zone->lifetime_ = file_lifetime;
  }


  // if(!s.ok()){
  //   return s;
  // }
  // if(allocated_zone!=nullptr){
  //   *zone_out=allocated_zone;
  // }
  return s;
}

IOStatus ZonedBlockDevice::AllocateMostL0FilesZone(std::vector<uint64_t>& zone_score,
                                                    std::vector<uint64_t>& fno_list,
                                                    std::vector<bool>& is_input_in_zone,
                                                    Zone** zone_out,
                                                    uint64_t min_capacity){
  Zone* allocated_zone=nullptr;
  Zone* target_zone=nullptr;
  IOStatus s;
  uint64_t max_score = 0;
  uint64_t cur_score;
  bool no_same_level_files=true;
  (void)(is_input_in_zone);




  {
    // std::lock_guard<std::mutex> lg(sst_file_map_lock_);
    for(auto fno : fno_list){
      ZoneFile* zFile=GetSSTZoneFileInZBDNoLock(fno);
      if(zFile==nullptr){
        continue;
      }
      if(zFile->selected_as_input_){
        continue;
      }
      auto extents=zFile->GetExtents();
      for(auto e : extents){
        
        if(!e->zone_->IsFull()){
          zone_score[e->zone_->zidx_-ZENFS_META_ZONES-ZENFS_SPARE_ZONES]+=e->length_;
          no_same_level_files=false;
        }
      }

    }
  }
  if(no_same_level_files){
    return IOStatus::OK();
  }

//////////////////////////////
  auto sorted = SortedByZoneScore(zone_score);

  for(auto zidx : sorted){
    // if(is_input_in_zone[i-ZENFS_META_ZONES-ZENFS_SPARE_ZONES]){
    //   continue;
    // }
    cur_score=zidx.first;
    target_zone=io_zones[zidx.second];


    
    if(cur_score == 0){
      continue;
    }
    if(cur_score<max_score){
      continue;
    }
    if(!target_zone->Acquire()){
      continue;
    }
    if(target_zone->capacity_<=min_capacity || target_zone->IsFull()){
      target_zone->Release();
      continue;
    }
    allocated_zone=target_zone;
    break;
  }

///////////////////
  // for(size_t i =ZENFS_META_ZONES+ZENFS_SPARE_ZONES; i<zone_score.size(); i++){
  //   if(is_input_in_zone[i-ZENFS_META_ZONES-ZENFS_SPARE_ZONES]){
  //     continue;
  //   }
  //   cur_score=zone_score[i];
  //   target_zone=io_zones[i-ZENFS_META_ZONES-ZENFS_SPARE_ZONES];



  //   if(cur_score == 0){
  //     continue;
  //   }
  //   if(cur_score<max_score){
  //     continue;
  //   }
  //   if(!target_zone->Acquire()){
  //     continue;
  //   }
  //   if(target_zone->capacity_<=min_capacity || target_zone->IsFull()){
  //     target_zone->Release();
  //     continue;
  //   }

  //   if(cur_score>max_score){
  //     if(allocated_zone){
  //       allocated_zone->Release();
  //       if(!s.ok()){
  //         printf("AllocateMostL0FilesZone :: fail 1\n");
  //         return s;
  //       }
  //     }
  //     allocated_zone=target_zone;
  //     max_score=cur_score;
  //     continue;
  //   }

  //   target_zone->Release();
  //   if(!s.ok()){
  //     printf("AllocateMostL0FilesZone :: fail 2\n");
  //     return s;
  //   }

  // }

  *zone_out=allocated_zone;
  return IOStatus::OK();
}


IOStatus ZonedBlockDevice::AllocateSameLevelFilesZone(Slice& smallest,Slice& largest ,
                                    const std::vector<uint64_t>& fno_list,std::vector<bool>& is_input_in_zone,
                                    Zone** zone_out,
                                    uint64_t min_capacity){
  Zone* allocated_zone = nullptr;
  IOStatus s;
  const Comparator* icmp = db_ptr_->GetDefaultICMP();
  ZoneFile* zFile;
  size_t idx;
  size_t l_idx;
  size_t r_idx;
  size_t fno_list_sz=fno_list.size();
  if(fno_list.empty()){
    return IOStatus::OK();
  }


  {
    // std::lock_guard<std::mutex> lg(sst_file_map_lock_);
    if(fno_list_sz==1) {
         zFile=GetSSTZoneFileInZBDNoLock(fno_list[0]);
         if(zFile!=nullptr){
            if(!zFile->selected_as_input_){
              s=GetNearestZoneFromZoneFile(zFile,is_input_in_zone,&allocated_zone,min_capacity);
              if(!s.ok()){
                return s;
              }    
              *zone_out=allocated_zone;
              return s;
            }
         }
    }
    // fno_list is increasing order : db/version_set.h line 580
    for(idx=0;idx< fno_list_sz;idx++){
      zFile=GetSSTZoneFileInZBDNoLock(fno_list[idx]);
      if(zFile==nullptr){
        continue;
      }
      if(zFile->selected_as_input_){
        continue;
      }
      int res=icmp->Compare(largest,zFile->smallest_);
      if(res<=0){
        res=icmp->Compare(smallest,zFile->largest_);
        assert(res<=0);
        break;
      }
    }
  
    l_idx=idx-1;
    r_idx=idx;

    // it is most smallest key file
    if(idx==0){
      for(auto it=fno_list.begin();it!=fno_list.end() ;it++){

          zFile=GetSSTZoneFileInZBDNoLock(*it);
          if(zFile==nullptr){
            continue;
          }
          if(zFile->selected_as_input_){
            continue;
          }
          s=GetNearestZoneFromZoneFile(zFile,is_input_in_zone,&allocated_zone,min_capacity);
          if(!s.ok()){
            return s;
          }
          if(allocated_zone!=nullptr){
            break;
          }
      }
    }
    // it is most largest key file
    else if(idx==fno_list_sz){
      for(auto it = fno_list.rbegin();it!=fno_list.rend();it++){
          zFile=GetSSTZoneFileInZBDNoLock(*it);
          if(zFile==nullptr){
            continue;
          }
          if(zFile->selected_as_input_){
            continue;
          }
          s=GetNearestZoneFromZoneFile(zFile,is_input_in_zone,&allocated_zone,min_capacity);
          if(!s.ok()){
            return s;
          }
          if(allocated_zone!=nullptr){
            break;
          }
      }
    }
    // it is middle key file
    else {
      // while((l_idx>=0) || (r_idx<fno_list_sz) ){
      

      for(bool flip = true; ((l_idx<r_idx) || (r_idx<fno_list_sz) )  ; flip=!flip){
        if(flip){
          if(l_idx<r_idx){
            zFile=GetSSTZoneFileInZBDNoLock(fno_list[l_idx]);
            if(zFile==nullptr){
              l_idx--;
              continue;
            }
            if(zFile->selected_as_input_){
              l_idx--;
              continue;
            }
            s=GetNearestZoneFromZoneFile(zFile,is_input_in_zone,&allocated_zone,min_capacity);
            if(!s.ok()){
              return s;
            }
            if(allocated_zone!=nullptr){
              break;
            }
            l_idx--;
          }
        }else{
          if(r_idx<fno_list_sz){
            zFile=GetSSTZoneFileInZBDNoLock(fno_list[r_idx]);
            if(zFile==nullptr){
              r_idx++;
              continue;
            }
            if(zFile->selected_as_input_){
              r_idx--;
              continue;
            }
            s=GetNearestZoneFromZoneFile(zFile,is_input_in_zone,&allocated_zone,min_capacity);
            if(!s.ok()){
              return s;
            }
            if(allocated_zone!=nullptr){
              break;
            }
            r_idx++;
          }
        }
      }


      // bool flip = true;
      // while((l_idx<r_idx) || (r_idx<fno_list_sz) ){
      //   // if(l_idx>=0){
      //   if(l_idx<r_idx && flip){
      //     flip=!flip;
      //     zFile=GetSSTZoneFileInZBDNoLock(fno_list[l_idx]);
      //     if(zFile==nullptr){
      //       continue;
      //     }
      //     s=GetNearestZoneFromZoneFile(zFile,&allocated_zone);
      //     if(!s.ok()){
      //       return s;
      //     }
      //     if(allocated_zone!=nullptr){
      //       break;
      //     }
          
      //     l_idx--;
      //   }

      //   if(r_idx<fno_list_sz&&!flip){
      //     flip=!flip;
      //     zFile=GetSSTZoneFileInZBDNoLock(fno_list[r_idx]);
      //     if(zFile==nullptr){
      //       continue;
      //     }
      //     s=GetNearestZoneFromZoneFile(zFile,&allocated_zone);
      //     if(!s.ok()){
      //       return s;
      //     }
      //     if(allocated_zone!=nullptr){
      //       break;
      //     }
      //     r_idx++;
      //   }
      // }
    }
  }

  *zone_out=allocated_zone;
  return s;
}

  void ZonedBlockDevice::AdjacentFileList(Slice& smallest ,Slice& largest, int level, std::vector<uint64_t>& fno_list){
    assert(db_ptr_!=nullptr);
    fno_list.clear();
    if(db_ptr_==nullptr){
      return;
    }
    db_ptr_->AdjacentFileList(smallest,largest,level,fno_list);
  }


  uint64_t ZonedBlockDevice::MostSmallDownwardAdjacentFile(Slice& s, Slice& l, int level){
    if(db_ptr_==nullptr){
      return 0;
    }
    return db_ptr_->MostSmallDownwardAdjacentFile(s,l,level);
  }
  uint64_t ZonedBlockDevice::MostLargeUpperAdjacentFile(Slice& s, Slice& l, int level){
    if(db_ptr_==nullptr){
      return 0;
    }
    return db_ptr_->MostLargeUpperAdjacentFile(s,l,level);
  }
  void ZonedBlockDevice::DownwardAdjacentFileList(Slice& s, Slice& l, int level, std::vector<uint64_t>& fno_list){
    if(db_ptr_==nullptr){
      return;
    }
    db_ptr_->DownwardAdjacentFileList(s,l,level,fno_list);
  }

  // return most large one
  // uint64_t ZonedBlockDevice::MostLargeUpperAdjacentFile(Slice& smallest ,Slice& largest, int level){
  //   assert(db_ptr_!=nullptr);

  //   return db_ptr_->MostLargeUpperAdjacentFile(smallest,largest,level);
  // }

  void ZonedBlockDevice::SameLevelFileList(int level, std::vector<uint64_t>& fno_list,bool exclude_being_compacted){
    assert(db_ptr_!=nullptr);
    fno_list.clear();
    // printf("level %d",level);
    db_ptr_->SameLevelFileList(level,fno_list,exclude_being_compacted);
  }

  IOStatus ZonedBlockDevice::GetNearestZoneFromZoneFile(ZoneFile* zFile,std::vector<bool>& is_input_in_zone,
                                                        Zone** zone_out,
                                                        uint64_t min_capacity){
    // IOStatus s;


   
    // Zone* allocated_zone=nullptr;
    std::vector<std::pair<uint64_t,uint64_t>> zone_score(io_zones.size(),{0,0});
    auto extents = zFile->GetExtents();
    (void)(is_input_in_zone);

    for(auto e : extents){
      uint64_t zidx= e->zone_->zidx_-ZENFS_META_ZONES-ZENFS_SPARE_ZONES;
      zone_score[zidx].second=zidx;
      zone_score[zidx].first+=e->length_;
    }

    std::sort(zone_score.rbegin(),zone_score.rend());

    for(auto zscore : zone_score){

      uint64_t score=zscore.first;
      uint64_t zidx = zscore.second;
      // printf("zscore : %lu zidx %lu\n",score>>20,zidx);
      if(score==0){
        break;
      }
      Zone* z = io_zones[zidx]; 

      if(!z->Acquire()){
        continue;
      }
      if(z->capacity_<=min_capacity || z->IsFull()){
        z->Release();
        continue;
      }
      // printf("return %lu\n",zidx);
      *zone_out=io_zones[zidx];
      return IOStatus::OK();
    }
    // printf("cannotget T.T\n");
    // for(auto e : extents){
    //   if(is_input_in_zone[e->zone_->zidx_-ZENFS_META_ZONES-ZENFS_SPARE_ZONES]){
    //     continue;
    //   }
    //   if(!e->zone_->Acquire()){
    //     continue;
    //   }
    //   if(e->zone_->capacity_<=min_capacity || e->zone_->IsFull()){
    //     e->zone_->Release();
    //     continue;
    //   }
    //   // if(e->zone_->IsFull()){
    //   //   e->zone_->Release();
    //   //   continue;
    //   // }
    //   allocated_zone=e->zone_;
    //   break;
    // }

    // *zone_out=allocated_zone;
    return IOStatus::OK();
  }

IOStatus ZonedBlockDevice::AllocateEmptyZone(Zone **zone_out) {
  IOStatus s;
  Zone *allocated_zone = nullptr;
  std::vector<Zone*> sorted_zone=io_zones;
  // if(allocation_scheme_==CAZA_W){
  //   sort(sorted_zone.begin(),sorted_zone.end(),Zone::SortByResetCount);
  // }

  for (const auto z : sorted_zone) {
    if (z->Acquire()) {
      if (z->IsEmpty()) {
        allocated_zone = z;
        break;
      } else {
        z->Release();
        if (!s.ok()) return s;
      }
    }
  }
  *zone_out = allocated_zone;
  return IOStatus::OK();
}

int ZonedBlockDevice::Read(char *buf, uint64_t offset, int n, bool direct) {
  int ret = 0;
  int left = n;
  int r = -1;
  auto start_chrono = std::chrono::high_resolution_clock::now();
  (void)(direct);
  while (left) {
    r = zbd_be_->Read(buf, left, offset, direct);
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ret += r;
    buf += r;
    left -= r;
    offset += r;
  }
  auto elapsed = std::chrono::high_resolution_clock::now() - start_chrono;
  
  long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();

  read_latency_sum_.fetch_add(microseconds);
  read_n_.fetch_add(1);
  if (r < 0) return r;
  return ret;
}

  // void ZonedBlockDevice::TakeSMRMigrateZone(Zone** out_zone){
  //   WaitForOpenIOZoneToken(true);
  //   while(GetActiveIOZoneTokenIfAvailable()==false);
  //   while((*out_zone)==nullptr){
  //     AllocateEmptyZone(out_zone);
  //   }
  // }

void ZonedBlockDevice::TakeSMRMigrateZone(Zone** out_zone,Env::WriteLifeTimeHint file_lifetime,uint64_t should_be_copied){
//     // 
//     WaitForOpenIOZoneToken(false);
//     while(GetActiveIOZoneTokenIfAvailable()==false);
//     while((*out_zone)==nullptr){
//       AllocateEmptyZone(out_zone);
//     }
//     (void)(should_be_copied);
//     (*out_zone)->lifetime_=file_lifetime;

  uint64_t try_n= 0 ;
  should_be_copied+=4096*256;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  WaitForOpenIOZoneToken(false,ZC);

  while((*out_zone)==nullptr){
    
    
    GetBestOpenZoneMatch(file_lifetime, &best_diff,std::vector<uint64_t>() ,out_zone, should_be_copied);

    if((*out_zone)!=nullptr){
      break;
    }
    


    if(GetActiveIOZoneTokenIfAvailable()){
      AllocateEmptyZone(out_zone);
      if((*out_zone)!=nullptr){
        (*out_zone)->lifetime_=file_lifetime;
        break;
      }else{
        PutActiveIOZoneToken();
      }
    }
    
    GetAnyLargestRemainingZone(out_zone,false,should_be_copied);
    if((*out_zone)!=nullptr){
      break;
    }

    try_n++;
    if(try_n>256){
      FinishCheapestIOZone();
    }
  }
}
  void ZonedBlockDevice::ReleaseSMRMigrateZone(Zone* zone){
    if (zone != nullptr) {
      bool full = zone->IsFull();
      zone->Close();
      zone->Release();
      
       PutOpenIOZoneToken(ZC);
      if(full){
        PutActiveIOZoneToken();
      }
    }
  }



IOStatus ZonedBlockDevice::ReleaseMigrateZone(Zone *zone) {
  IOStatus s = IOStatus::OK();
  {
    
    // std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
    // migrating_ = false;
    if (zone != nullptr) {
      // PutActiveIOZoneToken();

      bool full = zone->IsFull();
      zone->Close();
      // PutMigrationIOZoneToken();
      zone->Release();

      PutOpenIOZoneToken(ZC);

      if(full){
        PutActiveIOZoneToken();
        // PutMigrationIOZoneToken();
      }

      Info(logger_, "ReleaseMigrateZone: %lu", zone->start_);
    }
  }
  // migrate_resource_.notify_one();
  return s;
}

// IOStatus ZonedBlockDevice::TakeMigrateZone(Zone **out_zone,
//                                            Env::WriteLifeTimeHint file_lifetime,
//                                            uint64_t min_capacity,
//                                            bool* run_gc_worker_) {
IOStatus ZonedBlockDevice::TakeMigrateZone(Slice& smallest,Slice& largest, int level,Zone **out_zone,
                                           Env::WriteLifeTimeHint file_lifetime,uint64_t file_size,
                                           uint64_t min_capacity, bool* run_gc_worker_,
                                           bool is_sst) {
  // std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
  // ZenFSStopWatch z1("TakeMigrateZone");
  if((*run_gc_worker_)==false){
      migrating_=false;
      return IOStatus::OK();
  } 
  // migrate_resource_.wait(lock, [this] { return !migrating_; });
  IOStatus s;
  migrating_ = true;
  // bool force_get=false;
  int blocking_time=0;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  
  // {
  //   // ApplyFinishThreshold();
  // }
  (void)(smallest);
  (void)(largest);
  (void)(level);
  std::vector<uint64_t> none;
  //  no_input_fno_(0);
  // WaitForOpenIOZoneToken(false);

  // WaitForMigrationIOZoneToken();
  WaitForOpenIOZoneToken(false,ZC);

  while(CalculateCapacityRemain()>min_capacity){
    if((*run_gc_worker_)==false){
      migrating_=false;
      return IOStatus::OK();
    }
    // AllocateIOZone(is_sst,smallest,largest,level,file_lifetime,IOType::kWAL
    // , none,file_size,out_zone,min_capacity);
    if(is_sst){
      if(allocation_scheme_==CAZA){
        // printf("AllocateCompactionAwaredZone\n");
        AllocateCompactionAwaredZone(smallest,largest,level,file_lifetime,std::vector<uint64_t> (0),file_size,out_zone,min_capacity);
      }else if(allocation_scheme_==CAZA_ADV){
        // printf("AllocateCompactionAwaredZoneV2\n");
        AllocateCompactionAwaredZoneV2(smallest,largest,level,file_lifetime,std::vector<uint64_t> (0),file_size,out_zone,min_capacity);
        if (s.ok() && (*out_zone) != nullptr) {
          break;
        }

        if(GetActiveIOZoneTokenIfAvailable()){
          s=AllocateEmptyZone(out_zone); 
        if (s.ok() && (*out_zone) != nullptr) {
          Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
          (*out_zone)->lifetime_=file_lifetime;
          break;
        }else{
          PutActiveIOZoneToken();
        }
      } 
      
      }else{

        // printf("I am LIZA!\n");
      }
      
      if (s.ok() && (*out_zone) != nullptr) {
        break;
      }
    }

    s=GetBestOpenZoneMatch(file_lifetime, &best_diff,std::vector<uint64_t>() ,out_zone, min_capacity);
    


    if (s.ok() && (*out_zone) != nullptr) {
      Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
      break;
    }

    s = GetAnyLargestRemainingZone(out_zone,false,min_capacity);
    if(s.ok()&&(*out_zone)!=nullptr){
      Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
      break;
    }

    if(GetActiveIOZoneTokenIfAvailable()){
      s=AllocateEmptyZone(out_zone); 
      if (s.ok() && (*out_zone) != nullptr) {
        Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
        (*out_zone)->lifetime_=file_lifetime;
        break;
      }else{
        PutActiveIOZoneToken();
        // PutMigrationIOZoneToken();
      }
    }


    // FinishCheapestIOZone();




    s = ResetUnusedIOZones();
    // usleep(1000 * 1000);
    // sleep(1);
    blocking_time++;
    if(blocking_time>256){
      // FinishCheapestIOZone(false);
      // MoveResources(true);
      FinishCheapestIOZone();
      // s=AllocateEmptyZone(out_zone); 
      // if (s.ok() && (*out_zone) != nullptr) {
      //   Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
      //   (*out_zone)->lifetime_=file_lifetime;
      //   break;
      // }
      // // else{
      // //   // PutActiveIOZoneToken();
      // //   // PutMigrationIOZoneToken();
      // // }
    }
    if(!s.ok()){
      return s;
    }
  }

  
  if (s.ok() && (*out_zone) != nullptr) {
    Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
  } else {
    migrating_=false;
  }
  (*out_zone)->state_=Zone::State::OPEN;
  return s;
}

IOStatus ZonedBlockDevice::AllocateIOZone(std::string fname ,bool is_sst,Slice& smallest,Slice& largest, int level,
                                          Env::WriteLifeTimeHint file_lifetime, IOType io_type, 
                                          std::vector<uint64_t>& input_fno,uint64_t predicted_size,
                                          Zone **out_zone,uint64_t min_capacity) {
  
  auto start_chrono = std::chrono::high_resolution_clock::now();
  // RuntimeReset();
  Zone *allocated_zone = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  IOStatus s;
  
  auto tag = ZENFS_WAL_IO_ALLOC_LATENCY;
  if (io_type != IOType::kWAL) {
    // L0 flushes have lifetime MEDIUM
    if (file_lifetime == Env::WLTH_MEDIUM) {
      tag = ZENFS_L0_IO_ALLOC_LATENCY;
    } else {
      tag = ZENFS_NON_WAL_IO_ALLOC_LATENCY;
    }
  }
  // for(uint64_t fno : input_fno){
  //   DeleteSSTFileforZBDNoLock(fno);
  // }
  

  ZenFSMetricsLatencyGuard guard(metrics_, tag, Env::Default());
  metrics_->ReportQPS(ZENFS_IO_ALLOC_QPS, 1);

  // Check if a deferred IO error was set
  s = GetZoneDeferredStatus();
  if (!s.ok()) {
    return s;
  }

  // if (io_type != IOType::kWAL) {
  //   s = ApplyFinishThreshold();
  //   if (!s.ok()) {
  //     return s;
  //   }
  // }
    // L0,L1,L2,L3,L4,ZC,WAL
  WaitForOpenZoneClass open_class;
  if(io_type == IOType::kWAL){
    open_class=WAL;
  }else{
    switch (level)
    {
    case 0:
      open_class=L0;
      break;
    case 1:
      open_class = L1;
      break;
    case 2:
      open_class = L2;
      break;
    case 3:
      open_class = L3;
      break;
    case 4:
      open_class = L4;
      break;
    case 5:
      open_class = L5;
      break;
    default:
      printf("fname ?? %s level ?? %d\n",fname.c_str(),level);
      open_class = L5;
      break;
    }
  }
  WaitForOpenIOZoneToken(io_type == IOType::kWAL,open_class);
  
  if(is_sst&&level>=0 && allocation_scheme_==CAZA){
    s = AllocateCompactionAwaredZone(smallest,largest,level,file_lifetime,std::vector<uint64_t>(0),predicted_size,&allocated_zone,min_capacity);

    if(!s.ok()){
      PutOpenIOZoneToken(open_class);
      return s;
    }
  }else if(is_sst&&level>=0 && allocation_scheme_==CAZA_ADV){
    s = AllocateCompactionAwaredZoneV2(smallest,largest,level,file_lifetime,std::vector<uint64_t>(0),predicted_size,&allocated_zone,min_capacity);
    // printf("AllocateCompactionAwaredZoneV2\n");
    if(!s.ok()){
      PutOpenIOZoneToken(open_class);
      return s;
    }

    if(allocated_zone!=nullptr){
      goto end;
    }
    if(GetActiveIOZoneTokenIfAvailable()){
      s = AllocateEmptyZone(&allocated_zone);
      if(allocated_zone==nullptr){
        PutActiveIOZoneToken();
      }
    }
  }

  if(allocated_zone!=nullptr){
    goto end;
  }

  // if(GetActiveIOZoneTokenIfAvailable()){
  //   s = AllocateEmptyZone(&allocated_zone);
  //   if (allocated_zone != nullptr&&s.ok()) {
  //     // assert(allocated_zone->IsBusy());
  //     allocated_zone->lifetime_ = file_lifetime;
  //     new_zone = true;
  //     goto end;
  //   } else {
  //     PutActiveIOZoneToken();
  //   }
  // }

  // if(allocated_zone!=nullptr){
   
  // }

  /* Try to fill an already open zone(with the best life time diff) */
  s = GetBestOpenZoneMatch(file_lifetime, &best_diff,input_fno ,&allocated_zone,min_capacity);
  if (!s.ok()) {
    PutOpenIOZoneToken(open_class);
    return s;
  }

  // Holding allocated_zone if != nullptr

  if (allocated_zone==nullptr && GetActiveIOZoneTokenIfAvailable()) {
    s = AllocateEmptyZone(&allocated_zone);
    if (allocated_zone != nullptr&&s.ok()) {
      // assert(allocated_zone->IsBusy());
      allocated_zone->lifetime_ = file_lifetime;
      new_zone = true;
    } else {
      PutActiveIOZoneToken();
    }
    // }



    // bool got_token = GetActiveIOZoneTokenIfAvailable();

    /* If we did not get a token, try to use the best match, even if the life
     * time diff not good but a better choice than to finish an existing zone
     * and open a new one
     */

    /* If we haven't found an open zone to fill, open a new zone */
    // if (allocated_zone == nullptr) {
    //   /* We have to make sure we can open an empty zone */
    //   while (!got_token && !GetActiveIOZoneTokenIfAvailable()) {
    //     // printf("AllocateIOZone :: Sucks here?\n");
    //     s = FinishCheapestIOZone();
    //     if (!s.ok()) {
    //       PutOpenIOZoneToken();
    //       return s;
    //     }
    //   }

    //   s = AllocateEmptyZone(&allocated_zone);

    //   if (!s.ok()) {
    //     PutActiveIOZoneToken();
    //     PutOpenIOZoneToken();
    //     return s;
    //   }

    //   if (allocated_zone != nullptr) {
    //     // assert(allocated_zone->IsBusy());
    //     allocated_zone->lifetime_ = file_lifetime;
    //     new_zone = true;
    //   } else {
    //     PutActiveIOZoneToken();
    //   }
    // }
  }
  if(s.ok()&&allocated_zone==nullptr){
    s=GetAnyLargestRemainingZone(&allocated_zone,false,min_capacity);
    if(allocated_zone){
      allocated_zone->lifetime_=file_lifetime;
    }
  }

  if (allocated_zone) {
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
  } else {
    PutOpenIOZoneToken(open_class);
  }
  // if(allocated_zone==nullptr){
  //   s=GetAnyLargestRemainingZone(&allocated_zone,false);
  // }

  if (io_type != IOType::kWAL) {
    LogZoneStats();
  }
end:
  *out_zone = allocated_zone;

  auto elapsed = std::chrono::high_resolution_clock::now() - start_chrono;
  long long nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
  (void)(nanoseconds);
  // printf("\t\t\t\t%llu\n",nanoseconds);
  metrics_->ReportGeneral(ZENFS_OPEN_ZONES_COUNT, open_io_zones_);
  metrics_->ReportGeneral(ZENFS_ACTIVE_ZONES_COUNT, active_io_zones_);

  return IOStatus::OK();
}

std::string ZonedBlockDevice::GetFilename() { return zbd_be_->GetFilename(); }

uint64_t ZonedBlockDevice::GetBlockSize() { return 4096; }

uint64_t ZonedBlockDevice::GetZoneSize() { return zbd_be_->GetZoneSize(); }

uint64_t ZonedBlockDevice::GetNrZones() { return zbd_be_->GetNrZones(); }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream,
                                      const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, meta_zones);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones);
  json_stream << "}";
}

IOStatus ZonedBlockDevice::GetZoneDeferredStatus() {
  std::lock_guard<std::mutex> lock(zone_deferred_status_mutex_);
  return zone_deferred_status_;
}

void ZonedBlockDevice::SetZoneDeferredStatus(IOStatus status) {
  std::lock_guard<std::mutex> lk(zone_deferred_status_mutex_);
  if (!zone_deferred_status_.ok()) {
    zone_deferred_status_ = status;
  }
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot) {
  for (auto *zone : io_zones) {
    snapshot.emplace_back(zone);
  }
}

// void ZonedBlockDevice::GetZoneExtentSnapShotInZoneSnapshot(std::vector<ZoneSnapshot>* zone_snapshot,
//           std::vector<ZoneExtentSnapshot>& extents_snapshot){
//     // for(auto& ext : extents_snapshot){
//     //   (*zone_snapshot)[ext.zone_p->zidx_-ZENFS_SPARE_ZONES-ZENFS_META_ZONES].extents_in_zone.push_back(ext);
      
      
//     // }
//     return;
// }

  bool ZonedBlockDevice::SetSSTFileforZBDNoLock(uint64_t fno,ZoneFile* zoneFile){
    // auto sst=sst_file_map_.find(fno);
    // if(sst!=sst_file_map_.end()){
    //   return false;
    // }
    // zoneFile->fno_=fno;
    // sst_file_map_.insert({fno,zoneFile});
    // return true;

    auto sst=sst_file_bitmap_[fno];
    if(sst!=nullptr){ // already set
      return false;
    }
    zoneFile->fno_=fno;
    sst_file_bitmap_[fno]=zoneFile;
    return true;
  }
  bool ZonedBlockDevice::DeleteSSTFileforZBDNoLock(uint64_t fno){
    // if(fno==-1){
    //   return false;
    // }
  
    // auto sst=sst_file_map_.find(fno);
    // if(sst==sst_file_map_.end()){
    //   return false;
    // }
    // sst_file_map_.erase(fno);
    auto sst=sst_file_bitmap_[fno];
    if(sst==nullptr){
      return false;
    }
    sst_file_bitmap_[fno]=nullptr;
    return true;
  }
  ZoneFile* ZonedBlockDevice::GetSSTZoneFileInZBDNoLock(uint64_t fno){
    // auto ret=sst_file_map_.find(fno);
    // if(ret==sst_file_map_.end()){
    //   return nullptr;
    // }
    // if((*ret).second->IsDeleted()){
    //   return nullptr;
    // }
    // return (*ret).second;

    auto ret=sst_file_bitmap_[fno];
    if(ret==nullptr){
      return nullptr;
    }
    if(ret!=nullptr&&ret->IsDeleted()){
      return nullptr;
    }
    return ret;
  }

IOStatus ZonedBlockDevice::ResetAllZonesForForcedNewFileSystem(void) {
  IOStatus s;
  for(auto* zone : meta_zones){
    s = zone->Reset();
    if(!s.ok()){
      return s;
    }
  }
  for(auto* zone : io_zones){
    s = zone->Reset();
    if(!s.ok()){
      return s;
    }
  }
  return s;
}

void ZonedBlockDevice::PrintZoneToFileStatus(void){
  int zidx = 0;
  for(const auto z: io_zones){
    printf("\n");
    printf("-------------------------------------\n");
    // if(!z->Acquire()){
      // printf("[%d] :: someone writing or resetting\n",zidx++);
      // continue;
    // }
    printf("[%d]",zidx++);
    z->PrintZoneExtent(true);
    z->Release();
    }

}



uint64_t Zone::PrintZoneExtent(bool print){
  if(print){
   printf("[%lu] :: WP %lu / start %lu extents # %lu\n",
            zidx_,wp_,start_,zone_extents_.size());          
  }
  uint64_t sum_len = 0;
  // int i = 0 ;
  for(size_t i = 0 ;i <zone_extents_.size();i++){
    if(print){
      printf("[%4lu] ",i);
    }   
    ZoneExtent* ze= zone_extents_[i];
    if(ze!=nullptr)
      sum_len += ze->PrintExtentInfo(print);
    // sum_len += ze->length_+ze->header_size_;
  }
  if(print){
    printf("---------------------sum_len : %lu\n",sum_len);
  }
  // return expected wp
  return sum_len;
}

uint64_t Zone::GetBlockSize() { return 4096; }


void Zone::PushExtent(ZoneExtent* ze) {
  if(zbd_->RuntimeZoneResetOnly()){
    return;
  }
  // if(zone_extents_lock_.locked_by_caller()){
  //   while(zone_extents_lock_.locked_by_caller()){
  //     printf("Zone::PushExtent :: zone_extents_lock_ sucks here\n");
  //     sleep(1);
  //   }
  // }
  
  std::lock_guard<std::mutex> lg(zone_extents_lock_);
  zone_extents_.push_back(ze);

  // sort(zone_extents_.begin(),zone_extents_.end(),ZoneExtent::cmp);
  // zone_extents_lock_.unlock();
}

/* Called at Partial Reset, already zone extent lock is held
  no need to lock zone_extent_lock_
*/
void Zone::PushExtentAtFront(ZoneExtent* ze) {
  if(zbd_->RuntimeZoneResetOnly()){
    return;
  }
  
  zone_extents_.push_front(ze);
}

  void ZonedBlockDevice::ZCorPartialLock() {zc_or_partial_lock_.lock(); }
  bool ZonedBlockDevice::ZCorPartialTryLock() { return zc_or_partial_lock_.try_lock(); }
  void ZonedBlockDevice::ZCorPartialUnLock() {zc_or_partial_lock_.unlock(); }

  bool Zone::TryZoneWriteLock(){

    if(wp_-start_<erase_unit_size_){
      return false;
    }

    // if(!zbd_->ZCorPartialTryLock()){
    //   return false;
    // }


    if(!zone_extents_lock_.try_lock()){

      return false;
    }

    if(!zone_lock_.try_lock()){
      zone_extents_lock_.unlock();
      return false;
    }



    // 1. spin lock
    // while(zone_readers_.load()!=0 && Acquire()  );
    
    // 2. pass
    if(zone_readers_.load()>0 ){
      zone_extents_lock_.unlock();
      zone_lock_.unlock();
      return false;
    }
    return true;
  }


}
  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
