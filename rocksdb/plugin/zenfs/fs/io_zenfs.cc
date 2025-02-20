// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "io_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
// class ZenFS;
ZoneExtent::ZoneExtent(uint64_t start, uint64_t length, Zone* zone,std::string fname,uint64_t now_micros,ZoneFile* zfile)
    : start_(start), length_(length), zone_(zone), is_invalid_(false), fname_(fname), header_size_(0),last_accessed_(now_micros),
    zfile_(zfile) {
      // printf("ZoneExtent :: %lu \n",now_micros);
      if(zone==nullptr){
        return;
      }

      // printf("function 1 %lu %lu\n",start_,length_);
      // if(zone!=nullptr){
      zone->PushExtent(this);
      // }
      uint64_t block_sz = zone_->GetBlockSize();
      uint64_t align = (length_ + header_size_)%block_sz;
      if(align){
        pad_size_= block_sz-align;
      }
    }
ZoneExtent::ZoneExtent(uint64_t start, uint64_t length, Zone* zone)
    : start_(start), length_(length), zone_(zone), is_invalid_(true), header_size_(0) {
      if(zone==nullptr){
        return;
      }
      // printf("function 2 %lu %lu\n",start_,length_);
      fname_="Invalid[PushAtFront]";
      uint64_t block_sz = zone_->GetBlockSize();
      uint64_t align = (length_ + header_size_)%block_sz;
      if(align){
        pad_size_= block_sz-align;
      }
   
      zone->PushExtentAtFront(this);
      
  }
ZoneExtent::ZoneExtent(uint64_t start, uint64_t length, Zone* zone, std::string fname, uint64_t header_size,uint64_t now_micros,
ZoneFile* zfile) 
: start_(start), length_(length), zone_(zone), is_invalid_(false), fname_(fname), header_size_(header_size),last_accessed_(now_micros),
zfile_(zfile) {
    // printf("ZoneExtent :: %lu \n",now_micros);
  if(zone==nullptr){
      return;
  }
  // printf("function 3 %lu %lu\n",start_,length_);
  // if(zone!=nullptr){
  zone->PushExtent(this);
  // }
  uint64_t block_sz = zone_->GetBlockSize();
  uint64_t align = (length_ + header_size_)%block_sz;
  if(align){
    pad_size_= block_sz-align;
  }
}


Status ZoneExtent::DecodeFrom(Slice* input) {
  if (input->size() != (sizeof(start_) + sizeof(length_)))
    return Status::Corruption("ZoneExtent", "Error: length missmatch");
  uint64_t st=start_;
  GetFixed64(input, &st);
  GetFixed64(input, &length_);
  // start_=st;
  return Status::OK();
}

void ZoneExtent::EncodeTo(std::string* output) {
  PutFixed64(output, start_);
  PutFixed64(output, length_);
}

void ZoneExtent::EncodeJson(std::ostream& json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"length\":" << length_;
  json_stream << "}";
}
uint64_t ZoneExtent::PrintExtentInfo(bool print){
  uint64_t block_sz = zone_->GetBlockSize();
  uint64_t align = (length_ + header_size_)%block_sz;
  uint64_t pad_sz = 0;
  if(print){
    printf("[%s] :%s :: start_ %lu / len %lu zidx %lu header_sz %lu", is_invalid_ ? "Invalid" : "Valid",fname_.c_str() ,
          start_-header_size_,length_+header_size_,zone_->zidx_,header_size_);
  }
  if(align){
    pad_sz= block_sz-align;
    if(print)
      printf("/ pad_size_ :%lu / calcualted pad sz : %lu\n",pad_size_,pad_sz);
    pad_size_=pad_sz;
  }else{
    if(print)
      printf("/ pad_size_ %lu\n",pad_size_);
  }
  
  //return total block align size
  return  header_size_+ length_  + pad_size_;
}

enum ZoneFileTag : uint32_t {
  kFileID = 1,
  kFileNameDeprecated = 2,
  kFileSize = 3,
  kWriteLifeTimeHint = 4,
  kExtent = 5,
  kModificationTime = 6,
  kActiveExtentStart = 7,
  kIsSparse = 8,
  kLinkedFilename = 9,
};

void ZoneFile::EncodeTo(std::string* output, uint32_t extent_start) {
  PutFixed32(output, kFileID);
  PutFixed64(output, file_id_);

  PutFixed32(output, kFileSize);
  PutFixed64(output, file_size_);

  PutFixed32(output, kWriteLifeTimeHint);
  PutFixed32(output, (uint32_t)lifetime_);

  for (uint32_t i = extent_start; i < extents_.size(); i++) {
    std::string extent_str;

    PutFixed32(output, kExtent);
    extents_[i]->EncodeTo(&extent_str);
    PutLengthPrefixedSlice(output, Slice(extent_str));
  }

  PutFixed32(output, kModificationTime);
  PutFixed64(output, (uint64_t)m_time_);

  /* We store the current extent start - if there is a crash
   * we know that this file wrote the data starting from
   * active extent start up to the zone write pointer.
   * We don't need to store the active zone as we can look it up
   * from extent_start_ */
  PutFixed32(output, kActiveExtentStart);
  PutFixed64(output, extent_start_);

  if (is_sparse_) {
    PutFixed32(output, kIsSparse);
  }

  for (uint32_t i = 0; i < linkfiles_.size(); i++) {
    PutFixed32(output, kLinkedFilename);
    PutLengthPrefixedSlice(output, Slice(linkfiles_[i]));
  }
}

void ZoneFile::EncodeJson(std::ostream& json_stream) {
  json_stream << "{";
  json_stream << "\"id\":" << file_id_ << ",";
  json_stream << "\"size\":" << file_size_ << ",";
  json_stream << "\"hint\":" << lifetime_ << ",";
  json_stream << "\"extents\":[";

  for (const auto& name : GetLinkFiles())
    json_stream << "\"filename\":\"" << name << "\",";

  bool first_element = true;
  for (ZoneExtent* extent : extents_) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    extent->EncodeJson(json_stream);
  }
  json_stream << "]}";
}

Status ZoneFile::DecodeFrom(Slice* input) {
  uint32_t tag = 0;
  std::string filename;
  // uint64_t align=0;
  GetFixed32(input, &tag);
  if (tag != kFileID || !GetFixed64(input, &file_id_))
    return Status::Corruption("ZoneFile", "File ID missing");

  while (true) {
    Slice slice;
    ZoneExtent* extent;
    Status s;

    if (!GetFixed32(input, &tag)) break;

    switch (tag) {
      case kFileSize:
        if (!GetFixed64(input, &file_size_))
          return Status::Corruption("ZoneFile", "Missing file size");
        break;
      case kWriteLifeTimeHint:
        uint32_t lt;
        if (!GetFixed32(input, &lt))
          return Status::Corruption("ZoneFile", "Missing life time hint");
        lifetime_ = (Env::WriteLifeTimeHint)lt;
        break;
      case kExtent:
        filename="NONE(Decodefrom)";
        // not push to zone extents here
        extent = new ZoneExtent(0, 0, nullptr,filename,0,nullptr);
        GetLengthPrefixedSlice(input, &slice);
        s = extent->DecodeFrom(&slice);
        if (!s.ok()) {
          printf("hello ???@@@@@ @@@@@@@@@@@@@@?? %s\n",filename.c_str());
          delete extent;
          return s;
        }
        // extent->zone_ = zbd_->GetIOZone(extent->start_);
        
        // if (!extent->zone_){
        //   printf("%lu\n\n",extent->start_);
        //   return Status::Corruption("ZoneFile", "Invalid zone extent");
        // }
        // // push at here
        // extent->zone_->PushExtent(extent);
        // align=(extent->length_+extent->header_size_)%4096;
        // if(align){
        //   extent->pad_size_=4096-align;
        // }
        // extent->zone_->used_capacity_ += extent->length_;
        extents_.push_back(extent);
        break;
      case kModificationTime:
        uint64_t ct;
        if (!GetFixed64(input, &ct))
          return Status::Corruption("ZoneFile", "Missing creation time");
        m_time_ = (time_t)ct;
        break;
      case kActiveExtentStart:
        uint64_t es;
        if (!GetFixed64(input, &es))
          return Status::Corruption("ZoneFile", "Active extent start");
        extent_start_ = es;
        break;
      case kIsSparse:
        is_sparse_ = true;
        break;
      case kLinkedFilename:
        if (!GetLengthPrefixedSlice(input, &slice))
          return Status::Corruption("ZoneFile", "LinkFilename missing");

        if (slice.ToString().length() == 0)
          return Status::Corruption("ZoneFile", "Zero length Linkfilename");

        linkfiles_.push_back(slice.ToString());
        break;
      default:
        return Status::Corruption("ZoneFile", "Unexpected tag");
    }
  }

  MetadataSynced();
  return Status::OK();
}

Status ZoneFile::MergeUpdate(std::shared_ptr<ZoneFile> update, bool replace) {
  if (file_id_ != update->GetID())
    return Status::Corruption("ZoneFile update", "ID missmatch");
  // printf("MergeUpdate?\n\n");
  SetFileSize(update->GetFileSize());
  SetWriteLifeTimeHint(update->GetWriteLifeTimeHint());
  SetFileModificationTime(update->GetFileModificationTime());

  if (replace) {
    ClearExtents();
  }
  std::string filename;
  if(linkfiles_.size()){
    filename=linkfiles_[0];
  }else{
    filename="NONE(MERGEUPDATE)";
  }
  ZoneExtent* new_ext=nullptr;
  std::vector<ZoneExtent*> update_extents = update->GetExtents();
  for (long unsigned int i = 0; i < update_extents.size(); i++) {
    ZoneExtent* extent = update_extents[i];
    Zone* zone = extent->zone_;
    if(!zone){
      continue;
    }
    zone->used_capacity_ += extent->length_;
    printf("ZoneFile::MergeUpdate :: %lu %lu\n",extent->start_, extent->length_);
    new_ext=new ZoneExtent(extent->start_, extent->length_, nullptr,filename,0,nullptr);
    new_ext->zone_=zone;
    extents_.push_back(new_ext);
  }
  
  extent_start_ = update->GetExtentStart();
  is_sparse_ = update->IsSparse();
  MetadataSynced();

  linkfiles_.clear();
  for (const auto& name : update->GetLinkFiles()) linkfiles_.push_back(name);

  return Status::OK();
}

ZoneFile::ZoneFile(ZonedBlockDevice* zbd, uint64_t file_id,
                   MetadataWriter* metadata_writer, FileSystemWrapper* zenfs)
    : zbd_(zbd),
      active_zone_(NULL),
      extent_start_(NO_EXTENT),
      extent_filepos_(0),
      lifetime_(Env::WLTH_NOT_SET),
      io_type_(IOType::kUnknown),
      file_size_(0),
      file_id_(file_id),
      nr_synced_extents_(0),
      m_time_(0),
      metadata_writer_(metadata_writer),
      zenfs_(zenfs) {}
std::string ZoneFile::GetFilename() { return linkfiles_[0]; }
time_t ZoneFile::GetFileModificationTime() { return m_time_; }

uint64_t ZoneFile::GetFileSize() { return file_size_; }
void ZoneFile::SetFileSize(uint64_t sz) { file_size_ = sz; }
void ZoneFile::SetFileModificationTime(time_t mt) { m_time_ = mt; }
void ZoneFile::SetIOType(IOType io_type) { io_type_ = io_type; }

ZoneFile::~ZoneFile() { 
  // printf("file size %lu\n",file_size_>>20);
  ClearExtents();
  // ReleaseWRLock();
 }

void ZoneFile::ClearExtents() {
  for (auto e = std::begin(extents_); e != std::end(extents_); ++e) {
    if((*e)==nullptr){
      continue;
    }
    Zone* zone = (*e)->zone_;
    if(!zone){
      // printf("why zone nullptr? ext start : %lu\n",(*e)->start_);
      continue;
    }
    assert(zone && zone->used_capacity_ >= (*e)->length_);
    if(zone->used_capacity_<(*e)->length_){
      // printf("clearextents Error here@@@ %s %lu %lu\n",GetFilename().c_str(),zone->used_capacity_.load(),(*e)->length_);
      zone->used_capacity_=0;
      continue;
    }

    std::shared_ptr<char> tmp_cache = (*e)->page_cache_;
    if(tmp_cache!=nullptr){
      zbd_->page_cache_size_ -= (*e)->length_;
    }
    zone->used_capacity_.fetch_sub((*e)->length_); 
    delete (*e);
  }

  // for (auto e = std::begin(extents_); e != std::end(extents_); ++e) {
  //   if((*e)==nullptr){
  //     continue;
  //   }
  //   (*e)->is_invalid_=true;
   
  // }
  extents_.clear();
}

IOStatus ZoneFile::CloseActiveZone() {
  IOStatus s = IOStatus::OK();
  if (active_zone_) {
    bool full = active_zone_->IsFull();
    s = active_zone_->Close();
    ReleaseActiveZone();
    if (!s.ok()) {
      return s;
    }
    WaitForOpenZoneClass open_class;
    if(is_sst_){
      open_class= (WaitForOpenZoneClass)(level_+2);
    }else if(is_wal_){
      open_class=WAL;
    }else{ //unkown
      open_class=L5;
    }

    zbd_->PutOpenIOZoneToken(open_class);
    if (full) {
      zbd_->PutActiveIOZoneToken();
    }
  }
  return s;
}

void ZoneFile::AcquireWRLock() {
  open_for_wr_mtx_.lock();
  open_for_wr_ = true;
}

bool ZoneFile::TryAcquireWRLock() {
  if (!open_for_wr_mtx_.try_lock()) return false;
  open_for_wr_ = true;
  return true;
}

void ZoneFile::ReleaseWRLock() {
  assert(open_for_wr_);
  open_for_wr_ = false;
  open_for_wr_mtx_.unlock();
}

bool ZoneFile::IsOpenForWR() { return open_for_wr_; }

IOStatus ZoneFile::CloseWR() {
  IOStatus s;
  /* Mark up the file as being closed */
  extent_start_ = NO_EXTENT;
  s = PersistMetadata();
  if (!s.ok()) return s;
  ReleaseWRLock();
  return CloseActiveZone();
}

IOStatus ZoneFile::PersistMetadata() {
  assert(metadata_writer_ != NULL);
  return metadata_writer_->Persist(this);
}

ZoneExtent* ZoneFile::GetExtent(uint64_t file_offset, uint64_t* dev_offset) {
  for (unsigned int i = 0; i < extents_.size(); i++) {
    if (file_offset < extents_[i]->length_) {
      *dev_offset = extents_[i]->start_ + file_offset;

      // extents lock ? 
      return extents_[i];
    } else {
      file_offset -= extents_[i]->length_;
    }
  }
  return NULL;
}


IOStatus ZoneFile::PositionedRead(uint64_t offset, size_t n,const IOOptions& ioptions, Slice* result,
                                  char* scratch, bool direct) {
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_READ_LATENCY,
                                 Env::Default());
  zbd_->GetMetrics()->ReportQPS(ZENFS_READ_QPS, 1);
  (void)(ioptions);
  ReadLock lck(this);

  char* ptr;
  uint64_t r_off;
  size_t r_sz;
  ssize_t r = 0;
  size_t read = 0;
  ZoneExtent* extent;
  uint64_t extent_end;
  size_t pread_sz;
  bool aligned;
  IOStatus s;

  if (offset >= file_size_) {
    *result = Slice(scratch, 0);
    return IOStatus::OK();
  }

  r_off = 0;
  
  // while(zbd_->GetZCRunning());

  // TO CHECK WHICH zone
  GetExtent(offset, &r_off);
  // READLOCK HERE
  ZonedBlockDevice::ZoneReadLock zone_read_lock;



  // first, find the zone extents is located
  Zone* z = zbd_->GetIOZone(r_off);
  // then, lock the zone
  if(z){
    zone_read_lock.ReadLockZone(z);
  }
  // retreive r_offset again
  extent = GetExtent(offset, &r_off);

  if (!extent) {
    *result = Slice(scratch, 0);
    return s;
  }


  extent_end = extent->start_ + extent->length_;
  
  /* Limit read size to end of file */
  if ((offset + n) > file_size_)
    r_sz = file_size_ - offset;
  else
    r_sz = n;

  ptr = scratch;

  while (read != r_sz) {
    pread_sz = r_sz - read;

    if ((pread_sz + r_off) > extent_end){ 
      pread_sz = extent_end - r_off;
    }

    /* We may get some unaligned direct reads due to non-aligned extent lengths,
     * so increase read request size to be aligned to next blocksize boundary.
     */
    aligned = (pread_sz % zbd_->GetBlockSize() == 0);

    size_t bytes_to_align = 0;
    if (direct && !aligned) {
      bytes_to_align = zbd_->GetBlockSize() - (pread_sz % zbd_->GetBlockSize());
      pread_sz += bytes_to_align;

      aligned = true;
    }
    // if(ioptions.for_compaction){
      extent->last_accessed_ = zenfs_->NowMicros();
    // }
    std::shared_ptr<char> page_cache = (extent->page_cache_);
    if(page_cache!=nullptr){
    // if(page_cache!=nullptr && ioptions.for_compaction){
      // if(r_off<extent->start_){
        
      // }
      // char* debug_buffer;
      // if(posix_memalign((void**)(&debug_buffer),sysconf(_SC_PAGE_SIZE),256<<20)){
      //   printf("@@@@@@@@@@ debug buffer error\n");
      // }


      
      // printf("memcopy to debug buffer %p<- page_cache.get() + (r_off -extent->start_)  %p:\n",
      // debug_buffer
      // ,page_cache.get() + (r_off -extent->start_) );
      // memcpy(debug_buffer,page_cache.get() + (r_off -extent->start_),pread_sz);

      // printf("memcopy to ptr %p <- debug buffer %p\n",ptr,debug_buffer);
      //  memcpy(ptr,debug_buffer,pread_sz);

      // free(debug_buffer);

      // printf("Positionread ?? r_off %lu extent->start_ %lu extent->length_ %lu pread_sz %lu ptr %p pcptr %p offset %lu n %lu\n",
      // r_off,extent->start_,extent->length_,pread_sz,ptr,page_cache.get(),offset,n);
      memcpy(ptr,page_cache.get() + (r_off -extent->start_) ,pread_sz > extent->length_ ? extent->length_ : pread_sz);
      // printf("Positionread ?? r_off %lu extent->start_ %lu extent->length_ %lu pread_sz %lu ptr %p pcptr %p OKOKOK\n",
      // r_off,extent->start_,extent->length_,pread_sz,ptr,page_cache.get());

      // extent->page_cache_=std::move(page_cache);
      r=pread_sz;
      zbd_->rocksdb_page_cache_hit_size_+=r;
    }else{



      
      // char stopwatch_buf[50];
      // switch (z->state_)
      // {
      // case Zone::State::FINISH: // full
      //   sprintf((char*)stopwatch_buf, "Full&FinishedZone");
      //   break;
      // case Zone::State::OPEN:
      //   sprintf((char*)stopwatch_buf, "OpenZone");
      //   break;
      // case Zone::State::CLOSE:
      //   sprintf((char*)stopwatch_buf, "ClosedZone");
      //   break;
      // default:
      //   sprintf((char*)stopwatch_buf, "Unknown state ? %d",z->state_);
      //   break;
      // }

      // ZenFSStopWatch z1((const char*)stopwatch_buf,zbd_);

      // if(zbd_->PCAEnabled() && z->state_==Zone::State::FINISH){
      //   uint64_t align = extent->length_ % 4096;
      //   uint64_t aligned_extent_length = extent->length_;
      //   if(align){
      //     aligned_extent_length+= 4096-align;
      //   }
      //   char* debug_buffer=nullptr;
      //   if(posix_memalign((void**)(&debug_buffer),sysconf(_SC_PAGE_SIZE),aligned_extent_length)){
      //     printf("@@@@@@@@@@ debug buffer error\n");
      //   }
        
      //   zbd_->Read(debug_buffer,extent->start_,aligned_extent_length,true);

      //   memmove(ptr, debug_buffer+ (r_off -extent->start_) ,pread_sz > extent->length_ ? extent->length_ : pread_sz);
      //   extent->page_cache_.reset(debug_buffer);
      //   zbd_->page_cache_size_+=extent->length_;
      // }else{
      //   r = zbd_->Read(ptr, r_off, pread_sz, (direct && aligned));
      // }
      r = zbd_->Read(ptr, r_off, pread_sz, (direct && aligned));
      // 
      // if(ioptions.for_compaction){
      
      
      r=pread_sz;
      zbd_->rocksdb_page_cache_fault_size_+=r;
      // }

      
    }
    

    if (r <= 0) break;

    /* Verify and update the the bytes read count (if read size was incremented,
     * for alignment purposes).
     */
    if ((size_t)r <= pread_sz - bytes_to_align){
      // uint64_t bf_pread= pread_sz;
      pread_sz = (size_t)r;
      // if(pread_sz>(1<<30)){
      //   printf("ERROR to large pread  2: %lu %lu\n",bf_pread,bytes_to_align);
      // }
    }else{
      // uint64_t bf_pread= pread_sz;
      pread_sz -= bytes_to_align;
      // if(pread_sz>(1<<30)){
      //   printf("ERROR to large pread  3: %lu %lu\n",bf_pread,bytes_to_align);
      // }
    }
    ptr += pread_sz;
    read += pread_sz;
    r_off += pread_sz;



    if (read != r_sz && r_off == extent_end) {
      // extent->position_pulled_ = false;
      zone_read_lock.ReadUnLockZone();

      // first, find which zone has extents
      extent = GetExtent(offset + read, &r_off);
      
      if (!extent) {
        /* read beyond end of (synced) file data */
        break;
      }
      // then, read lock the zone
      z = zbd_->GetIOZone(extent->start_);
      if(z){
        zone_read_lock.ReadLockZone(z);
      }
      // retreive r_offset again
      extent = GetExtent(offset + read, &r_off);
      // after read lock, we can retrieve start
      r_off = extent->start_;
      extent_end = extent->start_ + extent->length_;
    }
  }

  if (r < 0) {
    printf("pread error at r_off : %lu / n :%lu / direct :%d / error code %ld @@@@\n",r_off,pread_sz,(direct && aligned),r);
    // sleep(3);
    s = IOStatus::IOError("pread error\n");
    read = 0;
  }
  zone_read_lock.ReadUnLockZone();
  *result = Slice((char*)scratch, read);
  return s;
}

// IOStatus ZoneFile::PositionedRead(uint64_t offset, size_t n, Slice* result,
//                                   char* scratch, bool direct) {
//   ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_READ_LATENCY,
//                                  Env::Default());
//   zbd_->GetMetrics()->ReportQPS(ZENFS_READ_QPS, 1);

//   ReadLock lck(this);

//   char* ptr;
//   uint64_t r_off;
//   size_t r_sz;
//   ssize_t r = 0;
//   size_t read = 0;
//   ZoneExtent* extent;
//   uint64_t extent_end;
//   size_t pread_sz;
//   bool aligned;
//   IOStatus s;

//   if (offset >= file_size_) {
//     *result = Slice(scratch, 0);
//     return IOStatus::OK();
//   }

//   r_off = 0;
  
//   // TO CHECK WHICH zone
//   GetExtent(offset, &r_off);
//   // READLOCK HERE
//   ZonedBlockDevice::ZoneReadLock zone_read_lock;



//   // first, find the zone extents is located
//   Zone* z = zbd_->GetIOZone(r_off);
//   // then, lock the zone
//   if(z){
//     zone_read_lock.ReadLockZone(z);
//   }
//   // retreive r_offset again
//   extent = GetExtent(offset, &r_off);

//   if (!extent) {
//     *result = Slice(scratch, 0);
//     return s;
//   }


//   extent_end = extent->start_ + extent->length_;
  
//   /* Limit read size to end of file */
//   if ((offset + n) > file_size_)
//     r_sz = file_size_ - offset;
//   else
//     r_sz = n;

//   ptr = scratch;

//   while (read != r_sz) {
//     pread_sz = r_sz - read;

//     if ((pread_sz + r_off) > extent_end){ 
//       pread_sz = extent_end - r_off;
//     }

//     /* We may get some unaligned direct reads due to non-aligned extent lengths,
//      * so increase read request size to be aligned to next blocksize boundary.
//      */
//     aligned = (pread_sz % zbd_->GetBlockSize() == 0);

//     size_t bytes_to_align = 0;
//     if (direct && !aligned) {
//       bytes_to_align = zbd_->GetBlockSize() - (pread_sz % zbd_->GetBlockSize());
//       pread_sz += bytes_to_align;

//       aligned = true;
//     }
//     extent->last_accessed_ = zenfs_->NowMicros();
    

//     std::shared_ptr<char> page_cache = (extent->page_cache_);
//     // std::shared_ptr<char> page_cache = std::move(extent->page_cache_);
//     {
//       if(page_cache==nullptr){
//         char* new_page_cache_ptr = nullptr;
//         if(posix_memalign((void**)(&new_page_cache_ptr),sysconf(_SC_PAGE_SIZE),extent->length_)){
//           printf("@@@@@@@@@@ new_page_cache_ptr error\n");
//         }
//         zbd_->Read(new_page_cache_ptr, extent->start_, extent->length_,false);

//         page_cache.reset(new_page_cache_ptr);
        
//       }
//     }
//     if(page_cache!=nullptr){
//       memmove(ptr,page_cache.get() + (r_off -extent->start_) ,pread_sz > extent->length_ ? extent->length_ : pread_sz);
//       // printf("Positionread ?? r_off %lu extent->start_ %lu extent->length_ %lu pread_sz %lu ptr %p pcptr %p OKOKOK\n",
//       // r_off,extent->start_,extent->length_,pread_sz,ptr,page_cache.get());
//       // bool overlaped_read = false;
//       {
//         std::lock_guard<std::mutex> lg(extent->page_cache_lock_);
//         if(extent->page_cache_ == nullptr){
//           extent->page_cache_=std::move(page_cache);
//         }
//       }
//       if(page_cache==nullptr){
//         zbd_->page_cache_size_+=extent->length_;
//       }


//       r=pread_sz;
//       zbd_->rocksdb_page_cache_hit_size_+=r;
//     }
//     else{
      
      
//       r = zbd_->Read(ptr, r_off, pread_sz, (direct && aligned));

//       zbd_->rocksdb_page_cache_fault_size_+=r;
//     }
    

//     if (r <= 0) break;

//     /* Verify and update the the bytes read count (if read size was incremented,
//      * for alignment purposes).
//      */
//     if ((size_t)r <= pread_sz - bytes_to_align){
//       // uint64_t bf_pread= pread_sz;
//       pread_sz = (size_t)r;
//       // if(pread_sz>(1<<30)){
//       //   printf("ERROR to large pread  2: %lu %lu\n",bf_pread,bytes_to_align);
//       // }
//     }else{
//       // uint64_t bf_pread= pread_sz;
//       pread_sz -= bytes_to_align;
//       // if(pread_sz>(1<<30)){
//       //   printf("ERROR to large pread  3: %lu %lu\n",bf_pread,bytes_to_align);
//       // }
//     }
//     ptr += pread_sz;
//     read += pread_sz;
//     r_off += pread_sz;



//     if (read != r_sz && r_off == extent_end) {
//       // extent->position_pulled_ = false;
//       zone_read_lock.ReadUnLockZone();

//       // first, find which zone has extents
//       extent = GetExtent(offset + read, &r_off);
      
//       if (!extent) {
//         /* read beyond end of (synced) file data */
//         break;
//       }
//       // then, read lock the zone
//       z = zbd_->GetIOZone(extent->start_);
//       if(z){
//         zone_read_lock.ReadLockZone(z);
//       }
//       // retreive r_offset again
//       extent = GetExtent(offset + read, &r_off);
//       // after read lock, we can retrieve start
//       r_off = extent->start_;
//       extent_end = extent->start_ + extent->length_;
//     }
//   }

//   if (r < 0) {
//     printf("pread error at r_off : %lu / n :%lu / direct :%d / error code %ld @@@@\n",r_off,pread_sz,(direct && aligned),r);
//     // sleep(3);
//     s = IOStatus::IOError("pread error\n");
//     read = 0;
//   }
//   zone_read_lock.ReadUnLockZone();
//   *result = Slice((char*)scratch, read);
//   return s;
// }

void ZoneFile::PushExtent() {
  uint64_t length;

  assert(file_size_ >= extent_filepos_);

  if (!active_zone_) return;

  length = file_size_ - extent_filepos_;
  if (length == 0) return;

  assert(length <= (active_zone_->wp_ - extent_start_));
  std::string filename;
  if(linkfiles_.size()){
    filename=linkfiles_[0];
  }else{
    filename="NONE(PushExtent)";
  }
  // printf("@@@ PushExtent %lu %lu\n",extent_start_,length);
  // sst extents, zone extents
  extents_.push_back(new ZoneExtent(extent_start_, length, active_zone_,filename,zenfs_->NowMicros(),this ));

  active_zone_->used_capacity_ += length;
  extent_start_ = active_zone_->wp_;
  extent_filepos_ = file_size_;
}

IOStatus ZoneFile::AllocateNewZone(uint64_t min_capacity) {
  Zone* zone;
  // struct timespec start_timespec, end_timespec;
  // if(zbd_->GetZCRunning() ){
  //     clock_gettime(CLOCK_MONOTONIC, &start_timespec);
  //     while(zbd_->GetZCRunning() );
  //     clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  //     long elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  //     // goto loop;
  //     zbd_->AddCumulativeIOBlocking(elapsed_ns_timespec);
  // }

  int try_n= 0;
  IOStatus s = zbd_->AllocateIOZone(linkfiles_[0],IsSST(),smallest_,largest_,level_,lifetime_, io_type_,input_fno_,predicted_size_ ,&zone,min_capacity);
  // assert(IOStatus::NoSpace("Not enough capacity for append")==IOStatus::NosSpace());
  //  no_input_fno_(0);
  input_fno_.clear();
  if(zone==nullptr){
    // clock_t start=clock();
    int start=zenfs_->GetMountTime();
    while(zbd_->CalculateCapacityRemain()> (1<<20)*128 ){
      // zenfs_->ZoneCleaning();
      // if(reclaimed_zone_n==0){
      //   break;
      // }
      // if(zbd_->IsZoneAllocationFailed()){
      //   return IOStatus::NoSpace("Zone allocation failed\n");
      // }
      // zenfs_->ZCLock();

      // sleep(1);

      s = zbd_->AllocateIOZone(linkfiles_[0],IsSST(),smallest_,largest_,level_,lifetime_, io_type_,input_fno_,predicted_size_,&zone,min_capacity);
      // zenfs_->ZCUnLock();
      try_n++;
      // usleep(1000 * 1000);

      if(zone!=nullptr){
        break;
      }
      if(try_n>100&& zbd_->CalculateFreePercent()<25){
        zbd_->MoveResources(false);
      }
      zbd_->RuntimeReset();
    }
    int end=zenfs_->GetMountTime();
    // clock_t end=clock();
    zbd_->AddIOBlockedTimeLapse(start,end);
  }
  if(!s.ok()){
    return s;
  }

  if(!zone){
    printf("Error :: zone allocation fail\n");
    zbd_->SetZoneAllocationFailed();
    return IOStatus::NoSpace("Zone allocation failure :: AllocateNewZone\n");
  }
  SetActiveZone(zone);
  extent_start_ = active_zone_->wp_;
  extent_filepos_ = file_size_;

  /* Persist metadata so we can recover the active extent using
     the zone write pointer in case there is a crash before syncing */
  return PersistMetadata();
}

/* Byte-aligned writes without a sparse header */
IOStatus ZoneFile::BufferedAppend(char** _buffer, uint64_t data_size) {
  uint64_t left = data_size;
  uint64_t wr_size;
  uint64_t block_sz = GetBlockSize();
  IOStatus s;
  
  // printf("@@ Bufferedappend :%u\n",data_size);
  // while(zbd_->GetZCRunning());
  if (active_zone_ == NULL) {
    s = AllocateNewZone();
    if (!s.ok()) return s;
  }
  std::string filename;
  if(linkfiles_.size()){
    filename=linkfiles_[0];
  }else{
    filename="NONE(BufferedAppend)";
  }
  while (left) {
    char* buffer = (*_buffer);
    wr_size = left;
    if (wr_size > active_zone_->capacity_) wr_size = active_zone_->capacity_;

    /* Pad to the next block boundary if needed */
    uint64_t align = wr_size % block_sz;
    uint64_t pad_sz = 0;

    if (align) pad_sz = block_sz - align;

    /* the buffer size s aligned on block size, so this is ok*/
    if (pad_sz) memset(buffer + wr_size, 0x0, pad_sz);

    uint64_t extent_length = wr_size;

    s = active_zone_->Append(buffer, wr_size + pad_sz);
    if (!s.ok()) return s;
    // printf("ZoneFile::BufferedAppend :: %lu %lu\n",extent_start_, extent_length);
    ZoneExtent* new_ext= new ZoneExtent(extent_start_, extent_length, active_zone_,filename, 
       zenfs_->NowMicros(),this);
    // zenfs_
    // zenfs_
    extents_.push_back(new_ext);

    // int ret =
    //       posix_memalign((void**)_buffer, sysconf(_SC_PAGESIZE), buffer_size_);

    // if (ret){ 
    //   printf("ZoneFile::BufferedAppend memory allocate failed\n");
    //   (*_buffer) = nullptr;
    // }


    extent_start_ = active_zone_->wp_;
    active_zone_->used_capacity_ += extent_length;
    file_size_ += extent_length;
    left -= extent_length;

    if (active_zone_->capacity_ == 0) {
      s = CloseActiveZone();
      if (!s.ok()) {
        return s;
      }
      if (left) {
        // memmove((void*)(buffer), (void*)(buffer + wr_size), left);
        memmove((void*)(*_buffer),(void*)(buffer+wr_size),left);
      }
      // while(zbd_->GetZCRunning());
      s = AllocateNewZone();
      if (!s.ok()) return s;
    }
    
    // zbd_->page_cache_size_+=extent_length;
    // new_ext->page_cache_.reset(buffer);

  }

  return IOStatus::OK();
}

/* Byte-aligned, sparse writes with inline metadata
   the caller reserves 8 bytes of data for a size header */
IOStatus ZoneFile::SparseAppend(char** _sparse_buffer, uint64_t data_size) {
  uint64_t left = data_size;
  uint64_t wr_size;
  uint64_t block_sz = GetBlockSize();
  IOStatus s;
  std::string filename;
  if(linkfiles_.size()){
    filename=linkfiles_[0];
  }else{
    filename="NONE(SparsAppend)";
  }
  if (active_zone_ == NULL) {
    // while(zbd_->GetZCRunning());
    s = AllocateNewZone();
    if (!s.ok()) return s;
  }

  while (left) {
    char* sparse_buffer = (*_sparse_buffer);
    wr_size = left + ZoneFile::SPARSE_HEADER_SIZE;
    if (wr_size > active_zone_->capacity_) wr_size = active_zone_->capacity_;

    /* Pad to the next block boundary if needed */
    uint64_t align = wr_size % block_sz;
    uint64_t pad_sz = 0;

    if (align) pad_sz = block_sz - align;

    /* the sparse buffer has block_sz extra bytes tail allocated for padding, so
     * this is safe */
    if (pad_sz) memset(sparse_buffer + wr_size, 0x0, pad_sz);

    uint64_t extent_length = wr_size - ZoneFile::SPARSE_HEADER_SIZE;
    EncodeFixed64(sparse_buffer, extent_length);

    s = active_zone_->Append(sparse_buffer, wr_size + pad_sz);
    if (!s.ok()) return s;

    ZoneExtent* new_ext = new ZoneExtent(extent_start_ + ZoneFile::SPARSE_HEADER_SIZE,
                       extent_length, active_zone_,filename,ZoneFile::SPARSE_HEADER_SIZE,
                               zenfs_->NowMicros() ,this );
    // zenfs_->db_ptr_;

    extents_.push_back(new_ext);
    // int ret =
    //       posix_memalign((void**)_sparse_buffer, sysconf(_SC_PAGESIZE), buffer_size_);

    // if (ret) {
    //     (*_sparse_buffer) = nullptr;
    //     printf("ZoneFile::SparseAppend memory allocation failed\n");
    // }
    extent_start_ = active_zone_->wp_;
    active_zone_->used_capacity_ += extent_length;
    file_size_ += extent_length;
    left -= extent_length;

    if (active_zone_->capacity_ == 0) {
      s = CloseActiveZone();
      if (!s.ok()) {
        return s;
      }
      if (left) {
        // memmove((void*)(sparse_buffer + ZoneFile::SPARSE_HEADER_SIZE),
        //         (void*)(sparse_buffer + wr_size), left);
        memmove((void*)( (*_sparse_buffer) + ZoneFile::SPARSE_HEADER_SIZE),
                (void*)(sparse_buffer+wr_size),left );
      }
      // while(zbd_->GetZCRunning());
      s = AllocateNewZone();
      if (!s.ok()) return s;
    }
    // zbd_->page_cache_size_+=extent_length;
    // new_ext->page_cache_.reset(sparse_buffer);

  }

  return IOStatus::OK();
}


IOStatus ZoneFile::CAZAAppend(const char* data, uint32_t size,bool positioned,uint64_t offset){
  IOStatus s=IOStatus::OK();
  // printf("@@@ CAZASstBufferedAppend called : %ld %u\n",fno_,size);
  char* buf;
  if(!IsSST()){
    return IOStatus::IOError("CAZASstBufferedAppend only apply to sst file");
  }
  if(is_sparse_){
    return IOStatus::IOError("sst file should not be sparse");
  }


  // if(is_sst_ended_){
  //   return IOStatus::IOError("SST file could not Append after file ended");
  // }
  // buf=new char[size];
  int r= posix_memalign((void**)&buf,sysconf(_SC_PAGE_SIZE),size);
  if(r<0){
    printf("posix memalign error here@@@@@@@@@@@2\n");
    return IOStatus::IOError("CAZASstBufferedAppend fail to allocate memory");
  }

  memcpy(buf,data,size);
  SSTBuffer* sst_buffer=new SSTBuffer(buf,size,positioned,offset);

  if(sst_buffer==nullptr){
    return IOStatus::IOError("CAZASstBufferedAppend fail to allocate memory");
  }
  sst_buffers_.push_back(sst_buffer);

  return s;
}


IOStatus ZonedWritableFile::CAZAFlushSST(){
  IOStatus s;


  // if(zoneFile_->IsSSTEnded()){
  //   return IOStatus::IOError("FlushSstAfterEnded only flush once");
  // }
  // if(zoneFile_->GetAllocationScheme()==LIZA){ // no need to flush
  //   return IOStatus::OK();
  // }
  if(!zoneFile_->IsSST()){
    // return IOStatus::IOError("FlushSstAfterEnded only apply to sst file");
    return IOStatus::OK();
    // return;
  }

  zoneFile_->fno_=fno_;
  zoneFile_->GetZbd()->SetSSTFileforZBDNoLock(fno_,zoneFile_.get());

  if(zoneFile_->GetZbd()->GetAllocationScheme()==LIZA){
    return IOStatus::OK();
  }



  // zoneFile_->SetSstEnded();

  // zoneFile_->input_fno_=input_fno_;

  // for(uint64_t fno : input_fno_){
  //   zoneFile_->GetZbd()->DeleteSSTFileforZBDNoLock(fno);
  // }

  std::vector<SSTBuffer*>* sst_buffers=zoneFile_->GetSSTBuffers();
  zoneFile_->predicted_size_=0;
  for(auto it : *sst_buffers){
    zoneFile_->predicted_size_ += it->size_;
  }
  
  // printf("predcited size :\t%lu\n",(zoneFile_->predicted_size_>>20));

  for(auto it : *sst_buffers){
    // if(it->positioned_==true){
    //   if (it->offset_ != wp) {
    //     // assert(false);
    //     return IOStatus::IOError("positioned append not at write pointer 2");
    //   }
    // }
    if(buffered){
      buffer_mtx_.lock();
      s=BufferedWrite(it->content_,it->size_);
      buffer_mtx_.unlock();
      if(!s.ok()){
        return s;
      }
    }else{
      s = zoneFile_->Append(it->content_,it->size_);
      if(!s.ok()){
        return s;
      }  
      wp+=it->size_;
    }
    delete it;
  }
  // printf("%lu %d\n",zoneFile_->predicted_size_,IS_BIG_SSTABLE(zoneFile_->predicted_size_));
  // if(zoneFile_->level_ ==0){
  //   zoneFile_->GetZbd()->lsm_tree_[0].fetch_add(1);
  // }else{
  zoneFile_->GetZbd()->lsm_tree_[zoneFile_->level_].fetch_add(zoneFile_->predicted_size_);
  // }
  input_fno_.clear();
  return s;
}

/* Assumes that data and size are block aligned */
IOStatus ZoneFile::Append(void* data, uint64_t data_size) {
  uint64_t left = data_size;
  uint64_t wr_size, offset = 0;
  IOStatus s = IOStatus::OK();
  // while(zbd_->GetZCRunning());
  if (!active_zone_) {
    s = AllocateNewZone();
    if (!s.ok()) return s;
  }

  while (left) {

    if (active_zone_->capacity_ == 0) {
      PushExtent();

      s = CloseActiveZone();
      if (!s.ok()) {
        return s;
      }
      // while(zbd_->GetZCRunning());
      s = AllocateNewZone();
      if (!s.ok()) return s;
    }

    wr_size = left;
    if (wr_size > active_zone_->capacity_) wr_size = active_zone_->capacity_;

    s = active_zone_->Append((char*)data + offset, wr_size);
    if (!s.ok()) return s;

    file_size_ += wr_size;
    left -= wr_size;
    offset += wr_size;
  }

  return IOStatus::OK();
}

IOStatus ZoneFile::RecoverSparseExtents(uint64_t start, uint64_t end,
                                        Zone* zone) {
  /* Sparse writes, we need to recover each individual segment */
  IOStatus s;
  uint64_t block_sz = GetBlockSize();
  uint64_t next_extent_start = start;
  char* buffer;
  int recovered_segments = 0;
  int ret;
  std::string filename;
  if(linkfiles_.size()){
    filename=linkfiles_[0];
  }else{
    filename="NONE(RecoverSparseExtents)";
  }
  ret = posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), block_sz);
  if (ret) {
    return IOStatus::IOError("Out of memory while recovering");
  }

  while (next_extent_start < end) {
    uint64_t extent_length;

    ret = zbd_->Read(buffer, next_extent_start, block_sz, false);
    if (ret != (int)block_sz) {
      s = IOStatus::IOError("Unexpected read error while recovering");
      break;
    }

    extent_length = DecodeFixed64(buffer);
    if (extent_length == 0) {
      s = IOStatus::IOError("Unexpected extent length while recovering");
      break;
    }
    recovered_segments++;
    
    zone->used_capacity_ += extent_length;
    // printf("ZoneFile::RecoverSparseExtents %lu %lu\n",next_extent_start + SPARSE_HEADER_SIZE,extent_length);
    extents_.push_back(new ZoneExtent(next_extent_start + SPARSE_HEADER_SIZE,
                                      extent_length, zone,filename,SPARSE_HEADER_SIZE,0,this) );

    uint64_t extent_blocks = (extent_length + SPARSE_HEADER_SIZE) / block_sz;
    if ((extent_length + SPARSE_HEADER_SIZE) % block_sz) {
      extent_blocks++;
    }
    next_extent_start += extent_blocks * block_sz;
  }

  free(buffer);
  return s;
}

IOStatus ZoneFile::Recover() {
  /* If there is no active extent, the file was either closed gracefully
     or there were no writes prior to a crash. All good.*/
  if (!HasActiveExtent()) return IOStatus::OK();

  /* Figure out which zone we were writing to */
  Zone* zone = zbd_->GetIOZone(extent_start_);

  if (zone == nullptr) {
    return IOStatus::IOError(
        "Could not find zone for extent start while recovering");
  }

  if (zone->wp_ < extent_start_) {
    return IOStatus::IOError("Zone wp is smaller than active extent start");
  }

  /* How much data do we need to recover? */
  uint64_t to_recover = zone->wp_ - extent_start_;

  /* Do we actually have any data to recover? */
  if (to_recover == 0) {
    /* Mark up the file as having no missing extents */
    extent_start_ = NO_EXTENT;
    return IOStatus::OK();
  }
  std::string filename;
  if(linkfiles_.size()){
    filename=linkfiles_[0];
  }else{
    filename="NONE(Recover)";
  }
  /* Is the data sparse or was it writted direct? */
  if (is_sparse_) {
    IOStatus s = RecoverSparseExtents(extent_start_, zone->wp_, zone);
    if (!s.ok()) return s;
  } else {
    /* For non-sparse files, the data is contigous and we can recover directly
       any missing data using the WP */
    zone->used_capacity_ += to_recover;
    // printf("ZoneFile::Recover %lu %lu\n",extent_start_, to_recover);
    extents_.push_back(new ZoneExtent(extent_start_, to_recover, zone,filename,0,this));
  }

  /* Mark up the file as having no missing extents */
  extent_start_ = NO_EXTENT;

  /* Recalculate file size */
  file_size_ = 0;
  for (uint32_t i = 0; i < extents_.size(); i++) {
    file_size_ += extents_[i]->length_;
  }

  return IOStatus::OK();
}

void ZoneFile::ReplaceExtentList(std::vector<ZoneExtent*> new_list) {
  assert(!IsOpenForWR() && new_list.size() > 0);
  assert(new_list.size() == extents_.size());

  WriteLock lck(this);
  extents_ = new_list;
}

void ZoneFile::AddLinkName(const std::string& linkf) {
  linkfiles_.push_back(linkf);
}

IOStatus ZoneFile::RenameLink(const std::string& src, const std::string& dest) {
  auto itr = std::find(linkfiles_.begin(), linkfiles_.end(), src);
  if (itr != linkfiles_.end()) {
    linkfiles_.erase(itr);
    linkfiles_.push_back(dest);
  } else {
    return IOStatus::IOError("RenameLink: Failed to find the linked file");
  }
  return IOStatus::OK();
}

IOStatus ZoneFile::RemoveLinkName(const std::string& linkf) {
  assert(GetNrLinks());
  auto itr = std::find(linkfiles_.begin(), linkfiles_.end(), linkf);
  if (itr != linkfiles_.end()) {
    linkfiles_.erase(itr);
  } else {
    return IOStatus::IOError("RemoveLinkInfo: Failed to find the link file");
  }
  return IOStatus::OK();
}

IOStatus ZoneFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime, int level) {
  // lifetime_ = lifetime;
  (void)(lifetime);
  // if(linkfiles_.size()){
  //   printf("SetWriteLifeTimeHint : %s %d\n",linkfiles_[0].c_str(),lifetime);
  // }
  if(is_wal_){
    lifetime_=Env::WLTH_SHORT;
    return IOStatus::OK();
  }
  switch (level)
  {
  case 0:
    /* fall through */
  case 1:
    lifetime_=Env::WLTH_MEDIUM;
    break;
  case 2:
    lifetime_=Env::WLTH_LONG;
    break;
  case 3:
    lifetime_=Env::WLTH_EXTREME;
    break;
  default:
    lifetime_=Env::WLTH_EXTREME;
    break;
  }
  // printf("%d -> %d\n",level,lifetime_);
  
  return IOStatus::OK();
}

void ZonedWritableFile::SetMinMaxKeyAndLevel(const Slice& s,const Slice& l,const int output_level){
  if(output_level<0){
    printf("@@@ ZonedWritableFile::SetMinMaxAndLEvel :: failed , level should be > 0\n");
    return;
  }
  // printf("set min max : fno :%ld at %d\n",zoneFile_->fno_,output_level);
  
  // if(zoneFile_->GetLinkFiles().size()){
  //   printf("SetMinMaxKeyAndLevel :: %s output level %d\n",zoneFile_->GetLinkFiles()[0].c_str(),output_level);
  // }
  zoneFile_->smallest_=s;
  zoneFile_->largest_=l;
  zoneFile_->level_=output_level;
  return;
}

void ZoneFile::ReleaseActiveZone() {
  active_zone_->Release();

  active_zone_ = nullptr;
}

void ZoneFile::SetActiveZone(Zone* zone) {
  assert(active_zone_ == nullptr);

  active_zone_ = zone;
  zone->state_=Zone::State::OPEN;
}

ZonedWritableFile::ZonedWritableFile(ZonedBlockDevice* zbd, bool _buffered,
                                     std::shared_ptr<ZoneFile> zoneFile) {
  wp = zoneFile->GetFileSize();

  buffered = _buffered;
  block_sz = zbd->GetBlockSize();
  zoneFile_ = zoneFile;
  buffer_pos = 0;
  sparse_buffer = nullptr;
  buffer = nullptr;

  default_buffer_size_ = zbd->GetDefaultExtentSize();

  if (buffered) {
    if (zoneFile->IsSparse()) {
      size_t sparse_buffer_sz;

      sparse_buffer_sz =
          default_buffer_size_ + block_sz; /* one extra block size for padding */
      int ret = posix_memalign((void**)&sparse_buffer, sysconf(_SC_PAGESIZE),
                               sparse_buffer_sz);

      if (ret) sparse_buffer = nullptr;

      assert(sparse_buffer != nullptr);
      
      buffer_sz = sparse_buffer_sz - ZoneFile::SPARSE_HEADER_SIZE - block_sz;
      buffer = sparse_buffer + ZoneFile::SPARSE_HEADER_SIZE;
      zoneFile->buffer_size_=buffer_sz;
    } else {
      buffer_sz = default_buffer_size_;
      int ret =
          posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), buffer_sz);
      zoneFile->buffer_size_=buffer_sz;
      if (ret) buffer = nullptr;
      assert(buffer != nullptr);
    }
  }

  open = true;
}

ZonedWritableFile::~ZonedWritableFile() {
  IOStatus s = CloseInternal();
  if (buffered) {
    if (sparse_buffer != nullptr) {
      free(sparse_buffer);
    } else {
      free(buffer);
    }
  }

  if (!s.ok()) {
    zoneFile_->GetZbd()->SetZoneDeferredStatus(s);
  }
}

MetadataWriter::~MetadataWriter() {}

IOStatus ZonedWritableFile::Truncate(uint64_t size,
                                     const IOOptions& /*options*/,
                                     IODebugContext* /*dbg*/) {
  zoneFile_->SetFileSize(size);
  return IOStatus::OK();
}

IOStatus ZonedWritableFile::DataSync() {

  if (buffered) {
    IOStatus s;
    buffer_mtx_.lock();
    /* Flushing the buffer will result in a new extent added to the list*/
    s = FlushBuffer();
    buffer_mtx_.unlock();
    if (!s.ok()) {
      return s;
    }

    /* We need to persist the new extent, if the file is not sparse,
     * as we can't use the active zone WP, which is block-aligned, to recover
     * the file size */
    if (!zoneFile_->IsSparse()) return zoneFile_->PersistMetadata();
  } else {
    /* For direct writes, there is no buffer to flush, we just need to push
       an extent for the latest written data */
    // printf("ZonedWritableFile::DataSync -> PushExtent\n");
    zoneFile_->PushExtent();
  }

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Fsync(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  IOStatus s;
  ZenFSMetricsLatencyGuard guard(zoneFile_->GetZBDMetrics(),
                                 zoneFile_->GetIOType() == IOType::kWAL
                                     ? ZENFS_WAL_SYNC_LATENCY
                                     : ZENFS_NON_WAL_SYNC_LATENCY,
                                 Env::Default());
  zoneFile_->GetZBDMetrics()->ReportQPS(ZENFS_SYNC_QPS, 1);

  s = DataSync();
  if (!s.ok()) return s;

  /* As we've already synced the metadata in DataSync, no need to do it again */
  if (buffered && !zoneFile_->IsSparse()) return IOStatus::OK();

  return zoneFile_->PersistMetadata();
}

IOStatus ZonedWritableFile::Sync(const IOOptions& /*options*/,
                                 IODebugContext* /*dbg*/) {
  return DataSync();
}

IOStatus ZonedWritableFile::Flush(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus ZonedWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                      const IOOptions& /*options*/,
                                      IODebugContext* /*dbg*/) {
  if (wp < offset + nbytes) return DataSync();

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Close(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  return CloseInternal();
}

IOStatus ZonedWritableFile::CloseInternal() {
  if (!open) {
    return IOStatus::OK();
  }

  IOStatus s = DataSync();
  if (!s.ok()) return s;

  s = zoneFile_->CloseWR();
  if (!s.ok()) return s;

  open = false;
  return s;
}

IOStatus ZonedWritableFile::FlushBuffer() {
  IOStatus s;

  if (buffer_pos == 0) return IOStatus::OK();
  
  if (zoneFile_->IsSparse()) {
    s = zoneFile_->SparseAppend(&sparse_buffer, buffer_pos);
  } else {
    s = zoneFile_->BufferedAppend(&buffer, buffer_pos);
  }

  if (!s.ok()) {
    return s;
  }

  wp += buffer_pos;
  buffer_pos = 0;

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::BufferedWrite(char* data, uint32_t data_left) {
  // uint32_t data_left = slice.size();
  IOStatus s;

  while (data_left) {
    uint32_t buffer_left = buffer_sz - buffer_pos;
    uint32_t to_buffer;

    if (!buffer_left) {
      s = FlushBuffer();
      if (!s.ok()) return s;
      buffer_left = buffer_sz;
    }

    to_buffer = data_left;
    if (to_buffer > buffer_left) {
      to_buffer = buffer_left;
    }

    memcpy(buffer + buffer_pos, data, to_buffer);
    buffer_pos += to_buffer;
    data_left -= to_buffer;
    data += to_buffer;
  }

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::BufferedWrite(const Slice& slice) {
  uint64_t data_left = slice.size();
  char* data = (char*)slice.data();
  IOStatus s;

  while (data_left) {
    uint64_t buffer_left = buffer_sz - buffer_pos;
    uint64_t to_buffer;

    if (!buffer_left) {
      s = FlushBuffer();
      if (!s.ok()) return s;
      buffer_left = buffer_sz;
    }

    to_buffer = data_left;
    if (to_buffer > buffer_left) {
      to_buffer = buffer_left;
    }

    memcpy(buffer + buffer_pos, data, to_buffer);
    buffer_pos += to_buffer;
    data_left -= to_buffer;
    data += to_buffer;
  }

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Append(const Slice& data,
                                   const IOOptions& /*options*/,
                                   IODebugContext* /*dbg*/) {
  IOStatus s;
  // ZenFSMetricsLatencyGuard guard(zoneFile_->GetZBDMetrics(),
  //                                zoneFile_->GetIOType() == IOType::kWAL
  //                                    ? ZENFS_WAL_WRITE_LATENCY
  //                                    : ZENFS_NON_WAL_WRITE_LATENCY,
  //                                Env::Default());
  zoneFile_->GetZBDMetrics()->ReportQPS(ZENFS_WRITE_QPS, 1);
  zoneFile_->GetZBDMetrics()->ReportThroughput(ZENFS_WRITE_THROUGHPUT,
                                               data.size());

  // if(zoneFile_->is_wal_ == false && zoneFile_->is_sst_ == false){
  //   return IOStatus::OK();
  // }
  if(zoneFile_->IsSST()&&zoneFile_->GetAllocationScheme()!=LIZA){
    // if(fno_set_==false){
    //   return IOStatus::IOError("PositionedAppend to SST should set fno before append");
    // }
    return zoneFile_->CAZAAppend(data.data(),data.size(),true,0);
  }



  if (buffered) {
    buffer_mtx_.lock();
    s = BufferedWrite(data);
    buffer_mtx_.unlock();
  } else {
    s = zoneFile_->Append((void*)data.data(), data.size());
    if (s.ok()) wp += data.size();
  }

  return s;
}

IOStatus ZonedWritableFile::PositionedAppend(const Slice& data, uint64_t offset,
                                             const IOOptions& /*options*/,
                                             IODebugContext* /*dbg*/) {
  IOStatus s;
  ZenFSMetricsLatencyGuard guard(zoneFile_->GetZBDMetrics(),
                                 zoneFile_->GetIOType() == IOType::kWAL
                                     ? ZENFS_WAL_WRITE_LATENCY
                                     : ZENFS_NON_WAL_WRITE_LATENCY,
                                 Env::Default());
  zoneFile_->GetZBDMetrics()->ReportQPS(ZENFS_WRITE_QPS, 1);
  zoneFile_->GetZBDMetrics()->ReportThroughput(ZENFS_WRITE_THROUGHPUT,
                                               data.size());
// /rocksdbtest/dbbench/OPTIONS-000007

    // if(zoneFile_->is_wal_ == false && zoneFile_->is_sst_ == false){
    //   return IOStatus::OK();
    // }

  // if(ends_with(zoneFile_->GetFilename().c_str(),".log")){

  // }

  if(zoneFile_->IsSST()&&zoneFile_->GetAllocationScheme()!=LIZA){
    // if(fno_set_==false){
    //   return IOStatus::IOError("PositionedAppend to SST should set fno before append");
    // }
    s = zoneFile_->CAZAAppend(data.data(),data.size(),true,offset);
    // if (s.ok()) wp += data.size();
    return s;
  }

  if (offset != wp) {
    assert(false);
    return IOStatus::IOError("positioned append not at write pointer");
  }

  if (buffered) {
    buffer_mtx_.lock();
    s = BufferedWrite(data);
    buffer_mtx_.unlock();
  } else {
    s = zoneFile_->Append((void*)data.data(), data.size());
    if (s.ok()) wp += data.size();
  }

  return s;
}

void ZonedWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
  if(zoneFile_->is_sst_){
    zoneFile_->fno_=fno_;
    zoneFile_->input_fno_=input_fno_;
    zoneFile_->GetZbd()->SetSSTFileforZBDNoLock(fno_,zoneFile_.get());
    zoneFile_->level_=level_;
  }
  zoneFile_->SetWriteLifeTimeHint(hint,level_);
}

IOStatus ZonedSequentialFile::Read(size_t n, const IOOptions& ioptions,
                                   Slice* result, char* scratch,
                                   IODebugContext* /*dbg*/) {
  IOStatus s;

  s = zoneFile_->PositionedRead(rp, n,ioptions ,result, scratch, direct_);
  if (s.ok()) rp += result->size();

  return s;
}

IOStatus ZonedSequentialFile::Skip(uint64_t n) {
  if (rp + n >= zoneFile_->GetFileSize())
    return IOStatus::InvalidArgument("Skip beyond end of file");
  rp += n;
  return IOStatus::OK();
}

IOStatus ZonedSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                             const IOOptions& ioptions,
                                             Slice* result, char* scratch,
                                             IODebugContext* /*dbg*/) {
  return zoneFile_->PositionedRead(offset, n,ioptions ,result, scratch, direct_);
}

IOStatus ZonedRandomAccessFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& ioptions,
                                     Slice* result, char* scratch,
                                     IODebugContext* /*dbg*/) const {
  return zoneFile_->PositionedRead(offset, n,ioptions ,result, scratch, direct_);
}




IOStatus ZoneFile::MigrateData(uint64_t offset, uint64_t length,
                               Zone* target_zone, std::shared_ptr<char> page_cache) {
  IOStatus s;
  uint64_t step = length;
  uint64_t block_sz = zbd_->GetBlockSize();
  uint64_t align = step % block_sz;
  
  if(align){
    step+= (block_sz-align);
  }

  uint64_t read_sz = step;
  // uint64_t zone_size=zbd_->GetZoneSize();
  // uint64_t from_zone = (offset/zone_size)*zone_size;

  // uint64_t read_sz;
  // if(offset+length>(from_zone+zone_size)){
  //   step = (1<<12);
  //   read_sz=(1<<12);
  // }



  assert(offset % block_sz == 0);
  if (offset % block_sz != 0) {
    return IOStatus::IOError("MigrateData offset is not aligned!\n");
  }
  
  char* buf;
  int ret = posix_memalign((void**)&buf, block_sz, step);
  if (ret) {
    return IOStatus::IOError("failed allocating alignment write buffer\n");
  }

  int pad_sz = 0;
  while (length > 0) {
    read_sz = length > read_sz ? read_sz : length;
    // read_sz = 1048576;
    pad_sz = read_sz % block_sz == 0 ? 0 : (block_sz - (read_sz % block_sz));


    int r = 0;
    {
        ZenFSStopWatch z1("READ",zbd_);
        if(page_cache==nullptr ){
          
          r= zbd_->Read(buf, offset, read_sz + pad_sz, true);

        }else{
          // zbd_->rocksdb_page_cache_hit_size_+=read_sz + pad_sz;
          memmove(buf,page_cache.get(),read_sz + pad_sz);
          r=(read_sz+pad_sz);
        }
    }

    if (r < 0) {
      free(buf);
      return IOStatus::IOError(strerror(errno));
    }
    {
      ZenFSStopWatch z2("WRITE",zbd_);
      target_zone->Append(buf, r,true);
    }
    length -= read_sz;
    offset += r;
  }

  free(buf);

  return IOStatus::OK();
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
