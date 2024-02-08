// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbdlib_zenfs.h"
#include <iostream>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <string>
#include <sys/ioctl.h>
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"

// #define BLKPARTIALRESETZONE	_IOW(0x12, 137, struct blk_zone_range)
// #define BLKDUMMYCMD	_IOW(0x12, 138, struct blk_zone_range)

namespace ROCKSDB_NAMESPACE {

ZbdlibBackend::ZbdlibBackend(std::string bdevname)
    : filename_("/dev/" + bdevname),
      read_f_(-1),
      read_direct_f_(-1),
      write_f_(-1) {}

std::string ZbdlibBackend::ErrorToString(int err) {
  char *err_str = strerror(err);
  if (err_str != nullptr) return std::string(err_str);
  return "";
}

IOStatus ZbdlibBackend::CheckScheduler() {
  std::ostringstream path;
  std::string s = filename_;
  std::fstream f;
  return IOStatus::OK();
  
  s.erase(0, 5);  // Remove "/dev/" from /dev/nvmeXnY
  path << "/sys/block/" << s << "/queue/scheduler";
  f.open(path.str(), std::fstream::in);
  if (!f.is_open()) {
    printf("failed to open\n");
    exit(-2);
    return IOStatus::InvalidArgument("Failed to open " + path.str());
  }

  std::string buf;
  getline(f, buf);
  if (buf.find("[mq-deadline]") == std::string::npos) {
    f.close();
    return IOStatus::InvalidArgument(
        "Current ZBD scheduler is not mq-deadline, set it to mq-deadline.");
  }

  f.close();
  return IOStatus::OK();
}

IOStatus ZbdlibBackend::Open(bool readonly, bool exclusive,
                             unsigned int *max_active_zones,
                             unsigned int *max_open_zones,
                             unsigned int *log2_erase_unit_size) {
  zbd_info info;
  // unsigned int report = 1;
  // struct zbd_zone z;
  (void)(log2_erase_unit_size);
  /* The non-direct file descriptor acts as an exclusive-use semaphore */
  if (exclusive) {
    // read_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_EXCL, &info);
    read_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_EXCL | O_DIRECT, &info);
  } else {
    read_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  }

  if (read_f_ < 0) {
    return IOStatus::InvalidArgument(
        "Failed to open zoned block device for read: " + ErrorToString(errno));
  }

  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_DIRECT, &info);
  if (read_direct_f_ < 0) {
    return IOStatus::InvalidArgument(
        "Failed to open zoned block device for direct read: " +
        ErrorToString(errno));
  }

  if (readonly) {
    write_f_ = -1;
  } else {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT, &info);
    // printf("@@@ write f zbd_open %d %s\n",write_f_,filename_.c_str());
    if (write_f_ < 0) {
      return IOStatus::InvalidArgument(
          "Failed to open zoned block device for write: " +
          ErrorToString(errno));
    }
  }

  if (info.model != ZBD_DM_HOST_MANAGED) {
    return IOStatus::NotSupported("Not a host managed block device");
  }

  IOStatus ios = CheckScheduler();
  if (ios != IOStatus::OK()) return ios;

  block_sz_ = info.pblock_size;
  // if(block_sz_!=4096){
  //   exit(-1);
  // }
  zone_sz_ = info.zone_size;

  nr_zones_ = info.nr_zones;
  *max_active_zones = info.max_nr_active_zones;
  *max_open_zones = info.max_nr_open_zones;
  // printf("MAX active zone : %lu , open zone : %lu\n",*max_active_zones,*max_open_zones);
  // printf("ZbdlibBackend :: Open()\n");
#ifdef BLKPARTIALRESETZONE
  unsigned int tmp;
  void* zones;
  zbd_list_zones(write_f_,0,info.zone_size*info.nr_zones,ZBD_RO_ALL,
		(struct zbd_zone**)&zones,&tmp,log2_erase_unit_size);
#endif  
 printf("ZbdlibBackend :: Open() log erase unit size %u\n",*log2_erase_unit_size);

  return IOStatus::OK();
}

std::unique_ptr<ZoneList> ZbdlibBackend::ListZones() {
  void *zones;
  int ret;
  unsigned int nr_zones;
  #ifdef BLKPARTIALRESETZONE
  ret=zbd_list_zones(write_f_, 0, zone_sz_ * nr_zones_, ZBD_RO_ALL,
                       (struct zbd_zone **)&zones, &nr_zones, &log2_erase_unit_size_);
  #else
  ret=zbd_list_zones(write_f_, 0, zone_sz_ * nr_zones_, ZBD_RO_ALL,
                       (struct zbd_zone **)&zones, &nr_zones);
  #endif
  if (ret) {
    return nullptr;
  }

  std::unique_ptr<ZoneList> zl(new ZoneList(zones, nr_zones));

  return zl;
}

IOStatus ZbdlibBackend::MultiReset(uint64_t start,uint64_t reset_size){
  int ret = zbd_reset_zones(write_f_, start, reset_size);
  if (ret){
    printf("ZbdlibBackend::MultiReset failed\n");
    return IOStatus::IOError("failed\n");
  }
  return IOStatus::OK();
}

IOStatus ZbdlibBackend::Reset(uint64_t start, bool *offline,
                              uint64_t *max_capacity) {
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  ret = zbd_reset_zones(write_f_, start, zone_sz_);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

#ifdef BLKPARTIALRESETZONE
  ret = zbd_report_zones(read_f_, start, zone_sz_, ZBD_RO_ALL, &z, &report,NULL);
#else
  ret = zbd_report_zones(read_f_, start, zone_sz_, ZBD_RO_ALL, &z, &report);
#endif
  if (ret || (report != 1)) return IOStatus::IOError("Zone report failed\n");

  if (zbd_zone_offline(&z)) {
    *offline = true;
    *max_capacity = 0;
  } else {
    *offline = false;
    *max_capacity = zbd_zone_capacity(&z);
  }

  return IOStatus::OK();
}
//start byte,erasesize(bytes) --> zidx,erasesize(bytes)
IOStatus ZbdlibBackend::PartialReset(uint64_t start, uint64_t erase_size, bool clflush){  
  #ifndef BLKPARTIALRESETZONE
    (void)(start);
    (void)(erase_size);
    (void)(clflush);
    printf("ZbdlibBackend::PartialReset  :: Not Supported\n");
    return IOStatus::IOError("Not supported\n");

  #else
  if(erase_size==0){
    return IOStatus::IOError("Erase unit # < 0\n");
  }
  if(clflush==true){
    zbd_circular_partial_reset_clflush(write_f_,start,erase_size);
  }else{
    zbd_circular_partial_reset(write_f_,start,erase_size);
  }

  #endif
  return IOStatus::OK();
}

IOStatus ZbdlibBackend::Finish(uint64_t start) {
  int ret;

  ret = zbd_finish_zones(write_f_, start, zone_sz_);
  if (ret) return IOStatus::IOError("Zone finish failed\n");

  return IOStatus::OK();
}

IOStatus ZbdlibBackend::Close(uint64_t start) {
  int ret;

  ret = zbd_close_zones(write_f_, start, zone_sz_);
  if (ret) return IOStatus::IOError("Zone close failed\n");

  return IOStatus::OK();
}

int ZbdlibBackend::Read(char *buf, int size, uint64_t pos, bool direct) {
  return pread(direct ? read_direct_f_ : read_f_, buf, size, pos);
}

int ZbdlibBackend::Write(char *data, uint64_t size, uint64_t pos) {
  return pwrite(write_f_, data, size, pos);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
