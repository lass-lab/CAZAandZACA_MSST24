// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "io_zenfs.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

// Indicate what stats info we want.
struct ZoneExtentSnapshot;
struct ZenFSSnapshotOptions {
  // Global zoned device stats info
  bool zbd_ = 0;
  // Per zone stats info
  bool zone_ = 0;
  // Get all file->extents & extent->file mappings
  bool zone_file_ = 0;
  bool trigger_report_ = 0;
  bool log_garbage_ = 0;
  bool as_lock_free_as_possible_ = 1;
};

class ZBDSnapshot {
 public:
  uint64_t free_space;
  uint64_t used_space;
  uint64_t reclaimable_space;

 public:
  ZBDSnapshot() = default;
  ZBDSnapshot(const ZBDSnapshot&) = default;
  ZBDSnapshot(ZonedBlockDevice& zbd)
      : free_space(zbd.GetFreeSpace()),
        used_space(zbd.GetUsedSpace()),
        reclaimable_space(zbd.GetReclaimableSpace()) {}
};

class ZoneSnapshot {
 public:
  uint64_t start;
  uint64_t wp;

  uint64_t capacity;
  uint64_t used_capacity;
  uint64_t max_capacity;
  uint64_t zidx;
  uint64_t is_finished;
  Zone* zone_p;
  std::vector<ZoneExtentSnapshot*> extents_in_zone;
  bool lock_held = false;
 public:
  ZoneSnapshot(Zone* zone)
      : start(zone->start_),
        wp(zone->wp_),
        capacity(zone->capacity_),
        used_capacity(zone->used_capacity_),
        max_capacity(zone->max_capacity_),
        zidx(zone->zidx_),
        is_finished(zone->is_finished_),
        zone_p(zone) {
          extents_in_zone.clear();
        }
};

class ZoneExtentSnapshot {
 public:
  uint64_t start;
  uint64_t length;
  uint64_t zone_start;
  uint64_t header_size;
  std::string filename;
  Zone* zone_p;
  std::shared_ptr<char> page_cache = nullptr;
  // ZoneFile* zfile_p;

 public:
  ZoneExtentSnapshot() {}
  ZoneExtentSnapshot(const ZoneExtent* extent, const std::string fname)
       {
          // printf("error at here222\n");
          if(extent==nullptr){
            printf("ZoneExtentSnapshot nullptr error\n");
            return;
          }
          start=extent->start_;
          length=extent->length_;
          header_size=extent->header_size_;
          Zone* z = extent->zone_;
          zone_start=z->start_;
          zone_p=z;
          filename=fname;
        }
  static bool SortByLBA(ZoneExtentSnapshot* za,ZoneExtentSnapshot* zb){
    return za->start < zb->start;
  }
    // ZoneExtentSnapshot(const ZoneExtent& extent, const std::string fname,bool debug)
    //   : start(extent.start_),
    //     length(extent.length_),
    //     zone_start(extent.zone_->start_),
    //     filename(fname) {
    //       if(debug){
    //         (void)(debug);
    //       }
    //     }
  ~ZoneExtentSnapshot(){
    page_cache.reset();
  }
};

class ZoneFileSnapshot {
 public:
  uint64_t file_id;
  std::string filename;
  std::vector<ZoneExtentSnapshot> extents;

 public:
  ZoneFileSnapshot(ZoneFile& file)
      : file_id(file.GetID()), filename(file.GetFilename()) {
    for (ZoneExtent* extent : file.GetExtents()) {
      extents.emplace_back(extent, filename);
    }
  }
};

class ZenFSSnapshot {
 public:
  ZenFSSnapshot() {}

  ZenFSSnapshot& operator=(ZenFSSnapshot&& snapshot) {
    zbd_ = snapshot.zbd_;
    zones_ = std::move(snapshot.zones_);
    zone_files_ = std::move(snapshot.zone_files_);
    extents_ = std::move(snapshot.extents_);
    return *this;
  }

 public:
  ZBDSnapshot zbd_;
  std::vector<ZoneSnapshot> zones_;
  std::vector<ZoneFileSnapshot> zone_files_;
  std::vector<ZoneExtentSnapshot> extents_;
};

}  // namespace ROCKSDB_NAMESPACE
