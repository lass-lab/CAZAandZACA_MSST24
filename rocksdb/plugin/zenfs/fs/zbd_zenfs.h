// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <deque>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <set>
#include <sys/types.h>
#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/comparator.h"
#include <libaio.h>

namespace ROCKSDB_NAMESPACE {

class ZonedBlockDevice;
class ZonedBlockDeviceBackend;
class ZoneSnapshot;
class ZenFSSnapshotOptions;
class Zone;
class ZoneFile;

// uint64_t ZONE_CLEANING_KICKING_POINT=20;

// #define ZONE_CLEANING_KICKING_POINT (40)
#define READ_FD 0
#define READ_DIRECT_FD 1
#define WRITE_DIRECT_FD 2

#define KB (1024)

#define MB (1024 * KB)

#define ZENFS_SPARE_ZONES (0)

#define ZENFS_META_ZONES (3)

#define log2_DEVICE_IO_CAPACITY (6) //64GB


// #define ZENFS_IO_ZONES (40) // 20GB

// #define ZONE_SIZE 512


// #define DEVICE_SIZE ((ZENFS_IO_ZONES)*(ZONE_SIZE))

// #define ZONE_SIZE_PER_DEVICE_SIZE (100/(ZENFS_IO_ZONES))

#define WP_TO_RELATIVE_WP(wp,zone_sz,zidx) ((wp)-(zone_sz*zidx))

#define BYTES_TO_MB(bytes) (bytes>>20)

#define PASS 0
#define SPINLOCK 1 

#define LIZA 0
#define CAZA 1
#define CAZA_ADV 2

#define PARTIAL_RESET_KICKED_THRESHOLD 40
                                                      // | zone-reset | partial-reset |
#define RUNTIME_ZONE_RESET_DISABLED 0    	           	// |      x     |       x       |
#define RUNTIME_ZONE_RESET_ONLY 1                 		// |      o     |       x       |
#define PARTIAL_RESET_WITH_ZONE_RESET 2            		// |      o     |       o       |
#define PARTIAL_RESET_ONLY 3             	          	// |      x     |       o       |
#define PARTIAL_RESET_AT_BACKGROUND 4    		          // |      ?     |       o       |
#define PARTIAL_RESET_BACKGROUND_T_WITH_ZONE_RESET 5	// |      o     |       o       |
#define PROACTIVE_ZONECLEANING 6                      // |      x     |       x       |


#define IS_BIG_SSTABLE(file_size) (bool)((uint64_t)(file_size) > (uint64_t)(63<<20))
// predicted_size > (uint64_t)( (uint64_t)1<<25 + (uint64_t)31<<20);
class ZoneExtent;
class ZoneList {
 private:
  void *data_;
  unsigned int zone_count_;

 public:
  ZoneList(void *data, unsigned int zone_count)
      : data_(data), zone_count_(zone_count){};
  void *GetData() { return data_; };
  unsigned int ZoneCount() { return zone_count_; };
  ~ZoneList() { free(data_); };
};

class Mutex: public std::mutex
{
public:
// #ifndef NDEBUG
    void lock()
    {
        // tryers.push_back(std::this_thread::get_id());
        mutex_.lock();
        m_holder = std::this_thread::get_id(); 
        tid_=gettid();
        // for(size_t i = 0; i<tryers.size();i++){
        //   if(tryers[i]==m_holder){
        //     tryers.erase(tryers.begin()+i);
        //     return;
        //   }
        // }
    }
// #endif // #ifndef NDEBUG

// #ifndef NDEBUG
    void unlock()
    {
        m_holder = std::thread::id();
        tid_=0;
        mutex_.unlock();
    }
// #endif // #ifndef NDEBUG

// #ifndef NDEBUG
    bool try_lock()
    {
        if (mutex_.try_lock()) {
            m_holder = std::thread::id();
            tid_=gettid();
            return true;
        }
        return false;
    }
// #endif // #ifndef NDEBUG

// #ifndef NDEBUG
    /**
    * @return true iff the mutex is locked by the caller of this method. */
    bool locked_by_caller() const
    {
        return m_holder == std::this_thread::get_id();
    }
// #endif // #ifndef NDEBUG
    std::string get_name(){
      std::ostringstream ss;

      ss << m_holder;

      std::string idstr = ss.str();
      return idstr;
    }
    pid_t get_tid(){
      return tid_;
    }
    void print_tryers(){
      for(size_t i = 0 ;i<tryers.size();i++){
        std::thread::id t=tryers[i];
        std::ostringstream ss;
        ss << t;
        std::string idstr = ss.str();
        printf("%s ",idstr.c_str());
      }
    }
private:
// #ifndef NDEBUG
    pid_t tid_;
    std::mutex mutex_;
    std::vector<std::thread::id> tryers; 
    std::atomic<std::thread::id> m_holder = std::thread::id{};
    // std::atomic<std::thread::id> tryer = std::thread::id{};
// #endif // #ifndef NDEBUG
};
inline bool ends_with(std::string const& value, std::string const& ending) {
  if (ending.size() > value.size()) return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}
struct AsyncZoneCleaningIocb{
    AsyncZoneCleaningIocb(struct iocb* iocb,std::string fname,uint64_t start,uint64_t length,uint64_t header_size)
    : iocb_(iocb),start_(start),length_(length) ,header_size_(header_size)
    {
      filename_=fname;
      if(header_size >4096){
        header_size=0;
        header_size_=0;
      }
      int r=posix_memalign((void**)(&buffer_),sysconf(_SC_PAGE_SIZE),length+header_size);
        // posix_memalign((void**)&buf,sysconf(_SC_PAGE_SIZE),size);
      if (r) {
        printf("AsyncZoneCleaningIocb Out of memory while recovering %s %lu %lu\n",fname.c_str(),length,header_size);
      }

    }
    ~AsyncZoneCleaningIocb() {
      free(buffer_);
    }

    struct iocb* iocb_;
    uint64_t start_;
    uint64_t length_;
    uint64_t header_size_;
    std::string filename_;
    char* buffer_;
};

class Zone {
  ZonedBlockDevice *zbd_;
  ZonedBlockDeviceBackend *zbd_be_;
  std::mutex zone_lock_;
  
 public:
  explicit Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
                std::unique_ptr<ZoneList> &zones, uint64_t idx);
  explicit Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
                std::unique_ptr<ZoneList> &zones, uint64_t idx,
                unsigned int log2_erase_unit_size);

  uint64_t start_; // absolute value, not changed
  uint64_t capacity_; /* remaining capacity, variable */
  uint64_t max_capacity_; // not changed
  uint64_t wp_; // absolute value, variable
  uint64_t zidx_; // not changed
  uint64_t zone_sz_;
  unsigned int log2_erase_unit_size_ = 0;
  uint64_t erase_unit_size_ = 0;
  uint64_t block_sz_;

  uint64_t reset_count_ = 0;
  // uint64_t invalid_wp_;


  std::atomic<int> zone_readers_{0};
  std::atomic<long> read_lock_overhead_{0};
  // std::atomic<int> writers_;
  std::mutex zone_writer_mtx_;
  std::deque<ZoneExtent*> zone_extents_;
  std::mutex zone_extents_lock_;
  Env::WriteLifeTimeHint lifetime_;
  std::atomic<uint64_t> used_capacity_;
  // bool compare(int a, int b){
  //     return a > b;
  // }
  static bool SortByResetCount(Zone* za,Zone* zb){
    return za->reset_count_ < zb->reset_count_;
  }
  inline ZonedBlockDevice* GetZBD(void) { return zbd_;}
  IOStatus Reset();
  IOStatus PartialReset(size_t* erase_sz);
  IOStatus PartialResetToAllInvalidZone(size_t erase_sz);
  IOStatus Finish();
  IOStatus Close();
  void PushExtent(ZoneExtent* ze);

  void PushExtentAtFront(ZoneExtent* ze);
  IOStatus Append(char *data, uint64_t size,bool zc=false);
  IOStatus ThrowAsyncZCWrite(io_context_t& ioctx, AsyncZoneCleaningIocb* aiocb);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  uint64_t GetBlockSize();

  // // bool DoingPartialReset { }
  bool Acquire() {
    return zone_lock_.try_lock();
  }
  void Release() {
    zone_lock_.unlock();
  }

  void EncodeJson(std::ostream &json_stream);


  /*
  Partial Reset::
  if reader in, pass it
  if no reader, block reader and partial reset 

  Reader ::
  if partial Reset, wait it
  if readeing, block partial reset

  after acquire, zone_extents_lock/zone_lock both should be held
  */
  bool TryZoneWriteLock();
  void ReleaseZoneWriteLock(){
    zone_extents_lock_.unlock();
    zone_lock_.unlock();
  }

  uint64_t PrintZoneExtent(bool print);
};



class ZonedBlockDeviceBackend {
 public:
  uint64_t block_sz_ = 0;
  uint64_t zone_sz_ = 0;
  uint64_t nr_zones_ = 0;
  unsigned int log2_erase_unit_size_ = 0;

 public:
  virtual int GetFD(int i) = 0;
  virtual IOStatus Open(bool readonly, bool exclusive,
                        unsigned int *max_active_zones,
                        unsigned int *max_open_zones,
                        unsigned int *log2_erase_unit_size) = 0;

  virtual std::unique_ptr<ZoneList> ListZones() = 0;
  virtual IOStatus Reset(uint64_t start, bool *offline,
                         uint64_t *max_capacity) = 0;
  virtual IOStatus PartialReset(uint64_t , uint64_t,bool) = 0;
  virtual IOStatus Finish(uint64_t start) = 0;
  virtual IOStatus Close(uint64_t start) = 0;
  virtual int Read(char *buf, int size, uint64_t pos, bool direct) = 0;
  virtual int Write(char *data, uint64_t size, uint64_t pos) = 0;
  virtual bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                         unsigned int idx) = 0;
  virtual bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                              unsigned int idx) = 0;
  virtual bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                            unsigned int idx) = 0;
  virtual bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                   unsigned int idx) = 0;
  virtual uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual std::string GetFilename() = 0;
  uint64_t GetBlockSize() { return 4096; };
  uint64_t GetZoneSize() { return zone_sz_; };
  uint64_t GetNrZones() { return nr_zones_; };
  #ifdef BLKPARTIALRESETZONE
  unsigned int GetLog2EraseUnitSize() { return log2_erase_unit_size_;}
  #else
  unsigned int GetLog2EraseUnitSize() { return 0;}
  #endif
  virtual ~ZonedBlockDeviceBackend(){};
};

enum class ZbdBackendType {
  kBlockDev,
  kZoneFS,
};

class ZonedBlockDevice {
 private:
  FileSystemWrapper* zenfs_;
  std::unique_ptr<ZonedBlockDeviceBackend> zbd_be_;
  std::vector<Zone *> spare_zones;
  std::vector<Zone *> io_zones;
  std::vector<Zone *> meta_zones;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint64_t finish_threshold_ = 0;
  unsigned int log2_erase_unit_size_;
  uint64_t zone_sz_;
/* FAR STATS */
  std::atomic<uint64_t> bytes_written_{0};
  std::atomic<uint64_t> gc_bytes_written_{0};
  std::atomic<bool> force_zc_should_triggered_{false};
  uint64_t reset_threshold_ = 0;
  uint64_t reset_threshold_arr_[101];
  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;

  std::atomic<size_t> reset_count_{0};

  std::atomic<size_t> erase_size_{0};
  std::atomic<size_t> erase_size_zc_{0};
  std::atomic<size_t> erase_size_proactive_zc_{0};
  std::atomic<size_t> partial_erase_size_{0};
  std::atomic<size_t> partial_to_valid_erase_size_{0};

  std::atomic<size_t> partial_reset_count_{0};
  std::atomic<size_t> partial_reset_to_valid_count_{0};
  std::atomic<size_t> reset_count_zc_{0};
  std::atomic<size_t> reset_resigned_{0};
  std::atomic<size_t> reset_tried_{0};
  std::atomic<size_t> ratio_sum_{0};
  std::atomic<size_t> ratio_sum2_{0};

  size_t candidate_ratio_sum_ = 0;
  size_t candidate_valid_ratio_sum_=0;
  size_t no_candidate_valid_ratio_sum_=0;

  size_t candidate_ratio_sum_before_zc_ = 0;
  size_t candidate_valid_ratio_sum_before_zc_=0;
  size_t no_candidate_valid_ratio_sum_before_zc_=0;
  size_t before_zc_T_=-1;
  bool before_zc_ = true;

  // read_latency_sum_.fetch_add(microseconds);
  // read_n_.fetch_add(1);
  std::atomic<uint64_t> read_latency_sum_{0};
  std::atomic<uint64_t> read_n_{0};

  std::atomic<uint64_t> wasted_wp_{0};
  std::atomic<clock_t> runtime_reset_reset_latency_{0};
  std::atomic<long long> runtime_reset_latency_{0};

  std::atomic<uint64_t> partial_reset_total_erased_n{0};
  std::atomic<uint64_t>  partial_reset_called_n{0};
  std::atomic<uint64_t> runtime_zone_reset_called_n_{0};
  std::atomic<uint64_t> device_free_space_;

  std::mutex compaction_refused_lock_;
  std::atomic<int> compaction_refused_by_zone_interface_{0};
  std::set<int> compaction_blocked_at_;
  std::vector<int> compaction_blocked_at_amount_;

  int zone_cleaning_io_block_ = 0;
  clock_t ZBD_mount_time_;
  bool zone_allocation_state_ = true;

  DB* db_ptr_;
  
  ZoneFile** sst_file_bitmap_;

  struct ZCStat{
    size_t zc_z;
    int s;
    int e;
    long long us;
    size_t copied;
    bool forced;
    // std::vector<int> levels_files_timelapse;
  };
  std::vector<ZCStat> zc_timelapse_;
  // std::vector<uint64_t> zc_copied_timelapse_;

  std::mutex io_lock_;

  struct IOBlockStat{
    pid_t tid;
    int s;
    int e;
  };
  std::vector<IOBlockStat> io_block_timelapse_;

  // std::atomic<int> io_blocked_thread_n_{0};
  int io_blocked_thread_n_ = 0;

  enum StallCondition {kNOTSET_COND=0,kNORMAL=1,kDELAYED=2,kSTOPPED=3  };
  enum StallCause {kNOTSET_CAUSE=0,kNONE=1,kMEMTABLE_LIMIT=2,kL0FILE_COUNT_LIMIT=3,kPENDING_COMPACTION=4,kNoFreeSpaceInZNS=5};
  struct WriteStallStat{
    
    StallCondition cond = StallCondition::kNOTSET_COND;
    StallCause cause = StallCause::kNOTSET_CAUSE;

    void PrintStat(void){
      std::string cond_str;
      std::string cause_str;
      switch (cond)
      {
      case StallCondition::kNORMAL:
        cond_str="NORMAL";
      break;
      case StallCondition::kDELAYED:
        cond_str="DELAYED";
        break;
      case StallCondition::kSTOPPED:
        cond_str="STOPPED";
        break;
      default:
        cond_str=" ";
        break;
      }

      switch (cause)
      {
      case StallCause::kNONE:
        cond_str=" ";
        break;
      case StallCause::kMEMTABLE_LIMIT:
        cause_str="MEMTABLE LIMIT";
        break;
      case StallCause::kL0FILE_COUNT_LIMIT:
        cause_str="L0FILE LIMIT";
        break;
      case StallCause::kPENDING_COMPACTION:
        cause_str="PENDING COMPACTION";
        break;
      case StallCause::kNoFreeSpaceInZNS:
        cause_str="kNoFreeSpaceInZNS";
        break;
      default:
        cause_str=" ";
        break;
      }
      printf(" %7s | %18s |\n", cond_str.c_str(), cause_str.c_str());
    }
  };
  std::unordered_map<int,WriteStallStat> write_stall_timelapse_;
  // std::atomic<double> reset_total_time_{0.0};
  /* Protects zone_resuorces_  condition variable, used
     for notifying changes in open_io_zones_ */
  std::mutex zone_resources_mtx_;
  std::condition_variable zone_resources_;
  std::mutex zone_deferred_status_mutex_;
  IOStatus zone_deferred_status_;



  int max_nr_active_io_zones_;
  int max_nr_open_io_zones_;

  std::vector<std::pair<int,uint64_t>> sst_file_size_last_;
  std::mutex sst_file_size_last_lock_;
  std::vector<std::pair<int,uint64_t>> sst_file_size_else_;
  std::mutex sst_file_size_else_lock_;
  

  std::shared_ptr<ZenFSMetrics> metrics_;
  uint64_t cur_free_percent_ = 100;
  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);
  void CalculateResetThreshold(uint64_t free_percent);
  uint32_t reset_scheme_;
  uint64_t allocation_scheme_;
  uint32_t partial_reset_scheme_;
  uint64_t input_aware_scheme_;
  uint64_t tuning_point_;
  uint64_t async_zc_enabled_;
  enum {
    kEager = 0,
    kLazy = 1,
    kFAR = 2,
    kLazy_Log = 3,
    kLazy_Linear = 4,
    kCustom = 5,
    kLogLinear = 6,
    kNoRuntimeReset = 7,
    kNoRuntimeLinear = 8,
    kLazyExponential = 9
  };
  struct CompactionStats{
    std::atomic<uint64_t> input_size_input_level_{0};
    std::atomic<uint64_t> input_size_output_level_{0};
    std::atomic<uint64_t> output_size_{0};
    std::atomic<uint64_t> compaction_triggered_{0};
  };
  
  CompactionStats compaction_stats_[10];
  struct FARStat{

    uint64_t free_percent_;

    size_t reset_count_;
    size_t reset_count_zc_;
    size_t partial_reset_count_;
    size_t erase_size_=0;
    size_t erase_size_zc_=0;
    size_t erase_size_proactive_zc_=0;

    size_t partial_erase_size_=0;

    int T_;
    uint64_t R_wp_; // (%)
    uint64_t RT_;

    size_t candidate_ratio_;

    std::vector<int> num_files_levels_;

    std::vector<double> compaction_scores_;


    std::vector<uint64_t> levels_size_;

    // uint64_t l1l2_compaction_triggered_;
    // CompactionStats _compaction_stats_[10];
    uint64_t compaction_triggered_[10];
    // double total_avg_same_zone_score_;
    // double total_avg_inval_s
    double avg_same_zone_score_[10];
    double avg_inval_score_ [10];
    double avg_invalid_ratio_;

    std::vector<uint64_t> invalid_percent_per_zone_;
    uint64_t cur_ops_;
    FARStat(uint64_t fr, size_t rc, size_t rc_zc,size_t partial_rc,size_t er_sz,size_t er_sz_zc,size_t er_sz_pr_zc,size_t p_er_sz,
            uint64_t wwp, int T, uint64_t rt,uint64_t zone_sz, std::vector<int> num_files_levels, 
            std::vector<double> compaction_scores, std::vector<uint64_t> levels_size,
            CompactionStats* compaction_stats,
            std::vector<double>* same_zone_score_for_timelapse,
            std::vector<double>* inval_score_for_timelapse ,
            double avg_invalid_ratio,
            std::vector<uint64_t> invalid_percent_per_zone,
            uint64_t cur_ops)
        : free_percent_(fr),  reset_count_(rc),reset_count_zc_(rc_zc),partial_reset_count_(partial_rc),
          erase_size_(er_sz),erase_size_zc_(er_sz_zc), erase_size_proactive_zc_(er_sz_pr_zc) ,partial_erase_size_(p_er_sz) 
          , T_(T), RT_(rt), num_files_levels_(num_files_levels), compaction_scores_(compaction_scores),
          levels_size_(levels_size),avg_invalid_ratio_(avg_invalid_ratio),cur_ops_(cur_ops) {
      if((rc+rc_zc)==0){
        R_wp_= 100;
      }else{
        R_wp_= (BYTES_TO_MB(zone_sz)*100-BYTES_TO_MB(wwp)*100/(rc+rc_zc))/BYTES_TO_MB(zone_sz);
      }
      (void)(same_zone_score_for_timelapse);
      (void)(inval_score_for_timelapse);
      // printf("++farstat? 0\n");
      double sum_sum_same_score = 0.0;
      size_t n_compaction = 0;
      double sum_sum_inval_score = 0.0;

      for(int i = 0 ; i <5;i++){
        compaction_triggered_[i]=compaction_stats[i].compaction_triggered_;
        double sum_score=0.0;
        for(double score : same_zone_score_for_timelapse[i]){
          sum_score+=score;
          // sum_sum_same_score+=score;
        }
        n_compaction+=same_zone_score_for_timelapse[i].size();
        sum_sum_same_score+=sum_score;
        avg_same_zone_score_[i]= same_zone_score_for_timelapse[i].size() ? 
                              sum_score/same_zone_score_for_timelapse[i].size() : 0.0;
        
        sum_score=0.0;
        for(double score : inval_score_for_timelapse[i]){
          sum_score+=score;
          
        }
        sum_sum_inval_score+=sum_score;
        avg_inval_score_[i]=inval_score_for_timelapse[i].size() ? 
                      sum_score/inval_score_for_timelapse[i].size() : 0.0;
      }
      // printf("++farstat? 1\n");
      avg_same_zone_score_[0]=n_compaction == 0 ? 1.0 : sum_sum_same_score/n_compaction;
      avg_inval_score_[0] = n_compaction == 0 ? 100.0 : sum_sum_inval_score/n_compaction;

      invalid_percent_per_zone_.clear();
      invalid_percent_per_zone_=invalid_percent_per_zone;
      
  // printf("++farstat?\n");

      // size_t score_n=same_zone_score_for_timelapse.size();
      // if(score_n>0){
      //   for(double score : same_zone_score_for_timelapse){
      //     sum_score+=score;
      //   }
      //   avg_same_zone_score_[0]=sum_score/score_n;

      //   sum_score=0.0;
      //   for(double score : inval_score_for_timelapse){
      //     sum_score+=score;
      //   }
      //   avg_inval_score_[0]=sum_score/score_n;
      // }
    //   num_files_levels_=num_files_levels;
    }

    void PrintInvalidZoneDist(){
      for(size_t i=0;i<invalid_percent_per_zone_.size();i++){
        printf("%lu\t",invalid_percent_per_zone_[i]);
      }
      printf("\n");
    }

    void PrintStat(void){
      //   Sec    | Free |  RC |  RCZ |  RCP  | R_wp  |      Twp   |   erase_sz   |      erase_sz_zc |   p_er_sz      |
      // printf("[%4d] | %3ld  | %3ld |  %3ld | [%3ld] | [ %4ld] | [ %4ld ] | [ %10ld ] | [ %10ld ] | [ %10ld ] |", 
      //           T_, free_percent_, reset_count_,reset_count_zc_,partial_reset_count_,
      //        R_wp_, (RT_ >> 20),(erase_size_>>20),(erase_size_zc_>>20),(partial_erase_size_>>20));
            printf("%4d\t%3ld\t%3ld\t%3ld\t%3ld\t%4ld\t%4ld\t%10ld\t%10ld\t%10ld\t%.4lf\t", 
                T_, free_percent_, reset_count_,reset_count_zc_,partial_reset_count_,
             R_wp_, (RT_ >> 20),(erase_size_>>20),(erase_size_zc_>>20),(partial_erase_size_>>20)
             ,avg_invalid_ratio_);
      for(int i = 0 ; i < 5 ; i++){
        printf("%.4lf\t",avg_same_zone_score_[i]);
      }
      for(int i = 0 ; i < 5 ; i++){
        printf("%.3lf\t",avg_inval_score_[i]);
      }
      printf("%lu\t",cur_ops_);
      // for(int n : num_files_levels_){
      //   printf("%d\t",n);
      // }
      // printf("|score\t");
      // for(double s : compaction_scores_){
      //   printf("%lf\t",s);
      // }
      // printf("|size\t");
      // for(uint64_t s : levels_size_){
      //   printf("%lu\t",(s>>20));
      // }
      // printf("|l1l2triggered\t");
      // for(auto trigger : compaction_triggered_){
      //   printf("%lu\t",(trigger));
      // }
      printf("\n");

    }
  };



  std::vector<FARStat> far_stats_;
  std::mutex same_zone_score_mutex_;
  std::vector<double> same_zone_score_[10];
  std::vector<double> same_zone_score_for_timelapse_[10];

  

  std::vector<double> invalidate_score_[10];
  std::vector<double> invalidate_score_for_timelapse_[10];

  std::atomic<uint64_t> same_zone_score_atomic_[10];
  std::atomic<uint64_t> invalidate_score_atomic_ [10];
  std::atomic<uint64_t> compaction_triggered_[10];
  // std::atomic<uint64_t> intral0_compaction_input_size_{0};
  // std::atomic<uint64_t> intral0_compaction_output_size_{0};
  // std::atomic<uint64_t> intral0_compaction_triggered_{0};
  
 public:
  uint64_t ZONE_CLEANING_KICKING_POINT=40;
  std::atomic<bool> migrating_{false};
  std::condition_variable migrate_resource_;
  std::mutex migrate_zone_mtx_;
  
  std::atomic<uint64_t> lsm_tree_[10];
  /*
  0 : 256
  1 : 256
  2 : 2560
  3 : 25600
  4 : 256000
  5 : 2560000
  6 : 25600000 
  */
  uint64_t max_bytes_for_level_base_ = 256<<20;

  bool zc_until_set_=false;
  uint64_t zc_;
  uint64_t until_;

  bool zc_running_=false;
  std::mutex zc_or_partial_lock_;
  void ZCorPartialLock(); 
  bool ZCorPartialTryLock();
  void ZCorPartialUnLock();
  uint64_t AsyncZCEnabled(){
    return async_zc_enabled_;
  }
  int GetFD(int i) {
    return zbd_be_->GetFD(i);
  }
  double PredictCompactionScore(int level){

    // if(db_ptr_!=nullptr){
    //   return  db_ptr_->ReCalculateCompactionScore(level);
    // }

    if(level ==0 ){
      if(db_ptr_==nullptr){
        return 0.0;
      }
      return db_ptr_->ReCalculateCompactionScore(0);
    }
    if(level == 1){
      return (double)((double)( lsm_tree_[level].load()) / (double)(max_bytes_for_level_base_));
    }
    uint64_t max_bytes_for_level = max_bytes_for_level_base_;
    for(int l = 1 ; l<level;l++){
      max_bytes_for_level*=10;
    }

    return (double)( (double)(lsm_tree_[level].load()) /(double) max_bytes_for_level);
  }
  inline uint64_t GetAllocationScheme() { return allocation_scheme_;}

  uint64_t GetZoneCleaningKickingPoint(){ 
    if(zc_until_set_){
      return zc_;
    }
    if(io_zones[0]->max_capacity_ > (1<<30)){
      return 30;
    }
    if(io_zones[0]->max_capacity_ == (1<<30)){
      return 30;
    }
    return 10;
  }


 uint64_t GetReclaimUntil(){
    if(RuntimeZoneResetDisabled()){
      return 100;
    }
    if(zc_until_set_){
      return until_;
    }
    if(RuntimeZoneResetOnly()){
      if(io_zones[0]->max_capacity_ > (1<<30)){
        // if(reset_scheme_==kFAR){
        //   return GetZoneCleaningKickingPoint()+30;
        // }else { // ezr
        //   return GetZoneCleaningKickingPoint()+20;
        // }
        return GetZoneCleaningKickingPoint()+30;
      }else{
        // if(reset_scheme_==kFAR){
        //   return GetZoneCleaningKickingPoint()+10;
        // }else { // ezr
        return GetZoneCleaningKickingPoint()+30;
        // }
      }
    }
    if(PartialResetWithZoneReset()){
      if(io_zones[0]->max_capacity_ > (1<<30)){
        if(reset_scheme_==kFAR){
          return GetZoneCleaningKickingPoint()+30;
        }else { // ezr
          return GetZoneCleaningKickingPoint()+20;
        }
        // return GetZoneCleaningKickingPoint()+30;
      }else{


        return GetZoneCleaningKickingPoint()+10;

      }
    }
    return GetZoneCleaningKickingPoint()+20;
  }
  void SetZCRunning(bool v){ zc_running_=v; }
  bool GetZCRunning(void) {return zc_running_; }
  uint64_t GetFullZoneN(){
    // uint64_t threshold = (100 - 3 * (GetZoneCleaningKickingPoint() - cur_free_percent_));
    uint64_t ret = 0;
    for(auto z : io_zones){
      if((z->used_capacity_*100/z->max_capacity_)>95){
        continue;
      }
      if(!z->IsFull()){
        continue; 
      }
      // uint64_t garbage_percent_approx =
      //       100 - 100 * z->used_capacity_ / z->max_capacity_;
      // if (garbage_percent_approx > threshold &&
      //     garbage_percent_approx < 100) {
        ret++;
      // }
      // ret++;
    }
    return ret;
  }
  explicit ZonedBlockDevice(std::string path, ZbdBackendType backend,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics =
                                std::make_shared<NoZenFSMetrics>());
  virtual ~ZonedBlockDevice();
  void SetFSptr(FileSystemWrapper* fs) { zenfs_=fs; }
  IOStatus Open(bool readonly, bool exclusive);

  Zone *GetIOZone(uint64_t offset);

  IOStatus AllocateIOZone(bool is_sst,Slice& smallest, Slice& largest ,int level,
                            Env::WriteLifeTimeHint file_lifetime, IOType io_type,
                            std::vector<uint64_t>& input_fno,uint64_t predicted_size,
                          Zone **out_zone ,uint64_t min_capacity);
  
  void SetZoneAllocationFailed() { zone_allocation_state_=false; }
  bool IsZoneAllocationFailed(){ return zone_allocation_state_==false; }
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();
  uint64_t GetFreePercent(void);
  uint64_t GetFreePercent(uint64_t diskfree);
  std::string GetFilename();
  uint64_t GetBlockSize();

 // void AddZCIOBlockedTime(clock_t t){ zone_cleaning_io_block_.fetch_add(t); }
  void AddIOBlockedTimeLapse(int s,int e) {
    std::lock_guard<std::mutex> lg_(io_lock_);
    io_block_timelapse_.push_back({gettid(),s,e});
    zone_cleaning_io_block_+=(e-s);
  }


  clock_t IOBlockedStartCheckPoint(void){
    std::lock_guard<std::mutex> lg_(io_lock_);
    clock_t ret=clock();
    io_blocked_thread_n_++;
    return ret;
  }
  void IOBlockedEndCheckPoint(int start){
    int end=clock();
    std::lock_guard<std::mutex> lg_(io_lock_);
    io_blocked_thread_n_--;
    io_block_timelapse_.push_back({gettid(),start,-1});
    if(io_blocked_thread_n_==0){
      zone_cleaning_io_block_+=(end-start);
    }
    return;
  }

  void AddZCTimeLapse(int s,int e,long long us,size_t zc_z,size_t copied,bool forced){
    
    if(forced==true){
      force_zc_should_triggered_.store(false);
    }

    // db_ptr_->SameLevelFileList

    zc_timelapse_.push_back({zc_z,s,e,us,copied,forced});
  }
  void AddTimeLapse(int T,uint64_t cur_ops);

  
  
  
  IOStatus ResetUnusedIOZones(void);
  IOStatus RuntimeZoneReset(std::vector<bool>& is_reseted);
  IOStatus RuntimePartialZoneReset(std::vector<bool>& is_reseted);
  void StatsPartialReset(uint64_t to_be_erased_unit_n){
    partial_reset_total_erased_n.fetch_add(to_be_erased_unit_n);
    partial_reset_called_n.fetch_add(1);
  }
  void LogZoneStats();
  void LogZoneUsage();
  void LogGarbageInfo();

  uint64_t GetZoneSize();
  uint64_t GetEraseUnitSize() { return (1<<log2_erase_unit_size_);}
  uint64_t GetNrZones();
  std::vector<Zone *> GetMetaZones() { return meta_zones; }

  void SetFinishTreshold(uint64_t threshold) { finish_threshold_ = threshold; }

  void PutOpenIOZoneToken();
  void PutActiveIOZoneToken();

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }

  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

    class ZoneReadLock{
      public:
      ZoneReadLock() {}


      ~ZoneReadLock() {
        ReadUnLockZone();
      }
      void ReadLockZone(Zone* zone){

        if(zone==nullptr){
          return;
        }
        if(zone->GetZBD()->RuntimeZoneResetOnly()){
          return;
        }
        zone_=zone;

        auto start = std::chrono::high_resolution_clock::now();
        long long microseconds;

        zone_->zone_extents_lock_.lock();
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        zone_->zone_readers_ .fetch_add(1);
        zone_->read_lock_overhead_.fetch_add(microseconds);
        zone_->zone_extents_lock_.unlock();
      }
      void ReadUnLockZone(void){
        // if(RuntimeZoneResetOnly()){
        //   return;
        // }
        if(zone_!=nullptr){
          if(zone_->GetZBD()->RuntimeZoneResetOnly()){
            zone_=nullptr;
            return;
          }
           zone_->zone_readers_.fetch_sub(1);
        }
        zone_=nullptr;
      }

      private:
        Zone* zone_=nullptr;
    };
  void EntireZoneReadLock(){
    ZoneReadLock zone_read_lock;
    for(auto z : io_zones){
      zone_read_lock.ReadLockZone(z);
    }
  }
  void EntireZoneReadUnLock(){
    for(auto z : io_zones){
      z->zone_readers_.fetch_sub(1);
    }
  }
  int Read(char *buf, uint64_t offset, int n, bool direct);

  IOStatus ReleaseMigrateZone(Zone *zone);

  // IOStatus TakeMigrateZone(Zone **out_zone, Env::WriteLifeTimeHint lifetime,
  //                          uint64_t min_capacity,bool* run_gc_worker_);
  IOStatus TakeMigrateZone(Slice& smallest,Slice& largest, int level,Zone **out_zone,
                                           Env::WriteLifeTimeHint file_lifetime,uint64_t file_size,
                                           uint64_t min_capacity,bool* run_gc_worker_,bool is_sst);
  uint64_t CalculateProperReclaimedZoneN(void){
    size_t zone_percentage=100/io_zones.size();
    size_t to_be_reclaimed_ratio=(ZONE_CLEANING_KICKING_POINT+5)-cur_free_percent_;
    if(to_be_reclaimed_ratio<ZONE_CLEANING_KICKING_POINT && to_be_reclaimed_ratio>0){
      return  (to_be_reclaimed_ratio/zone_percentage);
    
    }
    // return to_be_reclaimed_zone_n;
    return 1;
  }

  void AddBytesWritten(uint64_t written) { bytes_written_.fetch_add(written); };
  void AddGCBytesWritten(uint64_t written) {
    gc_bytes_written_.fetch_add(written); 
    // zc_copied_timelapse_.push_back(written);
  };

  uint64_t GetGCBytesWritten(void) { return gc_bytes_written_.load(); }
  uint64_t GetUserBytesWritten() {
    return bytes_written_.load() - gc_bytes_written_.load();
  };
  uint64_t GetTotalBytesWritten() { return bytes_written_.load(); };
  int GetResetCount() { return reset_count_.load(); }
  // int GetResetCountBG() {return reset_count_bg_.load();}
  uint64_t GetWWP() { return wasted_wp_.load();}
  
  void SetDBPtr(DB* db_ptr){
    db_ptr_=db_ptr;  
  }

  void SetCurFreepercent(uint64_t free_percent) { cur_free_percent_=free_percent; }
  
  uint64_t CalculateFreePercent(void) {
    // uint64_t zone_sz=BYTES_TO_MB(io_zones[0]->max_capacity_); // MB
    uint64_t device_size=(uint64_t)io_zones.size()*BYTES_TO_MB(io_zones[0]->max_capacity_) ; // MB
    uint64_t d_free_space=device_size ; // MB
    uint64_t writed = 0;
    for(const auto z : io_zones){
      // if(z->IsBusy()){
      //   d_free_space-=zone_sz;
      // }else{
        // if(z->wp_>z->max_capacity_){
        //   writed+=z->max_capacity_-z->start_;
        // }else{
        //   writed+=z->wp_-z->start_; // BYTE
        // }
      uint64_t tmp = z->wp_-z->start_;
      if(tmp > z->max_capacity_){
        tmp = z->max_capacity_;
      }
      writed+=tmp;
        

      // }
    }
    
    // printf("df1 %ld\n",d_free_space);
    d_free_space-=BYTES_TO_MB(writed);
    // printf("df 2%ld\n",d_free_space);
    device_free_space_.store(d_free_space);
    cur_free_percent_= (d_free_space*100)/device_size;

    return cur_free_percent_;
  }

  uint64_t CalculateCapacityRemain(){
    uint64_t ret = 0;
    for(const auto z: io_zones){
      ret+=z->capacity_;
    }
    return ret;
  }

  void WriteStallCheckPoint(int T,int write_stall_cause,int write_stall_cond){
    write_stall_cause++;
    write_stall_cond++;
    StallCause cause=(StallCause) write_stall_cause;
    StallCondition cond = (StallCondition) write_stall_cond;
    if(write_stall_timelapse_[T].cond<cond ){
      write_stall_timelapse_[T].cond=cond;
      write_stall_timelapse_[T].cause=cause;
    }
  }

  bool PreserveZoneSpace(uint64_t approx_size) {
    // RuntimeZoneReset();
    approx_size>>=20;
    std::lock_guard<std::mutex> lg_(compaction_refused_lock_);

    uint64_t tmp = device_free_space_.load();
    // printf("@@@ preserve free space : approx size : %ld, left %ld\n",approx_size,tmp);
    if(tmp<approx_size){
      // compaction_refused_by_zone_interface_.fetch_add(1);
      int s=zenfs_->GetMountTime();
    
      // compaction_refused_lock_
      compaction_blocked_at_amount_.push_back(s);
      compaction_blocked_at_.emplace(s);
      force_zc_should_triggered_.store(true);
      return false;
    }
    device_free_space_.store(tmp-approx_size);
    return true;
  }
  bool ShouldForceZCTriggered(void) { return force_zc_should_triggered_.load(); }
  
  IOStatus ResetAllZonesForForcedNewFileSystem(void);
  
  void StatsCompactionFileSize(bool is_last_file, int output_level,uint64_t file_size){
  // std::vector<uint64_t> sst_file_size_last_;
  // std::mutex sst_file_size_last_lock_;
  // std::vector<uint64_t> sst_file_size_else_;
  // std::mutex sst_file_size_else_lock_;
  return;
    if(is_last_file){
      std::lock_guard<std::mutex> lg(sst_file_size_last_lock_);
      sst_file_size_last_.emplace_back(output_level,file_size);
    }else{
      std::lock_guard<std::mutex> lg(sst_file_size_else_lock_);
      sst_file_size_else_.emplace_back(output_level,file_size);
    }
  }
  void StatsAverageCompactionInputSize(int start_level, int output_level,
                            uint64_t input_size_input_level, uint64_t input_size_output_level,
                            uint64_t output_size){
  //     std::atomic<uint64_t> total_compaction_input_size_{0};
  // std::atomic<uint64_t> compaction_triggered_{0};
    // if(start_level==0 && output_level==0){
    //   intral0_compaction_input_size_.fetch_add((input_size_input_level+input_size_output_level));
    //   intral0_compaction_output_size_.fetch_add(output_size);
    //   intral0_compaction_triggered_.fetch_add(1);
    // }else{
    //   compaction_stats_[]
    // }
    // total_compaction_input_size_.fetch_add(input_size);
    // compaction_triggered_.fetch_add(1);
    (void)(start_level);
    compaction_stats_[output_level].input_size_input_level_.fetch_add(input_size_input_level);
    compaction_stats_[output_level].input_size_output_level_.fetch_add(input_size_output_level);
    compaction_stats_[output_level].output_size_.fetch_add(output_size);
    compaction_stats_[output_level].compaction_triggered_.fetch_add(1);
    // if(start_level+1 != output_level){
    //   if(!(start_level == 0 && output_level==0) ){
    //     printf("error ? %d %d\n",start_level,output_level);
    //   }
    // }
  }

  void SetResetScheme(uint32_t r,uint32_t partial_reset_scheme,uint64_t T,uint64_t zc,uint64_t until,uint64_t allocation_scheme,
                    std::vector<uint64_t>& other_options) { 
    reset_scheme_=r; 
    allocation_scheme_=allocation_scheme;
    partial_reset_scheme_=partial_reset_scheme;
    tuning_point_=T;
    input_aware_scheme_ = other_options[0];
    async_zc_enabled_ = other_options[1];
    if(zc!=0){
      zc_until_set_=true;
      zc_=zc;
      until_=until;
    }

    
    for(uint64_t f=0;f<=100;f++){
      CalculateResetThreshold(f);
    }
  }
  void  GiveZenFStoLSMTreeHint(std::vector<uint64_t>& compaction_inputs_input_level_fno,
                            std::vector<uint64_t>& compaction_inputs_output_level_fno,int output_level,bool trivial_move);
  IOStatus RuntimeReset(void);
  double GetMaxInvalidateCompactionScore(std::vector<uint64_t>& file_candidates,uint64_t * candidate_size,bool stats);
  double GetMaxSameZoneScore(std::vector<uint64_t>& compaction_inputs_fno);
  inline bool RuntimeZoneResetDisabled() {return partial_reset_scheme_==RUNTIME_ZONE_RESET_DISABLED; }
  inline bool RuntimeZoneResetOnly() {return partial_reset_scheme_==RUNTIME_ZONE_RESET_ONLY; }
  inline bool PartialResetWithZoneReset() { return (partial_reset_scheme_==PARTIAL_RESET_WITH_ZONE_RESET ); }
  inline bool PartialResetOnly() { return partial_reset_scheme_==PARTIAL_RESET_ONLY&& log2_erase_unit_size_>0; }
  inline bool PartialResetAtBackground() { return partial_reset_scheme_==PARTIAL_RESET_AT_BACKGROUND;}
  inline bool PartialResetAtBackgroundThresholdWithZoneReset() {return partial_reset_scheme_==PARTIAL_RESET_BACKGROUND_T_WITH_ZONE_RESET; }
  inline bool ProactiveZoneCleaning() { return partial_reset_scheme_==PROACTIVE_ZONECLEANING;}
  
  uint32_t GetPartialResetScheme() {return partial_reset_scheme_;}
  

  void PrintZoneToFileStatus(void);


  bool SetSSTFileforZBDNoLock(uint64_t fno,ZoneFile* zoneFile);

  bool DeleteSSTFileforZBDNoLock(uint64_t fno);

  ZoneFile* GetSSTZoneFileInZBDNoLock(uint64_t fno);

  // Zone* GetIOZoneByOffset(uint64_t offset);
 private:
    std::vector<std::pair<uint64_t,uint64_t>> SortedByZoneScore(std::vector<uint64_t>& zone_score){
    std::vector<std::pair<uint64_t,uint64_t>> ret;
    ret.clear();
    for(uint64_t index = 0 ; index < zone_score.size() ; index++){
      ret.push_back( { zone_score[index], index });
    }
    std::sort(ret.rbegin(),ret.rend());
    return ret;
  }

  IOStatus GetZoneDeferredStatus();
  bool GetActiveIOZoneTokenIfAvailable();
  void WaitForOpenIOZoneToken(bool prioritized);
  IOStatus ApplyFinishThreshold();
  IOStatus FinishCheapestIOZone();
  IOStatus GetBestOpenZoneMatch(Env::WriteLifeTimeHint file_lifetime,
                                unsigned int *best_diff_out,std::vector<uint64_t> input_fno, Zone **zone_out,
                                uint64_t min_capacity = 0);
  IOStatus GetAnyLargestRemainingZone(Zone** zone_out,bool force,uint64_t min_capacity = 0);
  IOStatus AllocateEmptyZone(Zone **zone_out);

  bool CompactionSimulator(uint64_t predicted_size,int level,Slice& smallest, Slice& largest);
  bool CalculateZoneScore(std::vector<uint64_t>& fno_list,std::vector<uint64_t>& zone_score);
  void AllocateZoneBySortedScore(std::vector<std::pair<uint64_t,uint64_t>>& sorted,Zone** allocated_zone,uint64_t min_capacity);
  IOStatus AllocateCompactionAwaredZone(Slice& smallest, Slice& largest ,int level, 
                                          Env::WriteLifeTimeHint file_lifetime , std::vector<uint64_t> input_fno,
                                          uint64_t predicted_size,
                                          Zone **zone_out,
                                          uint64_t min_capacity = 0);
  IOStatus AllocateCompactionAwaredZoneV2(Slice& smallest, Slice& largest ,int level, 
                                          Env::WriteLifeTimeHint file_lifetime , std::vector<uint64_t> input_fno,
                                          uint64_t predicted_size,
                                          Zone **zone_out,
                                          uint64_t min_capacity = 0);

  IOStatus AllocateMostL0FilesZone(std::vector<uint64_t>& zone_score,std::vector<uint64_t>& fno_list,
                                    std::vector<bool>& is_input_in_zone,
                                    Zone** zone_out,uint64_t min_capacity);
  
  void AdjacentFileList(Slice& smallest,Slice& largest, int level, std::vector<uint64_t>& fno_list);
  void DownwardAdjacentFileList(Slice& s, Slice& l, int level, std::vector<uint64_t>& fno_list);
  uint64_t MostLargeUpperAdjacentFile(Slice& s, Slice& l, int level);
  uint64_t MostSmallDownwardAdjacentFile(Slice& s, Slice& l, int level);
  void SameLevelFileList(int level, std::vector<uint64_t>& fno_list, bool exclude_being_compacted = true);
  // int NumLevelFiles(int level);
  IOStatus AllocateSameLevelFilesZone(Slice& smallest, Slice& largest ,
                                      const std::vector<uint64_t>& fno_list,
                                      std::vector<bool>& is_input_in_zone,
                                      Zone** zone_out,
                                      uint64_t min_capacity);
  IOStatus GetNearestZoneFromZoneFile(ZoneFile* zFile,std::vector<bool>& is_input_in_zone,
                                      Zone** zone_out,
                                      uint64_t min_capacity);

  inline uint64_t LazyLog(uint64_t sz,uint64_t fr,uint64_t T);

  inline uint64_t LazyLinear(uint64_t sz,uint64_t fr,uint64_t T);
  inline uint64_t Custom(uint64_t sz,uint64_t fr,uint64_t T);
  inline uint64_t LogLinear(uint64_t sz,uint64_t fr,uint64_t T);
  inline uint64_t LazyExponential(uint64_t sz, uint64_t fr, uint64_t T);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
