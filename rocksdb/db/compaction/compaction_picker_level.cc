//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include <utility>
#include <vector>

#include "db/compaction/compaction_picker_level.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForForcedBlobGC().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return false;
}

namespace {
// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         SequenceNumber earliest_mem_seqno,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableOptions& ioptions,
                         const MutableDBOptions& mutable_db_options)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        earliest_mem_seqno_(earliest_mem_seqno),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        mutable_db_options_(mutable_db_options) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // For the specfied level, pick a file that we want to compact.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  // Picks a file from level_files to compact.
  // level_files is a vector of (level, file metadata) in ascending order of
  // level. If compact_to_next_level is true, compact the file to the next
  // level, otherwise, compact to the same level as the input file.
  void PickFileToCompact(
      const autovector<std::pair<int, FileMetaData*>>& level_files,
      bool compact_to_next_level);

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  SequenceNumber earliest_mem_seqno_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
};

void LevelCompactionBuilder::PickFileToCompact(
    const autovector<std::pair<int, FileMetaData*>>& level_files,
    bool compact_to_next_level) {
  for (auto& level_file : level_files) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    if ((compact_to_next_level &&
         start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      continue;
    }
    if (compact_to_next_level) {
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
    } else {
      output_level_ = start_level_;
    }
    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                   &start_level_inputs_)) {
      return;
    }
  }
  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    if (start_level_score_ >= 1) {
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        continue;
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      if (PickFileToCompact()) {
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;
          }
        }
      }
    } else {
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1.
      break;
    }
  }
  if (!start_level_inputs_.empty()) {
    // printf("SetupInitialFiles return 0\n");
    return;
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  parent_index_ = base_index_ = -1;

  compaction_picker_->PickFilesMarkedForCompaction(
      cf_name_, vstorage_, &start_level_, &output_level_, &start_level_inputs_);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
    printf("SetupInitialFiles return 1\n");
    return;
  }

  // Bottommost Files Compaction on deleting tombstones
  PickFileToCompact(vstorage_->BottommostFilesMarkedForCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kBottommostFiles;
    printf("SetupInitialFiles return 2\n");
    return;
  }

  // TTL Compaction
  PickFileToCompact(vstorage_->ExpiredTtlFiles(), true);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kTtl;
    printf("SetupInitialFiles return 3\n");
    return;
  }

  // Periodic Compaction
  PickFileToCompact(vstorage_->FilesMarkedForPeriodicCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kPeriodicCompaction;
    printf("SetupInitialFiles return 4\n");
    return;
  }

  // Forced blob garbage collection
  PickFileToCompact(vstorage_->FilesMarkedForForcedBlobGC(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kForcedBlobGC;
    printf("SetupInitialFiles return 5\n");
    return;
  }
}

bool LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    if (!compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(compaction_inputs_,
                                                            output_level_)) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                        output_level_inputs_, &grandparents_);
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

Compaction* LevelCompactionBuilder::PickCompaction() {
  // Pick up the first file to start compaction. It may have been extended
  // to a clean cut.
  SetupInitialFiles();
  if (start_level_inputs_.empty()) {
    return nullptr;
  }
  assert(start_level_ >= 0 && output_level_ >= 0);

  // If it is a L0 -> base level compaction, we need to set up other L0
  // files if needed.
  // if 0->1 and overlapped l0 in compaction, return nullptr
  if (!SetupOtherL0FilesIfNeeded()) {
    return nullptr;
  }

  // Pick files in the output level and expand more files in the start level
  // if needed.
  if (!SetupOtherInputsIfNeeded()) {
    return nullptr;
  }

  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

Compaction* LevelCompactionBuilder::GetCompaction() {
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(compaction_inputs_), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_,
                         vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      /* trim_ts */ "", start_level_score_, false /* deletion_compaction */,
      compaction_reason_);

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t LevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/main/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

bool LevelCompactionBuilder::PickFileToCompact() {
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }


  start_level_inputs_.clear();

  assert(start_level_ >= 0);

  // Pick the largest file in this level that is not already
  // being compacted
  const std::vector<int>& file_size =
      vstorage_->FilesByCompactionPri(start_level_);
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);
  // printf("---------------------------------------\n");
  // printf("PickFileToCompact(%lu) :: start level %d size %lu\n",ioptions_.compaction_scheme,start_level_,file_size.size());
  unsigned int cmp_idx;
  


// MAX
  uint64_t max_score = 0;
  uint64_t max_file_size_score = 0;
  uint64_t max_invalidation_ratio_score = 0;

  uint64_t file_size_score;
  uint64_t invalidation_ratio_score;
  uint64_t score;

  unsigned int max_cmp_idx = vstorage_->NextCompactionIndex(start_level_);
  int max_index = 0;
  // bool trial_move = true;
  std::vector<FileMetaData*> max_file_candiates;
  uint64_t max_candidate_compensate_size = 0;
  uint64_t normalized_candidate_compensate_size;

  uint64_t candidate_size;


  uint64_t zns_free_percent=100;
  // GetFileSystem()->GetFreeSpace(std::string(),IOOptions(),&zns_free_space,&zns_free_percent,nullptr);
  ioptions_.fs->GetFreeSpace(std::string(),IOOptions(),nullptr,&zns_free_percent,nullptr);
  // mutable_cf_options_.level0_slowdown_writes_trigger;
  // mutable_cf_options_.
  max_file_candiates.clear();
  if(ioptions_.compaction_scheme==BASELINE_COMPACTION
    // || zns_free_percent>=25
    ){
    goto baseline;
  }
  for(cmp_idx= vstorage_->NextCompactionIndex(start_level_);cmp_idx<file_size.size();cmp_idx++){

    std::vector<uint64_t> file_candidates;
    file_candidates.clear();

    int index = file_size[cmp_idx];
    auto* candidate = level_files[index];
    CompactionInputFiles start_i;
    std::vector<FileMetaData*> files;
    // uint64_t default_SSTABLE_size= (1<<26);
    start_i.clear();
    files.clear();



    // if l0 -> l1 compaction

    // else (l1.... -> ln)


    if(candidate->being_compacted){
      continue;
    }
    start_i.files.push_back(candidate);
    start_i.level=start_level_;
    if(!compaction_picker_->ExpandInputsToCleanCut(cf_name_,vstorage_,&start_i) 
        ||compaction_picker_->FilesRangeOverlapWithCompaction({start_i},output_level_) ) 
    {
      continue;
    }

    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_i, &smallest, &largest);


    CompactionInputFiles output_i;
    output_i.level=output_level_;
    vstorage_->GetOverlappingInputs(output_level_,&smallest,&largest,&output_i.files);

    if(!output_i.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_,vstorage_,&output_i)){
          continue;
    }
    
    files=start_i.files;
    vstorage_->GetOverlappingInputs(start_level_,&smallest,&largest,&files);
    for(auto f : files){
      file_candidates.push_back(f->fd.GetNumber());
    }
    for(auto f : output_i.files){
      file_candidates.push_back(f->fd.GetNumber());
    }

    // printf("[%u,%d] start fno : %lu.sst\n",cmp_idx,index,candidate->fd.GetNumber());

    if(ioptions_.compaction_scheme==BASELINE_COMPACTION ||
          file_candidates.size()==1 
         || output_level_<=1
      ){
      // trial move or baseline, return here
      // start_level_inputs_.files.push_back(candidate);
      // printf("It is %s, return here\n",ioptions_.compaction_scheme==BASELINE_COMPACTION ? "baseline compaction" : "trial move");
      start_level_inputs_.clear();
      start_level_inputs_.files=start_i.files;
      start_level_inputs_.level = start_level_;
      // if(output_level_<=1){
      //   vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);
      // }else{
      vstorage_->ResetNextCompactionIndex(start_level_);  
      // }
      // vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);
      base_index_ = index;
      return start_level_inputs_.size() > 0;
    }
    // should be different, original logic not using GetOverlappingInputs at start level.



    invalidation_ratio_score=ioptions_.fs->GetMaxInvalidateCompactionScore(file_candidates,&candidate_size);

    if(max_candidate_compensate_size==0){
      max_candidate_compensate_size=candidate->compensated_file_size;
    }

    normalized_candidate_compensate_size=(candidate->compensated_file_size*100)/max_candidate_compensate_size;
    (void)(max_candidate_compensate_size);
    (void)(normalized_candidate_compensate_size);
    (void)(max_invalidation_ratio_score);
    // file_size_score=(normalized_candidate_compensate_size*zns_free_percent)/100;

    file_size_score=(normalized_candidate_compensate_size*std::log(zns_free_percent)*100)/std::log(100);


    score = invalidation_ratio_score + file_size_score*2;


    // printf("[start] ");
    // for(auto s : files){
    //   printf("%lu.sst ",s->fd.GetNumber());
    // }
    // printf("\n");

    // printf("[out] ");
    // for(auto o : output_i.files){
    //   printf("%lu.sst ",o->fd.GetNumber());
    // }
    // printf("\n");
    // printf("score: %lf / %lu =  %lf\n",(score),(candidate_size>>20),((double)score/(double)(candidate_size>>20)));



    // if(candidate_size>>20 == 0 )
    // candidate_size = candidate_size>>20 == 0 ? 1 : candidate_size>>20;
    
    // candidate_size=candidate->compensated_file_size;
    

    // candidate_size=candidate->compensated_file_size/default_SSTABLE_size;

    // if(candidate_size>max_candidate_compensate_size ||
    //    (candidate_size==max_candidate_compensate_size && score>max_score) )
    if(score>max_score ||
        (score == max_score && file_size_score>max_file_size_score )
    // ||     (score==max_score && max_candidate_compensate_size>candidate_size) 
        // (score==max_score && candidate->compensated_file_size>max_candidate_compensate_size) 
    )
    {
      max_file_candiates.clear();
      max_file_candiates=start_i.files;
      
      max_cmp_idx=cmp_idx;
      max_index=index;
      // max_candidate_compensate_size=candidate->compensated_file_size;
      // max_candidate_compensate_size=candidate_size;
      max_file_size_score=file_size_score;
      max_invalidation_ratio_score=invalidation_ratio_score;
      max_score=score;
      // break;
    }



    

  }
  // if(max_score<zns_free_percent){
  //   goto baseline;
  // }
  start_level_inputs_.clear();
  start_level_inputs_.files=max_file_candiates;
  start_level_inputs_.level = start_level_;
  (void)(max_cmp_idx);
  // vstorage_->SetNextCompactionIndex(start_level_, max_cmp_idx);
  vstorage_->ResetNextCompactionIndex(start_level_);  
  base_index_=max_index;

  // if(start_level_inputs_.size()){
  //   printf("-----------------SELECTED--------------\n");
  //   printf("[%u,%d] start fno : %lu.sst\n",max_cmp_idx,max_index,max_file_candiates[0]->fd.GetNumber());
  //   printf("score : %lf\n",max_score);
  //   printf("-----------------END-------------------\n");
  // }
  return start_level_inputs_.size() > 0;
baseline:
//////////////////////////////////////////////////////////////////////////////////////////
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_size.size(); cmp_idx++) {
    int index = file_size[cmp_idx];
    auto* f = level_files[index];
    // f->fd.GetNumber();
    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      continue;
    }

    start_level_inputs_.files.push_back(f);
    start_level_inputs_.level = start_level_;
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_)) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();
      continue;
    }

    // Now that input level is fully expanded, we check whether any output files
    // are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      start_level_inputs_.clear();
      continue;
    }
    // printf("-----------------SELECTED--------------\n");
    // printf("[%u,%d] start fno : %lu.sst\n",cmp_idx,index,f->fd.GetNumber());

    // printf("[start] ");
    // for(auto s : start_level_inputs_.files){
    //   printf("%lu.sst ",s->fd.GetNumber());
    // }
    // printf("\n");
    // printf("[out] ");
    // for(auto o : output_level_inputs.files){
    //   printf("%lu.sst ",o->fd.GetNumber());
    // }
    // printf("\n");
    // printf("-----------------END-------------------\n");
    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);

  return start_level_inputs_.size() > 0;
}

bool LevelCompactionBuilder::PickIntraL0Compaction() {
  start_level_inputs_.clear();
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.
    return false;
  }
  return FindIntraL0Compaction(level_files, kMinFilesForIntraL0Compaction,
                               std::numeric_limits<uint64_t>::max(),
                               mutable_cf_options_.max_compaction_bytes,
                               &start_level_inputs_, earliest_mem_seqno_);
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, SequenceNumber earliest_mem_seqno) {
  LevelCompactionBuilder builder(cf_name, vstorage, earliest_mem_seqno, this,
                                 log_buffer, mutable_cf_options, ioptions_,
                                 mutable_db_options);
  // Compaction* ret= builder.PickCompaction();
  return builder.PickCompaction();
  // if(ret!=nullptr){
  //   printf("LevelCompactionPicker::PickCompaction :: %lu\n",ret->inputs()->size());
  //   for(auto ci : (*ret->inputs())){
  //     // for(auto ci2 : ci-)
  //     for(auto cif : ci.files){
  //       printf("%lu.sst ",cif->fd.GetNumber());
  //     }
  //     printf(" || ");
  //   }
  //   printf("\n");
  // }
  // return ret;
}
}  // namespace ROCKSDB_NAMESPACE
