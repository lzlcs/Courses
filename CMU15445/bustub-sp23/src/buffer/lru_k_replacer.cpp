//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {
LRUKNode::LRUKNode(size_t k, frame_id_t fid, bool is_evictable) {
  this->k_ = k;
  this->fid_ = fid;
  this->is_evictable_ = is_evictable;
  history_.push_back(INT32_MAX);
  kth_pos_ = history_.begin();
}

void LRUKNode::AddHistory(size_t timestamp) {
  history_.push_back(timestamp);
  if (history_.size() > k_) {
    kth_pos_++;
  }
}

auto LRUKNode::SetEvictable(bool to_val) -> int {
  int res = 0;
  if (is_evictable_ && !to_val) {
    res = -1;
  }
  if (!is_evictable_ && to_val) {
    res = 1;
  }
  is_evictable_ = to_val;
  return res;
}

auto LRUKNode::GetDistance() -> int {
  auto tmp = kth_pos_;
  if (*kth_pos_ == INT32_MAX) {
    return *(++tmp) - *kth_pos_;
  }
  return *kth_pos_;
}

auto LRUKNode::IsEvictable() -> bool { return this->is_evictable_; }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->curr_size_ == 0) {
    return false;
  }

  int min_dis = INT32_MAX;
  for (auto &[id, node] : this->node_store_) {
    if (node.IsEvictable()) {
      int cur_dis = node.GetDistance();

      if (cur_dis < min_dis) {
        min_dis = cur_dis;
        *frame_id = id;
      }
    }
  }
  node_store_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(this->latch_);
  BUSTUB_ASSERT((size_t)frame_id <= this->replacer_size_, "The frame_id is invalid!");

  if (node_store_.count(frame_id) == 0) {
    this->node_store_[frame_id] = LRUKNode(this->k_, frame_id, true);
    curr_size_++;
  }

  node_store_[frame_id].AddHistory(++current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(this->latch_);
  BUSTUB_ASSERT((size_t)frame_id <= this->replacer_size_, "The frame_id is invalid!");
  if (node_store_.count(frame_id) != 0) {
    curr_size_ += node_store_[frame_id].SetEvictable(set_evictable);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(this->latch_);
  if (node_store_.count(frame_id) != 0) {
    auto tmp_node = node_store_[frame_id];
    BUSTUB_ASSERT(tmp_node.IsEvictable(), "The node is not evictable!");
    node_store_.erase(frame_id);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return this->curr_size_; }

}  // namespace bustub
