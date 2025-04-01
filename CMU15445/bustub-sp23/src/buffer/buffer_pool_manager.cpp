//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id = -1;
  std::lock_guard<std::mutex> lock(latch_);

  if (free_list_.empty() && replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    page_table_.erase(pages_[frame_id].page_id_);
  }

  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }

  pages_[frame_id].page_id_ = *page_id = AllocatePage();
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory();

  page_table_[*page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return pages_ + frame_id;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;
  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return pages_ + frame_id;
  }

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
    page_table_.erase(pages_[frame_id].page_id_);
  }

  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }

  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *cur_page = pages_ + frame_id;

  if (cur_page->GetPinCount() == 0) {
    return false;
  }

  if (--cur_page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  cur_page->is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *cur_page = pages_ + frame_id;

  disk_manager_->WritePage(cur_page->page_id_, cur_page->data_);
  cur_page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &[k, v] : page_table_) {
    FlushPage(k);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *cur_page = pages_ + frame_id;

  if (cur_page->pin_count_ != 0) {
    return false;
  }
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  replacer_->Remove(frame_id);

  cur_page->page_id_ = INVALID_PAGE_ID;
  cur_page->pin_count_ = 0;
  cur_page->is_dirty_ = false;
  cur_page->ResetMemory();

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->rwlatch_.RLock();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->rwlatch_.WLock();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}
}  // namespace bustub
