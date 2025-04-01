/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *b, ReadPageGuard &g, int idx) {
  if (b != nullptr) {
    guard_ = std::move(g);
    page_id_ = guard_.PageId();
  } else {
    page_id_ = INVALID_PAGE_ID;
  }
  idx_ = idx;
  bpm_ = b;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto page = guard_.As<LeafPage>();
  return page->GetMappingKV(idx_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto page = guard_.As<LeafPage>();
  if (page->GetSize() - 1 == idx_) {
    page_id_ = page->GetNextPageId();
    if (page_id_ != INVALID_PAGE_ID) {
      ReadPageGuard new_guard = bpm_->FetchPageRead(page_id_);
      guard_ = std::move(new_guard);
    } else {
      guard_.Drop();
    }

    idx_ = 0;
  } else {
    idx_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
