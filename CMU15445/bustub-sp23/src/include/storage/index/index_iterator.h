//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *b, ReadPageGuard &g, int idx);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return (page_id_ == itr.page_id_) && (idx_ == itr.idx_); }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  BufferPoolManager *bpm_;
  ReadPageGuard guard_;
  page_id_t page_id_{INVALID_PAGE_ID};
  int idx_{0};
  // add your own private member variables here
};

}  // namespace bustub
