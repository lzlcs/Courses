#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

using std::cerr;
using std::endl;

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  Context ctx;

  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::move(guard);

  page_id_t cur_id = header_page->root_page_id_;

  // cerr << "rootid : " << cur_id << endl;

  while (true) {
    // std::cerr << "find id " << cur_id << std::endl;
    WritePageGuard guard = bpm_->FetchPageWrite(cur_id);
    auto cur_page = guard.AsMut<InternalPage>();

    if (cur_page->IsLeafPage()) {
      break;
    }

    ctx.write_set_.push_back(std::move(guard));

    int size = cur_page->GetSize();

    int l = 1;
    int r = size - 1;
    while (l < r) {
      int mid = (l + r + 1) / 2;
      if (comparator_(cur_page->KeyAt(mid), key) <= 0) {
        l = mid;
      } else {
        r = mid - 1;
      }
    }

    if (comparator_(cur_page->KeyAt(l), key) > 0) {
      l = 0;
    }

    cur_id = cur_page->ValueAt(l);
  }

  WritePageGuard cur_guard = bpm_->FetchPageWrite(cur_id);
  auto cur_page = cur_guard.AsMut<LeafPage>();

  // cerr << cur_guard.PageId() << endl;

  int size = cur_page->GetSize();
  for (int i = 0; i < size; i++) {
    // cerr << cur_page->KeyAt(i) << ' ';
    if (comparator_(cur_page->KeyAt(i), key) == 0) {
      result->push_back(cur_page->ValueAt(i));
      return true;
    }
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(BPlusTreePage *page, const KeyType &key, const ValueType &value, Transaction *txn)
    -> bool {
  auto *leaf = reinterpret_cast<LeafPage *>(page);
  int size = leaf->GetSize();
  int idx = 0;

  // find the position to insert
  for (idx = 0; idx < size; idx++) {
    auto res = comparator_(key, leaf->KeyAt(idx));
    if (res < 0) {
      break;
    }
    if (res == 0) {
      return false;
    }
  }

  // copy data
  leaf->IncreaseSize(1);
  for (int i = size; i > idx; i--) {
    leaf->CopyKV(i - 1, i);
  }

  // insert data
  leaf->SetKV(idx, key, value);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInInternal(BPlusTreePage *page, const page_id_t l, const KeyType &key, const page_id_t r)
    -> bool {
  auto *internal = reinterpret_cast<InternalPage *>(page);
  int size = internal->GetSize();
  int idx = 0;
  for (int i = 0; i < size; i++) {
    if (l == internal->ValueAt(i)) {
      idx = i + 1;
      break;
    }
  }

  internal->IncreaseSize(1);
  for (int i = size; i > idx; i--) {
    internal->CopyKV(i - 1, i);
  }

  internal->SetKV(idx, key, r);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(Context *ctx, const page_id_t l, const KeyType &key, const page_id_t r) {
  if (ctx->IsRootPage(l)) {
    page_id_t new_root = -1;
    BasicPageGuard guard = bpm_->NewPageGuarded(&new_root);
    assert(new_root != -1);
    auto new_page = guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);
    // std::cerr << "new root :" << new_root << std::endl;

    new_page->SetValueAt(0, l);
    new_page->SetKV(1, key, r);
    new_page->IncreaseSize(2);

    auto p = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
    p->root_page_id_ = new_root;
    ctx->header_page_ = std::nullopt;
    return;
  }

  auto guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto parent_page = guard.AsMut<InternalPage>();
  auto parent_id = guard.PageId();

  // std::cerr << "parent id" << parent_id  << ' ' << parent_page->GetSize() << std::endl;

  if (parent_page->GetSize() < parent_page->GetMaxSize()) {
    InsertInInternal(parent_page, l, key, r);
  } else {
    page_id_t tmp_id = -1;
    auto tmp_guard = bpm_->NewPageGuarded(&tmp_id);
    assert(tmp_id != -1);
    auto tmp_page = tmp_guard.AsMut<InternalPage>();
    tmp_page->Init(internal_max_size_ + 1);

    tmp_page->SetSize(parent_page->GetSize());
    for (int i = 0; i < parent_page->GetSize(); i++) {
      tmp_page->SetKV(i, parent_page->KeyAt(i), parent_page->ValueAt(i));
    }
    InsertInInternal(tmp_page, l, key, r);

    page_id_t new_id = -1;
    auto new_guard = bpm_->NewPageGuarded(&new_id);
    assert(new_id != -1);
    auto new_page = new_guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);

    int tmp = new_page->GetMinSize();
    // cerr << "tmpsize: " << tmp_page->GetSize() << endl;
    parent_page->SetSize(tmp);
    for (int i = 0; i < tmp; i++) {
      parent_page->SetKV(i, tmp_page->KeyAt(i), tmp_page->ValueAt(i));
    }

    new_page->SetSize(internal_max_size_ - tmp + 1);
    for (int i = tmp + 1; i < tmp_page->GetSize(); i++) {
      // std::cerr << i - tmp << std::endl;
      new_page->SetKV(i - tmp, tmp_page->KeyAt(i), tmp_page->ValueAt(i));
    }
    new_page->SetValueAt(0, tmp_page->ValueAt(tmp));
    auto up_key = tmp_page->KeyAt(tmp);

    // cerr << "before: " << bpm_->free_list_.size() << ' ' << bpm_->replacer_->Size() << endl;
    tmp_guard.Drop();
    // cerr << "after : " << bpm_->free_list_.size() << ' ' << bpm_->replacer_->Size() << endl;

    InsertInParent(ctx, parent_id, up_key, new_id);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  Context ctx;

  // std::cerr << leaf_max_size_ << ' ' << internal_max_size_ << std::endl;
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::move(guard);

  page_id_t cur_id = header_page->root_page_id_;
  // std::cerr << key << std::endl;
  // std::cerr << cur_id << ' ' << key << std::endl;

  // find leaf to insert
  if (cur_id == INVALID_PAGE_ID) {
    // empty tree

    BasicPageGuard guard = bpm_->NewPageGuarded(&cur_id);
    assert(cur_id != -1);
    auto page = guard.AsMut<LeafPage>();
    page->Init(leaf_max_size_);
    header_page->root_page_id_ = cur_id;
    ctx.root_page_id_ = cur_id;

  } else {
    // std::cerr << "find " << std::endl;
    while (true) {
      // std::cerr << "find id " << cur_id << std::endl;
      WritePageGuard guard = bpm_->FetchPageWrite(cur_id);
      auto cur_page = guard.AsMut<InternalPage>();

      if (cur_page->IsLeafPage()) {
        break;
      }

      ctx.write_set_.push_back(std::move(guard));

      int size = cur_page->GetSize();
      for (int i = 1; i < size; i++) {
        // std::cerr << cur_page->ValueAt(i) << std::endl;
        auto res = comparator_(key, cur_page->KeyAt(i));
        if (res == 0) {
          return false;
        }
        if (res < 0) {
          cur_id = cur_page->ValueAt(i - 1);
          break;
        }
        if (i == size - 1) {
          cur_id = cur_page->ValueAt(i);
          break;
        }
      }
    }

    // std::cerr << "find end" << std::endl;
  }

  auto cur_guard = bpm_->FetchPageBasic(cur_id);
  auto cur_page = cur_guard.AsMut<LeafPage>();

  // std::cerr << "leafid: " << cur_guard.PageId() << std::endl;

  // normal insert
  // cerr << "cursize: " << cur_page->GetSize() << endl;
  if (cur_page->GetSize() + 1 < cur_page->GetMaxSize()) {
    // std::cerr << "normal insert: " << key << std::endl;
    return InsertInLeaf(cur_page, key, value, txn);
  }

  page_id_t tmp_id;
  auto tmp_guard = bpm_->NewPageGuarded(&tmp_id);
  assert(tmp_id != -1);
  auto tmp_page = tmp_guard.AsMut<LeafPage>();
  tmp_page->Init(leaf_max_size_ + 1);
  tmp_page->SetSize(cur_page->GetSize());
  for (int i = 0; i < tmp_page->GetSize(); i++) {
    tmp_page->SetKV(i, cur_page->KeyAt(i), cur_page->ValueAt(i));
  }
  if (!InsertInLeaf(tmp_page, key, value, txn)) {
    return false;
  }
  // split
  page_id_t new_id;
  auto new_guard = bpm_->NewPageGuarded(&new_id);
  assert(new_id != -1);
  auto new_page = new_guard.AsMut<LeafPage>();
  new_page->Init(leaf_max_size_);

  int tmp = new_page->GetMinSize();

  cur_page->SetSize(tmp);
  for (int i = 0; i < tmp; i++) {
    cur_page->SetKV(i, tmp_page->KeyAt(i), tmp_page->ValueAt(i));
  }
  new_page->SetSize(leaf_max_size_ - tmp);
  for (int i = tmp; i < tmp_page->GetSize(); i++) {
    new_page->SetKV(i - tmp, tmp_page->KeyAt(i), tmp_page->ValueAt(i));
  }
  tmp_guard.Drop();

  new_page->SetNextPageId(cur_page->GetNextPageId());
  cur_page->SetNextPageId(new_id);
  InsertInParent(&ctx, cur_id, new_page->KeyAt(0), new_id);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveKV(Context *ctx, BPlusTreePage *page, const KeyType &key) -> bool {
  if (page->IsLeafPage()) {
    auto cur_page = reinterpret_cast<LeafPage *>(page);
    int idx = 0;
    bool flag = true;
    for (int i = 0; i < cur_page->GetSize(); i++) {
      if (comparator_(key, cur_page->KeyAt(i)) == 0) {
        idx = i;
        flag = false;
        break;
      }
    }
    if (flag) {
      return false;
    }
    cur_page->IncreaseSize(-1);
    for (int i = idx; i < cur_page->GetSize(); i++) {
      cur_page->CopyKV(i + 1, i);
    }
  } else {
    auto cur_page = reinterpret_cast<InternalPage *>(page);
    int idx = 0;
    bool flag = true;
    for (int i = 1; i < cur_page->GetSize(); i++) {
      if (comparator_(key, cur_page->KeyAt(i)) == 0) {
        idx = i;
        flag = false;
        break;
      }
    }
    if (flag) {
      return false;
    }

    cur_page->IncreaseSize(-1);
    for (int i = idx; i < cur_page->GetSize(); i++) {
      cur_page->CopyKV(i + 1, i);
    }
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromLeft(Context *ctx, BPlusTreePage *parent, BPlusTreePage *dst, BPlusTreePage *src,
                                    const KeyType &key_parent, int idx) {
  // return;
  if (dst->IsLeafPage()) {
    auto *src_page = reinterpret_cast<LeafPage *>(src);
    KeyType key = src_page->KeyAt(src_page->GetSize() - 1);
    ValueType value = src_page->ValueAt(src_page->GetSize() - 1);
    src_page->IncreaseSize(-1);

    auto dst_page = reinterpret_cast<LeafPage *>(dst);

    for (int i = dst_page->GetSize(); i > 0; i--) {
      dst_page->CopyKV(i - 1, i);
    }
    dst_page->SetValueAt(0, value);
    dst_page->SetKeyAt(0, key);
    dst_page->IncreaseSize(1);

    auto parent_page = reinterpret_cast<InternalPage *>(parent);
    parent_page->SetKeyAt(idx, key);

  } else {
    auto *src_page = reinterpret_cast<InternalPage *>(src);
    KeyType key = src_page->KeyAt(src_page->GetSize() - 1);
    page_id_t value = src_page->ValueAt(src_page->GetSize() - 1);
    src_page->IncreaseSize(-1);

    auto dst_page = reinterpret_cast<InternalPage *>(dst);
    for (int i = dst_page->GetSize(); i > 0; i--) {
      dst_page->CopyKV(i - 1, i);
    }
    dst_page->SetValueAt(0, value);
    dst_page->SetKeyAt(1, key_parent);
    dst_page->IncreaseSize(1);

    auto parent_page = reinterpret_cast<InternalPage *>(parent);
    parent_page->SetKeyAt(idx, key);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromRight(Context *ctx, BPlusTreePage *parent, BPlusTreePage *dst, BPlusTreePage *src,
                                     const KeyType &key_parent, int idx) {
  if (dst->IsLeafPage()) {
    auto *src_page = reinterpret_cast<LeafPage *>(src);
    KeyType key = src_page->KeyAt(0);
    ValueType value = src_page->ValueAt(0);
    for (int i = 1; i < src_page->GetSize(); i++) {
      src_page->CopyKV(i, i - 1);
    }
    src_page->IncreaseSize(-1);

    auto dst_page = reinterpret_cast<LeafPage *>(dst);
    dst_page->SetKeyAt(dst_page->GetSize(), key);
    dst_page->SetValueAt(dst_page->GetSize(), value);
    dst_page->IncreaseSize(1);

    auto parent_page = reinterpret_cast<InternalPage *>(parent);
    parent_page->SetKeyAt(idx, src_page->KeyAt(0));

  } else {
    auto *src_page = reinterpret_cast<InternalPage *>(src);
    KeyType key = src_page->KeyAt(1);
    auto value = src_page->ValueAt(0);

    // cerr << ">>>>>>" << endl;
    for (int i = 1; i < src_page->GetSize(); i++) {
      src_page->CopyKV(i, i - 1);
    }
    src_page->IncreaseSize(-1);

    auto *parent_page = reinterpret_cast<InternalPage *>(parent);
    parent_page->SetKeyAt(idx, key);

    auto dst_page = reinterpret_cast<InternalPage *>(dst);
    dst_page->SetKeyAt(dst_page->GetSize(), key_parent);
    dst_page->SetValueAt(dst_page->GetSize(), value);
    dst_page->IncreaseSize(1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(Context *ctx, BPlusTreePage *l, BPlusTreePage *r, const KeyType &key) {
  if (l->IsLeafPage()) {
    auto *l_node = reinterpret_cast<LeafPage *>(l);
    auto *r_node = reinterpret_cast<LeafPage *>(r);

    for (int i = 0; i < r_node->GetSize(); i++) {
      l_node->SetKV(i + l_node->GetSize(), r_node->KeyAt(i), r_node->ValueAt(i));
    }
    l_node->SetSize(l_node->GetSize() + r_node->GetSize());
    l_node->SetNextPageId(r_node->GetNextPageId());

  } else {
    auto *l_node = reinterpret_cast<InternalPage *>(l);
    auto *r_node = reinterpret_cast<InternalPage *>(r);

    r_node->SetKeyAt(0, key);
    for (int i = 0; i < r_node->GetSize(); i++) {
      l_node->SetKV(i + l_node->GetSize(), r_node->KeyAt(i), r_node->ValueAt(i));
    }
    l_node->SetSize(l_node->GetSize() + r_node->GetSize());
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(Context *ctx, BPlusTreePage *cur, const KeyType &key) {
  auto cur_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto *cur_page = reinterpret_cast<BPlusTreePage *>(cur);
  auto cur_id = cur_guard.PageId();

  if (!RemoveKV(ctx, cur_page, key)) {
    return;
  }

  if (ctx->IsRootPage(cur_id)) {
    if (!cur_page->IsLeafPage()) {
      auto *internal_page = reinterpret_cast<InternalPage *>(cur_page);
      if (internal_page->GetSize() == 1) {
        page_id_t new_root = internal_page->ValueAt(0);

        auto p = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
        p->root_page_id_ = new_root;
      }
    }
    return;
  }
  if (cur_page->GetSize() < cur_page->GetMinSize()) {
    auto parent_guard = std::move(ctx->write_set_.back());
    ctx->write_set_.pop_back();
    auto parent_page = parent_guard.AsMut<InternalPage>();

    KeyType tmp_key;
    int idx = -1;
    bool flag_predecessor = true;
    for (int i = 1; i < parent_page->GetSize(); i++) {
      if (parent_page->ValueAt(i) == cur_id) {
        idx = i - 1;
        tmp_key = parent_page->KeyAt(i);
        flag_predecessor = false;
        break;
      }
    }
    if (flag_predecessor) {
      tmp_key = parent_page->KeyAt(1);
      idx = 1;
    }
    // cerr << idx << ' ' << flag_predecessor << endl;

    int brother_id = parent_page->ValueAt(idx);

    // cerr << "BROTHER ID: " << cur_id << ' ' << brother_id << endl;
    // cerr << "REMOVE ENTRY: " << key << endl;
    // cerr << "TMPKEY" << tmp_key << endl;
    auto brother_guard = bpm_->FetchPageWrite(brother_id);
    auto brother_page = brother_guard.AsMut<LeafPage>();

    assert(brother_page->IsLeafPage() == cur_page->IsLeafPage());
    // cerr << "SUMSIZE: " << brother_page->GetSize() << ' ' << cur_page->GetSize() << endl;
    int total_size = brother_page->GetSize() + cur_page->GetSize();
    if (!cur_page->IsLeafPage()) {
      total_size -= 1;
    }
    if (total_size < cur_page->GetMaxSize()) {
      // cerr << ">" << endl;
      // if (comparator_(key, 9) == 0) exit(0);

      if (flag_predecessor) {
        Merge(ctx, cur_page, brother_page, tmp_key);
        brother_guard.Drop();
      } else {
        Merge(ctx, brother_page, cur_page, tmp_key);
        cur_guard.Drop();
      }
      ctx->write_set_.push_back(std::move(parent_guard));

      RemoveEntry(ctx, parent_page, tmp_key);
    } else {
      if (flag_predecessor) {
        BorrowFromRight(ctx, parent_page, cur_page, brother_page, tmp_key, 1);
      } else {
        BorrowFromLeft(ctx, parent_page, cur_page, brother_page, tmp_key, idx + 1);
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  Context ctx;

  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::move(guard);

  page_id_t cur_id = header_page->root_page_id_;

  if (cur_id == INVALID_PAGE_ID) {
    return;
  }

  while (true) {
    WritePageGuard guard = bpm_->FetchPageWrite(cur_id);
    auto cur_page = guard.AsMut<InternalPage>();

    if (cur_page->IsLeafPage()) {
      break;
    }

    ctx.write_set_.push_back(std::move(guard));

    int size = cur_page->GetSize();
    int idx = 0;
    for (int i = 1; i < size; i++) {
      if (comparator_(key, cur_page->KeyAt(i)) < 0) {
        break;
      }
      idx = i;
    }
    cur_id = cur_page->ValueAt(idx);
  }

  // cerr << "DELETE KEY: " << key << endl;

  auto leaf_guard = bpm_->FetchPageWrite(cur_id);
  auto cur_page = leaf_guard.AsMut<LeafPage>();
  ctx.write_set_.push_back(std::move(leaf_guard));
  RemoveEntry(&ctx, cur_page, key);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;

  // std::cerr << leaf_max_size_ << ' ' << internal_max_size_ << std::endl;
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  page_id_t cur_id = header_page->root_page_id_;

  while (true) {
    ReadPageGuard guard = bpm_->FetchPageRead(cur_id);
    auto cur_page = guard.As<InternalPage>();
    if (cur_page->IsLeafPage()) {
      return {bpm_, guard, 0};
    }
    ctx.read_set_.push_back(std::move(guard));
    cur_id = cur_page->ValueAt(0);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;

  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  page_id_t cur_id = header_page->root_page_id_;

  // cerr << "rootid : " << cur_id << endl;

  while (true) {
    // std::cerr << "find id " << cur_id << std::endl;
    WritePageGuard guard = bpm_->FetchPageWrite(cur_id);
    auto cur_page = guard.As<InternalPage>();

    if (cur_page->IsLeafPage()) {
      break;
    }

    ctx.write_set_.push_back(std::move(guard));

    int size = cur_page->GetSize();

    int l = 1;
    int r = size - 1;
    while (l < r) {
      int mid = (l + r + 1) / 2;
      if (comparator_(cur_page->KeyAt(mid), key) <= 0) {
        l = mid;
      } else {
        r = mid - 1;
      }
    }

    if (comparator_(cur_page->KeyAt(l), key) > 0) {
      l = 0;
    }

    cur_id = cur_page->ValueAt(l);
  }

  ReadPageGuard cur_guard = bpm_->FetchPageRead(cur_id);
  auto cur_page = cur_guard.As<LeafPage>();

  // cerr << cur_guard.PageId() << endl;

  int size = cur_page->GetSize();
  int l = 0;
  int r = size - 1;
  while (l < r) {
    int mid = (l + r) / 2;
    if (comparator_(cur_page->KeyAt(mid), key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }

  return {bpm_, cur_guard, l};
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  ReadPageGuard tmp(nullptr, nullptr);
  return {nullptr, tmp, 0};
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
