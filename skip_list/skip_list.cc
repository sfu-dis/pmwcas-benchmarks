#include "skip_list.h"

#include "glog/raw_logging.h"

#ifdef PMEM
#if defined(MwCASSafeAlloc) && defined(EBRSafeAlloc)
static_assert(false, "Cannot have both reclamation mechanism");
#elif not defined(MwCASSafeAlloc) && not defined(EBRSafeAlloc)
#warning "Unsafe allocation: persistent memory leak is possible (PCAS only)!"
#endif
#endif

namespace pmwcas {

ISkipList::ISkipList(int sync, uint32_t max_height)
    : sync_method_(sync), max_height_(max_height) {
  RAW_CHECK(sync_method_ == kSyncMwCAS || sync_method_ == kSyncCAS ||
                sync_method_ == kSyncPCAS,
            "invalid sync method.");
  // Preallocate a bunch of sentinel nodes; note however head_ points to the
  // sentinel node at the real current height, instead of max_height_.

  // FIXME(shiges): Ideally this should be wrapped in a PMDK tx, but
  // I don't really care how to do recovery if it crashes here...
  // Just make sure everything is on PM
  Allocator::Get()->AllocateAligned(
      (void**)&head_, sizeof(SkipListNode) * max_height, kCacheLineSize);
  char* head_memory = (char*)head_;
  memset(head_memory, 0, sizeof(SkipListNode) * max_height);

  Allocator::Get()->AllocateAligned(
      (void**)&tail_, sizeof(SkipListNode) * max_height, kCacheLineSize);
  char* tail_memory = (char*)tail_;
  memset(tail_memory, 0, sizeof(SkipListNode) * max_height);

  Slice dummy_key;
  tail_ = new (tail_memory) SkipListNode(dummy_key, 0);
  head_ = new (head_memory) SkipListNode(dummy_key, 0);
  head_->prev = head_->lower = tail_->lower = head_->upper = tail_->upper =
      nullptr;
  head_->level = 1;
  tail_->level = 1;
  head_->next = tail_;
  tail_->next = nullptr;
  tail_->prev = head_;
  RAW_CHECK(head_->prev == nullptr, "invalid prev pointer in head node");

  SkipListNode* head_lower = head_;
  SkipListNode* tail_lower = tail_;
  RAW_CHECK(head_->prev == nullptr, "invalid prev pointer in head node");
  for (uint32_t i = 2; i <= max_height_; ++i) {
    RAW_CHECK(head_->prev == nullptr, "invalid prev pointer in head node");
    auto* head_n = head_lower->upper =
        (SkipListNode*)(head_memory + sizeof(SkipListNode) * (i - 1));
    new (head_n) SkipListNode(dummy_key, 0);
    SkipListNode* tail_n =
        (SkipListNode*)(tail_memory + sizeof(SkipListNode) * (i - 1));
    new (tail_n) SkipListNode(dummy_key, 0);
    head_n->level = tail_n->level = i;

    head_n->lower = head_lower;
    tail_n->lower = tail_lower;

    head_n->next = tail_n;
    tail_n->prev = head_n;

    tail_n->next = nullptr;
    head_n->prev = nullptr;

    head_lower = head_n;
    tail_lower = tail_n;
  }
  RAW_CHECK(tail_->next == nullptr, "invalid next pointer in tail node");

#ifdef PMEM
  NVRAM::Flush(sizeof(SkipListNode) * max_height, head_memory);
  NVRAM::Flush(sizeof(SkipListNode) * max_height, tail_memory);
#endif
}

void DSkipList::Print() {
  SkipListNode* curr_node = READ(head_);
  while (true) {
    SkipListNode* curr_head = curr_node;
    while (!curr_node->IsTail()) {
      if (curr_node->IsHead()) {
        printf("H");
      } else {
        printf(" %lx", *(uint64_t*)curr_node->key.data());
      }
      curr_node = GetNext(curr_node);
    }
    printf(" T\n");
    curr_node = curr_head->lower;
    if (curr_node->level == 1) {
      break;
    }
  }
}

Status DSkipList::Find(const Slice& key, SkipListNode** value_node) {
  DCHECK(GetEpoch()->IsProtected());
  auto* stack = GetTlsPathStack();
  stack->Reset();

  auto* curr_node = READ(head_);
  DCHECK((((uint64_t)curr_node->next & kNodeDeleted)) == 0);
retry:
  DCHECK(curr_node);
  DCHECK(curr_node->next);
  DCHECK(false == curr_node->IsTail());

  auto* right = GetNext(curr_node);
  if (right->IsTail()) {
  down:
    if (curr_node->level == 1) {
      DCHECK(curr_node->lower == nullptr);
      if (value_node) {
        *value_node =
            curr_node;  // value_node will point to the left of the target
      }
      return Status::NotFound();  // no luck
    } else {
      // Go down the tower to continue to the right
      stack->Push(curr_node);
      curr_node = curr_node->lower;
      DCHECK(curr_node);
      goto retry;
    }
  } else {
    int cmp = right->key.compare(key);
    if (cmp == 0) {
      // Found it, descend (if needed) to level 1 and return
      SkipListNode* result = right;
      if (GetSyncMethod() == kSyncCAS || GetSyncMethod() == kSyncPCAS) {
        while (*&result->level == SkipListNode::kInvalidLevel) { /** spin **/
        }
      }
      while (result->level > 1) {
        DCHECK(result->key.compare(key) == 0);
        result = result->lower;
      }
      DCHECK(result->level == 1);
      DCHECK(result->key.compare(key) == 0);
      if (value_node) {
        *value_node = result;  // value_node points to the found node in level 1
      }
      return Status::OK();
    } else if (cmp > 0) {
      goto down;  // Too large, go down
    } else {
      curr_node = right;
      goto retry;  // Too small, go right
    }
  }
}

/// GetNext code that follows the original paper; appears to be costly.
#if 0
SkipListNode* CASDSkipList::GetNext(SkipListNode* node) {
  DCHECK(GetEpoch()->IsProtected());
  while(true) {
    if(node->IsTail()) {
      return nullptr;
    }
    auto* next = READ(node->next);
    auto* my_next = next;
    next = CleanPtr(next);
    DCHECK(next);
    SkipListNode* next_next = READ(next->next);
    if(((uint64_t)next_next & kNodeDeleted) && ((uint64_t)my_next & kNodeDeleted) == 0) {
      // I'm not deleted, but my successor is, so mark in its prev deleted
      MarkNodePointer(&next->prev);
      // Now try to unlink the deleted next node
      uint64_t desired = (uint64_t)next_next & ~kNodeDeleted;
#ifdef PMEM
      auto desc = descriptor_pool_->AllocateDescriptor(nullptr, DSkipList::FreeNode);
      desc.AddEntry((uint64_t*)&node->next, (uint64_t)next, desired,
                     Descriptor::kRecycleOldOnSuccess);
      desc.MwCAS();
#else
      if(next == CompareExchange64Ptr(&node->next, (SkipListNode*)desired, next)) {
        Status s = GetGarbageList()->Push(next, DSkipList::FreeNode, nullptr);
        RAW_CHECK(s.ok(), "failed recycling node");
      }
#endif
      continue;
    }
    return next;
  }
}
#endif

SkipListNode* CASDSkipList::GetPrev(SkipListNode* node) {
  DCHECK(GetEpoch()->IsProtected());
  auto* list_head = READ(head_);
  // Descend to the right level
  while (list_head->level != node->level) {
    list_head = list_head->lower;
  }
  DCHECK(list_head);

  while (true) {
    if (node == list_head) {
      return nullptr;
    }
    auto* prev = CleanPtr(READ(node->prev));
    auto* prev_next = READ(prev->next);
    auto* curr_next = READ(node->next);
    if (prev_next == node && ((uint64_t)curr_next & kNodeDeleted) == 0) {
      return prev;  // node could be list_head
    } else if ((uint64_t)curr_next & kNodeDeleted) {
      node = GetNext(node);
    } else {
      prev = CorrectPrev(prev, node);
    }
  }
}

SkipListNode* CASDSkipList::CorrectPrev(SkipListNode* prev,
                                        SkipListNode* node) {
  DCHECK(GetEpoch()->IsProtected());
  DCHECK(((uint64_t)node & kNodeDeleted) == 0);
  SkipListNode* last_link = nullptr;
  while (true) {
    // DCHECK(!((SkipListNode*)((uint64_t)READ(prev) &
    // ~kNodeDeleted))->IsTail());
    auto* link1 = READ(node->prev);
    // DCHECK(!((SkipListNode*)((uint64_t)READ(link1) &
    // ~kNodeDeleted))->IsTail());
    if ((uint64_t)link1 & kNodeDeleted) {
      break;
    }

    SkipListNode* prev_cleared = READ(prev);
    auto* prev_next = READ(prev_cleared->next);
    DCHECK(prev_next);
    if ((uint64_t)prev_next & kNodeDeleted) {
      if (last_link) {
        // Mark the deleted bit on the prev field
        MarkNodePointer(&prev_cleared->prev);
        DCHECK((uint64_t)prev_cleared->prev & kNodeDeleted);
        uint64_t desired = ((uint64_t)prev_next & ~kNodeDeleted);
#if defined(PMEM) && defined(MwCASSafeAlloc)
        DCHECK(!((uint64_t)prev & kDirtyFlag));
        DCHECK(!((uint64_t)desired & kDirtyFlag));
        auto desc = descriptor_pool_->AllocateDescriptor();
        desc.AddEntry((uint64_t*)&last_link->next, (uint64_t)prev, desired,
                      Descriptor::kRecycleOldOnSuccess);
        desc.MwCAS();
#else
        if (prev == CompareExchange64Ptr(&last_link->next,
                                         (SkipListNode*)(desired | kDirtyFlag),
                                         prev)) {
#ifdef PMEM
          ReadPersist(&last_link->next);
#endif
          Status s = GetGarbageList()->Push(prev, DSkipList::FreeNode, nullptr);
          RAW_CHECK(s.ok(), "failed recycling node");
        }
#endif
        prev = last_link;
        last_link = nullptr;
        continue;
      }
      prev_next =
          (SkipListNode*)((uint64_t)READ(prev_cleared->prev) & ~kNodeDeleted);
      prev = prev_next;
      DCHECK(prev);
      continue;
    }

    DCHECK(((uint64_t)prev_next & kNodeDeleted) == 0);
    if (prev_next != node) {
      last_link = prev_cleared;
      prev = prev_next;
      continue;
    }
    SkipListNode* p =
        (SkipListNode*)(((uint64_t)prev & ~kNodeDeleted) | kDirtyFlag);
    if (link1 == CompareExchange64Ptr(&node->prev, p, link1)) {
#ifdef PMEM
      ReadPersist(&node->prev);
#endif
      auto* prev_cleared_prev = READ(prev_cleared->prev);
      if ((uint64_t)prev_cleared_prev & kNodeDeleted) {
        continue;
      }
      break;
    }
  }
  return prev;
}

Status CASDSkipList::Insert(const Slice& key, const Slice& value,
                            bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
retry:
  // Get the supposedly left node at level 1; a larger value might have been
  // inserted after it by the time I start to install a new key, so must make
  // sure later
  SkipListNode* left = nullptr;
  SkipListNode* right = nullptr;
  auto ret = Find(key, &left);
  if (ret == Status::OK()) {
    DCHECK(left);
    return Status::KeyAlreadyExists();
  } else {
    DCHECK(ret.IsNotFound() && left);
    if ((uint64_t)left->next & kNodeDeleted) {
      left = GetPrev(left);
    }
  }

  DCHECK(left);
  DCHECK(left->level == 1);
  DCHECK(!left->IsTail());

  right = GetNext(left);

  DCHECK(right);
  DCHECK(right->level == left->level);
  DCHECK(right->level == 1);

  // We're sure that the predecessor's key is smaller than mine; now make sure
  // the to-be-successor has a key larger than mine.
  if (!right->IsTail()) {
    int cmp = right->key.compare(key);
    if (cmp < 0) {
      goto retry;
    } else if (cmp == 0) {
      return Status::KeyAlreadyExists();
    }
  }

  // Insert a new node between left and right
#if defined(PMEM)
#if defined(MwCASSafeAlloc)
  auto desc = descriptor_pool_->AllocateDescriptor();
  uint32_t idx =
      desc.ReserveAndAddEntry((uint64_t*)&left->next, (uint64_t)right,
                              Descriptor::kRecycleNewOnFailure);
  Allocator::Get()->AllocateAligned(
      (void**)desc.GetNewValuePtr(idx),
      sizeof(SkipListNode) + key.size() + value.size(), kCacheLineSize);
  SkipListNode* node = (SkipListNode*)desc.GetNewValue(idx);
  new (node) SkipListNode(key, value.size());
#else
  void** addr = PMAllocHelper::Get()->GetTlsPtr();
  Allocator::Get()->AllocateAligned(
      addr, sizeof(SkipListNode) + key.size() + value.size(), kCacheLineSize);
  SkipListNode* node = new (*addr) SkipListNode(key, value.size());
#endif
#else
  SkipListNode* node = nullptr;
  Allocator::Get()->AllocateAligned(
      (void**)&node, sizeof(SkipListNode) + key.size() + value.size(),
      kCacheLineSize);
  new (node) SkipListNode(key, value.size());
#endif
  node->level = 1;
  node->prev = left;
  node->next = right;
  node->lower = nullptr;
  DCHECK(node->lower == nullptr);
  memcpy(node->GetPayload(), value.data(), value.size());
#ifdef PMEM
  NVRAM::Flush(node->Size(), node);
#endif
#if defined(PMEM) && defined(MwCASSafeAlloc)
  if (!desc.MwCAS()) {
    goto retry;
  }
#else
  if (CompareExchange64Ptr(&left->next, node, right) != right) {
    Allocator::Get()->FreeAligned(node);
    goto retry;
  }
#endif
  CorrectPrev(left, right);

  FinishInsert(node);
  return Status::OK();
}

void CASDSkipList::FinishInsert(SkipListNode* node) {
  // One RNG per thread
  thread_local RandomNumberGenerator height_rng{};
  DCHECK(GetEpoch()->IsProtected());
  // Continue with higher levels
  auto* stack = GetTlsPathStack();
  uint32_t original_height = stack->Size() + 1;

  uint32_t h = height_rng.Generate();
  uint32_t height = 1;
  while (h & 1) {
    height++;
    h >>= 1;
  }
  height = std::min<uint32_t>(height, max_height_);
  bool grow = height > original_height;

  DCHECK(node->level == 1);
  while (stack->Size() > 0) {
    auto* prev = stack->Pop();
    if (prev->level > height) {
      return;
    }
    DCHECK(prev->key.compare(node->key) < 0);
  retry:
    auto* next = GetNext(prev);
    if (!next->IsTail()) {
      int cmp = next->key.compare(node->key);
      if (cmp == 0) {
        return;
      } else if (cmp < 0) {
        prev = next;
        goto retry;
      }
    }
    // DCHECK(next->level == prev->level || next->IsTail());

#if defined(PMEM)
#if defined(MwCASSafeAlloc)
    auto desc = descriptor_pool_->AllocateDescriptor();
    uint32_t idx =
        desc.ReserveAndAddEntry((uint64_t*)&prev->next, (uint64_t)next,
                                Descriptor::kRecycleNewOnFailure);
    Allocator::Get()->AllocateAligned((void**)desc.GetNewValuePtr(idx),
                                      sizeof(SkipListNode) + node->key.size(),
                                      kCacheLineSize);
    SkipListNode* n = (SkipListNode*)desc.GetNewValue(idx);
    new (n) SkipListNode(node->key, 0);
#else
    void** addr = PMAllocHelper::Get()->GetTlsPtr();
    Allocator::Get()->AllocateAligned(
        addr, sizeof(SkipListNode) + node->key.size(), kCacheLineSize);
    SkipListNode* n = new (*addr) SkipListNode(node->key, 0);
#endif
#else
    SkipListNode* n = nullptr;
    Allocator::Get()->AllocateAligned(
        (void**)&n, sizeof(SkipListNode) + node->key.size(), kCacheLineSize);
    new (n) SkipListNode(node->key, 0);
#endif

    if (CompareExchange64Ptr(&node->upper,
                             (SkipListNode*)((uint64_t)n | kDirtyFlag),
                             (SkipListNode*)0)) {
      // Failed making the lower node to point to me, already deleted?
      // Add a dummy entry to just delete the memory allocated after Abort()
#if defined(PMEM) && defined(MwCASSafeAlloc)
      desc.Abort();
#else
      Allocator::Get()->FreeAligned(n);
#endif
      return;
    }
#ifdef PMEM
    ReadPersist(&node->upper);
#endif

    n->lower = node;
    DCHECK(n->key.compare(node->key) == 0);
    n->prev = prev;
    n->next = next;
#ifdef PMEM
    NVRAM::Flush(n->Size(), n);
#endif
#if defined(PMEM) && defined(MwCASSafeAlloc)
    if (!desc.MwCAS()) {
      *&n->level = SkipListNode::kLevelAbondoned;
      return;
    }
#else
    if (CompareExchange64Ptr(&prev->next, n, next) != next) {
      *&n->level = SkipListNode::kLevelAbondoned;
      return;
    }
#endif
    CorrectPrev(prev, next);
    // Make it really available - visible to someone trying to delete
    *&n->level = node->level + 1;
    DCHECK(n->level == node->level + 1);
    // DCHECK(n->level == next->level);  // XXX(tzwang): have to go as [level]
    // might not be available for read immediately if [next] is being installed
    // too
    node = n;
  }
  DCHECK(stack->Size() == 0);

  // Are we growing the list-wide height?
  if (grow) {
    // Multiple threads might be doing the same, but one will succeed
    auto* expected = READ(head_);
    if (expected->level < height) {
      auto* desired = (SkipListNode*)((uint64_t)expected->upper | kDirtyFlag);
      DCHECK(desired);
      if (CompareExchange64Ptr(&head_, desired, expected) == expected) {
#ifdef PMEM
        ReadPersist(&head_);
#endif
      }
      DCHECK(READ(head_)->level > original_height);
    }
  }
}

Status CASDSkipList::Delete(const Slice& key, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  SkipListNode* node = nullptr;
  auto ret = Find(key, &node);
  if (ret.IsNotFound()) {
    return ret;
  }
  DCHECK(key.compare(node->key) == 0);
  DCHECK(node);
  DCHECK(node->level == 1);

  // Delete from top to bottom
  SkipListNode* top = nullptr;
  SkipListNode* n = nullptr;
  n = node;
  while (!top) {
    while (*&n->level == SkipListNode::kInvalidLevel) { /** spin **/
    }
    if (*&n->level == SkipListNode::kLevelAbondoned) {
      // Definitely the top node
      top = n;
      break;
    } else {
      DCHECK(n == node || n->level > 1);
    }
    auto* upper = READ(n->upper);
    DCHECK(n->key.compare(key) == 0);
    if ((uint64_t)upper == kNodeDeleted) {
      top = n;
    } else {
      if (upper == nullptr) {
        // See if there's an on-going insert and if so try to end it
        if (nullptr ==
            CompareExchange64Ptr(&n->upper,
                                 (SkipListNode*)(kNodeDeleted | kDirtyFlag),
                                 (SkipListNode*)0)) {
#ifdef PMEM
          ReadPersist(&n->upper);
#endif
          top = n;
        } else {
          upper = READ(n->upper);
          if ((uint64_t)upper & kNodeDeleted) {
            // It was a concurrent deleter...
            top = n;
          } else {
            n = upper;
          }
        }
      } else {
        n = (SkipListNode*)((uint64_t)upper & ~kNodeDeleted);
      }
    }
  }
  if (!top) {
    top = node;
  }
  while (top) {
    DCHECK(top->key.compare(key) == 0);
    auto* to_delete = top;
    top = READ(top->lower);
    if (to_delete->level != SkipListNode::kLevelAbondoned) {
      DeleteNode(to_delete);
    }
  }
  return Status::OK();
}

bool CASDSkipList::DeleteNode(SkipListNode* node) {
  DCHECK(GetEpoch()->IsProtected());
  DCHECK(!node->IsHead());
  DCHECK(!node->IsTail());
  while (true) {
    auto* node_next = READ(node->next);
    // No need to check node.upper - this function only cares about one
    // horizontal level
    if ((uint64_t)node_next & kNodeDeleted) {
      return false;
    }
    DCHECK(node->level == node_next->level);

    // Try to set the deleted bit in node->next
    MarkNodePointer(&node->next);
    SkipListNode* node_prev = READ(node->prev);
    // Now make try to set the deleted bit in node.prev
    MarkNodePointer(&node->prev);
    DCHECK(node_prev);
    node_prev = (SkipListNode*)((uint64_t)node_prev & ~kNodeDeleted);
    DCHECK(node_prev->level == node_next->level);
    // CorrectPrev will make next.prev point to node.prev
    CorrectPrev(node_prev, node_next);
    return true;
  }
  return false;
}

Status MwCASDSkipList::Insert(const Slice& key, const Slice& value,
                              bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  // Get the supposedly left node at level 1; a larger value might have been
  // inserted after it by the time I start to install a new key, so must make
  // sure later
retry:
  SkipListNode* left = nullptr;
  SkipListNode* right = nullptr;
  auto ret = Find(key, &left);
  if (ret == Status::OK()) {
    DCHECK(left);
    return Status::KeyAlreadyExists();
  } else {
    DCHECK(ret.IsNotFound() && left);
    if ((uint64_t)left->next & kNodeDeleted) {
      left = GetPrev(left);
    }
  }

  DCHECK(left);
  DCHECK(left->level == 1);
  DCHECK(!left->IsTail());

  right = GetNext(left);

  DCHECK(right);
  DCHECK(right->level == left->level);
  DCHECK(right->level == 1);

  // We're sure that the predecessor's key is smaller than mine; now make sure
  // the to-be-successor has a key larger than mine.
  if (!right->IsTail()) {
    int cmp = right->key.compare(key);
    if (cmp < 0) {
      left = right;
      goto retry;
    } else if (cmp == 0) {
      return Status::KeyAlreadyExists();
    }
  }

  DCHECK(MwcTargetField<uint64_t>::IsCleanPtr((uint64_t)left));
  DCHECK(MwcTargetField<uint64_t>::IsCleanPtr((uint64_t)right));
  auto desc = descriptor_pool_->AllocateDescriptor();
  RAW_CHECK(desc.GetRaw(), "null MwCAS descriptor");
  uint32_t alloc_size = sizeof(SkipListNode) + key.size() + value.size();
  uint32_t idx =
      desc.ReserveAndAddEntry((uint64_t*)&left->next, (uint64_t)right,
                              Descriptor::kRecycleNewOnFailure);

  Allocator::Get()->AllocateAligned((void**)desc.GetNewValuePtr(idx),
                                    alloc_size, kCacheLineSize);
  SkipListNode* node = (SkipListNode*)desc.GetNewValue(idx);
  new (node) SkipListNode(key, value.size());
  node->level = 1;
  memcpy(node->GetPayload(), value.data(), value.size());
  DCHECK(node->lower == nullptr);

  node->next = right;
  node->prev = left;

#ifdef PMEM
  NVRAM::Flush(node->Size(), node);
#endif

  desc.AddEntry((uint64_t*)&right->prev, (uint64_t)left, (uint64_t)node);
  if (!desc.MwCAS()) {
    goto retry;
  }
  FinishInsert(node);
  return Status::OK();
}

void MwCASDSkipList::FinishInsert(SkipListNode* node) {
  thread_local RandomNumberGenerator height_rng{};
  DCHECK(GetEpoch()->IsProtected());
  // Continue with higher levels
  auto* stack = GetTlsPathStack();
  uint32_t original_height = stack->Size() + 1;

  uint32_t h = height_rng.Generate();
  uint32_t height = 1;
  while (h & 1) {
    height++;
    h >>= 1;
  }
  height = std::min(height, (uint32_t)max_height_);
  bool grow = height > original_height;

  DCHECK(node->level == 1);
  while (stack->Size() > 0) {
    auto* prev = stack->Pop();
    if (prev->level > height) {
      return;
    }
  retry:
    if (prev->key.compare(node->key) == 0) {
      continue;
    }
    auto* next = GetNext(prev);
    if (!next->IsTail()) {
      int cmp = next->key.compare(node->key);
      if (cmp == 0) {
        return;
      } else if (cmp < 0) {
        prev = next;
        goto retry;
      }
    }
    auto desc = descriptor_pool_->AllocateDescriptor();
    RAW_CHECK(desc.GetRaw(), "null MwCAS descriptor");
    uint32_t alloc_size = sizeof(SkipListNode) + node->key.size();
    uint32_t idx =
        desc.ReserveAndAddEntry((uint64_t*)&prev->next, (uint64_t)next,
                                Descriptor::kRecycleNewOnFailure);

    Allocator::Get()->AllocateAligned((void**)desc.GetNewValuePtr(idx),
                                      alloc_size, kCacheLineSize);
    SkipListNode* n = (SkipListNode*)desc.GetNewValue(idx);
    new (n) SkipListNode(node->key, 0);
    n->lower = node;
    n->level = prev->level;
    DCHECK(n->level == node->level + 1);
    DCHECK(n->level == prev->level);
    DCHECK(n->level == next->level);
    n->prev = prev;
    n->next = next;
#ifdef PMEM
    NVRAM::Flush(n->Size(), n);
#endif

    desc.AddEntry((uint64_t*)&node->upper, 0, (uint64_t)n);
    desc.AddEntry((uint64_t*)&next->prev, (uint64_t)prev, (uint64_t)n);
    if (!desc.MwCAS()) {
      return;
    }
    node = n;
  }
  DCHECK(stack->Size() == 0);

  // Are we growing the list-wide height?
  if (grow) {
    // Multiple threads might be doing the same, but one will succeed
    auto* expected = READ(head_);
    if (expected->level < height) {
      auto* desired = (SkipListNode*)((uint64_t)expected->upper | kDirtyFlag);
      DCHECK(desired);
      if (CompareExchange64Ptr(&head_, desired, expected) == expected) {
#ifdef PMEM
        ReadPersist(&head_);
#endif
      }
      DCHECK(READ(head_)->level > original_height);
    }
  }
}

/// Same as CAS implementation, except we don't explicitly put nodes on garbage
/// list.
Status MwCASDSkipList::Delete(const Slice& key, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  SkipListNode* node = nullptr;
  auto ret = Find(key, &node);
  if (ret.IsNotFound()) {
    return ret;
  }
  DCHECK(key.compare(node->key) == 0);
  DCHECK(node);
  DCHECK(node->level == 1);

  // Delete from top to bottom
  SkipListNode* top = nullptr;
  SkipListNode* n = nullptr;
  n = node;
  while (!top) {
    DCHECK(n->key.compare(key) == 0);
    auto* upper = ResolveNodePointer(&n->upper);
    if (!upper) {
      // If succeeded, this causes the concurrent insert's mwcas to fail
      if (nullptr == CompareExchange64Ptr(
                         &n->upper, (SkipListNode*)(kNodeDeleted | kDirtyFlag),
                         (SkipListNode*)0)) {
#ifdef PMEM
        ReadPersist(&n->upper);
#endif
        top = n;
      } else {
        upper = ResolveNodePointer(&n->upper);
        if ((uint64_t)upper & kNodeDeleted) {
          top = n;
        } else {
          n = upper;
        }
      }
    } else if ((uint64_t)upper == kNodeDeleted) {
      top = n;
    } else {
      n = upper;
    }
  }

  while (top) {
    DeleteNode(top);
    top = top->lower;
  }
  return Status::OK();
}

bool MwCASDSkipList::DeleteNode(SkipListNode* node) {
  DCHECK(GetEpoch()->IsProtected());
  while (((uint64_t)node->next & kNodeDeleted) == 0) {
    SkipListNode* prev = ResolveNodePointer(&node->prev);
    SkipListNode* next = ResolveNodePointer(&node->next);
    if (prev->next != node || next->prev != node) {
      continue;
    }
    auto desc = descriptor_pool_->AllocateDescriptor();
    RAW_CHECK(desc.GetRaw(), "null MwCAS descriptor");
    desc.AddEntry((uint64_t*)&prev->next, (uint64_t)node, (uint64_t)next,
                  Descriptor::kRecycleOldOnSuccess);
    desc.AddEntry((uint64_t*)&next->prev, (uint64_t)node, (uint64_t)prev);
    desc.AddEntry((uint64_t*)&node->next, (uint64_t)next,
                  (uint64_t)next | kNodeDeleted);
    desc.MwCAS();
  }
  return true;
}
}  // namespace pmwcas
