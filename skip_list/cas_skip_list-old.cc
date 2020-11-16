#include "skip_list.h"

#include "glog/raw_logging.h"

#ifdef PMEM
#if defined(MwCASSafeAlloc) && defined(UsePMAllocHelper)
#error "Cannot use both MwCASSafeAlloc and UsePMAllocHelper"
#endif

#if defined(MwCASSafeAlloc)
#warning "Using single-word PMwCAS for CASDSkipList"
#elif defined(UsePMAllocHelper)
#warning "Using PCAS with unsafe allocation: persistent memory leak is possible (PCAS only)!"
#else
#error "MwCASSafeAlloc or UsePMAllocHelper needed for PMEM"
#endif

#endif

namespace pmwcas {

ptr<SkipListNode> CASDSkipList::GetPrev(ptr<SkipListNode> node) {
  DCHECK(GetEpoch()->IsProtected());
  auto list_head = READ(head_);
  // Descend to the right level
  while (list_head->level != node->level) {
    list_head = list_head->lower;
  }
  DCHECK(list_head);

  while (true) {
    if (node == list_head) {
      return nullptr;
    }
    auto prev = CleanPtr(READ(node->prev));
    auto prev_next = READ(prev->next);
    auto curr_next = READ(node->next);
    if (prev_next == node && ((uint64_t)curr_next & kNodeDeleted) == 0) {
      return prev;  // node could be list_head
    } else if ((uint64_t)curr_next & kNodeDeleted) {
      node = GetNext(node);
    } else {
      prev = CorrectPrev(prev, node);
    }
  }
}

ptr<SkipListNode> CASDSkipList::CorrectPrev(ptr<SkipListNode> prev,
                                            ptr<SkipListNode> node) {
  DCHECK(GetEpoch()->IsProtected());
  DCHECK(((uint64_t)node & kNodeDeleted) == 0);
  ptr<SkipListNode> last_link = nullptr;
  while (true) {
    // DCHECK(!((ptr<SkipListNode>)((uint64_t)READ(prev) &
    // ~kNodeDeleted))->IsTail());
    auto link1 = READ(node->prev);
    // DCHECK(!((ptr<SkipListNode>)((uint64_t)READ(link1) &
    // ~kNodeDeleted))->IsTail());
    if ((uint64_t)link1 & kNodeDeleted) {
      break;
    }

    ptr<SkipListNode> prev_cleared = READ(prev);
    auto prev_next = READ(prev_cleared->next);
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
        if (prev == CompareExchange64(&last_link->next,
                                      (ptr<SkipListNode>)(desired | kDirtyFlag),
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
      prev_next = (ptr<SkipListNode>)((uint64_t)READ(prev_cleared->prev) &
                                      ~kNodeDeleted);
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
    ptr<SkipListNode> p =
        (ptr<SkipListNode>)(((uint64_t)prev & ~kNodeDeleted) | kDirtyFlag);
    if (link1 == CompareExchange64(&node->prev, p, link1)) {
#ifdef PMEM
      ReadPersist(&node->prev);
#endif
      auto prev_cleared_prev = READ(prev_cleared->prev);
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
  ptr<SkipListNode> left = nullptr;
  ptr<SkipListNode> right = nullptr;
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
  auto allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
  allocator->AllocateOffset(desc.GetNewValuePtr(idx),
                            sizeof(SkipListNode) + key.size() + value.size());
  ptr<SkipListNode> node =
      allocator->GetDirect<SkipListNode>(desc.GetNewValue(idx));
  new (node) SkipListNode(key, value.size());
#else
  auto allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
  uint64_t* tls_addr = (uint64_t*)PMAllocHelper::Get()->GetTlsPtr();
  allocator->AllocateOffset(
      tls_addr, sizeof(SkipListNode) + key.size() + value.size(), false);
  ptr<SkipListNode> node = allocator->GetDirect<SkipListNode>(*tls_addr);
  new (node) SkipListNode(key, value.size());
#endif
#else
  ptr<SkipListNode> node = nullptr;
  Allocator::Get()->AllocateAligned(
      (void**)&node, sizeof(SkipListNode) + key.size() + value.size(),
      kCacheLineSize);
  new (node) SkipListNode(key, value.size());
#endif
  node->level = 1;
  node->prev = left;
  node->next = right;
  node->lower = nullptr;
  DCHECK(!node->lower);
  memcpy(node->GetPayload(), value.data(), value.size());
#ifdef PMEM
  NVRAM::Flush(node->Size(), node);
#endif
#if defined(PMEM)
#if defined(MwCASSafeAlloc)
  if (!desc.MwCAS()) {
    goto retry;
  }
#else
  if (CompareExchange64(&left->next, node, right) != right) {
    allocator->FreeOffset(tls_addr);
    goto retry;
  }
#endif
#else
  if (CompareExchange64(&left->next, node, right) != right) {
    Allocator::Get()->FreeAligned((void**)&node);
    goto retry;
  }
#endif
  CorrectPrev(node, right);

  FinishInsert(node);
  return Status::OK();
}


void CASDSkipList::FinishInsert(ptr<SkipListNode> node) {
  // One RNG per thread
  thread_local RandomNumberGenerator height_rng{};
  DCHECK(GetEpoch()->IsProtected());
  // Continue with higher levels
  auto stack = GetTlsPathStack();
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
    auto prev = stack->Pop();
    if (prev->level > height) {
      return;
    }
    DCHECK(prev->key.compare(node->key) < 0);
  retry:
    auto next = GetNext(prev);
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
    auto allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
    allocator->AllocateOffset(desc.GetNewValuePtr(idx),
                              sizeof(SkipListNode) + node->key.size());
    ptr<SkipListNode> n =
        allocator->GetDirect<SkipListNode>(desc.GetNewValue(idx));
    new (n) SkipListNode(node->key, 0);
#else
    auto allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
    uint64_t* tls_addr = (uint64_t*)PMAllocHelper::Get()->GetTlsPtr();
    allocator->AllocateOffset(tls_addr, sizeof(SkipListNode) + node->key.size(),
                              false);
    ptr<SkipListNode> n = allocator->GetDirect<SkipListNode>(*tls_addr);
    new (n) SkipListNode(node->key, 0);
#endif
#else
    ptr<SkipListNode> n = nullptr;
    Allocator::Get()->AllocateAligned(
        (void**)&n, sizeof(SkipListNode) + node->key.size(), kCacheLineSize);
    new (n) SkipListNode(node->key, 0);
#endif

    if (CompareExchange64(&node->upper,
                          (ptr<SkipListNode>)((uint64_t)n | kDirtyFlag),
                          (ptr<SkipListNode>)nullptr)) {
      // Failed making the lower node to point to me, already deleted?
      // Add a dummy entry to just delete the memory allocated after Abort()
#if defined(PMEM)
#if defined(MwCASSafeAlloc)
      desc.Abort();
#else
      allocator->FreeOffset(tls_addr);
#endif
#else
      Allocator::Get()->FreeAligned((void**)&n);
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
      node->upper = nullptr;
      *&n->level = SkipListNode::kLevelAbondoned;
      return;
    }
#else
    if (CompareExchange64(&prev->next, n, next) != next) {
      node->upper = nullptr;
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
    auto expected = READ(head_);
    if (expected->level < height) {
      auto desired =
          (ptr<SkipListNode>)((uint64_t)expected->upper | kDirtyFlag);
      DCHECK(desired);
      if (CompareExchange64(&head_, desired, expected) == expected) {
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
  ptr<SkipListNode> node = nullptr;
  auto ret = Find(key, &node);
  if (ret.IsNotFound()) {
    return ret;
  }
  DCHECK(key.compare(node->key) == 0);
  DCHECK(node);
  DCHECK(node->level == 1);

  // Delete from top to bottom
  ptr<SkipListNode> top = nullptr;
  ptr<SkipListNode> n = nullptr;
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
    auto upper = READ(n->upper);
    DCHECK(n->key.compare(key) == 0);
    if ((uint64_t)upper == kNodeDeleted) {
      top = n;
    } else {
      if (!upper) {
        // See if there's an on-going insert and if so try to end it
        if (!CompareExchange64(&n->upper,
                               (ptr<SkipListNode>)(kNodeDeleted | kDirtyFlag),
                               (ptr<SkipListNode>)nullptr)) {
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
        n = (ptr<SkipListNode>)((uint64_t)upper & ~kNodeDeleted);
      }
    }
  }
  if (!top) {
    top = node;
  }
  while (top) {
    DCHECK(top->key.compare(key) == 0);
    auto to_delete = top;
    top = READ(top->lower);
    if (to_delete->level != SkipListNode::kLevelAbondoned) {
      DeleteNode(to_delete);
    }
  }
  return Status::OK();
}

bool CASDSkipList::DeleteNode(ptr<SkipListNode> node) {
  DCHECK(GetEpoch()->IsProtected());
  DCHECK(!node->IsHead());
  DCHECK(!node->IsTail());
  while (true) {
    auto node_next = READ(node->next);
    // No need to check node.upper - this function only cares about one
    // horizontal level
    if ((uint64_t)node_next & kNodeDeleted) {
      return false;
    }
    DCHECK(node->level == node_next->level);

    // Try to set the deleted bit in node->next
    MarkNodePointer(&node->next);
    ptr<SkipListNode> node_prev = READ(node->prev);
    // Now make try to set the deleted bit in node.prev
    MarkNodePointer(&node->prev);
    DCHECK(node_prev);
    node_prev = (ptr<SkipListNode>)((uint64_t)node_prev & ~kNodeDeleted);
    DCHECK(node_prev->level == node_next->level);
    // CorrectPrev will make next.prev point to node.prev
    CorrectPrev(node_prev, node_next);
    return true;
  }
  return false;
}

/// GetNext code that follows the original paper; appears to be costly.
#if 0
ptr<SkipListNode> CASDSkipList::GetNext(ptr<SkipListNode> node) {
  DCHECK(GetEpoch()->IsProtected());
  while(true) {
    if(node->IsTail()) {
      return nullptr;
    }
    auto next = READ(node->next);
    auto my_next = next;
    next = CleanPtr(next);
    DCHECK(next);
    ptr<SkipListNode> next_next = READ(next->next);
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
      if(next == CompareExchange64(&node->next, (ptr<SkipListNode>)desired, next)) {
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

}  // namespace pmwcas
