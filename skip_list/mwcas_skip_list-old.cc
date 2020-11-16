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

Status MwCASDSkipList::Insert(const Slice& key, const Slice& value,
                              bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  // Get the supposedly left node at level 1; a larger value might have been
  // inserted after it by the time I start to install a new key, so must make
  // sure later
retry:
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

#if defined(PMEM)
  auto allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
  allocator->AllocateOffset(desc.GetNewValuePtr(idx), alloc_size);
  ptr<SkipListNode> node =
      allocator->GetDirect<SkipListNode>(desc.GetNewValue(idx));
#else
  Allocator::Get()->AllocateAligned((void**)desc.GetNewValuePtr(idx),
                                    alloc_size, kCacheLineSize, true);
  ptr<SkipListNode> node = (ptr<SkipListNode>)desc.GetNewValue(idx);
#endif
  new (node) SkipListNode(key, value.size());
  node->level = 1;
  memcpy(node->GetPayload(), value.data(), value.size());
  DCHECK(!node->lower);

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

void MwCASDSkipList::FinishInsert(ptr<SkipListNode> node) {
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
  height = std::min(height, (uint32_t)max_height_);
  bool grow = height > original_height;

  DCHECK(node->level == 1);
  while (stack->Size() > 0) {
    auto prev = stack->Pop();
    if (prev->level > height) {
      return;
    }
  retry:
    if (prev->key.compare(node->key) == 0) {
      continue;
    }
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
    auto desc = descriptor_pool_->AllocateDescriptor();
    RAW_CHECK(desc.GetRaw(), "null MwCAS descriptor");
    uint32_t alloc_size = sizeof(SkipListNode) + node->key.size();
    uint32_t idx =
        desc.ReserveAndAddEntry((uint64_t*)&prev->next, (uint64_t)next,
                                Descriptor::kRecycleNewOnFailure);

#if defined(PMEM)
    auto allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
    allocator->AllocateOffset(desc.GetNewValuePtr(idx), alloc_size);
    ptr<SkipListNode> n =
        allocator->GetDirect<SkipListNode>(desc.GetNewValue(idx));
#else
    Allocator::Get()->AllocateAligned((void**)desc.GetNewValuePtr(idx),
                                      alloc_size, kCacheLineSize, true);
    ptr<SkipListNode> n = (ptr<SkipListNode>)desc.GetNewValue(idx);
#endif
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

/// Same as CAS implementation, except we don't explicitly put nodes on garbage
/// list.
Status MwCASDSkipList::Delete(const Slice& key, bool already_protected) {
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
    DCHECK(n->key.compare(key) == 0);
    auto upper = ResolveNodePointer(&n->upper);
    if (!upper) {
      // If succeeded, this causes the concurrent insert's mwcas to fail
      if (!CompareExchange64(&n->upper,
                             (ptr<SkipListNode>)(kNodeDeleted | kDirtyFlag),
                             (ptr<SkipListNode>)nullptr)) {
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

bool MwCASDSkipList::DeleteNode(ptr<SkipListNode> node) {
  DCHECK(GetEpoch()->IsProtected());
  while (((uint64_t)node->next & kNodeDeleted) == 0) {
    ptr<SkipListNode> prev = ResolveNodePointer(&node->prev);
    ptr<SkipListNode> next = ResolveNodePointer(&node->next);
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
