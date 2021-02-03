#include "skip_list.h"

#include "glog/raw_logging.h"

namespace pmwcas {
CASDSkipList::CASDSkipList() {
  Status s = epoch_.Initialize();
  RAW_CHECK(s.ok(), "epoch init failure");

  for (uint32_t i = 0; i < SKIPLIST_MAX_HEIGHT; ++i) {
    head_.next[i] = &tail_;
    head_.prev[i] = nullptr;
    tail_.next[i] = nullptr;
    tail_.prev[i] = &head_;
  }

  height = head_.height = tail_.height = 1;
  DCHECK(head_.key_size == 0);
  DCHECK(head_.payload_size == 0);
  DCHECK(tail_.key_size == 0);
  DCHECK(tail_.payload_size == 0);

#ifdef PMEM
  NVRAM::Flush(sizeof(CASDSkipList), this);
#endif
}

nv_ptr<SkipListNode> CASDSkipList::getNext(nv_ptr<SkipListNode> node, uint32_t level) {
  // return CleanPtr(node->next[level]);
  DCHECK(GetEpoch()->IsProtected());

  while (true) {
    if (node == &tail_) {
      return nullptr;
    }
    auto next_ptr = READ(node->next[level]);
    auto next = CleanPtr(next_ptr);
    auto next_next = READ(next->next[level]);
    bool d = (uint64_t)next_next & SkipListNode::kNodeDeleted;
    if (d && !((uint64_t)next_ptr & SkipListNode::kNodeDeleted)) {
      // [next] is deleted, but [node] is not. We need to logically
      // delete [next] before progressing forward.
      DCHECK(next_ptr == next);
      MarkNodePointer(&next->prev[level]);
      auto desired = CleanPtr(next_next);
      PersistentCAS(&node->next[level], desired, next_ptr);
      continue;
    }
    node = next;
    if (!d) {
      // [next] is not deleted - that's the desired successor.
      return next;
    }
    // Ok, both nodes are deleted. We have to retry the same procedure
    // with [next] recursively.
  }
}

nv_ptr<SkipListNode> CASDSkipList::getPrev(nv_ptr<SkipListNode> node, uint32_t level) {
  DCHECK(GetEpoch()->IsProtected());

  while (true) {
    if (node == &head_) {
      return nullptr;
    }
    auto prev = CleanPtr(READ(node->prev[level]));
    auto prev_next = READ(prev->next[level]);
    bool deleted = (uint64_t)READ(node->next[level]) & SkipListNode::kNodeDeleted;

    if (prev_next == node && !deleted) {
      // [node] is not deleted, and its prev ptr seems consistent.
      // Note that [prev] could be the head sentinel.
      return prev;
    } else if (deleted) {
      // [node] is deleted. Fix this cursor to the actual non-deleted node
      // in the list before retrying GetPrev().
      node = GetNext(node, level);
    } else {
      // [node] is not deleted, but its prev ptr is inconsistent.
      // Try to correct it before retrying GetPrev().
      CorrectPrev(prev, node, level);
    }
  }
}

Status CASDSkipList::insert(const Slice& key, const Slice& value, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  DCHECK(GetEpoch()->IsProtected());

  nv_ptr<SkipListNode> left = nullptr;
  nv_ptr<SkipListNode> right = nullptr;
  nv_ptr<SkipListNode> node = nullptr;
  uint64_t node_height = 1;

retry:
  auto *array = GetTlsPathArray();
  Status ret = Traverse(key, nullptr);
  array->Get(0, &left, &right);
  DCHECK(left);
  DCHECK(right);
  if (ret == Status::OK()) {
    // Reclaim the allocated space (if applicable)
    if (node) {
      FreeNode(nullptr, node);
    }
    return Status::KeyAlreadyExists();
  }

  // XXX(shiges): Traverse() should have ensured the following property
  DCHECK(left == &head_ ||
         key.compare(Slice(left->GetKey(), left->key_size)) > 0);
  DCHECK(right == &tail_ ||
         key.compare(Slice(right->GetKey(), right->key_size)) < 0);

  // Now prepare a node and try to insert it between left and right
  if (!node) {
    AllocateNode(&node, key.size(), value.size());
    RAW_CHECK(node, "cannot allocate node");

    thread_local RandomNumberGenerator height_rng{};
    // Determine the height
    uint32_t h = height_rng.Generate();
    while (h & 1) {
      ++node_height;
      h >>= 1;
    }
    node_height = std::min<uint32_t>(node_height, SKIPLIST_MAX_HEIGHT);
    new (node) SkipListNode(key, value, node_height);
  }

  // Build the lowest level first
  node->next[0] = right;
  node->prev[0] = left;

  for (uint32_t i = 1; i < node_height; ++i) {
    nv_ptr<SkipListNode> left = nullptr;
    nv_ptr<SkipListNode> right = nullptr;
    if (i <= array->max_level) {
      array->Get(i, &left, &right);
    } else {
      left = &head_;
      right = &tail_;
    }

    node->prev[i] = left;
    node->next[i] = right;
  }

#ifdef PMEM
  NVRAM::Flush(sizeof(SkipListNode) + key.size() + value.size(), node);
#endif

  // Link with pred in the lowest level
  if (PersistentCAS(&left->next[0], node, right) != right) {
    // TODO(shiges): implement InsertBefore()
    goto retry;
  }

  // Setup [right.prev] to point to [node]
  // [left] here is given as a "suggestion" using which CorrectPrev can find the
  // true predecessor of [right]
  CorrectPrev(left, right, 0);

  // If [right] is deleted by a concurrent thread, the above CorrectPrev() would
  // be aborted, and we risk linking [node] to a deleted node, which confuses
  // garbage collection. Make sure we are linking to an non-deleted node by finding
  // the logical successor.
  GetNext(node, 0);

  // Now we're sure that the insertion will eventually succeed.
  // Finish building the remaining levels
  // See if we are growing the list; if so, increase it to enable the next/prev pointers
  uint64_t current_height = READ(height);
  if (node_height > current_height) {
    while (PersistentCAS(&height, node_height, current_height) < node_height) {}
  }

  for (uint64_t i = 1; i < node_height; ++i) {
    left = READ(node->prev[i]);
    right = READ(node->next[i]);

    if ((uint64_t)right & SkipListNode::kNodeDeleted) {
      // This node has already been deleted by a concurrent thread. Just don't bother
      // linking it and stop. Note that on all the higher levels this node should have
      // been marked deleted as well (because the deleter marks the ptrs from top to
      // bottom), so we don't need to check the higher levels.
      // This is rather an optimization, though, since if [node] is already marked
      // deleted at the time of insertion, CorrectPrev() would remove it anyway.
      break;
    }

    DCHECK(left == &head_ ||
          key.compare(Slice(left->GetKey(), left->key_size)) > 0);
    DCHECK(right == &tail_ ||
          key.compare(Slice(right->GetKey(), right->key_size)) < 0);

    if (PersistentCAS(&left->next[i], node, right) != right) {
      // Failed, give up?
      // The filled prev[i] field shouldn't matter - node height remains the old value
      break;
    }

    // Succeeded, setup the prev pointer
    CorrectPrev(left, right, i);

    // Validate the successor
    GetNext(node, i);
  }

  return Status::OK();
}

// @prev: suggested predecessor of [node] - may be the old predecessor before an
//        insert was made in front of [node]
nv_ptr<SkipListNode> CASDSkipList::CorrectPrev(nv_ptr<SkipListNode> prev, nv_ptr<SkipListNode> node, uint16_t level) {
  DCHECK(GetEpoch()->IsProtected());
  nv_ptr<SkipListNode> last_link = nullptr;
  nv_ptr<SkipListNode> node0 = node;
  nv_ptr<SkipListNode> prev0 = prev;

  while (true) {
    auto link1 = READ(node->prev[level]);
    if ((uint64_t)link1 & SkipListNode::kNodeDeleted) {
      // Deleted bit marked on [prev]field, node is already deleted
      break;
    }

    auto prev2 = READ(prev->next[level]);
    DCHECK(prev2);
    if ((uint64_t)prev2 & SkipListNode::kNodeDeleted) {
      if (last_link) {
        // [prev] has deleted mark in its [next] field, i.e., it's at least being
        // deleted; so mark also the deleted bit in its [prev] field.
        nv_ptr<SkipListNode> expected = READ(prev->prev[level]);
        while (!((uint64_t)expected & SkipListNode::kNodeDeleted)) {
          nv_ptr<SkipListNode> desired = (nv_ptr<SkipListNode>)((uint64_t)expected | SkipListNode::kNodeDeleted);
          expected = PersistentCAS(&prev->prev[level], desired, expected);
        }

        // Try to fix prev.prev.next to point to [node] to unlink this node ([prev])
        nv_ptr<SkipListNode> desired = CleanPtr(prev2);
        //uint64_t desired = ((uint64_t)prev2 & ~SkipListNode::kNodeDeleted);
        PersistentCAS(&last_link->next[level], desired, prev);
        prev = last_link;
        last_link = nullptr;
        continue;
      }
      prev2 = CleanPtr(READ(prev->prev[level]));
      // (nv_ptr<SkipListNode>)((uint64_t)prev->prev[level] & ~SkipListNode::kNodeDeleted);
      prev = prev2;
      DCHECK(prev);
      continue;
    }

    DCHECK(((uint64_t)prev2 & SkipListNode::kNodeDeleted) == 0);
    if (prev2 != node) {
      // The given or current [prev] is not the true predecessor of [node], advance it to
      // see its successor is the true pred of [node]
      last_link = prev;
      prev = prev2;
      continue;
    }
    // Now [prev] should be the true predecessor, try a CAS to finalize it
    DCHECK(((uint64_t)prev & SkipListNode::kNodeDeleted) == 0);
    nv_ptr<SkipListNode> p = CleanPtr(prev); // (nv_ptr<SkipListNode> )((uint64_t)prev & ~SkipListNode::kNodeDeleted);
    if (link1 == PersistentCAS(&node->prev[level], p, link1)) {
      if ((uint64_t)READ(prev->prev[level]) & SkipListNode::kNodeDeleted) {
        continue;
      }
      break;
    } // Fine if it failed - someone else should have fixed the link
  }
  return prev;
}

Status CASDSkipList::remove(const Slice& key, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  nv_ptr<SkipListNode> node = nullptr;
  Status ret = Traverse(key, &node);
  if (ret != Status::OK()) {
    // Not found?
    return Status::NotFound();
  }

  DCHECK(node);

  // Start from the highest level to delete nodes.
  // To delete a node (in a level), mark the [next] pointer first, then mark the
  // deleted bit in [prev] pointer (must follow this order).
  // Only the thread that succeeded marking [next] would continue to mark [prev].
  uint32_t node_height = node->height;
  bool deleted = false;
  for (uint32_t h = node_height; h > 0; h--) {
    uint32_t level = h - 1;
    while (true) {
      nv_ptr<SkipListNode> node_next = READ(node->next[level]);
      if ((uint64_t)node_next & SkipListNode::kNodeDeleted) {
        // Already marked as deleted on [next] pointer by someone else
        break;  // continue to the next level
      }

      nv_ptr<SkipListNode> p = (nv_ptr<SkipListNode>)((uint64_t)node_next | SkipListNode::kNodeDeleted);
      if (node_next == PersistentCAS(&node->next[level], p, node_next)) {
        if (level == 0) {
          // Among all the concurrent deleters, I'm the one that's going
          // to report something positive to the user.
          deleted = true;
        }
        // Continue to mark the [prev] pointer
        nv_ptr<SkipListNode> prev = nullptr;
        while (true) {
          prev = READ(node->prev[level]);
          if ((uint64_t)prev & SkipListNode::kNodeDeleted) {
            break;
          }

          p = (nv_ptr<SkipListNode>)((uint64_t)prev | SkipListNode::kNodeDeleted);
          if (prev == PersistentCAS(&node->prev[level], p, prev)) {
            // Succeeded
            break;
          }
        }

        // Correct links
        prev = CorrectPrev(CleanPtr(prev), node_next, level);
        break;  // continue to the next level
      }
    }
  }

  if (deleted) {
    Status s = GetGarbageList()->Push(node, CASDSkipList::FreeNode, nullptr);
    RAW_CHECK(s.ok(), "failed recycling node");
    return Status::OK();
  } else {
    // The deletion was logically fulfilled by another thread. I'm reporting
    // something negative to the user.
    return Status::NotFound();
  }
}

MwCASDSkipList::MwCASDSkipList(DescriptorPool* pool) : desc_pool_(pool) {
  for (uint32_t i = 0; i < SKIPLIST_MAX_HEIGHT; ++i) {
    head_.next[i] = &tail_;
    head_.prev[i] = nullptr;
    tail_.next[i] = nullptr;
    tail_.prev[i] = &head_;
  }

  height = head_.height = tail_.height = 1;
  DCHECK(head_.key_size == 0);
  DCHECK(head_.payload_size == 0);
  DCHECK(tail_.key_size == 0);
  DCHECK(tail_.payload_size == 0);

#ifdef PMEM
  NVRAM::Flush(sizeof(MwCASDSkipList), this);
#endif
}

Status MwCASDSkipList::insert(const Slice& key, const Slice& value, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  DCHECK(GetEpoch()->IsProtected());

  nv_ptr<SkipListNode> left = nullptr;
  nv_ptr<SkipListNode> right = nullptr;
  nv_ptr<SkipListNode> node = nullptr;

retry:
  auto *array = GetTlsPathArray();
  Status ret = Traverse(key, nullptr);
  array->Get(0, &left, &right);
  DCHECK(left);
  DCHECK(right);
  if (ret == Status::OK()) {
    return Status::KeyAlreadyExists();
  }

  // XXX(shiges): Traverse() should have ensured the following property
  DCHECK(left == &head_ ||
         key.compare(Slice(left->GetKey(), left->key_size)) > 0);
  DCHECK(right == &tail_ ||
         key.compare(Slice(right->GetKey(), right->key_size)) < 0);

  auto desc = desc_pool_->AllocateDescriptor();
  RAW_CHECK(desc.GetRaw(), "null MwCAS descriptor");
  auto idx =
      desc.ReserveAndAddEntry((uint64_t *)&left->next[0], (uint64_t)right,
                              Descriptor::kRecycleNewOnFailure);

#ifdef PMEM
  auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
  allocator->AllocateOffset(desc.GetNewValuePtr(idx),
                            sizeof(SkipListNode) + key.size() + value.size());
#else
  Allocator::Get()->AllocateAligned(
      (void **)desc.GetNewValuePtr(idx),
      sizeof(SkipListNode) + key.size() + value.size(), kCacheLineSize);
#endif
  node = (nv_ptr<SkipListNode>)desc.GetNewValue(idx);

  // Now prepare the new node
  uint64_t node_height = 1;
  thread_local RandomNumberGenerator height_rng{};
  // Determine the height
  uint32_t h = height_rng.Generate();
  while (h & 1) {
    ++node_height;
    h >>= 1;
  }
  node_height = std::min<uint32_t>(node_height, SKIPLIST_MAX_HEIGHT);
  new (node) SkipListNode(key, value, node_height);

  // Build the lowest level first
  node->next[0] = right;
  node->prev[0] = left;

  for (uint32_t i = 1; i < node_height; ++i) {
    nv_ptr<SkipListNode> left = nullptr;
    nv_ptr<SkipListNode> right = nullptr;
    if (i <= array->max_level) {
      array->Get(i, &left, &right);
    } else {
      left = &head_;
      right = &tail_;
    }

    node->prev[i] = left;
    node->next[i] = right;
  }

#ifdef PMEM
  NVRAM::Flush(sizeof(SkipListNode) + key.size() + value.size(), node);
#endif

  // Link with pred & next in the lowest level
  auto idx2 = desc.AddEntry((uint64_t *)&right->prev[0], (uint64_t)left,
                            (uint64_t)node);
  DCHECK(desc.GetNewValue(idx) == desc.GetNewValue(idx2));

  if (!desc.MwCAS()) {
    goto retry;
  }

  // Validate the successor
  GetNext(node, 0);

  // Now we're sure that the insertion will eventually succeed.
  // Finish building the remaining levels
  // See if we are growing the list; if so, increase it to enable the next/prev
  // pointers
  uint64_t current_height = READ(height);
  if (node_height > current_height) {
    while (PersistentCAS(&height, node_height, current_height) < node_height) {
    }
  }

  for (uint64_t i = 1; i < node_height; ++i) {
    left = readMwCASPtr(&node->prev[i]);
    right = readMwCASPtr(&node->next[i]);

    if ((uint64_t)right & SkipListNode::kNodeDeleted) {
      // This node has already been deleted by a concurrent thread. Just don't
      // bother linking it and stop. Note that on all the higher levels this
      // node should have been marked deleted as well (because the deleter marks
      // the ptrs from top to bottom), so we don't need to check the higher
      // levels.
      break;
    }

    DCHECK(left == &head_ ||
           key.compare(Slice(left->GetKey(), left->key_size)) > 0);
    DCHECK(right == &tail_ ||
           key.compare(Slice(right->GetKey(), right->key_size)) < 0);

    auto desc = desc_pool_->AllocateDescriptor();
    RAW_CHECK(desc.GetRaw(), "null MwCAS descriptor");

    auto idx = desc.AddEntry((uint64_t *)&left->next[i], (uint64_t)right,
                             (uint64_t)node);

    auto idx2 = desc.AddEntry((uint64_t *)&right->prev[i], (uint64_t)left,
                              (uint64_t)node);
    DCHECK(desc.GetNewValue(idx) == desc.GetNewValue(idx2));

    if (!desc.MwCAS()) {
      // Failed, give up?
      // The filled prev[i] field shouldn't matter - node height remains the old value
      break;
    }

    // Validate the successor
    GetNext(node, i);
  }

  return Status::OK();
}

Status MwCASDSkipList::remove(const Slice& key, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  nv_ptr<SkipListNode> node = nullptr;
  Status ret = Traverse(key, &node);
  if (ret != Status::OK()) {
    // Not found?
    return Status::NotFound();
  }

  DCHECK(node);

  // Start from the highest level to delete nodes.
  uint32_t node_height = node->height;
  bool deleted = false;
  for (uint32_t level = node_height - 1; level > 0; level--) {
    while (true) {
      auto prev = readMwCASPtr(&node->prev[level]);
      auto next = readMwCASPtr(&node->next[level]);
      if ((uint64_t)next & SkipListNode::kNodeDeleted) {
        // Already marked as deleted on [next] pointer by someone else
        break;
      }
      auto desc = desc_pool_->AllocateDescriptor();
      RAW_CHECK(desc.GetRaw(), "null MwCAS descriptor");

      // We allow a node to not be linked in an upper level even if it
      // claims itself to be (i.e. with a large [height] value.) Thus
      // it is vital to check the neighbors to see if they indeed point
      // to this node and only issue MwCAS to them if so. If they do not
      // point to [node], we simply toggle the deleted mark.
      auto prev_next = readMwCASPtr(&prev->next[level]);
      if (prev_next == node) {
        auto next_prev = readMwCASPtr(&next->prev[level]);
        DCHECK(next_prev == node);
        desc.AddEntry((uint64_t *)&prev->next[level], (uint64_t)node,
                      (uint64_t)next);
        desc.AddEntry((uint64_t *)&next->prev[level], (uint64_t)node,
                      (uint64_t)prev);
      }
      desc.AddEntry((uint64_t *)&node->next[level], (uint64_t)next,
                    (uint64_t)next | SkipListNode::kNodeDeleted);
      if (desc.MwCAS()) {
        break;
      }
    }
  }

  // Try to unlink the node from the lowest level
  while (true) {
    auto prev = readMwCASPtr(&node->prev[0]);
    auto next = readMwCASPtr(&node->next[0]);
    if ((uint64_t)next & SkipListNode::kNodeDeleted) {
      // Already marked as deleted on [next] pointer by someone else
      break;
    }
    auto desc = desc_pool_->AllocateDescriptor();
    RAW_CHECK(desc.GetRaw(), "null MwCAS descriptor");
    desc.AddEntry((uint64_t *)&prev->next[0], (uint64_t)node, (uint64_t)next,
                  Descriptor::kRecycleOldOnSuccess);
    desc.AddEntry((uint64_t *)&next->prev[0], (uint64_t)node, (uint64_t)prev);
    desc.AddEntry((uint64_t *)&node->next[0], (uint64_t)next,
                  (uint64_t)next | SkipListNode::kNodeDeleted);
    if (desc.MwCAS()) {
      deleted = true;
      break;
    }
  }

  if (deleted) {
    // I unlinked the node from the lowest level.
    return Status::OK();
  } else {
    // The deletion was logically fulfilled by another thread. I'm reporting
    // something negative to the user.
    return Status::NotFound();
  }
}

}  // namespace pmwcas
