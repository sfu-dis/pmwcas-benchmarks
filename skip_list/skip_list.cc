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

nv_ptr<SkipListNode> CASDSkipList::GetNext(nv_ptr<SkipListNode> node, uint32_t level) {
  // return CleanPtr(node->next[level]);
  DCHECK(GetEpoch()->IsProtected());

  while (true) {
    if (node == &tail_) {
      return nullptr;
    }
    auto next = CleanPtr(READ(node->next[level]));

    if ((uint64_t)READ(next->next[level]) & SkipListNode::kNodeDeleted) {
      // [next] is deleted. We deviate from the paper and simply skip
      // this node without trying to unlink it from [node].
      node = next;
      continue;
    }

    return next;
  }
}

nv_ptr<SkipListNode> CASDSkipList::GetPrev(nv_ptr<SkipListNode> node, uint32_t level) {
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

Status CASDSkipList::Traverse(const Slice& key, nv_ptr<SkipListNode> *value_node) {
  DCHECK(GetEpoch()->IsProtected());
  auto *stack = GetTlsPathStack();
  stack->Reset();

  nv_ptr<SkipListNode> prev_node = nullptr;
  nv_ptr<SkipListNode> curr_node = &head_;
  int32_t curr_level_idx = height - 1;
  DCHECK((((uint64_t)head_.next[curr_level_idx] & SkipListNode::kNodeDeleted)) == 0);

  DCHECK(curr_node);
  DCHECK(curr_node->next[curr_level_idx]);

  // Drill down to the lowest level
  while (curr_level_idx > 0) {
    // Descend until the right isn't tail
    auto right = GetNext(curr_node, curr_level_idx);
    if (right == &tail_) {
      --curr_level_idx;
      stack->Push(curr_node);
      continue;
    }

    // Look right to see if we can move there
    Slice right_key(right->GetKey(), right->key_size);
    int cmp = key.compare(right_key);
    if (cmp > 0) {
      // Right key smaller than target key, move there
      prev_node = curr_node;
      curr_node = right;
    } else {
      // Right is too big, go down
      --curr_level_idx;
      stack->Push(curr_node);
    }
  }

  // Now at the lowest level, do linear search starting from [curr_node]
  DCHECK(curr_level_idx == 0);
  while (curr_node != &tail_) {
    Slice curr_key(curr_node->GetKey(), curr_node->key_size);
    int cmp = curr_key.compare(key);
    if (cmp == 0) {
      // Found the target key
      if (value_node) {
        *value_node = curr_node;
      }
      return Status::OK();
    } else if (cmp > 0) {
      // Too big - not found;
      if (value_node) {
        *value_node = prev_node;
      }
      return Status::NotFound();
    } else {
      prev_node = curr_node;
      curr_node = GetNext(curr_node, 0);
    }
  }

  if (value_node) {
    *value_node = prev_node;
  }
  return Status::NotFound();
}

Status CASDSkipList::Search(const Slice& key, nv_ptr<SkipListNode> *value_node,
                            bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  return Traverse(key, value_node);
}

Status CASDSkipList::Insert(const Slice& key, const Slice& value, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);

  nv_ptr<SkipListNode> left = nullptr;
  nv_ptr<SkipListNode> right = nullptr;
  nv_ptr<SkipListNode> node = nullptr;
  uint64_t node_height = 1;

retry:
  Status ret = Traverse(key, &left);
  if (ret == Status::OK()) {
    DCHECK(left);
    return Status::KeyAlreadyExists();
  }
  DCHECK(left);

  // Get the right neighbour and see if it is indeed greater, otherwise fast forward
  right = GetNext(left, 0);
  if (right != &tail_) {
    Slice right_key(right->GetKey(), right->key_size);
    int cmp = right_key.compare(key);
    if (cmp == 0) {
      return Status::KeyAlreadyExists();
    } else if (cmp < 0) {
      goto retry;
    }
  }

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
    new (node) SkipListNode(key, value, 1); //node_height);
    NVRAM::Flush(sizeof(SkipListNode) + key.size() + value.size(), node);
  }
  node->height = 1;

  // Build the lowest level first
  node->next[0] = right;
  node->prev[0] = left;
#ifdef PMEM
  NVRAM::Flush(sizeof(SkipListNode), node);
#endif

  // Link with pred in the lowest level
  if (PersistentCAS(&left->next[0], node, right) != right) {
    goto retry;
  }

  // Setup [right.prev] to point to [node]
  // [left] here is given as a "suggestion" using which CorrectPrev can find the
  // true predecessor of [right]
  CorrectPrev(left, right, 0);

  // Finish building the remaining levels
  // See if we are growing the list; if so, increase it to enable the next/prev pointers
  uint64_t current_height = height;
  if (node_height > current_height) {
    while (CompareExchange64(&height, node_height, current_height) < node_height) {}
  }

  auto *stack = GetTlsPathStack();
  for (uint16_t i = 1; i < node_height; ++i) {
    if (stack->count > 0) {
      left = stack->Pop();
      // XXX(tzwang): the check below won't always succeed - towers are first
      // linked, and then have their height updated. It should be safe to ignore
      // this.
      // DCHECK(left == &head_ || left->height > i);
    } else {
      left = &head_;
    }
  build_tower:
    right = GetNext(left, i);
    if (right != &tail_) {
      Slice right_key(right->GetKey(), right->key_size);
      int cmp = right_key.compare(key);
      if (cmp == 0) {
        // Someone else acted faster - continue to the next level (if any)
        continue;
      } else if (cmp < 0) {
        left = right;
        goto build_tower;
      } else {
        // Good to build it, but need to see if we are the first
      }
    }

    node->prev[i] = left;
    node->next[i] = right;
#ifdef PMEM
    // FIXME(shiges): Adjust this
    NVRAM::Flush(sizeof(SkipListNode), node);
#endif

    if (PersistentCAS(&left->next[i], node, right) != right) {
      // Failed, give up?
      // The filled prev[i] field shouldn't matter - node height remains the old value
      break;
    }

    // Succeeded, increment node height and setup the prev pointer
    ++node->height;
    CorrectPrev(left, right, i);
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
        if (prev == PersistentCAS(&last_link->next[level], desired, prev)) {
          // We need to unlink the node in all levels, so put the node only when
          // we have unlinked all levels
          if (Decrement64(&prev->height) == 0) {
            Status s = GetGarbageList()->Push(prev, CASDSkipList::FreeNode, nullptr);
            RAW_CHECK(s.ok(), "failed recycling node");
          }
        }
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
    nv_ptr<SkipListNode> p = CleanPtr(prev); // (nv_ptr<SkipListNode>)((uint64_t)prev & ~SkipListNode::kNodeDeleted);
    if (link1 == PersistentCAS(&node->prev[level], p, link1)) {
      if ((uint64_t)READ(prev->prev[level]) & SkipListNode::kNodeDeleted) {
        continue;
      }
      break;
    } // Fine if it failed - someone else should have fixed the link
  }
  return prev;
}

Status CASDSkipList::Delete(const Slice& key, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  nv_ptr<SkipListNode> node = nullptr;
  Status ret = Traverse(key, &node);
  if (ret != Status::OK()) {
    // Not found?
    return Status::NotFound();
  }

  DCHECK(node);

  // Start from the lowest level to delete nodes.
  // To delete a node (in a level), mark the [next] pointer first, then mark the
  // deleted bit in [prev] pointer (must follow this order).
  // Only the thread that succeeded marking [next] would continue to mark [prev].
  uint32_t node_height = node->height;
  for (uint32_t level = 0; level < node_height; level++) {
    while (true) {
      nv_ptr<SkipListNode> node_next = READ(node->next[level]);
      if ((uint64_t)node_next & SkipListNode::kNodeDeleted) {
        // Already marked as deleted on [next] pointer by someone else
        break;  // continue to the next level
      }

      nv_ptr<SkipListNode> p = (nv_ptr<SkipListNode>)((uint64_t)node_next | SkipListNode::kNodeDeleted);
      if (node_next == PersistentCAS(&node->next[level], p, node_next)) {
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
  return Status::OK();
}

void CASDSkipList::SanityCheck(bool print) {
  for (uint16_t i = 0; i < height; ++i) {//SKIPLIST_MAX_HEIGHT; ++i) {
    nv_ptr<SkipListNode> curr_node = &head_;
    while (curr_node != &tail_) {
      if (print) {
        if (curr_node == &head_) {
          std::cout << "HEAD";
        } else {
          std::cout << "->" << *(uint64_t*)curr_node->GetKey();
        }
      }

      nv_ptr<SkipListNode> right_node = GetNext(curr_node, i);
      //RAW_CHECK(right_node->prev[i] == curr_node, "next/prev don't match");

      Slice curr_key(curr_node->GetKey(), curr_node->key_size);
      Slice right_key(right_node->GetKey(), right_node->key_size);
      int cmp = curr_key.compare(right_key);
      if (cmp == 0) {
        RAW_CHECK(curr_node == &head_, "duplicate key");
      } else {
        RAW_CHECK(cmp < 0 || right_node == &tail_, "left > right");
      }
      curr_node = GetNext(curr_node, i);
    }
    if (print) {
      std::cout << "->TAIL" << std::endl;
    }
  }
}

}  // namespace pmwcas
