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
}

Status CASDSkipList::Traverse(const Slice& key, SkipListNode **value_node) {
  DCHECK(GetEpoch()->IsProtected());
  auto *stack = GetTlsPathStack();
  stack->Reset();

  SkipListNode *prev_node = nullptr;
  SkipListNode *curr_node = &head_;
  int32_t curr_level_idx = height - 1;
  DCHECK((((uint64_t)head_.next[curr_level_idx] & SkipListNode::kNodeDeleted)) == 0);

  DCHECK(curr_node);
  DCHECK(curr_node->next[curr_level_idx]);

  // Drill down to the lowest level
  while (curr_level_idx > 0) {
    // Descend until the right isn't tail
    auto *right = GetNext(curr_node, curr_level_idx);
    if (right == &tail_) {
      --curr_level_idx;
      stack->Push(curr_node);
      continue;
    }

    // Look right to see if we can move there
    Slice right_key(curr_node->GetKey(), curr_node->key_size);
    int cmp = key.compare(right_key);
    if (cmp == 0) {
      prev_node = curr_node;
      curr_node = right;
      break;
    } else if (cmp < 0) {
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

Status CASDSkipList::Search(const Slice& key, SkipListNode **value_node,
                            bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);
  return Traverse(key, value_node);
}

Status CASDSkipList::Insert(const Slice& key, const Slice& value, bool already_protected) {
  EpochGuard guard(GetEpoch(), !already_protected);

  SkipListNode *left = nullptr;
  SkipListNode *right = nullptr;
  SkipListNode *node = nullptr;
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
    Allocator::Get()->AllocateAligned(
        (void**)&node, sizeof(SkipListNode) + key.size() + value.size(),
        kCacheLineSize);

    thread_local RandomNumberGenerator height_rng{};
    // Determine the height
    uint32_t h = height_rng.Generate();
    while (h & 1) {
      ++node_height;
      h >>= 1;
    }
    node_height = std::min<uint32_t>(node_height, SKIPLIST_MAX_HEIGHT);
  }
  new (node) SkipListNode(key, value, 1); //node_height);

  // Build the lowest level first
  node->next[0] = right;
  node->prev[0] = left;

  // Link with pred in the lowest level
  if (CompareExchange64(&left->next[0], node, right) != right) {
    goto retry;
  }

  // Setup [right.prev] to point to [node]
  CorrectPrev(node, right, 0);

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
      DCHECK(left == &head_ || left->height > i);
    } else {
      left = &head_;
    }
  build_tower:
    right = GetNext(left, i);
    if (right != &tail_) {
      Slice right_key(right->GetKey(), right->key_size);
      int cmp = right_key.compare(key);
      if (cmp == 0) {
        // Someone else acted faster
      } else if (cmp < 0) {
        left = right;
        goto build_tower;
      } else {
        // Good to build it, but need to see if we are the first
      }
    }

    node->prev[i] = left;
    node->next[i] = right;

    if (CompareExchange64(&left->next[i], node, right) != right) {
      // Failed, give up?
      // The filled prev[i] field shouldn't matter - node height remains the old value
      break;
    }

    // Succeeded, increment node height and setup the prev pointer
    ++node->height;
    CorrectPrev(node, right, i);
  }

  return Status::OK();
}


void CASDSkipList::CorrectPrev(SkipListNode *new_prev, SkipListNode *node, uint16_t level) {
  DCHECK(GetEpoch()->IsProtected());
  //DCHECK(((uint64_t)node & SkipListNode::kNodeDeleted) == 0);
  SkipListNode *last_link = nullptr;
  SkipListNode *prev = new_prev;

  while (true) {
    auto *link1 = node->prev[level];
    if ((uint64_t)link1 & SkipListNode::kNodeDeleted) {
      // Node already deleted
      break;
    }

    auto prev_next = prev->next[level];
    DCHECK(prev_next);
    if ((uint64_t)prev_next & SkipListNode::kNodeDeleted) {
      DCHECK(false);
      /*
      // The predecessor is deleted, mark the deleted bit on its prev field as well
      if (last_link) {
        MarkNodePointer(&prev->prev[level]);
        DCHECK((uint64_t)prev_cleared->prev & SkipListNode::kNodeDeleted);

        // Try to fix prev.prev.next to point to [node] to unlink this node ([prev])
        uint64_t desired = ((uint64_t)prev_next & ~SkipListNode::kNodeDeleted);
        if (prev == CompareExchange64(&last_link->next,
                                     (ptr<SkipListNode>)(desired | kDirtyFlag),
                                      prev)) {
          Status s = GetGarbageList()->Push(prev, DSkipList::FreeNode, nullptr);
          RAW_CHECK(s.ok(), "failed recycling node");
        }
        prev = last_link;
        last_link = nullptr;
        continue;
      }
      prev_next = (ptr<SkipListNode>)((uint64_t)READ(prev_cleared->prev) &
                                      ~SkipListNode::kNodeDeleted);
      prev = prev_next;
      DCHECK(prev);
      continue;
      */
    }

    DCHECK(((uint64_t)prev_next & SkipListNode::kNodeDeleted) == 0);
    if (prev_next != node) {
      prev = prev_next;
    } else {
      SkipListNode *p = (SkipListNode *)((uint64_t)prev & ~SkipListNode::kNodeDeleted);
      if (link1 == CompareExchange64(&node->prev[level], p, link1)) {
        if ((uint64_t)prev->prev & SkipListNode::kNodeDeleted) {
          DCHECK(false);
          continue;
        }
      } // Fine if it failed - someone else should have fixed the link
      break;
    }
  }
}

void CASDSkipList::SanityCheck(bool print) {
  for (uint16_t i = 0; i < height; ++i) {//SKIPLIST_MAX_HEIGHT; ++i) {
    SkipListNode *curr_node = &head_;
    while (curr_node != &tail_) {
      if (print) {
        if (curr_node == &head_) {
          std::cout << "HEAD";
        } else {
          std::cout << "->" << *(uint64_t*)curr_node->GetKey();
        }
      }

      SkipListNode *right_node = GetNext(curr_node, i);
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
