#pragma once
#include <pmwcas.h>

#include "../utils/random_number_generator.h"

namespace pmwcas {

#define SKIPLIST_MAX_HEIGHT 32

/// Describes a node in a skip list - common to all implementations.
/// The first key byte is pointed to by key.data_.
/// Payload follows the key
struct SkipListNode {
  static const uint32_t kInvalidHeight = 0;
  /// Indicates the node pointed to is leaving the list (used on
  /// SkipListNode.next)
  static const uint64_t kNodeDeleted = ((uint64_t)1 << 60);

  // Note: next, prev, lower, and level should be put together, so a single
  // NVRAM::Flush() is enough to flush them all.
  SkipListNode *next[SKIPLIST_MAX_HEIGHT];   // next node in the same level
  SkipListNode *prev[SKIPLIST_MAX_HEIGHT];   // previous node in the same level
  volatile uint32_t height;  // levels grow upward from 1
  uint32_t key_size;
  uint32_t payload_size;
  char data[0];

  SkipListNode() : key_size(0), payload_size(0), height(kInvalidHeight) { 
    memset(next, 0, sizeof(SkipListNode *) * SKIPLIST_MAX_HEIGHT);
    memset(prev, 0, sizeof(SkipListNode *) * SKIPLIST_MAX_HEIGHT);
  }

  SkipListNode(const Slice& key_to_copy, const Slice& value_to_copy, uint32_t initial_height) 
    : SkipListNode() {
    // Copy the key
    memcpy(data, key_to_copy.data(), key_to_copy.size());
    key_size = key_to_copy.size();

    // Copy the value
    memcpy(data + key_size, value_to_copy.data(), value_to_copy.size());
    payload_size = value_to_copy.size();

    height = initial_height;
  }

  ~SkipListNode() {}

  inline char* GetKey() {
    if (key_size > 0) {
      return data;
    }
    return nullptr;
  }

  /// Returns the payload attached immediately after the above fields.
  /// Note that high level nodes don't have payload.
  inline char* GetPayload() { return data + key_size; }

  /// Returns the size of this node, including key and payload
  inline uint32_t Size() { return sizeof(*this) + payload_size + key_size; }

  /// Deallocates a node
  inline static void FreeNode(void* context, void* node) {
    Allocator::Get()->FreeAligned(&node);
  }
};

class CASDSkipList {
 public:
  CASDSkipList();
  ~CASDSkipList() {}
  void SanityCheck(bool print = false);

  inline EpochManager* GetEpoch() { return &epoch_; }

  /// Insert [key, value] to the skip list.
  Status Insert(const Slice& key, const Slice& value, bool already_protected);

  /// Delete [key] from the skip list.
  Status Delete(const Slice& key, bool already_protected);

  /// Find the value of [key], result stored in [*value_node].
  Status Search(const Slice& key, SkipListNode **value_node, bool already_protected);

  /// Helper functions
  static inline SkipListNode *CleanPtr(SkipListNode *node) {
    return (SkipListNode *)((uint64_t)node & ~SkipListNode::kNodeDeleted);
  }

  /// Helper function to setup [node].prev properly
  /// @prev: a "suggested" predecessor of [node] that would allow the algorithm
  ///        to locate the true predecessor of [node]; might be an old
  ///        predecessor that points to another node which points to [node]. 
  ///
  /// Returns the predecessor set for [node] on node.prev
  SkipListNode *CorrectPrev(SkipListNode *prev, SkipListNode *node, uint16_t level);

  inline bool NodeIsDeleted(SkipListNode *node) {
    return ((uint64_t)node->next & SkipListNode::kNodeDeleted);
  }

  inline void MarkNodePointer(SkipListNode **node) {
    uint64_t flags = SkipListNode::kNodeDeleted;
    while (true) {
      SkipListNode *node_ptr = *node;
      if ((uint64_t)node_ptr & SkipListNode::kNodeDeleted) {
        return;
      }
      auto desired = (SkipListNode *)((uint64_t)node_ptr | flags);
      if (node_ptr == CompareExchange64(node, desired, node_ptr)) {
        return;
      }
    }
  }

  /// Figure out the next node still valid after [node]
  /// XXX(tzwang): 20170119: Under PCAS, looking at next.next takes quite a lot
  /// of cycles (>15%, it uses mwcas read so need to check flags etc), making
  /// it much slower than other variants. Just return the next node for now,
  /// the caller knows how to handle it anyway. In skip_list.cc the code that
  /// follows the original paper is commented out.
  inline SkipListNode *GetNext(SkipListNode *node, uint32_t level_idx) {
    return CleanPtr(node->next[level_idx]);
  }

  static const uint64_t kDirtyFlag = 0;

  /// [*value_node] points to the found node, or if not found, the predecessor node
  Status Traverse(const Slice& key, SkipListNode **value_node);

  struct PathStack {
    static const uint32_t kMaxFrames = 128;
    SkipListNode *frames[kMaxFrames];
    uint32_t count;

    PathStack() : count(0) {}
    SkipListNode *operator[](uint32_t index) { return frames[index]; }
    inline void Reset() { count = 0; }
    inline void Push(SkipListNode *node) {
      RAW_CHECK(count + 1 < kMaxFrames, "too many frames");
      RAW_CHECK(node, "pushing nullptr into path stack");
      frames[count] = node;
      count++;
    }
    inline SkipListNode *Pop() { return frames[--count]; }
    inline uint32_t Size() { return count; }
  };

  /// Thread-local list of all level 2 and higher nodes visited during a Find
  /// operation
  inline PathStack* GetTlsPathStack() {
    thread_local PathStack stack;
    return &stack;
  }

 private:
  SkipListNode head_;  // head node, search starts here
  SkipListNode tail_;  // tail node, search ends here
  EpochManager epoch_;
  uint64_t height;
};

}  // namespace pmwcas
