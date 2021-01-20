#pragma once
#include <pmwcas.h>

#include "../utils/random_number_generator.h"
#include "pm_alloc_helper.h"

namespace pmwcas {

#define SKIPLIST_MAX_HEIGHT 32

#define PERSISTENT_CAS_ADR 1

#ifdef PMEM
namespace persistent_ptr {
static constexpr uint64_t kDirtyFlag = pmwcas::Descriptor::kDirtyFlag;

inline uint64_t read(uint64_t* addr) {
  uint64_t value = *addr;
  if (value & kDirtyFlag) {
    NVRAM::Flush(sizeof(uint64_t), addr);
    CompareExchange64(addr, value & ~kDirtyFlag, value);
  }
  return value & ~kDirtyFlag;
}

inline uint64_t pcas(uint64_t* addr, uint64_t expected, uint64_t desired) {
  uint64_t dirty_desired = desired | kDirtyFlag;
retry:
  auto value = CompareExchange64(addr, dirty_desired, expected);
  if (value & kDirtyFlag) {
    NVRAM::Flush(sizeof(uint64_t), addr);
    CompareExchange64(addr, value & ~kDirtyFlag, value);
    goto retry;
  }

  if (value == expected) {
    NVRAM::Flush(sizeof(uint64_t), addr);
    CompareExchange64(addr, desired, dirty_desired);
  }
  return value;
}
}  // namespace persistent_ptr
#endif

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
  nv_ptr<SkipListNode> next[SKIPLIST_MAX_HEIGHT];   // next node in the same level
  nv_ptr<SkipListNode> prev[SKIPLIST_MAX_HEIGHT];   // previous node in the same level
  volatile uint32_t height;  // levels grow upward from 1
  uint32_t key_size;
  uint32_t payload_size;
  char data[0];

  SkipListNode() : key_size(0), payload_size(0), height(kInvalidHeight) { 
    memset(next, 0, sizeof(nv_ptr<SkipListNode>) * SKIPLIST_MAX_HEIGHT);
    memset(prev, 0, sizeof(nv_ptr<SkipListNode>) * SKIPLIST_MAX_HEIGHT);
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
};

#if defined(PMEM) && PERSISTENT_CAS_ADR == 1
template <typename T>
inline T ResolveNodePointer(T* addr) {
  return persistent_ptr::read(reinterpret_cast<uint64_t*>(addr));
}
#define READ(x) ResolveNodePointer(&x)

template <typename T>
inline T PersistentCompareExchange64(T* addr, T desired, T expected) {
  static_assert(sizeof(T) == sizeof(uint64_t),
                "PCAS only supports 8-byte target words");
  return persistent_ptr::pcas(reinterpret_cast<uint64_t*>(addr),
                              static_cast<uint64_t>(expected),
                              static_cast<uint64_t>(desired));
}
#define PersistentCAS(addr, desired, expected) \
  PersistentCompareExchange64(addr, desired, expected)
#else
#define READ(x) x
#define PersistentCAS(addr, desired, expected) \
  CompareExchange64(addr, desired, expected)
#endif

template <typename DSkipList>
struct DSkipListCursor;

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
  Status Search(const Slice& key, nv_ptr<SkipListNode> *value_node, bool already_protected);

  /// Helper functions
  static inline nv_ptr<SkipListNode> CleanPtr(nv_ptr<SkipListNode> node) {
    return (nv_ptr<SkipListNode>)((uint64_t)node & ~SkipListNode::kNodeDeleted);
  }

  /// Helper function to setup [node].prev properly
  /// @prev: a "suggested" predecessor of [node] that would allow the algorithm
  ///        to locate the true predecessor of [node]; might be an old
  ///        predecessor that points to another node which points to [node]. 
  ///
  /// Returns the predecessor set for [node] on node.prev
  nv_ptr<SkipListNode> CorrectPrev(nv_ptr<SkipListNode> prev, nv_ptr<SkipListNode> node, uint16_t level);

  inline void MarkNodePointer(nv_ptr<SkipListNode> *node) {
    uint64_t flags = SkipListNode::kNodeDeleted;
    while (true) {
      nv_ptr<SkipListNode> node_ptr = READ(*node);
      if ((uint64_t)node_ptr & SkipListNode::kNodeDeleted) {
        return;
      }
      auto desired = (nv_ptr<SkipListNode>)((uint64_t)node_ptr | flags);
      if (node_ptr == PersistentCAS(node, desired, node_ptr)) {
        return;
      }
    }
  }

  /// Allocate a node
  inline void AllocateNode(nv_ptr<SkipListNode> *node, uint32_t key_size, uint32_t value_size) {
#ifdef PMEM
  auto allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
  uint64_t* tls_addr = (uint64_t*)PMAllocHelper::Get()->GetTlsPtr();
  allocator->AllocateOffset(
      tls_addr, sizeof(SkipListNode) + key_size + value_size, false);
  *node = nv_ptr<SkipListNode>(*tls_addr);
#else
    Allocator::Get()->AllocateAligned(
      (void**)node, sizeof(SkipListNode) + key_size + value_size,
      kCacheLineSize);
#endif
  }

  /// Deallocate a node
  inline static void FreeNode(void* context, void* node) {
#ifdef PMEM
    auto allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
    uint64_t* tls_addr = (uint64_t*)PMAllocHelper::Get()->GetTlsPtr();
    *tls_addr = (uint64_t)node;
    allocator->FreeOffset(tls_addr);
#else
    Allocator::Get()->FreeAligned(&node);
#endif
  }


  /// Figure out the next node still valid after [node]
  /// XXX(tzwang): 20170119: Under PCAS, looking at next.next takes quite a lot
  /// of cycles (>15%, it uses mwcas read so need to check flags etc), making
  /// it much slower than other variants. Just return the next node for now,
  /// the caller knows how to handle it anyway. In skip_list.cc the code that
  /// follows the original paper is commented out.
  nv_ptr<SkipListNode> GetNext(nv_ptr<SkipListNode> node, uint32_t level);

  nv_ptr<SkipListNode> GetPrev(nv_ptr<SkipListNode> node, uint32_t level);

  inline bool IsHead(nv_ptr<SkipListNode> node) const { return &head_ == node; }

  inline bool IsTail(nv_ptr<SkipListNode> node) const { return &tail_ == node; }

  /// [*value_node] points to the found node, or if not found, the predecessor node
  Status Traverse(const Slice& key, nv_ptr<SkipListNode> *value_node);

  struct PathArray {
    SkipListNode *entries_[2 * SKIPLIST_MAX_HEIGHT];
    uint64_t max_level;

    PathArray() : entries_{}, max_level(0) {}
    inline void Reset() { max_level = 0; }
    inline void Set(uint64_t level, SkipListNode *prev, SkipListNode *next) {
      DCHECK(level < SKIPLIST_MAX_HEIGHT);
      entries_[2 * level] = prev;
      entries_[2 * level + 1] = next;
      max_level = std::max(max_level, level);
    }
    inline void Get(uint64_t level, nv_ptr<SkipListNode> *prev, nv_ptr<SkipListNode> *next) {
      DCHECK(level < SKIPLIST_MAX_HEIGHT);
      *prev = entries_[2 * level];
      *next = entries_[2 * level + 1];
      max_level = std::max(max_level, level);
    }
  };

  inline PathArray* GetTlsPathArray() {
    thread_local PathArray array;
    return &array;
  }

  inline GarbageListUnsafe* GetGarbageList() {
    thread_local GarbageListUnsafe garbage_list;
    thread_local bool initialized = false;
    if (!initialized) {
      auto s = garbage_list.Initialize(&epoch_);
      RAW_CHECK(s.ok(), "garbage list init failure");
      initialized = true;
    }
    return &garbage_list;
  }

 private:
  friend class DSkipListCursor<CASDSkipList>;

  SkipListNode head_;  // head node, search starts here
  SkipListNode tail_;  // tail node, search ends here
  EpochManager epoch_;
  uint64_t height;

#ifdef PMEM
 public:
  nv_ptr<PMAllocTable> table_{nullptr};
#endif
};

template <typename DSkipList>
struct DSkipListCursor {
 public:
  DSkipListCursor(DSkipList *list, const Slice &key, bool already_protected)
      : list_(list),
        curr_(nullptr),
        guard_(list->GetEpoch(), !already_protected) {
    DCHECK(list->GetEpoch()->IsProtected());
    auto s = list->Search(key, &curr_, true);
    RAW_CHECK(curr_, "Cursor starts at invalid node");
  }

  DSkipListCursor(DSkipList *list, bool forward, bool already_protected)
      : list_(list),
        curr_(nullptr),
        guard_(list->GetEpoch(), !already_protected) {
    DCHECK(list->GetEpoch()->IsProtected());
    curr_ = forward ? &list->head_ : &list->tail_;
    RAW_CHECK(curr_, "Cursor starts at invalid node");
  }

  nv_ptr<SkipListNode> Curr() { return curr_; }

  nv_ptr<SkipListNode> Next() {
    DCHECK(list_);
    curr_ = list_->GetNext(curr_, 0);
    return curr_;
  }

  nv_ptr<SkipListNode> Prev() {
    DCHECK(list_);
    curr_ = list_->GetPrev(curr_, 0);
    return curr_;
  }

 private:
  DSkipList *list_;
  nv_ptr<SkipListNode> curr_;
  EpochGuard guard_;
};

using CASDSkipListCursor = DSkipListCursor<CASDSkipList>;

}  // namespace pmwcas
