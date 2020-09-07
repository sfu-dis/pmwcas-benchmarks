#pragma once
// #include "common/allocator_internal.h"
#include <pmwcas.h>

#include "../utils/random_number_generator.h"
#include "pm_alloc_helper.h"

/// PCAS variants (i.e., CASDSkipList with PMEM defined): 
// #define MwCASSafeAlloc 1  // Use single-word PMwCAS for safe allocation/deallocation (i.e., PCAS implemented as single-word PMwCAS)
#define UsePMAllocHelper 1  // Plain PCAS (dirty bit design only), only safe allocation guaranteed; pmem may leak upon failed PCAS.

namespace pmwcas {

#ifdef PMEM
#define READ(x) ResolveNodePointer(&x)
#else
#define READ(x) x
#endif

/// Describes a node in a skip list - common to all implementations.
/// The node content is laid out as:
/// [key, next, prev, etc. fields][padding to cacheline size][Key bytes][Payload
/// bytes] Key bytes starts after the padding. The first key byte is pointed to
/// by key.data_.
struct SkipListNode {
  static const uint32_t kInvalidLevel = 0;
  static const uint32_t kLevelAbondoned = ~uint32_t{0};
  // Note: next, prev, lower, and level should be put together, so a single
  // NVRAM::Flush() is enough to flush them all.
  SkipListNode* next;       // next node in the same level
  SkipListNode* prev;       // previous node in the same level
  SkipListNode* lower;      // lower level node in the same tower
  SkipListNode* upper;      // upper level node in the same tower
  volatile uint32_t level;  // levels grow upward from 1
  uint32_t payload_size;
#ifdef PMDK
  char padding[48 - 40];
#else
  char padding[64 - 40];
#endif
  Slice key;

  SkipListNode(const Slice& key_to_copy, uint32_t psize, bool copy_key = true)
      : next(nullptr),
        prev(nullptr),
        lower(nullptr),
        upper(nullptr),
        level(kInvalidLevel),
        payload_size(psize) {
    // Copy the key
    if (copy_key) {
      key.data_ = (char*)this + sizeof(*this);
      memcpy((char*)key.data_, key_to_copy.data(), key_to_copy.size());
      key.size_ = key_to_copy.size();
    } else {
      key.data_ = key_to_copy.data_;
      key.size_ = key_to_copy.size_;
    }
  }
  ~SkipListNode() {}

  inline bool IsHead() { return prev == nullptr; }
  inline bool IsTail() { return next == nullptr; }

  /// Returns the payload attached immediately after the above fields.
  /// Note that high level nodes don't have payload.
  inline char* GetPayload() {
    DCHECK(level == 1);
    return (char*)this + sizeof(*this) + key.size();
  }
  inline uint32_t Size() {
    if (key.data_ == (char*)this + sizeof(*this)) {
      return sizeof(*this) + payload_size + key.size();
    } else {
      return sizeof(*this);
    }
  }
};

#ifdef PMDK
static_assert(offsetof(SkipListNode, key) == 48);
#else
static_assert(offsetof(SkipListNode, key) == 64);
#endif

class ISkipList {
 public:
  inline static void FreeNode(void* context, void* node) {
#if defined(PMEM) && defined(UsePMAllocHelper)
    void** addr = PMAllocHelper::Get()->GetTlsPtr();
    *addr = node;
    Allocator::Get()->FreeAligned(addr);
#else
    Allocator::Get()->FreeAligned(&node);
#endif
  }

  ISkipList(int sync, uint32_t max_height);

  ISkipList(int sync_method, uint32_t max_height, SkipListNode* head,
            SkipListNode* tail)
      : sync_method_(sync_method),
        max_height_(max_height),
        head_(head),
        tail_(tail) {}

  /// Insert [key, value] to the skip list.
  virtual Status Insert(const Slice& key, const Slice& value,
                        bool already_protected) = 0;

  /// Delete [key] from the skip list.
  virtual Status Delete(const Slice& key, bool already_protected) = 0;

  /// Find the value of [key], result stored in [value].
  virtual Status Search(const Slice& key, SkipListNode** value_node,
                        bool already_protected) = 0;

  inline int GetSyncMethod() { return sync_method_; }
  inline SkipListNode* GetTail() { return tail_; }

  static const int kSyncUnknown = 0;
  static const int kSyncMwCAS = 1;
  static const int kSyncCAS = 2;
  static const int kSyncPCAS = 3;

 public:
  /// Indicates the node pointed to is leaving the list (used on
  /// SkipListNode.next)
  static const uint64_t kNodeDeleted = ((uint64_t)1 << 60);

  int sync_method_;
  /// The number of head/tail sentinel nodes to preallocate. Search starts from
  /// the height_-th head node.
  uint32_t max_height_;

  SkipListNode* head_;  // top node of the left-most tower, search starts here
  SkipListNode* tail_;  // top node of the right-most tower, search ends here
};

struct DSkipListCursor;

class DSkipList : public ISkipList {
  friend struct DSkipListCursor;

  struct PathStack {
    static const uint32_t kMaxFrames = 128;
    SkipListNode* frames[kMaxFrames];
    uint32_t count;

    PathStack() : count(0) {}
    SkipListNode* operator[](uint32_t index) { return frames[index]; }
    inline void Reset() { count = 0; }
    inline void Push(SkipListNode* node) {
      RAW_CHECK(count + 1 < kMaxFrames, "too many frames");
      frames[count] = node;
      count++;
    }
    inline SkipListNode* Pop() { return frames[--count]; }
    inline uint32_t Size() { return count; }
  };

 protected:
#ifdef PMEM
  static const uint64_t kDirtyFlag = Descriptor::kDirtyFlag;
  inline SkipListNode* ReadPersist(SkipListNode** node) {
    // DCHECK(GetSyncMpethod() == kSyncPCAS);
    auto* node_ptr = *node;
    if ((uint64_t)node_ptr & kDirtyFlag) {
      NVRAM::Flush(sizeof(uint64_t), (void*)node);
      CompareExchange64Ptr(
          node, (SkipListNode*)((uint64_t)node_ptr & ~kDirtyFlag), node_ptr);
    }
    return (SkipListNode*)((uint64_t)node_ptr & ~kDirtyFlag);
  }
#else
  static const uint64_t kDirtyFlag = 0;
#endif

  Status Find(const Slice& key, SkipListNode** value_node);

  /// Thread-local list of all level 2 and higher nodes visited during a Find
  /// operation
  inline PathStack* GetTlsPathStack() {
    thread_local PathStack stack;
    return &stack;
  }

 public:
  DSkipList(int sync, uint32_t initial_max_height)
      : ISkipList(sync, initial_max_height) {
    tail_->prev = head_;
  }

  DSkipList(int sync_method, uint32_t max_height, SkipListNode* head,
            SkipListNode* tail)
      : ISkipList(sync_method, max_height, head, tail) {}

  void Print();

  /// XXX(tzwang): It appears calling the GetValueProtected function is quite
  /// expensive for skiplist; take a look first then decide if we call it.
  inline SkipListNode* ResolveNodePointer(SkipListNode** node) {
    SkipListNode* n = *node;
    if (MwcTargetField<uint64_t>::IsCleanPtr((uint64_t)n)) {
      return n;
    }
    return (
        SkipListNode*)(((MwcTargetField<uint64_t>*)node)->GetValueProtected());
  }

  inline SkipListNode* GetHead() { return READ(head_); }
  virtual SkipListNode* GetNext(SkipListNode* node) = 0;
  virtual SkipListNode* GetPrev(SkipListNode* node) = 0;
  virtual EpochManager* GetEpoch() = 0;
  virtual bool DeleteNode(SkipListNode* node) = 0;
  static inline SkipListNode* CleanPtr(SkipListNode* node) {
    return (SkipListNode*)((uint64_t)node & ~kNodeDeleted);
  }
};

/// A lock-free skip list implementation using doubly-linked list.
/// If PMEM is set, the list will use MwCAS for safe memory ownership transfer
/// and installing new node, instead of using CAS.
///
/// Memory recycling: happens for failed inserts and deletes only.
/// For failed inserts: if PMEM is set then the MwCAS will do it automatically
/// as part of the failure rollback protocol; otherwise we put the node in the
/// dedicated garbage list for recycling.
/// For deletes, recycling happens when we change the predecessor node's next
/// pointer to point to a new successor; again if PMEM is set then it's handled
/// by MwCAS.
class CASDSkipList : public DSkipList {
 public:
#ifdef PMEM
#if defined(MwCASSafeAlloc)
  CASDSkipList(uint32_t initial_max_height, DescriptorPool* pool)
      : DSkipList(kSyncPCAS, initial_max_height), descriptor_pool_(pool) {}

  CASDSkipList(int sync_method, uint32_t max_height, SkipListNode* head,
               SkipListNode* tail, DescriptorPool* pool)
      : DSkipList(sync_method, max_height, head, tail),
        descriptor_pool_(pool) {}
#else
  CASDSkipList(uint32_t initial_max_height)
      : DSkipList(kSyncPCAS, initial_max_height) {
    Status s = epoch_.Initialize();
    RAW_CHECK(s.ok(), "epoch init failure");
  }
  CASDSkipList(int sync_method, uint32_t max_height, SkipListNode* head,
               SkipListNode* tail)
      : DSkipList(sync_method, max_height, head, tail) {}
#endif
#else
  CASDSkipList(uint32_t initial_max_height)
      : DSkipList(kSyncCAS, initial_max_height) {
    Status s = epoch_.Initialize();
    RAW_CHECK(s.ok(), "epoch init failure");
  }
  CASDSkipList(int sync_method, uint32_t max_height, SkipListNode* head,
               SkipListNode* tail)
      : DSkipList(sync_method, max_height, head, tail) {}
#endif

  virtual Status Insert(const Slice& key, const Slice& value,
                        bool already_protected) override;
  virtual Status Delete(const Slice& key, bool already_protected) override;
  virtual inline EpochManager* GetEpoch() override {
#if defined(PMEM) && defined(MwCASSafeAlloc)
    return descriptor_pool_->GetEpoch();
#else
    return &epoch_;
#endif
  }

  /// Set the deleted bit on the given node
  inline void MarkNodePointer(SkipListNode** node) {
#ifdef PMEM
    uint64_t flags = kNodeDeleted | kDirtyFlag;
#else
    uint64_t flags = kNodeDeleted;
#endif
    DCHECK(node != &head_->next);
    DCHECK(node != &tail_->prev);
    while (true) {
#ifdef PMEM
      SkipListNode* node_ptr = ResolveNodePointer(node);
#else
      SkipListNode* node_ptr = *node;
#endif
      if ((uint64_t)node_ptr & kNodeDeleted) {
        return;
      }
      auto* desired = (SkipListNode*)((uint64_t)node_ptr | flags);
      if (node_ptr == CompareExchange64Ptr(node, desired, node_ptr)) {
#ifdef PMEM
        ReadPersist(node);
#endif
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
  inline virtual SkipListNode* GetNext(SkipListNode* node) override {
    return CleanPtr(READ(node->next));
  }
  virtual SkipListNode* GetPrev(SkipListNode* node) override;

  inline virtual Status Search(const Slice& key, SkipListNode** value_node,
                               bool already_protected) override {
    EpochGuard guard(GetEpoch(), !already_protected);
    return Find(key, value_node);
  }

 public:
#if defined(PMEM) && defined(MwCASSafeAlloc)
  DescriptorPool* descriptor_pool_;
#else
  EpochManager epoch_;
  // Thread-local garbage_list to avoid contention under high core counts (eg
  // >18)
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
#endif

  /// Helper function similar to (almost the same) as the one in CAS-based
  /// doubly-linked list.
  SkipListNode* CorrectPrev(SkipListNode* prev, SkipListNode* node);

  /// Helper function for Insert() that inserts higher level (>1) nodes.
  void FinishInsert(SkipListNode* node);

  /// Helper function for Delete() to delete a specific node; works at any tower
  /// level.
  virtual bool DeleteNode(SkipListNode* node) override;
};

/// A lock-free skip list implementation using doubly-linked list and multi-word
/// CAS
class MwCASDSkipList : public DSkipList {
 public:
  DescriptorPool* descriptor_pool_;

 public:
  MwCASDSkipList(uint32_t initial_max_height, DescriptorPool* pool)
      : DSkipList(kSyncMwCAS, initial_max_height), descriptor_pool_(pool) {}

  MwCASDSkipList(int sync_method, uint32_t max_height, SkipListNode* head,
                 SkipListNode* tail, DescriptorPool* pool)
      : DSkipList(sync_method, max_height, head, tail),
        descriptor_pool_(pool) {}

  virtual Status Insert(const Slice& key, const Slice& value,
                        bool already_protected) override;
  virtual Status Delete(const Slice& key, bool already_protected) override;

  virtual inline EpochManager* GetEpoch() override {
    return descriptor_pool_->GetEpoch();
  }

  /// Figure out the next node still valid after [node]
  virtual inline SkipListNode* GetNext(SkipListNode* node) override {
    node = (SkipListNode*)((uint64_t)node & ~kNodeDeleted);
    SkipListNode* next = ResolveNodePointer(&node->next);
    return (SkipListNode*)((uint64_t)next & ~kNodeDeleted);
  }

  virtual inline SkipListNode* GetPrev(SkipListNode* node) override {
    node = (SkipListNode*)((uint64_t)node & ~kNodeDeleted);
    SkipListNode* prev = ResolveNodePointer(&node->prev);
    return (SkipListNode*)((uint64_t)prev & ~kNodeDeleted);
  }

  inline virtual Status Search(const Slice& key, SkipListNode** value_node,
                               bool already_protected) override {
    EpochGuard guard(GetEpoch(), !already_protected);
    return Find(key, value_node);
  }

 private:
  /// Helper function for Insert() that inserts higher level (>1) nodes.
  void FinishInsert(SkipListNode* node);

  /// Helper function for Delete() to delete a specific node; works at any tower
  /// level.
  virtual bool DeleteNode(SkipListNode* node) override;
};

/// Cursor for both forward and backward scan
struct DSkipListCursor {
  DSkipList* list;
  SkipListNode* curr_node;
  bool unprot;

  inline SkipListNode* Next() {
    curr_node = list->GetNext(curr_node);
    return curr_node;
  }
  inline SkipListNode* Prev() {
    RAW_CHECK(curr_node->level == 1, "invalid level");
    if (curr_node->IsHead()) {
      return nullptr;
    }
    curr_node = list->GetPrev(curr_node);
    return curr_node;
  }

  /// Start from [key] till the end of the list
  DSkipListCursor(DSkipList* list, const Slice& key, bool already_protected)
      : list(list), curr_node(nullptr), unprot(false) {
    if (!already_protected) {
      list->GetEpoch()->Protect();
      unprot = true;
    }
    auto s = list->Find(key, &curr_node);
    RAW_CHECK(curr_node, "invalid cursor start node");
    while (curr_node->level > 1) {
      curr_node = curr_node->lower;
    }
    RAW_CHECK(curr_node->level == 1, "invalid level");
  }

  /// Start from the very end/beginning
  DSkipListCursor(DSkipList* list, bool already_protected, bool reverse)
      : list(list), curr_node(nullptr), unprot(false) {
#ifdef PMEM
    curr_node = reverse ? list->ResolveNodePointer(&list->tail_)
                        : list->ResolveNodePointer(&list->head_);
#else
    curr_node = reverse ? list->tail_ : list->head_;
#endif
    if (!already_protected) {
      list->GetEpoch()->Protect();
      unprot = true;
    }
    RAW_CHECK(curr_node, "invalid cursor start node");
    while (curr_node->level > 1) {
      curr_node = curr_node->lower;
    }
    RAW_CHECK(curr_node->level == 1, "invalid level");
  }
  ~DSkipListCursor() {
    if (unprot) {
      list->GetEpoch()->Unprotect();
    }
  }
};
}  // namespace pmwcas
