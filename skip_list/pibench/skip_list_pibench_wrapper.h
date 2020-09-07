#pragma once

#include <sys/stat.h>

#include <tree_api.hpp>

#include "../skip_list.h"
#include "glog/logging.h"

static constexpr size_t kPayloadSize = sizeof(uint64_t);

inline bool FileExists(const char *pool_path) {
  struct stat buffer;
  return (stat(pool_path, &buffer) == 0);
}

template <typename T>
class skip_list_wrapper : public tree_api {
 public:
  virtual ~skip_list_wrapper();

  virtual bool find(const char *key, size_t key_sz,
                    char *value_out) override final;
  virtual bool insert(const char *key, size_t key_sz, const char *value,
                      size_t value_sz) override final;
  virtual bool update(const char *key, size_t key_sz, const char *value,
                      size_t value_sz) override final;
  virtual bool remove(const char *key, size_t key_sz) override final;
  virtual int scan(const char *key, size_t key_sz, int scan_sz,
                   char *&values_out) override final;

  bool recovery(const tree_options_t &opt);

 protected:
  T *list;

  const uint64_t kProtectCountThreshold = 100;

  void ensure_protected() {
    thread_local uint64_t protect_count{0};

    if (protect_count == 0) {
      list->GetEpoch()->Protect();
    }

    protect_count++;
    if (protect_count == kProtectCountThreshold) {
      list->GetEpoch()->Unprotect();
      protect_count = 1;
      list->GetEpoch()->Protect();
    }
  }
};

struct PMDKRootObj {
  pmwcas::DescriptorPool *desc_pool_{nullptr};
  pmwcas::DSkipList *list_{nullptr};
#ifdef UsePMAllocHelper
  pmwcas::PMAllocTable *table_{nullptr};
#endif
};

/*
skip_list_wrapper::skip_list_wrapper(const tree_options_t &opt) {
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create("/mnt/pmem0/geshi/skip_list_pibench_pool",
                                                    "skip_list_layout",
                                                    static_cast<uint64_t >(1024)
* 1024 * 1024 * 1), pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);

  auto allocator =
reinterpret_cast<pmwcas::PMDKAllocator*>(pmwcas::Allocator::Get()); PMDKRootObj*
root_obj_ = reinterpret_cast<PMDKRootObj*>(
      allocator->GetRoot(sizeof(PMDKRootObj)));

  pmwcas::Allocator::Get()->Allocate((void**)&root_obj_->desc_pool_,
                              sizeof(pmwcas::DescriptorPool));
  new (root_obj_->desc_pool_)
      pmwcas::DescriptorPool(100000, opt.num_threads, false);
  auto pool = root_obj_->desc_pool_;
  pmwcas::Allocator::Get()->Allocate(
      (void**)&(root_obj_->list_),
      sizeof(pmwcas::CASDSkipList));
  list = new (root_obj_->list_) pmwcas::CASDSkipList(32, pool);
}
*/

template <typename T>
skip_list_wrapper<T>::~skip_list_wrapper() {
  pmwcas::Thread::ClearRegistry();
}

template <typename T>
bool skip_list_wrapper<T>::find(const char *key, size_t key_sz,
                                char *value_out) {
  ensure_protected();
  pmwcas::SkipListNode *vnode = nullptr;
  auto Key = pmwcas::Slice(key, key_sz);
  bool ok = list->Search(Key, &vnode, true).ok();
  if (ok) {
    memcpy(value_out, vnode->GetPayload(), kPayloadSize);
    return true;
  } else {
    return false;
  }
}

template <typename T>
bool skip_list_wrapper<T>::insert(const char *key, size_t key_sz,
                                  const char *value, size_t value_sz) {
  ensure_protected();
  auto Key = pmwcas::Slice(key, key_sz);
  auto Value = pmwcas::Slice(value, value_sz);
  return list->Insert(Key, Value, true).ok();
}

template <typename T>
bool skip_list_wrapper<T>::update(const char *key, size_t key_sz,
                                  const char *value, size_t value_sz) {
  LOG(FATAL) << "update is not implemented yet!" << std::endl;
  return false;
}

template <typename T>
bool skip_list_wrapper<T>::remove(const char *key, size_t key_sz) {
  ensure_protected();
  auto Key = pmwcas::Slice(key, key_sz);
  return list->Delete(Key, true).ok();
}

template <typename T>
int skip_list_wrapper<T>::scan(const char *key, size_t key_sz, int scan_sz,
                               char *&values_out) {
  LOG(FATAL) << "scan is not implemented yet!" << std::endl;
  return false;
}
