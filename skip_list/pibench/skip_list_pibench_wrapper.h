
#pragma once

#include <sys/stat.h>

#include "../skip_list.h"
#include "glog/logging.h"
#include "tree_api.hpp"

static constexpr size_t kPayloadSize = sizeof(uint64_t);

inline bool FileExists(const char *pool_path) {
  struct stat buffer;
  return (stat(pool_path, &buffer) == 0);
}

template <typename T>
class skip_list_wrapper : public tree_api {
 public:
  virtual ~skip_list_wrapper();

  virtual bool find(const char *key, size_t key_sz, char *value_out) override final;
  virtual bool insert(const char *key, size_t key_sz, const char *value,
                      size_t value_sz) override final;
  virtual bool update(const char *key, size_t key_sz, const char *value,
                      size_t value_sz) override final;
  virtual bool remove(const char *key, size_t key_sz) override final;
  virtual int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) override final;

  bool recovery(const tree_options_t &opt);

 protected:
  T *list;
};

struct PMDKRootObj {
  pmwcas::DescriptorPool* desc_pool_{nullptr};
  pmwcas::DSkipList* list_{nullptr};
};

/*
skip_list_wrapper::skip_list_wrapper(const tree_options_t &opt) {
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create("/mnt/pmem0/geshi/skip_list_pibench_pool",
                                                    "skip_list_layout",
                                                    static_cast<uint64_t >(1024) * 1024 * 1024 * 1),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);

  auto allocator = reinterpret_cast<pmwcas::PMDKAllocator*>(pmwcas::Allocator::Get());
  PMDKRootObj* root_obj_ = reinterpret_cast<PMDKRootObj*>(
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
bool skip_list_wrapper<T>::find(const char *key, size_t key_sz, char *value_out) {
  pmwcas::SkipListNode* vnode = nullptr;
  auto Key = pmwcas::Slice(key, key_sz);
  bool ok = list->Search(Key, &vnode, false).ok();
  if (ok) {
    memcpy(value_out, vnode->GetPayload(), kPayloadSize);
    return true;
  } else {
    return false;
  }
}

template <typename T>
bool skip_list_wrapper<T>::insert(const char *key, size_t key_sz, const char *value, size_t value_sz) {
  auto Key = pmwcas::Slice(key, key_sz);
  auto Value = pmwcas::Slice(value, value_sz);
  return list->Insert(Key, Value, false).ok();
}

template <typename T>
bool skip_list_wrapper<T>::update(const char *key, size_t key_sz, const char *value, size_t value_sz) {
  LOG(FATAL) << "update is not implemented yet!" << std::endl;
  return false;
}

template <typename T>
bool skip_list_wrapper<T>::remove(const char *key, size_t key_sz) {
  auto Key = pmwcas::Slice(key, key_sz);
  return list->Delete(Key, false).ok();
}

template <typename T>
int skip_list_wrapper<T>::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) {
  LOG(FATAL) << "scan is not implemented yet!" << std::endl;
  return false;
}
