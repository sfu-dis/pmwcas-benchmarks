#include "pmwcas_skip_list_wrapper.hpp"

inline static bool FileExists(const char *pool_path) {
  struct stat buffer;
  return (stat(pool_path, &buffer) == 0);
}

extern "C" tree_api* create_tree(const tree_options_t& opt) {
  return new pmwcas_skip_list_wrapper(opt);
}

pmwcas_skip_list_wrapper::pmwcas_skip_list_wrapper(const tree_options_t& opt)
    : options_(opt) {
  bool recovery = FileExists(opt.pool_path.c_str());
#ifdef PMEM
  pmwcas::InitLibrary(
      pmwcas::PMDKAllocator::Create(opt.pool_path.c_str(), "skip_list_layout",
                                    opt.pool_size),
      pmwcas::PMDKAllocator::Destroy, pmwcas::LinuxEnvironment::Create,
      pmwcas::LinuxEnvironment::Destroy);
  auto allocator =
      reinterpret_cast<pmwcas::PMDKAllocator*>(pmwcas::Allocator::Get());
  auto root_obj = reinterpret_cast<pmwcas_skip_list_wrapper_pmdk_obj *>(
      allocator->GetRoot(sizeof(pmwcas_skip_list_wrapper_pmdk_obj)));
  if (recovery) {
    pool_ = root_obj->desc_pool_;
    pool_->Recovery(opt.num_threads, false);
    slist_ = root_obj->mwlist_;
  } else {
    allocator->AllocateOffset(reinterpret_cast<uint64_t *>(&root_obj->desc_pool_),
                              sizeof(pmwcas::DescriptorPool), false);
    allocator->AllocateOffset(reinterpret_cast<uint64_t *>(&root_obj->mwlist_),
                              sizeof(pmwcas::MwCASDSkipList), false);
    pool_ = root_obj->desc_pool_;
    slist_ = root_obj->mwlist_;
    new (pool_) pmwcas::DescriptorPool(100000, opt.num_threads, false);
    new (slist_) pmwcas::MwCASDSkipList(pool_);
  }
#else
  pmwcas::InitLibrary(
      pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
      pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
  
  pool_ = new pmwcas::DescriptorPool(100000, opt.num_threads, false);
  slist_ = new pmwcas::MwCASDSkipList(pool_);
#endif
}

pmwcas_skip_list_wrapper::~pmwcas_skip_list_wrapper() {
  std::cout << "Sanity checking...\n";
  {
    pmwcas::EpochGuard guard(slist_->GetEpoch());
    slist_->SanityCheck(false);
  }
  pmwcas::Thread::ClearRegistry(true);
}

bool pmwcas_skip_list_wrapper::find(const char* key, size_t key_sz,
                                  char* value_out) {
  pmwcas::nv_ptr<pmwcas::SkipListNode> vnode = nullptr;
  pmwcas::Slice k(key, key_sz);

  // FIXME(shiges): It looks like the memcpy followed by the
  // skip-list traversal should also be protected by epoch to
  // ensure reading a valid payload. Not sure about the overhead...
  pmwcas::EpochGuard guard(slist_->GetEpoch());
  bool ok = slist_->Search(k, &vnode, true).ok();
  if (ok) {
    memcpy(value_out, vnode->GetPayload(), options_.value_size);
    return true;
  } else {
    return false;
  }
}

bool pmwcas_skip_list_wrapper::insert(const char* key, size_t key_sz,
                                    const char* value, size_t value_sz) {
  pmwcas::Slice k(key, key_sz);
  pmwcas::Slice v(value, value_sz);
  return slist_->Insert(k, v, false).ok();
}

bool pmwcas_skip_list_wrapper::update(const char* key, size_t key_sz,
                                    const char* value, size_t value_sz) {
  // TODO(shiges): implementation
  return false;
}

bool pmwcas_skip_list_wrapper::remove(const char* key, size_t key_sz) {
  pmwcas::Slice k(key, key_sz);
  return slist_->Delete(k, false).ok();
}

int pmwcas_skip_list_wrapper::scan(const char* key, size_t key_sz, int scan_sz,
                                 char*& values_out) {
  // XXX(shiges): We simply keep a 4KB buffer here. A key-value
  // pair in our normal workload takes 16B and we only scan for
  // 100 entries, so it should be large enough.
  static const size_t kBufferSize = 4096;
  thread_local char buffer[kBufferSize];

  char *dst = buffer;
  int scanned = 0;

  pmwcas::Slice k(key, key_sz);
  pmwcas::MwCASDSkipListCursor cursor(slist_, k, false);
  DCHECK(slist_->GetEpoch()->IsProtected());

  if (slist_->IsHead(cursor.Curr())) {
    cursor.Next();
  }

  for (scanned = 0; scanned < scan_sz; ++scanned) {
    pmwcas::SkipListNode *curr = cursor.Curr();
    if (slist_->IsTail(curr)) {
      break;
    }

    DCHECK(curr->GetKey());
    memcpy(dst, curr->GetKey(), curr->key_size);
    dst += curr->key_size;
    DCHECK(dst - buffer <= kBufferSize);

    DCHECK(curr->GetPayload());
    memcpy(dst, curr->GetPayload(), curr->payload_size);
    dst += curr->payload_size;
    DCHECK(dst - buffer <= kBufferSize);

    cursor.Next();
  }

  values_out = buffer;
  return scanned;
}
