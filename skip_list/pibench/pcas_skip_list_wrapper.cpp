#include "pcas_skip_list_wrapper.hpp"

inline static bool FileExists(const char *pool_path) {
  struct stat buffer;
  return (stat(pool_path, &buffer) == 0);
}

extern "C" tree_api* create_tree(const tree_options_t& opt) {
  return new pcas_skip_list_wrapper(opt);
}

pcas_skip_list_wrapper::pcas_skip_list_wrapper(const tree_options_t& opt)
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
  auto root_obj = reinterpret_cast<pcas_skip_list_wrapper_pmdk_obj *>(
      allocator->GetRoot(sizeof(pcas_skip_list_wrapper_pmdk_obj)));
  if (recovery) {
    slist_ = root_obj->list_;
    // XXX(shiges): Hack the epoch manager
    new (slist_->GetEpoch()) pmwcas::EpochManager;
    pmwcas::Status s = slist_->GetEpoch()->Initialize();
    RAW_CHECK(s.ok(), "epoch init failure");
    pmwcas::PMAllocHelper::Get()->Initialize(&slist_->table_);
  } else {
    allocator->AllocateOffset(reinterpret_cast<uint64_t *>(&root_obj->list_),
                              sizeof(pmwcas::CASDSkipList), false);
    slist_ = root_obj->list_;
    new (slist_) pmwcas::CASDSkipList;
    pmwcas::PMAllocHelper::Get()->Initialize(&slist_->table_);
  }
#else
  pmwcas::InitLibrary(
      pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
      pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
  slist_ = new pmwcas::CASDSkipList();
#endif
}

pcas_skip_list_wrapper::~pcas_skip_list_wrapper() {
  std::cout << "Sanity checking...\n";
  {
    pmwcas::EpochGuard guard(slist_->GetEpoch());
    slist_->SanityCheck(false);
  }
  pmwcas::Thread::ClearRegistry(true);
}

bool pcas_skip_list_wrapper::find(const char* key, size_t key_sz,
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

bool pcas_skip_list_wrapper::insert(const char* key, size_t key_sz,
                                    const char* value, size_t value_sz) {
  pmwcas::Slice k(key, key_sz);
  pmwcas::Slice v(value, value_sz);
  return slist_->Insert(k, v, false).ok();
}

bool pcas_skip_list_wrapper::update(const char* key, size_t key_sz,
                                    const char* value, size_t value_sz) {
  // TODO(shiges): implementation
  return false;
}

bool pcas_skip_list_wrapper::remove(const char* key, size_t key_sz) {
  pmwcas::Slice k(key, key_sz);
  return slist_->Delete(k, false).ok();
}

int pcas_skip_list_wrapper::scan(const char* key, size_t key_sz, int scan_sz,
                                 char*& values_out) {
  // XXX(shiges): We simply keep a 4KB buffer here. A key-value
  // pair in our normal workload takes 16B and we only scan for
  // 100 entries, so it should be large enough.
  static const size_t kBufferSize = 4096;
  thread_local char buffer[kBufferSize];

  char *dst = buffer;
  int scanned = 0;

  pmwcas::Slice k(key, key_sz);
  pmwcas::CASDSkipListCursor cursor(slist_, k, false);
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
