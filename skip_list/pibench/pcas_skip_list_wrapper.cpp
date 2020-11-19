#include "pcas_skip_list_wrapper.hpp"

extern "C" tree_api* create_tree(const tree_options_t& opt) {
  return new pcas_skip_list_wrapper(opt);
}

pcas_skip_list_wrapper::pcas_skip_list_wrapper(const tree_options_t& opt)
    : options_(opt) {
  // TODO(shiges): volatile only for now
  pmwcas::InitLibrary(
      pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
      pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
  slist_ = new pmwcas::CASDSkipList();
}

pcas_skip_list_wrapper::~pcas_skip_list_wrapper() {
  delete slist_;
  // FIXME(shiges): Thread::ClearRegistry()?
}

bool pcas_skip_list_wrapper::find(const char* key, size_t key_sz,
                                  char* value_out) {
  pmwcas::SkipListNode* vnode = nullptr;
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
  // TODO(shiges): implementation
  return false;
}

int pcas_skip_list_wrapper::scan(const char* key, size_t key_sz, int scan_sz,
                                 char*& values_out) {
  // TODO(shiges): implementation
  return false;
}
