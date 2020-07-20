#include "skip_list_pibench_wrapper.h"

struct pcas_skip_list_wrapper : skip_list_wrapper<pmwcas::CASDSkipList> {
  pcas_skip_list_wrapper(const tree_options_t &);
};

pcas_skip_list_wrapper::pcas_skip_list_wrapper(const tree_options_t &opt) {
  bool recovery = FileExists(opt.pool_path.c_str());
  pmwcas::InitLibrary(
      pmwcas::PMDKAllocator::Create(opt.pool_path.c_str(), "skip_list_layout",
                                    opt.pool_size),
      pmwcas::PMDKAllocator::Destroy, pmwcas::LinuxEnvironment::Create,
      pmwcas::LinuxEnvironment::Destroy);

  auto allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());

  PMDKRootObj *root_obj_ =
      reinterpret_cast<PMDKRootObj *>(allocator->GetRoot(sizeof(PMDKRootObj)));

#if defined(PMEM) && defined(UsePMAllocHelper)
  pmwcas::PMAllocHelper::Get()->Initialize(&root_obj_->table_);
#endif

  if (recovery) {
    std::cout << "recovery from existing pool." << std::endl;

#if defined(PMEM) && defined(MwCASSafeAlloc)
    auto pool = root_obj_->desc_pool_;
    pool->Recovery(false);
#endif

    list = static_cast<pmwcas::CASDSkipList *>(root_obj_->list_);

    // XXX(shiges): hack: fix the vptr
    void *tmp_space = malloc(sizeof(pmwcas::CASDSkipList));
    memcpy(tmp_space, (void *)list, sizeof(pmwcas::CASDSkipList));
    pmwcas::CASDSkipList *prev = (pmwcas::CASDSkipList *)tmp_space;
#if defined(PMEM) && defined(MwCASSafeAlloc)
    new (list)
        pmwcas::CASDSkipList(prev->sync_method_, prev->max_height_, prev->head_,
                             prev->tail_, prev->descriptor_pool_);
#else
    new (list) pmwcas::CASDSkipList(prev->sync_method_, prev->max_height_,
                                    prev->head_, prev->tail_);
#endif
    free(tmp_space);
  } else {
    std::cout << "creating new tree on pool." << std::endl;

#if defined(PMEM) && defined(MwCASSafeAlloc)
    pmwcas::Allocator::Get()->Allocate((void **)&root_obj_->desc_pool_,
                                       sizeof(pmwcas::DescriptorPool));

    new (root_obj_->desc_pool_)
        pmwcas::DescriptorPool(100000, opt.num_threads, false);
    auto pool = root_obj_->desc_pool_;
#endif
    pmwcas::Allocator::Get()->Allocate((void **)&(root_obj_->list_),
                                       sizeof(pmwcas::CASDSkipList));
#if defined(PMEM) && defined(MwCASSafeAlloc)
    list = new (root_obj_->list_) pmwcas::CASDSkipList(32, pool);
#else
    list = new (root_obj_->list_) pmwcas::CASDSkipList(32);
#endif
  }
}

extern "C" tree_api *create_tree(const tree_options_t &opt) {
  return new pcas_skip_list_wrapper(opt);
}
