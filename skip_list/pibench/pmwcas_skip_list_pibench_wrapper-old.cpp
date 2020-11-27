#include "skip_list_pibench_wrapper.h"

struct pmwcas_skip_list_wrapper : skip_list_wrapper<pmwcas::MwCASDSkipList> {
  pmwcas_skip_list_wrapper(const tree_options_t &opt);
};

pmwcas_skip_list_wrapper::pmwcas_skip_list_wrapper(const tree_options_t &opt) {
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

  if (recovery) {
    std::cout << "recovery from existing pool." << std::endl;

    auto pool = root_obj_->desc_pool_;
    pool->Recovery(0, false);

    list = static_cast<pmwcas::MwCASDSkipList *>(root_obj_->list_);

    // XXX(shiges): hack: fix the vptr
    void *tmp_space = malloc(sizeof(pmwcas::MwCASDSkipList));
    memcpy(tmp_space, (void *)list, sizeof(pmwcas::MwCASDSkipList));
    pmwcas::MwCASDSkipList *prev = (pmwcas::MwCASDSkipList *)tmp_space;
    new (list) pmwcas::MwCASDSkipList(prev->sync_method_, prev->max_height_,
                                      prev->head_, prev->tail_,
                                      prev->descriptor_pool_);
    free(tmp_space);
  } else {
    std::cout << "creating new tree on pool." << std::endl;

    pmwcas::Allocator::Get()->Allocate((void **)&root_obj_->desc_pool_,
                                       sizeof(pmwcas::DescriptorPool));

    new (root_obj_->desc_pool_)
        pmwcas::DescriptorPool(100000, opt.num_threads, false);
    auto pool = root_obj_->desc_pool_;
    pmwcas::Allocator::Get()->Allocate((void **)&(root_obj_->list_),
                                       sizeof(pmwcas::MwCASDSkipList));
    list = new (root_obj_->list_) pmwcas::MwCASDSkipList(32, pool);
  }
}

extern "C" tree_api *create_tree(const tree_options_t &opt) {
  return new pmwcas_skip_list_wrapper(opt);
}