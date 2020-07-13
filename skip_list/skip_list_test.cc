
#include "skip_list.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <vector>

#include "../utils/performance_test.h"
#include "../utils/random_number_generator.h"
#include "pmwcas.h"

using namespace pmwcas;

#ifdef PMEM
struct PMDKRootObj {
  pmwcas::DescriptorPool *desc_pool_{nullptr};
  pmwcas::DSkipList *list_{nullptr};
#ifdef UsePMAllocHelper
  pmwcas::PMAllocTable *table_{nullptr};
#endif
};
#endif

const uint32_t descriptor_pool_size = 100000;
const uint32_t initial_max_height = 32;
DEFINE_string(pmdk_pool, "skip_list_test_pool", "path to pmdk pool");

static void GenerateSliceFromInt(int64_t k, char *out) {
  int64_t swapped = _bswap64(k);
  memcpy(out, &swapped, sizeof(int64_t));
};

struct DSkipListTest : public PerformanceTest {
  const int64_t kTotalInserts = 50000;
  DSkipList *slist_;
  std::atomic<int64_t> total_inserts_;
  Barrier barrier1_;
  Barrier barrier2_;
  Barrier barrier3_;
  Barrier barrier4_;
  DSkipListTest(DSkipList *list, uint64_t thread_count)
      : PerformanceTest{},
        slist_(list),
        total_inserts_(0),
        barrier1_{thread_count},
        barrier2_{thread_count},
        barrier3_{thread_count},
        barrier4_{thread_count} {}

  void Entry(size_t thread_index) {
    // *(slist_->GetEpoch()->epoch_table_->GetTlsValuePtr()) = nullptr;
    auto key_guard = std::make_unique<char[]>(sizeof(int64_t));
    auto value_guard = std::make_unique<char[]>(sizeof(int64_t));

    if (slist_->GetSyncMethod() == ISkipList::kSyncMwCAS) {
      MwCASMetrics::ThreadInitialize();
    }

    WaitForStart();
    while (true) {
      int64_t k = total_inserts_++;
      if (k >= kTotalInserts) {
        break;
      }
      GenerateSliceFromInt(k, key_guard.get());
      Slice key(key_guard.get(), sizeof(int64_t));
      GenerateSliceFromInt(k, value_guard.get());
      Slice value(value_guard.get(), sizeof(int64_t));
      auto ret = slist_->Insert(key, value, false);
      ASSERT_TRUE(ret.ok());
    }

    barrier1_.CountAndWait();
    // GenerateSliceFromInt(0, sizeof(int64_t), true, &key);

    uint64_t nnodes = 0;
    for (uint64_t k = 0; k < kTotalInserts; ++k) {
      GenerateSliceFromInt(k, key_guard.get());
      Slice key(key_guard.get(), sizeof(int64_t));
      auto s = slist_->Search(key, nullptr, false);
      if (s.ok()) {
        nnodes++;
      }
    }
    EXPECT_EQ(nnodes, kTotalInserts);

    barrier2_.CountAndWait();

    // See if we can find them
    for (int64_t search_key = 0; search_key < kTotalInserts; search_key++) {
      GenerateSliceFromInt(search_key, key_guard.get());
      Slice key(key_guard.get(), sizeof(int64_t));
      SkipListNode *value = nullptr;
      auto ret = slist_->Search(key, &value, false);
      EXPECT_TRUE(ret.ok());
      EXPECT_NE(value, nullptr);
      ASSERT_TRUE(
          memcmp(value->GetPayload(), key.data(), value->payload_size) == 0);
      ASSERT_TRUE(value->level == 1);
    }

    // Forward scan
    // GenerateSliceFromInt(kTotalInserts - 1, sizeof(int64_t), true, &key);
    nnodes = 0;
    DSkipListCursor cursor(slist_, false, false);
    while (true) {
      SkipListNode *n = cursor.Next();
      if (n->IsTail()) {
        break;
      }
      nnodes++;
    }
    EXPECT_EQ(nnodes, kTotalInserts);

    // Reverse scan
    // GenerateSliceFromInt(kTotalInserts - 1, sizeof(int64_t), true, &key);
    nnodes = 0;
    DSkipListCursor rcursor(slist_, false, true);
    while (true) {
      SkipListNode *n = rcursor.Prev();
      if (n->IsHead()) {
        break;
      }
      nnodes++;
    }
    EXPECT_EQ(nnodes, kTotalInserts);

    if (thread_index == 0) {
      total_inserts_ = kTotalInserts;
    }
    barrier3_.CountAndWait();
    while (true) {
      int64_t k = --total_inserts_;
      if (k < 0) {
        break;
      }
      GenerateSliceFromInt(k, key_guard.get());
      Slice key(key_guard.get(), sizeof(int64_t));
      auto ret = slist_->Delete(key, false);
      ASSERT_TRUE(ret.ok());
    }
    barrier4_.CountAndWait();
  }
};

GTEST_TEST(DSkipListTest, CASSingleThreadTest) {
  auto thread_count = 1;
#ifdef PMEM
  auto allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());

  PMDKRootObj *root_obj_ =
      reinterpret_cast<PMDKRootObj *>(allocator->GetRoot(sizeof(PMDKRootObj)));

#if defined(MwCASSafeAlloc)
  pmwcas::Allocator::Get()->Allocate((void **)&root_obj_->desc_pool_,
                                     sizeof(pmwcas::DescriptorPool));

  new (root_obj_->desc_pool_)
      pmwcas::DescriptorPool(descriptor_pool_size, thread_count);
  auto pool = root_obj_->desc_pool_;
  pmwcas::Allocator::Get()->Allocate((void **)&(root_obj_->list_),
                                     sizeof(pmwcas::CASDSkipList));
  auto list =
      new (root_obj_->list_) pmwcas::CASDSkipList(initial_max_height, pool);
#else
  pmwcas::Allocator::Get()->Allocate((void **)&(root_obj_->list_),
                                     sizeof(pmwcas::CASDSkipList));
  auto list = new (root_obj_->list_) pmwcas::CASDSkipList(initial_max_height);
#endif
#else
  CASDSkipList *list = new CASDSkipList(initial_max_height);
#endif
  DSkipListTest test(list, thread_count);
  test.Run(thread_count);
  // delete list;
}
GTEST_TEST(DSkipListTest, CASConcurrentTest) {
  uint32_t thread_count =
      std::max<uint32_t>(Environment::Get()->GetCoreCount() / 2, 1);
#ifdef PMEM
  auto allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());

  PMDKRootObj *root_obj_ =
      reinterpret_cast<PMDKRootObj *>(allocator->GetRoot(sizeof(PMDKRootObj)));

#if defined(MwCASSafeAlloc)
  pmwcas::Allocator::Get()->Allocate((void **)&root_obj_->desc_pool_,
                                     sizeof(pmwcas::DescriptorPool));

  new (root_obj_->desc_pool_)
      pmwcas::DescriptorPool(descriptor_pool_size, thread_count);
  auto pool = root_obj_->desc_pool_;
  pmwcas::Allocator::Get()->Allocate((void **)&(root_obj_->list_),
                                     sizeof(pmwcas::CASDSkipList));
  auto list =
      new (root_obj_->list_) pmwcas::CASDSkipList(initial_max_height, pool);
#else
  pmwcas::Allocator::Get()->Allocate((void **)&(root_obj_->list_),
                                     sizeof(pmwcas::CASDSkipList));
  auto list = new (root_obj_->list_) pmwcas::CASDSkipList(initial_max_height);
#endif
#else
  CASDSkipList *list = new CASDSkipList(initial_max_height);
#endif
  DSkipListTest test(list, thread_count);
  test.Run(thread_count);
  // delete list;
}

GTEST_TEST(DSkipListTest, MwCASSingleThreadTest) {
  auto thread_count = 1;
#ifdef PMEM
  auto allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());

  PMDKRootObj *root_obj_ =
      reinterpret_cast<PMDKRootObj *>(allocator->GetRoot(sizeof(PMDKRootObj)));

  pmwcas::Allocator::Get()->Allocate((void **)&root_obj_->desc_pool_,
                                     sizeof(pmwcas::DescriptorPool));

  new (root_obj_->desc_pool_)
      pmwcas::DescriptorPool(descriptor_pool_size, thread_count);
  auto pool = root_obj_->desc_pool_;
  pmwcas::Allocator::Get()->Allocate((void **)&(root_obj_->list_),
                                     sizeof(pmwcas::MwCASDSkipList));
  auto list =
      new (root_obj_->list_) pmwcas::MwCASDSkipList(initial_max_height, pool);
#else
  DescriptorPool *pool = new DescriptorPool(descriptor_pool_size, thread_count);
  MwCASDSkipList *list = new MwCASDSkipList(initial_max_height, pool);
#endif
  DSkipListTest test(list, thread_count);
  test.Run(thread_count);
  // delete list;
  // delete pool;
}
GTEST_TEST(DSkipListTest, MwCASConcurrentTest) {
  uint32_t thread_count =
      std::max<uint32_t>(Environment::Get()->GetCoreCount() / 2, 1);
#ifdef PMEM
  auto allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());

  PMDKRootObj *root_obj_ =
      reinterpret_cast<PMDKRootObj *>(allocator->GetRoot(sizeof(PMDKRootObj)));

  pmwcas::Allocator::Get()->Allocate((void **)&root_obj_->desc_pool_,
                                     sizeof(pmwcas::DescriptorPool));

  new (root_obj_->desc_pool_)
      pmwcas::DescriptorPool(descriptor_pool_size, thread_count);
  auto pool = root_obj_->desc_pool_;
  pmwcas::Allocator::Get()->Allocate((void **)&(root_obj_->list_),
                                     sizeof(pmwcas::MwCASDSkipList));
  auto list =
      new (root_obj_->list_) pmwcas::MwCASDSkipList(initial_max_height, pool);
#else
  DescriptorPool *pool = new DescriptorPool(descriptor_pool_size, thread_count);
  MwCASDSkipList *list = new MwCASDSkipList(initial_max_height, pool);
#endif
  DSkipListTest test(list, thread_count);
  test.Run(thread_count);
  // delete list;
  // delete pool;
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
#ifdef PMDK
  // Start with a new pool
  system((std::string("rm ") + FLAGS_pmdk_pool).c_str());
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                          FLAGS_pmdk_pool.c_str(), "skip_list_layout",
                          static_cast<uint64_t>(1024) * 1024 * 1024 * 8),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
#else
  pmwcas::InitLibrary(
      pmwcas::TlsAllocator::Create, pmwcas::TlsAllocator::Destroy,
      pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
#endif  // PMDK

#if defined(PMEM) && defined(UsePMAllocHelper)
  auto allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  PMDKRootObj *root_obj_ =
      reinterpret_cast<PMDKRootObj *>(allocator->GetRoot(sizeof(PMDKRootObj)));
  pmwcas::PMAllocHelper::Get()->Initialize(&root_obj_->table_);
#endif
  return RUN_ALL_TESTS();
}
