
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

#ifdef PMDK
DEFINE_string(pmdk_pool, "/mnt/pmem0/skip_list_test_pool", "path to pmdk pool");

struct PMDKRootObj {
  pmwcas::nv_ptr<pmwcas::CASDSkipList> list_{nullptr};
  pmwcas::nv_ptr<pmwcas::MwCASDSkipList> mwlist_{nullptr};
  pmwcas::nv_ptr<pmwcas::DescriptorPool> desc_pool_{nullptr};
};
#endif

static void GenerateSliceFromInt(int64_t k, char *out) {
  int64_t swapped = _bswap64(k);
  memcpy(out, &swapped, sizeof(int64_t));
};

template <typename DSkipList>
struct DSkipListTest : public PerformanceTest {
  const int64_t kTotalInserts = 15000;
  DSkipList *slist_;
  std::atomic<int64_t> total_inserts_;
  Barrier barrier1_;
  Barrier barrier2_;
  Barrier barrier3_;
  uint64_t thread_count;
  DSkipListTest(DSkipList *list, uint64_t thread_count)
      : PerformanceTest{},
        slist_(list),
        total_inserts_(0),
        barrier1_{thread_count},
        barrier2_{thread_count},
        barrier3_{thread_count},
        thread_count(thread_count) {}

  void Entry(size_t thread_index) {
    // *(slist_->GetEpoch()->epoch_table_->GetTlsValuePtr()) = nullptr;
    auto key_guard = std::make_unique<char[]>(sizeof(int64_t));
    auto value_guard = std::make_unique<char[]>(sizeof(int64_t));

    WaitForStart();
    while (true) {
      int64_t k = total_inserts_++;
      //LOG(INFO) << "INSERT " << k;
      if (k >= kTotalInserts) {
        break;
      }
      GenerateSliceFromInt(k, key_guard.get());
      Slice key(key_guard.get(), sizeof(int64_t));
      GenerateSliceFromInt(k, value_guard.get());
      Slice value(value_guard.get(), sizeof(int64_t));
      auto ret = slist_->Insert(key, value, false);
      DCHECK(ret.ok());
      ASSERT_TRUE(ret.ok());
    }

    barrier1_.CountAndWait();
    // GenerateSliceFromInt(0, sizeof(int64_t), true, &key);

    uint64_t nnodes = 0;
    for (uint64_t k = 0; k < kTotalInserts; ++k) {
      //LOG(INFO) << "READ " << k;
      GenerateSliceFromInt(k, key_guard.get());
      Slice key(key_guard.get(), sizeof(int64_t));
      nv_ptr<SkipListNode> node = nullptr;
      {
        EpochGuard guard(slist_->GetEpoch());
        auto s = slist_->Search(key, &node, true);
        EXPECT_TRUE(s.ok());
        nnodes++;
        EXPECT_NE(node, nullptr);
        EXPECT_EQ(memcmp(node->GetPayload(), key.data(), node->payload_size),
                  0);
      }
    }
    EXPECT_EQ(nnodes, kTotalInserts);

    // Forward scan
    nnodes = 0;
    slist_->GetEpoch()->Protect();
    DSkipListCursor<DSkipList> cursor(slist_, true, true);
    slist_->GetEpoch()->Unprotect();
    while (true) {
      EpochGuard guard(slist_->GetEpoch());
      SkipListNode *n = cursor.Next();
      if (slist_->IsTail(n)) {
        break;
      }
      nnodes++;
    }
    EXPECT_EQ(nnodes, kTotalInserts);

    // Reverse scan
    nnodes = 0;
    slist_->GetEpoch()->Protect();
    DSkipListCursor<DSkipList> rcursor(slist_, false, true);
    slist_->GetEpoch()->Unprotect();
    while (true) {
      EpochGuard guard(slist_->GetEpoch());
      SkipListNode *n = rcursor.Prev();
      if (slist_->IsHead(n)) {
        break;
      }
      nnodes++;
    }
    EXPECT_EQ(nnodes, kTotalInserts);

    barrier2_.CountAndWait();

    for (uint64_t k = 0; k < kTotalInserts; ++k) {
      if (k % thread_count == thread_index) {
        GenerateSliceFromInt(k, key_guard.get());
        Slice key(key_guard.get(), sizeof(int64_t));
        auto ret = slist_->Delete(key, false);
        ASSERT_TRUE(ret.ok());
      }
    }
    barrier3_.CountAndWait();

    Thread::ClearRegistry(true);
  }
};

using CASDSkipListTest = DSkipListTest<CASDSkipList>;
using MwCASDSkipListTest = DSkipListTest<MwCASDSkipList>;

void RunCASDSkipListTest(uint64_t thread_count) {
#ifdef PMEM
  LOG(INFO) << "pool init to " << nv_ptr<int>(nullptr);
  auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
  auto root_obj =
      reinterpret_cast<PMDKRootObj *>(allocator->GetRoot(sizeof(PMDKRootObj)));
  allocator->AllocateOffset(reinterpret_cast<uint64_t *>(&root_obj->list_),
                            sizeof(CASDSkipList), false);
  CASDSkipList *list = root_obj->list_;
  new (list) CASDSkipList;
  pmwcas::PMAllocHelper::Get()->Initialize(&list->table_);
#else
  CASDSkipList *list = new CASDSkipList;
#endif
  CASDSkipListTest test(list, thread_count);
  test.Run(thread_count);
  {
    EpochGuard guard(list->GetEpoch());
    list->SanityCheck(true);
  }
#ifdef PMEM
  allocator->FreeOffset(reinterpret_cast<uint64_t *>(&root_obj->list_));
#else
  delete list;
#endif
  Thread::ClearRegistry(true);
}

void RunMwCASDSkipListTest(uint64_t thread_count) {
#ifdef PMEM
  LOG(INFO) << "pool init to " << nv_ptr<int>(nullptr);
  auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
  auto root_obj =
      reinterpret_cast<PMDKRootObj *>(allocator->GetRoot(sizeof(PMDKRootObj)));
  allocator->AllocateOffset(reinterpret_cast<uint64_t *>(&root_obj->desc_pool_),
                            sizeof(DescriptorPool), false);
  allocator->AllocateOffset(reinterpret_cast<uint64_t *>(&root_obj->mwlist_),
                            sizeof(MwCASDSkipList), false);
  DescriptorPool *pool = root_obj->desc_pool_;
  MwCASDSkipList *list = root_obj->mwlist_;
  new (pool) DescriptorPool(100000, thread_count, false);
  new (list) MwCASDSkipList(pool);
#else
  DescriptorPool *pool = new DescriptorPool(100000, thread_count, false);
  MwCASDSkipList *list = new MwCASDSkipList(pool);
#endif
  MwCASDSkipListTest test(list, thread_count);
  test.Run(thread_count);
  {
    EpochGuard guard(list->GetEpoch());
    list->SanityCheck(true);
  }
#ifdef PMEM
  allocator->FreeOffset(reinterpret_cast<uint64_t *>(&root_obj->mwlist_));
  allocator->FreeOffset(reinterpret_cast<uint64_t *>(&root_obj->desc_pool_));
#else
  delete list;
  delete pool;
#endif
  Thread::ClearRegistry(true);
}

GTEST_TEST(CASDSkipListTest, SingleThread) {
  uint32_t thread_count = 1;
  RunCASDSkipListTest(thread_count);
}

GTEST_TEST(CASDSkipListTest, Concurrent) {
  uint32_t thread_count =
      std::max<uint32_t>(Environment::Get()->GetCoreCount() / 2, 1);
  RunCASDSkipListTest(thread_count);
}

GTEST_TEST(MwCASDSkipListTest, SingleThread) {
  uint32_t thread_count = 1;
  RunMwCASDSkipListTest(thread_count);
}

GTEST_TEST(MwCASDSkipListTest, Concurrent) {
  uint32_t thread_count =
      std::max<uint32_t>(Environment::Get()->GetCoreCount() / 2, 1);
  RunMwCASDSkipListTest(thread_count);
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
#ifdef PMDK
  // Start with a new pool
  system((std::string("rm -f ") + FLAGS_pmdk_pool).c_str());
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                          FLAGS_pmdk_pool.c_str(), "skip_list_layout",
                          static_cast<uint64_t>(1024) * 1024 * 1024 * 1),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
#else
  pmwcas::InitLibrary(
      pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
      pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
#endif  // PMDK

  return RUN_ALL_TESTS();
}
