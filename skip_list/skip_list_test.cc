
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

static void GenerateSliceFromInt(int64_t k, char *out) {
  int64_t swapped = _bswap64(k);
  memcpy(out, &swapped, sizeof(int64_t));
};

struct CASDSkipListTest : public PerformanceTest {
  const int64_t kTotalInserts = 15000;
  CASDSkipList *slist_;
  std::atomic<int64_t> total_inserts_;
  Barrier barrier1_;
  Barrier barrier2_;
  Barrier barrier3_;
  uint64_t thread_count;
  CASDSkipListTest(CASDSkipList *list, uint64_t thread_count)
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
      SkipListNode *node = nullptr;
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
    CASDSkipListCursor cursor(slist_, true, true);
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
    CASDSkipListCursor rcursor(slist_, false, true);
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

GTEST_TEST(CASDSkipListTest, SingleThread) {
  auto thread_count = 1;
  CASDSkipList *list = new CASDSkipList;
  CASDSkipListTest test(list, thread_count);
  test.Run(thread_count);
  {
    EpochGuard guard(list->GetEpoch());
    list->SanityCheck(true);
  }
  delete list;
  Thread::ClearRegistry(true);
}

GTEST_TEST(CASDSkipListTest, Concurrent) {
  uint32_t thread_count =
      std::max<uint32_t>(Environment::Get()->GetCoreCount() / 2, 1);
  CASDSkipList *list = new CASDSkipList;
  CASDSkipListTest test(list, thread_count);
  test.Run(thread_count);
  {
    EpochGuard guard(list->GetEpoch());
    list->SanityCheck(true);
  }
  delete list;
  Thread::ClearRegistry(true);
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
                          static_cast<uint64_t>(1024) * 1024 * 1024 * 8),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
#else
  pmwcas::InitLibrary(
      pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
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
