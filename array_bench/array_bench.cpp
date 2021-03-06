// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <fcntl.h>
#include <gflags/gflags.h>
#include <pmwcas.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/wait.h>

#include <fstream>
#include <iostream>
#include <string>

#include "../utils/benchmark.h"
#include "../utils/random_number_generator.h"

typedef MwcTargetField<uint64_t> CasPtr;

DEFINE_string(
    benchmarks, "mwcas",
    "Comma-separated list of benchmarks to run in the specified order."
    " Available benchmarks:\n\tmwcas -- Configurable multi-threaded benchmark"
    " for multi-word cas\n");
DEFINE_uint64(array_size, 100, "size of the word array for mwcas benchmark");
DEFINE_uint64(seed, 1234,
              "base random number generator seed, the thread index"
              "is added to this number to form the full seed");
DEFINE_uint64(word_count, 4,
              "number of words in the multi-word compare and"
              " swap");
DEFINE_uint64(threads, 2, "number of threads to use for multi-threaded tests");
DEFINE_uint64(seconds, 30, "default time to run a benchmark");
DEFINE_uint64(metrics_dump_interval, 0,
              "if greater than 0, the benchmark "
              "driver dumps metrics at this fixed interval (in seconds)");
DEFINE_int32(affinity, 1, "affinity to use in scheduling threads");
DEFINE_uint64(descriptor_pool_size, 262144, "number of total descriptors");
DEFINE_int32(enable_stats, 1,
             "whether to enable stats on MwCAS internal"
             " operations");
DEFINE_bool(cacheline_padding, false,
            "whether to pad each array element to cacheline");
#ifdef PMDK
DEFINE_string(pmdk_pool, "/mnt/pmem0/mwcas_benchmark_pool",
              "path to pmdk pool");
DEFINE_string(semaphore_name, "/mwcas-recovery-bench-sem", "path to semaphore");
#endif

namespace pmwcas {

/// Maximum number of threads that the benchmark driver supports.
const size_t kMaxNumThreads = 64;

/// Dumps args in a format that can be extracted by an experiment script
void DumpArgs() {
  std::cout << "> DESC_CAP " << DESC_CAP << std::endl;
  std::cout << "> Args threads " << FLAGS_threads << std::endl;
  std::cout << "> Args word_count " << FLAGS_word_count << std::endl;
  std::cout << "> Args array_size " << FLAGS_array_size << std::endl;
  std::cout << "> Args affinity " << FLAGS_affinity << std::endl;
  std::cout << "> Args desrciptor_pool_size " << FLAGS_descriptor_pool_size
            << std::endl;
  std::cout << "> Args cacheline_padding " << FLAGS_cacheline_padding
            << std::endl;
#ifdef PMDK
  std::cout << "> Args pmdk_pool " << FLAGS_pmdk_pool << std::endl;
#endif
}

#ifdef PMEM
struct PMDKRootObj {
  PMEMoid desc_pool;
  PMEMoid test_array;
};
#endif

struct MwCas : public Benchmark {
  MwCas() : Benchmark{}, previous_dump_run_ticks_{}, cumulative_stats_{} {
    total_success_ = 0;
  }

  inline CasPtr *array_by_index(uint32_t index) {
    if (FLAGS_cacheline_padding) {
      return (CasPtr *)((char *)test_array_ + index * kCacheLineSize);
    } else {
      return test_array_ + index;
    }
  }

  void Setup(size_t thread_count) {
#ifdef WIN32
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create,
                        pmwcas::DefaultAllocator::Destroy,
                        pmwcas::WindowsEnvironment::Create,
                        pmwcas::WindowsEnvironment::Destroy);
#else
#ifdef PMDK
    pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                            FLAGS_pmdk_pool.c_str(), "mwcas_layout",
                            static_cast<uint64_t>(1024) * 1024 * 1204 * 1),
                        pmwcas::PMDKAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
#else
    pmwcas::InitLibrary(
        pmwcas::TlsAllocator::Create, pmwcas::TlsAllocator::Destroy,
        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
#endif  // PMDK
#endif
    // Ideally the descriptor pool is sized to the number of threads in the
    // benchmark to reduce need for new allocations, etc.
    size_t element_size =
        FLAGS_cacheline_padding ? kCacheLineSize : sizeof(uint64_t);
#ifdef PMDK
    auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    auto root_obj = reinterpret_cast<PMDKRootObj *>(
        allocator->GetRoot(sizeof(PMDKRootObj)));
    pmemobj_zalloc(allocator->GetPool(), &root_obj->desc_pool,
                   sizeof(DescriptorPool), TOID_TYPE_NUM(char));
    // Allocate the thread array and initialize to consecutive even numbers
    pmemobj_zalloc(allocator->GetPool(), &root_obj->test_array,
                   FLAGS_array_size * element_size, TOID_TYPE_NUM(char));
    // TODO: might have some memory leak here, but for benchmark we don't care
    // (yet).

    descriptor_pool_ =
        reinterpret_cast<DescriptorPool *>(pmemobj_direct(root_obj->desc_pool));
    test_array_ =
        reinterpret_cast<CasPtr *>(pmemobj_direct(root_obj->test_array));
#else
    Allocator::Get()->Allocate((void **)&descriptor_pool_,
                               sizeof(DescriptorPool));
    // Allocate the thread array and initialize to consecutive even numbers
    Allocator::Get()->Allocate((void **)&test_array_,
                               FLAGS_array_size * element_size);
#endif
    new (descriptor_pool_)
        DescriptorPool(FLAGS_descriptor_pool_size, FLAGS_threads, true);

    // Now we can start from a clean slate (perhaps not necessary)
    for (uint32_t i = 0; i < FLAGS_array_size; ++i) {
      *array_by_index(i) = uint64_t(i * 4);
    }
  }

  void Teardown() {
    if (FLAGS_array_size > 100) {
      return;
    }
    // Check the array for correctness
    std::unique_ptr<int64_t> found(
        (int64_t *)malloc(sizeof(int64_t) * FLAGS_array_size));

    for (uint32_t i = 0; i < FLAGS_array_size; i++) {
      found.get()[i] = 0;
    }

    for (uint32_t i = 0; i < FLAGS_array_size; i++) {
      uint32_t idx =
          uint32_t((uint64_t(*array_by_index(i)) % (4 * FLAGS_array_size)) / 4);
      LOG(INFO) << "idx=" << idx << ", pos=" << i
                << ", val=" << (uint64_t)*array_by_index(i);

      if (!(idx >= 0 && idx < FLAGS_array_size)) {
        LOG(INFO) << "Invalid: pos=" << i
                  << "val=" << uint64_t(*array_by_index(i));
        continue;
      }
      found.get()[idx]++;
    }

    uint32_t missing = 0;
    uint32_t duplicated = 0;
    for (uint32_t i = 0; i < FLAGS_array_size; i++) {
      if (found.get()[i] == 0) missing++;
      if (found.get()[i] > 1) duplicated++;
    }

    CHECK(0 == missing && 0 == duplicated)
        << "Failed final sanity test, missing: " << missing
        << " duplicated: " << duplicated;

#ifdef PMDK
    auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    auto root_obj = reinterpret_cast<PMDKRootObj *>(
        allocator->GetRoot(sizeof(PMDKRootObj)));
    pmemobj_free(&root_obj->test_array);
    test_array_ = nullptr;
#else
    Allocator::Get()->Free((void **)&test_array_);
#endif
  }

  void Main(size_t thread_index) {
    CasPtr *address[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    CasPtr value[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    RandomNumberGenerator rng(FLAGS_seed + thread_index, 0, FLAGS_array_size);
    auto s = MwCASMetrics::ThreadInitialize();
    RAW_CHECK(s.ok(), "Error initializing thread");
    WaitForStart();
    const uint64_t kEpochThreshold = 100;
    uint64_t epochs = 0;

    descriptor_pool_->GetEpoch()->Protect();
    uint64_t n_success = 0;
    while (!IsShutdown()) {
      if (++epochs == kEpochThreshold) {
        descriptor_pool_->GetEpoch()->Unprotect();
        descriptor_pool_->GetEpoch()->Protect();
        epochs = 0;
      }

      // Pick a random word each time
      for (uint32_t i = 0; i < FLAGS_word_count; ++i) {
      retry:
        uint64_t idx = rng.Generate(FLAGS_array_size);
        for (uint32_t j = 0; j < i; ++j) {
          if (address[j] == reinterpret_cast<CasPtr *>(array_by_index(idx))) {
            goto retry;
          }
        }
        address[i] = reinterpret_cast<CasPtr *>(array_by_index(idx));
        value[i] = array_by_index(idx)->GetValueProtected();
        CHECK(value[i] % (4 * FLAGS_array_size) >= 0 &&
              (value[i] % (4 * FLAGS_array_size)) / 4 < FLAGS_array_size);
      }

      auto descriptor = descriptor_pool_->AllocateDescriptor();
      CHECK_NOTNULL(descriptor.GetRaw());
      for (uint64_t i = 0; i < FLAGS_word_count; i++) {
        descriptor.AddEntry(
            (uint64_t *)(address[i]), uint64_t(value[i]),
            uint64_t(value[FLAGS_word_count - 1 - i] + 4 * FLAGS_array_size));
      }
      bool status = false;
      status = descriptor.MwCAS();
      n_success += (status == true);
    }
    descriptor_pool_->GetEpoch()->Unprotect();
    auto n = total_success_.fetch_add(n_success, std::memory_order_seq_cst);
    LOG(INFO) << "Thread " << thread_index << " success updates: " << n_success
              << " " << n;
  }

  uint64_t GetOperationCount() {
    MwCASMetrics metrics;
    MwCASMetrics::Sum(metrics);
    return metrics.GetUpdateAttemptCount();
  }

  uint64_t GetTotalSuccess() { return total_success_.load(); }

  virtual void Dump(size_t thread_count, uint64_t run_ticks, uint64_t dump_id,
                    bool final_dump) {
    MARK_UNREFERENCED(thread_count);
    uint64_t interval_ticks = 0;
    if (final_dump) {
      interval_ticks = run_ticks;
    } else {
      interval_ticks = run_ticks - previous_dump_run_ticks_;
      previous_dump_run_ticks_ = run_ticks;
    }
    Benchmark::Dump(thread_count, run_ticks, dump_id, final_dump);

    MwCASMetrics stats;
    MwCASMetrics::Sum(stats);
    if (!final_dump) {
      stats -= cumulative_stats_;
      cumulative_stats_ += stats;
    }

    stats.Print();

#ifdef WIN32
    LARGE_INTEGER frequency;
    QueryPerformanceFrequency(&frequency);
    uint64_t ticks_per_second = frequency.QuadPart;
#else
    uint64_t ticks_per_second = 1000000;
#endif
    std::cout << "> Benchmark " << dump_id << " TicksPerSecond "
              << ticks_per_second << std::endl;
    std::cout << "> Benchmark " << dump_id << " RunTicks " << run_ticks
              << std::endl;

    double run_seconds = (double)run_ticks / ticks_per_second;
    std::cout << "> Benchmark " << dump_id << " RunSeconds " << run_seconds
              << std::endl;

    std::cout << "> Benchmark " << dump_id << " IntervalTicks "
              << interval_ticks << std::endl;
    double interval_seconds = (double)interval_ticks / ticks_per_second;
    std::cout << "> Benchmark " << dump_id << " IntervalSeconds "
              << interval_seconds << std::endl;
  }

  CasPtr *test_array_;
  DescriptorPool *descriptor_pool_;
  uint64_t previous_dump_run_ticks_;
  MwCASMetrics cumulative_stats_;
  std::atomic<uint64_t> total_success_;
};

#ifdef PMDK
struct RecoveryBenchmark {
  inline CasPtr *array_by_index(uint32_t index) {
    if (FLAGS_cacheline_padding) {
      return (CasPtr *)((char *)test_array_ + index * kCacheLineSize);
    } else {
      return test_array_ + index;
    }
  }

  void ChildWorkload(sem_t *sem) {
#ifdef WIN32
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create,
                        pmwcas::DefaultAllocator::Destroy,
                        pmwcas::WindowsEnvironment::Create,
                        pmwcas::WindowsEnvironment::Destroy);
#else
#ifdef PMDK
    pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                            FLAGS_pmdk_pool.c_str(), "mwcas_layout",
                            static_cast<uint64_t>(1024) * 1024 * 1204 * 1),
                        pmwcas::PMDKAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
#else
    pmwcas::InitLibrary(
        pmwcas::TlsAllocator::Create, pmwcas::TlsAllocator::Destroy,
        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
#endif  // PMDK
#endif
    size_t element_size =
        FLAGS_cacheline_padding ? kCacheLineSize : sizeof(uint64_t);

    auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    auto root_obj = reinterpret_cast<PMDKRootObj *>(
        allocator->GetRoot(sizeof(PMDKRootObj)));
    pmemobj_zalloc(allocator->GetPool(), &root_obj->desc_pool,
                   sizeof(DescriptorPool), TOID_TYPE_NUM(char));
    // Allocate the thread array and initialize to consecutive even numbers
    pmemobj_zalloc(allocator->GetPool(), &root_obj->test_array,
                   FLAGS_array_size * element_size, TOID_TYPE_NUM(char));
    // TODO: might have some memory leak here, but for benchmark we don't care
    // (yet).

    descriptor_pool_ =
        reinterpret_cast<DescriptorPool *>(pmemobj_direct(root_obj->desc_pool));
    test_array_ =
        reinterpret_cast<CasPtr *>(pmemobj_direct(root_obj->test_array));

    new (descriptor_pool_)
        DescriptorPool(FLAGS_descriptor_pool_size, FLAGS_threads, true);

    // Now we can start from a clean slate (perhaps not necessary)
    for (uint32_t i = 0; i < FLAGS_array_size; ++i) {
      *array_by_index(i) = uint64_t(i * 4);
    }

    NVRAM::Flush(FLAGS_array_size * element_size, test_array_);
    NVRAM::Flush(sizeof(DescriptorPool), descriptor_pool_);
    NVRAM::Flush(sizeof(PMDKRootObj), root_obj);
    LOG(INFO) << "data flushed" << std::endl;

    std::atomic<uint64_t> prepared{0};
    std::atomic<bool> start{false};

    std::vector<std::thread> workers;
    for (uint32_t t = 0; t < FLAGS_threads; ++t) {
      workers.emplace_back(
          [&prepared, &start, this](DescriptorPool *desc_pool, CasPtr *array,
                                    uint32_t thread_index) {
            CasPtr *address[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
            CasPtr value[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
            RandomNumberGenerator rng(FLAGS_seed + thread_index, 0,
                                      FLAGS_array_size);
            auto s = MwCASMetrics::ThreadInitialize();
            RAW_CHECK(s.ok(), "Error initializing thread");

            const uint64_t kEpochThreshold = 100;
            uint64_t epochs = 0;

            prepared++;
            while (!start)
              ;

            desc_pool->GetEpoch()->Protect();
            while (true) {
              if (++epochs == kEpochThreshold) {
                descriptor_pool_->GetEpoch()->Unprotect();
                descriptor_pool_->GetEpoch()->Protect();
                epochs = 0;
              }

              // Pick a random word each time
              for (uint32_t i = 0; i < FLAGS_word_count; ++i) {
              retry:
                uint64_t idx = rng.Generate(FLAGS_array_size);
                for (uint32_t j = 0; j < i; ++j) {
                  if (address[j] ==
                      reinterpret_cast<CasPtr *>(array_by_index(idx))) {
                    goto retry;
                  }
                }
                address[i] = reinterpret_cast<CasPtr *>(array_by_index(idx));
                value[i] = array_by_index(idx)->GetValueProtected();
                CHECK(value[i] % (4 * FLAGS_array_size) >= 0 &&
                      (value[i] % (4 * FLAGS_array_size)) / 4 <
                          FLAGS_array_size);
              }

              auto descriptor = descriptor_pool_->AllocateDescriptor();
              CHECK_NOTNULL(descriptor.GetRaw());
              for (uint64_t i = 0; i < FLAGS_word_count; i++) {
                descriptor.AddEntry((uint64_t *)(address[i]),
                                    uint64_t(value[i]),
                                    uint64_t(value[FLAGS_word_count - 1 - i] +
                                             4 * FLAGS_array_size));
              }
              descriptor.MwCAS();
            }
          },
          descriptor_pool_, test_array_, t);
    }

    while (prepared != FLAGS_threads)
      ;
    start = true;
    sem_post(sem);

    for (auto &w : workers) {
      w.join();
    }
    LOG(FATAL) << "Child process finished, this should not happen" << std::endl;
  }

  void Setup() {
    sem_t *sem = sem_open(FLAGS_semaphore_name.c_str(), O_CREAT, 0666, 0);
    if (sem == SEM_FAILED) {
      LOG(FATAL) << "Semaphore creation failure, errno: " << errno;
    }
    sem_unlink(FLAGS_semaphore_name.c_str());
    pid_t pid = fork();
    if (pid > 0) {
      sem_wait(sem);
      std::this_thread::sleep_for(std::chrono::seconds(FLAGS_seconds));

      /// Step 3: force kill all running threads without noticing them
      kill(pid, SIGKILL);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

    } else if (pid == 0) {
      ChildWorkload(sem);
      LOG(FATAL) << "Child process finished without being terminated";
    }
#ifdef WIN32
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create,
                        pmwcas::DefaultAllocator::Destroy,
                        pmwcas::WindowsEnvironment::Create,
                        pmwcas::WindowsEnvironment::Destroy);
#else
#ifdef PMDK
    pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                            FLAGS_pmdk_pool.c_str(), "mwcas_layout",
                            static_cast<uint64_t>(1024) * 1024 * 1204 * 1),
                        pmwcas::PMDKAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
#else
    pmwcas::InitLibrary(
        pmwcas::TlsAllocator::Create, pmwcas::TlsAllocator::Destroy,
        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
#endif  // PMDK
#endif
    LOG_IF(FATAL, !Allocator::Get()) << "Allocator initialization failed";
    LOG_IF(FATAL, !Environment::Get()) << "Environment initialization failed";
    LOG(INFO) << "Recovery starts here.";
  }

  void Main() {
    auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    auto root_obj = reinterpret_cast<PMDKRootObj *>(
        allocator->GetRoot(sizeof(PMDKRootObj)));

    descriptor_pool_ =
        reinterpret_cast<DescriptorPool *>(pmemobj_direct(root_obj->desc_pool));
    test_array_ =
        reinterpret_cast<CasPtr *>(pmemobj_direct(root_obj->test_array));

    descriptor_pool_->Recovery(0, false);
  }

  void Dump() {
    std::cout << "Recovery took " << run_ms_ << " milliseconds.\n";
  }

  void Teardown() {
    // Check the array for correctness
    std::unique_ptr<int64_t> found(
        (int64_t *)malloc(sizeof(int64_t) * FLAGS_array_size));

    for (uint32_t i = 0; i < FLAGS_array_size; i++) {
      found.get()[i] = 0;
    }

    for (uint32_t i = 0; i < FLAGS_array_size; i++) {
      uint32_t idx =
          uint32_t((uint64_t(*array_by_index(i)) % (4 * FLAGS_array_size)) / 4);

      if (!(idx >= 0 && idx < FLAGS_array_size)) {
        LOG(INFO) << "Invalid: pos=" << i
                  << "val=" << uint64_t(*array_by_index(i));
        continue;
      }
      found.get()[idx]++;
    }

    uint32_t missing = 0;
    uint32_t duplicated = 0;
    for (uint32_t i = 0; i < FLAGS_array_size; i++) {
      if (found.get()[i] == 0) missing++;
      if (found.get()[i] > 1) duplicated++;
    }

    CHECK(0 == missing && 0 == duplicated)
        << "Failed final sanity test, missing: " << missing
        << " duplicated: " << duplicated;
  }

  void Run() {
    Setup();

    uint64_t start = Environment::Get()->NowMicros();
    Main();
    uint64_t end = Environment::Get()->NowMicros();

    run_ms_ = double(end - start) / double(1000);

    Dump();

    Teardown();
  }

  CasPtr *test_array_;
  DescriptorPool *descriptor_pool_;
  double run_ms_;
};
#endif

}  // namespace pmwcas

using namespace pmwcas;

Status RunMwCas() {
  MwCas test{};
  std::cout << "Starting benchmark..." << std::endl;
  test.Run(FLAGS_threads, FLAGS_seconds,
           static_cast<AffinityPattern>(FLAGS_affinity),
           FLAGS_metrics_dump_interval);

  printf("mwcas: %.2f ops/sec\n",
         (double)test.GetOperationCount() / test.GetRunSeconds());
  printf("mwcas: %.2f successful updates/sec\n",
         (double)test.GetTotalSuccess() / test.GetRunSeconds());

  std::ofstream outfile;
  outfile.open("mwcas_bench_output.txt");
  outfile << (double)test.GetOperationCount() / test.GetRunSeconds() << " ";
  outfile << (double)test.GetTotalSuccess() / test.GetRunSeconds();
  outfile.close();

  return Status::OK();
}

#ifdef PMEM
Status RunRecovery() {
  RecoveryBenchmark test{};
  std::cout << "Starting benchmark..." << std::endl;
  test.Run();

  return Status::OK();
}
#endif

void RunBenchmark() {
  std::string benchmark_name{};
  std::stringstream benchmark_stream(FLAGS_benchmarks);
  DumpArgs();

  while (std::getline(benchmark_stream, benchmark_name, ',')) {
    Status s{};
    if ("mwcas" == benchmark_name) {
      s = RunMwCas();
    } else if ("recovery" == benchmark_name) {
#ifdef PMEM
      s = RunRecovery();
#else
      fprintf(stderr, "recovery benchmark is only available under PMDK backend!\n");
#endif
    } else {
      fprintf(stderr, "unknown benchmark name: %s\n", benchmark_name.c_str());
    }

    ALWAYS_ASSERT(s.ok());
  }
}

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  RunBenchmark();
  return 0;
}
