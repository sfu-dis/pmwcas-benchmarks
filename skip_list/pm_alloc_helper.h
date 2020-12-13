#pragma once

#include <pmwcas.h>

#include <atomic>
#include <cstdint>

namespace pmwcas {
struct PMAllocTable {
  static constexpr uint64_t kMaxEntries = 256;
  struct alignas(64) Entry {
    void *addr{nullptr};
  };

  Entry entries[kMaxEntries];
};

class PMAllocHelper {
 public:
  static PMAllocHelper *Get() {
    static PMAllocHelper helper = {};
    return &helper;
  }

  void Initialize(nv_ptr<PMAllocTable> *table_addr) {
    auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    allocator->AllocateOffset(reinterpret_cast<uint64_t *>(table_addr),
                              sizeof(PMAllocTable), false);
    this->table = new (*table_addr) PMAllocTable();
    std::cerr << "PMAllocTable initialized to " << this->table << std::endl;
  }

  void **GetTlsPtr() {
    thread_local void **my_ptr = nullptr;
    if (!my_ptr) {
      auto pos = next.fetch_add(1);
      auto &entry = table->entries[pos];
      my_ptr = &entry.addr;
      Thread::RegisterTls((uint64_t *)&my_ptr, (uint64_t)nullptr);
    }
    RAW_CHECK(my_ptr != nullptr, "Tls ptr is nullptr");
    return my_ptr;
  }

 private:
  std::atomic<uint64_t> next{0};
  PMAllocTable *table{nullptr};
};
}  // namespace pmwcas
