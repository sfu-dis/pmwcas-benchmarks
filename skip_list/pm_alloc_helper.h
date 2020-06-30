#pragma once

#include <atomic>
#include <cstdint>

#include <pmwcas.h>

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

  void Initialize(PMAllocTable **table_addr) {
    pmwcas::Allocator::Get()->Allocate(reinterpret_cast<void **>(table_addr),
                                       sizeof(PMAllocTable));
    this->table = new (*table_addr) PMAllocTable();
    std::cerr << "PMAllocTable initialized to " << this->table << std::endl;
  }

  void **GetTlsPtr() {
    thread_local void **my_ptr = nullptr;
    if (!my_ptr) {
      auto pos = next.fetch_add(1);
      auto &entry = table->entries[pos];
      my_ptr = &entry.addr;
    }
    RAW_CHECK(my_ptr != nullptr, "Tls ptr is nullptr");
    return my_ptr;
  }

 private:
  std::atomic<uint64_t> next{0};
  PMAllocTable *table{nullptr};
};
}  // namespace pmwcas
