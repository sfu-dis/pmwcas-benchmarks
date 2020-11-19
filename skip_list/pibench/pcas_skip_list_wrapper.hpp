#pragma once

#include <tree_api.hpp>

#include "../skip_list.h"

class pcas_skip_list_wrapper : public tree_api {
 public:
  pcas_skip_list_wrapper(const tree_options_t& opt);
  virtual ~pcas_skip_list_wrapper();

  virtual bool find(const char* key, size_t key_sz, char* value_out) override;
  virtual bool insert(const char* key, size_t key_sz, const char* value,
                      size_t value_sz) override;
  virtual bool update(const char* key, size_t key_sz, const char* value,
                      size_t value_sz) override;
  virtual bool remove(const char* key, size_t key_sz) override;
  virtual int scan(const char* key, size_t key_sz, int scan_sz,
                   char*& values_out) override;

 private:
  bool recovery(const tree_options_t& opt);

  pmwcas::CASDSkipList* slist_;
  tree_options_t options_;
};
