set(UTIL_HEADERS
  skip_list.h
)

set(UTIL_SOURCES
  skip_list.cc
)

add_library(skiplist ${UTIL_SOURCES} ${UTIL_HEADERS})
target_link_libraries(skiplist glog::glog pmwcas)

add_executable(skip_list_tests skip_list_test.cc)
target_link_libraries(skip_list_tests skiplist gtest_main gflags::gflags numa)
if(${PMEM_BACKEND} STREQUAL "PMDK")
  target_link_libraries(skip_list_tests pmemobj)
endif()
gtest_add_tests(TARGET skip_list_tests)

add_subdirectory(pibench)
