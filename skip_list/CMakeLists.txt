set(UTIL_HEADERS
  skip_list.h
)

set(UTIL_SOURCES
  skip_list.cc
)

add_library(skip-list ${UTIL_SOURCES} ${UTIL_HEADERS})
target_link_libraries(skip-list glog::glog pmwcas)

add_executable(skip_list_tests skip_list_test.cc)
target_link_libraries(skip_list_tests skip-list gtest_main pmemobj)
gtest_add_tests(TARGET skip_list_tests)
