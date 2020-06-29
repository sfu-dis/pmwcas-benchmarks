set(UTIL_HEADERS
  doubly_linked_list.h
)

set(UTIL_SOURCES
  doubly_linked_list.cc
)

add_library(double-linked-list doubly_linked_list.h doubly_linked_list.cpp)
target_link_libraries(double-linked-list glog::glog pmwcas)

add_executable(doubly_linked_list_tests doubly_linked_list_tests.cpp)
target_link_libraries(doubly_linked_list_tests double-linked-list gtest_main pmemobj)
gtest_add_tests(TARGET doubly_linked_list_tests)