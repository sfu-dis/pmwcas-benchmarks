set(UTIL_HEADERS
  skip_list.h
)

set(UTIL_SOURCES
  skip_list.cc
)

add_library(skip-list ${UTIL_SOURCES} ${UTIL_HEADERS})
target_link_libraries(skip-list glog::glog)

if(${PMEM_BACKEND} STREQUAL "PMDK")
add_subdirectory(pibench)
endif()

ADD_PMWCAS_TEST(skip_list_test)
