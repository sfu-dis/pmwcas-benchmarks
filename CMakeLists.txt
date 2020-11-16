if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif ()

project(pmwcas-benchmark)
cmake_minimum_required(VERSION 3.11)
include(FetchContent)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -march=native")
# Google Test
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.8.1
)
FetchContent_GetProperties(googletest)
if (NOT googletest_POPULATED)
  FetchContent_Populate(googletest)
  add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
endif ()
include(GoogleTest)

# Google Log
FetchContent_Declare(
        glog
        GIT_REPOSITORY https://github.com/google/glog.git
        GIT_TAG v0.4.0
)
FetchContent_GetProperties(glog)
if (NOT glog_POPULATED)
  FetchContent_Populate(glog)
  set(WITH_GFLAGS OFF CACHE BOOL "we don't want gflags")
  add_subdirectory(${glog_SOURCE_DIR} ${glog_BINARY_DIR})
endif ()

# Google Flags 
FetchContent_Declare(
        gflags
        GIT_REPOSITORY https://github.com/gflags/gflags.git
        GIT_TAG v2.2.2
)
FetchContent_GetProperties(gflags)
if (NOT gflags_POPULATED)
  FetchContent_Populate(gflags)
  add_subdirectory(${gflags_SOURCE_DIR} ${gflags_BINARY_DIR})
endif()


# PMwCAS
FetchContent_Declare(
        pmwcas
        GIT_REPOSITORY https://github.com/sfu-dis/pmwcas.git
        GIT_TAG reclamation-upon-recovery
)
FetchContent_GetProperties(pmwcas)
if (NOT pmwcas_POPULATED)
  FetchContent_Populate(pmwcas)
  add_subdirectory(${pmwcas_SOURCE_DIR})
  include_directories(${pmwcas_SOURCE_DIR} ${pmwcas_SOURCE_DIR}/include)
#  PMWCAS_DEFINE_ENV()
endif()

enable_testing()

include_directories(utils)

#add_subdirectory(array_bench)
#add_subdirectory(doubly_linked_list)
add_subdirectory(skip_list)
