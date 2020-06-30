
##################### PiBench #########################
FetchContent_Declare(
        pibench 
        GIT_REPOSITORY git@github.com:wangtzh/pibench.git
        GIT_TAG master 
)
if (NOT pibench_POPULATED)
  FetchContent_Populate(pibench)
  include_directories(${pibench_SOURCE_DIR}/include)
endif ()

add_library(pcas_skip_list_pibench_wrapper SHARED pcas_skip_list_pibench_wrapper.cpp)
target_link_libraries(pcas_skip_list_pibench_wrapper skip-list ${PMWCAS_TEST_LINK_LIBS} ${PMWCAS_APPS_LINK_LIBS})

add_library(pmwcas_skip_list_pibench_wrapper SHARED pmwcas_skip_list_pibench_wrapper.cpp)
target_link_libraries(pmwcas_skip_list_pibench_wrapper skip-list ${PMWCAS_TEST_LINK_LIBS} ${PMWCAS_APPS_LINK_LIBS})