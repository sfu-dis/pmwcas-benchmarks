if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif ()

project(pmwcas-benchmark)
cmake_minimum_required(VERSION 3.11)
include(FetchContent)


#Function to automate building benchmark binaries with appropriate environment sources
FUNCTION(ADD_PMWCAS_BENCHMARK BENCHMARK_NAME)
  add_executable(${BENCHMARK_NAME} ${BENCHMARK_HEADERS} ${BENCHMARK_NAME}.cc ${PROJECT_SOURCE_DIR}/${LINUX_ENVIRONMENT_SOURCES})
  target_link_libraries(${BENCHMARK_NAME} ${PMWCAS_BENCHMARK_LINK_LIBS} ${PMWCAS_APPS_LINK_LIBS})
ENDFUNCTION()

add_subdirectory(array_bench)
