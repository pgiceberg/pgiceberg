cmake_minimum_required(VERSION 3.25)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules")

project(pgIceberg
        VERSION 0.1.0
        DESCRIPTION "PostgreSQL extension for Apache Iceberg tables"
        LANGUAGES C CXX)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(PGICEBERG_USE_CCACHE "Use ccache when available" ON)
option(PGICEBERG_FETCH_ICEBERG "Fetch and build apache/iceberg-cpp" ON)
option(PGICEBERG_ENABLE_REST_CATALOG
       "Build pgiceberg with iceberg-cpp REST catalog support" OFF)
set(PGICEBERG_SANITIZERS
    ""
    CACHE STRING "Comma-separated sanitizer list, for example address,undefined")

if(PGICEBERG_USE_CCACHE)
  find_program(CCACHE_PROGRAM ccache)
  if(CCACHE_PROGRAM)
    if(NOT CMAKE_C_COMPILER_LAUNCHER)
      set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    endif()
    if(NOT CMAKE_CXX_COMPILER_LAUNCHER)
      set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    endif()
  endif()
endif()

find_package(PostgreSQL REQUIRED)
include(DependencyToolchain)

if(PGICEBERG_FETCH_ICEBERG)
  resolve_iceberg_dependency(PGICEBERG_ICEBERG_TARGET)
endif()

enable_testing()

add_subdirectory(src)
add_subdirectory(test)
