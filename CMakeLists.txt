# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

option(PGICEBERG_ENABLE_REST_CATALOG
       "Build pgiceberg with iceberg-cpp REST catalog support" OFF)
set(PGICEBERG_SANITIZERS
    ""
    CACHE STRING "Comma-separated sanitizer list, for example address,undefined")

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  if(NOT CMAKE_C_COMPILER_LAUNCHER)
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
  endif()
  if(NOT CMAKE_CXX_COMPILER_LAUNCHER)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
  endif()
endif()

find_package(PostgreSQL REQUIRED)
if(POSTGRESQL_VERSION VERSION_LESS "16" OR POSTGRESQL_VERSION VERSION_GREATER_EQUAL "19")
  message(FATAL_ERROR "pgiceberg supports PostgreSQL 16, 17, and 18; found ${POSTGRESQL_VERSION_STRING}"
  )
endif()

include(DependencyToolchain)

resolve_iceberg_dependency(PGICEBERG_ICEBERG_TARGET)

enable_testing()

add_subdirectory(src)
add_subdirectory(test)
