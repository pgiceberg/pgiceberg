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

# ----------------------------------------------------------------------
# FetchContent

include(FetchContent)
set(FC_DECLARE_COMMON_OPTIONS)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
  list(APPEND FC_DECLARE_COMMON_OPTIONS EXCLUDE_FROM_ALL TRUE)
endif()

macro(prepare_fetchcontent)
  set(BUILD_SHARED_LIBS OFF)
  set(BUILD_STATIC_LIBS ON)
  set(CMAKE_COMPILE_WARNING_AS_ERROR FALSE)
  set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY TRUE)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endmacro()

# ----------------------------------------------------------------------
# Apache Iceberg C++

set(PGICEBERG_ICEBERG_GIT_TAG
    "ab083862eab5ac1a002200bcc174e51697851afc"
    CACHE STRING "apache/iceberg-cpp commit or tag to fetch")

function(resolve_iceberg_dependency out_target)
  prepare_fetchcontent()

  set(ICEBERG_ARROW
      ON
      CACHE BOOL "" FORCE)
  set(ICEBERG_BUILD_STATIC
      ON
      CACHE BOOL "" FORCE)
  set(ICEBERG_BUILD_SHARED
      OFF
      CACHE BOOL "" FORCE)
  set(ICEBERG_BUILD_BUNDLE
      ON
      CACHE BOOL "" FORCE)
  set(ICEBERG_BUILD_REST
      ${PGICEBERG_ENABLE_REST_CATALOG}
      CACHE BOOL "" FORCE)
  set(ICEBERG_BUILD_SQL_CATALOG
      ON
      CACHE BOOL "" FORCE)
  set(ICEBERG_SQL_POSTGRESQL
      ON
      CACHE BOOL "" FORCE)
  set(ICEBERG_SQL_SQLITE
      ON
      CACHE BOOL "" FORCE)
  set(ICEBERG_BUILD_TESTS
      OFF
      CACHE BOOL "" FORCE)
  # iceberg-cpp's REST toolchain builds cpr against the system libcurl and calls
  # find_package(CURL REQUIRED), so the REST catalog build needs a system
  # libcurl development package (for example libcurl4-openssl-dev).
  set(CPR_USE_SYSTEM_CURL
      ON
      CACHE BOOL "" FORCE)

  fetchcontent_declare(Iceberg
                       GIT_REPOSITORY https://github.com/apache/iceberg-cpp.git
                       GIT_TAG ${PGICEBERG_ICEBERG_GIT_TAG})
  fetchcontent_makeavailable(Iceberg)

  # iceberg-cpp generates iceberg/version.h relative to the top-level build
  # directory. When it is embedded with FetchContent, expose that generated
  # header to the data target that includes it for deletion-vector writers.
  foreach(candidate iceberg_data_static iceberg_data_shared)
    if(TARGET ${candidate})
      target_include_directories(${candidate} PRIVATE "${CMAKE_BINARY_DIR}/src")
    endif()
  endforeach()

  set(iceberg_targets)

  foreach(candidate iceberg_bundle_static iceberg_bundle_shared)
    if(TARGET ${candidate})
      list(APPEND iceberg_targets ${candidate})
      break()
    endif()
  endforeach()

  if(NOT iceberg_targets)
    message(FATAL_ERROR "iceberg-cpp was fetched, but no known bundle library target was found"
    )
  endif()

  if(PGICEBERG_ENABLE_REST_CATALOG)
    foreach(candidate iceberg_rest_static iceberg_rest_shared)
      if(TARGET ${candidate})
        list(APPEND iceberg_targets ${candidate})
        break()
      endif()
    endforeach()

    list(FIND iceberg_targets iceberg_rest_static rest_static_index)
    list(FIND iceberg_targets iceberg_rest_shared rest_shared_index)
    if(rest_static_index EQUAL -1 AND rest_shared_index EQUAL -1)
      message(FATAL_ERROR "iceberg-cpp REST catalog support is enabled, but no known REST library target was found"
      )
    endif()
  endif()

  foreach(candidate iceberg_sql_catalog_static iceberg_sql_catalog_shared)
    if(TARGET ${candidate})
      list(APPEND iceberg_targets ${candidate})
      set(${out_target}
          ${iceberg_targets}
          PARENT_SCOPE)
      return()
    endif()
  endforeach()

  message(FATAL_ERROR "iceberg-cpp SQL catalog support is required, but no known SQL catalog library target was found"
  )
endfunction()
