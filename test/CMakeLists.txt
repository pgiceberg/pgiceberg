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

if(POSTGRESQL_REGRESS)
  set(PGICEBERG_REGRESS_TESTS
      fdw_options
      logical_decoding
      fdw_dml
      fdw_import_schema
      tableam_native
      fdw_parquet
      fdw_avro
      fdw_register_table
      fdw_yellow_trip_dataset
      fdw_metadata
      fdw_snapshots
      fdw_pushdown)
  set(PGICEBERG_REGRESS_DATASETS yellow_trip)
  set(PGICEBERG_REGRESS_EXTENSION_DIR "${CMAKE_CURRENT_BINARY_DIR}/extension")

  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/regress")
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tmp_check")
  set(PGICEBERG_REGRESS_INPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/regress")
  set(PGICEBERG_REGRESS_EXPECTED_DIR "${PGICEBERG_REGRESS_INPUT_DIR}/expected")
  file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/regress/sql"
       DESTINATION "${PGICEBERG_REGRESS_INPUT_DIR}")
  file(MAKE_DIRECTORY "${PGICEBERG_REGRESS_EXPECTED_DIR}")
  file(GLOB PGICEBERG_REGRESS_EXPECTED_FILES
       "${CMAKE_CURRENT_SOURCE_DIR}/regress/expected/*.out")
  foreach(expected_file IN LISTS PGICEBERG_REGRESS_EXPECTED_FILES)
    get_filename_component(expected_name "${expected_file}" NAME)
    if(expected_name MATCHES "^fdw_pushdown")
      continue()
    endif()
    configure_file("${expected_file}"
                   "${PGICEBERG_REGRESS_EXPECTED_DIR}/${expected_name}" COPYONLY)
  endforeach()
  if(POSTGRESQL_VERSION VERSION_GREATER_EQUAL "18")
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/regress/expected/fdw_pushdown_pg18.out"
                   "${PGICEBERG_REGRESS_EXPECTED_DIR}/fdw_pushdown.out" COPYONLY)
  else()
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/regress/expected/fdw_pushdown_pg16.out"
                   "${PGICEBERG_REGRESS_EXPECTED_DIR}/fdw_pushdown.out" COPYONLY)
  endif()
  file(MAKE_DIRECTORY "${PGICEBERG_REGRESS_EXTENSION_DIR}")

  file(READ "${PROJECT_SOURCE_DIR}/pgiceberg.control" PGICEBERG_CONTROL)
  string(REGEX
         REPLACE "module_pathname = '[^']+'"
                 "module_pathname = '$<TARGET_FILE:pgiceberg>'" PGICEBERG_REGRESS_CONTROL
                 "${PGICEBERG_CONTROL}")
  file(GENERATE
       OUTPUT "${PGICEBERG_REGRESS_EXTENSION_DIR}/pgiceberg.control"
       CONTENT "${PGICEBERG_REGRESS_CONTROL}")
  configure_file("${PROJECT_SOURCE_DIR}/sql/pgiceberg--0.1.0.sql"
                 "${PGICEBERG_REGRESS_EXTENSION_DIR}/pgiceberg--0.1.0.sql" COPYONLY)

  file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/pg_regress.conf"
       "unix_socket_directories = '/tmp'\n" "listen_addresses = '127.0.0.1'\n"
       "wal_level = logical\n"
       "dynamic_library_path = '${PROJECT_BINARY_DIR}/src:$libdir'\n")
  if(POSTGRESQL_VERSION VERSION_GREATER_EQUAL "18")
    file(APPEND "${CMAKE_CURRENT_BINARY_DIR}/pg_regress.conf"
         "extension_control_path = '${CMAKE_CURRENT_BINARY_DIR}:$system'\n")
  endif()
  configure_file("${CMAKE_CURRENT_SOURCE_DIR}/run_regress.cmake.in"
                 "${CMAKE_CURRENT_BINARY_DIR}/run_regress.cmake" @ONLY)
  set_property(DIRECTORY
               APPEND
               PROPERTY CMAKE_CONFIGURE_DEPENDS
                        "${CMAKE_CURRENT_SOURCE_DIR}/regress/expected/fdw_pushdown_pg16.out"
                        "${CMAKE_CURRENT_SOURCE_DIR}/regress/expected/fdw_pushdown_pg18.out"
  )

  add_test(NAME regress_fdw COMMAND "${CMAKE_COMMAND}" -P
                                    "${CMAKE_CURRENT_BINARY_DIR}/run_regress.cmake")
  set_tests_properties(regress_fdw
                       PROPERTIES WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/regress"
                                  ENVIRONMENT "PG_REGRESS_SOCK_DIR=/tmp")
else()
  message(STATUS "pg_regress not found; regression tests are not registered")
endif()
