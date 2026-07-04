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

# - Find PostgreSQL for building server extensions.
#
# This module intentionally discovers PostgreSQL through pg_config so pgenv can
# switch versions by changing PATH or by passing -DPG_CONFIG=/path/to/pg_config.
#
# Variables:
#   POSTGRESQL_FOUND
#   POSTGRESQL_VERSION_STRING
#   POSTGRESQL_VERSION
#   POSTGRESQL_INCLUDE_DIR
#   POSTGRESQL_SERVER_INCLUDE_DIR
#   POSTGRESQL_LIBRARY_DIR
#   POSTGRESQL_PKGLIB_DIR
#   POSTGRESQL_SHARE_DIR
#   POSTGRESQL_EXTENSION_DIR
#   POSTGRESQL_BIN_DIR
#   POSTGRESQL_SERVER_EXECUTABLE
#   POSTGRESQL_REGRESS
#
# Imported targets:
#   PostgreSQL::Server
#   PostgreSQL::Client
#   PostgreSQL::PostgreSQL

include(FindPackageHandleStandardArgs)

set(PG_CONFIG
    ""
    CACHE FILEPATH "Path to pg_config")

set(POSTGRESQL_BIN_DIR
    ""
    CACHE PATH "Path to PostgreSQL program executables")

if(PG_CONFIG)
  set(POSTGRESQL_PG_CONFIG "${PG_CONFIG}")
elseif(POSTGRESQL_BIN_DIR)
  find_program(POSTGRESQL_PG_CONFIG
               NAMES pg_config
               PATHS "${POSTGRESQL_BIN_DIR}"
               NO_DEFAULT_PATH)
else()
  find_program(POSTGRESQL_PG_CONFIG
               NAMES pg_config
               PATHS /usr/lib/postgresql/*/bin)
endif()

if(POSTGRESQL_PG_CONFIG)
  execute_process(COMMAND "${POSTGRESQL_PG_CONFIG}" --version
                  OUTPUT_STRIP_TRAILING_WHITESPACE
                  OUTPUT_VARIABLE POSTGRESQL_VERSION_STRING)
  string(REGEX REPLACE "^PostgreSQL ([0-9]+(\\.[0-9]+)?).*$" "\\1" POSTGRESQL_VERSION
                       "${POSTGRESQL_VERSION_STRING}")
  execute_process(COMMAND "${POSTGRESQL_PG_CONFIG}" --includedir
                  OUTPUT_STRIP_TRAILING_WHITESPACE
                  OUTPUT_VARIABLE POSTGRESQL_INCLUDE_DIR)
  execute_process(COMMAND "${POSTGRESQL_PG_CONFIG}" --includedir-server
                  OUTPUT_STRIP_TRAILING_WHITESPACE
                  OUTPUT_VARIABLE POSTGRESQL_SERVER_INCLUDE_DIR)
  execute_process(COMMAND "${POSTGRESQL_PG_CONFIG}" --libdir
                  OUTPUT_STRIP_TRAILING_WHITESPACE
                  OUTPUT_VARIABLE POSTGRESQL_LIBRARY_DIR)
  execute_process(COMMAND "${POSTGRESQL_PG_CONFIG}" --pkglibdir
                  OUTPUT_STRIP_TRAILING_WHITESPACE
                  OUTPUT_VARIABLE POSTGRESQL_PKGLIB_DIR)
  execute_process(COMMAND "${POSTGRESQL_PG_CONFIG}" --sharedir
                  OUTPUT_STRIP_TRAILING_WHITESPACE
                  OUTPUT_VARIABLE POSTGRESQL_SHARE_DIR)
  execute_process(COMMAND "${POSTGRESQL_PG_CONFIG}" --bindir
                  OUTPUT_STRIP_TRAILING_WHITESPACE
                  OUTPUT_VARIABLE POSTGRESQL_BIN_DIR)

  set(POSTGRESQL_EXTENSION_DIR "${POSTGRESQL_SHARE_DIR}/extension")

  find_program(POSTGRESQL_REGRESS
               NAMES pg_regress
               PATHS "${POSTGRESQL_PKGLIB_DIR}/pgxs/src/test/regress"
                     "${POSTGRESQL_BIN_DIR}"
               NO_DEFAULT_PATH)
  find_program(POSTGRESQL_SERVER_EXECUTABLE
               NAMES postgres
               PATHS "${POSTGRESQL_BIN_DIR}"
               NO_DEFAULT_PATH)
  find_library(POSTGRESQL_LIBPQ_LIBRARY
               NAMES pq
               PATHS "${POSTGRESQL_LIBRARY_DIR}"
               NO_DEFAULT_PATH)
endif()

find_package_handle_standard_args(
  PostgreSQL
  REQUIRED_VARS POSTGRESQL_PG_CONFIG
                POSTGRESQL_SERVER_INCLUDE_DIR
                POSTGRESQL_PKGLIB_DIR
                POSTGRESQL_EXTENSION_DIR
                POSTGRESQL_LIBPQ_LIBRARY
  VERSION_VAR POSTGRESQL_VERSION)

if(POSTGRESQL_FOUND AND NOT TARGET PostgreSQL::Server)
  add_library(PostgreSQL::Server INTERFACE IMPORTED)
  set_target_properties(PostgreSQL::Server
                        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                   "${POSTGRESQL_INCLUDE_DIR};${POSTGRESQL_SERVER_INCLUDE_DIR}"
  )
endif()

if(POSTGRESQL_FOUND AND NOT TARGET PostgreSQL::Client)
  add_library(PostgreSQL::Client INTERFACE IMPORTED)
  set_target_properties(PostgreSQL::Client
                        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                   "${POSTGRESQL_INCLUDE_DIR}"
                                   INTERFACE_LINK_LIBRARIES "${POSTGRESQL_LIBPQ_LIBRARY}")
endif()

if(POSTGRESQL_FOUND AND NOT TARGET PostgreSQL::PostgreSQL)
  add_library(PostgreSQL::PostgreSQL INTERFACE IMPORTED)
  set_target_properties(PostgreSQL::PostgreSQL
                        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                   "${POSTGRESQL_INCLUDE_DIR}"
                                   INTERFACE_LINK_LIBRARIES "${POSTGRESQL_LIBPQ_LIBRARY}")
endif()

mark_as_advanced(POSTGRESQL_PG_CONFIG
                 POSTGRESQL_INCLUDE_DIR
                 POSTGRESQL_SERVER_INCLUDE_DIR
                 POSTGRESQL_LIBRARY_DIR
                 POSTGRESQL_PKGLIB_DIR
                 POSTGRESQL_SHARE_DIR
                 POSTGRESQL_EXTENSION_DIR
                 POSTGRESQL_SERVER_EXECUTABLE
                 POSTGRESQL_REGRESS
                 POSTGRESQL_LIBPQ_LIBRARY)
