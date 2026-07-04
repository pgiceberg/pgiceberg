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

add_library(pgiceberg MODULE
            extension.cc
            common/catalog.cc
            common/datum_convert.cc
            common/type_mapping.cc
            fdw/fdw.cc
            fdw/iceberg_scan.cc
            fdw/modify_state.cc
            fdw/options.cc
            fdw/scan_state.cc
            functions/create_table.cc
            functions/metadata.cc
            functions/register_table.cc)
target_include_directories(pgiceberg PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(pgiceberg PRIVATE PostgreSQL::Server PostgreSQL::Client)
include(CheckLinkerFlag)
check_linker_flag(CXX "LINKER:--exclude-libs,ALL" PGICEBERG_HAS_EXCLUDE_LIBS)
if(APPLE)
  if(NOT POSTGRESQL_SERVER_EXECUTABLE)
    message(FATAL_ERROR "postgres executable is required to link pgiceberg on macOS")
  endif()
  target_link_options(pgiceberg PRIVATE
                      "LINKER:-bundle_loader,${POSTGRESQL_SERVER_EXECUTABLE}")
endif()
if(PGICEBERG_HAS_EXCLUDE_LIBS)
  target_link_options(pgiceberg PRIVATE "LINKER:--exclude-libs,ALL")
endif()
set_target_properties(pgiceberg
                      PROPERTIES PREFIX ""
                                 OUTPUT_NAME pgiceberg
                                 CXX_VISIBILITY_PRESET hidden
                                 VISIBILITY_INLINES_HIDDEN ON)
set_target_properties(pgiceberg PROPERTIES BUILD_RPATH "${POSTGRESQL_LIBRARY_DIR}"
                                           INSTALL_RPATH "${POSTGRESQL_LIBRARY_DIR}")

target_link_libraries(pgiceberg PRIVATE ${PGICEBERG_ICEBERG_TARGET})

if(PGICEBERG_SANITIZERS)
  add_library(pgiceberg_sanitizers INTERFACE)
  target_compile_options(pgiceberg_sanitizers
                         INTERFACE "-fsanitize=${PGICEBERG_SANITIZERS}"
                                   -fno-omit-frame-pointer)
  target_link_options(pgiceberg_sanitizers INTERFACE "-fsanitize=${PGICEBERG_SANITIZERS}")
  target_link_libraries(pgiceberg PRIVATE pgiceberg_sanitizers)
endif()

if(PGICEBERG_ENABLE_REST_CATALOG)
  target_compile_definitions(pgiceberg PRIVATE PGICEBERG_ENABLE_REST_CATALOG=1)
endif()

install(TARGETS pgiceberg LIBRARY DESTINATION "${POSTGRESQL_PKGLIB_DIR}"
                                  COMPONENT pgiceberg)
if(APPLE)
  install(CODE "file(REMOVE \"\$ENV{DESTDIR}${POSTGRESQL_PKGLIB_DIR}/pgiceberg\")
file(CREATE_LINK \"$<TARGET_FILE_NAME:pgiceberg>\"
     \"\$ENV{DESTDIR}${POSTGRESQL_PKGLIB_DIR}/pgiceberg\" SYMBOLIC)" COMPONENT pgiceberg)
endif()
install(FILES "${PROJECT_SOURCE_DIR}/pgiceberg.control"
        DESTINATION "${POSTGRESQL_EXTENSION_DIR}"
        COMPONENT pgiceberg)
install(FILES "${PROJECT_SOURCE_DIR}/sql/pgiceberg--0.1.0.sql"
        DESTINATION "${POSTGRESQL_EXTENSION_DIR}"
        COMPONENT pgiceberg)
