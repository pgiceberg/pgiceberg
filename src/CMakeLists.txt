add_library(pgiceberg MODULE
            extension.cc
            common/catalog.cc
            common/datum_convert.cc
            common/type_mapping.cc
            fdw/fdw.cc
            fdw/modify_state.cc
            fdw/options.cc
            fdw/scan_state.cc
            udf/create_table.cc)
target_include_directories(pgiceberg PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(pgiceberg PRIVATE PostgreSQL::Server PostgreSQL::Client)
if(APPLE)
  if(NOT POSTGRESQL_SERVER_EXECUTABLE)
    message(FATAL_ERROR "postgres executable is required to link pgiceberg on macOS")
  endif()
  target_link_options(pgiceberg PRIVATE
                      "LINKER:-bundle_loader,${POSTGRESQL_SERVER_EXECUTABLE}")
endif()
set_target_properties(pgiceberg PROPERTIES PREFIX "" OUTPUT_NAME pgiceberg)
set_target_properties(pgiceberg PROPERTIES BUILD_RPATH "${POSTGRESQL_LIBRARY_DIR}"
                                           INSTALL_RPATH "${POSTGRESQL_LIBRARY_DIR}")

if(PGICEBERG_ICEBERG_TARGET)
  target_link_libraries(pgiceberg PRIVATE ${PGICEBERG_ICEBERG_TARGET})
endif()

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

configure_file("${PROJECT_SOURCE_DIR}/pgiceberg.control.in"
               "${PROJECT_BINARY_DIR}/pgiceberg.control" @ONLY)

install(TARGETS pgiceberg LIBRARY DESTINATION "${POSTGRESQL_PKGLIB_DIR}"
                                  COMPONENT pgiceberg)
install(FILES "${PROJECT_BINARY_DIR}/pgiceberg.control"
        DESTINATION "${POSTGRESQL_EXTENSION_DIR}"
        COMPONENT pgiceberg)
install(FILES "${PROJECT_SOURCE_DIR}/sql/pgiceberg--0.1.0.sql"
        DESTINATION "${POSTGRESQL_EXTENSION_DIR}"
        COMPONENT pgiceberg)
