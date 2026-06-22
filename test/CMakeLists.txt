if(POSTGRESQL_REGRESS)
  set(PGICEBERG_REGRESS_TESTS fdw_options fdw_dml fdw_import_schema)
  set(PGICEBERG_REGRESS_EXTENSION_DIR "${CMAKE_CURRENT_BINARY_DIR}/extension")

  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/regress")
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tmp_check")
  file(MAKE_DIRECTORY "${PGICEBERG_REGRESS_EXTENSION_DIR}")

  file(READ "${PROJECT_SOURCE_DIR}/pgiceberg.control" PGICEBERG_CONTROL)
  string(REGEX
         REPLACE "module_pathname = '[^']+'"
                 "module_pathname = '${PROJECT_BINARY_DIR}/src/pgiceberg'"
                 PGICEBERG_REGRESS_CONTROL "${PGICEBERG_CONTROL}")
  file(WRITE "${PGICEBERG_REGRESS_EXTENSION_DIR}/pgiceberg.control"
       "${PGICEBERG_REGRESS_CONTROL}")
  configure_file("${PROJECT_SOURCE_DIR}/sql/pgiceberg--0.1.0.sql"
                 "${PGICEBERG_REGRESS_EXTENSION_DIR}/pgiceberg--0.1.0.sql" COPYONLY)

  file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/pg_regress.conf"
       "unix_socket_directories = '${CMAKE_CURRENT_BINARY_DIR}/regress'\n"
       "listen_addresses = '127.0.0.1'\n")
  file(APPEND "${CMAKE_CURRENT_BINARY_DIR}/pg_regress.conf"
       "extension_control_path = '${PGICEBERG_REGRESS_EXTENSION_DIR}:$system'\n")

  add_test(NAME regress_fdw
           COMMAND "${POSTGRESQL_REGRESS}" --bindir "${POSTGRESQL_BIN_DIR}" --inputdir
                   "${CMAKE_CURRENT_SOURCE_DIR}/regress" --expecteddir
                   "${CMAKE_CURRENT_SOURCE_DIR}/regress/expected" --outputdir
                   "${CMAKE_CURRENT_BINARY_DIR}/regress" --temp-config
                   "${CMAKE_CURRENT_BINARY_DIR}/pg_regress.conf" --temp-instance
                   "${CMAKE_CURRENT_BINARY_DIR}/tmp_check" ${PGICEBERG_REGRESS_TESTS})
  set_tests_properties(regress_fdw
                       PROPERTIES WORKING_DIRECTORY
                                  "${CMAKE_CURRENT_BINARY_DIR}/regress"
                                  ENVIRONMENT
                                  "PG_REGRESS_SOCK_DIR=${CMAKE_CURRENT_BINARY_DIR}/regress"
  )
else()
  message(STATUS "pg_regress not found; regression tests are not registered")
endif()
