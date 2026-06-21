if(POSTGRESQL_REGRESS)
  set(PGICEBERG_REGRESS_TESTS fdw_options fdw_dml fdw_import_schema)

  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/regress")
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tmp_check")
  file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/pg_regress.conf"
       "unix_socket_directories = '${CMAKE_CURRENT_BINARY_DIR}/regress'\n"
       "listen_addresses = '127.0.0.1'\n")

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
