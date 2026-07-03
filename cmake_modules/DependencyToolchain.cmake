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
    "93577b3fe713912b3017e5e3374e89d64c4d0c7a"
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
  set(CPR_USE_SYSTEM_CURL
      OFF
      CACHE BOOL "" FORCE)

  fetchcontent_declare(Iceberg
                       GIT_REPOSITORY https://github.com/apache/iceberg-cpp.git
                       GIT_TAG ${PGICEBERG_ICEBERG_GIT_TAG})
  fetchcontent_makeavailable(Iceberg)

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
