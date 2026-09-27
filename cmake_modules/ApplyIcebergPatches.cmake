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

function(apply_iceberg_patches source_dir)
  find_package(Git REQUIRED)
  file(GLOB patches CONFIGURE_DEPENDS
       "${PROJECT_SOURCE_DIR}/cmake_modules/patches/*.patch")
  foreach(patch IN LISTS patches)
    # Reconfiguration is idempotent, but an upstream change must fail closed.
    execute_process(COMMAND "${GIT_EXECUTABLE}" apply --reverse --check "${patch}"
                    WORKING_DIRECTORY "${source_dir}"
                    RESULT_VARIABLE applied
                    OUTPUT_QUIET ERROR_QUIET)
    if(NOT applied EQUAL 0)
      execute_process(COMMAND "${GIT_EXECUTABLE}" apply --check "${patch}"
                      WORKING_DIRECTORY "${source_dir}"
                      RESULT_VARIABLE checked
                      ERROR_VARIABLE patch_error)
      if(NOT checked EQUAL 0)
        message(FATAL_ERROR "Iceberg patch does not match the pinned dependency: ${patch}\n${patch_error}"
        )
      endif()
      execute_process(COMMAND "${GIT_EXECUTABLE}" apply "${patch}"
                      WORKING_DIRECTORY "${source_dir}" COMMAND_ERROR_IS_FATAL ANY)
    endif()
    set_property(DIRECTORY
                 APPEND
                 PROPERTY CMAKE_CONFIGURE_DEPENDS "${patch}")
  endforeach()
endfunction()
