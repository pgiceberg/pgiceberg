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

# Invoked by Boost's project(), before its library subdirectories are added.
if(NOT PROJECT_VERSION VERSION_EQUAL "1.88.0")
  return()
endif()

set(locale_cmake "${CMAKE_CURRENT_SOURCE_DIR}/libs/locale/CMakeLists.txt")
file(READ "${locale_cmake}" locale_source)
string(REPLACE [=[source_group(TREE "${CMAKE_CURRENT_SOURCE_DIR}/include/boost/locale" PREFIX "headers" FILES ${sources_inc})]=]
               [=[source_group(TREE "${CMAKE_CURRENT_SOURCE_DIR}/include/boost" PREFIX "headers" FILES ${sources_inc})]=]
               locale_patched
               "${locale_source}")
if(NOT locale_source STREQUAL locale_patched)
  # Both locale.hpp and locale/*.hpp are below this common root. This only
  # changes IDE grouping; sources, compiler flags, and library contents stay put.
  file(WRITE "${locale_cmake}" "${locale_patched}")
endif()
