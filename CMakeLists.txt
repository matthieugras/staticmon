cmake_minimum_required(VERSION 3.23)

include(CheckIPOSupported)
project(
  staticmon
  VERSION 0.1
  DESCRIPTION "C++ monitor"
  LANGUAGES C CXX)

option(USE_JEMALLOC "Use jemalloc" OFF)
option(ENABLE_SANITIZERS "Enable ASAN and UBSAN" OFF)
option(ENABLE_FILE_INPUT "Enable reading traces from files" ON)
option(ENABLE_TRACE_TIME "Enable compile time tracing" OFF)
# option(ENABLE_SOCK_INTF "Enable socket interface" ON) set_property(GLOBAL
# PROPERTY GLOBAL_DEPENDS_DEBUG_MODE 1)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup(TARGETS)

string(REPLACE "-O2" "-O3" CMAKE_CXX_FLAGS_RELWITHDEBINFO
               "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
string(REPLACE "-O2" "-O3" CMAKE_C_FLAGS_RELWITHDEBINFO
               "${CMAKE_C_FLAGS_RELWITHDEBINFO}")

check_ipo_supported(LANGUAGES C CXX)

if(CMAKE_BUILD_TYPE MATCHES DEBUG)
  add_compile_definitions(GLIBCXX_DEBUG=1)
  # add_compile_options(-O2)
endif()

if((CMAKE_BUILD_TYPE MATCHES RELEASE) OR (CMAKE_BUILD_TYPE MATCHES
                                          RELWITHDEBINFO))
  add_compile_options(-flto=full -march=native)
endif()

find_program(TEST_LLD ld.lld)
if(TEST_LLD AND (CMAKE_CXX_COMPILER_ID STREQUAL "Clang"))
  add_link_options(-fuse-ld=lld)
endif()

if(ENABLE_SANITIZERS)
  find_library(ASAN asan)
  find_library(UBSAN ubsan)
  link_libraries(${ASAN} ${UBSAN})
  add_compile_options(-fsanitize=address,undefined)
endif()

add_link_options(-static-libstdc++)

if(ENABLE_FILE_INPUT)
  include(cmake/lexy.cmake)
endif()

# if(ENABLE_SOCK_INTF) include(cmake/uring.cmake) endif()

add_subdirectory(src)
# add_subdirectory(bench) add_subdirectory(tests)
