cmake_minimum_required(VERSION 3.20)

include(CheckIPOSupported)
project(
  staticmon
  VERSION 0.1
  DESCRIPTION "C++ monitor"
  LANGUAGES C CXX
)

# option(USE_JEMALLOC "Use jemalloc" OFF)
option(ENABLE_SANITIZERS "Enable ASAN and UBSAN" OFF)
option(ENABLE_FILE_INPUT "Enable reading traces from files" ON)
# option(ENABLE_SOCK_INTF "Enable socket interface" ON)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
set(CMAKE_INCLUDE_CURRENT_DIR ON)

string(
  REPLACE "-O2" "-O3" CMAKE_CXX_FLAGS_RELWITHDEBINFO
  "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}"
)
string(
  REPLACE "-O2" "-O3" CMAKE_C_FLAGS_RELWITHDEBINFO
  "${CMAKE_C_FLAGS_RELWITHDEBINFO}"
)
string(APPEND CMAKE_CXX_FLAGS_DEBUG " -O2")
string(APPEND CMAKE_C_FLAGS_DEBUG " -O2")

set(
  CMAKE_CXX_FLAGS
  "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wconversion -Wsign-conversion -Wno-unused-command-line-argument"
)
check_ipo_supported(LANGUAGES C CXX)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup(TARGETS)

if(ENABLE_SANITIZERS)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address,undefined")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address,undefined")
endif()

set(
  CMAKE_EXE_LINKER_FLAGS
  "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++"
)
set(
  CMAKE_SHARED_LINKER_FLAGS
  "${CMAKE_SHARED_LINKER_FLAGS} -static-libgcc -static-libstdc++"
)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(ENABLE_FILE_INPUT)
  include(cmake/lexy.cmake)
endif()

# if(ENABLE_SOCK_INTF)
#   include(cmake/uring.cmake)
# endif()

add_subdirectory(src)
# add_subdirectory(bench)
# add_subdirectory(tests)
