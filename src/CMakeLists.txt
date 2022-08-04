configure_file(config.h.in config.h @ONLY)

set(STATICMON_SOURCES staticmon.cpp)
add_executable(staticmon ${STATICMON_SOURCES})
target_include_directories(staticmon PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
                                             ${CMAKE_CURRENT_BINARY_DIR})
if(USE_JEMALLOC)
  target_link_libraries(staticmon CONAN_PKG::jemalloc)
endif()

target_link_libraries(staticmon CONAN_PKG::boost CONAN_PKG::fmt
                      CONAN_PKG::abseil foonathan::lexy)
target_compile_options(staticmon PRIVATE -Wall -Wextra -Wconversion
                                         -Wsign-conversion)
if(ENABLE_TRACE_TIME)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_compile_options(staticmon PRIVATE -ftime-trace)
  else()
    message(FATAL_ERROR "tracing only works with Clang")
  endif()
endif()
