set(MONITOR_SOURCES deps.cpp monitor.cpp)
add_library(monitor STATIC ${MONITOR_SOURCES})
target_include_directories(monitor PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(
  monitor
  formula
  common
  operators
  CONAN_PKG::abseil
  CONAN_PKG::fmt
  CONAN_PKG::boost)
