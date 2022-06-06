set(MONITOR_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
add_library(monitor INTERFACE)
target_include_directories(monitor INTERFACE ${MONITOR_INCLUDES})
target_link_libraries(monitor INTERFACE common operators CONAN_PKG::abseil CONAN_PKG::fmt CONAN_PKG::boost)
