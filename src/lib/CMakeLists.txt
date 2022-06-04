set(OPERATORS_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
add_library(operators INTERFACE)
target_include_directories(
  operators
  INTERFACE ${OPERATORS_INCLUDES}
)
target_link_libraries(operators INTERFACE common CONAN_PKG::abseil CONAN_PKG::boost CONAN_PKG::fmt)
