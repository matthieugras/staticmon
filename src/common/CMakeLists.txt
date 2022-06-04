set(COMMON_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
add_library(common INTERFACE)
target_include_directories(common INTERFACE ${COMMON_INCLUDES})
target_link_libraries(common INTERFACE CONAN_PKG::boost CONAN_PKG::fmt CONAN_PKG::abseil)
