configure_file(config.h.in config.h @ONLY)

set(COMMON_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
set(COMMON_SOURCES util.cpp)
add_library(common STATIC ${COMMON_SOURCES})
target_include_directories(common PUBLIC ${COMMON_INCLUDES})
target_link_libraries(common CONAN_PKG::boost CONAN_PKG::fmt CONAN_PKG::abseil)
