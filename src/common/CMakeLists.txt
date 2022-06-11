configure_file(config.h.in config.h @ONLY)
set(COMMON_CONFIG_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}
                          ${CMAKE_CURRENT_SOURCE_DIR})
set(COMMON_SOURCES deps.cpp util.cpp)
add_library(common STATIC ${COMMON_SOURCES})
target_include_directories(common PUBLIC ${COMMON_CONFIG_INCLUDE})
target_link_libraries(common CONAN_PKG::boost CONAN_PKG::fmt CONAN_PKG::abseil)
