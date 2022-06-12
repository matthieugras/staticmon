configure_file(config.h.in config.h @ONLY)
set(STATICMON_SOURCES staticmon.cpp)
add_executable(staticmon ${STATICMON_SOURCES})
target_include_directories(staticmon PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
                                             ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(staticmon PRIVATE CONAN_PKG::fmt CONAN_PKG::abseil
                                        foonathan::lexy)
