add_library(operators STATIC deps.cpp)
target_include_directories(operators PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(operators common CONAN_PKG::abseil CONAN_PKG::boost
                      CONAN_PKG::fmt)
