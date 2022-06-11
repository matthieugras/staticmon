add_library(formula STATIC deps.cpp)
target_include_directories(formula PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(formula operators common CONAN_PKG::abseil
                      CONAN_PKG::boost)
