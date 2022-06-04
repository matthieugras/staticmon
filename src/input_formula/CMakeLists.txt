set(FORMULA_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
add_library(formula INTERFACE)
target_include_directories(formula INTERFACE ${FORMULA_INCLUDES})
target_link_libraries(formula INTERFACE operators common CONAN_PKG::abseil CONAN_PKG::boost)
