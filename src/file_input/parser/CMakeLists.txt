set(PARSER_SOURCES trace_parser.cpp)
add_library(parser STATIC ${PARSER_SOURCES})
target_include_directories(parser PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(
  parser
  common
  operators
  formula
  CONAN_PKG::fmt
  CONAN_PKG::boost
  CONAN_PKG::abseil
  foonathan::lexy
)
