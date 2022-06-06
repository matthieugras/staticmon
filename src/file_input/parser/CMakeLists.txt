set(PARSER_SOURCES trace_parser.cpp)
set(PARSER_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
add_library(parser STATIC ${PARSER_SOURCES})
target_include_directories(parser PUBLIC ${PARSER_INCLUDES})
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
