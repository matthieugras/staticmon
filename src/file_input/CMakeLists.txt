add_subdirectory(parser)

add_library(file_driver STATIC deps.cpp)
target_include_directories(file_driver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(file_driver formula monitor parser)
