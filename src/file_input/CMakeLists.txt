add_subdirectory(parser)

set(FILE_DRIVER_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
add_library(file_driver INTERFACE)
target_include_directories(file_driver INTERFACE ${FILE_DRIVER_INCLUDES})
target_link_libraries(file_driver INTERFACE formula monitor parser)
