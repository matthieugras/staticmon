add_subdirectory(parser)

set(FILE_DRIVER_SOURCES deps.cpp file_monitor_driver.cpp)
add_library(file_driver STATIC ${FILE_DRIVER_SOURCES})
target_include_directories(file_driver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(file_driver formula monitor parser)
