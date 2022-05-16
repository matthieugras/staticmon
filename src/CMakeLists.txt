set(MAIN_SOURCES main.cpp)

add_executable(monitor ${MAIN_SOURCES})
target_link_libraries(monitor CONAN_PKG::abseil CONAN_PKG::boost CONAN_PKG::fmt)
