# Author : Animesh Trivedi

cmake_minimum_required (VERSION 2.6)

project (benchmark-arrow-cpp)

set(PROJECT_SOURCE_DIR ${CMAKE_SOURCE_DIR}/src)
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/bin)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})

set(CMAKE_CXX_COMPILER g++) # or clang++
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -ffast-math -funroll-loops -march=native") # get crazy here
add_compile_options("-std=c++17")

set(CMAKE_BUILD_TYPE Release) # or debug if you need to use gdb on it.

find_library(ARROW_LIBRARY arrow HINTS /home/atr/local/lib)

link_libraries(pthread ${ARROW_LIBRARY})

include_directories("${PROJECT_SOURCE_DIR}" "/home/atr/local/include/")

add_executable(benchmark-arrow-cpp ${PROJECT_SOURCE_DIR}/main.cc)