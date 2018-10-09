# Author : Animesh Trivedi 

cmake_minimum_required (VERSION 2.6)
set(CMAKE_CXX_FLAGS "-fPIC -std=c++17 -O3 -g")

project (benchmark-arrow-cpp)
find_library(ARROW_LIBRARY arrow HINTS /home/atr/local/lib)

add_executable(benchmark-arrow-cpp ${PROJECT_SOURCE_DIR}/src/main.cc)
target_link_libraries (benchmark-arrow-cpp PUBLIC ${ARROW_LIBRARY})

