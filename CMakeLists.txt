cmake_minimum_required(VERSION 3.15)
project(solar)

include(cmake/proto.cmake)

set(CXX_FLAGS "-Wall")
set(CMAKE_CXX_FLAGS "${CXX_FLAGS}")

# cpp projects
add_subdirectory(sequencer)

