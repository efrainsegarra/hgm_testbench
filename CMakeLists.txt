cmake_minimum_required(VERSION 3.11)

project(hgm_testbench)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(N2read)


add_executable(hgm_test hgm_test.cpp )
target_link_libraries( hgm_test LINK_PUBLIC N2readData )
