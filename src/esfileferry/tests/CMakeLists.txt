cmake_minimum_required(VERSION 3.16)

cmake_policy(SET CMP0077 NEW)

project(esfileferry_tests LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(ESFILEFERRY_TESTS_DIR "${CMAKE_CURRENT_LIST_DIR}" ABSOLUTE)
get_filename_component(ZLM_ROOT "${ESFILEFERRY_TESTS_DIR}/../../.." ABSOLUTE)

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  # Standalone mode: bootstrap a minimal ZLMediaKit build and then reuse its
  # integrated ferry implementation through the zlmediakit target.
  set(ENABLE_FERRY ON CACHE BOOL "" FORCE)
  set(ENABLE_API ON CACHE BOOL "" FORCE)
  set(ENABLE_API_STATIC_LIB ON CACHE BOOL "" FORCE)
  set(ENABLE_TESTS OFF CACHE BOOL "" FORCE)
  set(ESFILEFERRY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(ENABLE_PLAYER OFF CACHE BOOL "" FORCE)
  set(ENABLE_SERVER OFF CACHE BOOL "" FORCE)
  set(ENABLE_WEBRTC OFF CACHE BOOL "" FORCE)
  set(ENABLE_SRT OFF CACHE BOOL "" FORCE)
  add_subdirectory("${ZLM_ROOT}" "${CMAKE_CURRENT_BINARY_DIR}/zlm")
endif()

find_package(CURL REQUIRED)

add_executable(esfileferry_packer_tests test_packer.cpp)
target_link_libraries(esfileferry_packer_tests PRIVATE zlmediakit mk_api)

add_executable(esfileferry_http_fetcher_tests test_http_fetcher.cpp)
target_link_libraries(esfileferry_http_fetcher_tests PRIVATE zlmediakit mk_api)

add_executable(esfileferry_unpacker_tests test_unpacker.cpp)
target_link_libraries(esfileferry_unpacker_tests PRIVATE zlmediakit mk_api CURL::libcurl)

add_custom_target(esfileferry_tests_all
    DEPENDS
        esfileferry_packer_tests
        esfileferry_http_fetcher_tests
        esfileferry_unpacker_tests)

message(STATUS "esfileferry tests root: ${ESFILEFERRY_TESTS_DIR}")
message(STATUS "ZLMediaKit root: ${ZLM_ROOT}")
message(STATUS "Build one test with: cmake --build <build-dir> --target esfileferry_packer_tests")
