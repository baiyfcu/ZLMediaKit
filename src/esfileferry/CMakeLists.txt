project(esfileferry)

option(ESFILEFERRY_BUILD_TESTS "Build esfileferry tests" ON)

set(esfileferry_Root ${CMAKE_CURRENT_SOURCE_DIR})
#收集源代码
file(GLOB esfileferry_src_list ${esfileferry_Root}/*.h ${esfileferry_Root}/*.cpp)

add_library(esfileferry STATIC ${esfileferry_src_list})
add_library(ZLMediaKit::EsFileFerry ALIAS esfileferry)

target_compile_options(esfileferry
    PRIVATE ${COMPILE_OPTIONS_DEFAULT})
if(ENABLE_CXX_API)
    target_compile_definitions(esfileferry PRIVATE MediaKit_EXPORTS)
endif()
target_include_directories(esfileferry
    PUBLIC
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../src>"
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../3rdpart/ZLToolKit/src>"
)

update_cached_list(MK_COMPILE_DEFINITIONS ENABLE_FERRY)
#update_cached_list(MK_LINK_LIBRARIES ZLMediaKit::EsFileFerry)

if(ESFILEFERRY_BUILD_TESTS)
    find_package(CURL REQUIRED)
    add_executable(esfileferry_packer_tests tests/test_packer.cpp)
    target_sources(esfileferry_packer_tests PRIVATE $<TARGET_OBJECTS:esfileferry>)
    target_link_libraries(esfileferry_packer_tests PRIVATE ext-codec mk_api)
    add_executable(esfileferry_http_fetcher_tests tests/test_http_fetcher.cpp)
    target_sources(esfileferry_http_fetcher_tests PRIVATE $<TARGET_OBJECTS:esfileferry>)
    target_link_libraries(esfileferry_http_fetcher_tests PRIVATE ext-codec mk_api)
    add_executable(esfileferry_unpacker_tests tests/test_unpacker.cpp)
    target_sources(esfileferry_unpacker_tests PRIVATE $<TARGET_OBJECTS:esfileferry>)
    target_link_libraries(esfileferry_unpacker_tests PRIVATE ext-codec mk_api CURL::libcurl)
endif()
