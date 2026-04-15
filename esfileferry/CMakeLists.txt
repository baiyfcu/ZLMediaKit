project(esfileferry)

option(ESFILEFERRY_BUILD_TESTS "Build esfileferry tests" ON)

set(ESFILEFERRY_SOURCES HttpStreamFetcher.cpp EsFileFerryPacker.cpp EsFileFerryPlayer.cpp EsFileFerryPuller.cpp EsFilePayloadProtocol.cpp)

add_library(esfileferry STATIC ${ESFILEFERRY_SOURCES})
add_library(ZLMediaKit::EsFileFerry ALIAS esfileferry)

target_compile_options(esfileferry
    PRIVATE ${COMPILE_OPTIONS_DEFAULT})
target_include_directories(esfileferry
    PUBLIC
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../src>"
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../3rdpart/ZLToolKit/src>"
)

if(ESFILEFERRY_BUILD_TESTS)
    find_package(CURL REQUIRED)
    add_executable(esfileferry_packer_tests tests/test_packer.cpp)
    target_link_libraries(esfileferry_packer_tests PRIVATE esfileferry ext-codec mk_api)
    add_executable(esfileferry_http_fetcher_tests tests/test_http_fetcher.cpp)
    target_link_libraries(esfileferry_http_fetcher_tests PRIVATE esfileferry ext-codec mk_api)
    add_executable(esfileferry_unpacker_tests tests/test_unpacker.cpp)
    target_link_libraries(esfileferry_unpacker_tests PRIVATE esfileferry ext-codec mk_api CURL::libcurl)
endif()
