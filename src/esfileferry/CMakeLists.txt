project(esfileferry)

option(ESFILEFERRY_BUILD_TESTS "Build esfileferry tests" ON)

find_package(CURL REQUIRED)

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
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>"
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../3rdpart/ZLToolKit/src>"
)
target_link_libraries(esfileferry
    PRIVATE CURL::libcurl)

update_cached_list(MK_COMPILE_DEFINITIONS ENABLE_FERRY)
#update_cached_list(MK_LINK_LIBRARIES ZLMediaKit::EsFileFerry)
