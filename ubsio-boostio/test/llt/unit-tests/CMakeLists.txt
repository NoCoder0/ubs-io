cmake_minimum_required(VERSION 3.16)
if(COMMAND cmake_policy)
    cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)
project(BIO_TEST VERSION 1.0 LANGUAGES CXX)
EXECUTE_PROCESS(COMMAND g++ -dumpversion
        TIMEOUT 5
        OUTPUT_VARIABLE cPlusPlusVerString
        OUTPUT_STRIP_TRAILING_WHITESPACE)
string(REGEX MATCH "[(0-9)]*" cPlusPlusVerNum ${cPlusPlusVerString})
message(${cPlusPlusVerNum})
if (${cPlusPlusVerNum} GREATER 4)
    set(CMAKE_CXX_STANDARD 14)
else()
    set(CMAKE_CXX_STANDARD 11)
endif()

set(GTEST_INSTALL_DIR "${TEST_TOOL_INSTALL_PATH}/googletest")
if (NOT EXISTS ${GTEST_INSTALL_DIR})
    message(ERROR "GTEST_INSTALL_DIR(${GTEST_INSTALL_DIR}) is invalid")
endif()
find_library(GTEST_LIBRARY
        NAMES gtest
        PATHS
        "${GTEST_INSTALL_DIR}/lib64"
        "${GTEST_INSTALL_DIR}/lib"
        NO_DEFAULT_PATH)
if (NOT GTEST_LIBRARY)
    message(FATAL_ERROR "libgtest was not found under ${GTEST_INSTALL_DIR}/lib64 or ${GTEST_INSTALL_DIR}/lib")
endif()

set(MOCKCPP_INSTALL_DIR "${TEST_TOOL_INSTALL_PATH}/mockcpp")
if (NOT EXISTS ${MOCKCPP_INSTALL_DIR})
    message(ERROR "MOCKCPP_INSTALL_DIR(${MOCKCPP_INSTALL_DIR}) is invalid")
endif()

aux_source_directory(mock BIO_TEST)
aux_source_directory(htracer BIO_TEST)
aux_source_directory(server BIO_TEST)
aux_source_directory(cache/read BIO_TEST)
aux_source_directory(cache/write BIO_TEST)
aux_source_directory(net BIO_TEST)
aux_source_directory(cm BIO_TEST)
aux_source_directory(disk BIO_TEST)
aux_source_directory(sdk BIO_TEST)
aux_source_directory(common BIO_TEST)
aux_source_directory(underfs BIO_TEST)
add_executable(bio_test ${BIO_TEST} main.cpp ut_common.cpp ut_common.h)

# Startup configuration tests must not inherit bio_test's live SDK/network threads.
add_executable(bio_kvc_config_test sdk/config/bio_kvc_config_test.cpp)
target_include_directories(bio_kvc_config_test PRIVATE
        ${SRC_DIR}/sdk
        ${PROJECT_OUTPUT_PATH}/boostio_test_tools/googletest/include/)
target_link_libraries(bio_kvc_config_test bio_sdk ${GTEST_LIBRARY} pthread)

set(TEST_DIR ${PROJECT_SOURCE_DIR})
target_include_directories(bio_test PRIVATE
        ${TEST_DIR}/mock
        ${TEST_DIR}/htracer
        ${TEST_DIR}/server
        ${TEST_DIR}/cm
        ${TEST_DIR}/disk
        ${TEST_DIR}/sdk
        ${TEST_DIR}/common
        ${TEST_DIR}/cache/read
        ${TEST_DIR}/cache/write
        ${TEST_DIR}/net
        ${TEST_DIR}/underfs
		${TEST_DIR}/../../tools/tracepoint/include
		${TEST_DIR}/../../tools/bio_tp
        ${SRC_DIR}/common
        ${SRC_DIR}/cache
        ${SRC_DIR}/cache/read
        ${SRC_DIR}/cache/write
        ${SRC_DIR}/cluster
        ${SRC_DIR}/cluster/common
        ${SRC_DIR}/cluster/client
        ${SRC_DIR}/cluster/server
        ${SRC_DIR}/config
        ${SRC_DIR}/disk
        ${SRC_DIR}/htracer
		${SRC_DIR}/net
        ${SRC_DIR}/sdk
        ${SRC_DIR}/server
        ${SRC_DIR}/stream
        ${SRC_DIR}/underfs
        ${PROJECT_OUTPUT_PATH}/boostio_test_tools/googletest/include/
        ${PROJECT_OUTPUT_PATH}/boostio_test_tools/mockcpp/include/)

target_link_libraries(bio_test bio_sdk bio_server
        -Wl,--start-group
        pthread dl rt uring bio_underfs
        ${GTEST_LIBRARY}
        ${PROJECT_OUTPUT_PATH}/boostio_test_tools/mockcpp/lib/libmockcpp.a
        -Wl,--end-group)

set_target_properties(bio_test PROPERTIES OUTPUT_NAME "bio_test")
set_target_properties(bio_test PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties(bio_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
