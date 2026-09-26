set(SOURCE_DIR ${PROJECT_3RDPARTY_SRC_DIR}/ubs-comm/ubs-comm)
set(BUILD_DIR ${PROJECT_BUILD_PATH}/build/ubs-comm/)
set(INSTALL_DIR ${PROJECT_3RDPARTY_BIN_DIR}/ubs-comm)

set(CHECK_FILE_HCOM ${INSTALL_DIR}/include/hcom/hcom.h)

set(UBSIO_HCOM_PINNED_COMMIT 439961a605d39d2eb8eaabc69d7442dad4395e42)
set(UBSIO_HCOM_PINNED_SOURCE OFF)
if(DEFINED ENV{UBSIO_HCOM_SOURCE_DIR} AND NOT "$ENV{UBSIO_HCOM_SOURCE_DIR}" STREQUAL "")
    set(SOURCE_DIR "$ENV{UBSIO_HCOM_SOURCE_DIR}")
    set(UBSIO_HCOM_PINNED_SOURCE ON)
    if(UBSIO_HCOM_ENABLE_RDMA)
        message(FATAL_ERROR "Pinned TCP HCOM requires UBSIO_HCOM_ENABLE_RDMA=OFF")
    endif()
    execute_process(COMMAND git -C "${SOURCE_DIR}" rev-parse HEAD
                    OUTPUT_VARIABLE HCOM_SOURCE_COMMIT OUTPUT_STRIP_TRAILING_WHITESPACE
                    RESULT_VARIABLE HCOM_SOURCE_RESULT)
    if(NOT HCOM_SOURCE_RESULT EQUAL 0 OR NOT HCOM_SOURCE_COMMIT STREQUAL "${UBSIO_HCOM_PINNED_COMMIT}")
        message(FATAL_ERROR "HCOM source must be ${UBSIO_HCOM_PINNED_COMMIT}; got ${HCOM_SOURCE_COMMIT}")
    endif()
endif()

find_path(HCOM_SYSTEM_INCLUDE_DIR
        NAMES hcom/hcom.h
        PATHS /usr/include
        NO_DEFAULT_PATH
)

find_library(HCOM_SYSTEM_SHARED_LIBRARY
        NAMES hcom libhcom.so
        PATHS /usr/lib64 /usr/lib
        NO_DEFAULT_PATH
)

if (NOT UBSIO_HCOM_PINNED_SOURCE AND HCOM_SYSTEM_INCLUDE_DIR AND HCOM_SYSTEM_SHARED_LIBRARY)
    message(STATUS "Found system-installed HCOM")
    set(USE_SYSTEM_HCOM ON)
    set(HCOM_FINAL_INCLUDE_DIR ${HCOM_SYSTEM_INCLUDE_DIR})
    set(HCOM_FINAL_LIBRARY ${HCOM_SYSTEM_SHARED_LIBRARY})
else ()
    message(STATUS "HCOM not found in system. Building from source...")
    set(USE_SYSTEM_HCOM OFF)
endif ()

if (NOT USE_SYSTEM_HCOM)
    set(ubs_comm_SOURCE_DIR ${SOURCE_DIR})
    if(NOT EXISTS "${SOURCE_DIR}/CMakeLists.txt")
        include(FetchContent)
        FetchContent_Declare(
                ubs_comm
                GIT_REPOSITORY https://gitcode.com/openeuler/ubs-comm.git
                GIT_TAG tag_BeiMing-I26.0.RC1.B013
                SOURCE_DIR ${SOURCE_DIR}
        )
        FetchContent_GetProperties(ubs_comm)
        if(NOT ubs_comm_POPULATED)
            FetchContent_Populate(ubs_comm)
        endif()
    endif()

    if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm)")
        set(ENABLE_KP_OPT on)
    else()
        set(ENABLE_KP_OPT off)
    endif()

    if(UBSIO_HCOM_PINNED_SOURCE OR NOT EXISTS ${CHECK_FILE_HCOM} OR
       NOT EXISTS ${INSTALL_DIR}/lib/libhcom_static.a)
        file(MAKE_DIRECTORY ${BUILD_DIR})

        execute_process(
                COMMAND ${CMAKE_COMMAND}
                -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                -DBUILD_TESTS=off
                -DENABLE_ARM_KP=${ENABLE_KP_OPT}
                -DBUILD_WITH_RDMA=${UBSIO_HCOM_ENABLE_RDMA}
                -DCMAKE_INSTALL_PREFIX=${INSTALL_DIR}
                ${ubs_comm_SOURCE_DIR}
                WORKING_DIRECTORY ${BUILD_DIR}
                RESULT_VARIABLE HCOM_CONFIGURE_RESULT
        )
        if(NOT HCOM_CONFIGURE_RESULT EQUAL 0)
            message(FATAL_ERROR "HCOM configure failed")
        endif()
        execute_process(COMMAND ${CMAKE_COMMAND} --build ${BUILD_DIR} --parallel 8
                        RESULT_VARIABLE HCOM_BUILD_RESULT)
        if(NOT HCOM_BUILD_RESULT EQUAL 0)
            message(FATAL_ERROR "HCOM build failed")
        endif()
        execute_process(COMMAND ${CMAKE_COMMAND} --install ${BUILD_DIR}
                        RESULT_VARIABLE HCOM_INSTALL_RESULT)
        if(NOT HCOM_INSTALL_RESULT EQUAL 0)
            message(FATAL_ERROR "HCOM install failed")
        endif()
    endif()

    set(HCOM_FINAL_INCLUDE_DIR "${INSTALL_DIR}/include")
    set(HCOM_FINAL_LIBRARY "${INSTALL_DIR}/lib/libhcom_static.a")
endif ()

if(NOT TARGET hcom_lib)
    add_library(hcom_lib STATIC IMPORTED GLOBAL)

    set_target_properties(hcom_lib PROPERTIES
            IMPORTED_LOCATION "${HCOM_FINAL_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${HCOM_FINAL_INCLUDE_DIR}"
    )

    set(HCOM_LIBRARIES hcom_lib CACHE INTERNAL "HCOM libraries")
    set(HCOM_INCLUDE_DIRS "${HCOM_FINAL_INCLUDE_DIR}" CACHE INTERNAL "HCOM includes")
endif()

message(STATUS "HCOM Target defined. Lib: ${HCOM_FINAL_LIBRARY}")
