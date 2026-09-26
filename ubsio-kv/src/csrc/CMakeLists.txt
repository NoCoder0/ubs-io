file(GLOB_RECURSE KVC_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/kvc/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/nds/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/utils/*.cpp
)

add_library(ubsio_kvc SHARED ${KVC_SRC})

target_include_directories(ubsio_kvc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/kvc
    ${CMAKE_CURRENT_SOURCE_DIR}/nds
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${PROJECT_SOURCE_DIR}/include
    ${UBSIO_BOOSTIO_INCLUDE_DIR}
    ${PROJECT_SOURCE_DIR}/../ubsio-boostio/src/common
)

target_link_libraries(ubsio_kvc
    pthread
    dl
)

set_target_properties(ubsio_kvc PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    OUTPUT_NAME ubsio_kvc
)
if (UBSIO_ENABLE_ORIGIN_RUNPATH)
    set_target_properties(ubsio_kvc PROPERTIES
        BUILD_RPATH "$ORIGIN"
        INSTALL_RPATH "$ORIGIN"
    )
    target_link_options(ubsio_kvc PRIVATE "LINKER:--enable-new-dtags")
endif()

set(TARGET_INSTALL_DIR ${OUTPUT_PATH})
set(TARGET_INSTALL_LIB_DIR ${TARGET_INSTALL_DIR}/lib)
set(TARGET_INSTALL_INCLUDE_DIR ${TARGET_INSTALL_DIR}/include)

install(TARGETS ubsio_kvc 
    LIBRARY DESTINATION ${TARGET_INSTALL_LIB_DIR}
    ARCHIVE DESTINATION ${TARGET_INSTALL_LIB_DIR}
    RUNTIME DESTINATION ${TARGET_INSTALL_LIB_DIR}
)

install(FILES ${PROJECT_SOURCE_DIR}/include/ubsio_kvc.h
    DESTINATION ${TARGET_INSTALL_INCLUDE_DIR}
)
