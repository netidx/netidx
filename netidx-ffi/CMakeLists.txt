cmake_minimum_required(VERSION 3.18)
project(netidx-ffi LANGUAGES C CXX)

find_program(CARGO cargo REQUIRED)

# Map CMake build type to Cargo profile
if(CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo" OR CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
    set(CARGO_PROFILE "--release")
    set(CARGO_OUTPUT_DIR "release")
else()
    set(CARGO_PROFILE "")
    set(CARGO_OUTPUT_DIR "debug")
endif()

set(WORKSPACE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/..")
set(CARGO_TARGET_DIR "${WORKSPACE_ROOT}/target")
set(LIB_PATH "${CARGO_TARGET_DIR}/${CARGO_OUTPUT_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}netidx_ffi${CMAKE_STATIC_LIBRARY_SUFFIX}")

add_custom_target(netidx_ffi_cargo ALL
    COMMAND ${CARGO} build -p netidx-ffi ${CARGO_PROFILE}
    WORKING_DIRECTORY "${WORKSPACE_ROOT}"
    BYPRODUCTS "${LIB_PATH}"
    COMMENT "Building netidx-ffi via cargo"
)

add_library(netidx::ffi STATIC IMPORTED GLOBAL)
set_target_properties(netidx::ffi PROPERTIES
    IMPORTED_LOCATION "${LIB_PATH}"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
add_dependencies(netidx::ffi netidx_ffi_cargo)

# Platform-specific system libraries the Rust runtime needs
if(APPLE)
    set_property(TARGET netidx::ffi APPEND PROPERTY
        INTERFACE_LINK_LIBRARIES "-framework Security" "-framework CoreFoundation" "-framework SystemConfiguration")
elseif(UNIX)
    set_property(TARGET netidx::ffi APPEND PROPERTY
        INTERFACE_LINK_LIBRARIES pthread dl m)
endif()
