cmake_minimum_required(VERSION 3.16)
project(myko_cpp VERSION 3.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find corrosion for Rust integration, or use manual approach
find_package(Corrosion QUIET)

if(Corrosion_FOUND)
    # Use corrosion for automatic Rust compilation
    corrosion_import_crate(MANIFEST_PATH Cargo.toml)

    # Create interface library for headers
    add_library(myko INTERFACE)
    target_link_libraries(myko INTERFACE myko_cpp)
    target_include_directories(myko INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/myko_cpp/src
    )
else()
    # Manual approach: assume library is pre-built
    message(STATUS "Corrosion not found. Using pre-built library approach.")
    message(STATUS "Run 'cargo build --release' first to build the Rust library.")

    # Create imported library
    add_library(myko_cpp_rust STATIC IMPORTED)

    # Set library location based on build type
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(RUST_LIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/target/debug")
    else()
        set(RUST_LIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/target/release")
    endif()

    set_target_properties(myko_cpp_rust PROPERTIES
        IMPORTED_LOCATION "${RUST_LIB_DIR}/libmyko_cpp.a"
    )

    # Create interface library
    add_library(myko INTERFACE)
    target_link_libraries(myko INTERFACE myko_cpp_rust)
    target_include_directories(myko INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${RUST_LIB_DIR}/cxxbridge/myko_cpp/src
    )

    # Link system libraries needed by Rust
    if(UNIX AND NOT APPLE)
        target_link_libraries(myko INTERFACE pthread dl m)
    elseif(APPLE)
        target_link_libraries(myko INTERFACE pthread dl)
        find_library(SECURITY_FRAMEWORK Security)
        find_library(COREFOUNDATION_FRAMEWORK CoreFoundation)
        target_link_libraries(myko INTERFACE ${SECURITY_FRAMEWORK} ${COREFOUNDATION_FRAMEWORK})
    elseif(WIN32)
        target_link_libraries(myko INTERFACE ws2_32 userenv bcrypt)
    endif()
endif()

# Example executable (optional)
option(BUILD_EXAMPLES "Build example programs" OFF)
if(BUILD_EXAMPLES)
    add_executable(myko_example examples/main.cpp)
    target_link_libraries(myko_example PRIVATE myko)
endif()

# Install rules
include(GNUInstallDirs)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
