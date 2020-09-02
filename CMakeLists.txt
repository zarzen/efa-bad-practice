cmake_minimum_required(VERSION 3.2)
project(fabTrans)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# set(CMAKE_CXX_FLAGS "-Wall -Wextra")
# set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O0")

set (CMAKE_CXX_STANDARD 11)

if(UNIX AND NOT APPLE)
    # for Linux, BSD, Solaris, Minix
    set(LIBFABRIC_LIBRARY /opt/amazon/efa/lib/libfabric.so)
    set(LIBFABRIC_INCLUDE_DIR /opt/amazon/efa/include)
endif()

# file(COPY scripts ./scripts)
configure_file(scripts/shm_launcher.py ./shm_launcher.py COPYONLY)

######################################################
# - Try to find libfabric (http://directory.fsf.org/wiki/Libfabric)
# Once done this will define
#  LIBFABRIC_FOUND - System has libfabric
#  LIBFABRIC_INCLUDE_DIRS - The libfabric include directories
#  LIBFABRIC_LIBRARIES - The libraries needed to use libfabric
#  LIBFABRIC_DEFINITIONS - Compiler switches required for using libfabric

######################################################
set(LIBFABRIC_PREFIX "" CACHE STRING "Help cmake to find libfabric library (https://github.com/ofiwg/libfabric) into your system.")

######################################################
find_path(LIBFABRIC_INCLUDE_DIR rdma/fabric.h
        HINTS ${LIBFABRIC_PREFIX}/include)

######################################################
find_library(LIBFABRIC_LIBRARY NAMES fabric
        HINTS ${LIBFABRIC_PREFIX}/lib)

######################################################
set(LIBFABRIC_LIBRARIES ${LIBFABRIC_LIBRARY} )
set(LIBFABRIC_INCLUDE_DIRS ${LIBFABRIC_INCLUDE_DIR} )

######################################################
include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBFABRIC_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(libfabric  DEFAULT_MSG
        LIBFABRIC_LIBRARY LIBFABRIC_INCLUDE_DIR)

#######################################################
#mark_as_advanced(LIBFABRIC_INCLUDE_DIR LIBFABRIC_LIBRARY )

include_directories(${LIBFABRIC_INCLUDE_DIR})

######################################################
message(${LIBFABRIC_INCLUDE_DIR})
message( ${LIBFABRIC_LIBRARY} )

# include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)


if(NOT TARGET spdlog)
    # Stand-alone build
    find_package(spdlog REQUIRED)
endif()

add_executable(thd_ctest thd_ctest.cpp util.cpp helper_socket.cpp efa_ep.cpp thd_comm.cpp)
target_link_libraries(thd_ctest ${LIBFABRIC_LIBRARY} pthread rt spdlog::spdlog)

add_executable(thd_comm_pull thd_comm_pull.cpp tcp.cpp efa_ep.cpp thd_comm.cpp helper_socket.cpp util.cpp)
target_link_libraries(thd_comm_pull ${LIBFABRIC_LIBRARY} pthread rt spdlog::spdlog)

add_executable(test_sockets test_socket.cpp helper_socket.cpp)
target_link_libraries(test_sockets spdlog::spdlog)


add_executable(shard_ctl shard_ctl.cpp tcp.cpp helper_socket.cpp)
target_link_libraries(shard_ctl spdlog::spdlog pthread)

add_executable(shard_server shard_serv.cpp helper_socket.cpp efa_ep.cpp thd_comm.cpp util.cpp tcp.cpp)
target_link_libraries(shard_server spdlog::spdlog ${LIBFABRIC_LIBRARY} pthread)

add_executable(shard_cli shard_cli.cpp helper_socket.cpp efa_ep.cpp thd_comm.cpp util.cpp tcp.cpp)
target_link_libraries(shard_cli spdlog::spdlog ${LIBFABRIC_LIBRARY} pthread)