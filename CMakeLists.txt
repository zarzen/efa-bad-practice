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
set(common_files
        efa_ep.cpp
        worker.cpp
        communicator.cpp
        util.cpp
        sock_cli_serv.cpp
        efa_thd.cpp
        )

add_executable(bw_cli demo_cli_bw.cpp ${common_files})
target_link_libraries(bw_cli ${LIBFABRIC_LIBRARY} pthread)

add_executable(bw_serv demo_serv_bw.cpp ${common_files})
target_link_libraries(bw_serv ${LIBFABRIC_LIBRARY} pthread)

add_executable(msg_cli demo_msg_cli.cpp ${common_files})
target_link_libraries(msg_cli ${LIBFABRIC_LIBRARY} pthread)

add_executable(msg_serv demo_msg_serv.cpp ${common_files})
target_link_libraries(msg_serv ${LIBFABRIC_LIBRARY} pthread)

add_executable(nothd_cli demo_nothd_cli.cpp ${common_files})
target_link_libraries(nothd_cli ${LIBFABRIC_LIBRARY} pthread)

add_executable(nothd_serv demo_nothd_serv.cpp ${common_files})
target_link_libraries(nothd_serv ${LIBFABRIC_LIBRARY} pthread)

add_executable(shm_worker shm_worker.cpp util.cpp efa_ep.cpp)
target_link_libraries(shm_worker ${LIBFABRIC_LIBRARY} pthread rt)

add_executable(shm_cli_comm shm_cli_comm.cpp util.cpp sock_cli_serv.cpp efa_ep.cpp)
target_link_libraries(shm_cli_comm ${LIBFABRIC_LIBRARY} pthread rt)

add_executable(shm_serv_comm shm_serv_comm.cpp util.cpp sock_cli_serv.cpp efa_ep.cpp)
target_link_libraries(shm_serv_comm ${LIBFABRIC_LIBRARY} pthread rt)

# add_executable(fi_rdm src/rdm.c ${common_files})
# target_link_libraries(fi_rdm ${LIBFABRIC_LIBRARY})
