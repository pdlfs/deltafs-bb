#
# CMakeLists.txt  cmake file for deltafs-preload directory
# 14-Jan-2017  saukad@cs.cmu.edu
#

#
# this file is either included from ../CMakeLists.txt or some other
# file if we are being embedded within another project.
#
#
# since this is for a preload library, we don't expect anyone to
# link to us like a normal lib, so this CMakeLists.txt is cut down
# from a normal library config...
#

# configure/load in standard modules we plan to use
set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)

find_package (deltafs CONFIG REQUIRED)
#set (CMAKE_THREAD_LIBS_INIT TRUE)
#
# create the library target 
#
#add_library (deltafs-bb rpc/rpc_engine.cc server/buddyserver.cc client/buddyclient.cc)
add_library (deltafs-bb client/buddyclient.cc server/buddyserver.cc)
target_link_libraries (deltafs-bb mercury deltafs pdlfs-common pthread)
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (deltafs-bb "${CMAKE_THREAD_LIBS_INIT}")
endif ()

#
# test program
#
#find_package(MERCURY REQUIRED)
add_executable(bbos_server ../tests/bbos_unit_tests.cc server/buddyserver.cc)
target_link_libraries(bbos_server mercury mercury_hl deltafs pdlfs-common pthread)
add_executable(bbos_client ../tests/bbos_unit_tests_client.cc client/buddyclient.cc)
target_link_libraries(bbos_client mercury mercury_hl deltafs pdlfs-common pthread)

#add_executable(bbos_test_client ../tests/bbos_unit_tests_client.cc rpc/rpc_engine.cc server/buddyserver.cc client/buddyclient.cc)
#target_link_libraries(bbos_test_client mercury pdlfs-common pthread)
