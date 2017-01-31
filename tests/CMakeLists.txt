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
# mercury is required
find_package (MERCURY REQUIRED)
#include_directories( /usr/local/include/)
#link_directories( /usr/local/lib/)
include_directories(/users/saukad/devel/deltafs-bb/server/)
#
# test program
#
add_executable(bbos_test bbos_unit_tests.cc ../src/rpc/rpc.c ../src/rpc/rpc_engine.c ../src/server/buddyserver.cc ../src/client/buddyclient.cc)
target_link_libraries(bbos_test mercury)
#add_executable (deltafs-bb-test deltafs-bb-test.c)
#add_test (bb-test ${CMAKE_CURRENT_SOURCE_DIR}/bb-test.sh)
