#
# CMakeLists.txt  cmake file for deltafs-bb directory
# 14-Jan-2017  saukad@cs.cmu.edu
#

# configure/load in standard modules we plan to use
set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)

#
# create the library target 
#
add_library (deltafs-bb client/buddyclient.cc server/buddyserver.cc)
target_link_libraries (deltafs-bb mercury mercury_hl)
target_include_directories (deltafs-bb PUBLIC ${MERCURY_INCLUDE_DIR})
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (deltafs-bb "${CMAKE_THREAD_LIBS_INIT}")
endif ()

#
# test program
#
add_executable(bbos_server ../tests/bbos_unit_tests.cc)
target_link_libraries(bbos_server deltafs-bb)

add_executable(bbos_client ../tests/bbos_unit_tests_client.cc)
target_link_libraries(bbos_client deltafs-bb)

add_executable(eval_bb ../tests/eval_bb.c)
target_link_libraries(eval_bb deltafs-bb)

#
# "make install" rules
#
install (TARGETS bbos_server bbos_client
         RUNTIME DESTINATION bin)

