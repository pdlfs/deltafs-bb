#
# CMakeLists.txt  cmake file for deltafs-bb/src/libbbos library
# 07-Mar-2017  chuck@ece.cmu.edu
#

# list of source files
set (bbos-srcs bbos.cc bbos_rpc_client.cc bbos_rpc_svc.cc)

#
# configure/load in standard modules we plan to use and probe the enviroment
#
include (CMakePackageConfigHelpers)

set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
# XXX: cmake 3.1 and newer define a Threads::Threads imported target
# that we should switch too when we are ready to require 3.1 or better.
# (3.1 was released late 2014)
find_package (Threads REQUIRED)

# where to install files for "find_package"
set (bbos-pkg-loc "share/cmake/bbos")

#
# library version set here (e.g. for shared libs).
#
set (BBOS_VERSION_MAJOR 1)
set (BBOS_VERSION_MINOR 0)
set (BBOS_VERSION_PATCH 0)
set (bbos-vers "${BBOS_VERSION_MAJOR}.${BBOS_VERSION_MINOR}")
set (BBOS_VERSION "${bbos-vers}.${BBOS_VERSION_PATCH}")

#
# additional compiler flags to be applied for this directory
# (must come before any add_library/add_executable calls)
#
set (bbos-try-common-flags -Wpedantic -Wno-long-long -Wall
                           -Wno-sign-compare -pthread)
foreach (lcv ${bbos-try-common-flags})
    CHECK_CXX_COMPILER_FLAG (${lcv} flag${lcv})
    if (${flag${lcv}})
        add_compile_options (${lcv})     # directory property
    endif ()
endforeach ()

#
# create library target (user can specify shared vs. static using
# BUILD_SHARED_LIBS).  arrange for users of our lib to get the correct
# -Is for our include directories.
#
add_library (bbos ${bbos-srcs})
target_link_libraries (bbos mercury)
# XXX: mercury link target should do this for us
target_include_directories (bbos PUBLIC ${MERCURY_INCLUDE_DIR}
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
    $<INSTALL_INTERFACE:include>)
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (bbos "${CMAKE_THREAD_LIBS_INIT}")
endif ()

set_target_properties(bbos
                      PROPERTIES VERSION ${BBOS_VERSION}
                      SOVERSION ${BBOS_VERSION_MAJOR})

#
# installation stuff (packaging and install commands)
#
write_basic_package_version_file(
    "bbos-config-version.cmake"
    VERSION ${BBOS_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
configure_file (bbos-config.cmake.in bbos-config.cmake @ONLY)

#
# "make install" rules
#
install (TARGETS bbos EXPORT bbos-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (EXPORT bbos-targets
         DESTINATION ${bbos-pkg-loc}
         FILE "bbos-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/bbos-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/bbos-config-version.cmake"
         DESTINATION ${bbos-pkg-loc} )
install (DIRECTORY ../../include/bbos
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
