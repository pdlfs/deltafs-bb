#
# CMakeLists.txt  deltafs-bb/src cmake file
# 07-Mar-2017  chuck@ece.cmu.edu
#

include (CheckCXXCompilerFlag)

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

add_subdirectory (libbbos)
add_subdirectory (bbos_client)
add_subdirectory (bbos_server)
add_subdirectory (eval_bb)
