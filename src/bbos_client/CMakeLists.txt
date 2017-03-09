#
# CMakeLists.txt  cmake file for bbos_client test program
# 08-Mar-2017  chuck@ece.cmu.edu
#

add_executable (bbos_client bbos_client.cc)
target_link_libraries (bbos_client bbos)

install(TARGETS bbos_client RUNTIME DESTINATION bin)
