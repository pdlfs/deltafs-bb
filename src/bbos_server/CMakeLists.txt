#
# CMakeLists.txt  cmake file for bbos_server program
# 08-Mar-2017  chuck@ece.cmu.edu
#

add_executable (bbos_server bbos_server.cc)
target_link_libraries (bbos_server bbos)

install(TARGETS bbos_server RUNTIME DESTINATION bin)
