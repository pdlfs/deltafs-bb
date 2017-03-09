#
# CMakeLists.txt  cmake file for eval_bb program
# 08-Mar-2017  chuck@ece.cmu.edu
#

add_executable (eval_bb eval_bb.c)

install(TARGETS eval_bb RUNTIME DESTINATION bin)
