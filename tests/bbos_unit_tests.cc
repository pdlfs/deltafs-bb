#include <mercury_hl.h>
#include <mercury_hl_macros.h>
#include <assert.h>
#include <iostream>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include "../src/server/buddyserver.cc"

using namespace pdlfs;
using namespace bb;

/**
 * argv[1] - server config file
 */
int main(int argc, char **argv) {
  BuddyServer *bs = new BuddyServer(argv[1]);
  delete bs;
  return 0;
}
