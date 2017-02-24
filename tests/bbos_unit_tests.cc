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

int main(int argc, char **argv) {
  BuddyServer *bs = new BuddyServer();
  delete bs;
  return 0;
}
