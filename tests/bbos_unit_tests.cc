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

int main() {
  BuddyServer *bs = new BuddyServer("/users/saukad/devel/deltafs-bb/config/narwhal_server.conf");
  delete bs;
  return 0;
}
