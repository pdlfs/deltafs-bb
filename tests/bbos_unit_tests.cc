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
  BuddyServer *bs = new BuddyServer("MANIFEST.txt",
                                    "/tmp/bb/",
                                    "127.0.0.1",
                                    1234,
                                    8,
                                    2,
                                    4,
                                    36,
                                    RR_WITH_CURSOR,
                                    4,
                                    36);
  delete bs;
  return 0;
}
