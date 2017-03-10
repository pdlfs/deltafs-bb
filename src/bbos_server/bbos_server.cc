#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <map>
#include <string>

#include "../libbbos/bbos.h"

int main(int argc, char **argv) {
  pdlfs::bb::BuddyStore *bs = new pdlfs::bb::BuddyStore();
  delete bs;
  return 0;
}
