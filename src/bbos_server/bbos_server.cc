#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>

#include <map>
#include <string>

#include "../libbbos/bbos.h"

using namespace pdlfs; /* XXX */
using namespace bb; /* XXX */

int main(int argc, char **argv) {
  BuddyStore *bs = new BuddyStore();
  delete bs;
  return 0;
}
