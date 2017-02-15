#include <assert.h>
#include <iostream>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include "../src/client/buddyclient.cc"

using namespace pdlfs;
using namespace bb;

static const char alphanum[] =
"0123456789"
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz";

int stringLength = sizeof(alphanum) - 1;

char genRandom() {
  return alphanum[rand() % stringLength];
}

void write_data(BuddyClient *bc, const char *name, char *input, int num_chars) {
  size_t total_data_written = 0;
  size_t data_written = 0;
  do {
    data_written = bc->append(name, (void *)(input + total_data_written), num_chars - total_data_written);
    total_data_written += data_written;
  } while(total_data_written < num_chars);
}

void read_data(BuddyClient *bc, const char *name, char *output, int num_chars) {
  size_t total_data_read = 0;
  size_t data_read = 0;
  do {
    data_read = bc->read(name, (void *)(output + total_data_read), total_data_read, num_chars - total_data_read);
    total_data_read += data_read;
  } while(total_data_read < num_chars);
}

/**
 * argv[1] = object name
 * argv[2] = config file path
 * argv[3] = object size
 * argv[4] = chunk size
 */
int main(int argc, char **argv) {
  size_t file_size = strtoul(argv[3], NULL, 0);
  size_t chunk_size = strtoul(argv[4], NULL, 0);
  char *input = (char *) malloc (sizeof(char) * chunk_size);
  char *output = (char *) malloc (sizeof(char) * chunk_size * (file_size / chunk_size));
  srand(time(0));
  for(int i=0; i<chunk_size; i++) {
    input[i] = genRandom();
  }
  int ret;
  size_t size = 0;
  if(argc < 5) {
    printf("Not enough arguments for testing client.\n");
    exit(1);
  }
  BuddyClient *bc = new BuddyClient(argv[2]);
  ret = bc->mkobj(argv[1]);
  assert(ret == 0);
  uint64_t num_chunks = (file_size / chunk_size);
  for(uint64_t n=0; n<num_chunks; n++) {
      write_data(bc, argv[1], input, chunk_size);
  }
  // FIXME: uncomment while performing read tests
  // num_chunks = (file_size / chunk_size);
  // read_data(bc, argv[1], output, chunk_size * num_chunks);
  // printf("data read = %s\n", output);
  delete bc;
  return 0;
}
