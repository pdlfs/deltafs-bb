#include <assert.h>
#include <iostream>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include "../src/client/buddyclient.cc"

using namespace pdlfs;
using namespace bb;

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

int main(int argc, char **argv) {
  char input[8] = "12345";
  char output[8] = "";
  int ret;
  if(argc < 2) {
    printf("Not enough arguments for testing client.\n");
    exit(1);
  }
  BuddyClient *bc = new BuddyClient();
  ret = bc->mkobj(argv[1]);
  write_data(bc, argv[1], input, 6);
  assert(ret == 0);
  read_data(bc, argv[1], output, 6);
  printf("data read = from object %s = %s\n", argv[1], output);
  size_t size = bc->get_size(argv[1]);
  printf("size of object %s = %lu\n", argv[1], size);
  delete bc;
  return 0;
}
