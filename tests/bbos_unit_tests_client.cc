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

int main() {
  char input[8] = "12345";
  char output[8] = "";
  int ret;
  BuddyClient *bc = new BuddyClient(1234);
  ret = bc->mkobj("first");
  write_data(bc, "first", input, 6);
  assert(ret == 0);
  read_data(bc, "first", output, 6);
  printf("data read = %s\n", output);
  delete bc;
  return 0;
}
