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

void write_data(BuddyServer *bs, const char *name, char *input, int num_chars) {
  size_t total_data_written = 0;
  size_t data_written = 0;
  do {
    data_written = bs->append(name, (void *)(input + total_data_written), num_chars - total_data_written);
    total_data_written += data_written;
  } while(total_data_written < num_chars);
}

void read_data(BuddyServer *bs, const char *name, char *output, int num_chars) {
  size_t total_data_read = 0;
  size_t data_read = 0;
  do {
    data_read = bs->read(name, (void *)(output + total_data_read), total_data_read, num_chars - total_data_read);
    total_data_read += data_read;
  } while(total_data_read < num_chars);
}

int main() {
  char input[9] = "12345678";
  char output[9] = "";
  BuddyServer *bs = new BuddyServer("/tmp/BBOS_MANIFEST.txt", 8, 2, 1);
  delete bs;
  //std::cout << "starting client" << std::endl;
  //BuddyClient *bc = new BuddyClient();
  /*assert(bs->mkobj("first") == 0);
  assert(bs->mkobj("second") == 0);
  write_data(bs, "first", input, 8);
  read_data(bs, "first", output, 8);
  std::cout << "Size of first object = " << bs->get_size("first") << std::endl;
  sprintf(input, "abcdef");
  write_data(bs, "second", input, 6);
  read_data(bs, "second", output, 6);
  binpacking_policy policy = GREEDY;
  bs->binpack("/tmp/container.bin", policy);
  bs->shutdown("/tmp/BBOS_MANIFEST.txt");*/
  // sleep(5);
  // delete bs;
  //delete bc;
  /*BuddyServer *bs2 = new BuddyServer(8, 2);
  bs2->bootstrap("/tmp/BBOS_MANIFEST.txt");
  std::cout << "Size of second object = " << bs2->get_size("second") << std::endl;
  //assertbs2->open("first");
  //read_data(bs2, id_3, output, 2);
  delete bs2;*/
  //pthread_exit(NULL);
  return 0;
}
