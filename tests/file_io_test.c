#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <datawarp.h>

/* build with:
 *   gcc basic_dw_file_test.c -o basic_dw_file_test `pkg-config --cflags \
 *     --libs cray-datawarp`
 */

#define PATH_LEN 256

void usage() {
  printf("file_io_test <path> <i/o size in bytes>\n");
}

int main(int argc, char **argv) {
  if(argc != 3) {
    usage();
    exit(1);
  }

  // load the DataWarp module.
  system("module load datawarp");

  // setup DW with a private (i.e. 1:1) config
  system("#DW jobdw type=scratch capacity=1GB access_mode=private");

  char path[PATH_LEN] = "";
  memcpy(path, argv[1], PATH_LEN);
  printf("File path = %s\n", path);

  uint64_t size = atoll(argv[2]);
  printf("I/O size = %llu\n", size);

  int fd = open(path, O_CREAT | O_WRONLY);
  if(fd < 0) {
    printf("Error opening file for write. Exiting test.\n");
    exit(1);
  }

  char *buf = (char *) malloc (sizeof(char) * size);
  for(int i=0; i<size; i++) {
    
  }
}
