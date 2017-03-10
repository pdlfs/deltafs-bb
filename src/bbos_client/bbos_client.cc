#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>

#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>


#include "bbos/bbos_api.h"
#define PATH_LEN 256   /* XXX */

static const char alphanum[] =
"0123456789"
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz";

int stringLength = sizeof(alphanum) - 1;

char genRandom() {
  return alphanum[rand() % stringLength];
}

void write_data(bbos_handle_t bc, const char *name, char *input,
                int num_chars) {
  size_t total_data_written = 0;
  size_t data_written = 0;
  do {
    data_written = bbos_append(bc, name, (void *)(input + total_data_written),
                               num_chars - total_data_written);
    total_data_written += data_written;
  } while(total_data_written < num_chars);
}

void read_data(bbos_handle_t bc, const char *name, char *output,
               int num_chars) {
  size_t total_data_read = 0;
  size_t data_read = 0;
  do {
    data_read = bbos_read(bc, name, (void *)(output + total_data_read),
                          total_data_read, num_chars - total_data_read);
    total_data_read += data_read;
  } while(total_data_read < num_chars);
}

int main(int argc, char **argv) {
  size_t file_size = 0;
  char obj_name[PATH_LEN] = "";
  size_t chunk_size = 0;
  int fd;
  struct ifreq ifr;

  char iface[] = "eth0";

  fd = socket(AF_INET, SOCK_DGRAM, 0);

  //Type of address to retrieve - IPv4 IP address
  ifr.ifr_addr.sa_family = AF_INET;

  //Copy the interface name in the ifreq structure
  strncpy(ifr.ifr_name , iface , IFNAMSIZ-1);

  ioctl(fd, SIOCGIFADDR, &ifr);

  close(fd);
  const char *v = getenv("BB_Mercury_transfer_size");
  if(v == NULL) {
    printf("BB_Mercury_transfer_size not set!");
    exit(1);
  }
  chunk_size = strtoul(v, NULL, 0);

  v = getenv("BB_Core_num");
  if(v == NULL) {
    printf("BB_Core_num not set\n");
    exit(1);
  }
  int core_num = atoi(v);

  snprintf(obj_name, PATH_LEN, "%s-%d", inet_ntoa(( (struct sockaddr_in *)&ifr.ifr_addr )->sin_addr), core_num);

  v = getenv("BB_Object_size");
  if(v == NULL) {
    printf("BB_Object_size not set!");
    exit(1);
  }
  file_size = strtoul(v, NULL, 0);

  char *input = (char *) malloc (sizeof(char) * chunk_size);
  //char *output = (char *) malloc (sizeof(char) * chunk_size * (file_size / chunk_size));
  srand(time(0));
  for(int i=0; i<chunk_size; i++) {
    input[i] = genRandom();
  }
  int ret;

  const char *srvr, *local;
  char server_url[512];
  srvr = getenv("BB_Server");
  if (!srvr) {
    const char *port, *addr;
    port = getenv("BB_Server_port");
    if (port == NULL) port = "19900";
    addr = getenv("BB_Server_IP_address");
    if (addr == NULL) {
      printf("neither BB_Server nor BB_Server_IP_address set!\n");
      exit(1);
    }
    snprintf(server_url, sizeof(server_url), "tcp://%s:%s", addr, port);
    srvr = server_url;
  }
  local = getenv("BB_Local");
  if (!local) local = "tcp";

  bbos_handle_t bc;
  ret = bbos_init(local, srvr, &bc);
  if (ret != BB_SUCCESS) abort();
  ret = bbos_mkobj(bc, obj_name, WRITE_OPTIMIZED);
  if (ret != 0) abort();
  uint64_t num_chunks = (file_size / chunk_size);
  for(uint64_t n=0; n<num_chunks; n++) {
      write_data(bc, obj_name, input, chunk_size);
  }
  // FIXME: uncomment while performing read tests
  // num_chunks = (file_size / chunk_size);
  // read_data(bc, argv[1], output, chunk_size * num_chunks);
  // printf("data read = %s\n", output);
  bbos_finalize(bc);
  return 0;
}
