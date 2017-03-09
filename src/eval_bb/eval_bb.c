#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>

int main() {
  const char *fname = getenv("BB_Dummy_file");
  const char *v = getenv("BB_Lustre_chunk_size");
  unsigned long pchunk = strtoul(v, NULL, 0);
  char command[512];
  unsigned long container_size;
  uint64_t count;
  int ret;

  v = getenv("BB_Max_container_size");
  container_size = strtoul(v, NULL, 0);
  count = (container_size / pchunk);
  snprintf(command, 512, "dd if=/dev/zero of=%s bs=%lu count=%lu", fname, pchunk, count);
  ret = system(command);
  assert(ret == 0);
  return(ret);
}
