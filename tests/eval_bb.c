#include <stdio.h>

int main() {
  char *fname = getenv("BB_Dummy_file");
  uint64_t pchunk = strtoul(getenv("BB_Lustre_chunk_size"), NULL, 0);
  char command[512];
  uint64_t container_size = strtoul(genenv("BB_Max_container_size"), NULL, 0);
  uint64_t count = (container_size / pchunk);
  snprintf(command, 512, "dd if=/dev/zero of=%s bs=%lu count=%lu", fname, pchunk, count);
  system(command);
  return 0;
}
