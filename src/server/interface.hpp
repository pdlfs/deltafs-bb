#include <iostream.h>

class DeltaFSBBClientInterface
{
  public:
    virtual ~DeltaFSBBServerInterface() {}

    /* Initialize BB server with the compute node fan in. */
    virtual int bb_init(int num_streams);

    /* Handler for the RPC on the BB server */
    virtual int bb_listen(char *path, int flag);

    /* Write to a particular stream ID. */
    virtual int bb_write(int s_id, const void *buf, size_t len);

    /* Commit a particular steam ID to persistent storage. */
    virtual int bb_sync(int s_id);

    /* Close a particluar stream ID and remove all its state. */
    virtual int bb_close(int s_id);
};
