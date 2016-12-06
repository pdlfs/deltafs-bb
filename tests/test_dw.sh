#!/bin/bash
module load dws # load the DW module
dd if=/dev/urandom of=/tmp/random.txt bs=1k count=1k # create a random file
sync
CHECKSUM=$(sha1sum /tmp/random.txt) # calculate checksum to verify integrity later
#DW jobdw type=scratch capacity=1GB access_mode=private
#DW stage_in type=file source=/tmp/random.txt destination=$DW_JOB_STRIPED/random.txt
sync
#DW stage_out type=file destination=/tmp/random_clone.txt source=$DW_JOB_STRIPED/random.txt
sync
CLONED_CHECKSUM=$(sha1sum /tmp/random_clone.txt) # calculate checksum of cloned file for verification
if [ "$CHECKSUM" = "$CLONED_CHECKSUM" ];
then
  echo "DataWarp basic integrity check passed!"
else
  echo "DataWarp basic integrity check failed!"
fi
