#!/bin/sh

: ${MANTA_URL?"MANTA_URL must be set"}

for file in `mls /manta_gc/all/do | json -a name`; do
    mget /manta_gc/all/do/$file | bash && mrm /manta_gc/all/do/$file;
done

exit 0;
