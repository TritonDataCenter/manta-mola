#!/bin/sh

PATH=/opt/smartdc/mola/node_modules/manta/bin:$PATH
[ -z $MANTA_KEY_ID ] && MANTA_KEY_ID=$(ssh-keygen -l -f ~/.ssh/id_rsa.pub | awk '{print $2}')
[ -z $MANTA_URL ] && MANTA_URL=$(mdata-get manta_url)
[ -z $MANTA_USER ] && MANTA_USER=poseidon

for file in `mls /$MANTA_USER/stor/manta_gc/all/do | json -a name`; do
    mget /$MANTA_USER/stor/manta_gc/all/do/$file | bash && \
	mrm /$MANTA_USER/stor/manta_gc/all/do/$file;
done

exit 0;
