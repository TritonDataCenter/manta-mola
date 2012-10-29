#!/bin/bash

export PATH=/opt/smartdc/mola/build/node/bin:/opt/smartdc/mola/node_modules/manta/bin:$PATH
[ -z $MANTA_KEY_ID ] && export MANTA_KEY_ID=$(ssh-keygen -l -f ~/.ssh/id_rsa.pub | awk '{print $2}')
[ -z $MANTA_URL ] && export MANTA_URL=$(mdata-get manta_url)
[ -z $MANTA_USER ] && export MANTA_USER=poseidon

MDIR=/$MANTA_USER/stor/manta_gc/all/do
NOW=`date`

function fatal {
        echo "$NOW: $(basename $0): fatal error: $*" >&2
    exit 1
}

function log {
    echo "$NOW: $(basename $0): info: $*" >&2
    }

COUNT=0

log "listing $MDIR"
for json in `mls $MDIR`
do
    FILE=`echo $json | json -a name`
    MFILE=$MDIR/$FILE
    mget $MFILE | bash
    [[ $? -eq 0 ]] || fatal "Couldnt execute $MFILE"

    mrm $MFILE
    [[ $? -eq 0 ]] || fatal "Couldnt rm $MFILE"

    log "processed $MFILE successfully"
    ((COUNT++))
done

[[ $? -eq 0 ]] || fatal "Couldnt list $MDIR"

log "done, processed $COUNT files"
exit 0;
