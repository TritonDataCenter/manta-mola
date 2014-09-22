---
title: Mola Mako Object Rebalancing
markdown2extras: tables, code-friendly
apisections:
---
<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# Overview

Unfortunately there have been some screw-ups in manta where it has been
necessary to move objects from one mako to another.  This was originally written
when we ran into a situation where all objects were being put on two different
mako nodes, but within the same datacenter.

# Implementation Details

## Input

1. Manta tables from moray shard dumps.  Currently located at

    /poseidon/stor/manatee_backups/[shard]/[date]/manta-[date].gz

2. Current set of mako nodes.  The kick_off_rebalance app will automatically
   pull this list from the `manta_storage` table in moray.  You can optionally
   give a set of manta_storage_ids that will be ignored so that any rebalanced
   objects won't land on those hosts.

## Phase 1: Marlin job

The marlin jobs sifts through the live objects and finds objects that need to
be rebalanced.  For example, it will find objects where all copies are located
in the same datacenter.  It will then randomly choose one shark to place a new
copy of the object and emit a record with the object information, the new shark
and the old shark.  These messages will be recorded in a file specific to the
new shark and uploaded to Manta under:

    /poseidon/stor/manta_rebalance/do/[manta_storage_id]/[file]

## Phase 2: Rebalancing

On each mako node, an operator runs an application by hand that will process all
files located in the mako-specific manta directory.  For each object it will:

1. Fetch all moray records for the object and verify it hasn't been updated
   since the job ran.
2. Pull down the object from the old mako node.
3. Verify the checksum of the new file matches what's in Moray.
4. Move the new file into the correct location on the new mako node.
5. Update all moray records, replacing the old shark with the new shark.
6. Move the remote file to the daily tombstone directory.

# Performing a rebalance

## Running the Marlin Job

It is expected that common need for rebalancing is when all objects need to be
moved from a mako node (dying host).  This example walks you through doing that.

First, find the manta_storage_id for the host that is problematic and verify
that:

1. The mako node is receiving no more writes.
2. nginx is still up and running as well as registrar in the storage zone.
3. The latest moray dumps have been taken *after* the last write.

Like so:

    [root@409edbbe (storage) ~]$ svcadm disable minnow
    [root@409edbbe (storage) ~]$ json -f /opt/smartdc/mako/etc/mako_rebalancer_config.json manta_storage_id
    3.stor.coal.joyent.us
    [root@27b5d86a (moray) ~]$ (set -o pipefail; \
        export KEY=$(findobjects manta_storage '(manta_storage_id=3.stor.coal.joyent.us)' | json key); \
        getobject manta_storage $KEY | json value | \
            json -e 'this.timestamp = 1356998400000' > new.json && \
        putobject -d "$(cat new.json)" manta_storage $KEY && echo okay)
    [root@8d626e7f (webapi) ~]$ svcadm restart *muskie*
    [root@1bf3106e (ops) ~]$ mls -l /poseidon/stor/manatee_backups/1.moray.coal.joyent.us/2014/01/14/15/manta-2014-01-14-15-05-11.gz
    -rwxr-xr-x 1 poseidon        276543 Jan 14 15:40 manta-2014-01-14-15-05-11.gz
    [root@1bf3106e (ops) ~]$ date
    Tue Jan 14 15:40:59 UTC 2014

Now verify that no other operator is currently running a rebalance, then log
into the ops zone and clear out any old rebalance data:

    $ mrm -r /poseidon/stor/manta_rebalance

This isn't strictly necessary, but guarantees that any stale data won't be read.

Next, kick of the rebalance job, specifying the dying host:

    $ /opt/smartdc/mola/bin/kick_off_rebalance.js \
      -h 3.stor.coal.joyent.us | bunyan

You can specify other hosts to ignore (so they will will receive no objects as
part of the rebaolance) by specifying the -i option.  For example, this will
balance off 3.stor and no rebalanced objects will land on 2.stor and 4.stor:

    $ /opt/smartdc/mola/bin/kick_off_rebalance.js \
      -h 3.stor.coal.joyent.us \
      -i 2.stor.coal.joyent.us -i 4.stor.coal.joyent.us | bunyan

After starting the job you should verify that the set of mako nodes you expect
is exactly this list:

    $ mget /poseidon/stor/manta_rebalance/assets/sharks.json | json

If the full list isn't being populated or if there are extra nodes, you should
debug by looking at the manta_storage table.  *Warning*: The picking algorithm
is really dumb- it chooses a random available node, regardless if it has room or
not.  We'll have to modify this in the future if/when we need to.

Once the job has finished (the job id should be in the output), check for
errors.  If there are no errors, verify that it produced files:

    $ mfind -t o /poseidon/stor/manta_rebalance/do/

For each of the storage nodes listed under `do`, you'll run the rebalance
app, then resolve any errors.  Depending on the errors, it may be easiest to
run through the whole process again.

## Running the app on makos

The rebalance command attempts to be idempotent.  If the moray record for an
object has been touched (by etag comparison), the record will be skipped.  This
could either be because the object has already been rebalanced or because the
object has changed.  Otherwise, if there are any errors with the other
operations (pulling down object, updating moray, moving old object), an error
will be emitted and the process will continue.  This means that at the end of a
rebalance, you (the operator), need to make sure everything is in a good state,
ie. there isn't cruft left over in the system.

First find out how many rows the mako node will process, run this on the ops
zone:

    $ mfind /poseidon/stor/manta_rebalance/do/[manta_storage_id] | \
        xargs -n 1 mget | wc -l

Take a note of the count, then to run the rebalance app, log onto the mako zone
and:

    $ /opt/smartdc/mako/bin/mako_rebalance.js 2>&1 >/var/tmp/mako_rebalance.log &

Since the current version serializes all operations, this can take hours
depending on the number of objects.  Note that the concurrency is trivially
changed, but we'd rather run nicely in the background and not unintentionally
affect manta unless the risk is justified.  You can monitor progress with
something like this:

    $ grep 'starting pipeline for object' /var/tmp/mako_rebalance.log | wc -l

Once the application has finished, verify that no more files are located in the
storage node's manta location:

    $ mfind /poseidon/stor/manta_rebalance/do/[manta_storage_id] | wc -l

Then check for errors in the logs:

    $ bunyan -l error /var/tmp/mako_rebalance.log

Clean up any crufty data.  Once all mako nodes have processed their files you
should run a job that verifies that there are no more references to the dying
mako node in the live tables.  Here is an example job:

    $ TODO

# Known errors

## ECONNREFUSED on remote mako move

You'll see this in the logs:

    [2013-08-27T23:41:33.554Z] ERROR: moray_gc/33784 on 00c3e6bd-b8bc-4300-919b-bf36d4cd8920: error with object
        err: {
           "code": "ECONNREFUSED",
           "errno": "ECONNREFUSED",
           "syscall": "connect"
        }
        --
        object: {
           "key": "/ddb63097-4093-4a74-b8e8-56b23eb253e0/reports/usage/storage/2013/08/17/21/h21.json"
    ...

This is an indication that the file was successfully moved onto the local node
and that moray was updated, but that the moving to the tombstone directory
failed.  Unfortunately, you have to do the move by hand.  Fortunately, it's
easy.  First check that the moray record for the object has, indeed, been
updated:

    ops$ getobject -h electric-moray.us-east.joyent.us manta [object_key]

Then check that the oldShark is gone and the newShark is there.  If so, check
that the file is on the shark and that the file sizes match (just to be
paranoid):

    mako$ ll /manta/[ownerId]/[objectId]

If so, check that the object is still on the remote shark:

    mako$ curl -v http://[old shark manta_compute_id]/[ownerId]/[objectId] -X HEAD

And, finally, move the file to the remote tombstone:

    mako$ curl -v http://[old shark manta_compute_id]/[ownerId]/[objectId] \
          -X MOVE -H 'Destination: /tombstone/YYYY-MM-DD/[objectId]'

## MD5 Mismatch

You'll see this in the logs:

    [2013-08-28T20:09:15.325Z] ERROR: moray_gc/35338 on 8386d8f5-d4ff-4b51-985a-061832b41179: error with object
        err: {
           "code": "Md5Mismatch",
           "message": "Calculated md5: P3pn+fxW0qq+SMmDF2qZog== didn't match UQ9siwCyyDizKQjdtrrdTg=="
        }
    ...

What this normally means is that node somehow believed that the stream ended,
but the file wasn't fully transferred.  To verify, get the moray record:

    ops$ getobject -h electric-moray.us-east.joyent.us manta [object_key]

Then list the file in the local temp directory:

    mako$ ll /manta/rebalance_tmp/[objectId]

If the sizes don't match, then checking the MD5 saved us.  The record can be
safely reprocessed or you can just run rebalance again.  If they do match,
something is seriously wrong and you'll need to start digging.

## InternalError on moray write

You'll see this in the logs:

    [2013-08-28T00:45:09.189Z] ERROR: moray_gc/33784 on 00c3e6bd-b8bc-4300-919b-bf36d4cd8920: error with object
        err: {
            "name": "InternalError",
            "context": {},
            "ase_errors": []
        }
    ...

Unfortunately, this is a moray issue.  You can try to rerun, but if it is
reproducible then it's bug fixing time.