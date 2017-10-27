---
title: Mola Multipart Upload GC Overview
markdown2extras: tables, code-friendly
apisections:
---
<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2017, Joyent, Inc.
-->

# Overview

Using the multipart upload API, Manta users are able to upload objects in chunks
called "parts", then commit or abort the upload. Committing the MPU exposes a
new object in Manta, and aborting cancels it, which prevents the upload from
later being committed.

Parts are represented as objects in Manta, and they are stored in a directory
referred to as the upload directory of an MPU. We also store an additional
record in Moray on the same shard as the target object of the MPU. This record,
called the "finalizing record", exists allows clients to query the status of
an MPU shortly after it has been finalized.

For all finalized MPUs, we leave some garbage in the system that needs to be
cleaned up. In particular, we need to remove:
- part metadata records and their associated data on mako
- upload directory records in Moray
- finalizing records in the Moray `manta_uploads` bucket

It is worth noting that these records do not necessarily exist on the same
shard. As such, in order to safely determine an MPU can be garbage collected, we
need a more global view of the system than needed with normal GC.

Finalizing records are removed directly from Moray. Parts and upload directories
are removed using an operator-only query parameter through the front door of
Manta. This allows us to do all of the normal verification associated with
normal object and directory removal, without allowing for deletion of part data
and MPUs without operator-intervention. This strategy does induce some
additional latency for garbage collection of parts, as they will incur the grace
period and tombstone period from normal GC.

# MPU GC vs. Existing GC

The MPU GC process was designed based on the current garbage collection
implementation, deviating where it seems reasonable to.

The first half of the process, as with normal GC, is a Manta job that operates
on Moray shard database dumps. Many of the scripts for MPU GC are modeled
directly from existing GC scripts and will look quite similar to them.

The second half of the MPU differs much more from normal GC. In normal GC,
instructions for individual moray zones and mako zones are placed in Manta, and
executed later by zone. This approach is not tenable for MPU GC, as records for
a given MPU can exist on more than one shard, and thus we need a more global
view of the system in order to clean up records in a safe way. Instead, the MPU
GC job produces one logical list of instructions, corresponding to records
to delete, which are executed by a cleanup script that deletes records in a
safe order.

# MPU GC Implementation Details

## Input

1. Moray shard dumps. Currently located at:

    /poseidon/stor/manatee_backups/[shard]/[date]/[table]-[date].gz

The two tables required for MPU garbage collection are:

1. `manta`: Record of the set of 'live' objects, including part records and
upload directories.
2. `manta_uploads`: Record of finalized MPUs.

## Phase 1: Marlin job

The MPU garbage collection job is kicked off from the "ops" zone deployed as
part of Manta. The cron invokes `mola/bin/kick_off_mpu_gc.js`, which does a few
things:

1. Verifies that an MPU GC job is not currently running
2. Finds the latest Moray dumps and does some verification
3. Sets up assets and directories required by MPU GC
4. Kicks off a marlin job

All output for the Marlin job is located under:

    /poseidon/stor/manta_mpu_gc

From a high-level, the Marlin job does the following:

1. Transforms all MPU-related live records in the `manta` table and all rows in
the `manta_uploads` table from the Moray dumps into objects representing each
record. The representation of these records include the multipart upload ID,
the date the record was produced, and the type of MPU record it is: a part
record, an upload record, or a finalizing record. Finalizing records also
contain their shard and the Moray key for the record; part and upload records
contain the record's path in Manta.
2. The records for each MPU are then sent to a number of reducers, where the
reducer is guaranteed to have all records for a given MPU.
3. Reducers sort the set of rows so that records for the same MPU are grouped
together, then sorted in the order of: finalizing record, upload record, part
records.  The reducer can iterate over these rows and determine whether records
for a given MPU should be deleted: in particular, that it has a finalizing
record and that the finalizing record was created before a system-wide grace
period begun.

The output of the Marlin job is a set of list of records that can be safely
deleted by the cleanup script. These output is stored at:

    /poseidon/stor/manta_mpu_gc/cleanup/[date]-[job_id]-X-[uuid]

## Phase 2: Cleanup

This phase is responsible for the actual cleanup of the records that need to be
garbage collected. The cron invokes `mola/bin/kick_off_mpu_cleanup.js`, which
will look at the files in the cleanup directory that are output by the MPU GC
job.

For each file in the cleanup directory, the script will:

1. Get a stream using the node-manta client that represents the contents of the
file.
2. Collect related MPU records together, which should be in the same sorted
order as created in the MPU GC job, using `mola/lib/mpu/mpuBatchStream.js`.
3. Double check that all records are present as expected, using
`mola/lib/mpu/mpuVerifyStream.js`.
4. Delete part records, if they exist, from the Manta front door for each MPU
batch, using `mola/lib/mpu/mpuUnlinkLiveRecordStream.js`.
5. Delete the upload record, if it exists, from the Manta front door for each
MPU batch, using `mola/lib/mpu/mpuUnlinkLiveRecordStream.js`.
6. Delete the finalizing record for each MPU using
`mola/lib/mpu/mpuMorayCleanerStream.js`.
7. To maintain the cleanup instructions for debugging purposes, link the cleanup
file to:

    /poseidon/stor/manta_mpu_gc/completed/[date]-[job_id]-X-[uuid]

8. Delete the original cleanup file from Manta.

If at any point in steps 2-6, an error occurs for a given MPU, the MPU will be
dropped from the stream, and no further records will be garbage collected. This
is to ensure that records are always deleted in a safe order: the part records
first, as they are entries in the upload directory, followed by the upload
directory, followed by the finalizing record, as it is the only definitive
evidence that an MPU was finalized and thus can be safely garbage collected.

# Running a GC manually

For testing purposes, or if a job fails, it is often useful to be able to run
the MPU GC job manually.

## Kick of the Marlin job from the ops zone

The first stage is to kick off the MPU GC job.

```
ops$ /opt/smartdc/mola/bin/kick_off_mpu_gc.js | bunyan
```

This will use the defaults for the environment.  Note that
kicking off a GC job requires db dumps to be "recent".  Please refer to the
[System Crons](system-crons.md) for the timeline.

If you just want to check on the last job run:

```
ops$ mjob get $(mget -q /poseidon/stor/manta_mpu_gc/jobs.json | json -ak | tail -1)
```

This stage produces as many files as there are reducers for the job that are
stored under:

`/poseidon/stor/manta_mpu_gc/cleanup`

## Run the cleanup script

From the ops zone, kick off the cleanup pipeline:

```
ops$ /opt/smartdc/mola/bin/kick_off_mpu_gc_cleanup.js | bunyan
```

As this script runs, it will delete files from the cleanup directory and will
link its completed input to:

    /poseidon/stor/manta_mpu_gc/completed/[date]-[job_id]-X-[uuid]


# See Also

* [RFD 65](https://github.com/joyent/rfd/tree/master/rfd/0065): Multipart Uploads for Manta.
