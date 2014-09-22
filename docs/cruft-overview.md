---
title: Mola Cruft Overview
markdown2extras: tables, code-friendly, fenced-code-blocks
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

In a perfect world, mako objects would never be abandoned.  They would be
tracked in Moray until deleted by the user, then get garbage collected after a
grace period.  Unfortunately there are failure conditions where a mako object
would be inadvertently abandoned.  Specifically, here are two:

1. All objects streamed to sharks, moray write fails.
2. Object is streamed successfully to N - 1 of the N sharks, final shark node
   fails to complete, causes request to fail.

The above is *not* a complete list.  Objects that are on mako nodes but, for
some reason or another, aren't and have never been in moray are lovingly
referred to as "cruft".  While possible that cruft build-up may be a significant
cost in Manta, accidentally deleting objects that aren't actually dead is not
worth the cost of trying to automate cruft cleanup since we're dealing with a
long tail of unknown issues.

Cruft cleanup should be able to be run on demand by Manta operators.

# Background

Consider this table, which is a representation of where references to an object
(or the object itself) exist over time:

```
+-----------------------------+------+-------------+------------------+----------------+
| Time                        | Mako | Moray.manta | Moray.delete_log | Mako.tombstone |
+-----------------------------+------+-------------+------------------+----------------+
| 1. Mako                     | x    |             |                  |                |
| 2. Moray: manta             | x    | x           |                  |                |
| 3. Moray: link              | x    | x           |                  |                |
| 4. Moray: Link Deleted      | x    | x           | x                |                |
| 5. Moray: Last link deleted | x    |             | x                |                |
| 6. GC: Produces delete list | x    |             | x                |                |
| 6a. Moray cleans up first   | x    |             |                  |                |
| 6b. Mako cleans up first    |      |             | x                | x              |
| 7. Grace period             |      |             |                  | x              |
| 8. Purge                    |      |             |                  |                |
+-----------------------------+------+-------------+------------------+----------------+
```

The object at time 1 is "potential cruft" since it cannot be distinguished from
an object in state 6a.  The object from 2-4 is a "live" object.  Data that we
keep around from 5-7 is garbage that will eventually be collected.  So, to find
the potential cruft we need to find all objects that:

1. Exist in the "live" portion of mako (not under /manta/tombstone)
2. Doesn't exist in Moray, either in the live table or the delete log.

Objects at time 1 could be erroneously put in the "cruft" bucket if we aren't
careful.  Items at time 6a are to be GCed, so it wouldn't matter if a "cruft"
job collects them rather than the normal GC processes.

# Design alternatives

The cruft job is looking for objects that exist on the makos but not in the
morays.  The logical way of selecting which moray and mako dumps to use is to
first take the entire set of mako dumps, then take the set of moray dumps that
postdate the latest mako dump.  Since the moray record for an object is written
after the object is on disk, taking the mako dumps first guarantees that objects
exist somewhere in the morays or it is cruft.

Alternatively, the mako dumps have the last modified timestamp of the file on
disk.  Since an object is PUT and only touched for reading and GC, the timestamp
is the "first create" timestamp.  If we were to take the moray dumps first, we
could write the cruft job to ignore any mako object that is "newer" than the
earliest mako dump.

The problem with the latter approach is that until we have the postgres dumps
writing a manifest for when they started, the current timestamp is when the sql
to json marlin job uploads the transformed tables, not when the dump was
originally started Since that's the case, we would need to pad the earliest
timestamp to account for the time it took for the dump and transform.  To be
safe, we could pad by 48 hours.  This would collect less cruft, but since this
is meant as an operator-run process, the operator can run again if the objects
in that 48 hour window are "big".

The reason we have to go with the latter approach is that the moray and mako
dumps are run on a daily cadence.  We'll need to schedule one set of dumps to
happen after the other (either moray or mako need to go first).  We do want the
dumps to be as close in time as possible, so it's not tenable to have moray/mako
dumps for audit and mako/moray dumps for cruft.  Since Audit is the more
important job, we want to keep that as is, so we'll need to use the same dumps
for the cruft job.

# Implementation Details

## Input

1. Moray shard dumps of the manta table and the manta_delete_log table.
   Currently located at:

    /poseidon/stor/manatee_backups/[shard]/[date]/manta-[date].gz
    /poseidon/stor/manatee_backups/[shard]/[date]/manta-delete-log-[date].gz

2. Mako dumps with tombstone objects removed.  Currently located at:

    /poseidon/stor/mako/[manta storage id]

To mitigate collecting the "Time 1" objects, we'll filter out any objects from
the mako dumps which are "new".  "New" is going to be defined as any object that
was created within 48 hours of the oldest moray dump.  Giving this much time for
a grace period is probably overkill, but, as explained above, better safe than
sorry.

## Marlin job

The cruft job is kicked off from the "ops" zone deployed as part of Manta.  The
cron invokes `/opt/smartdc/mola/bin/kick_off_cruft.js`, which does a few things:

1. Verifies that a cruft job isn't currently running
2. Finds the latest Moray dumps, does some verification
3. Finds the Moray dumps right before the earliest mako dump, does some
   verification (see Design Alternatives above)
4. Sets up assets and directories required by cruft
5. Kicks off a marlin job

All output for the Marlin job is located under:

    /poseidon/stor/mola_cruft

From a high-level, the Marlin job does the following:

1. Transforms the Mako dumps into rows that represent which objects actually
   exist on the mako node, filtering out the tombstone entries and entries that
   are considered too "new" to look at (see above).
2. Transforms the Moray dumps for tables `manta` and `manta_delete_log` into
   records for each row.  Each manta record is rolled out into several rows that
   represent the makos where Moray expects objects to be.
3. The records for each object are then sent off to a number of reducers where a
   reducer is guaranteed to have all records for a given object.
4. The records for each object are ordered such that the moray record (that the
   object is in the index tier), followed by all mako locations.  For all places
   where an object is in moray, but doesn't exist on the mako, the mako record
   is written to stdout.
5. The records are split into files for each storage node.

We should also have a tool or marlin job that takes the output from the cruft
job and outputs the number of objects and aggregate bytes of cruft.

## Cleaning the cruft

The tricky part is verifying that the object no longer exists in moray before
garbage collecting it.  We also want time to make sure audit passes before the
entry is deleted.  To that end, we'll hook into "normal" mako GC.  The job is
the only thing that should be different.  The cruft cleanup will be done from
the ops zone:

    /opt/smartdc/mola/bin/cruft_gc.js [cruft job id]

Which:

1. Finds all output objects for the given cruft job.
2. Pulls the output objects down one at a time.
3. In large moray batches, verifies that the objects no longer exist on each of
   the mako shards (checks in both the manta and manta delete log).  If it finds
   an object, we know something is really wrong with the cruft job, and error
   exit.
4. Creates a set of links from the cruft job output into the manta_gc mako
   directories.
5. Makos will GC objects in those files "normally".

# Performing a cruft collection

Note that parts of this process are "expensive", specifically, the verification
process will verify that an objectid exists on no shard (the manta and
manta_delete_log tables on all index moray shards) by making batch-gets in
parallel to each shard.  If your index shards are already close to capacity,
this could pose a problem for your system.  Be sure to watch the performance
of your Manta while you run the verification and linking step.

## Running the Marlin Job

First, make sure that all previous cruft files have been cleared out.  The
command to kick off cruft collection will fail if it detects there are files
from a previous job that weren't processed or aren't done being processed.  If
you do find that there are files that were not processed, it is safe to delete
them since the current cruft run will find any objects that weren't processed on
a previous run.  The guard is there to try and avoid duplicating work.

To see if there are unprocessed files, log into the Manta ops zone and run:

    $ mfind /poseidon/stor/manta_cruft/do | wc -l

If the output is "0", then you should be ready to go.  Log into the ops zone and
run the command to find crufty objects:

    $ /opt/smartdc/mola/bin/kick_off_cruft.js | bunyan

Take note of the jobId.  Wait for the job to complete.

## Verifying and Linking cruft files

Once the job has successfully completed, verify the job completed with no errors:

    $ mjob get $JOBID | json state stats

If it has completed with no errors, then kick off the verification phase.  The
verification phase has the potential of running for a "long time", so you may
want to leave it running in a screen session or background/disown it.  From
the ops box:

    $ /opt/smartdc/mola/bin/cruft_verify_and_link.js >>cruft_verify.log

You can "watch" the progress of this by tailing the logs or looking at how many
files are in the "do" directory:

    $ mls /poseidon/stor/manta_cruft/do | wc -l

The cruft files are linked into "regular" mako GC, so you should see the cruft
objects moved into the daily tombstone.  To purge them completely you can either
wait for the daily directory to roll off or manually "rm -rf" the tombstone
directory (though you should be very, very careful when doing the latter).
