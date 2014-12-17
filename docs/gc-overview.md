---
title: Mola GC Overview
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

Each time a Manta consumer uploads and subsequently deletes data in Manta
garbage is left over in the system in the form of files on mako nodes.  Get
enough garbage in the system and Manta will run out of room for new data.
Moray, the indexing tier for Manta, is the authoritative source for which manta
objects are "live".  In other words, accessible to be retrieved through the
Manta we api.  Since Moray is the authoritative store, it must be consulted to
in order to figure out if a particular object should be collected.

Since Moray is actually composed of many shards, each responsible for a slice of
live objects and that references to the files in Mako can exist on any of the
shards, all must be consulted.  Due to the nature of distributed systems, all
cannot be consulted at the same instant in time.  Due to this limitation,
garbage collection is implemented as a Manta job, post-processing the set of
Moray shard data dumps.

# Running a GC manually

All of these processes are kicked off from cron on a daily basis, so there is
usually no need to run GC manually.  If there is some reason you do, here is the
process.

## Kick of the Marlin job from the ops zone

The first stage is to kick off the marlin job.

```
ops$ kick_off_gc.js | bunyan
```

This will use the defaults for the stage (coal, production, etc).  Note that
kicking off a GC job requires db dumps to be "recent".  Please refer to the
[System Crons](system-crons.md) for the timeline.

If you just want to check on the last job run:

```
ops$ mjob get $(mget -q /poseidon/stor/manta_gc/jobs.json | json -ak | tail -1)
```

This stage produces many files that end up under
`/poseidon/stor/manta_gc/all/done` and a links file that ends up under
`/poseidon/stor/manta_gc/all/do`.

## Link up output files

The GC link app will verify that the job ran successfully and link files into
their appropriate locations to be picked up by the moray cleaner and the makos.
The commands that will be run are in the link file under
`/poseidon/stor/manta_gc/all/do`.

```
ops$ gc_create_links.js | bunyan
```

This should remove the links file from `/poseidon/stor/manta_gc/all/do` after
processing the commands.  There should be many files linked into
`/poseidon/stor/manta_gc/mako` and `/poseidon/stor/manta_gc/moray`:

```
ops$ mfind /poseidon/stor/manta_gc/mako /poseidon/stor/manta_gc/moray
```

## Run Moray GC

The moray cleaner will delete records from the `manta_delete_log` table in
all moray shards.

```
ops$ moray_gc.js | bunyan
```

## Run Mako GC

On each moray:

```
mako$ /opt/smartdc/mako/bin/mako_gc.sh
```

This will move objects from `/manta/[creator]/[objectid]` to
`/manta/tombstone/[date]/[objectid]`.

It will also delete any `[date]` directories that are "too old", currently 21
days.

To verify that the GC pipeline is working:

```
mako$ ls /manta/tombstone/ | while read l \
   do echo -n "$l "; \
   ls /manta/tombstone/$l | wc -l; \
done
```

# Implementation Details

## Input

1. Moray shard dumps.  Currently located at

    /poseidon/stor/manatee_backups/[shard]/[date]/[table]-[date].gz

The two tables required for garbage collection are:

1. `manta`: Record of the set of 'live' objects.
2. `manta_delete_log`: Record of candidates for deletion.

## Phase 1: Marlin job

The garbage collection job is kicked off from the "mola" or "cron" zone deployed
as part of Manta.  The cron invokes `mola/bin/kick_off_gc.js`, which does a few
things:

1. Verifies that a gc job isn't currently running
2. Finds the latest Moray dumps, does some verification
3. Sets up assets and directories required by gc
4. Kicks off a marlin job

All output for the Marlin job is located under:

    /poseidon/stor/manta_gc

From a high-level, the Marlin job does the following:

1. Transforms the Moray dumps for tables `manta` and `manta_delete_log` into
records for each row.  The first field in each record is the Manta object id,
the second field is the time the record was produced.
2. The records for each object are then sent off to a number of reducers where
a reducer is guaranteed to have all records for a given object.
3. Reducers sort the set of rows so that records for objects are grouped
together and ordered by time.  The reducer can then walk the history of an
object and decide if it should be garbage collected.  An object is garbage
collected if the only record of that object is a deletion record that has passed
a system-wide grace period.  In addition, Moray deletion records are cleaned up
if records for the object referenced in the deletion record have later records.

The output of the Marlin job is a set of cleanup tasks for Moray shards and Mako
nodes.  Since we currently don't have a mechanism for making directories from
Marlin jobs and we want to keep the cleanup tasks around for debugging issues,
tasks are first uploaded to:

    /poseidon/stor/manta_gc/all/done/[date]-[job_id]-X-[uuid]-[mako|moray]-[serverid]

As well as uploading all cleanup task files, a file is uploaded with the set of
commands to run to link the above files into the correct locations for the Moray
and Mako cleanup agents.  These commands are uploaded to:

    /poseidon/stor/manta_gc/all/do/[date]-[job_id]-X-[uuid]-links

## Phase 2: Linking

A cron runs in the cron zone that will periodically look in:

    /poseidon/stor/manta_gc/all/do/

Download, and execute the instructions.  Once the links are successfully created,
the links file is deleted.  The executable is in `mola/bin/gc_create_links.sh`.

## Phase 3: Moray Cleanup

A cron runs in the cron zone that will periodically execute moray cleanup tasks.
The executable is located in `mola/bin/moray_gc.js`.  It looks for files in the
following directories:

    /poseidon/stor/manta_gc/moray/[shard]/

Downloads them and processes each line to delete from Moray.  It runs in the
cron zone so that it can reach out "directly" to each shard.

## Phase 4: Mako cleanup

A cron runs on each Mako node that periodically looks in:

    /poseidon/stor/manta_gc/mako/[zoneid]/

Downloads them and processes each line to tombstone objects.  The executable is
located in `mako/bin/mako_gc.sh`.

Objects are moved to a daily tombstone directory.  These directories are purged
after some number of days.  This final grace period allows operators to find and
restore data that has been accidentally deleted.  Once a tombstone directory has
been deleted, there is little to no chance of data recovery.

# See Also

* [Design Alternatives](gc-design-alternatives.md): Alternative designs we considered for Garbage Collection.
