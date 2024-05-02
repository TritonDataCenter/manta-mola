---
title: Mola System Crons Overview
markdown2extras: tables, code-friendly, fenced-code-blocks
apisections:
---
<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2018, Joyent, Inc.
    Copyright 2024 MNX Cloud, Inc.
-->

# Overview

The contents of this repo are joined with Mackerel and work with components
running on other systems to produce system-wide "crons".  Some of these system
tasks depend on others.  In the absence of an uber-coordinator (MANTA-2452),
we rely on time spacing to push tasks through the system.

# Dependency "Graph"

Here is a json representation of the dependency tree:

```
{
    "postgres": {
        "sql-to-json": {
            "storage-hourly-metering": null,
            "mako": {
                "audit": null,
                "cruft": null
            },
            "gc": {
                "gc-links": {
                    "moray-gc": null,
                    "mako-gc": null
                }
            },
            "mpu-gc": {
                "mpu-cleanup": null
            }
        }
    }
}
```

And a description of each of those:

* postgres: Runs on a manatee async, dumps the manatee DB and uploads to Manta.
* sql-to-json: Runs as a Manta job to take the output from postgres and
  transform it to a json format that later jobs understand.
* hourly-metering: Runs as a Manta job, takes output from sql-to-json and
  computes storage metering.
* mako: Runs on each mako zone, dumps a recursive directory listing into Manta.
* audit: Runs as a Manta job, Takes the output from sql-to-json and mako,
  verifies that all objects in moray exist on makos.
* cruft: In not currently running in a cron, but looks exactly like audit.
* gc: Runs as a Manta job, takes output from sql-to-json to figure out what
  objects should be garbage collected.
* gc-links: Runs from the ops zone, verifies the gc Manta jobs and links output
  so that it is picked up by moray-gc and mako gc.
* moray-gc: Runs from the ops zone, takes output from gc/gc-links to find and
  clean up Manta delete log records.
* mako-gc: Runs on each Mako, takes output from gc/gc-create-links to find and
  tombstone dead objects.  Also removes object tombstoned some number of days
  ago (21 days as of this writing).
* mpu-gc: Runs as a Manta job. Takes output from sql-to-json to determine what
  records need to be garbage collected as a result of multipart uploads.
* mpu-cleanup: Runs from the ops zone. Executes the instructions provided by the
  output of mpu-gc to clean up records related to multipart uploads.

NOTE: There are three additional jobs scheduled in cron that don't aren't
listed here. They are hourly compute metering, hourly request metering and a
daily summary job. The hourly compute and request metering jobs run every hour
and meter for the previous hour. The daily summary jobs takes as input all of
the previous day's compute and request metering records (24 records each) and
the single storage metering record for a total of 49 records. Since the daily
summary job depends on the last hour of metering from the previous day, it is
run a few hours after midnight to allow those jobs to finish.

# Timeline

Each of the processes above is given some fixed time to complete work for the
day.  This is the "authoritative" timeline.  Jobs listed in parenthesis run on
the following day.

The job name can be cross referenced to the executed job by cross referencing
the [cron configuration][cron] and the [manifest file][manifest].

[cron]: ../boot/setup.sh
[manifest]: https://github.com/TritonDataCenter/manta-mackerel/blob/master/sapi_manifests/mackerel-jobs/template

```
| 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 |
|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|
|postgres |
|         |sql-to-json                  |
|                                       |storage-hourly-metering      |
|                                       |mako                         |
|                                                                     |audit
|                                                                     |cruft
|                                       |gc            |
|                                                      |gc-l|inks
|                                                           |moray-gc
|                                                           |mako-gc
|
|                                             |mpu-gc            |
|                                                                |mpu-cleanup
|    |(daily-metering)
|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|
| 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 |
```

| Manta Job Name          | Cron job command         | Type        | Start Time |
| ----------------------- | ------------------------ | ----------- | ----------:|
| postgres                | (in manatee zone)        | maintenance |      00:00 |
| sql-to-json             | kick_off_pg_transform.js | meta        |      02:00 |
| gc                      | kick_off_gc.js           | maintenance |      08:05 |
| mpu-gc                  | kick_off_mpu_gc.js       | maintenance |      09:05 |
| storage-hourly-metering | meter-storage.sh         | metering    |      08:15 |
| gc-links                | gc_create_links.js       | maintenance |      11:10 |
| moray-gc                | moray_gc.js              | maintenance |      12:15 |
| mpu-cleanup             | kick_off_mpu_cleanup.js  | maintenance |      13:15 |
| audit                   | kick_off_audit.js        | maintenance |      14:20 |
| [none]                  | daily.sh                 | metering    |      14:55 |
| (daily-metering)        | meter-previous-day.sh    | metering    |      01:00 |

Also see MANTA-2438.

# Administration

Mola and Mackerel cron jobs can be disabled on an individual or global basis.
This can be done from the headnode using `sapiadm`. All jobs are enabled by
default.

All jobs (both mackerel and mola) can be disabled by setting `DISABLE_ALL_JOBS`
to 'true' in the 'ops' SAPI service, like so:
```
$ sapiadm update $(sdc-sapi /services?name=ops | json -Ha uuid) metadata.DISABLE_ALL_JOBS=true
```

Jobs can be disabled on an individual basis as well. These are the fields that
can be set to either 'true' or 'false' to disable or enable jobs:

| Manta Job Name                              | Enable/Disable field   |
| ------------------------------------------- | ---------------------- |
| all                                         | DISABLE_ALL_JOBS       |
| audit                                       | AUDIT_ENABLED          |
| gc, gc-links, moray-gc, mpu-gc, mpu-cleanup | GC_ENABLED             |
| sql-to-json                                 | PG_ENABLED             |
| storage-hourly-metering                     | METER_STORAGE_ENABLED  |
| compute-hourly-metering                     | METER_COMPUTE_ENABLED  |
| request-hourly-metering                     | METER_REQUEST_ENABLED  |
| (daily-metering)                            | METER_PREV_DAY_ENABLED |

For example, to disable all of the GC-family jobs you could run this command
from the headnode:
```
$ sapiadm update $(sdc-sapi /services?name=ops | json -Ha uuid) metadata.GC_ENABLED=false
```

And if you decide to later re-enable GC you can run the same command setting
`GC_ENABLED` to 'true':
```
$ sapiadm update $(sdc-sapi /services?name=ops | json -Ha uuid) metadata.GC_ENABLED=true
```

The change will be written to the mola or mackerel config files the next time
that the in-zone config-agent polls SAPI.
