<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# A Manual GC Run in COAL

This outlines tracing an object through coal as it is Garbage Collected.  It
shows a new Mola developer how to walk the object through the pipeline.

This assumes you are familiar with the Manta ops zone and Manta tools.

# Disable All Mola crons

First log into the ops zone and disable all crons so that you (the operator)
controls the garbage collection.

```
[root@e19ae56a (ops) ~]$ crontab -l >c
[root@e19ae56a (ops) ~]$ vi c
[root@e19ae56a (ops) ~]$ crontab c
[root@e19ae56a (ops) ~]$ crontab -l | grep mola
#0 2 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/kick_off_pg_transform.js >>/var/log/mola-pg-transform.log 2>&1
#5 8 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/kick_off_gc.js >>/var/log/mola.log 2>&1
#10 11 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/gc_create_links.js >>/var/log/mola-gc-create-links.log 2>&1
#15 12 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/moray_gc.js >>/var/log/mola-moray-gc.log 2>&1
#20 14 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/kick_off_audit.js >>/var/log/mola-audit.log 2>&1
[root@e19ae56a (ops) ~]$
```

Also disable the mako gcs in each of the storage zones:

```
[root@a8bf2143 (storage) ~]$ crontab -l >c
[root@a8bf2143 (storage) ~]$ vi c
[root@a8bf2143 (storage) ~]$ crontab c
[root@a8bf2143 (storage) ~]$ crontab -l | grep mako_gc
#15 12 * * * /opt/smartdc/mako/bin/mako_gc.sh >>/var/log/mako-gc.log 2>&1
```

# Create A Delete Log Record

First create an object, then delete it to create a delete log record:

```
[root@e19ae56a (ops) ~]$ echo 'delete_me' | mput /poseidon/stor/delete_me
[root@e19ae56a (ops) ~]$ mget /poseidon/stor/delete_me
delete_me
[root@e19ae56a (ops) ~]$ minfo /poseidon/stor/delete_me | grep etag
etag: 0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d
[root@e19ae56a (ops) ~]$ mrm /poseidon/stor/delete_me
[root@e19ae56a (ops) ~]$ export OBJECT_ID=0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d
```

Make sure you grab the etag before you delete the object.  That is the same as
the object id and will be used to trace the record through the delete pipeline.

You can fetch the delete log record from moray or log into postgres and look at
the db:
```
[root@e19ae56a (ops) ~]$ findobjects -h 1.moray.coal.joyent.us manta_delete_log "(objectId=$OBJECT_ID)" | json -Ha value.sharks
[
  {
    "datacenter": "coal",
    "manta_storage_id": "1.stor.coal.joyent.us"
  },
  {
    "datacenter": "coal",
    "manta_storage_id": "2.stor.coal.joyent.us"
  }
]
[root@3c9e482d (postgres) ~]$ psql moray
moray=# select _key from manta_delete_log where objectId = '0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d';
                        _key
-----------------------------------------------------
 /0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d/1419022448155
(1 row)

moray=#
```

# Dump the Postgres db to Manta

First find out who the sync is, then force it to dump:

```
[root@3c9e482d (postgres) ~]$ manatee-adm state | json -Ha sync.zoneId
3c9e482d-8b65-4995-b9ac-e7d6021e1e9e
[root@3c9e482d (postgres) ~]$ # I happened to already be on the sync...
[root@3c9e482d (postgres) ~]$ # note the uuid in the prompt
[root@3c9e482d (postgres) ~]$ /opt/smartdc/manatee/pg_dump/pg_dump.sh
...
[2014-12-19T20:59:56Z] /opt/smartdc/manatee/pg_dump/pg_backup_common.sh:198: upload_pg_dumps(): mput -H 'm-pg-size: 63261' -f /zones/3c9e482d-8b65-4995-b9ac-e7d6021e1e9e/data/pg_dump/fc0b49b6-87c1-11e4-9ea5-a76573502a7a/2014-12-19-20_moray-2014-12-19-20-59-55.gz /poseidon/stor/manatee_backups/1.moray.coal.joyent.us/2014/12/19/20/moray-2014-12-19-20-59-55.gz
...ray-2014-12-19-20-59-55.gz [====================================================>] 100%  25.69KB
...
[root@3c9e482d (postgres) ~]$ mls -l /poseidon/stor/manatee_backups/1.moray.coal.joyent.us/2014/12/19/20/moray-2014-12-19-20-59-55.gz
-rwxr-xr-x 1 poseidon         26305 Dec 19 21:00 moray-2014-12-19-20-59-55.gz
```

# Transform the PG dump

Now from the ops zone we can transform the dump:

```
[root@e19ae56a (ops) ~]$ kick_off_pg_transform.js -b /poseidon/stor/manatee_backups/1.moray.coal.joyent.us/2014/12/19/20/moray-2014-12-19-20-59-55.gz | bunyan
...
      },
      "jobId": "cf666335-f138-c56e-dc9c-eb62fe714188"
    }
[root@e19ae56a (ops) ~]$ mjob get cf666335-f138-c56e-dc9c-eb62fe714188 | json state stats
done
{
  "errors": 0,
  "outputs": 1,
  "retries": 0,
  "tasks": 1,
  "tasksDone": 1
}
```

The verify that there are tables:

```
[root@e19ae56a (ops) ~]$ mls /poseidon/stor/manatee_backups/1.moray.coal.joyent.us/2014/12/19/20
buckets_config-2014-12-19-20-59-55.gz
manta-2014-12-19-20-59-55.gz
manta_delete_log-2014-12-19-20-59-55.gz
manta_directory_counts-2014-12-19-20-59-55.gz
manta_storage-2014-12-19-20-59-55.gz
marlin_domains_v2-2014-12-19-20-59-55.gz
marlin_errors_v2-2014-12-19-20-59-55.gz
marlin_health_v2-2014-12-19-20-59-55.gz
marlin_jobinputs_v2-2014-12-19-20-59-55.gz
marlin_jobs_v2-2014-12-19-20-59-55.gz
marlin_taskinputs_v2-2014-12-19-20-59-55.gz
marlin_taskoutputs_v2-2014-12-19-20-59-55.gz
marlin_tasks_v2-2014-12-19-20-59-55.gz
medusa_sessions-2014-12-19-20-59-55.gz
moray-2014-12-19-20-59-55.gz
```

You can pull down the delete log and verify that the object exists:

```
[root@e19ae56a (ops) ~]$ mget -q /poseidon/stor/manatee_backups/1.moray.coal.joyent.us/2014/12/19/20/manta_delete_log-2014-12-19-20-59-55.gz | zcat | grep $OBJECT_ID
{"entry":["1","\\N","/0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d/1419022448155","{\"dirname\":\"/074d493f-c3e5-cf02-bb85-c30c1bf85de5/stor\",\"key\":\"/074d493f-c3e5-cf02-bb85-c30c1bf85de5/stor/delete_me\",\"headers\":{},\"mtime\":1419022417433,\"name\":\"delete_me\",\"creator\":\"074d493f-c3e5-cf02-bb85-c30c1bf85de5\",\"owner\":\"074d493f-c3e5-cf02-bb85-c30c1bf85de5\",\"roles\":[],\"type\":\"object\",\"contentLength\":10,\"contentMD5\":\"+/Cvbr8nT1YyrB/+/kaMPw==\",\"contentType\":\"application/octet-stream\",\"etag\":\"0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d\",\"objectId\":\"0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d\",\"sharks\":[{\"datacenter\":\"coal\",\"manta_storage_id\":\"1.stor.coal.joyent.us\"},{\"datacenter\":\"coal\",\"manta_storage_id\":\"2.stor.coal.joyent.us\"}],\"_etag\":\"67CE89E2\"}","1C461260","1419022448155","\\N","0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d"]}
```

# Run the Marlin GC Job

Now we run GC on that dump, saying that we should have any grace period.  This
will allow GC to pick up the object we just deleted rather than sitting around
for a few days waiting for the object to expire past the grace period:

```
[root@e19ae56a (ops) ~]$ kick_off_gc.js -g 1 | bunyan
...
      },
      "jobId": "133fad4a-6d29-6966-970f-e68691772986"
    }
[root@e19ae56a (ops) ~]$ mjob get 133fad4a-6d29-6966-970f-e68691772986 | json state stats
done
{
  "errors": 0,
  "outputs": 5,
  "retries": 0,
  "tasks": 4,
  "tasksDone": 4
}
```

Now we can see the files the job created.  First the link files:

```
[root@e19ae56a (ops) ~]$ mls /poseidon/stor/manta_gc/all/do
2014-12-19-21-25-42-133fad4a-6d29-6966-970f-e68691772986-X-98a0179a-87c5-11e4-9304-0b3ffe28c7d3-links
2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-links
```

Then the gc files:

```
[root@e19ae56a (ops) ~]$ mls /poseidon/stor/manta_gc/all/done | grep 133fad4a-6d29-6966-970f-e68691772986
2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-1.stor.coal.joyent.us
2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-2.stor.coal.joyent.us
2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-moray-1.moray.coal.joyent.us
```

Note that there is one for the moray shard and then one for each of the storage
zones.  We can see that the object exists once in all of them:

```
[root@e19ae56a (ops) ~]$ mfind /poseidon/stor/manta_gc/all/done | while read l; do echo $l; mget -q $l | grep $OBJECT_ID; echo; done
/poseidon/stor/manta_gc/all/done/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-1.stor.coal.joyent.us
mako    1.stor.coal.joyent.us   074d493f-c3e5-cf02-bb85-c30c1bf85de5    0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d

/poseidon/stor/manta_gc/all/done/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-2.stor.coal.joyent.us
mako    2.stor.coal.joyent.us   074d493f-c3e5-cf02-bb85-c30c1bf85de5    0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d

/poseidon/stor/manta_gc/all/done/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-moray-1.moray.coal.joyent.us
moray   1.moray.coal.joyent.us  0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d    1419022448155

```

# Link the files from `done` to `do`

The create links script checks that the gc marlin job ran successfully, then
sets up some links for GC.  Taking a look inside:

```
[root@e19ae56a (ops) ~]$ mfind /poseidon/stor/manta_gc/all/do | while read l; do echo $l; mget -q $l; echo; done/poseidon/stor/manta_gc/all/do/2014-12-19-21-25-42-133fad4a-6d29-6966-970f-e68691772986-X-98a0179a-87c5-11e4-9304-0b3ffe28c7d3-links

/poseidon/stor/manta_gc/all/do/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-links
mmkdir /poseidon/stor/manta_gc/mako/1.stor.coal.joyent.us
mln /poseidon/stor/manta_gc/all/done/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-1.stor.coal.joyent.us /poseidon/stor/manta_gc/mako/1.stor.coal.joyent.us/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-1.stor.coal.joyent.us
mmkdir /poseidon/stor/manta_gc/mako/2.stor.coal.joyent.us
mln /poseidon/stor/manta_gc/all/done/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-2.stor.coal.joyent.us /poseidon/stor/manta_gc/mako/2.stor.coal.joyent.us/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-2.stor.coal.joyent.us
mmkdir /poseidon/stor/manta_gc/moray/1.moray.coal.joyent.us
mln /poseidon/stor/manta_gc/all/done/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-moray-1.moray.coal.joyent.us /poseidon/stor/manta_gc/moray/1.moray.coal.joyent.us/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-moray-1.moray.coal.joyent.us

```

Running the link command:

```
[root@e19ae56a (ops) ~]$ gc_create_links.js | bunyan
...
[2014-12-19T21:34:43.861Z]  INFO: moray_gc_create_links/40517 on e19ae56a-6314-47db-8cd4-df553d9a1cca: audit (audit=true, cronExec=1, cronFailed=0, count=2, startTime=2014-12-19T21:34:43.080Z, endTime=2014-12-19T21:34:43.861Z, cronRunMillis=781)
    opts: {
      "mantaDir": "/poseidon/stor/manta_gc/all/do"
    }
```

Now the link files should be gone and the files should be linked into the moray
and mako locations:

```
[root@e19ae56a (ops) ~]$ mfind /poseidon/stor/manta_gc/all/do
[root@e19ae56a (ops) ~]$ mfind /poseidon/stor/manta_gc/moray | sort
/poseidon/stor/manta_gc/moray/1.moray.coal.joyent.us
/poseidon/stor/manta_gc/moray/1.moray.coal.joyent.us/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-moray-1.moray.coal.joyent.us
[root@e19ae56a (ops) ~]$ mfind /poseidon/stor/manta_gc/mako | sort
/poseidon/stor/manta_gc/mako/1.stor.coal.joyent.us
/poseidon/stor/manta_gc/mako/1.stor.coal.joyent.us/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-1.stor.coal.joyent.us
/poseidon/stor/manta_gc/mako/2.stor.coal.joyent.us
/poseidon/stor/manta_gc/mako/2.stor.coal.joyent.us/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-2.stor.coal.joyent.us
```

# Cleaning Moray

Just for kicks, verify that the object still exists in moray:

```
[root@e19ae56a (ops) ~]$ findobjects -h 1.moray.coal.joyent.us manta_delete_log "(objectId=$OBJECT_ID)"
{
  "bucket": "manta_delete_log",
  "key": "/0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d/1419022448155",
...
```

Then run the moray gc command:

```
[root@e19ae56a (ops) ~]$ moray_gc.js | bunyan
...
[2014-12-19T21:38:13.860Z]  INFO: moray_gc/41247 on e19ae56a-6314-47db-8cd4-df553d9a1cca: Done with obj.
    obj: /poseidon/stor/manta_gc/moray/1.moray.coal.joyent.us/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-moray-1.moray.coal.joyent.us
...
[2014-12-19T21:38:13.864Z]  INFO: moray_gc/41247 on e19ae56a-6314-47db-8cd4-df553d9a1cca: Done.
```

The object no longer exists in the delete log:

```
[root@e19ae56a (ops) ~]$ findobjects -h 1.moray.coal.joyent.us manta_delete_log "(objectId=$OBJECT_ID)"
[root@e19ae56a (ops) ~]$
```

# Cleaning Makos

Objects are moved from their original locations to the tombstone directory.  We
can locate the object on disk by taking the "creator" field and the "object id"
field from the moray record.  Since we don't have that, we can just use what
GC output:

```
[root@bcfa928c (storage) ~]$ export OBJECT_ID=0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d
[root@bcfa928c (storage) ~]$ json -f /opt/smartdc/mako/etc/gc_config.json manta_storage_id
1.stor.coal.joyent.us
[root@bcfa928c (storage) ~]$ mfind /poseidon/stor/manta_gc/mako/$(json -f /opt/smartdc/mako/etc/gc_config.json manta_storage_id) | xargs mget -q | grep $OBJECT_ID
mako    1.stor.coal.joyent.us   074d493f-c3e5-cf02-bb85-c30c1bf85de5    0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d
[root@bcfa928c (storage) ~]$ cat /manta/074d493f-c3e5-cf02-bb85-c30c1bf85de5/0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d
delete_me
```

Now run the mako gc, then see that the object has been moved to the tombstone
directory and that there are no more files for this mako node to process:

```
[root@bcfa928c (storage) ~]$ /opt/smartdc/mako/bin/mako_gc.sh
...
2014-12-19T21:46:22.000Z: mako_gc.sh (42995): info: success processing /manta_gc/mako/1.stor.coal.joyent.us/2014-12-19-21-26-12-133fad4a-6d29-6966-970f-e68691772986-X-aa589aac-87c5-11e4-98eb-073657a894a8-mako-1.stor.coal.joyent.us.
...
[root@bcfa928c (storage) ~]$ cat /manta/tombstone/2014-12-19/0dbe2527-e6b2-e1bc-ecf6-f4fe07b2451d
delete_me
[root@bcfa928c (storage) ~]$ mfind /poseidon/stor/manta_gc/mako/$(json -f /opt/smartdc/mako/etc/gc_config.json manta_storage_id)
[root@bcfa928c (storage) ~]$
```

Do that for each storage zone.

# End state

All of these directories should be empty:

```
[root@e19ae56a (ops) ~]$ mfind /poseidon/stor/manta_gc/all/do
[root@e19ae56a (ops) ~]$ mfind /poseidon/stor/manta_gc/moray
/poseidon/stor/manta_gc/moray/1.moray.coal.joyent.us
[root@e19ae56a (ops) ~]$ mfind /poseidon/stor/manta_gc/mako
/poseidon/stor/manta_gc/mako/1.stor.coal.joyent.us
/poseidon/stor/manta_gc/mako/2.stor.coal.joyent.us
```
