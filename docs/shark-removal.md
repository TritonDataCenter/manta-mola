---
title: Shark removal
markdown2extras: tables, code-friendly
apisections:
---
<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2015, Joyent, Inc.
-->

# Shark removal

"Sharks" are individual nodes in the storage tier.  Each one is represented by a
"storage" zone (also called a "mako" zone).  In a real deployment, you would
have at most one Mako zone on a physical server, though Manta supports multiple
Makos on a server for testing configurations.  For details on how Mako zones fit
into the Manta architecture, see the [Manta Operator's
Guide](https://joyent.github.io/manta).

In some cases, it is desirable to remove a particular Mako zone.  This can be
done without impacting service.  The basic process is to perform a _rebalance_
operation to move all the copies stored in that Mako zone to some other zone and
then remove the zone.  **It's important to be careful when executing this
procedure, since skipping a step or doing it wrong could result in unintended
data loss.**


## Prerequisites

1. Figure out which Mako zone you want to remove.  This depends on why you're
   removing it.  With a zone selected, you'll want to make sure you've got both
   the zonename and manta\_storage\_id identifiers for that zone handy.  You can
   list these properties using the "manta-adm" tool:

        [root@headnode (emy-10) ~]# manta-adm show -o service,zonename,storage_id
        storage
        SERVICE        ZONENAME                             STORAGE ID                
        storage        8e89b3de-7027-4c00-9f58-627089f6a194 3.stor.emy-10.joyent.us   
        storage        b2d25434-1afd-49d9-97bc-1d6993309973 2.stor.emy-10.joyent.us   
        storage        ef4a50d1-a083-4cff-99e3-7b20c5ddb564 4.stor.emy-10.joyent.us  

2. It is recommended to create a backup of the Mako zone before proceeding with
   this procedure.  A simple way to do this is to "zfs send" the delegated
   dataset associated with the Mako zone to a file in the global zone, presuming
   there's enough space there.

3. Make sure that various Manta components are up-to-date, especially the Manta
   deployment zone and the "ops" zone.  Specifically, you'll want to be using
   versions of these components after 2015-03-13.  Specific tickets of interest
   are MANTA-2606, MANTA-2594, and MANTA-2545.


## Procedure

1. Perform the [rebalancing procedure](rebalancing-objects.md).  This procedure
   involves making the Mako read-only so that Manta stops using it for writes,
   running a job to identify the object copies that need to be moved from this
   Mako, and then running commands on all the other Makos to pull down copies
   from the Mako that's being removed.

2. After completing that procedure, you should have already verified that the
   given mako zone is not referenced anywhere in the metadata tier.  You'll want
   to verify that there are also no files in "/manta" inside that mako zone.
   (You may see a lot of zero-byte files.  That should be okay, since you've
   already verified that they're not referenced by the metadata tier as part of
   the previous step.)

3. From the Manta deployment zone, use "manta-undeploy ZONENAME" to remove the
   Mako zone.   There is no check to make sure you're not removing something
   with live data, so make sure this is the zone you've been working with and
   its "/manta" directory is empty of all but zero-byte files.

4. Remove the object from /poseidon/stor/mako corresponding to the zone you just
   removed.

5. Once completed, check the subsequent day's audit job to make sure everything
   went well.  Also make sure that GC and metering are running as expected.  (To
   check GC, you can check that new daily directories are being created under
   "/manta/tombstone" on the remaining Makos.  To check metering, check for the
   presence of the daily summary file under /poseidon/stor/usage.  To check
   auditing, see the [audit](audit-overview.md) documentation.)
