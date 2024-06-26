<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019 Joyent, Inc.
    Copyright 2024 MNX Cloud, Inc.
-->

# manta-mola

This repository is part of the Triton Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/TritonDataCenter/manta) project page.

Mola is one of two things:

- The manta zone that manages running manta "system" crons like garbage collection.
- The actual cron job in the cron zone that manages garbage collection.

This package contains the source code for #2.


## Active Branches

There are currently two active branches of this repository, for the two
active major versions of Manta. See the [mantav2 overview
document](https://github.com/TritonDataCenter/manta/blob/master/docs/mantav2.md)
for details on major Manta versions.

- [`master`](../../tree/master/) - For development of mantav2, the latest
  version of Manta. This is the version used by Triton.
- [`mantav1`](../../tree/mantav1/) - For development of mantav1, the long
  term support maintenance version of Manta.


# Repository

    bin/            Commands available in $PATH.
    boot/           Configuration scripts on zone setup.
    data/           Garbage collection data samples, used for testing.
    deps/           Git submodules and/or commited 3rd-party deps should go
                    here. See "node_modules/" for node.js deps.
    docs/           Project docs (restdown)
    lib/            Source files.
    node_modules/   Node.js deps, either populated at build time or commited.
                    See Managing Dependencies.
    sapi_manifests/ SAPI manifests for zone configuration.
    test/           Test suite (using node-tap)
    tools/          Miscellaneous dev/upgrade/deployment tools and data.
    Makefile
    package.json    npm module info (holds the project version)
    README.md



# Development

To check out and run the tests:

    git clone git@github.com:TritonDataCenter/manta-mola.git
    cd mola
    make all
    make test

Before commiting/pushing run `make prepush` and, if possible, get a code
review.



# Testing

    make test

You can also run a full GC cycle locally by first downloading some pg dumps into
`./tmp/` (make sure nothing else is in there), then:

    EARLIEST=$(ls tmp/ | sed 's/^\w*-//; s/.gz$//;' | sort | head -1); \
    for f in `ls tmp`; do \
       export DD=$(echo $f | sed 's/^\w*-//; s/.gz$//;'); \
       zcat tmp/$f | \
       node ./bin/gc_pg_transform.js -d $DD -e $EARLIEST \
         -m 1.moray.coal.joyent.us; \
    done | sort | node ./bin/gc.js -g 60

The `-g 60` is the grace period.  In order to be cleaned out of mako, the only
reference to an object will be in the manta_delete_log table and the creation
date for that record will be more than `-g [seconds]` old.

You can test a full audit cycle by first causing postgres dumps for each moray
shard, then mako dumps for each storage node.  This example uses the input from
a previously run audit job:

    for MANTA_INPUT_OBJECT in `mjob inputs b1448c8d-53f0-4f63-91b8-c351a716c3a3`
    do
        mget $MANTA_INPUT_OBJECT | \
        if [[ "$MANTA_INPUT_OBJECT" = *.gz ]]; then zcat; else cat; fi | \
        ./build/node/bin/node ./bin/audit_transform.js -k $MANTA_INPUT_OBJECT
    done | sort | ./build/node/bin/node ./bin/audit.js
