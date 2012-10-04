# Mola

Repository: <git@git.joyent.com:mola.git>
Browsing: <https://mo.joyent.com/mola>
Who: Nate Fitch
Docs: <https://mo.joyent.com/docs/mola>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>


# Overview

Mola is one of two things:

- The manta zone that manages running manta "system" crons like garbage collection.
- The actual cron job in the cron zone that manages garbage collection.

This package contains the source code for #2.



# Repository

    data/           Garbage collection data samples, used for testing.
    deps/           Git submodules and/or commited 3rd-party deps should go
                    here. See "node_modules/" for node.js deps.
    docs/           Project docs (restdown)
    lib/            Source files.
    node_modules/   Node.js deps, either populated at build time or commited.
                    See Managing Dependencies.
    pkg/            Package lifecycle scripts
    test/           Test suite (using node-tap)
    tools/          Miscellaneous dev/upgrade/deployment tools and data.
    Makefile
    package.json    npm module info (holds the project version)
    README.md



# Development

To check out and run the tests:

    git clone git@git.joyent.com:mola.git
    cd mola
    make all
    make test

Before commiting/pushing run `make prepush` and, if possible, get a code
review.



# Testing

    make test
