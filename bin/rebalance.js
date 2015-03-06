#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * rebalance.js: read object metadata on stdin and produce commands for mako
 * zones to execute a rebalance.  See kick_off_rebalance.js and the
 * documentation in docs/rebalancing-objects.md for details on how this works.
 * This program is typically executed as the body of a reduce task using the
 * shark configuration as an asset.
 *
 * The implementation is largely in lib/rebalancer.js.
 *
 * Usage information:
 *
 *     -d TMPDIR        Temporary directory for working files.
 *     (required)
 *
 *     -h STORID        If specified, make sure to migrate all copies of
 *     (optional)       all objects stored on shark STORID.
 *
 *     -s SHARKS_FILE   Path to a file containing Manta-wide shark
 *     (required)       configuration.  This should be a JSON object with keys
 *                      for each datacenter.  The values should be an array of
 *                      objects, one for each shark in that datacenter, with
 *                      "datacenter" and "manta_storage_id" keys.  This file may
 *                      omit shards that the user has requested that we avoid
 *                      using for the rebalance operation.
 *
 * The input records are JSON objects of the same form stored in the "manta"
 * Moray bucket.
 */

var getopt = require('posix-getopt');
var fs = require('fs');
var lib = require('../lib');
var path = require('path');
var util = require('util');



///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('d:h:s:',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'd':
                        opts.tmpDirectory = option.optarg;
                        break;
                case 'h':
                        opts.mantaStorageId = option.optarg;
                        break;
                case 's':
                        opts.sharksFile = option.optarg;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (!opts.tmpDirectory) {
                usage('-d [tmp directory] is a required argument');
        }

        if (!opts.sharksFile) {
                usage('-s [sharks file] is a required argument');
        }

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += '-d [tmp directory]';
        str += '-h [manta_storage_id]';
        str += '-s [sharks file]';
        str += '';
        console.error(str);
        process.exit(1);
}



///--- Main

var _opts = parseOptions();
_opts.reader = process.stdin;

var _sharks = JSON.parse(fs.readFileSync(_opts.sharksFile));
var _rebalancer = lib.createRebalancer({
        mantaStorageId: _opts.mantaStorageId,
        reader: process.stdin,
        sharks: _sharks,
        dir: _opts.tmpDirectory
});

_rebalancer.on('error', function (err) {
        console.error(err);
        process.exit(1);
});

process.stdin.resume();
