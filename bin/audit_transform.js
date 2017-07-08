#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var getopt = require('posix-getopt');
var lib = require('../lib');
var path = require('path');



///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('k:', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'k':
                        opts.mantaKey = option.optarg;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (!opts.mantaKey) {
                usage('-k [manta_key] is a required argument');
        }

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-k manta_key]';
        console.error(str);
        process.exit(1);
}



///--- Main

var _opts = parseOptions();
_opts.reader = process.stdin;

var _auditRowTransformer = lib.createAuditRowTransformer({
        reader: process.stdin,
        mantaKey: _opts.mantaKey
});

_auditRowTransformer.on('row', function (row) {
        process.stdout.write(row.toString() + '\n');
});

_auditRowTransformer.on('error', function (err) {
        console.error(err);
        process.exit(1);
});

process.stdin.resume();
