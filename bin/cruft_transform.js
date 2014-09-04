#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var carrier = require('carrier');
var getopt = require('posix-getopt');
var lib = require('../lib');
var path = require('path');
var util = require('util');



///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('f:k:',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'f':
                        opts.filterTimestamp = parseInt(option.optarg, 10);
                        break;
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
        if (!opts.filterTimestamp) {
                usage('-f [filter_timestamp] is a required argument');
        }
        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-f filter_timestamp]';
        str += ' [-k manta_key]';
        console.error(str);
        process.exit(1);
}



///--- Main

var _opts = parseOptions();
_opts.reader = process.stdin;

var _cruftRowTransformer = lib.createCruftRowTransformer({
        reader: process.stdin,
        mantaKey: _opts.mantaKey,
        filterTimestamp: _opts.filterTimestamp
});

_cruftRowTransformer.on('row', function (row) {
        process.stdout.write(row.toString() + '\n');
});

_cruftRowTransformer.on('error', function (err) {
        console.error(err);
        process.exit(1);
});

process.stdin.resume();
