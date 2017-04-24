#!/usr/bin/env node
// -*- mode: js -*-
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
var util = require('util');

/*
 * Transforms the unpacked dump into records using the format specified by
 * lib/mpu_gc_pg_row_transformer.js.
 *
 * This is analogous to bin/gc_pg_transform.js and lib/gc_pg_row_transformer.js
 * for normal GC.
 */

///--- Helpers

function isValidDate(date) {
        return (util.isDate(date) && !isNaN(date.getTime()));
}

function parseDate(dateString) {
        // So we're forcing a weird format here.  File dates come in the format
        // 2012-10-18-23-00-02.
        var parts = dateString.split('-');
        if (parts.length != 6) {
                usage('Invalid date: ' + dateString);
        }
        var ds = parts[0] + '-' + parts[1] + '-' + parts[2] + 'T' +
                parts[3] + ':' + parts[4] + ':' + parts[5] + 'Z';
        var date = new Date(ds);
        if (isValidDate(date)) {
                return (date);
        }
        // We'll let the caller catch this.
        return (dateString);
}


function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('d:e:m:',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'd':
                        opts.dumpDate = parseDate(option.optarg);
                        break;
                case 'e':
                        opts.earliestDumpDate = parseDate(option.optarg);
                        break;
                case 'm':
                        opts.morayHostname = option.optarg;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (!opts.dumpDate) {
                usage('-d [dump_date] is a required argument');
        }
        if (!opts.earliestDumpDate) {
                usage('-e [earliest_dump_date] is a required argument');
        }
        if (!opts.morayHostname) {
                usage('-m [moray_hostname] is a required argument');
        }
        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-d dump_date] [-e earliest_dump_time] [-m moray_hostname]';
        console.error(str);
        process.exit(1);
}



///--- Main

var _opts = parseOptions();
_opts.reader = process.stdin;

var _gcPgRowTransformer = lib.createMpuGcPgRowTransformer(_opts);
_gcPgRowTransformer.on('row', function (row) {
        console.log(row.toString());
});

process.stdin.resume();
