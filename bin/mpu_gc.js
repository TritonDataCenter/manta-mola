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

var bunyan = require('bunyan');
var getopt = require('posix-getopt');
var lib = require('../lib');
var path = require('path');


///--- Globals

var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'mpu_gc',
        stream: process.stderr,
        serializers: bunyan.stdSerializers
});


///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('g:', process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'g':
                        opts.gracePeriodSeconds = parseInt(option.optarg, 10);
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }
        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-g grace_period_seconds]';
        console.error(str);
        process.exit(1);
}


///--- Main

var _opts = parseOptions();
_opts.reader = process.stdin;
_opts.log = LOG.child({
        component: 'MpuGarbageCollector'
});

// As a convenience seconds to millis
if (_opts.gracePeriodSeconds) {
        _opts.gracePeriodMillis = _opts.gracePeriodSeconds * 1000;
}

var _garbageCollector = lib.createMpuGarbageCollector(_opts);
_garbageCollector.on('mpuCleanup', function (record) {
        console.log(record.toString());
});

_garbageCollector.on('error', function (err) {
        console.error({ err: err }, 'Error with line, exiting.');
        process.exit(1);
});

process.stdin.resume();
