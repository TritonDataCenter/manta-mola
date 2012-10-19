#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var getopt = require('posix-getopt');
var lib = require('../lib');
var path = require('path');



///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('g:',
                                            process.argv);
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
//As a convience, seconds to millis
if (_opts.gracePeriodSeconds) {
        _opts.gracePeriodMillis = _opts.gracePeriodSeconds * 1000;
}

var _garbageCollector = lib.createGarbageCollector(_opts);
_garbageCollector.on('moray', function (moray) {
        console.log('moray\t' + moray.toString());
});

_garbageCollector.on('mako', function (mako) {
        console.log('mako\t' + mako.toString());
});

_garbageCollector.on('error', function (line) {
        console.error('error\t' + line);
});

process.stdin.resume();
