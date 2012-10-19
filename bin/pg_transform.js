#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var getopt = require('posix-getopt');
var lib = require('../lib');
var path = require('path');
var util = require('util');



///--- Helpers

function isValidDate(date) {
        return (util.isDate(date) && !isNaN(date.getTime()));
}

function parseDate(dateString) {
        var date = new Date(dateString);
        if (isValidDate(date)) {
                return (date);
        }
        //So we're cheating a bit here.  File dates come in the format
        // 2012-10-18-23-00-02.
        var parts = dateString.split('-');
        while (parts.length < 6) {
                parts.append('00');
        }
        var ds = parts[0] + '-' + parts[1] + '-' + parts[2] + 'T' +
                parts[3] + ':' + parts[4] + ':' + parts[5] + 'Z';
        date = new Date(ds);
        if (isValidDate(date)) {
                return (date);
        }
        //We'll let the caller catch this.
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

var _pgRowTransformer = lib.createPgRowTransformer(_opts);
_pgRowTransformer.on('row', function (row) {
        console.log(row.toString());
});

process.stdin.resume();
