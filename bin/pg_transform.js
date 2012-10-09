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
        var parser = new getopt.BasicParser('d:l:h:',
                                            process.argv);
        var tmp;

        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'd':
                        opts.dump_date = new Date(option.optarg);
                        break;
                case 'l':
                        opts.least_dump_date = new Date(option.optarg);
                        break;
                case 'h':
                        opts.moray_hostname = option.optarg;
                        break;
                }
        }
        if (!opts.dump_date) {
                usage('-d [dump_date] is a required argument');
        }
        if (!opts.least_dump_date) {
                usage('-l [least_dump_date] is a required argument');
        }
        if (!opts.moray_hostname) {
                usage('-h [moray_hostname] is a required argument');
        }
        return (opts);
}

function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-d dump_date] [-l least_dump_time] [-h moray_hostname]';
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
