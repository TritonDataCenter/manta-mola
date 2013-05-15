#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var getopt = require('posix-getopt');
var lib = require('../lib');
var path = require('path');
var util = require('util');



///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
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
        str += '';
        console.error(str);
        process.exit(1);
}



///--- Main

var _opts = parseOptions();
_opts.reader = process.stdin;

var _auditor = lib.createAuditor({
        reader: process.stdin
});

_auditor.on('problem', function (problem) {
        process.stdout.write(problem + '\n');
});

_auditor.on('error', function (err) {
        console.error(err);
        process.exit(1);
});

process.stdin.resume();
