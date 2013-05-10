#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var carrier = require('carrier');
var getopt = require('posix-getopt');
var lib = require('../lib');
var path = require('path');
var util = require('util');



///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('k:',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
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
