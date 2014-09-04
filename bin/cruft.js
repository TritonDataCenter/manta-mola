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

var _cruftCollector = lib.createCruftCollector({
        reader: process.stdin
});

_cruftCollector.on('mako', function (mako) {
        console.log(mako.toString());
});

_cruftCollector.on('error', function (err) {
        console.error(err);
        process.exit(1);
});

process.stdin.resume();
