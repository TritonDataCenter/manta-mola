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
var child_process = require('child_process');
var getopt = require('posix-getopt');
var path = require('path');



/**
 * Bucketize by fields in a line, uploading to manta via mpipe.  For example,
 * this will bucketize quotes into last/first name files, given a stream
 * of records with lines like:
 *  FIRST,LAST,QUOTE
 *  cat quotes.txt | ./bin/mdemux.js -f 2,1 -d ',' \
 *    -p /$MANTA_USER/stor/quotes/{2}/{1}/quotes.txt
 *
 * The -p is required.  -f defaults to 1, -d defaults to (tab).
 */


///--- Defaults

var DEFAULT_DELIMITER = '\t';



///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('d:p:',
                                            process.argv);

        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'd':
                        opts.delimiter = option.optarg;
                        break;
                case 'p':
                        opts.pattern = option.optarg;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }

        }
        if (!opts.pattern) {
                usage('-p [pattern] is a required argument');
        }

        opts.delimiter = opts.delimiter || DEFAULT_DELIMITER;

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-d delimiter] [-p pattern]';
        console.error(str);
        process.exit(1);
}


//Any number between {}...
function extractFields(pattern) {
        /* JSSTYLED */
        var regexp = /\{(\d+)\}/g;
        var match = null;
        var fields = {};
        var res = [];
        /*jsl:ignore*/
        while (match = regexp.exec(pattern)) {
                /*jsl:end*/
                var n = match[1]; //The capture
                fields[n] = '';
        }

        for (var key in fields) {
                var f = parseInt(key, 10);
                res.push(f);
        }
        return (res);
}


function ifError(err, msg) {
        if (err) {
                console.log(err, msg);
                process.exit(1);
        }
}


function lookupPipe(key, pipes) {
        if (!pipes[key]) {
                var pipe = child_process.spawn('mpipe', [key]);
                pipes[key] = pipe;
        }
        return (pipes[key].stdin);
}



///--- Main

var _opts = parseOptions();

var _c = carrier.carry(process.stdin);
var _fields = extractFields(_opts.pattern);
var _pipes = {};

_c.on('line', function (line) {
        var parts = line.split(_opts.delimiter);
        var p = _opts.pattern;
        for (var i = 0; i < _fields.length; ++i) {
                var field = _fields[i];
                var part = parts[field - 1];
                var regexp = new RegExp('\\{' + field + '\\}', 'g');
                if (part === undefined) {
                        continue;
                }
                p = p.replace(regexp, part);
        }

        var pipe = lookupPipe(p, _pipes);
        pipe.write(line + '\n');
});

_c.on('end', function () {
        for (var key in _pipes) {
                _pipes[key].stdin.end();
        }
});

process.stdin.resume();
