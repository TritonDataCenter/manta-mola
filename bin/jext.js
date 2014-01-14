#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var carrier = require('carrier');
var getopt = require('posix-getopt');
var path = require('path');



/**
 * jext takes a stream of json objects and extracts fields as prefixes.
 * For example:
 *     $ echo '{ "foo": "bar" }' | jext -f foo
 *     bar { "foo": "bar" }
 *
 * When run in reverse mode (-r) it will remove the prefixes (if any).
 *
 * When run with the -x (exclude) flag, it will not include the line if any
 * one of the fields is null or undefined.
 *
 * TODO: Add a delimiter other than space.
 */

///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        opts.fields = [];
        opts.reverse = false;
        opts.exclude = false;
        var parser = new getopt.BasicParser('f:rx',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'f':
                        opts.fields.push(option.optarg);
                        break;
                case 'r':
                        opts.reverse = true;
                        break;
                case 'x':
                        opts.exclude = true;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (opts.fields.length === 0 && !opts.reverse) {
                usage('-f [field] is a required argument');
        }

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += '-f [json field] ';
        str += '-r ';
        str += '-x ';
        str += '';
        console.error(str);
        process.exit(2);
}



///--- Helpers

//Walks down the "path" of a javascript object to find the
// end field.
function getField(field, obj) {
        var ret = obj;
        var parts = field.split('.');
        for (var i = 0; i < parts.length; ++i) {
                var n = parts[i];
                if (ret[n]) {
                        ret = ret[n];
                } else {
                        ret = null;
                        break;
                }
        }
        return ({
                name: field,
                value: ret
        });
}



///--- Main

var _opts = parseOptions();
var _carrier = carrier.carry(process.stdin);
var _lineNumber = 0;

_carrier.on('line', function (line) {
        ++_lineNumber;
        if (_opts.reverse) {
                console.log(line.substring(line.indexOf('{')));
        } else {
                if (line === '') {
                        return;
                }
                try {
                        var obj = JSON.parse(line);
                } catch (e) {
                        console.err({
                                'line': line,
                                'lineNumber': _lineNumber
                        }, 'line cannot be parsed as a json object');
                }
                var s = '';
                var shouldLog = true;
                _opts.fields.forEach(function (field) {
                        var f = getField(field, obj);
                        if (f.value !== null && f.value !== undefined) {
                                s += f.value;
                        } else if (_opts.exclude) {
                                shouldLog = false;
                        }
                        s += ' ';
                });
                if (shouldLog) {
                        console.log(s + line);
                }
        }
});

process.stdin.resume();
