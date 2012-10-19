#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var path = require('path');



/**
 * Examples:
 *
 * Bucketize by hashing first field into 4 buckets, files go in directory foo:
 *  cat /tmp/foo.txt | ./bin/demux.js -d /tmp/foo -p xyz -b 4
 */

///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser(':d:p:b:',
                                            process.argv);

        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'd':
                        opts.directory = option.optarg;
                        break;
                case 'p':
                        opts.file_prefix = option.optarg;
                        break;
                case 'b':
                        opts.num_buckets = parseInt(option.optarg, 10);
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }

        }
        if (!opts.directory) {
                usage('-d [directory] is a required argument');
        }
        if (!opts.file_prefix) {
                usage('-p [file_prefix] is a required argument');
        }
        if (isNaN(opts.num_buckets) || opts.num_buckets < 1) {
                usage('-b [num_buckets] is required and must be a number.');
        }

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-d output_directory] [-p file_prefix] [-b num_buckets]';
        console.error(str);
        process.exit(1);
}



///--- Main

var _opts = parseOptions();

//Make sure that the dir exists
fs.stat(_opts.directory, function (err, stat) {
        var dir = _opts.directory;
        var filePrefix = _opts.file_prefix;
        var filenames = [];
        var streams = [];
        var filesOpened = 0;

        if (err || !stat.isDirectory()) {
                console.error(_opts.directory +
                              ' doesnt exist or isnt a directory.');
                process.exit(1);
        }

        //Creates the stream demux.
        function createStreamDemux() {
                var opts = {
                        input: process.stdin,
                        output: streams
                };
                lib.createStreamDemux(opts, function (err2) {
                        if (err) {
                                console.log(err2);
                                process.exit();
                        }
                });
                process.stdin.resume();
        }

        //Delegates to createStreamDemux after all files
        // are open.
        function writeWhenReady(err2) {
                if (err) {
                        console.error(err2);
                        process.exit(1);
                }
                ++filesOpened;
                if (filesOpened === filenames.length) {
                        createStreamDemux();
                }
        }

        //Create all the output streams.
        if (dir.indexOf('/', dir.length - 1) === -1) {
                dir += '/';
        }
        var i;
        for (i = 0; i < _opts.num_buckets; ++i) {
                filenames.push(dir + filePrefix + '.' + i);
        }
        for (i = 0; i < filenames.length; ++i) {
                var stream = fs.createWriteStream(filenames[i]);
                stream.once('open', writeWhenReady);
                streams.push(stream);
        }
});
