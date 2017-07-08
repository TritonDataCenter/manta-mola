#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var forkexec = require('forkexec');
var fs = require('fs');
var getopt = require('posix-getopt');
var lstream = require('lstream');
var path = require('path');
var sprintf = require('sprintf-js').sprintf;
var stream = require('stream');
var util = require('util');
var vasync = require('vasync');



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
        var parser = new getopt.BasicParser('d:p:vn', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'd':
                        opts.delimiter = option.optarg;
                        break;
                case 'p':
                        opts.pattern = option.optarg;
                        break;
                case 'v':
                        opts.verbose = true;
                        break;
                case 'n':
                        opts.noupload = true;
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
        str += ' [-d delimiter] [-p pattern] [-v] [-n]';
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


/*
 * DemuxFileStream is a writable, object mode stream.  It accepts strings as
 * input, using a consumer-supplied callback to transform them into an output
 * directive object, and then writes them to one of a dynamic set of temporary
 * files.
 *
 * The "options" object requires two properties:
 *
 *      processFunc:    a function which accepts a string and returns an output
 *                      directive object, described below.
 *
 *      outputDir:      the name of a temporary directory, which must already
 *                      exist, into which to collect output files.
 *
 * The output directive is an object with properties:
 *
 *      act_key:        a string which identifies the particular output stream
 *                      for this output line.
 *
 *      act_line:       the string (without terminating newline) that is to be
 *                      written to the file identified by "act_key".
 *
 * The names of temporary files have no particular relation to the key provided
 * in the output directive.  Once input to the stream ends, the "filesDone"
 * event will be emitted with an array of objects describing the output files.
 * Each object in this array will have the following properties:
 *
 *      key:            the key provided in "act_key" that identifies this
 *                      stream.
 *
 *      localPath:      the local file to which the data for this key was
 *                      written.
 */
function DemuxFileStream(options) {
        var self = this;

        assert.object(options, 'options');
        assert.func(options.processFunc, 'options.processFunc');
        assert.string(options.outputDir, 'options.outputDir');

        stream.Writable.call(this, {
                objectMode: true,
                highWaterMark: 0
        });

        self.dfs_processFunc = options.processFunc;
        self.dfs_outputDir = options.outputDir;

        self.dfs_lineCount = 0;

        self.dfs_files = {};
        self.dfs_nfiles = 0;

        self.dfs_finished = false;
        self.on('finish', function onFinish() {
                self.dfs_finished = true;

                /*
                 * End the write stream for all of the files we opened.  Push
                 * this to the next tick so that consumer "finish" events can
                 * run first.
                 */
                setImmediate(function endAllFiles() {
                        for (var fn in self.dfs_files) {
                                if (!self.dfs_files.hasOwnProperty(fn)) {
                                        continue;
                                }

                                var dfsf = self.dfs_files[fn];

                                dfsf.dfsf_stream.end();
                        }
                });
        });
}
util.inherits(DemuxFileStream, stream.Writable);

DemuxFileStream.prototype.dfsCommit = function dfsCommit(act, done) {
        var self = this;

        assert.string(act.act_key, 'act.act_key');
        assert.string(act.act_line, 'act.act_line');
        assert.func(done, 'done');
        assert.strictEqual(self.dfs_finished, false, 'dfsCommit after finish');

        var dfsf = self.dfs_files[act.act_key];
        assert.object(dfsf, 'dfsf for key: ' + act.act_key);

        dfsf.dfsf_lines++;
        if (!dfsf.dfsf_stream.write(act.act_line + '\n')) {
                /*
                 * This file is blocked for writes.  To avoid exhausting
                 * available memory with buffered records, hold processing
                 * until the file stream has drained.
                 */
                dfsf.dfsf_nblocks++;
                dfsf.dfsf_stream.once('drain', function dfsOnFileDrain() {
                        done();
                });
                return;
        }

        setImmediate(done);
};

DemuxFileStream.prototype.dfsFinish = function dfsFinish() {
        var self = this;
        var results = [];

        for (var k in self.dfs_files) {
                if (!self.dfs_files.hasOwnProperty(k)) {
                        continue;
                }

                var dfsf = self.dfs_files[k];

                results.push({
                        localPath: dfsf.dfsf_filepath,
                        key: dfsf.dfsf_key,
                        lineCount: dfsf.dfsf_lines
                });
        }

        self.emit('filesDone', results);
};

DemuxFileStream.prototype._write = function dfsWrite(line, _, done) {
        var self = this;

        assert.string(line, 'line');
        assert.strictEqual(self.dfs_finished, false, '_write after finish');

        self.dfs_lineCount++;

        var action;
        if ((action = self.dfs_processFunc(line)) === null) {
                setImmediate(done);
                return;
        }

        assert.string(action.act_key, 'action.act_key');
        assert.string(action.act_line, 'action.act_line');

        /*
         * Check to see if we've already opened this file.
         */
        if (self.dfs_files[action.act_key]) {
                /*
                 * The file is already open.  Write the record immediately.
                 */
                self.dfsCommit(action, done);
                return;
        }

        /*
         * Open the new file, holding processing until the open completes.
         */
        var dfsf = self.dfs_files[action.act_key] = {
                dfsf_key: action.act_key,
                dfsf_filepath: path.join(self.dfs_outputDir,
                    sprintf('tmp.%016d', self.dfs_nfiles)),
                dfsf_stream: null,
                dfsf_lines: 0,
                /*
                 * Number of times this file blocked due to backpressure:
                 */
                dfsf_nblocks: 0
        };
        self.dfs_nfiles++;

        dfsf.dfsf_stream = fs.createWriteStream(
            dfsf.dfsf_filepath, { flags: 'wx' });

        dfsf.dfsf_stream.once('open', function fstrOnOpen() {
                self.emit('fileOpen', dfsf.dfsf_key, dfsf.dfsf_filepath);
                self.dfsCommit(action, done);
        });

        dfsf.dfsf_stream.once('finish', function fstrFinish() {
                assert.strictEqual(self.dfs_finished, true,
                    'file "' + dfsf.dfsf_filepath + '" finished before ' +
                    'input processing was done');
                assert.ok(self.dfs_nfiles > 0, 'nfiles > 0');

                if (--self.dfs_nfiles === 0) {
                        /*
                         * Once all files are finished streaming out, we
                         * emit a final event.
                         */
                        self.dfsFinish();
                }

        });
};



var _opts = parseOptions();

var _fields = extractFields(_opts.pattern);
var _outputDir = path.join('/var/tmp', 'mdemux.' + process.pid);

try {
        fs.mkdirSync(_outputDir, parseInt('0700', 8));
} catch (ex) {
        console.error('ERROR: could not mkdir(' + _outputDir + '): ' +
            ex.stack);
        process.exit(1);
}

var DFS = new DemuxFileStream({
        outputDir: _outputDir,
        processFunc: function (line) {
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

                return ({
                        act_key: p,
                        act_line: line
                });
        }
});

if (_opts.verbose) {
        DFS.on('fileOpen', function (remotePath, localPath) {
                console.error('open remote: ' + remotePath);
                console.error('      local: ' + localPath);
        });
}

DFS.on('filesDone', function (files) {
        if (_opts.verbose) {
                var not = _opts.noupload ? 'not ' : '';
                console.error('\nall files written; %suploading...\n', not);
        }

        /*
         * Input to the demux stream has finished, and all temporary files
         * have been closed.  Upload each one in turn using "mpipe".
         */
        vasync.forEachPipeline({
                inputs: files,
                func: function uploadFile(file, next) {
                        var argv = [ 'mpipe', '-f', file.localPath, file.key ];

                        if (_opts.verbose) {
                                console.error('execFile: %s',
                                    argv.join(' ') + '\n');
                        }

                        if (_opts.noupload) {
                                setImmediate(next);
                                return;
                        }

                        forkexec.forkExecWait({
                                argv: argv,
                                includeStderr: true
                        }, function (err, info) {
                                if (err) {
                                        err._extra = file.localPath + ' -> ' +
                                            file.key;
                                        next(err);
                                        return;
                                }

                                try {
                                        fs.unlinkSync(file.localPath);
                                } catch (ex) {
                                        next(ex);
                                        return;
                                }

                                next();
                        });
                }
        }, function (err) {
                if (err && err._extra) {
                        console.error('ERROR: upload failure (%s): %s',
                            err._extra, err.stack);
                        process.exit(1);
                } else if (err) {
                        console.error('ERROR: upload failure: %s',
                            err.stack);
                        process.exit(1);
                }

                if (_opts.noupload) {
                        console.error('dryrun only; output in: %s',
                            _outputDir);
                        return;
                }

                try {
                        fs.rmdirSync(_outputDir);
                } catch (ex) {
                        console.error('ERROR: could not unlink "%s": %s',
                            _outputDir, ex.message);
                        process.exit(1);
                }
        });
});

if (_opts.verbose) {
        console.error('reading input...');
}
process.stdin.pipe(new lstream()).pipe(DFS);
