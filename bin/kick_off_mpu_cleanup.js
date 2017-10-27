#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var fs = require('fs');
var http = require('http');
var lstream = require('lstream');
var mahi = require('mahi');
var manta = require('manta');
var path = require('path');
var util = require('util');
var vasync = require('vasync');
var vstream = require('vstream');

var lib = require('../lib');
var mpu = require('../lib/mpu');

/*
 * TODO block comment
 *
 */

///--- Globals

var LOG_LEVEL = process.env.LOG_LEVEL || 'info';
var LOG = bunyan.createLogger({
        level: LOG_LEVEL,
        name: 'mpu_gc_cleanup',
        stream: process.stdout,
        serializers: bunyan.stdSerializers
});

var MANTA_CONFIG = (process.env.MANTA_CONFIG ||
                    '/opt/smartdc/mola/etc/config.json');
var CONFIG = JSON.parse(fs.readFileSync(MANTA_CONFIG, { encoding: 'utf8' }));

var MANTA_CLIENT = manta.createClientFromFileSync(MANTA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var MAHI_CLIENT = mahi.createClient(CONFIG.auth);

var MPU_GC_ROOT = '/' + MANTA_USER + '/stor/manta_mpu_gc';
var MPU_GC_CLEANUP_DIR = MPU_GC_ROOT + '/cleanup';
var MPU_GC_COMPLETED_DIR = MPU_GC_ROOT + '/completed';

var sprintf = util.format;


///--- Helpers

function getOptions() {
        var options = [
                {
                        names: [ 'dryRun', 'n' ],
                        type: 'bool',
                        help: 'Perform a dry run of the cleanup: not ' +
                              'removing any records, deleting input ' +
                              'instructions or uploading completed ' +
                              'instructions to Manta.'
                },
                {
                        names: ['verbose', 'v'],
                        type: 'bool',
                        help: 'Print out deletion commands to stderr.'
                },
                {
                        names: ['file', 'f'],
                        type: 'arrayOfString',
                        help: 'Local file to use for cleanup instructions. ' +
                              'Only remote files, local files, or the ' +
                              'default instruction directory may be used. ' +
                              'Local files will not be uploaded to Manta ' +
                              'after processing. '
                },
                {
                        names: ['remoteFile', 'r'],
                        type: 'arrayOfString',
                        help: 'File in Manta to use for cleanup ' +
                              'instructions. Only remote files, local files, ' +
                              'or the default instruction directory may be ' +
                              'used.'
                },
                {
                        names: ['help', 'h'],
                        type: 'bool',
                        help: 'Print this help and exit.'
                }
        ];

        function usage() {
                var str  = 'usage: ' + path.basename(process.argv[1]);
                str += ' [-n] [-v] [-d] [-t]';
                console.log(str);
                console.log(help);
        }

        var parser = dashdash.createParser({options: options});
        var help = parser.help().trimRight();
        var o;
        try {
                o = parser.parse(process.argv);
        } catch (e) {
                console.error('error: %s', e.message);
                usage();
                process.exit(1);
        }

        if (o.help) {
                usage();
                process.exit(0);
        }

        if (o.file && o.remoteFile) {
                var msg = 'only one of --file and --remoteFile may be used';
                console.error('error: %s', msg);
                usage();
                process.exit(1);
        }

        return (o);
}

/*
 * Returns a new pipeline stream suitable for cleaning up MPU garbage.
 *
 * TODO stream descriptions
 *
 * Parameters:
 * - args: an arguments object with the following properties:
 *      - mantaClient: a Manta client
 *      - mahiClient: a Mahi client
 *      - log: a bunyan logger
 *      - onFinishCb: callback function to be called when the last stream in the
 *        pipeline emits 'finish'
 *      - instrFile: for logging purposes, the path to the instructions file
 *        that will be used with this stream
 *      - dryRun: optional bool, which, if true, will do a dry run (no deletion
 *        of metadata records)
 *      - verbose: optional bool, which, if true, will print out each action
 *        taken by the cleanup streams to stderr
 *
 */
function newMpuGcStream(args) {
        assert.object(args, 'args');
        assert.object(args.log, 'args.log');
        assert.object(args.mantaClient, 'args.mantaClient');
        assert.object(args.mahiClient, 'args.mahiClient');
        assert.string(args.instrFile, 'args.instrFile');
        assert.func(args.onFinishCb, 'args.onFinishCb');
        assert.optionalBool(args.dryRun, 'args.dryRun');
        assert.optionalBool(args.verbose, 'args.verbose');

        var ls = vstream.wrapTransform(new lstream({
                highWaterMark: 0
        }));

        var mbs = vstream.wrapTransform(new mpu.createMpuBatchStream({
                log: args.log.child({
                        step: 1,
                        streamName: 'MpuBatchStream',
                        instrFile: args.inputFile
                })
        }));

        var mvs = vstream.wrapTransform(new mpu.createMpuVerifyStream({
                log: args.log.child({
                        step: 2,
                        streamName: 'MpuVerifyStream',
                        instrFile: args.inputFile
                })
        }));

        var mulrsPR = vstream.wrapTransform(
                new mpu.createMpuUnlinkLiveRecordStream({
                        log: args.log.child({
                                step: 3,
                                streamName: 'MpuUnlinkLivePartRecordsStream',
                                instrFile: args.inputFile
                        }),
                        dryRun: args.dryRun,
                        verbose: args.verbose,
                        mantaClient: args.mantaClient,
                        mahiClient: args.mahiClient,
                        type: 'partRecords'
        }));

        var mulrsUR = vstream.wrapTransform(
                new mpu.createMpuUnlinkLiveRecordStream({
                        log: args.log.child({
                                step: 4,
                                streamName: 'MpuUnlinkLiveUploadRecordStream',
                                instrFile: args.inputFile
                        }),
                        dryRun: args.dryRun,
                        verbose: args.verbose,
                        mantaClient: args.mantaClient,
                        mahiClient: args.mahiClient,
                        type: 'uploadRecord'
        }));

        var mmcls = vstream.wrapStream(
                new mpu.createMpuMorayCleanerStream({
                        log: args.log.child({
                                step: 5,
                                streamName: 'MpuMorayCleanerStream',
                                instrFile: args.inputFile
                        }),
                        dryRun: args.dryRun,
                        verbose: args.verbose
        }));

        /*
         * We know the pipeline has completed when the last stream emits
         * 'finish', not the PipelineStream itself.
         */
        mmcls.on('finish', args.onFinishCb);

        var mpuGcStreams = new vstream.PipelineStream({
                streams: [
                        ls,
                        mbs,
                        mvs,
                        mulrsPR,
                        mulrsUR,
                        mmcls
                ],

                streamOpts: {
                        highWaterMark: 0,
                        objectMode: true
                }
        });

        return (mpuGcStreams);
}

/*
 * Fetches entries from Manta for the input directory. These entries represent
 * the inputs to the pipeline of MPU GC streams.
 *
 * Parameters:
 * - args: an arguments object with the following values:
 *      - mantaClient: a Manta client
 *      - log: a bunyan logger
 *      - dir: full Manta path representing input directory
 * - cb: callback function of the form cb(err, entries)
 */
function getInputsFromDir(args, cb) {
        assert.object(args, 'args');
        assert.object(args.mantaClient, 'args.mantaClient');
        assert.object(args.log, 'args.log');
        assert.string(args.dir, 'args.dir');
        assert.func(cb, 'cb');

        var log = args.log;

        log.debug({
                dir: args.dir
        }, 'getInputsFromDir: entered');


        var entries = [];
        args.mantaClient.ls(args.dir, function (err, res) {
                if (err) {
                        cb(err);
                }

                res.on('entry', function (e) {
                        entries.push(e.parent + '/' + e.name);
                });


                res.on('end', function () {
                        log.debug({
                                dir: args.dir,
                                entries: entries
                        }, 'getInputsFromDir: done');

                        cb(null, entries);
                });
        });
}

/*
 * This function is used in a vasync.pipeline() series to kick off the cleanup
 * stream process.
 *
 * For a list of inputs to use for the MPU GC stream pipeline, fetch the input
 * from Manta as a Readable stream. These streams are stored in an array passed
 * in by the caller, `args.cleanupStreams`.
 *
 * Parameters:
 * - args: an arguments object with the following values:
 *      - mantaClient: a Manta client
 *      - log: a bunyan logger
 *      - inputs: array of strings representing full Manta paths of objects that
 *        will be fetched and returned as a stream
 *      - cleanupStreams: array streams are pushed on to as each object is
 *        fetched from Manta
 * - cb: callback of the form cb(err, streamObjs)
 *
 *   TODO fix comment
 */
function getInputStreams(args, cb) {
        assert.object(args, 'args');
        assert.object(args.mantaClient, 'args.mantaClient');
        assert.object(args.log, 'args.log');
        assert.arrayOfString(args.inputs, 'args.inputs');
        assert.func(cb, 'cb');

        var log = args.log;
        var client = args.mantaClient;
        var streamObjs = [];

        log.debug({
                inputs: args.inputs
        }, 'getInputStreams: entered');

        function mantaGet(p, mcb) {
                assert.string(p, 'p');

                client.get(p, function (merr, s) {
                        if (merr) {
                                var msg = sprintf('failed to ' +
                                        'retrieve input file %s', p);
                                log.error(msg, merr);
                                mcb();
                                return;
                        }

                        streamObjs.push({
                                stream: s,
                                file: p
                        });
                        mcb();
                });
        }


        vasync.forEachParallel({
                inputs: args.inputs,
                func: mantaGet
        }, function (err) {
                log.debug({
                        inputs: args.inputs
                }, 'getInputStreams: done');

                cb(err, streamObjs);
        });
}


/*
 * This function is used as a helper for a function called in a
 * vasync.pipeline() series to kick off the cleanup stream process.
 *
 * For a list of streams containing instructions for the MPU GC streams
 * pipeline, instantiate a new version of the MPU GC pipeline, and pipe the
 * stream to it. As streams finish piping their instructions to the pipeline,
 * they are recorded on an array passed in by the caller.
 * the caller, `args.completed`.
 *
 * Parameters:
 * - args: an arguments object with the following values:
 *      - stream: Readable stream to pipe into the MPU GC streams pipeline
 *      - input: string representing the full path in Manta used to create the
 *        stream
 *      - completed: array input paths will be pushed onto as their associated
 *        streams have finished being piped to the MPU GC pipeline
 *      - log: a bunyan logger
 *      - mantaClient: a Manta client
 *      - mahiClient: a Mahi client
 *      - dryRun: optional bool, which, if true, will do a dry run of MPU GC
 *        cleanup (no deletion of metadata records)
 *      - verbose: optional bool, which, if true, will print out each action
 *        taken by the cleanup streams to stderr
 * - cb: callback function
 *
 *   TODO fix comment
 */
function cleanupFromStream(args, cb) {
        assert.object(args, 'args');
        assert.object(args.stream, 'args.stream');
        assert.string(args.instrFile, 'args.instrFile');
        assert.object(args.mantaClient, 'args.mantaClient');
        assert.object(args.mahiClient, 'args.mahiClient');
        assert.object(args.log, 'args.log');
        assert.optionalBool(args.dryRun, 'args.dryRun');
        assert.optionalBool(args.verbose, 'args.verbose');
        assert.func(cb, 'cb');

        var log = args.log;

        log.info({
                instrFile: args.instrFile
        }, 'cleanupFromStream: entered');

        function onFinish() {
                log.info({
                        instrFile: args.instrFile,
                        completed: args.completed
                }, 'cleanupFromStream: done');

                cb();
        }

        var mpuGcStreamArgs = {
                mantaClient: args.mantaClient,
                mahiClient: args.mahiClient,
                log: args.log,
                onFinishCb: onFinish,
                instrFile: args.instrFile,
                dryRun: args.dryRun,
                verbose: args.verbose
        };

        // TODO: attach an error listener here?
        args.stream.pipe(newMpuGcStream(mpuGcStreamArgs));
}


/*
 * This function is used in a vasync.pipeline() series to kick off the cleanup
 * stream process.
 *
 *
 * Parameters:
 * - args: an arguments object with the following values:
 *      - streamOpts: an options block to pass to the MPU GC streams pipeline
 *        constructor
 *      - cleanupStreams: array of Readable streams to pipe into the MPU GC
 *        cleanup streams pipeline
 *      - inputs: array of strings representing the full path in Manta used to
 *        create `cleanupStreams`, listed in the same order as `cleanupStreams`
 *      - log: a bunyan logger
 * - cb: vasync.pipeline callback function
 */
function linkAndDelCompleted(args, cb) {
        assert.object(args, 'args');
        assert.object(args.mantaClient, 'args.mantaClient');
        assert.object(args.log, 'args.log');
        assert.arrayOfString(args.completed, 'args.completed');
        assert.string(args.completedDir, 'args.completedDir');
        assert.optionalBool(args.dryRun, 'args.dryRun');
        assert.optionalBool(args.verbose, 'args.verbose');
        assert.func(cb, 'cb');

        var log = args.log;

        log.debug({
                files: args.completed,
                linkDir: args.completedDir,
                dryRun: args.dryRun ? true : false,
                verbose: args.verbose ? true : false
        }, 'linkAndDelCompleted: entered');


        function linkCompleted(arg, lccb) {
                assert.string(arg.src, 'arg.src');
                assert.string(arg.dest, 'arg.dest');
                assert.object(arg.client, 'arg.client');

                if (args.verbose) {
                        console.error('link ' + arg.src + ' to ' + arg.dest);
                }

                if (!args.dryRun) {
                        arg.client.ln(arg.src, arg.dest, function (err) {
                                if (err) {
                                        log.error({
                                                src: arg.src,
                                                dest: arg.dest,
                                                err: err
                                        }, 'could not link instruction file');
                                } else {
                                        log.info({
                                                src: arg.src,
                                                dest: arg.dest
                                        }, 'instructions file linked');
                                }

                                lccb(err);
                        });
                } else {
                        lccb();
                }
        }

        function delCompleted(arg, dccb) {
                assert.string(arg.src, 'arg.src');
                assert.object(arg.client, 'arg.client');

                if (args.verbose) {
                        console.error('unlink ' + arg.src);
                }

                if (!args.dryRun) {
                        arg.client.unlink(arg.src, function (err) {
                                if (err) {
                                        log.error({
                                                file: arg.src,
                                                err: err
                                        }, 'could not unlink instruction file');
                                } else {
                                        log.info({
                                                src: arg.src
                                        }, 'instructions file unlinked');
                                }

                                dccb(err);
                        });
                } else {
                        dccb();
                }
        }

        vasync.forEachParallel({
                inputs: args.completed,
                func: function linkAndDel(p, lcb) {
                        vasync.pipeline({
                                arg: {
                                        src: p,
                                        dest: args.completedDir + '/' +
                                                path.basename(p),
                                        client: args.mantaClient
                                },
                                funcs: [
                                        linkCompleted,
                                        delCompleted
                                ]
                        }, function (perr, r) {
                                lcb(perr);
                        });
                }
        }, function (err, _) {
                log.debug({
                        files: args.completed,
                        linkDir: args.completedDir,
                        dryRun: args.dryRun ? true : false,
                        verbose: args.verbose ? true : false
                }, 'linkAndDelCompleted: done');

                cb(err);
        });
}

/*
 * TODO
 */
function runCleanupInstructions(args, cb) {
        assert.object(args, 'args');
        assert.arrayOfObject(args.toCleanup, 'args.toCleanup');
        assert.object(args.mantaClient, 'args.mantaClient');
        assert.object(args.mahiClient, 'args.mahiClient');
        assert.object(args.log, 'args.log');
        assert.optionalBool(args.dryRun, 'args.dryRun');
        assert.optionalBool(args.verbose, 'args.verbose');
        assert.func(cb, 'cb');

        function runCleanup(s, rcb) {
                assert.object(s, 's');
                assert.object(s.stream, 's.stream');
                assert.string(s.file, 's.file');

                cleanupFromStream({
                        mantaClient: args.mantaClient,
                        mahiClient: args.mahiClient,
                        log: args.log,
                        dryRun: args.dryRun,
                        verbose: args.verbose,
                        stream: s.stream,
                        instrFile: s.file
                }, function () {
                        rcb(null, true);
                });
        }

        vasync.filter(toCleanup, runCleanup, function (err, results) {
                        //args.log.info({
                                //err: err,
                                //results: results
                        //});
                if (err) {
                        cb(err);
                } else {
                        completed = [];
                        results.forEach(function (s) {
                                completed.push(s.file);
                        });

                        cb(null, completed);
                }
        });
}


function scriptFinish() {
        // Close open clients.
        MAHI_CLIENT.close();
        MANTA_CLIENT.close();

        // Log what we did for posterity.
        LOG.info({
                instrFiles: instrFiles,
                completed: completed
        }, 'MPU cleanup script finished.');

        process.exit(exitCode);
}


///--- Main

var userOpts = getOptions();
var exitCode = 0;

/*
 * Array of strings representing all input instruction files.
 */
var instrFiles;

/*
 * An array of objects containing the stream fetched for each input file. Each
 * object is of the form:
 *  {
 *      stream: Readable stream
 *      file: Manta path to object
 *  }
 *
 * The set of files contained in all objects in this array are a subset of those
 * contained in `inputs`. If there are no errors fetching each input, we would
 * expect the length of `inputs` and `sObjs` to be the same. If there is an
 * error fetching some of the objects, we still attempt to use the instructions
 * that were successfully fetched.
 */
var toCleanup;

/*
 * An array of strings representing the instruction files whose instructions
 * were successfully completed. This set of files is a subset of those contained
 * in `toCleanup`.
 */
var completed;

if (!userOpts.file) {
        vasync.pipeline({
                funcs: [
                        /*
                         * First, determine what cleanup instruction files exist
                         * in Manta.
                         */
                        function getInputs(_, cb) {
                                var dir = MPU_GC_CLEANUP_DIR;

                                getInputsFromDir({
                                        mantaClient: MANTA_CLIENT,
                                        log: LOG,
                                        dir: dir
                                }, function (err, inputs) {
                                        if (err) {
                                                LOG.fatal({
                                                        err: err,
                                                        dir: dir
                                                }, 'error listing input files');

                                                cb(err);
                                        } else {
                                                instrFiles = inputs;

                                                LOG.info({
                                                        instrFiles: inputs
                                                }, 'instruction files fetched');
                                                cb();
                                        }
                                });
                        },

                        /*
                         * Fetch each cleanup file as a Readable stream.
                         */
                        function getStreams(_, cb) {
                                getInputStreams({
                                        mantaClient: MANTA_CLIENT,
                                        log: LOG,
                                        inputs: instrFiles
                                }, function (err, sObjs) {
                                        /*
                                         * On error, only stop the pipeline if
                                         * we don't get any valid streams to use
                                         * for cleanup.
                                         */
                                        if (err && sObjs &&
                                                sObjs.length === 0) {

                                                LOG.fatal({
                                                        err: err,
                                                        instrFiles: instrFiles
                                                }, 'could not fetch any ' +
                                                        'input files');

                                                cb(err);
                                        } else {
                                                toCleanup = sObjs;

                                                var cFiles = [];
                                                toCleanup.forEach(function (s) {
                                                        cFiles.push(s.file);
                                                });
                                                LOG.info({
                                                        instrFiles: cFiles
                                                }, 'fetched instructions ' +
                                                        'from Manta');

                                                cb();
                                        }
                                });
                        },

                        /*
                         * Start an instance of an MPU GC cleanup pipeline for
                         * each stream of instructions we have.
                         */
                        function executeCleanupInstructions(_, cb) {
                                runCleanupInstructions({
                                        toCleanup: toCleanup,
                                        mantaClient: MANTA_CLIENT,
                                        mahiClient: MAHI_CLIENT,
                                        log: LOG,
                                        dryRun: userOpts.dryRun,
                                        verbose: userOpts.verbose
                                }, function (err, c) {
                                        if (err) {
                                                var incomplete = [];
                                                toCleanup.forEach(function (s) {
                                                        incomplete.push(s.file);
                                                });

                                                LOG.fatal({
                                                        err: err,
                                                        instrFiles: incomplete
                                                }, 'could not finish cleanup');

                                                cb(err);
                                        } else {
                                                completed = c;

                                                LOG.info({
                                                        completed: completed
                                                }, 'completed MPU GC ' +
                                                        'cleanup instructions');

                                                cb();
                                        }
                                });
                        },

                        /*
                         * Finally, link the instruction files in the
                         * "completed" directory of Manta to indicate they've
                         * been processed, then unlink them from the input
                         * directory.
                         */
                        function linkAndDelCompletedInstructions(_, cb) {
                                linkAndDelCompleted({
                                        mantaClient: MANTA_CLIENT,
                                        log: LOG,
                                        completed: completed,
                                        completedDir: MPU_GC_COMPLETED_DIR,
                                        dryRun: userOpts.dryRun,
                                        verbose: userOpts.verbose
                                }, function (err) {
                                        if (err) {
                                                LOG.fatal({
                                                        err: err
                                                }, 'could not cleanup ' +
                                                        'all instruction ' +
                                                        'files');
                                                cb(err);
                                        } else {
                                                LOG.info({
                                                        completed: completed
                                                }, sprintf('linked ' +
                                                        'instruction files ' +
                                                        'to "%s" and deleted ' +
                                                        'them from "%s"',
                                                        MPU_GC_COMPLETED_DIR,
                                                        MPU_GC_CLEANUP_DIR));

                                                cb();
                                        }
                                });
                        }
                ]}, function (verr, results) {
                        if (verr) {
                                exitCode = 1;
                                LOG.error(verr);
                        }

                        scriptFinish();

        });
} else {
        toCleanup = [];
        instrFiles = [];
        userOpts.file.forEach(function (f) {
                instrFiles.push(f);
                toCleanup.push({
                        stream: fs.createReadStream(f),
                        file: f
                });
        });

        runCleanupInstructions({
                toCleanup: toCleanup,
                mantaClient: MANTA_CLIENT,
                mahiClient: MAHI_CLIENT,
                log: LOG,
                dryRun: userOpts.dryRun,
                verbose: userOpts.verbose
        }, function (err, c) {
                if (err) {
                        var incomplete = [];
                        toCleanup.forEach(function (s) {
                                incomplete.push(s.file);
                        });

                        LOG.fatal({
                                err: err,
                                instrFiles: incomplete
                        }, 'could not complete cleanup');

                        exitCode = 1;
                } else {
                        completed = c;
                }

                scriptFinish();
        });
}
