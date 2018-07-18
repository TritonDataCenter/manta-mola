#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * The maximum number of HTTP connections was extremely limited in older
 * versions of Node.  Raise the limit to cover concurrent processing of
 * a potentially large number of Moray shards.
 */
require('../lib/maxsockets')(256);

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var common = require('../lib').common;
var getopt = require('posix-getopt');
var manta = require('manta');
var path = require('path');
var vasync = require('vasync');
var stream = require('stream');
var lstream = require('lstream');
var verror = require('verror');

var VE = verror.VError;


var NAME = 'moray_gc_create_links';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MANTA_CONFIG = (process.env.MANTA_CONFIG ||
                    '/opt/smartdc/common/etc/config.json');
var MOLA_CONFIG = (process.env.MOLA_CONFIG ||
                    '/opt/smartdc/mola/etc/config.json');
var MOLA_CONFIG_OBJ = JSON.parse(fs.readFileSync(MOLA_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MANTA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var MANTA_DIR = '/' + MANTA_USER + '/stor/manta_gc/all/do';
var AUDIT = {
        'audit': true,
        'cronExec': 1,
        'cronFailed': 1,
        'count': 0,
        'startTime': new Date()
};
var DIR_CACHE = {};
var JOB_DISABLED_ERR = 'JobDisabled';


function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('d:', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'd':
                        opts.mantaDir = option.optarg;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.mantaDir = opts.mantaDir || MANTA_DIR;
        opts.jobEnabled = MOLA_CONFIG_OBJ.gcEnabled;
        opts.disableAllJobs = MOLA_CONFIG_OBJ.disableAllJobs;

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-d manta_directory]';
        console.error(str);
        process.exit(1);
}


function deleteObject(objPath, cb) {
        LOG.info({ objPath: objPath }, 'deleting object');
        ++AUDIT.count;
        MANTA_CLIENT.unlink(objPath, function (err) {
                return (cb(err));
        });
}


function plfCreateLink(log, linkObj, done) {
        assert.object(log, 'log');
        assert.object(linkObj, 'linkObj');
        assert.string(linkObj.from, 'linkObj.from');
        assert.string(linkObj.to, 'linkObj.to');
        assert.func(done, 'done');

        log.info({ linkObj: linkObj }, 'linking object');

        MANTA_CLIENT.ln(linkObj.from, linkObj.to, function (linkErr) {
                if (linkErr) {
                        done(VE(linkErr, 'mln: from "%s" to "%s"',
                            linkObj.from, linkObj.to));
                        return;
                }

                done();
        });
}


function plfCreateDirectory(log, dirName, done) {
        assert.object(log, 'log');
        assert.string(dirName, 'dirName');
        assert.func(done, 'done');

        var dc = DIR_CACHE[dirName];

        if (!dc) {
                /*
                 * This directory has not yet been checked or created.
                 */
                dc = DIR_CACHE[dirName] = {
                        dc_exists: false,
                        dc_callbacks: [ done ]
                };
        } else if (dc.dc_exists) {
                /*
                 * This directory definitely exists already.
                 */
                setImmediate(done);
                return;
        } else {
                /*
                 * Wait for this directory to be processed by a mkdirp()
                 * operation already underway.
                 */
                dc.dc_callbacks.push(done);
                return;
        }

        log.info({ dirName: dirName }, 'creating directory');

        MANTA_CLIENT.mkdirp(dirName, function (dirErr) {
                var wrapped = null;

                if (dirErr) {
                        wrapped = VE(dirErr, 'mmkdirp "%s"', dirName);
                        delete DIR_CACHE[dirName];
                } else {
                        dc.dc_exists = true;
                }

                dc.dc_callbacks.forEach(function (otherNext) {
                        setImmediate(otherNext, wrapped);
                });
                dc.dc_callbacks = null;
        });
}


function processLinkFile(objPath, cb) {
        var log = LOG.child({ objPath: objPath });

        log.info('processing link file');

        var s = MANTA_CLIENT.createReadStream(objPath);
        var sres = null;
        var done = false;

        var finish = function (err) {
                if (done) {
                        return;
                }
                done = true;

                if (err) {
                        if (sres !== null) {
                                sres.destroy();
                        }

                        cb(err);
                        return;
                }

                /*
                 * As processing completed successfully, we can delete
                 * the input file.
                 */
                log.info('link file processing complete');
                deleteObject(objPath, cb);
        };

        s.on('open', function (res) {
                sres = res;
        });
        s.on('error', function (err) {
                finish(VE(err, 'streaming "%s"', objPath));
        });

        var w = new stream.Writable({ objectMode: true, highWaterMark: 0 });
        w._write = function (line, _, next) {
                assert.string(line, 'ch');
                assert.func(next, 'next');

                if (done || line === '') {
                        setImmediate(next);
                        return;
                }

                var parts = line.split(' ');

                if (parts.length === 2 && parts[0] === 'mmkdir') {
                        plfCreateDirectory(log, parts[1], next);
                } else if (parts.length === 3 && parts[0] === 'mln') {
                        plfCreateLink(log, { from: parts[1], to: parts[2] },
                            next);
                } else {
                        next(VE('invalid line: "%s"', line));
                }
        };

        w.on('error', function (err) {
                finish(VE(err, 'processing "%s"', objPath));
        });
        w.on('finish', function () {
                finish();
        });

        s.pipe(new lstream()).pipe(w);
}


/* BEGIN JSSTYLED */
// Example path to job:
// /nfitch/stor/manta_gc/all/do/2013-04-29-18-10-07-600e0d9d-b9b0-43e7-8893-a292397bcbb1-X-06925570-b0f8-11e2-8ab7-1f4a20f74bfb-links
//                              [------ date -----]-[----------- job uuid -------------]-X-[----------- random uuid ----------]-links
/* END JSSTYLED */
function findAndVerifyJob(objPath, cb) {
        //Extract the job from the name.  This relies on the gc_links script
        // to put the job in the right place, and to not change.
        var objName = path.basename(objPath);
        var dateJobId = objName.split('-X-')[0];
        var jobId = dateJobId.substring(20);
        var opts = {
                'client': MANTA_CLIENT,
                'jobId': jobId
        };
        common.getJob(opts, function (err, job) {
                if (err) {
                        cb(err);
                        return;
                }

                if (job.state === 'running' || job.inputDone === false) {
                        LOG.info({ jobId: jobId }, 'Job still running, not ' +
                                 'doing anything.');
                        cb(null);
                        return;
                }

                if (job.stats && (job.stats.errors > 0 ||
                                  job.cancelled === true)) {
                        LOG.error({ jobId: jobId, objectPath: objPath },
                                  'Job had errors, not processing links.');
                        //My first thought was to clean up all the data
                        // associated with the job, but we need to do that for
                        // all other jobs anyways.  So rather we just delete
                        // the link file.
                        deleteObject(objPath, cb);
                        return;
                }

                processLinkFile(objPath, function (plfErr) {
                        if (plfErr) {
                                LOG.error({
                                        err: plfErr,
                                        objPath: objPath
                                }, 'failed to process link file');
                        }

                        cb(plfErr);
                });
                return;
        });
}


function createGcLinks(opts, cb) {
        var gopts = {
                'client': MANTA_CLIENT,
                'dir': opts.mantaDir
        };

        if (opts.disableAllJobs === true) {
                cb(new VE({ 'name': JOB_DISABLED_ERR },
                        'all jobs are disabled'));
                return;
        }
        if (opts.jobEnabled === false) {
                cb(new VE({ 'name': JOB_DISABLED_ERR }, 'GC job is disabled'));
                return;
        }

        common.getObjectsInDir(gopts, function (err, objs) {
                if (err && err.code === 'ResourceNotFound') {
                        LOG.info('GC not ready yet: ' + opts.mantaDir +
                                 ' doesn\'t exist');
                        cb();
                        return;
                } else if (err) {
                        cb(err);
                        return;
                }

                if (objs.length === 0) {
                        LOG.info('No objects found in ' + opts.mantaDir);
                        cb();
                        return;
                }

                vasync.forEachParallel({
                        func: findAndVerifyJob,
                        inputs: objs
                }, function (err2, results) {
                        cb(err2);
                        return;
                });
        });
}



///--- Main

var _opts = parseOptions();

createGcLinks(_opts, function (err) {
        if (err) {
                if (verror.hasCauseWithName(err, JOB_DISABLED_ERR)) {
                        LOG.info(err);
                } else {
                        LOG.fatal(err, 'Error.');
                }
        } else {
                AUDIT.cronFailed = 0;
        }

        //Write out audit record.
        AUDIT.endTime = new Date();
        AUDIT.cronRunMillis = (AUDIT.endTime.getTime() -
                               AUDIT.startTime.getTime());
        AUDIT.opts = _opts;
        LOG.info(AUDIT, 'audit');
        process.exit(AUDIT.cronFailed);
});
