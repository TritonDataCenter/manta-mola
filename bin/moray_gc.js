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

/*
 * This program streams one object at a time per shard, up to a limit.  We need
 * to allow at least one, and perhaps several, concurrent HTTP connections.
 * See the comments in "maxsockets.js" for more information.
 */
var SHARD_PARALLELISM = 32;
require('../lib/maxsockets')(SHARD_PARALLELISM * 4);


var assert = require('assert-plus');
var bunyan = require('bunyan');
var exec = require('child_process').exec;
var fs = require('fs');
var lib = require('../lib');
var manta = require('manta');
var vasync = require('vasync');
var verror = require('verror');
var stream = require('stream');

var VE = verror.VError;


var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'moray_gc',
        stream: process.stdout,
        serializers: bunyan.stdSerializers
});
var MANTA_CONFIG = (process.env.MANTA_CONFIG ||
                    '/opt/smartdc/common/etc/config.json');
var MOLA_CONFIG = (process.env.MOLA_CONFIG ||
                    '/opt/smartdc/mola/etc/config.json');
var MOLA_CONFIG_OBJ = JSON.parse(fs.readFileSync(MOLA_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MANTA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var MORAY_CLEANUP_PATH = '/' + MANTA_USER + '/stor/manta_gc/moray';
var PID_FILE = '/var/tmp/moray_gc.pid';
var CRON_START = new Date();



/*
 * Called for each Manta object created by the garbage collection job.  This
 * function streams the contents of the input object from Manta, processing
 * each directive in the object to remove old records from the particular Moray
 * shard.
 */
function cleanShardOneObject(log, shard, input, cb) {
        assert.object(log, 'log');
        assert.string(shard, 'shard');
        assert.string(input, 'input');
        assert.func(cb, 'cb');

        log.info('deleting moray records listed in cleanup object');

        MANTA_CLIENT.get(input, {}, function (err, strom) {
                if (err) {
                        cb(VE(err, 'get "%s"', input));
                        return;
                }

                /*
                 * The Moray cleaner stream knows how to interpret the contents
                 * of the garbage collection input objects.  See the comments
                 * in "lib/moray_cleaner.js" for more information.
                 */
                var mcs = MORAY_CLEANER.cleanStream({
                        shard: shard,
                        object: input
                });

                mcs.once('error', function (mcsErr) {
                        cb(VE(mcsErr, 'cleanStream "%s"', input));
                });
                mcs.once('workComplete', function () {
                        MANTA_CLIENT.unlink(input, {}, function (ulErr) {
                                if (ulErr) {
                                        cb(VE(ulErr, 'unlink "%s"', input));
                                        return;
                                }
                                log.info('cleanup object complete');
                                cb();
                        });
                });

                strom.pipe(mcs);
        });
}


/*
 * For a particular Moray shard, look in the cleanup directory: "dir", a Manta
 * path.  This directory contains objects that are the output of the garbage
 * collection Manta job.  Each object contains a list of Moray records marked
 * for deletion.
 */
function cleanShardDirectory(log, shard, dir, cb) {
        assert.object(log, 'log');
        assert.string(shard, 'shard');
        assert.string(dir, 'dir');
        assert.func(cb, 'cb');

        log.info('cleaning shard');

        var errs = [];
        var finished = false;
        var outstanding = false;
        var nobjects = 0;

        var finish = function (err) {
                if (err) {
                        errs.push(err);
                }

                if (finished || outstanding) {
                        /*
                         * Don't call the callback twice.  If the list stream
                         * fails while a call to "cleanShardOneObject()" is
                         * in progress, don't call the callback until that
                         * operation completes.
                         */
                        return;
                }
                finished = true;

                if (errs.length === 0) {
                        log.info('cleaned %d shard objects', nobjects);
                        setImmediate(cb);
                } else {
                        log.info('cleaned %d shard objects (before error)',
                            nobjects);
                        setImmediate(cb, errs.length === 1 ? errs[0] :
                            new verror.MultiError(errs));
                }
        };

        var ls = MANTA_CLIENT.createListStream(dir, { type: 'object' });
        ls.on('error', function (err) {
                finish(VE(err, 'listing "%s"', dir));
        });

        var w = new stream.Writable({ objectMode: true, highWaterMark: 0 });
        w.on('error', finish);
        w.on('finish', function () {
                finish();
        });

        /*
         * For each directory entry from the list stream, invoke
         * "cleanShardOneObject()" to download that object and process its
         * contents.
         */
        w._write = function (ent, _, next) {
                if (finished)
                        return;

                assert.strictEqual(ent.type, 'object', 'wanted objects only');
                assert.strictEqual(ent.parent, dir, 'unexpected directory');

                var input = dir + '/' + ent.name;

                outstanding = true;
                cleanShardOneObject(log.child({ input: input }), shard, input,
                    function (err) {
                        outstanding = false;

                        if (err) {
                                next(VE(err, 'cleanShardOneObject: ' +
                                    'shard "%s", input "%s"', shard,
                                    input));
                                return;
                        }

                        nobjects++;
                        next();
                });
        };

        ls.pipe(w);
}


/*
 * This routine lists entries in the top-level Moray cleanup directory,
 * MORAY_CLEANUP_PATH.  Each child directory represents a Moray shard for which
 * we may have clean up work to do.
 */
function startGc(cb) {
        var errs = [];
        var q = vasync.queuev({
                worker: function (shard, next) {
                        assert.string(shard, 'shard');
                        assert.func(next, 'next');

                        var dir = MORAY_CLEANUP_PATH + '/' + shard;
                        var log = LOG.child({ shard: shard, dir: dir });

                        cleanShardDirectory(log, shard, dir, function (err) {
                                if (err) {
                                        next(VE(err, 'cleanShardDirectory "%s"',
                                            shard));
                                        return;
                                }

                                next();
                        });
                },
                concurrency: SHARD_PARALLELISM
        });
        q.on('end', function () {
                if (errs.length === 0) {
                        cb();
                        return;
                }

                cb(errs.length === 1 ? errs[0] : new verror.MultiError(errs));
        });
        var qCallback = function (qErr) {
                if (!qErr)
                        return;

                errs.push(qErr);
                LOG.error({ err: qErr }, 'shard cleaning failure; draining ' +
                    'already issued tasks');
                q.kill();
        };

        var ls = MANTA_CLIENT.createListStream(MORAY_CLEANUP_PATH,
            { type: 'directory' });

        ls.on('error', function (err) {
                if (err.name === 'ResourceNotFoundError') {
                        LOG.info({ path: MORAY_CLEANUP_PATH },
                            'No directories yet for manta gc.');
                        q.close();
                        return;
                }

                qCallback(VE(err, 'listing "%s"', MORAY_CLEANUP_PATH));
        });
        ls.on('readable', function () {
                var dir;

                while ((dir = ls.read()) !== null) {
                        assert.strictEqual(dir.type, 'directory',
                            'wanted directories only');

                        q.push(dir.name, qCallback);
                }
        });
        ls.on('end', function () {
                q.close();
        });
}


function checkAlreadyRunning(cb) {
        function recordPid() {
                LOG.debug('Taking process ownership.');
                fs.writeFileSync(PID_FILE, process.pid, 'utf8');
                startGc(function (err) {
                        cleanupPidFile(function () {
                                if (err) {
                                        cb(VE(err, 'startGc'));
                                        return;
                                }

                                cb();
                        });
                });
        }

        fs.stat(PID_FILE, function (err, stat) {
                if (err && err.code === 'ENOENT') {
                        recordPid();
                        return;
                }

                if (err && err) {
                        cb(err);
                        return;
                }

                if (!stat.isFile()) {
                        recordPid();
                        return;
                }

                var pid = fs.readFileSync(PID_FILE, 'utf8');
                LOG.info({ file: PID_FILE, foundPid: pid },
                          'Found process in pid file.');

                if (pid === '' || pid.length < 1) {
                        recordPid();
                        return;
                }

                exec('ps ' + pid, function (err2, stdout, stderr) {
                        //We ignore the error since ps will exit(1) and Node
                        // sets err2 if the pid doesn't exist.

                        LOG.debug({ stdout: stdout }, 'Got output.');
                        var lines = stdout.split('\n');
                        if (lines.length > 1 && lines[1].length > 0 &&
                            lines[1].indexOf(pid) !== -1) {
                                LOG.info({
                                        foundPid: pid
                                }, 'Moray GC process already running.');
                                cb();
                                return;
                        }
                        recordPid();
                        return;
                });
        });
}


function cleanupPidFile(cb) {
        fs.unlinkSync(PID_FILE);
        cb();
}


function auditCron(err) {
        var end = new Date();
        var cronRunMillis = end.getTime() - CRON_START.getTime();
        var mcStats = MORAY_CLEANER.getStats();

        var audit = {
                'audit': true,
                'cronFailed': (err !== null && err !== undefined),
                'startTime': CRON_START,
                'endTime': end,
                'rowsDeleted': mcStats.rowsDeleted,
                'rowsAlreadyDeleted': mcStats.rowsAlreadyDeleted,
                'cronRunMillis': cronRunMillis
        };

        LOG.info(audit, 'audit');

        return (audit.cronFailed);
}

///--- Main

if (MOLA_CONFIG_OBJ.disableAllJobs === true) {
        LOG.info('All jobs are disabled, exiting.');
        process.exit(0);
}

if (MOLA_CONFIG_OBJ.gcEnabled === false) {
        LOG.info('GC is disabled, exiting.');
        process.exit(0);
}


var MORAY_CLEANER = lib.createMorayCleaner({ log: LOG, batchSize: 1000 });
MORAY_CLEANER.on('error', function (err) {
        LOG.fatal(err);
        var returnCode = auditCron(err);
        process.exit(returnCode);
});

checkAlreadyRunning(function (err) {
        if (err) {
                LOG.fatal(err);
        }
        var returnCode = auditCron(err);
        MANTA_CLIENT.close();
        MORAY_CLEANER.close(function () {
                LOG.info('Done.');
                process.exit(returnCode);
        });
});
