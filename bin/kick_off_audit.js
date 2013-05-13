#!/usr/bin/env node
// -*- mode: js -*-
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var getopt = require('posix-getopt');
var exec = require('child_process').exec;
var lib = require('../lib');
var manta = require('manta');
var MemoryStream = require('memorystream');
var path = require('path');
var sprintf = require('sprintf-js').sprintf;
var sys = require('sys');
var vasync = require('vasync');



///--- Global Objects

var NAME = 'mola-audit';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MOLA_AUDIT_CONFIG = (process.env.MOLA_AUDIT_CONFIG ||
                   '/opt/smartdc/mola/etc/config.json');
var MOLA_AUDIT_CONFIG_OBJ = JSON.parse(fs.readFileSync(MOLA_AUDIT_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MOLA_AUDIT_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var AUDIT = {
        'audit': true,
        'startedJob': 0,
        'cronFailed': 1,
        'startTime': new Date()
};



///--- Global Constants

var MP = '/' + MANTA_USER + '/stor';
var MANATEE_BACKUP_DIR = MP + '/manatee_backups';
var MAKO_BACKUP_DIR = MP + '/mako';
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var RUNNING_STATE = 'running';
var MAX_SECONDS_IN_AUDIT_OBJECT = 60 * 60 * 24 * 7; // 7 days



///--- Helpers

/* BEGIN JSSTYLED */
function getEnvCommon(opts) {
        return (' \
set -o pipefail && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getTransformCmd(opts) {
        var grepForStorageNode = '';
        if (opts.mantaStorageId) {
                grepForStorageNode = ' | grep ' + opts.mantaStorageId + ' | ';
        }
        return (getEnvCommon(opts) + ' \
if [[ "$MANTA_INPUT_OBJECT" = *.gz ]]; then zcat; else cat; fi | \
  ./build/node/bin/node ./bin/audit_transform.js -k $MANTA_INPUT_OBJECT \
    ' + grepForStorageNode + ' \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getAuditCmd(opts) {
        return (getEnvCommon(opts) + ' \
sort | ./build/node/bin/node ./bin/audit.js \
');
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        //First take what's in the config file, override what's on the
        // command line, and use the defaults if all else fails.
        var opts = MOLA_AUDIT_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        var parser = new getopt.BasicParser('c:m:r:s:t',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'c':
                        opts.codeBundle = option.optarg;
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'r':
                        opts.marlinReducerMemory = parseInt(option.optarg, 10);
                        break;
                case 's':
                        opts.mantaStorageId = option.optarg;
                        break;
                case 't':
                        opts.auditJobName = 'manta_audit_test';
                        opts.mantaAuditDir = MP + '/manta_audit_test';
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.codeBundle = opts.codeBundle ||
                '/opt/smartdc/common/bundle/mola.tar.gz';
        opts.auditJobName = opts.auditJobName || 'manta_audit';
        opts.mantaAuditDir = opts.mantaAuditDir || MP + '/manta_audit';
        opts.mantaAssetDir = opts.mantaAuditDir + '/assets';
        opts.molaAssetObject = opts.mantaAssetDir + '/mola.tar.gz';
        opts.molaPreviousJobsObject = opts.mantaAuditDir + '/jobs.json';

        opts.marlinReducerMemory = opts.marlinReducerMemory || 4096;
        opts.marlinPathToAsset = opts.molaAssetObject.substring(1);
        opts.marlinAssetObject = opts.molaAssetObject;

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-c code_bundle]';
        str += ' [-m moray_shard]';
        str += ' [-r marlin_reducer_memory]';
        str += ' [-s manta_storage_id]';
        str += ' [-t output_to_test]';
        console.error(str);
        process.exit(1);
}


//TODO: Factor out into common lib
function startsWith(str, prefix) {
        return (str.slice(0, prefix.length) === prefix);
}


//TODO: Factor out into common lib
function endsWith(str, suffix) {
        return (str.indexOf(suffix, str.length - suffix.length) !== -1);
}


//TODO: Factor out into common lib
function getObject(objectPath, cb) {
        var res = '';
        MANTA_CLIENT.get(objectPath, {}, function (err, stream) {
                if (err) {
                        cb(err);
                        return;
                }

                stream.on('error', function (err1) {
                        cb(err1);
                        return;
                });

                stream.on('data', function (data) {
                        res += data;
                });

                stream.on('end', function () {
                        cb(null, res);
                });
        });
}


//TODO: Factor out into common lib
function getJob(jobId, cb) {
        MANTA_CLIENT.job(jobId, function (err, job) {
                cb(err, job);
        });
}


//TODO: Factor out into common lib
function getObjectsInDir(dir, cb) {
        var objects = [];
        MANTA_CLIENT.ls(dir, {}, function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                res.on('object', function (obj) {
                        objects.push({
                                'directory': dir,
                                'object': obj,
                                'fullPath': dir + '/' + obj.name
                        });
                });

                res.once('error', function (err2) {
                        cb(err2);
                });

                res.once('end', function () {
                        cb(null, objects);
                });
        });
}


//TODO: Factor out into common lib (parts of this, anyways)
function createAuditMarlinJob(opts, cb) {
        //TODO: Fix this.
        opts.numberReducers = 1;

        var pgCmd = getTransformCmd(opts);
        var auditCmd = getAuditCmd(opts);

        /**
         * TODO: Add this reduce back in after we have multiple
         *       reducers working.
         * , {
         *         type: 'reduce',
         *         count: 1,
         *         exec: 'cat'
         * }
         */
        var job = {
                name: opts.auditJobName,
                phases: [ {
                        type: 'storage-map',
                        assets: [ opts.marlinAssetObject ],
                        exec: pgCmd
                }, {
                        type: 'reduce',
                        count: opts.numberReducers,
                        assets: [ opts.marlinAssetObject ],
                        memory: opts.marlinReducerMemory,
                        exec: auditCmd
                }]
        };

        LOG.info({ job: job }, 'Audit Marlin Job Definition');

        MANTA_CLIENT.createJob(job, function (err, jobId) {
                if (err) {
                        cb(err);
                        return;
                }

                opts.jobId = jobId;
                //Create the previous job record
                opts.previousJobs[jobId] = {
                        'timeCreated': new Date(),
                        'audited': false
                };

                LOG.info({ jobId: jobId }, 'Created Job.');
                var aopts = {
                        end: true
                };
                var objects = opts.objects;

                //Add objects to job...
                MANTA_CLIENT.addJobKey(jobId, objects, aopts, function (err2) {
                        if (err2) {
                                cb(err2);
                                return;
                        }

                        LOG.info({
                                objects: objects,
                                jobId: jobId
                        }, 'Added objects to job');

                        AUDIT.numberOfObjects = objects.length;
                        AUDIT.startedJob = 1;
                        LOG.info('Done for now.');
                        cb();
                });
        });
}


//TODO: Factor out into common lib.
function setupAuditDirectories(opts, cb) {
        var m = MANTA_CLIENT;
        vasync.pipeline({
                funcs: [
                        function (_, c) { m.mkdir(opts.mantaAuditDir, c); },
                        function (_, c) { m.mkdir(opts.mantaAssetDir, c); }
                ]
        }, function (err) {
                cb(err);
        });
}


//TODO: Factor out into common lib.
function setupAuditMarlinJob(opts, cb) {
        setupAuditDirectories(opts, function (err) {
                if (err) {
                        cb(err);
                        return;
                }

                //Upload the bundle to manta
                fs.stat(opts.codeBundle, function (err2, stats) {
                        if (err2) {
                                cb(err2);
                                return;
                        }

                        if (!stats.isFile()) {
                                cb(new Error(opts.codeBundle +
                                             ' isn\'t a file'));
                                return;
                        }

                        var o = {
                                copies: 2,
                                size: stats.size
                        };

                        var s = fs.createReadStream(opts.codeBundle);
                        var p = opts.molaAssetObject;
                        s.pause();
                        s.on('open', function () {
                                MANTA_CLIENT.put(p, s, o, function (e) {
                                        if (e) {
                                                cb(e);
                                                return;
                                        }
                                        createAuditMarlinJob(opts, cb);
                                });
                        });
                });
        });
}


function findMorayBackupObjects(opts, cb) {
        var shard = opts.shard;
        var earliestMakoDump = opts.earliestMakoDump;
        var offset = (opts.offset === undefined) ? 0 : opts.offset;

        if (offset === 7) {
                cb(new Error('Couldn\'t find moray backup for shard ' +
                             shard));
                return;
        }

        //We need to find a backup that is as close in time to the earliest
        // mako dump, but still earlier.  We're looking for
        // /[MANTA_USER]/stor/manatee_backups/[shard]/\
        //    [year]/[month]/[day]/[hour]/\
        //    manta-[year]-[month]-[day]-[hour]-[minutes]-[seconds].gz

        //Subtract one hour for each offset
        var ed = new Date(earliestMakoDump);
        var d = new Date(ed.getTime() - (offset * 60 * 60 * 1000));

        var dir = sprintf('%s/%s/%04d/%02d/%02d/%02d',
                          MANATEE_BACKUP_DIR, shard,
                          d.getUTCFullYear(), d.getUTCMonth() + 1,
                          d.getUTCDate(), d.getUTCHours() + 1);
        getObjectsInDir(dir, function (err, objects) {
                if (err && err.name === 'ResourceNotFoundError') {
                        findMorayBackupObjects({
                                'shard': shard,
                                'earliestMakoDump': earliestMakoDump,
                                'offset': offset + 1
                        }, cb);
                        return;
                }

                if (err) {
                        cb(err);
                        return;
                }

                var obj = null;
                for (var i = 0; i < objects.length; ++i) {
                        var o = objects[i].object;
                        if (startsWith(o.name, MANTA_DUMP_NAME_PREFIX)) {
                                obj = o;
                                break;
                        }
                }

                if (obj === null) {
                        findMorayBackupObjects({
                                'shard': shard,
                                'earliestMakoDump': earliestMakoDump,
                                'offset': offset + 1
                        }, cb);
                        return;
                }

                obj.directory = dir;
                obj.fullPath = obj.directory + '/' + obj.name;

                cb(null, obj);
        });
}


function runAudit(opts, cb) {
        LOG.info({ opts: opts }, 'Running Audit.');
        var shards = opts.shards;
        var earliestMakoDump = opts.earliestMakoDump;

        if (shards.length === 0 || !earliestMakoDump) {
                cb(new Error('Shards or earliest mako dump date missing.'));
                return;
        }

        vasync.forEachParallel({
                func: findMorayBackupObjects,
                inputs: shards.map(function (shard) {
                        return ({
                                'shard': shard,
                                'earliestMakoDump': earliestMakoDump
                        });
                })
        }, function (err, results) {
                if (err) {
                        cb(err);
                        return;
                }
                if (results.successes.length !== shards.length) {
                        cb(new Error('Couldnt find backup for all ' +
                                     'shards.'));
                        return;
                }

                for (var i = 0; i < shards.length; ++i) {
                        var obj = results.successes[i];
                        opts.objects.push(obj.fullPath);
                }

                setupAuditMarlinJob(opts, cb);
        });
}


function findLatestMakoDumps(opts, cb) {
        getObjectsInDir(MAKO_BACKUP_DIR, function (err, objects) {
                if (err) {
                        cb(err);
                        return;
                }

                var earliestDump = null;
                for (var i = 0; i < objects.length; ++i) {
                        var o = objects[i].object;
                        //We can string compare here since we have an
                        // ISO 8601 date.
                        if (earliestDump === null || earliestDump > o.mtime) {
                                earliestDump = o.mtime;
                        }
                }
                if (earliestDump === null) {
                        cb(new Error('Couldn\'t determine earliest dump from ' +
                                     'mako dumps.'));
                        return;
                }

                opts.earliestMakoDump = earliestDump;
                opts.objects = objects.map(function (ob) {
                        return (ob.fullPath);
                });

                runAudit(opts, cb);
        });
}


//This kinda sucks.  Since the new job APIs, the list doesn't return
// all the relevant information, so we have to fetch them all.  Since audit
// should run at most once per day it shouldn't be too bad... but still.
//TODO: Factor out into common lib.
function findRunningAuditJobs(opts, cb) {
        var lopts = { state: RUNNING_STATE };
        MANTA_CLIENT.listJobs(lopts, function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                var jobs = [];

                res.on('job', function (job) {
                        jobs.push(job.name);
                });

                res.on('error', function (err2) {
                        cb(err2);
                });

                res.on('end', function () {
                        if (jobs.length === 0) {
                                cb(null, null);
                                return;
                        }
                        vasync.forEachParallel({
                                func: getJob,
                                inputs: jobs
                        }, function (err2, results) {
                                if (err2) {
                                        cb(err2);
                                        return;
                                }

                                var aj = null;
                                for (var i = 0; i < jobs.length; ++i) {
                                        var j = results.successes[i];
                                        if (j.name === opts.auditJobName) {
                                                aj = j;
                                        }
                                }
                                cb(null, aj);
                        });
                });
        });
}


//TODO: Factor out into common lib
function checkForRunningJobs(opts, cb) {
        findRunningAuditJobs(opts, function (err, job) {
                if (err) {
                        cb(err);
                        return;
                }

                if (job && !job.inputDone) {
                        //Check if the job's input is still open, if so,
                        // kill it and continue since it's pointless
                        // to try and resume if we have newer dumps.
                        MANTA_CLIENT.cancelJob(job.id, function (err2) {
                                if (err2) {
                                        cb(err2);
                                        return;
                                }
                                findLatestMakoDumps(opts, cb);
                                return;
                        });
                } else if (job) {
                        var started = (new Date(job.timeCreated)).getTime() /
                                1000;
                        var now = (new Date()).getTime() / 1000;
                        AUDIT.currentJobSecondsRunning =
                                Math.round(now - started);
                        LOG.info(job, 'Audit Job already running.');
                        cb();
                        return;
                } else {
                        findLatestMakoDumps(opts, cb);
                        return;
                }
        });
}



///--- Auditing previous jobs

//TODO: Factor out into common lib
function auditJob(job) {
        if (job.state === 'running') {
                return (false);
        }

        var audit = {
                audit: true,
                id: job.id
        };

        if (job.stats && job.stats.errors) {
                audit.jobErrors = job.stats.errors;
        } else {
                audit.jobErrors = 0;
        }

        audit.timeCreated = job.timeCreated;
        audit.jobDurationMillis =
                Math.round((new Date(job.timeDone)).getTime()) -
                Math.round((new Date(job.timeCreated)).getTime());

        LOG.info(audit, 'audit');

        return (true);
}


//TODO: Factor out into common lib
function auditPreviousJobs(opts, cb) {
        LOG.info('Auditing previous jobs.');
        var objPath = opts.molaPreviousJobsObject;
        opts.previousJobs = {};
        getObject(objPath, function (err, data) {
                if (err && err.code === 'ResourceNotFound') {
                        LOG.info(objPath + ' doesn\'t exist yet.');
                        cb();
                        return;
                }
                if (err) {
                        cb(err);
                        return;
                }

                try {
                        var pJobs = JSON.parse(data);
                        opts.previousJobs = pJobs;
                } catch (err2) {
                        cb(err2);
                        return;
                }

                var jobsToAudit = [];
                var jobsToDelete = [];
                for (var jobId in pJobs) {
                        var job = pJobs[jobId];
                        var now = (new Date()).getTime();
                        var created = (new Date(job.timeCreated)).getTime();
                        var secondsSinceDone = Math.round(
                                (now - created) / 1000);
                        if (!job.audited) {
                                jobsToAudit.push(jobId);
                        }
                        if (secondsSinceDone >
                            MAX_SECONDS_IN_AUDIT_OBJECT) {
                                jobsToDelete.push(jobId);
                        }
                }

                if (jobsToAudit.length === 0) {
                        LOG.info('No jobs to audit.');
                        cb();
                        return;
                }

                vasync.forEachParallel({
                        func: getJob,
                        inputs: jobsToAudit
                }, function (err2, results) {
                        if (err2) {
                                cb(err2);
                                return;
                        }

                        var i;
                        for (i = 0; i < jobsToAudit.length; ++i) {
                                var cJob = results.successes[i];
                                var cJobId = jobsToAudit[i];
                                if (auditJob(cJob)) {
                                        pJobs[cJobId].audited = true;
                                }
                        }

                        for (i = 0; i < jobsToDelete.length; ++i) {
                                var dJobId = jobsToDelete[i];
                                if (pJobs[dJobId].audited) {
                                        delete pJobs[dJobId];
                                }
                        }

                        opts.previousJobs = pJobs;
                        cb();
                });
        });
}


//TODO: Factor out into common lib
function recordJobs(opts, cb) {
        var recordString = JSON.stringify(opts.previousJobs);
        var o = { size: Buffer.byteLength(recordString) };
        var s = new MemoryStream();

        var objPath = opts.molaPreviousJobsObject;
        MANTA_CLIENT.put(objPath, s, o, function (err2) {
                cb(err2);
        });

        process.nextTick(function () {
                s.write(recordString);
                s.end();
        });

}



///--- Main

var _opts = parseOptions();

//TODO: Factor out into common lib
auditPreviousJobs(_opts, function (err) {
        if (err) {
                //We don't care that it failed, so we just log and continue.
                LOG.error(err);
        }

        checkForRunningJobs(_opts, function (err2) {
                if (err2) {
                        LOG.fatal(err2, 'Error.');
                } else {
                        AUDIT.cronFailed = 0;
                }

                recordJobs(_opts, function (err3) {
                        if (err3) {
                                LOG.error(err3, 'Error saving audit records.');
                        }

                        //Write out audit record.
                        AUDIT.endTime = new Date();
                        AUDIT.cronRunMillis = (AUDIT.endTime.getTime() -
                                               AUDIT.startTime.getTime());
                        AUDIT.opts = _opts;
                        LOG.info(AUDIT, 'audit');
                        process.exit(AUDIT.cronFailed);
                });
        });
});
