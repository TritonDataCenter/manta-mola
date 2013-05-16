#!/usr/bin/env node
// -*- mode: js -*-
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var path = require('path');
var sprintf = require('sprintf-js').sprintf;
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
        var parser = new getopt.BasicParser('a:m:nr:s:t',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'n':
                        opts.noJobStart = true;
                        break;
                case 'r':
                        opts.marlinReducerMemory = parseInt(option.optarg, 10);
                        break;
                case 's':
                        opts.mantaStorageId = option.optarg;
                        break;
                case 't':
                        opts.jobName = 'manta_audit_test';
                        opts.jobRoot = MP + '/manta_audit_test';
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.jobName = opts.jobName || 'manta_audit';
        opts.jobRoot = opts.jobRoot || MP + '/manta_audit';
        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.marlinReducerMemory = opts.marlinReducerMemory || 4096;
        opts.marlinPathToAsset = opts.assetObject.substring(1);
        opts.marlinAssetObject = opts.assetObject;

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-a asset_object]';
        str += ' [-m moray_shard]';
        str += ' [-n no_job_start]';
        str += ' [-r marlin_reducer_memory]';
        str += ' [-s manta_storage_id]';
        str += ' [-t output_to_test]';
        console.error(str);
        process.exit(1);
}


function startsWith(str, prefix) {
        return (str.slice(0, prefix.length) === prefix);
}


function endsWith(str, suffix) {
        return (str.indexOf(suffix, str.length - suffix.length) !== -1);
}


//TODO: Factor out somewhere?
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


function getAuditJob(opts, cb) {
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
                phases: [ {
                        type: 'storage-map',
                        exec: pgCmd
                }, {
                        type: 'reduce',
                        count: opts.numberReducers,
                        memory: opts.marlinReducerMemory,
                        exec: auditCmd
                }]
        };

        LOG.info({ job: job }, 'Audit Marlin Job Definition');

        cb(null, job);
}


function findMorayBackupObject(opts, cb) {
        var shard = opts.shard;
        var earliestMakoDump = opts.earliestMakoDump;
        var offset = (opts.offset === undefined) ? 0 : opts.offset;

        if (offset === 7) {
                LOG.info('Couldn\'t find moray backup for shard ' + shard);
                cb(null);
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
                        findMorayBackupObject({
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

                if (obj === null || obj.mtime > earliestMakoDump) {
                        findMorayBackupObject({
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


function findMorayObjects(opts, cb) {
        LOG.info({ opts: opts }, 'Find Moray Objects.');
        var shards = opts.shards;
        var earliestMakoDump = opts.earliestMakoDump;

        if (shards.length === 0 || !earliestMakoDump) {
                LOG.info('No shard backups.  Must not be setup yet.');
                cb(null);
                return;
        }

        vasync.forEachParallel({
                func: findMorayBackupObject,
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

                var objects = [];
                for (var i = 0; i < shards.length; ++i) {
                        var obj = results.successes[i];
                        //Ok, this is a little strange.  If we don't find one
                        // of them, then we don't want the job to continue
                        // but we don't want to log FATAL either.  So we
                        // return nothing, and that should give us ^^.
                        if (obj === null || obj === undefined) {
                                cb(null, []);
                                return;
                        }
                        objects.push(obj.fullPath);
                }

                cb(null, objects);
        });
}


function findLatestMakoObjects(opts, cb) {
        getObjectsInDir(MAKO_BACKUP_DIR, function (err, objects) {
                if (err && err.name === 'ResourceNotFoundError') {
                        LOG.info('No Mako Objects found');
                        cb(null, []);
                        return;
                }
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
                var paths = objects.map(function (ob) {
                        return (ob.fullPath);
                });

                cb(null, paths);
        });
}


function findObjects(opts, cb) {
        vasync.pipeline({
                funcs: [
                        findLatestMakoObjects,
                        findMorayObjects
                ],
                arg: opts
        }, function (err, results) {
                if (err) {
                        cb(err);
                        return;
                }

                var objects = [];
                for (var i = 0; i < results.successes.length; ++i) {
                        //If no objects were found for one of the results,
                        // return empty.
                        if (results.successes[i].length === 0) {
                                cb(null, []);
                                return;
                        } else {
                                objects = objects.concat(results.successes[i]);
                        }
                }
                cb(null, objects);
        });
}


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


function checkJobResults(job, audit, opts, cb) {
        // If the job wasn't successful, we don't want any alarms.  Audit will
        // take care of everything we need.
        if (job.stats.errors > 0) {
                cb(null);
                return;
        }

        //Find the output
        var p = '/' + MANTA_CLIENT.user + '/jobs/' + job.id + '/out.txt';
        getObject(p, function (err, res) {
                if (err) {
                        //Don't know if it failed or not, so don't audit.
                        cb(err);
                        return;
                }

                LOG.info({ jobId: job.id, outputs: res },
                         'Looking at job output.');
                var parts = res.split('\n');
                if (parts.length < 2 && parts[1] !== '') {
                        LOG.fatal({ jobId: job.id },
                                  'Somehow job has more than one output!');
                        cb(new Error('Job has more than one output!'));
                        return;
                }

                getObject(parts[0], function (err2, errorLines) {
                        if (err2) {
                                //Don't know if it failed or not.
                                cb(err2);
                                return;
                        }
                        if (errorLines !== '') {
                                //Bad juju.
                                LOG.fatal({
                                        job: job,
                                        outputObject: parts[0]
                                }, 'Audit job detected abnormalities between ' +
                                          'mako and moray.');
                        }
                        cb(null);
                });
        });
}


///--- Main

var _opts = parseOptions();

_opts.getJobDefinition = getAuditJob;
_opts.getJobObjects = findObjects;
_opts.preAudit = checkJobResults;

var jobManager = lib.createJobManager(_opts, MANTA_CLIENT, LOG);
jobManager.run(function () {
        LOG.info('Done for now.');
});
