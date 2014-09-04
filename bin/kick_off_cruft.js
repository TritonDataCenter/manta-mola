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

var NAME = 'mola-crufy';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MOLA_CRUFT_CONFIG = (process.env.MOLA_CRUFT_CONFIG ||
                   '/opt/smartdc/mola/etc/config.json');
var MOLA_CRUFT_CONFIG_OBJ = JSON.parse(fs.readFileSync(MOLA_CRUFT_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MOLA_CRUFT_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;



///--- Global Constants

var MP = '/' + MANTA_USER + '/stor';
var MANATEE_BACKUP_DIR = MP + '/manatee_backups';
var MAKO_BACKUP_DIR = MP + '/mako';
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var MANTA_DELETE_LOG_NAME_PREFIX = 'manta_delete_log-';
var RUNNING_STATE = 'running';
var TOO_NEW_SECONDS = 60 * 60 * 24 * 2; // 2 days



///--- Helpers

/* BEGIN JSSTYLED */
function getEnvCommon(opts) {
        return (' \
set -o pipefail && \
export MANTA_CRUFT=' + opts.jobName + ' && \
export MARLIN_JOB=$(echo $MANTA_OUTPUT_BASE | cut -d "/" -f 4) && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getTransformCmd(opts) {
        var grepForStorageNode = '';
        var filterTimestamp =
                Math.floor(opts.earliestMorayDump.getTime() / 1000) -
                TOO_NEW_SECONDS;
        if (opts.mantaStorageId) {
                grepForStorageNode = ' | grep ' + opts.mantaStorageId + ' | ';
        }
        return (getEnvCommon(opts) + ' \
gzcat -f | \
  ./build/node/bin/node ./bin/cruft_transform.js -k $MANTA_INPUT_OBJECT \
    -f ' + filterTimestamp + ' \
    ' + grepForStorageNode + ' | \
  msplit -n ' + opts.numberReducers + ' \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getCruftCmd(opts) {
        //We reverse sort here so that the moray lines come first, followed
        // by the mako lines.  The other way was to insert a useless field
        // into the map output.

        // Output is (currently):
        // [object uuid] [mako node] mako [size] [create time]
        // The demux will split into [job]-[mako node]-[uuid]
        return (getEnvCommon(opts) + ' \
export UUID=$(uuid) && \
export MANTA_PRE=/$MANTA_USER/stor/$MANTA_CRUFT/do && \
export MANTA_PATTERN=$MANTA_PRE/$MARLIN_JOB-{2}-$UUID && \
sort -r | ./build/node/bin/node ./bin/cruft.js | \
  ./build/node/bin/node ./bin/mdemux.js -p $MANTA_PATTERN \
');
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        //First take what's in the config file, override what's on the
        // command line, and use the defaults if all else fails.
        var opts = MOLA_CRUFT_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        var parser = new getopt.BasicParser('a:d:m:np:r:s:t',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'd':
                        opts.marlinReducerDisk = parseInt(option.optarg, 10);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'n':
                        opts.noJobStart = true;
                        break;
                case 'p':
                        opts.marlinMapDisk = parseInt(option.optarg, 10);
                        break;
                case 'r':
                        opts.marlinReducerMemory = parseInt(option.optarg, 10);
                        break;
                case 's':
                        opts.mantaStorageId = option.optarg;
                        break;
                case 't':
                        opts.jobName = 'manta_cruft_test';
                        opts.jobRoot = MP + '/manta_cruft_test';
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.jobName = opts.jobName || 'manta_cruft';
        opts.jobRoot = opts.jobRoot || MP + '/manta_cruft';
        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.marlinMapDisk = opts.marlinMapDisk || 16;
        opts.marlinReducerMemory = opts.marlinReducerMemory || 4096;
        opts.marlinReducerDisk = opts.marlinReducerDisk || 16;
        opts.marlinPathToAsset = opts.assetObject.substring(1);
        opts.marlinAssetObject = opts.assetObject;

        opts.directories = [
                opts.jobRoot + '/do',
                opts.jobRoot + '/done'
        ];

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


//TODO: Factor out to common
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


function getCruftJob(opts, cb) {
        //Use the same number of reducers as input files.
        opts.numberReducers = opts.objects.length;

        var pgCmd = getTransformCmd(opts);
        var cruftCmd = getCruftCmd(opts);

        var job = {
                phases: [ {
                        type: 'storage-map',
                        exec: pgCmd,
                        disk: opts.marlinMapDisk
                }, {
                        type: 'reduce',
                        count: opts.numberReducers,
                        memory: opts.marlinReducerMemory,
                        disk: opts.marlinReducerDisk,
                        exec: cruftCmd
                } ]
        };

        LOG.info({ job: job }, 'Cruft Marlin Job Definition');

        cb(null, job);
}


function findMorayBackupObjects(opts, cb) {
        var shard = opts.shard;
        var earliestMakoDump = opts.earliestMakoDump;
        var offset = (opts.offset === undefined) ? 0 : opts.offset;

        if (offset === 7) {
                LOG.info('Couldn\'t find moray backup for shard ' + shard +
                         ' for 8 hours before ' + new Date(earliestMakoDump));
                cb(null);
                return;
        }

        //We need to find a backup that is as close in time to the earliest
        // mako dump, but still earlier.  We're looking for
        // /[MANTA_USER]/stor/manatee_backups/[shard]/\
        //    [year]/[month]/[day]/[hour]/\
        //    manta-[year]-[month]-[day]-[hour]-[minutes]-[seconds].[\w*]

        //Subtract one hour for each offset
        var ed = new Date(earliestMakoDump);
        var d = new Date(ed.getTime() - (offset * 60 * 60 * 1000));

        var dir = sprintf('%s/%s/%04d/%02d/%02d/%02d',
                          MANATEE_BACKUP_DIR, shard,
                          d.getUTCFullYear(), d.getUTCMonth() + 1,
                          d.getUTCDate(), d.getUTCHours() + 1);

        getObjectsInDir(dir, function (err, objects) {
                if (err && err.name === 'NotFoundError') {
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

                var objs = [];
                for (var i = 0; i < objects.length; ++i) {
                        var o = objects[i].object;
                        o.directory = dir;
                        o.fullPath = o.directory + '/' + o.name;
                        if (startsWith(o.name, MANTA_DUMP_NAME_PREFIX)) {
                                objs.push(o);
                        }
                        if (startsWith(o.name, MANTA_DELETE_LOG_NAME_PREFIX)) {
                                objs.push(o);
                        }
                }

                if (objs.length != 2 ||
                    objs[0].mtime > earliestMakoDump ||
                    objs[1].mtime > earliestMakoDump) {
                        findMorayBackupObjects({
                                'shard': shard,
                                'earliestMakoDump': earliestMakoDump,
                                'offset': offset + 1
                        }, cb);
                        return;
                }

                cb(null, objs);
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

                var objects = [];
                var earliestMorayDump = null;
                for (var i = 0; i < shards.length; ++i) {
                        var objs = results.successes[i];
                        //Ok, this is a little strange.  If we don't find one
                        // of them, then we don't want the job to continue
                        // but we don't want to log FATAL either.  So we
                        // return nothing, and that should give us ^^.
                        if (objs === null || objs === undefined ||
                            objs.length === 0) {
                                cb(null, []);
                                return;
                        }
                        objs.forEach(function (o) {
                                var mtime = new Date(o.mtime);
                                if (!earliestMorayDump ||
                                    mtime < earliestMorayDump) {
                                        earliestMorayDump = mtime;
                                }
                                objects.push(o.fullPath);
                        });
                }
                opts.earliestMorayDump = earliestMorayDump;

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
                if (err || !results.successes) {
                        cb(err, []);
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


//TODO: Use the one in common
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


///--- Main

var _opts = parseOptions();

_opts.getJobDefinition = getCruftJob;
_opts.getJobObjects = findObjects;

var _doDir = _opts.jobRoot + '/do';
getObjectsInDir(_doDir, function (err, objects) {
        if (err && err.name !== 'NotFoundError') {
                LOG.fatal(err, 'error fetching do objects');
                process.exit(1);
        }

        if (objects && objects.length > 0) {
                var m = 'Previous job output still exists in ' + _doDir +
                        '.  All previous output must be cleared before ' +
                        'a new job can be run.  Exiting...';
                LOG.info(m);
                process.exit(1);
        }

        var jobManager = lib.createJobManager(_opts, MANTA_CLIENT, LOG);
        jobManager.run(function () {
                MANTA_CLIENT.close();
                LOG.info('Done for now.');
        });
});
