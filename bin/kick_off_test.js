#!/usr/bin/env node
// -*- mode: js -*-
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var MemoryStream = require('memorystream');
var path = require('path');
var vasync = require('vasync');



///--- Global Objects

var NAME = 'mola-test';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MOLA_CONFIG = (process.env.MOLA_CONFIG ||
                   '/opt/smartdc/mola/etc/config.json');
var MOLA_CONFIG_OBJ = JSON.parse(fs.readFileSync(MOLA_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MOLA_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;



///--- Global Constants

var MP = '/' + MANTA_USER + '/stor';
var BACKUP_DIR = MP + '/manatee_backups';
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var MANTA_DELETE_LOG_DUMP_NAME_PREFIX = 'manta_delete_log-';
var JOB_DIR = MP + '/manta_test';



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
function getMapCmd(opts) {
        var grepForObject = '';
        if (opts.objectId) {
                grepForObject = ' | grep ' + opts.objectId + ' | ';
        }
        var cmd = getEnvCommon(opts) + ' \
zcat | ./build/node/bin/node ./bin/pg_transform.js | \
   ' + grepForObject + ' \
   msplit -j -f "objectId" -n ' + opts.numberReducers + ' \
';
        return (cmd);
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getReduceCmd(opts) {
        var cmd = getEnvCommon(opts) + ' \
json -ga objectid | sort | uniq -c \
';
        return (cmd);
}
/* END JSSTYLED */


function startsWith(str, prefix) {
        return (str.slice(0, prefix.length) === prefix);
}


function endsWith(str, suffix) {
        return (str.indexOf(suffix, str.length - suffix.length) !== -1);
}


function parseOptions() {
        var option;
        //First take what's in the config file, override what's on the
        // command line, and use the defaults if all else fails.
        var opts = MOLA_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        var parser = new getopt.BasicParser('a:m:o:r:',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'o':
                        opts.objectId = option.optarg;
                        break;
                case 'r':
                        opts.marlinReducerMemory = parseInt(option.optarg, 10);
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.jobName = opts.jobName || 'manta_test';
        opts.jobRoot = opts.jobRoot || MP + '/manta_test';

        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.marlinReducerMemory = opts.marlinReducerMemory || 4096;
        opts.marlinPathToAsset = opts.assetObject.substring(1);
        opts.marlinAssetObject = opts.assetObject;

        opts.testDir = opts.jobRoot + '/test';

        opts.directories = [ opts.testDir ];

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-a asset_file]';
        str += ' [-r marlin_reducer_memory]';
        str += ' [-t output_to_test]';
        console.error(str);
        process.exit(1);
}


function getObjectsInDir(dir, cb) {
        var objects = [];
        MANTA_CLIENT.ls(dir, {}, function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                res.on('object', function (obj) {
                        objects.push(dir + '/' + obj.name);
                });

                res.once('error', function (err2) {
                        cb(err2);
                });

                res.once('end', function () {
                        cb(null, objects);
                });
        });
}


function findLatestBackupObjects(opts, cb) {
        if ((typeof (opts)) === 'string' || opts instanceof String) {
                opts = {
                        dir: BACKUP_DIR + '/' + opts
                };
        }
        assert.string(opts.dir);

        var dir = opts.dir;

        MANTA_CLIENT.ls(dir, {}, function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                var dirs = [];
                var objs = [];

                res.on('directory', function (d) {
                        dirs.push(d.name);
                });

                res.on('object', function (o) {
                        objs.push(o.name);
                });

                res.on('error', function (err2) {
                        cb(err2);
                });

                res.on('end', function () {
                        //Assume that if there's objects or no further
                        // directories to walk down, we're done.
                        if (dirs.length === 0 || objs.length > 0) {
                                cb(null, {
                                        directory: dir,
                                        objects: objs
                                });
                                return;
                        }
                        dirs.sort(function (a, b) { return (b - a); });
                        dir += '/' + dirs[0];
                        findLatestBackupObjects({ dir: dir }, cb);
                });
        });
}


function getTestJob(opts, cb) {
        // 2 just for fun.
        opts.numberReducers = 2;

        var mapCmd = getMapCmd(opts);
        var reduceCmd = getReduceCmd(opts);
        var job = {
                phases: [ {
                        type: 'map',
                        exec: mapCmd
                }, {
                        type: 'reduce',
                        count: opts.numberReducers,
                        memory: opts.marlinReducerMemory,
                        exec: reduceCmd
                } ]
        };

        LOG.info({ job: job }, 'Marlin Job Definition');

        cb(null, job);
}


//Expects the filename to be in the format:
// manta-2012-11-30-23-00-07.gz
function extractDate(prefix, filename) {
        var d = filename.replace(prefix, '');
        d = d.substring(0, d.indexOf('.'));
        return (d);
}


function getTestObjects(opts, cb) {
        LOG.info({ opts: opts }, 'Running Test.');
        var shards = opts.shards;

        if (shards.length === 0) {
                cb(new Error('No shards specified.'));
                return;
        }

        vasync.forEachParallel({
                func: findLatestBackupObjects,
                inputs: shards
        }, function (err, results) {
                if (err) {
                        cb(err);
                        return;
                }
                if (results.successes.length !== shards.length) {
                        cb(new Error('Couldnt find latest backup for all ' +
                                     'shards.'));
                        return;
                }

                var objects = [];
                var dates = [];

                for (var i = 0; i < shards.length; ++i) {
                        var res = results.successes[i];
                        var dir = res.directory;
                        var objs = res.objects;

                        //Search the objects for the tables we need to process
                        var foundManta = false;
                        var foundMantaDeleteLog = false;
                        var mdnp = MANTA_DUMP_NAME_PREFIX;
                        var mdldnp = MANTA_DELETE_LOG_DUMP_NAME_PREFIX;
                        for (var j = 0; j < objs.length; ++j) {
                                var obj = objs[j];
                                if (startsWith(obj, mdnp)) {
                                        foundManta = true;
                                        objects.push(dir + '/' + obj);
                                        //Get the date from the filename...
                                        dates.push(extractDate(mdnp, obj));
                                } else if (startsWith(obj, mdldnp)) {
                                        foundMantaDeleteLog = true;
                                        objects.push(dir + '/' + obj);
                                }
                        }

                        if (!foundManta || !foundMantaDeleteLog) {
                                var m = 'Couldnt find all tables in dump ' +
                                        'directory.';
                                LOG.error({ dir: dir, objs: objs },
                                          m);
                                cb(new Error(m));
                                return;
                        }
                }

                dates.sort();
                LOG.info({ dates: dates }, 'found dates');
                opts.earliestDumpDate = dates[0];
                opts.objects = objects;
                cb(null, objects);
        });
}


///--- Main

var _opts = parseOptions();

_opts.getJobDefinition = getTestJob;
_opts.getJobObjects = getTestObjects;

var jobManager = lib.createJobManager(_opts, MANTA_CLIENT, LOG);
jobManager.run(function () {
        LOG.info('Done for now.');
});
