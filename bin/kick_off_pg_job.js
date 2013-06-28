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
var vasync = require('vasync');



///--- Global Objects

var NAME = 'pg_job';
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
        var cmd = getEnvCommon(opts) + ' \
zcat | ./build/node/bin/node ./bin/pg_transform.js | \
   ' + opts.map + ' \
';
        return (cmd);
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getReduceCmd(opts, red) {
        var cmd = getEnvCommon(opts) + ' \
' + red + ' \
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
        opts.reduces = opts.reduces || [];
        opts.tablePrefixes = opts.tablePrefixes || [];
        var parser = new getopt.BasicParser('a:c:e:m:np:r:t:',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'c':
                        opts.reduces.push(option.optarg);
                        break;
                case 'e':
                        opts.numberReducers = parseInt(option.optarg, 10);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'n':
                        opts.noJobStart = true;
                        break;
                case 'p':
                        opts.map = option.optarg;
                        break;
                case 'r':
                        opts.marlinReducerMemory = parseInt(option.optarg, 10);
                        break;
                case 't':
                        opts.tablePrefixes.push(option.optarg);
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (!opts.map) {
                usage('map must be specified');
        }

        //Set up some defaults...
        opts.jobName = opts.jobName || 'manta_pg_job';
        opts.jobRoot = opts.jobRoot || MP + '/manta_pg_job';

        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.marlinReducerMemory = opts.marlinReducerMemory || 4096;
        opts.marlinPathToAsset = opts.assetObject.substring(1);
        opts.marlinAssetObject = opts.assetObject;

        opts.numberReducers = opts.numberReducers || 1;

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-a asset_file]';
        str += ' [-c reduce_command (can be repeated)]';
        str += ' [-e number of reducers]';
        str += ' [-m moray_shard (can be repeated)]';
        str += ' [-n no_job_start]';
        str += ' [-p map_command (only once)]';
        str += ' [-r marlin_reducer_memory]';
        str += ' [-t table_prefix]';
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


//TODO: Factor out in a common place?  This is copied from gc.
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
                        //Assume that if there's no further directories to walk
                        // down, we're done.
                        if (dirs.length === 0) {
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


function getJob(opts, cb) {
        var job = {
                phases: []
        };

        job.phases.push({
                type: 'map',
                exec: getMapCmd(opts)
        });
        for (var i = 0; i < opts.reduces.length; ++i) {
                job.phases.push({
                        type: 'reduce',
                        count: opts.numberReducers,
                        memory: opts.marlinReducerMemory,
                        exec: getReduceCmd(opts, opts.reduces[i])
                });
        }

        cb(null, job);
}


function getObjects(opts, cb) {
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

                for (var i = 0; i < shards.length; ++i) {
                        var res = results.successes[i];
                        var dir = res.directory;
                        var objs = res.objects;
                        var tp = opts.tablePrefixes.map(function (p) {
                                return (p);
                        });

                        //Search the objects for the tables we need to process
                        for (var j = 0; j < objs.length; ++j) {
                                var obj = objs[j];
                                for (var k = 0; k < tp.length; ++k) {
                                        if (startsWith(obj, tp[k])) {
                                                objects.push(dir + '/' + obj);
                                        }
                                }
                        }
                }

                cb(null, objects);
        });
}


///--- Main

var _opts = parseOptions();

_opts.getJobDefinition = getJob;
_opts.getJobObjects = getObjects;

var jobManager = lib.createJobManager(_opts, MANTA_CLIENT, LOG);
jobManager.run(function () {
        MANTA_CLIENT.close();
        LOG.info('Done for now.');
});
