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

var NAME = 'mola';
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
var AUDIT = {
        'audit': true,
        'startedJob': 0,
        'cronFailed': 1,
        'startTime': new Date()
};



///--- Global Constants

var MP = '/' + MANTA_USER + '/stor';
var BACKUP_DIR = MP + '/manatee_backups';
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var MANTA_DELETE_LOG_DUMP_NAME_PREFIX = 'manta_delete_log-';
var RUNNING_STATE = 'running';
var MAX_SECONDS_IN_AUDIT_OBJECT = 60 * 60 * 24 * 7; // 7 days
var MAX_HOURS_IN_PAST = 8;



///--- Helpers

/* BEGIN JSSTYLED */
function getEnvCommon(opts) {
        return (' \
set -o pipefail && \
export MANTA_USER=' + MANTA_USER + ' && \
export MANTA_GC=' + opts.jobName + ' && \
export MARLIN_JOB=$(echo $MANTA_OUTPUT_BASE | cut -d "/" -f 4) && \
export NOW=$(date "+%Y-%m-%d-%H-%M-%S") && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getPgTransformCmd(opts) {
        var grepForObject = '';
        if (opts.objectId) {
                grepForObject = ' | grep ' + opts.objectId + ' | ';
        }
        return (getEnvCommon(opts) + ' \
export MORAY_SHARD=$(echo $mc_input_key | cut -d "/" -f 5) && \
export DUMP_DATE=$(basename $mc_input_key | sed \'s/^\\w*-//; s/.\\w*$//;\') && \
gzcat -f | \
  ./build/node/bin/node ./bin/gc_pg_transform.js -d $DUMP_DATE \
    -e ' + opts.earliestDumpDate + ' \
    -m $MORAY_SHARD' + grepForObject + ' \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getGcCmd(opts) {
        var gracePeriodOption = '';
        if (opts.gracePeriodSeconds) {
                gracePeriodOption = ' -g ' + opts.gracePeriodSeconds;
        }
        //We use a UUID only because there's no way (yet) to get a reference
        // to which reducer this is running on.
        return (getEnvCommon(opts) + ' \
export UUID=$(uuid) && \
export MANTA_PRE=/$MANTA_USER/stor/$MANTA_GC/all && \
export MANTA_FILE_PRE=$MANTA_PRE/done/$NOW-$MARLIN_JOB-X-$UUID && \
export MANTA_PATTERN=$MANTA_FILE_PRE-{1}-{2} && \
export MANTA_LINKS=$MANTA_PRE/do/$NOW-$MARLIN_JOB-X-$UUID-links && \
export PERL=/usr/perl5/bin/perl && \
export LINKS_FILE=./links.txt && \
sort | \
  ./build/node/bin/node ./bin/gc.js' + gracePeriodOption + ' | \
  $PERL ./bin/gc_links.pl $MANTA_USER $LINKS_FILE $MANTA_FILE_PRE | \
  ./build/node/bin/node ./bin/mdemux.js -p $MANTA_PATTERN && \
cat $LINKS_FILE | mpipe $MANTA_LINKS \
');
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        //First take what's in the config file, override what's on the
        // command line, and use the defaults if all else fails.
        var opts = MOLA_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        var parser = new getopt.BasicParser('a:g:m:no:r:t',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'g':
                        opts.gracePeriodSeconds = parseInt(option.optarg, 10);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 'n':
                        opts.noJobStart = true;
                        break;
                case 'o':
                        opts.objectId = option.optarg;
                        break;
                case 'r':
                        opts.marlinReducerMemory = parseInt(option.optarg, 10);
                        break;
                case 't':
                        opts.jobName = 'manta_gc_test';
                        opts.jobRoot = MP + '/manta_gc_test';
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        //Set up some defaults...
        opts.jobName = opts.jobName || 'manta_gc';
        opts.jobRoot = opts.jobRoot || MP + '/manta_gc';

        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.marlinReducerMemory = opts.marlinReducerMemory || 4096;
        opts.marlinPathToAsset = opts.assetObject.substring(1);
        opts.marlinAssetObject = opts.assetObject;

        opts.directories = [
                opts.jobRoot + '/all',
                opts.jobRoot + '/all/do',
                opts.jobRoot + '/all/done',
                opts.jobRoot + '/mako',
                opts.jobRoot + '/moray'
        ];

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-a asset_file]';
        str += ' [-g grace_period_seconds]';
        str += ' [-m moray_shard]';
        str += ' [-n no_job_start]';
        str += ' [-o object_id]';
        str += ' [-r marlin_reducer_memory]';
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


function pad(n) {
        return ((n < 10) ? '0' + n : '' + n);
}


function findLatestBackupObjects(opts, cb) {
        if ((typeof (opts)) === 'string' || opts instanceof String) {
                opts = {
                        'shard': opts,
                        'iteration': 0,
                        'timestamp': new Date().getTime()
                };
        }
        assert.string(opts.shard, 'opts.shard');
        assert.number(opts.iteration, 'opts.iteration');
        assert.number(opts.timestamp, 'opts.timestamp');

        // Kick out here
        if (opts.iteration >= MAX_HOURS_IN_PAST) {
                cb(new Error('Couldnt find objects for ' +
                             opts.shard + ' in past ' +
                             opts.iteration + ' hours'));
                return;
        }

        // # of iteration hours before
        var d = new Date(opts.timestamp - (opts.iteration * 60 * 60 * 1000));

        // Construct a path like:
        // /poseidon/stor/manatee_backups/1.moray.coal.joyent.us/2014/05/04/20
        var dir = BACKUP_DIR + '/' +
                opts.shard + '/' +
                d.getFullYear() + '/' +
                pad(d.getMonth() + 1) + '/' +
                pad(d.getDate()) + '/' +
                pad(d.getHours());

        MANTA_CLIENT.ls(dir, {}, function (err, res) {
                function next() {
                        opts.iteration += 1;
                        findLatestBackupObjects(opts, cb);
                }
                if (err && err.code !== 'NotFoundError') {
                        cb(err);
                        return;
                }
                if (err) {
                        next();
                        return;
                }

                var objs = [];

                res.on('object', function (o) {
                        objs.push(o.name);
                });

                res.on('error', function (err2) {
                        cb(err2);
                });

                res.on('end', function () {
                        var foundManta = false;
                        var foundDeleteLog = false;
                        objs.forEach(function (o) {
                                if (startsWith(o, MANTA_DUMP_NAME_PREFIX)) {
                                        foundManta = true;
                                }
                                var mdlp = MANTA_DELETE_LOG_DUMP_NAME_PREFIX;
                                if (startsWith(o, mdlp)) {
                                        foundDeleteLog = true;
                                }
                        });
                        if (foundManta && foundDeleteLog) {
                                cb(null, {
                                        directory: dir,
                                        objects: objs
                                });
                                return;
                        }
                        next();
                });
        });
}


function getGcJob(opts, cb) {
        //We use the number of shards + 1 so that we know
        // we are always using multiple reducers.  There's
        // no reason this can't be much more.

        //MANTA-840
        //var nReducers = opts.shards.length + 1;
        opts.numberReducers = 1;

        var pgCmd = getPgTransformCmd(opts);
        var gcCmd = getGcCmd(opts);
        var job = {
                phases: [ {
                        type: 'storage-map',
                        exec: pgCmd
                }, {
                        type: 'reduce',
                        count: opts.numberReducers,
                        memory: opts.marlinReducerMemory,
                        exec: gcCmd
                } ]
        };

        LOG.info({ job: job }, 'GC Marlin Job Definition');

        cb(null, job);
}


//Expects the filename to be in the format:
// manta-2012-11-30-23-00-07.gz
function extractDate(prefix, filename) {
        var d = filename.replace(prefix, '');
        d = d.substring(0, d.indexOf('.'));
        return (d);
}


function findGcObjects(opts, cb) {
        LOG.info({ opts: opts }, 'Finding Gc Objects.');
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
                LOG.info({
                        dates: dates,
                        objects: objects
                }, 'found gc objects');
                opts.earliestDumpDate = dates[0];

                cb(null, objects);
        });
}



///--- Main

var _opts = parseOptions();

_opts.getJobDefinition = getGcJob;
_opts.getJobObjects = findGcObjects;

var jobManager = lib.createJobManager(_opts, MANTA_CLIENT, LOG);
jobManager.run(function () {
        MANTA_CLIENT.close();
        LOG.info('Done for now.');
});
