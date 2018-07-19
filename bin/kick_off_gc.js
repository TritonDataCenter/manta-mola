#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var bunyan = require('bunyan');
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var path = require('path');



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



///--- Global Constants

var MP = '/' + MANTA_USER + '/stor';
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var MANTA_DELETE_LOG_DUMP_NAME_PREFIX = 'manta_delete_log-';



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
    -m $MORAY_SHARD' + grepForObject + ' | \
  msplit -n ' + opts.numberReducers + ' \
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
        var parser = new getopt.BasicParser('a:d:g:m:no:p:r:tF', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'd':
                        opts.gcReduceDisk = lib.common.parseNumberOption(
                            option.optarg, '-d', 1, null, usage);
                        break;
                case 'g':
                        opts.gracePeriodSeconds = lib.common.parseNumberOption(
                            option.optarg, '-g', 1, null, usage);
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
                case 'p':
                        opts.gcMapDisk = lib.common.parseNumberOption(
                            option.optarg, '-p', 1, null, usage);
                        break;
                case 'r':
                        opts.gcReduceMemory = lib.common.parseNumberOption(
                            option.optarg, '-r', 1, null, usage);
                        break;
                case 't':
                        opts.jobName = 'manta_gc_test';
                        opts.jobRoot = MP + '/manta_gc_test';
                        break;
                case 'F':
                        opts.forceRun = true;
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

        opts.jobEnabled = opts.gcEnabled;
        opts.gcMapDisk = opts.gcMapDisk || 32;
        opts.gcReduceMemory = opts.gcReduceMemory || 8192;
        opts.gcReduceDisk = opts.gcReduceDisk || 32;
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
        str += ' [-F force_run]';
        console.error(str);
        process.exit(1);
}



function getGcJob(opts, cb) {
        //We use the number of shards + 1 so that we know
        // we are always using multiple reducers.  There's
        // no reason this can't be much more.
        opts.numberReducers = opts.shards.length + 1;

        var pgCmd = getPgTransformCmd(opts);
        var gcCmd = getGcCmd(opts);
        var job = {
                phases: [ {
                        type: 'storage-map',
                        exec: pgCmd,
                        disk: opts.gcMapDisk
                }, {
                        type: 'reduce',
                        count: opts.numberReducers,
                        memory: opts.gcReduceMemory,
                        disk: opts.gcReduceDisk,
                        exec: gcCmd
                } ]
        };

        LOG.info({ job: job }, 'GC Marlin Job Definition');

        cb(null, job);
}


//Expects the filename to be in the format:
// /.../manta-2012-11-30-23-00-07.gz
// Returns: 2012-11-30-23-00-07
function extractDate(p) {
        var filename = path.basename(p);
        var d = filename.substring(filename.indexOf('-') + 1);
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

        lib.common.findObjectsForShards({
                'log': LOG,
                'shards': shards,
                'client': MANTA_CLIENT,
                'tablePrefixes': [
                        MANTA_DUMP_NAME_PREFIX,
                        MANTA_DELETE_LOG_DUMP_NAME_PREFIX
                ]
        }, function (err, results) {
                if (err) {
                        cb(err);
                        return;
                }

                var objects = [];
                var dates = [];

                for (var j = 0; j < results.length; ++j) {
                        var obj = results[j];
                        objects.push(obj);
                        //Get the date from the filename...
                        dates.push(extractDate(obj));
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
