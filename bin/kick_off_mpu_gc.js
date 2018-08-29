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

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var path = require('path');

/*
 * Kicks off the MPU GC job, which pg-transforms the PG backups, sorts them, and
 * determines which MPU-related records can be garbage collected.
 *
 * This script is analogous to bin/kick_off_gc.js for normal GC.
 */

///--- Global Objects

var NAME = 'mola-mpu-gc';
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
var MANTA_UPLOADS_NAME_PREFIX = 'manta_uploads-';


///--- Helpers

/*
 * Helper that sets up necessary environment variables for the commands run as
 * part of a phase in the MPU GC job.
 *
 * Inputs:
 *  - opts: an options blob that must include:
 *      - jobName: name of the job to pass to the job manager
 *      - marlinPathToAsset: the relative path of a tarball that is unpacked as
 *          an asset in the job
 */
function getEnvCommon(opts) {
        assert.object(opts, 'opts');
        assert.string(opts.jobName, 'opts.jobName');
        assert.string(opts.marlinPathToAsset, 'opts.marlinPathToAsset');

/* BEGIN JSSTYLED */
        return (' \
set -o pipefail && \
export MANTA_USER=' + MANTA_USER + ' && \
export MANTA_MPU_GC=' + opts.jobName + ' && \
export MARLIN_JOB=$(echo $MANTA_OUTPUT_BASE | cut -d "/" -f 4) && \
export NOW=$(date "+%Y-%m-%d-%H-%M-%S") && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
/* END JSSTYLED */
}


/*
 * Returns the command that is run during the map phase of the MPU GC job.
 * This command calls into bin/mpu_gc_pg_transform.js, which transforms
 * input from the dump into tab-separated records that can be processed
 * by the reduce phase of the job.
 *
 * Inputs:
 *  - opts: an options blob with the following values:
 *      - earliestDumpDate: the earliest dump date to use
 *      - numberReducers: number of reducers to assign to the job
 *      - objectId: optional objectId
 *      - jobName: required for getEnvCommon
 *      - marlinPathToAsset: required for getEnvCommon
 */
function getMpuPgTransformCmd(opts) {
        assert.object(opts, 'opts');
        assert.string(opts.earliestDumpDate, 'opts.earliestDumpDate');
        assert.number(opts.numberReducers, 'opts.numberReducers');

/* BEGIN JSSTYLED */
        var grepForObject = '';
        if (opts.objectId) {
                grepForObject = ' | grep ' + opts.objectId + ' | ';
        }
        return (getEnvCommon(opts) + ' \
export MORAY_SHARD=$(echo $mc_input_key | cut -d "/" -f 5) && \
export DUMP_DATE=$(basename $mc_input_key | sed \'s/^\\w*-//; s/.\\w*$//;\') && \
gzcat -f | \
  ./build/node/bin/node ./bin/mpu_gc_pg_transform.js -d $DUMP_DATE \
    -e ' + opts.earliestDumpDate + ' \
    -m $MORAY_SHARD' + grepForObject + ' | \
  msplit -n ' + opts.numberReducers + ' \
');
/* END JSSTYLED */
}


/*
 * Returns the command that is run during the reduce phase of the MPU GC job.
 * This phase calls into bin/mpu_gc.js, which is a thin wrapper that calls into
 * lib/mpu_garbage_collector.js, which performs the actual logic of deciding
 * what mako and moray actions need to be taken.
 *
 * Inputs:
 *  - opts: an options blob with the following values:
 *      - gracePeriodSeconds: optional grace period for MPU
 *      - jobName: required for getEnvCommon
 *      - marlinPathToAsset: required for getEnvCommon
 */
function getMpuGcCmd(opts) {
        assert.object(opts, 'opts');
        assert.optionalNumber(opts.gracePeriodSeconds,
            'opts.gracePeriodSeconds');

        var gracePeriodOption = '';
        if (opts.gracePeriodSeconds) {
                gracePeriodOption = ' -g ' + opts.gracePeriodSeconds;
        }
        /*
         * As the normal GC job does, we use a UUID only because there's no way
         * (yet) to get a reference to which reducer this is running on.
         */
/* BEGIN JSSTYLED */
        return (getEnvCommon(opts) + ' \
export UUID=$(uuid) && \
export MANTA_PRE=/$MANTA_USER/stor/$MANTA_MPU_GC && \
export MANTA_MPU_GC_CLEANUP_FILE=$MANTA_PRE/cleanup/$NOW-$MARLIN_JOB-X-$UUID && \
sort | \
./build/node/bin/node ./bin/mpu_gc.js' + gracePeriodOption + ' | \
mpipe $MANTA_MPU_GC_CLEANUP_FILE \
');
/* END JSSTYLED */
}

function parseOptions() {
        var option;
        /*
         * First take what's in the config file, override what's on the
         * command line, and use the defaults if all else fails.
         */
        var opts = MOLA_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        var parser = new getopt.BasicParser('a:bd:g:s:m:no:p:r:tF',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'a':
                        opts.assetFile = option.optarg;
                        break;
                case 'b':
                        opts.mapPhaseOnly = true;
                        break;
                case 'd':
                        opts.gcReduceDisk = parseInt(option.optarg, 10);
                        break;
                case 'g':
                        opts.gracePeriodSeconds = parseInt(option.optarg, 10);
                        break;
                case 's':
                        opts.maxHoursInPast = parseInt(option.optarg, 10);
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
                        opts.gcMapDisk = parseInt(option.optarg, 10);
                        break;
                case 'r':
                        opts.gcReduceMemory = parseInt(option.optarg, 10);
                        break;
                case 't':
                        opts.jobName = 'manta_mpu_gc_test';
                        opts.jobRoot = MP + '/manta_mpu_gc_test';
                        break;
                case 'F':
                        opts.forceRun = true;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        opts.jobName = opts.jobName || 'manta_mpu_gc';
        opts.jobRoot = opts.jobRoot || MP + '/manta_mpu_gc';

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
                opts.jobRoot + '/cleanup',
                opts.jobRoot + '/completed'
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


/*
 * Returns a job definition for the MPU GC job that can be passed to the
 * job manager.
 *
 * Inputs:
 *  - opts: options blob passed to helpers creating phases of the job (see
 *          those functions for documentation)
 *  - cb: callback of the form cb(err, job)
 */
function getMpuGcJob(opts, cb) {
        /*
         * As with the regular GC job, use the number of shards + 1 reducers so
         * that we are always using multiple reducers.
         */
        opts.numberReducers = opts.shards.length + 1;

        var mpuPgCmd = getMpuPgTransformCmd(opts);
        var mpuGcCmd = getMpuGcCmd(opts);

        var phases = [
                {
                        type: 'storage-map',
                        exec: mpuPgCmd,
                        disk: opts.gcMapDisk
                }
        ];

        if (!opts.mapPhaseOnly) {
                phases.push({
                        type: 'reduce',
                        count: opts.numberReducers,
                        memory: opts.gcReduceMemory,
                        disk: opts.gcReduceDisk,
                        exec: mpuGcCmd
                });
        }

        var job = {
                phases: phases
        };

        LOG.info({
                job: job
        }, 'MPU GC Marlin Job Definition');

        cb(null, job);
}


// Expects the filename to be in the format:
//      /.../manta-2012-11-30-23-00-07.gz
//
// Returns: 2012-11-30-23-00-07
function extractDate(p) {
        var filename = path.basename(p);
        var d = filename.substring(filename.indexOf('-') + 1);
        d = d.substring(0, d.indexOf('.'));
        return (d);
}


/*
 * Determines what input objects to pass to the MPU GC job.
 *
 * Inputs:
 *  - opts: an options block passed directly to common.findObjectsForShards
 *  - cb: callback of the form cb(err, objects)
 */
function findMpuGcObjects(opts, cb) {
        LOG.info({ opts: opts }, 'Finding MPU Gc Objects.');
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
                        MANTA_UPLOADS_NAME_PREFIX
                ],
                'maxHoursInPast': opts.maxHoursInPast
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
                        // Get the date from the filename.
                        dates.push(extractDate(obj));
                }

                dates.sort();
                LOG.info({
                        dates: dates,
                        objects: objects
                }, 'found mpu gc objects');
                opts.earliestDumpDate = dates[0];
                cb(null, objects);
        });
}



///--- Main

var _opts = parseOptions();

_opts.getJobDefinition = getMpuGcJob;
_opts.getJobObjects = findMpuGcObjects;

var jobManager = lib.createJobManager(_opts, MANTA_CLIENT, LOG);
jobManager.run(function () {
        MANTA_CLIENT.close();
        LOG.info('Done for now.');
});
